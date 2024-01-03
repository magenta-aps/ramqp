# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""This module contains the Abstract AMQPSystem."""
import asyncio
import json
from abc import ABCMeta
from asyncio import CancelledError
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from functools import partial
from types import TracebackType
from typing import Any
from typing import cast
from typing import Generic
from typing import TypeVar

import structlog
from aio_pika import connect_robust
from aio_pika import ExchangeType
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractQueue
from aio_pika.abc import AbstractRobustChannel
from aio_pika.abc import AbstractRobustConnection
from aiormq import ChannelPreconditionFailed
from fastapi.encoders import jsonable_encoder
from more_itertools import all_unique
from more_itertools import one

from .config import AMQPConnectionSettings
from .depends import dependency_injected
from .metrics import _handle_publish_metrics
from .metrics import _handle_receive_metrics
from .metrics import _setup_channel_metrics
from .metrics import _setup_connection_metrics
from .metrics import _setup_periodic_metrics
from .metrics import callbacks_registered
from .metrics import routes_bound
from .utils import AcknowledgeMessage
from .utils import CallbackType
from .utils import function_to_name
from .utils import RejectMessage
from .utils import RequeueMessage

logger = structlog.get_logger()


class AbstractRouter:
    """Abstract base-class for Routers.

    Shared code used by both Router and MORouter.
    """

    def __init__(self) -> None:
        self.registry: dict[CallbackType, set[str]] = {}

    def _register(self, routing_key: Any) -> Callable[[CallbackType], CallbackType]:
        """Get a decorator for registering callbacks.

        Examples:
            ```
            address_create_decorator = router.register("EMPLOYEE.ADDRESS.CREATE")

            @address_create_decorator
            def callback1(message: IncomingMessage):
                pass

            @address_create_decorator
            def callback2(message: IncomingMessage):
                pass
            ```
            Or directly:
            ```
            @router.register("EMPLOYEE.ADDRESS.CREATE")
            def callback1(message: IncomingMessage):
                pass

            @router.register("EMPLOYEE.ADDRESS.CREATE")
            def callback2(message: IncomingMessage):
                pass
            ```

        Args:
            routing_key: The routing key to bind messages for.

        Returns:
            A decorator for registering a function to receive callbacks.
        """
        # Allow using any custom object as routing key as long as it implements __str__
        routing_key = str(routing_key)
        assert routing_key != ""

        def decorator(function: CallbackType) -> CallbackType:
            """Registers the given callback and routing key in the callback registry.

            Args:
                function: The callback to registry.

            Returns:
                The unmodified given function.
            """
            function_name = function_to_name(function)

            log = logger.bind(routing_key=routing_key, function=function_name)
            log.info("Register called")

            callbacks_registered.labels(routing_key).inc()
            self.registry.setdefault(function, set()).add(routing_key)
            return function

        return decorator


class AbstractPublishMixin:
    """Abstract base-class for Publish Mixins.

    Shared code used by both PublishMixin and MOPublishMixin.
    """

    _exchange: AbstractExchange | None
    _channel: AbstractRobustChannel | None = None

    async def _publish_message(
        self, routing_key: Any, payload: Any, exchange: str | None = None
    ) -> None:
        """Publish a message to the given routing key.

        Args:
            routing_key: The routing key to send the message to.
            payload: The message payload.
            exchange: Defaults to the configured exchange if not given.

        Raises:
            ValueError: If the AMQPSystem has not been started yet.
        """
        if self._channel is None or self._exchange is None:
            raise ValueError("Must call start() before publish message!")

        if exchange is None or exchange == self._exchange.name:
            publish_exchange = self._exchange
        else:
            publish_exchange = await self._channel.get_exchange(exchange, ensure=False)

        # Allow using any custom object as routing key as long as it implements __str__
        routing_key = str(routing_key)
        with _handle_publish_metrics(routing_key):
            message = Message(
                body=json.dumps(jsonable_encoder(payload)).encode("utf-8")
            )
            await publish_exchange.publish(routing_key=routing_key, message=message)


# pylint: disable=invalid-name
TRouter = TypeVar("TRouter", bound=AbstractRouter)
# Workaround until Self Types in Python 3.11 (PEP673)
# pylint: disable=invalid-name
TAMQPSystem = TypeVar("TAMQPSystem", bound="AbstractAMQPSystem")


# pylint: disable=too-many-instance-attributes
class AbstractAMQPSystem(AbstractAsyncContextManager, Generic[TRouter]):
    """Abstract base-class for AMQPSystems.

    Shared code used by both AMQPSystem and MOAMQPSystem.
    """

    __metaclass__ = ABCMeta
    router_cls: type[TRouter]

    def __init__(
        self,
        settings: AMQPConnectionSettings,
        router: TRouter | None = None,
        context: Mapping | None = None,
    ) -> None:
        self.settings = settings

        if router is None:
            router = self.router_cls()
        self.router: TRouter = router

        # None check is important to avoid losing the reference to falsy passed object
        if context is None:
            context = {}
        self.context = context

        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._exchange: AbstractExchange | None = None
        self._queues: dict[str, AbstractQueue] = {}

        self._periodic_task: asyncio.Task | None = None

    @property
    def started(self) -> bool:
        """Whether a connection has been made.

        Returns:
            Whether a connection has been made.
        """
        return self._connection is not None

    def healthcheck(self) -> bool:
        """Whether the system is running, alive and well

        Returns:
            Whether the system is running, alive and well
        """
        return (
            self._connection is not None
            and self._connection.is_closed is False
            and self._channel is not None
            and self._channel.is_closed is False
            and self._channel.is_initialized
        )

    async def start(self) -> None:
        """Start the AMQPSystem.

        This method:
        * Connects to the AMQP server.
        * Sets up the AMQP exchange if it does not exist
        * Creates durable queues for each callback in the callback registry.
        * Binds routes to the queues according to the callback registry.
        * Setups up periodic metrics.
        """
        logger.info("Starting AMQP system")
        settings = self.settings

        # We expect no active connections to be established
        assert self._connection is None
        assert self._channel is None
        assert self._exchange is None

        # We expect function_to_name to be unique for each callback
        assert all_unique(map(function_to_name, self.router.registry.keys()))

        # We expect queue_prefix to be set if any callbacks are registered
        if self.router.registry:
            assert settings.queue_prefix is not None

        url = settings.get_url()
        logger.info(
            "Establishing AMQP connection",
            scheme=url.scheme,
            user=url.user,
            host=url.host,
            port=url.port,
            path=url.path,
        )
        self._connection = await connect_robust(url)
        _setup_connection_metrics(self._connection)

        logger.info("Creating AMQP channel")
        self._channel = cast(AbstractRobustChannel, await self._connection.channel())
        await self._channel.set_qos(prefetch_count=settings.prefetch_count)
        _setup_channel_metrics(self._channel)

        logger.info("Attaching AMQP exchange to channel", exchange=settings.exchange)
        # Make our exchange durable so it survives broker restarts
        self._exchange = await self._channel.declare_exchange(
            settings.exchange, ExchangeType.TOPIC, durable=True
        )

        # TODO: Create queues and binds in parallel?
        self._queues = {}
        for callback, routing_keys in self.router.registry.items():
            function_name = function_to_name(callback)
            queue_name = f"{settings.queue_prefix}_{function_name}"
            log = logger.bind(queue=queue_name)

            log.info("Declaring unique message queue")
            # Make our queue quorum, so it survives broker restarts, and so that
            # messages that are rejected or nacked will be returned to the _back_ of
            # the queue. This allows us to consume as many messages as possible,
            # instead of blocking on the first unprocessable one.
            # NOTE: If a delivery-limit is set, the queue will use the original
            # behaviour of returning the message near the head of the queue.
            # https://www.rabbitmq.com/quorum-queues.html#repeated-requeues

            # TODO: BEGIN quorum migration
            async def ensure_quorum() -> bool:
                """Attempt to allow migration to a quorum queue.

                Returns: True, if the queue is already, or can be, migrated to
                         (declared as) a quorum queue. False otherwise.
                """
                # pylint: disable=cell-var-from-loop
                assert self._connection is not None
                try:
                    # Try to declare the queue as quorum. The call uses a temporary
                    # channel since it will be closed on ChannelPreconditionFailed.
                    async with self._connection.channel() as temporary_channel:
                        await temporary_channel.declare_queue(
                            queue_name,
                            durable=True,
                            arguments={
                                "x-queue-type": "quorum",
                            },
                        )
                    # If that worked, the queue is already migrated
                    return True
                except ChannelPreconditionFailed as error:
                    # If we cannot declare the queue as quorum, it's probably because
                    # it already exists as a non-quorum classical queue.
                    assert "inequivalent arg 'x-queue-type'" in one(error.args)
                    # If so -- and it's empty -- try to delete it
                    try:
                        log.warning("Quorum migration: Deleting existing classic queue")
                        async with self._connection.channel() as temporary_channel:
                            q = await temporary_channel.get_queue(
                                queue_name, ensure=True
                            )
                            await q.delete(if_empty=True)
                            # If that worked, the queue can be declared as quorum
                            return True
                    except ChannelPreconditionFailed:
                        # If we are unable to delete the existing queue, there's nothing
                        # we can do. Let the caller know they should use a classic queue
                        # for now.
                        log.warning("Unable to delete queue (probably non-empty)")
                        return False

            if not await ensure_quorum():
                queue = await self._channel.declare_queue(queue_name, durable=True)
            else:
                # TODO: END quorum migration
                # This is the code that would remain after the migration period
                queue = await self._channel.declare_queue(
                    queue_name,
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                    },
                )

            self._queues[function_name] = queue

            log.info("Starting message listener")
            await queue.consume(partial(self._on_message, callback))

            log.info("Binding routing keys")
            for routing_key in routing_keys:
                log.info("Binding routing-key", routing_key=routing_key)
                await queue.bind(self._exchange, routing_key=routing_key)
                routes_bound.labels(function_name).inc()

        self._periodic_task = _setup_periodic_metrics(self._queues)

    async def stop(self) -> None:
        """Stop the AMQPSystem.

        This method disconnects from the AMQP server, and stops the periodic metrics.
        """
        logger.info("Stopping AMQP system")
        if self._periodic_task is not None:
            self._periodic_task.cancel()
            self._periodic_task = None

        self._exchange = None

        if self._channel is not None:
            self._channel = None

        if self._connection is not None:
            logger.info("Closing AMQP connection")
            await self._connection.close()
            self._connection = None

    async def __aenter__(self: TAMQPSystem) -> TAMQPSystem:
        """Start the AMQPSystem.

        Returns:
            Self
        """
        await self.start()
        return await super().__aenter__()  # type: ignore[no-any-return]

    async def __aexit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        """Stop the AMQPSystem.

        Args:
            __exc_type: AbstractAsyncContextManager argument.
            __exc_value: AbstractAsyncContextManager argument.
            __traceback: AbstractAsyncContextManager argument.

        Returns:
            None
        """
        await self.stop()
        return await super().__aexit__(__exc_type, __exc_value, __traceback)

    async def run_forever(self) -> None:
        """Start the AMQPSystem, if it isn't already, and run it forever."""
        logger.info("Running forever")
        if not self.started:
            await self.start()
        try:
            # Wait until terminate
            await asyncio.get_event_loop().create_future()
        except CancelledError:
            logger.info("Run cancelled")

    async def _on_message(
        self, callback: CallbackType, message: IncomingMessage
    ) -> None:
        """Message handler.

        Handles logging, metrics, retrying and exception handling.

        Args:
            callback: The callback to call with the message.
            message: The message to deliver to the callback.

        Raises:
            Exception: If any exception occurs during the callback.
        """
        assert message.routing_key is not None
        routing_key = message.routing_key
        function_name = function_to_name(callback)
        log = logger.bind(
            function=function_name,
            routing_key=routing_key,
            message_id=message.message_id,
        )

        log.debug("Received message")
        try:
            with _handle_receive_metrics(routing_key, function_name):
                # Requeue messages on exceptions, so they can be retried.
                async with message.process(ignore_processed=True):
                    try:
                        # TODO: Add retry metric
                        await dependency_injected(callback)(
                            message=message, context=self.context
                        )
                    except RejectMessage:
                        await message.reject(requeue=False)
                        log.info("Rejected message")
                    except AcknowledgeMessage:
                        return
                    except RequeueMessage:
                        await message.reject(requeue=True)
                        log.info("Requested requeueing of message")
                    except Exception as exception:
                        await message.reject(requeue=True)
                        raise exception
        except Exception as exception:
            log.exception("Exception during on_message()")
            raise exception
