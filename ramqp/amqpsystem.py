# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import json
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import structlog
from aio_pika import connect_robust
from aio_pika import ExchangeType
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractRobustConnection
from more_itertools import all_unique

from .config import ConnectionSettings
from .metrics import backlog_count
from .metrics import callbacks_registered
from .metrics import event_counter
from .metrics import exception_callback_counter
from .metrics import last_loop_periodic
from .metrics import last_on_message
from .metrics import last_periodic
from .metrics import processing_calls
from .metrics import processing_inprogress
from .metrics import processing_time
from .metrics import routes_bound
from .utils import CallbackType


logger = structlog.get_logger()


class InvalidRegisterCallException(Exception):
    pass


def function_to_name(function: Callable) -> str:
    """Get a uniquely qualified name for a given function."""
    return function.__name__


class AMQPSystem:
    def __init__(self) -> None:
        self._registry: Dict[CallbackType, Set[str]] = {}

        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._exchange: Optional[AbstractExchange] = None

        self._periodic_task: Optional[asyncio.Task] = None

    @property
    def started(self) -> bool:
        return self._connection is not None

    def register(self, routing_key: str) -> Callable[[CallbackType], CallbackType]:
        assert routing_key != ""

        def decorator(function: CallbackType) -> CallbackType:
            function_name = function_to_name(function)

            log = logger.bind(routing_key=routing_key, function=function_name)
            log.info("Register called")

            if self.started:
                message = "Cannot register callback after run() has been called!"
                log.error(message)
                raise InvalidRegisterCallException(message)

            callbacks_registered.labels(routing_key).inc()
            self._registry.setdefault(function, set()).add(routing_key)
            return function

        return decorator

    async def stop(self) -> None:
        if self._periodic_task is not None:
            self._periodic_task.cancel()
            self._periodic_task = None

        self._exchange = None

        if self._channel is not None:
            await self._channel.close()
            self._channel = None

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def start(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        settings = ConnectionSettings(*args, **kwargs)

        assert self._connection is None
        assert self._channel is None
        assert self._exchange is None

        logger.info(
            "Establishing AMQP connection",
            url=settings.amqp_url.replace(
                ":" + (settings.amqp_url.password or ""), ":xxxxx"
            ),
        )
        self._connection = await connect_robust(settings.amqp_url)

        logger.info("Creating AMQP channel")
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=10)

        logger.info(
            "Attaching AMQP exchange to channel", exchange=settings.amqp_exchange
        )
        self._exchange = await self._channel.declare_exchange(
            settings.amqp_exchange, ExchangeType.TOPIC
        )

        # We expect function_to_name to be unique for each callback
        assert all_unique(map(function_to_name, self._registry.keys()))
        if self._registry:
            assert settings.queue_name is not None

        # TODO: Create queues and binds in parallel?
        queues = {}
        for callback, routing_keys in self._registry.items():
            function_name = function_to_name(callback)
            log = logger.bind(function=function_name)

            queue_name = f"{settings.queue_name}_{function_name}"
            log.info("Declaring unique message queue", queue_name=queue_name)
            queue = await self._channel.declare_queue(queue_name, durable=True)
            queues[function_name] = queue

            log.info("Starting message listener")
            await queue.consume(partial(self.on_message, callback))  # type: ignore

            log.info("Binding routing keys")
            for routing_key in routing_keys:
                log.info("Binding routing-key", routing_key=routing_key)
                await queue.bind(self._exchange, routing_key=routing_key)
                routes_bound.labels(function_name).inc()

        # Setup periodic metrics
        async def periodic_metrics() -> None:
            loop = asyncio.get_running_loop()
            while True:
                last_periodic.set_to_current_time()
                last_loop_periodic.set(loop.time())
                for function_name, queue in queues.items():
                    backlog_count.labels(function_name).set(
                        queue.declaration_result.message_count
                    )
                await asyncio.sleep(1)

        self._periodic_task = asyncio.create_task(periodic_metrics())

    def run_forever(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        loop = asyncio.get_event_loop()
        # Setup everything
        loop.run_until_complete(self.start(*args, **kwargs))
        # Run forever listening to messages
        loop.run_forever()
        # Clean up everything
        loop.run_until_complete(self.stop())
        loop.close()

    async def publish_message(self, routing_key: str, payload: dict) -> None:
        if self._exchange is None:
            raise ValueError("Must call start() before publish message!")

        message = Message(body=json.dumps(payload).encode("utf-8"))
        await self._exchange.publish(
            message=message,
            routing_key=routing_key,
        )

    async def on_message(
        self, callback: CallbackType, message: IncomingMessage
    ) -> None:
        last_on_message.set_to_current_time()

        assert message.routing_key is not None
        routing_key = message.routing_key
        function_name = function_to_name(callback)
        log = logger.bind(function=function_name, routing_key=routing_key)

        log.debug("Received message")
        try:
            event_counter.labels(routing_key, function_name).inc()
            async with message.process(requeue=True):
                with exception_callback_counter.labels(
                    routing_key, function_name
                ).count_exceptions():
                    processing_calls.labels(routing_key, function_name).inc()
                    wrapped_callback = processing_inprogress.labels(
                        routing_key, function_name
                    ).track_inprogress()(callback)
                    wrapped_callback = processing_time.labels(
                        routing_key, function_name
                    ).time()(wrapped_callback)
                    await wrapped_callback(message)
        except Exception:
            log.exception("Exception during on_message()", routing_key=routing_key)
