# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import json
from functools import partial
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Set

import structlog
from aio_pika import connect_robust
from aio_pika import ExchangeType
from aio_pika import IncomingMessage
from aio_pika import Message
from more_itertools import all_unique
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from pydantic import AmqpDsn
from pydantic import BaseSettings
from pydantic import parse_obj_as

from .utils import CallbackType


logger = structlog.get_logger()


event_counter = Counter("amqp_events", "AMQP Events", ["routing_key", "function_name"])
exception_callback_counter = Counter(
    "amqp_exceptions_callback",
    "Exception counter",
    ["routing_key", "function"],
)
processing_time = Histogram(
    "amqp_processing_seconds",
    "Time spent running callback",
    ["routing_key", "function"],
)
processing_inprogress = Gauge(
    "amqp_inprogress",
    "Number of callbacks currently running",
    ["routing_key", "function"],
)
processing_calls = Counter(
    "amqp_calls",
    "Number of callbacks made",
    ["routing_key", "function"],
)
last_on_message = Gauge("amqp_last_on_message", "Timestamp of the last on_message call")
last_periodic = Gauge("amqp_last_periodic", "Timestamp of the last periodic call")
last_loop_periodic = Gauge(
    "amqp_last_loop_periodic", "Timestamp (monotonic) of the last periodic call"
)
last_heartbeat = Gauge(
    "amqp_last_heartbeat", "Timestamp (monotonic) of the last connection heartbeat"
)
backlog_count = Gauge(
    "amqp_backlog",
    "Number of messages waiting for processing in the backlog",
    ["function"],
)
routes_bound = Counter(
    "amqp_routes_bound", "Number of routing-keys bound to the queue", ["function"]
)
callbacks_registered = Counter(
    "amqp_callbacks_registered", "Number of callbacks registered", ["routing_key"]
)


class InvalidRegisterCallException(Exception):
    pass


def function_to_name(function: Callable) -> str:
    """Get a uniquely qualified name for a given function."""
    return function.__name__


class ConnectionSettings(BaseSettings):
    queue_name: Optional[str]
    amqp_url: AmqpDsn = parse_obj_as(AmqpDsn, "amqp://guest:guest@localhost:5672")
    amqp_exchange: str = "os2mo"


class AMQPSystem:
    def __init__(self, *args, **kwargs):
        self._started: bool = False
        self._registry: Dict[CallbackType, Set[str]] = {}

        self._connection = None
        self._channel = None
        self._exchange = None

        self._periodic_task = None

    def has_started(self) -> bool:
        return self._started

    def register(self, routing_key: str) -> Callable[[CallbackType], CallbackType]:
        assert routing_key != ""

        def decorator(function: CallbackType) -> CallbackType:
            function_name = function_to_name(function)

            log = logger.bind(routing_key=routing_key, function=function_name)
            log.info("Register called")

            if self._started:
                message = "Cannot register callback after run() has been called!"
                log.error(message)
                raise InvalidRegisterCallException(message)

            callbacks_registered.labels(routing_key).inc()
            self._registry.setdefault(function, set()).add(routing_key)
            return function

        return decorator

    async def stop(self) -> None:
        if self._periodic_task:
            self._periodic_task.cancel()
            self._periodic_task = None

        self._exchange = None

        if self._channel:
            await self._channel.close()
            self._channel = None

        if self._connection:
            await self._connection.close()
            self._connection = None

    async def start(self, *args, **kwargs) -> None:
        settings = ConnectionSettings(*args, **kwargs)

        self._started = True
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
                # last_heartbeat.set(self._connection.heartbeat_last)  # type: ignore
                for function_name, queue in queues.items():
                    backlog_count.labels(function_name).set(
                        queue.declaration_result.message_count
                    )
                await asyncio.sleep(1)

        self._periodic_task = asyncio.create_task(periodic_metrics())

    async def run_forever(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        # Setup everything
        loop.run_until_complete(self.start(*args, **kwargs))
        # Run forever listening to messages
        loop.run_forever()
        self.stop()
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
