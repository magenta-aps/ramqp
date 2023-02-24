# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the all prometheus metrics."""
import asyncio
from collections.abc import Callable
from collections.abc import Generator
from contextlib import contextmanager
from contextlib import ExitStack
from typing import Any

from aio_pika.abc import AbstractQueue
from aio_pika.abc import AbstractRobustChannel
from aio_pika.abc import AbstractRobustConnection
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

# --------------- #
# Various metrics #
# --------------- #

exception_decode_counter = Counter(
    "amqp_decode_exceptions",
    "Number of exceptions during body decoding",
    ["routing_key"],
)
exception_parse_counter = Counter(
    "amqp_parse_exceptions",
    "Number of exceptions during body parsing",
    ["routing_key"],
)
exception_mo_routing_counter = Counter(
    "amqp_mo_routing_exceptions",
    "Number of exceptions during mo routing key parsing",
    ["routing_key"],
)
routes_bound = Counter(
    "amqp_routes_bound", "Number of routing-keys bound to the queue", ["function"]
)
callbacks_registered = Counter(
    "amqp_callbacks_registered", "Number of callbacks registered", ["routing_key"]
)
event_counter = Counter("amqp_event", "Number of events made", ["event_key"])
event_last = Gauge("amqp_event_last", "Timestamp of last event", ["event_key"])


# --------------- #
# Receive metrics #
# --------------- #

receive_last_received = Gauge(
    "amqp_receive_last_received",
    "Timestamp of the last received message",
    ["routing_key", "function"],
)
receive_calls = Counter(
    "amqp_receive",
    "Number of calls made to receiver callbacks",
    ["routing_key", "function"],
)
receive_exceptions = Counter(
    "amqp_receive_exceptions",
    "Number of exceptions during receiver callbacks",
    ["routing_key", "function"],
)
receive_inprogress = Gauge(
    "amqp_receive_inprogress",
    "Number of receiver callbacks currently running",
    ["routing_key", "function"],
)
receive_time = Histogram(
    "amqp_receive_seconds",
    "Time spent running receiver callbacks",
    ["routing_key", "function"],
)


@contextmanager
def _handle_receive_metrics(
    routing_key: str, function_name: str
) -> Generator[None, None, None]:
    """Expose metrics about a callback.

    Args:
        routing_key: Routing key of the message.
        function_name: Name of the callback function.

    Yields:
        None
    """
    labels = (routing_key, function_name)

    receive_last_received.labels(*labels).set_to_current_time()
    receive_calls.labels(*labels).inc()
    with ExitStack() as stack:
        stack.enter_context(receive_exceptions.labels(*labels).count_exceptions())
        stack.enter_context(receive_inprogress.labels(*labels).track_inprogress())
        stack.enter_context(receive_time.labels(*labels).time())
        yield


# --------------- #
# Publish metrics #
# --------------- #

publish_last_published = Gauge(
    "amqp_publish_last_published",
    "Timestamp of the last published message",
    ["routing_key"],
)
publish_calls = Counter(
    "amqp_publish",
    "Number of messages published",
    ["routing_key"],
)
publish_exceptions = Counter(
    "amqp_publish_exceptions",
    "Number of exceptions during message publishing",
    ["routing_key"],
)
publish_inprogress = Gauge(
    "amqp_publish_inprogress",
    "Number of messages being published currently",
    ["routing_key"],
)
publish_time = Histogram(
    "amqp_publish_seconds",
    "Time spent publishing messages",
    ["routing_key"],
)


@contextmanager
def _handle_publish_metrics(routing_key: str) -> Generator[None, None, None]:
    """Expose metrics about a published message.

    Args:
        routing_key: Routing key of the message.

    Yields:
        None
    """
    labels = (routing_key,)

    publish_last_published.labels(*labels).set_to_current_time()
    publish_calls.labels(*labels).inc()
    with ExitStack() as stack:
        stack.enter_context(publish_exceptions.labels(*labels).count_exceptions())
        stack.enter_context(publish_inprogress.labels(*labels).track_inprogress())
        stack.enter_context(publish_time.labels(*labels).time())
        yield


# ---------------- #
# Periodic metrics #
# ---------------- #

last_periodic = Gauge("amqp_last_periodic", "Timestamp of the last periodic call")
last_loop_periodic = Gauge(
    "amqp_last_loop_periodic", "Timestamp (monotonic) of the last periodic call"
)
backlog_count = Gauge(
    "amqp_backlog",
    "Number of messages waiting for processing in the backlog",
    ["function"],
)


def _setup_periodic_metrics(queues: dict[str, AbstractQueue]) -> asyncio.Task:
    """Setup a periodic job to update non-eventful metrics.

    Args:
        queues: The aio_pika queues to export periodic metrics for.

    Returns:
        asynchronous task for cancellation
    """

    # Setup periodic metrics
    async def periodic_metrics() -> None:
        loop = asyncio.get_running_loop()
        while True:
            last_periodic.set_to_current_time()
            last_loop_periodic.set(loop.time())
            for function_name, queue in queues.items():
                backlog_count.labels(function_name).set(
                    queue.declaration_result.message_count or 0
                )
            await asyncio.sleep(1)

    return asyncio.create_task(periodic_metrics())


# ------------------ #
# Connection metrics #
# ------------------ #


def event_callback_generator(event_key: str) -> Callable:
    """Generate event-keyed event callback function.

    Args:
        event_key: The key to label all metrics with.

    Returns:
        Callback function to update event metrics.
    """

    def event_callback(*_1: Any, **_2: Any) -> None:  # pragma: no cover
        """Reconnect callback to update reconnect metrics.

        Args:
            _1: Unused arguments
            _2: Unused arguments
        """
        event_counter.labels(event_key).inc()
        event_last.labels(event_key).set_to_current_time()

    return event_callback


def _setup_connection_metrics(connection: AbstractRobustConnection) -> None:
    """Setup connection metrics collecting for reconnecting events.

    Args:
        connection: The connection install callbacks on.
    """
    connection.reconnect_callbacks.add(
        event_callback_generator("RobustConnection.reconnect")
    )
    connection.close_callbacks.add(event_callback_generator("Connection.close"))


def _setup_channel_metrics(channel: AbstractRobustChannel) -> None:
    """Setup channel metrics collecting for reconnecting events.

    Args:
        channel: The channel install callbacks on.
    """
    channel.reopen_callbacks.add(event_callback_generator("RobustChannel.reopen"))
    channel.close_callbacks.add(event_callback_generator("Channel.close"))
    channel.return_callbacks.add(event_callback_generator("Channel.return"))
