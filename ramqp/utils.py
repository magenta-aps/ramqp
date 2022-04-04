# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import json
from asyncio import TimerHandle
from functools import wraps
from inspect import signature
from typing import Awaitable
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional

import structlog
from aio_pika import IncomingMessage
from prometheus_client import Counter


logger = structlog.get_logger()


CallbackType = Callable[[IncomingMessage], Awaitable]


exception_parse_counter = Counter(
    "amqp_exceptions_parse",
    "Exception counter",
    ["routing_key"],
)


class Batch:
    def __init__(
        self,
        callback,
        batch_refresh_time: int = 5,
        batch_max_time: int = 60,
        batch_max_size: int = 10,
    ):
        self.callback = callback

        self.batch_refresh_time = batch_refresh_time
        self.batch_max_time = batch_max_time
        self.batch_max_size = batch_max_size

        self.refresh_dispatch: Optional[TimerHandle] = None
        self.max_time_dispatch: Optional[TimerHandle] = None
        self.payloads: List[dict] = []

    def append(self, payload: dict) -> None:
        loop = asyncio.get_running_loop()

        if self.refresh_dispatch:
            self.refresh_dispatch.cancel()
        self.refresh_dispatch = loop.call_later(
            self.batch_refresh_time, self.dispatch_refresh_time
        )

        if self.max_time_dispatch is None:
            self.max_time_dispatched = loop.call_at(
                loop.time() + self.batch_max_time, self.dispatch_max_time
            )

        self.payloads.append(payload)
        if len(self.payloads) == self.batch_max_size:
            self.dispatch_max_length()

    def clear(self) -> None:
        if self.refresh_dispatch:
            self.refresh_dispatch.cancel()
        self.refresh_dispatch = None

        if self.max_time_dispatch:
            self.max_time_dispatch.cancel()
        self.max_time_dispatch = None

        self.payloads = []

    # Dispatch functions
    def dispatch_refresh_time(self):
        logger.debug("Dispatched by refresh time timer")
        return self.dispatch()

    def dispatch_max_time(self):
        logger.debug("Dispatched by max time timer")
        return self.dispatch()

    def dispatch_max_length(self):
        logger.debug("Dispatched by max length")
        return self.dispatch()

    def dispatch(self):
        payloads = self.payloads
        self.clear()
        return self.callback(payloads)


# XXX: Do not use, this needs to handle ACKs better
def __bulk_messages(*args, **kwargs):
    """Bulk messages before calling wrapped function."""

    def decorator(function):
        batches: Dict[str, Batch] = {}

        @wraps(function)
        async def wrapper(routing_key: str, payload: dict) -> None:
            batch = batches.setdefault(routing_key, Batch(function, *args, **kwargs))
            batch.append(payload)

        return wrapper

    return decorator


def _decode_body(message: IncomingMessage) -> str:
    """Decode the message body into a string.

    Args:
        message: The aio_pika message.

    Returns:
        The decoded body as a string.
    """
    routing_key = message.routing_key
    with exception_parse_counter.labels(routing_key).count_exceptions():
        decoded = message.body.decode("utf-8")
    logger.debug("Decoded body", routing_key=routing_key, decoded=decoded)
    return decoded


def _parse_payload(message: IncomingMessage) -> dict:
    """Parse the message body as json.

    Args:
        message: The aio_pika message.

    Returns:
        JSON parsed body.
    """
    routing_key = message.routing_key
    with exception_parse_counter.labels(routing_key).count_exceptions():
        decoded = _decode_body(message)
        payload = json.loads(decoded)
    logger.debug("Parsed body", routing_key=routing_key, payload=payload)
    return cast(dict, payload)


# Map af parameters supported by pass_arguments
parameter_map = {
    "message": lambda message: message,
    "routing_key": lambda message: message.routing_key,
    "message_id": lambda message: message.message_id,
    "payload": lambda message: _parse_payload(message),
    "body": lambda message: _decode_body(message),
}


# TODO: Should we integration this directly with amqpsystem?
def pass_arguments(function: Callable) -> CallbackType:
    """Decorator which automatically transform parameters as desired.

    The decorator inspects the parameters taken by the provided function and
    transforms the AMQP message to fit those parameters, then calls the function
    with the requested parameters.

    Examples:
        We may wish to just receive the callback without any arguments:
        ```
        @amqp_system.register("my.routing.key")
        @pass_arguments
        async def callback() -> None:
            pass
        ```
        Or we may wish to only receive the `routing_key`:
        ```
        @amqp_system.register("my.routing.key")
        @amqp_system.register("your.routing.key")
        @pass_arguments
        async def callback(routing_key: str) -> None:
            pass
        ```
        Or the `message_id` and the `payload`:
        ```
        @amqp_system.register("my.routing.key")
        @pass_arguments
        async def callback(message_id: str, payload: dict) -> None:
            pass
        ```
        Alternatively without the decorator we receive the `routing_key` and `message`:
        ```
        @amqp_system.register("my.routing.key")
        async def callback(routing_key: str, message: IncomingMessage) -> None:
            pass
        ```

    Note:
        The parameters that can be mapped to are found in the `parameter_map`.

    Args:
        function: The callable to match parameters to.

    Returns:
        Callable with the signature expected by AMQPSystem.register
    """
    sig = signature(function)
    parameters = list(sig.parameters.keys())

    # Verify that all parameters of the function can be mapped
    # Having this outside the decorator function allows earlier errors
    for parameter in parameters:
        assert parameter in parameter_map, "Unknown parameter in pass_argument"

    # Function mapping parameters from CallbackType to function
    async def message_receiver(message: IncomingMessage) -> None:
        """Maps parameters via the parameter_map and calls the decorated function.

        Note:
            This function fulfills the CallbackType signature, thus the decorator
            maps from arbitrary function signatures to the CallbackType signature.

        Args:
            message: The aio_pika message.

        Returns:
            None
        """
        kwargs = {}
        for parameter in parameters:
            kwargs[parameter] = parameter_map[parameter](message)
        await function(**kwargs)

    return message_receiver
