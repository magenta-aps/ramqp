# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
import json
from inspect import signature
from typing import Awaitable
from typing import Callable
from typing import cast

import structlog
from aio_pika import IncomingMessage

from .metrics import exception_decode_counter
from .metrics import exception_parse_counter


logger = structlog.get_logger()


CallbackType = Callable[[IncomingMessage], Awaitable]


def function_to_name(function: Callable) -> str:
    """Get a uniquely qualified name for a given function."""
    return function.__name__


def decode_body(message: IncomingMessage) -> str:
    """Decode the message body into a string.

    Args:
        message: The aio_pika message.

    Returns:
        The decoded body as a string.
    """
    routing_key = message.routing_key
    with exception_decode_counter.labels(routing_key).count_exceptions():
        decoded = message.body.decode("utf-8")
    logger.debug("Decoded body", routing_key=routing_key, decoded=decoded)
    return decoded


def parse_payload(message: IncomingMessage) -> dict:
    """Parse the message body as json.

    Args:
        message: The aio_pika message.

    Returns:
        JSON parsed body.
    """
    routing_key = message.routing_key
    with exception_parse_counter.labels(routing_key).count_exceptions():
        decoded = decode_body(message)
        payload = json.loads(decoded)
    logger.debug("Parsed body", routing_key=routing_key, payload=payload)
    return cast(dict, payload)


# Map af parameters supported by pass_arguments
parameter_map = {
    "body": decode_body,
    "message": lambda message: message,
    "message_id": lambda message: message.message_id,
    "payload": parse_payload,
    "routing_key": lambda message: message.routing_key,
}


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
