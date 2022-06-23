# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
from typing import Awaitable
from typing import Callable

import structlog
from aio_pika import IncomingMessage

from .metrics import exception_decode_counter

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
