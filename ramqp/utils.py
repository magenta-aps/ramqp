# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TypeVar

import structlog

logger = structlog.get_logger()


CallbackType = Callable[..., Awaitable]
T = TypeVar("T")


def function_to_name(function: Callable) -> str:
    """Get a uniquely qualified name for a given function.

    Args:
        function: The function to extract the name of.

    Returns:
        The uniquely qualified name of the function.
    """
    return function.__name__


class RejectMessage(Exception):
    """Raise to reject a message, turning it into a dead letter.

    Examples:
        Simple usage::

            @router.register("my.routing.key")
            async def callback_function(...) -> None:
                if unrecoverable_condition:
                    raise RejectMessage("Due to X, the message will never be accepted.")
    """


class RequeueMessage(Exception):
    """Raise to requeue a message for later redelivery.

    Examples:
        Simple usage::

            @router.register("my.routing.key")
            async def callback_function(...) -> None:
                if temporary_condition:
                    raise RejectMessage("Due to X, the message should be retried later")
    """


class AcknowledgeMessage(Exception):
    """Raise to acknowledge a message removing it from the queue.

    NOTE: There is often no need for this exception; simply returning from the
    callback handler achieves the same thing. This functionality is mostly
    useful to acknowledge a message from deep within application code.

    Examples:
        Simple usage::

            @router.register("my.routing.key")
            async def callback_function(...) -> None:
                if expected_condition:
                    raise AcknowledgeMessage("Due to X, the message should be acked")
    """
