# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
from collections import defaultdict
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Hashable
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any
from typing import DefaultDict
from typing import TypeVar

import anyio
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


def handle_exclusively(key: Callable[..., Hashable]) -> Callable:
    """Avoids race conditions in handlers by ensuring exclusivity for arguments.

    This decorator is used to ensure that the "same" message cannot be handled by a
    message handler more than once at the same time. Here, the "same" message refers
    to a message with arguments which would make the handling functionally identical,
    e.g. a MO message with identical `uuid` and `object_uuid`.

    Examples:
        Simple usage::

            @handle_exclusively(key=lambda x, y, z: x, y)
            async def f(x, y, z):
                pass

            await f(x=1, y=2, z=8)
            await f(x=3, y=4, z=8)  # accepted
            await f(x=1, y=2, z=9)  # blocked

    Args:
        key: A custom key function returning the arguments considered when determining
             exclusivity.

    Returns:
        Decorator to be applied to callback functions.
    """
    locks: DefaultDict[Hashable, anyio.Lock] = defaultdict(anyio.Lock)

    @asynccontextmanager
    async def named_lock(key: Hashable) -> AsyncGenerator[None, None]:
        lock = locks[key]
        async with lock:
            try:
                yield
            finally:
                # Garbage collect lock if no others are waiting to acquire.
                # This MUST be done before we release the lock.
                if not lock.statistics().tasks_waiting:
                    del locks[key]

    def wrapper(coro: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(coro)
        async def wrapped(*args: Any, **kwargs: Any) -> T:
            async with named_lock(key=key(*args, **kwargs)):
                return await coro(*args, **kwargs)

        return wrapped

    return wrapper


class RejectMessage(Exception):
    """Raise to reject a message, turning it into a dead letter.

    Examples:
        Simple usage::

            @router.register("my.routing.key")
            async def callback_function(**kwargs: Any) -> None:
                if unrecoverable_condition:
                    raise RejectMessage("Due to X, the message will never be accepted.")
    """


class RequeueMessage(Exception):
    """Raise to requeue a message for later redelivery.

    Examples:
        Simple usage::

            @router.register("my.routing.key")
            async def callback_function(**kwargs: Any) -> None:
                if temporary_condition:
                    raise RejectMessage("Due to X, the message should be retried later")
    """
