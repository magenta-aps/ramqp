# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any
from typing import AsyncGenerator
from typing import Awaitable
from typing import Callable
from typing import DefaultDict
from typing import Hashable
from typing import TypeVar

import anyio
import structlog

logger = structlog.get_logger()


CallbackType = Callable[..., Awaitable]
T = TypeVar("T")


def function_to_name(function: Callable) -> str:
    """Get a uniquely qualified name for a given function."""
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
        key: A custom key function which should return the arguments considered when
         determining exclusivity.

    Returns: Wrapper.
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


@asynccontextmanager
async def sleep_on_error(delay: int = 30) -> AsyncGenerator:
    """Async context manager that delays returning on errors.

    This is used to prevent race-conditions on writes to MO/LoRa, when the upload times
    out initially but is completed by the backend afterwards. The sleep ensures that
    the AMQP message is not retried immediately, causing the handler to act on
    information which could become stale by the queued write. This happens because the
    backend does not implement fairness of requests, such that read operations can
    return soon-to-be stale data while a write operation is queued on another thread.

    Specifically, duplicate objects would be created when a write operation failed to
    complete within the timeout (but would be completed later), and the handler, during
    retry, read an outdated list of existing objects, and thus dispatched another
    (duplicate) write operation.

    See: https://redmine.magenta-aps.dk/issues/51949#note-23.
    """
    try:
        yield
    except Exception:  # pylint: disable=broad-except
        await asyncio.sleep(delay)
        raise


class RejectMessage(Exception):
    """Raise to reject a message, turning it into a dead letter."""


class RequeueMessage(Exception):
    """Raise to requeue a message for later redelivery."""
