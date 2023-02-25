# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains the utilities."""
import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Hashable
from contextlib import asynccontextmanager
from functools import wraps
from inspect import signature
from typing import Any
from typing import DefaultDict
from typing import TypeVar

import anyio
import structlog
from aio_pika import IncomingMessage
from pydantic import parse_raw_as

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


@asynccontextmanager
async def sleep_on_error(delay: int = 30) -> AsyncGenerator[None, None]:
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

    Args:
        delay: The delay in seconds to sleep for.

    Raises:
        Exception: Whatever exception was thrown by the decorated function.

    Yields:
        None
    """
    try:
        yield
    except Exception:  # pylint: disable=broad-except
        await asyncio.sleep(delay)
        raise


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


def context_extractor(function: Callable) -> Callable:
    """AMQPSystem callback decorator to explode context as kwargs.

    Examples:
        Simple usage::

            context = {
                "settings": Settings(),
                "amqpsystem": ...,
                "...": ...
            }

            ...

            @router.register("my.routing.key")
            @context_extractor
            async def callback_function(
                amqpsystem: AMQPSystem, context: dict[str, Any], **kwargs: Any
            ) -> None:
                # assert context["amqpsystem"] == amqpsystem
                await amqpsystem.publish_message(...)

    Args:
        function: Message callback function.

    Returns:
        A decorated function which calls 'function' with a context applied as kwargs.
    """

    @wraps(function)
    async def extractor(context: dict[str, Any], **kwargs: Any) -> Any:
        return await function(**context, context=context, **kwargs)

    return extractor


def message2model(function: Callable) -> Callable:
    """AMQPSystem callback decorator to parse message bodies as models.

    Examples:
        Simple usage::

            class MyModel(BaseModel):
                payload: str

            @router.register("my.routing.key")
            @message2model
            async def callback_function(model: MyModel, **kwargs: Any) -> None:
                assert model.payload == "deadbeef"

            async def trigger():
                await amqpsystem.publish_message(
                    "my.routing.key", MyModel(payload="deadbeef").dict()
                )

    Args:
        function: Message callback function which takes a typed 'model' parameter.

    Raises:
        ValueError: If the model argument was not found on the decorated function.
        ValueError: If the model argument was not annotated on the decorated function.

    Returns:
        A decorated function which calls 'function' with a parsed model.
    """
    sig = signature(function)
    parameter = sig.parameters.get("model", None)
    if parameter is None:
        raise ValueError("model argument not found on message2model function")
    if parameter.annotation == parameter.empty:
        raise ValueError("model argument not annotated on message2model function")

    model = parameter.annotation

    @wraps(function)
    async def parsed_message_wrapper(message: IncomingMessage, **kwargs: Any) -> Any:
        payload = parse_raw_as(model, message.body)
        return await function(model=payload, message=message, **kwargs)

    return parsed_message_wrapper


def message2json(function: Callable) -> Callable:
    """AMQPSystem callback decorator to parse messages to json.

    Examples:
        Simple usage::

            @router.register("my.routing.key")
            @message2json
            async def callback_function(payload: dict[str, Any], **kwargs: Any) -> None:
                assert payload["hello"] == "world"

            async def trigger():
                await amqpsystem.publish_message(
                    "my.routing.key", {"hello": "world"}
                )

    Args:
        function: Message callback function.

    Returns:
        A decorated function which calls 'function' with a parsed json payload.
    """

    @wraps(function)
    async def parsed_message_wrapper(message: IncomingMessage, **kwargs: Any) -> Any:
        payload = json.loads(message.body)
        return await function(payload=payload, message=message, **kwargs)

    return parsed_message_wrapper
