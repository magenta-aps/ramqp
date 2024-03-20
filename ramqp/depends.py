# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""This module implement FastAPI dependency injection for RAMQP."""
import asyncio
from asyncio import Task
from collections import defaultdict
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Hashable
from contextlib import asynccontextmanager
from contextlib import AsyncExitStack
from contextlib import suppress
from functools import cache
from functools import wraps
from typing import Annotated
from typing import Any
from typing import cast
from typing import DefaultDict
from typing import TypeVar

import anyio
from aio_pika import IncomingMessage
from fastapi import Depends
from fastapi import Request
from fastapi.dependencies.utils import get_dependant
from fastapi.dependencies.utils import solve_dependencies
from pydantic import parse_raw_as
from starlette.datastructures import State as StarletteState

T = TypeVar("T")


def dependency_injected(function: Callable) -> Callable:
    """AMQPSystem callback decorator to implement dependency injection.

    Examples:
        Simple usage::

            from .utils import dependency_injected
            from fastapi import Depends

            @dependency_injected
            @router.register("my.routing.key")
            def f(z=Depends(lambda: 3)):
                return z

    Note:
        The dependency injection system is implemented using FastAPIs dependency
        injection system, and thus detailed usage examples can be found on the FastAPI
        documentation.

    Args:
        function: Callback function with dependency injection parameters.

    Returns:
        A new wrapper function which fulfills the RAMQP message callback interface.
        The wrapper analyses the decorated function and resolves the dependency
        injection before calling the decorated function.
    """

    @wraps(function)
    async def wrapper(message: IncomingMessage, context: Context) -> Any:
        """Wrapper function fulfilling the RAMQP message callback interface.

        This wrapper implements the dependency injection, ensuring that the wrapped
        function is called with the dependencies it requested.

        Args:
            message: The AMQP message to process.
            context: The application context.

        Raises:
            ValueError: If the dependencies could not be resolved.

        Returns:
            What the wrapped function returns.
        """
        async with AsyncExitStack() as stack:
            request = Request(
                {
                    "type": "http",
                    "headers": [],
                    "query_string": "",
                    "state": {
                        "context": context,
                        "message": message,
                        "callback": function,
                    },
                }
            )
            dependant = get_dependant(path="", call=function)

            values, errors, *_ = await solve_dependencies(
                request=request,
                dependant=dependant,
                async_exit_stack=stack,
            )
            if errors:
                # TODO: Utilize Python 3.11 ExceptionGroup?
                raise ValueError(errors)

            return await function(**values)

    return wrapper


def get_state(request: Request) -> StarletteState:
    """Extract the request state from the request.

    Args:
        request: HTTP Request object.

    Returns:
        The request state contained within the request.
    """
    return request.state


State = Annotated[StarletteState, Depends(get_state)]


def get_context(state: State) -> Any:
    """Extract the application context from the request state.

    Args:
        state: The request state from within the request.

    Returns:
        The application context contained within the request state.
    """
    return state.context


Context = Annotated[Any, Depends(get_context)]


@cache
def from_context(field: str) -> Callable[..., Any]:
    """Construct a Callable which extracts 'field' from the application context.

    Args:
        field: The field to extract.

    Returns:
        A callable which extracts 'field' from the application context.
    """

    def inner(context: Context) -> Any:
        return context[field]

    return inner


def get_message(state: State) -> IncomingMessage:
    """Extract the AMQP message from the request state.

    Args:
        state: The request state from within the request.

    Returns:
        The AMQP message for this request.
    """
    return state.message


Message = Annotated[IncomingMessage, Depends(get_message)]


def get_callback(state: State) -> Callable:
    """Extract the callback function from the request state.

    Args:
        state: The request state from within the request.

    Returns:
        The callback function for this request.
    """
    return cast(Callable, state.callback)


Callback = Annotated[Callable, Depends(get_callback)]


def get_routing_key(message: Message) -> str:
    """Extract the AMQP message routing key.

    Args:
        message: The AMQP message to extract the payload from.

    Returns:
        The AMQP message routing key for this request.
    """
    assert message.routing_key is not None
    return cast(str, message.routing_key)


RoutingKey = Annotated[str, Depends(get_routing_key)]


def get_payload_bytes(message: Message) -> bytes:
    """Extract the AMQP message payload.

    Args:
        message: The AMQP message to extract the payload from.

    Returns:
        The binary payload.
    """
    return cast(bytes, message.body)


PayloadBytes = Annotated[bytes, Depends(get_payload_bytes)]


def get_payload_as_type(type_: type[T]) -> Callable[..., T]:
    """Construct a Callable which parses the message payload to a pydantic type.

    Args:
        type_: The type to parse the payload into.

    Returns:
        A callable which parses the message payload into 'model'.
    """

    def inner(payload: PayloadBytes) -> T:
        return parse_raw_as(type_, payload)

    return inner


H = TypeVar("H", bound=Hashable)


def handle_exclusively(key: Callable[..., H]) -> Callable:
    """Avoids race conditions in handlers by ensuring exclusivity based on key.

    This dependency is used to ensure that the "same" message cannot be handled by a
    message handler more than once at the same time. Here, the "same" message is
    defined by the key function given as argument. If a handler depends on multiple
    handle_exclusively, it needs to obtain the lock for each of them before proceeding.

    Examples:
        Simple usage::

                @dependency_injected
                async def handler(
                    _: Annotated[None, Depends(handle_exclusively(get_routing_key))],
                    msg: Annotated[Message, Depends(handle_exclusively(get_message))],
                ):
                    pass

    Args:
        key: A custom key function returning lock exclusivity key. Note that this
             function can specify Dependencies itself.

    Returns:
        A wrapper which yields the result of the key function.
    """
    locks: DefaultDict[H, anyio.Lock] = defaultdict(anyio.Lock)

    @wraps(key)
    async def wrapper(*args: Any, **kwargs: Any) -> AsyncGenerator[H, None]:
        key_value = key(*args, **kwargs)
        lock = locks[key_value]
        async with lock:
            try:
                yield key_value
            finally:
                # Garbage collect lock if no others are waiting to acquire. This MUST
                # be done before we release the lock, and works since the control is
                # asynchronous instead of truly concurrent.
                if not lock.statistics().tasks_waiting:
                    del locks[key_value]

    return wrapper


def handle_exclusively_decorator(key: Callable[..., Hashable]) -> Callable:
    """Avoids race conditions in handlers by ensuring exclusivity based on key.

    Basic wrapper for handle_exclusively, allowing it to work as a function decorator.

    Examples:
        Simple usage::

            @handle_exclusively_decorator(key=lambda x, y, z: x, y)
            async def f(x, y, z):
                pass

            await f(x=1, y=2, z=8)
            await f(x=3, y=4, z=8)  # accepted

    Args:
        key: A custom key function returning the arguments considered when determining
            exclusivity.

    Returns:
        Decorator to be applied to callback functions.
    """

    exclusive_context_manager = asynccontextmanager(handle_exclusively(key=key))

    def wrapper(coro: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(coro)
        async def wrapped(*args: Any, **kwargs: Any) -> T:
            async with exclusive_context_manager(*args, **kwargs):
                return await coro(*args, **kwargs)

        return wrapped

    return wrapper


def rate_limit(
    delay: int = 30,
) -> Callable[[Message, Callback], AsyncGenerator[None, None]]:
    """Rate-limit processing of the same message by `delay` seconds.

    Rate-limiting is applied per-handler, so different message callback handlers can
    process the same message simultaneously, without any limits.

    Args:
        delay: Number of seconds to wait between processing the same message.

    Returns: A dependency-injectable rate-limiter.
    """
    tasks: dict[tuple[str, int], Task] = {}

    async def inner(message: Message, callback: Callback) -> AsyncGenerator[None, None]:
        key = (message.message_id, id(callback))

        with suppress(KeyError):
            task = tasks[key]
            await task

        async def rate_limiter() -> None:
            await asyncio.sleep(delay)
            tasks.pop(key)

        if key not in tasks:
            task = asyncio.create_task(rate_limiter())
            tasks[key] = task

        yield

    return inner


RateLimit = Annotated[None, Depends(rate_limit())]
