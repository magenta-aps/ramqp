# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
"""This module implement FastAPI dependency injection for RAMQP."""
from collections.abc import Callable
from contextlib import AsyncExitStack
from functools import wraps
from typing import Annotated
from typing import Any
from typing import cast
from typing import TypeVar

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
                    "fastapi_astack": stack,
                    "type": "http",
                    "headers": [],
                    "query_string": "",
                    "state": {"context": context, "message": message},
                }
            )
            dependant = get_dependant(path="", call=function)

            values, errors, *_ = await solve_dependencies(
                request=request, dependant=dependant
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
