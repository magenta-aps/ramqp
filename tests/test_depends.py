# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Test helper utilities from utils.py."""
import asyncio
from asyncio import Event
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterator
from typing import Annotated
from typing import Any

import pytest
from fastapi import Depends
from pydantic import BaseModel
from pytest import MonkeyPatch

from ramqp import AMQPSystem
from ramqp.depends import Context
from ramqp.depends import dependency_injected
from ramqp.depends import from_context
from ramqp.depends import get_payload_as_type
from ramqp.depends import get_payload_bytes
from ramqp.depends import Message
from ramqp.depends import sleep_on_error
from tests.amqp_helpers import payload2incoming


# pylint: disable=too-few-public-methods
class HelloWorldModel(BaseModel):
    """Dummy model."""

    hello: str


async def test_depends_errors() -> None:
    """Test that invalid dependencies results in an error."""

    @dependency_injected
    async def function(_: Any) -> None:
        return None

    message = payload2incoming({"hello": "world"})
    with pytest.raises(ValueError, match="MissingError"):
        await function(message=message, context={})


async def test_from_context() -> None:
    """Test that from_context works as expected."""
    context = {"a": 1, "b": 2}

    result = from_context("a")(context)
    assert result == 1

    result = from_context("b")(context)
    assert result == 2

    with pytest.raises(KeyError) as exc_info:
        await from_context("c")(context)
    assert "'c'" in str(exc_info.value)


async def test_payload_as_x() -> None:
    """Test get_payload_bytes/get_payload_as_type works as expected."""
    amqp_message_payload = {"hello": "world"}
    amqp_message = payload2incoming(amqp_message_payload)

    payload = get_payload_bytes(amqp_message)
    assert payload == b'{"hello": "world"}'

    json = get_payload_as_type(dict)(payload)
    assert json == amqp_message_payload

    model = get_payload_as_type(HelloWorldModel)(payload)
    assert isinstance(model, HelloWorldModel)
    assert model.hello == "world"


async def test_dependency_injected_awaitable() -> None:
    """Test dependency_injected works as expected."""

    called = {
        "func": 0,
        "awaitable": 0,
        "generator_before": 0,
        "generator_after": 0,
        "agenerator_before": 0,
        "agenerator_after": 0,
    }

    def func() -> int:
        called["func"] = 1
        return 1

    async def awaitable() -> int:
        called["awaitable"] = 1
        return 2

    def generator() -> Iterator[int]:
        called["generator_before"] = 1
        yield 3
        called["generator_after"] = 1

    async def agenerator() -> AsyncIterator[int]:
        called["agenerator_before"] = 1
        yield 4
        called["agenerator_after"] = 1

    # pylint: disable=invalid-name
    @dependency_injected
    async def function(
        a: Annotated[int, Depends(func)],
        b: Annotated[int, Depends(awaitable)],
        c: Annotated[int, Depends(generator)],
        d: Annotated[int, Depends(agenerator)],
    ) -> dict[str, Any]:
        return {
            "a": a,
            "b": b,
            "c": c,
            "d": d,
            "called": dict(called.items()),
        }

    message = payload2incoming({"hello": "world"})
    args = await function(message=message, context={})
    assert args["a"] == 1
    assert args["b"] == 2
    assert args["c"] == 3
    assert args["d"] == 4
    assert args["called"] == {
        "func": 1,
        "awaitable": 1,
        "generator_before": 1,
        "generator_after": 0,
        "agenerator_before": 1,
        "agenerator_after": 0,
    }


async def test_dependency_injected_message_and_context() -> None:
    """Test dependency_injected works as expected."""

    PayloadDict = Annotated[dict, Depends(get_payload_as_type(dict))]
    HelloWorld = Annotated[
        HelloWorldModel, Depends(get_payload_as_type(HelloWorldModel))
    ]

    # pylint: disable=invalid-name
    @dependency_injected
    async def function(
        message: Message,
        context: Context,
        payload: PayloadDict,
        model: HelloWorld,
        value: Annotated[str, Depends(from_context("key"))],
    ) -> dict[str, Any]:
        return {
            "message": message,
            "context": context,
            "key": value,
            "payload": payload,
            "model": model,
        }

    amqp_message_payload = {"hello": "world"}
    amqp_message = payload2incoming(amqp_message_payload)
    amqp_context = {"key": "value"}
    # pylint: disable=no-value-for-parameter
    args = await function(message=amqp_message, context=amqp_context)
    assert args["message"] == amqp_message
    assert args["context"] == amqp_context
    assert args["key"] == amqp_context["key"]
    assert args["payload"] == amqp_message_payload
    assert isinstance(args["model"], HelloWorldModel)
    assert args["model"].dict() == amqp_message_payload


@pytest.mark.integrationtest
async def test_context_amqp(amqp_test: Callable) -> None:
    """Test that AMQP handlers are passed the context object."""
    context = {"foo": "bar"}
    call_args = {}

    async def callback(
        context: Context,
    ) -> None:
        call_args["context"] = context

    def post_start(amqp_system: AMQPSystem) -> None:
        amqp_system.context = context

    await amqp_test(callback, post_start=post_start)
    assert call_args["context"] is context


async def test_sleep_on_error(monkeypatch: MonkeyPatch) -> None:
    """Test that the decorator sleeps if an error is thrown."""

    @dependency_injected
    async def function(_: Annotated[None, Depends(sleep_on_error(delay=10))]) -> None:
        raise ValueError("no thanks")

    sleep_event = Event()

    async def fake_sleep(*_: Any, **__: Any) -> None:
        sleep_event.set()

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(ValueError, match="no thanks"):
        message = payload2incoming({"hello": "world"})
        await function(message=message, context={})

    assert sleep_event.is_set()


async def test_dont_sleep_on_success(monkeypatch: MonkeyPatch) -> None:
    """Test that the decorator does not sleep if there are no errors."""

    @dependency_injected
    async def function(_: Annotated[None, Depends(sleep_on_error(delay=10))]) -> None:
        return None

    async def fake_sleep(*_: Any, **__: Any) -> None:
        assert False, "we should not sleep on success"

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    message = payload2incoming({"hello": "world"})
    await function(message=message, context={})
