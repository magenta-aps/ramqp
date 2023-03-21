# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Test helper utilities from utils.py."""
from typing import Any

import pytest
from pydantic import BaseModel

from ramqp.utils import context_extractor
from ramqp.utils import message2json
from ramqp.utils import message2model
from tests.amqp_helpers import payload2incoming


async def callback(**kwargs: Any) -> dict[str, Any]:
    """Dummy AMQP callback function.

    Args:
        kwargs: Returned as-is.

    Returns:
        kwargs as provided.
    """
    return kwargs


async def test_context_extractor() -> None:
    """Test that context_extractor works as expected."""
    result = await callback(a=1, context={"b": 2, "c": 3}, d=4)
    assert result == {"a": 1, "context": {"b": 2, "c": 3}, "d": 4}

    extractor_callback = context_extractor(callback)
    result = await extractor_callback(a=1, context={"b": 2, "c": 3}, d=4)
    assert result == {"a": 1, "b": 2, "c": 3, "context": {"b": 2, "c": 3}, "d": 4}

    with pytest.raises(TypeError) as exc_info:
        await extractor_callback(a=1, context={"a": 1})
    assert "got multiple values for keyword argument 'a'" in str(exc_info.value)


async def test_message2json() -> None:
    """Test message2json works as expected."""
    message = payload2incoming({})

    result = await callback(message=message)
    assert result == {"message": message}

    message_callback = message2json(callback)
    result = await message_callback(message=message)
    assert result == {"payload": {}, "message": message}

    with pytest.raises(TypeError) as exc_info:
        await message_callback(payload=1, message=message)
    assert "got multiple values for keyword argument 'payload'" in str(exc_info.value)


def test_message2model_preconditions() -> None:
    """Test message2model throws the expected exceptions."""

    with pytest.raises(ValueError) as exc_info:
        message2model(callback)
    assert "model argument not found on message2model function" in str(exc_info.value)

    # pylint: disable=unused-argument
    async def model_callback(model) -> None:  # type: ignore
        pass

    with pytest.raises(ValueError) as exc_info:
        message2model(model_callback)
    assert "model argument not annotated on message2model function" in str(
        exc_info.value
    )

    async def model_annotated_callback(model: int, **kwargs: Any) -> dict[str, Any]:
        return {"model": model, **kwargs}

    message2model(model_annotated_callback)


async def test_message2model() -> None:
    """Test message2model works as expected."""

    # pylint: disable=too-few-public-methods
    class Model(BaseModel):
        """Dummy model."""

        hello: str

    async def model_callback(model: Model, **kwargs: Any) -> dict[str, Any]:
        return {"model": model, **kwargs}

    message = payload2incoming({"hello": "world"})

    with pytest.raises(TypeError) as exc_info:
        # pylint: disable=no-value-for-parameter
        await model_callback(message=message)  # type: ignore
    assert "missing 1 required positional argument: 'model'" in str(exc_info.value)

    model_callback = message2model(model_callback)

    result = await model_callback(message=message)
    assert result == {"model": Model(hello="world"), "message": message}

    with pytest.raises(TypeError) as exc_info:
        await model_callback(message=message, model=1)
    assert "got multiple values for keyword argument 'model'" in str(exc_info.value)
