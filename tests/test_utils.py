# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Test helper utilities from utils.py."""
from typing import Any

import pytest

from ramqp.utils import message2json
from tests.amqp_helpers import payload2incoming


async def callback(**kwargs: Any) -> dict[str, Any]:
    """Dummy AMQP callback function.

    Args:
        kwargs: Returned as-is.

    Returns:
        kwargs as provided.
    """
    return kwargs


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
