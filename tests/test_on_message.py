# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the AMQPSystem.on_message handler."""
from typing import Any
from typing import Callable
from typing import Dict

import pytest
from aio_pika import IncomingMessage
from more_itertools import one


@pytest.mark.integrationtest
async def test_happy_path(amqp_test: Callable) -> None:
    """Test that messages can flow through our AMQP system."""
    params: Dict[str, Any] = {}

    async def callback(message: IncomingMessage) -> None:
        params["message"] = message

    await amqp_test(callback)
    assert list(params.keys()) == ["message"]
    assert isinstance(params["message"], IncomingMessage)


@pytest.mark.integrationtest
async def test_callback_retrying(amqp_test: Callable) -> None:
    """Test that messages are resend when an exception occur."""
    params: Dict[str, Any] = {"call_count": 0, "message_ids": set()}

    async def callback(message: IncomingMessage) -> None:
        params["message_ids"].add(message.message_id)
        params["call_count"] += 1
        if params["call_count"] < 5:
            raise ValueError()

    await amqp_test(callback)

    assert params["call_count"] == 5
    message_id = one(params["message_ids"])
    assert isinstance(message_id, str)
