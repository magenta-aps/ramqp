# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the AMQPSystem.on_message handler."""
import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from aio_pika import IncomingMessage
from more_itertools import one

from ramqp.depends import Message
from ramqp.utils import RejectMessage
from ramqp.utils import RequeueMessage


@pytest.mark.integrationtest
async def test_happy_path(amqp_test: Callable) -> None:
    """Test that messages can flow through our AMQP system."""
    params: dict[str, Any] = {}

    async def callback(message: Message) -> None:
        params["message"] = message

    await amqp_test(callback)
    assert list(params.keys()) == ["message"]
    assert isinstance(params["message"], IncomingMessage)


@pytest.mark.integrationtest
@pytest.mark.parametrize(
    "exception,count",
    [
        (ValueError, 5),
        (RequeueMessage, 5),
        (RejectMessage, 1),
    ],
)
async def test_callback_retrying_and_rejection(
    amqp_test: Callable, exception: type[Exception], count: int
) -> None:
    """Test that messages are resend when an exception occur."""
    params: dict[str, Any] = {"call_count": 0, "message_ids": set()}

    async def callback(message: Message) -> None:
        params["message_ids"].add(message.message_id)
        params["call_count"] += 1
        if params["call_count"] < 5:
            raise exception()

    # Process at most 5 messages, if less we time out, which is fine
    try:
        await amqp_test(callback, num_messages=5)
    except asyncio.exceptions.TimeoutError:
        pass

    assert params["call_count"] == count
    message_id = one(params["message_ids"])
    assert isinstance(message_id, str)
