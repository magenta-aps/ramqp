# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import random
import string
from typing import Any
from typing import Callable
from typing import Dict
from typing import List

import pytest
from aio_pika import IncomingMessage

from ramqp import AMQPSystem
from ramqp.utils import pass_arguments


def random_string(length: int = 30) -> str:
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(length)
    )


@pytest.fixture
def amqp_test(amqp_system_creator: Callable) -> Callable:
    async def make_amqp_test(callback: Callable) -> None:
        test_id = random_string()
        queue_name = f"test_{test_id}"
        routing_key = "test.routing.key"
        payload = {"value": test_id}
        event = asyncio.Event()

        async def callback_wrapper(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
            await callback(*args, **kwargs)
            event.set()

        amqp_system = amqp_system_creator()
        amqp_system.register(routing_key)(callback_wrapper)
        await amqp_system.start(queue_name=queue_name, amqp_exchange=test_id)
        await amqp_system.publish_message(routing_key, payload)
        await event.wait()
        await amqp_system.stop()

    return make_amqp_test


def test_run_forever(amqp_system: AMQPSystem) -> None:
    # Instead of starting, shutdown the event-loop
    async def start(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        loop.stop()

    # mypy says: Cannot assign to a method, we ignore it
    amqp_system.start = start  # type: ignore
    amqp_system.run_forever()


async def test_publish_before_start(amqp_system: AMQPSystem) -> None:
    with pytest.raises(ValueError):
        await amqp_system.publish_message("test.routing.key", {"key": "value"})


@pytest.mark.integrationtest
async def test_happy_path(amqp_test: Callable) -> None:
    """Test that messages can flow through our AMQP system."""
    params: Dict[str, Any] = {}

    async def callback(message: IncomingMessage) -> None:
        assert type(message) == IncomingMessage
        params["message"] = message

    await amqp_test(callback)
    assert ["message"] == list(params.keys())


@pytest.mark.integrationtest
async def test_callback_retrying(amqp_test: Callable) -> None:
    """Test that messages are resend when an exception occur."""
    params: Dict[str, Any] = {"call_count": 0, "message_ids": set()}

    @pass_arguments
    async def callback(message_id: str) -> None:
        assert type(message_id) == str
        params["message_ids"].add(message_id)
        params["call_count"] += 1
        if params["call_count"] < 5:
            raise ValueError()

    await amqp_test(callback)

    assert params["call_count"] == 5
    assert len(params["message_ids"]) == 1
