# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import random
import string
from typing import Any
from typing import Dict

import pytest
from aio_pika import IncomingMessage

from ramqp.utils import pass_arguments


def random_string(length=30):
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(length)
    )


@pytest.fixture
def amqp_test(amqp_system_creator):
    async def make_amqp_test(callback):
        test_id = random_string()
        queue_name = f"test_{test_id}"
        routing_key = "test.routing.key"
        payload = {"value": test_id}
        event = asyncio.Event()

        async def callback_wrapper(*args, **kwargs) -> None:
            await callback(*args, **kwargs)
            event.set()

        amqp_system = amqp_system_creator()
        amqp_system.register(routing_key)(callback_wrapper)
        await amqp_system.start(queue_name=queue_name, amqp_exchange=test_id)
        await amqp_system.publish_message(routing_key, payload)
        await event.wait()
        await amqp_system.stop()

    return make_amqp_test


@pytest.mark.integrationtest
async def test_happy_path(amqp_test):
    params: Dict[str, Any] = {}

    async def callback(routing_key: str, message: IncomingMessage) -> None:
        assert type(routing_key) == str
        assert type(message) == IncomingMessage
        params["routing_key"] = routing_key
        params["message"] = message

    await amqp_test(callback)
    assert "routing_key" in params.keys()
    assert "message" in params.keys()


@pytest.mark.integrationtest
async def test_callback_retrying(amqp_test):
    params: Dict[str, Any] = {"call_count": 0, "message_ids": set()}

    @pass_arguments("routing_key", "message_id")
    async def callback(routing_key: str, message_id: str) -> None:
        assert type(routing_key) == str
        assert type(message_id) == str
        params["message_ids"].add(message_id)
        params["call_count"] += 1
        if params["call_count"] < 5:
            raise ValueError()

    await amqp_test(callback)

    assert params["call_count"] == 5
    assert len(params["message_ids"]) == 1
