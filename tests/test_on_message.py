# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import random
import string
from typing import Any
from typing import Dict

import pytest


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

        async def callback_wrapper(routing_key: str, payload: dict) -> None:
            await callback(routing_key, payload)
            event.set()

        amqp_system = amqp_system_creator(queue_name=queue_name, amqp_exchange=test_id)
        amqp_system.register(routing_key)(callback_wrapper)
        await amqp_system.start()
        await amqp_system.publish_message(routing_key, payload)
        await event.wait()
        await amqp_system.stop()

    return make_amqp_test


@pytest.mark.integrationtest
async def test_happy_path(amqp_test):
    params: Dict[str, Any] = {}

    async def callback(routing_key: str, payload: dict) -> None:
        params["routing_key"] = routing_key
        params["payload"] = payload

    await amqp_test(callback)
    assert "routing_key" in params.keys()
    assert "payload" in params.keys()


@pytest.mark.integrationtest
async def test_callback_retrying(amqp_test):
    params: Dict[str, Any] = {"call_count": 0}

    async def callback(routing_key: str, payload: dict) -> None:
        params["call_count"] += 1
        if params["call_count"] < 5:
            raise ValueError()

    await amqp_test(callback)
    assert params == {
        "call_count": 5,
    }
