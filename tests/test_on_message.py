# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import random
import string
import json

import pytest


def random_string(length=30):
    return ''.join(random.choice(
        string.ascii_uppercase + string.ascii_lowercase + string.digits
    ) for _ in range(length))


@pytest.mark.integrationtest
async def test_context_process(amqp_system_creator):
    test_id = random_string()
    queue_name = f"test_{test_id}"
    routing_key = "test.routing.key"
    payload = {"value": test_id}
    event = asyncio.Event()

    params = {}

    async def callback(routing_key: str, payload: dict) -> None:
        params["routing_key"] = routing_key
        params["payload"] = payload
        event.set()

    amqp_system = amqp_system_creator(queue_name=queue_name, amqp_exchange=test_id)
    amqp_system.register(routing_key)(callback)
    await amqp_system.start()
    await amqp_system.publish_message(routing_key, payload)
    await event.wait()
    await amqp_system.stop()
    assert params == {
        "routing_key": routing_key,
        "payload": payload,
    }
