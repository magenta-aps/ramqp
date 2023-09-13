# SPDX-FileCopyrightText: 2023-2023 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the AMQPSystem.publish_message method."""
import asyncio
from collections.abc import Callable

import pytest
from pydantic import AmqpDsn
from pydantic import parse_obj_as

from ramqp import AMQPSystem
from ramqp.config import AMQPConnectionSettings
from tests.common import random_string


@pytest.mark.integrationtest
async def test_publish_exchange(amqp_test: Callable) -> None:
    """Test that messages can be published to a specific exchange."""
    test_id = random_string()
    exchange_1 = f"{test_id}_exchange_1"
    exchange_2 = f"{test_id}_exchange_2"
    routing_key = "test.routing.key"

    amqp_system_1 = AMQPSystem(
        settings=AMQPConnectionSettings(
            url=parse_obj_as(AmqpDsn, "amqp://guest:guest@rabbitmq:5672"),
            queue_prefix=exchange_1,
            exchange=exchange_1,
        ),
    )
    amqp_system_2 = AMQPSystem(
        settings=AMQPConnectionSettings(
            url=parse_obj_as(AmqpDsn, "amqp://guest:guest@rabbitmq:5672"),
            queue_prefix=exchange_2,
            exchange=exchange_2,
        ),
    )

    calls: dict[int, bool] = {}

    @amqp_system_1.router.register(routing_key)
    async def callback_1() -> None:
        calls[1] = True

    @amqp_system_2.router.register(routing_key)
    async def callback_2() -> None:
        calls[2] = True

    async with amqp_system_1, amqp_system_2:
        await amqp_system_1.publish_message(
            routing_key=routing_key,
            payload="hello",
            exchange=exchange_2,
        )
        await asyncio.sleep(1)

    assert calls.get(1) is None
    assert calls.get(2) is True
