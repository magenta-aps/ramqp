# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from uuid import uuid4

import pytest
from pydantic import parse_obj_as

from .common import random_string
from ramqp.moqp import MOAMQPSystem
from ramqp.moqp import ObjectType
from ramqp.moqp import PayloadType
from ramqp.moqp import RequestType
from ramqp.moqp import ServiceType


@pytest.fixture
def moamqp_test(amqp_system_creator: Callable) -> Callable:
    async def make_amqp_test(callback: Callable) -> None:
        test_id = random_string()
        queue_name = f"test_{test_id}"
        routing_key = (ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.CREATE)
        payload = parse_obj_as(
            PayloadType,
            {
                "uuid": uuid4(),
                "object_uuid": uuid4(),
                "time": datetime.now().isoformat(),
            },
        )
        event = asyncio.Event()

        async def callback_wrapper(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
            await callback(*args, **kwargs)
            event.set()

        amqp_system = amqp_system_creator(MOAMQPSystem)
        amqp_system.register(*routing_key)(callback_wrapper)
        await amqp_system.start(
            queue_name=queue_name,  # type: ignore
            amqp_exchange=test_id,  # type: ignore
        )
        await amqp_system.publish_message(*routing_key, payload)
        await event.wait()
        await amqp_system.stop()

    return make_amqp_test


@pytest.mark.integrationtest
async def test_happy_path(moamqp_test: Callable) -> None:
    """Test that messages can flow through our AMQP system."""
    params: Dict[str, Any] = {}

    async def callback(routing_key: str, payload: PayloadType) -> None:
        assert type(routing_key) == str
        assert type(payload) == PayloadType
        params["routing_key"] = routing_key
        params["payload"] = payload

    await moamqp_test(callback)
    assert ["routing_key", "payload"] == list(params.keys())
    assert params["routing_key"] == "EMPLOYEE.ADDRESS.CREATE"


def test_run_forever(moamqp_system: MOAMQPSystem) -> None:
    # Instead of starting, shutdown the event-loop
    async def start(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        loop.stop()

    # mypy says: Cannot assign to a method, we ignore it
    moamqp_system.start = start  # type: ignore
    moamqp_system.run_forever()


async def test_cannot_publish_before_start(moamqp_system: MOAMQPSystem) -> None:
    routing_key = (ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.CREATE)
    payload = parse_obj_as(
        PayloadType,
        {
            "uuid": uuid4(),
            "object_uuid": uuid4(),
            "time": datetime.now().isoformat(),
        },
    )

    with pytest.raises(ValueError):
        await moamqp_system.publish_message(*routing_key, payload)


def test_has_started(moamqp_system: MOAMQPSystem) -> None:
    # Fake that the system has started
    assert moamqp_system.started is False
    moamqp_system._connection = {}  # type: ignore
    assert moamqp_system.started is True
