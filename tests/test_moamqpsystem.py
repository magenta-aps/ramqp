# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import asyncio
import random
import string
from datetime import datetime
from uuid import uuid4
from typing import Any
from typing import Callable
from typing import Dict
from typing import List

import pytest
from aio_pika import IncomingMessage
from pydantic import parse_obj_as

from ramqp.moqp import ServiceType
from ramqp.moqp import ObjectType
from ramqp.moqp import RequestType
from ramqp.moqp import PayloadType
from ramqp.moqp import MOAMQPSystem
from ramqp.utils import pass_arguments


def random_string(length: int = 30) -> str:
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(length)
    )


@pytest.fixture
def moamqp_test(amqp_system_creator: Callable) -> Callable:
    async def make_amqp_test(callback: Callable) -> None:
        test_id = random_string()
        queue_name = f"test_{test_id}"
        routing_key = [ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.CREATE]
        payload = parse_obj_as(
            PayloadType,
            {
                "uuid": uuid4(),
                "object_uuid": uuid4(),
                "time": datetime.now().isoformat(),
            }
        )
        event = asyncio.Event()

        async def callback_wrapper(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
            await callback(*args, **kwargs)
            event.set()

        inner_amqp_system = amqp_system_creator()
        amqp_system = MOAMQPSystem(inner_amqp_system)
        amqp_system.register(*routing_key)(callback_wrapper)
        await amqp_system.start(queue_name=queue_name, amqp_exchange=test_id)
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
