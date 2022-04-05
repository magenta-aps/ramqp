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
from hypothesis import given
from hypothesis import strategies as st
from pydantic import parse_obj_as

from .common import random_string
from ramqp.moqp import from_routing_key
from ramqp.moqp import MOAMQPSystem
from ramqp.moqp import MOCallbackType
from ramqp.moqp import ObjectType
from ramqp.moqp import PayloadType
from ramqp.moqp import RequestType
from ramqp.moqp import ServiceType
from ramqp.moqp import to_routing_key


@pytest.fixture
def moamqp_test(amqp_system_creator: Callable) -> Callable:
    async def make_amqp_test(callback: MOCallbackType) -> None:
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
            await callback(*args, **kwargs)  # type: ignore
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

    async def callback(
        service_type: ServiceType,
        object_type: ObjectType,
        request_type: RequestType,
        payload: PayloadType,
    ) -> None:
        params["service_type"] = service_type
        params["object_type"] = object_type
        params["request_type"] = request_type
        params["payload"] = payload

    await moamqp_test(callback)

    assert list(params.keys()) == [
        "service_type",
        "object_type",
        "request_type",
        "payload",
    ]
    assert type(params["service_type"]) == ServiceType
    assert params["service_type"] == ServiceType.EMPLOYEE
    assert type(params["object_type"]) == ObjectType
    assert params["object_type"] == ObjectType.ADDRESS
    assert type(params["request_type"]) == RequestType
    assert params["request_type"] == RequestType.CREATE
    assert type(params["payload"]) == PayloadType


def test_run_forever(moamqp_system: MOAMQPSystem) -> None:
    # Instead of starting, shutdown the event-loop
    async def start(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        loop.stop()

    # mypy says: Cannot assign to a method, we ignore it
    moamqp_system.start = start  # type: ignore
    moamqp_system.run_forever()


async def test_cannot_publish_before_start(moamqp_system: MOAMQPSystem) -> None:
    routing_tuple = (ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.CREATE)
    payload = parse_obj_as(
        PayloadType,
        {
            "uuid": uuid4(),
            "object_uuid": uuid4(),
            "time": datetime.now().isoformat(),
        },
    )

    with pytest.raises(ValueError):
        await moamqp_system.publish_message(*routing_tuple, payload)


def test_has_started(moamqp_system: MOAMQPSystem) -> None:
    # Fake that the system has started
    assert moamqp_system.started is False
    moamqp_system._connection = {}  # type: ignore
    assert moamqp_system.started is True


def test_to_routing_key() -> None:
    routing_tuple = (ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.CREATE)
    routing_key = to_routing_key(*routing_tuple)
    assert routing_key == "EMPLOYEE.ADDRESS.CREATE"


def test_from_routing_key() -> None:
    service_type, object_type, request_type = from_routing_key(
        "EMPLOYEE.ADDRESS.CREATE"
    )
    assert service_type == ServiceType.EMPLOYEE
    assert object_type == ObjectType.ADDRESS
    assert request_type == RequestType.CREATE


def test_from_routing_key_too_few_parts() -> None:
    with pytest.raises(ValueError) as exc_info:
        from_routing_key("a.b")
    assert str(exc_info.value) == "Expected a three tuple!"


def test_from_routing_key_too_many_parts() -> None:
    with pytest.raises(ValueError) as exc_info:
        from_routing_key("a.b.c.d")
    assert str(exc_info.value) == "Expected a three tuple!"


def test_from_routing_key_invalid_values() -> None:
    with pytest.raises(ValueError) as exc_info:
        from_routing_key("a.b.c")
    assert str(exc_info.value) == "'a' is not a valid ServiceType"

    with pytest.raises(ValueError) as exc_info:
        from_routing_key("EMPLOYEE.b.c")
    assert str(exc_info.value) == "'b' is not a valid ObjectType"

    with pytest.raises(ValueError) as exc_info:
        from_routing_key("EMPLOYEE.ADDRESS.c")
    assert str(exc_info.value) == "'c' is not a valid RequestType"


@given(
    service_type=st.sampled_from(ServiceType),
    object_type=st.sampled_from(ObjectType),
    request_type=st.sampled_from(RequestType),
)
def test_from_routing_key_inverts_to_routing_key(
    service_type: ServiceType, object_type: ObjectType, request_type: RequestType
) -> None:
    routing_tuple = (service_type, object_type, request_type)
    assert from_routing_key(to_routing_key(*routing_tuple)) == routing_tuple
