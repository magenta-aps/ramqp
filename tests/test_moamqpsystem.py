# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the MOAMQPSystem."""
from typing import Any
from typing import Callable
from typing import Dict

import pytest

from .common import _test_run_forever_worker
from ramqp.moqp import MOAMQPSystem
from ramqp.moqp import MORoutingTuple
from ramqp.moqp import ObjectType
from ramqp.moqp import PayloadType
from ramqp.moqp import RequestType
from ramqp.moqp import ServiceType


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
    assert isinstance(params["service_type"], ServiceType)
    assert params["service_type"] == ServiceType.EMPLOYEE
    assert isinstance(params["object_type"], ObjectType)
    assert params["object_type"] == ObjectType.ADDRESS
    assert isinstance(params["request_type"], RequestType)
    assert params["request_type"] == RequestType.CREATE
    assert isinstance(params["payload"], PayloadType)


def test_run_forever(moamqp_system: MOAMQPSystem) -> None:
    """Test that run_forever calls start, then stop."""
    _test_run_forever_worker(moamqp_system)


async def test_cannot_publish_before_start(
    moamqp_system: MOAMQPSystem,
    mo_payload: PayloadType,
    mo_routing_tuple: MORoutingTuple,
) -> None:
    """Test that messages cannot be published before system start."""
    with pytest.raises(ValueError):
        await moamqp_system.publish_message(*mo_routing_tuple, mo_payload)


def test_has_started(moamqp_system: MOAMQPSystem) -> None:
    """Test the started property."""
    # Fake that the system has started
    assert moamqp_system.started is False
    # pylint: disable=protected-access
    moamqp_system._connection = {}  # type: ignore
    assert moamqp_system.started is True
