# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the MOAMQPSystem."""
from typing import Any
from typing import Callable
from typing import Dict
from typing import Set

import pytest
from more_itertools import all_unique

from .common import _test_run_forever_worker
from ramqp.moqp import MOAMQPSystem
from ramqp.moqp import MOCallbackType
from ramqp.moqp import MORoutingTuple
from ramqp.moqp import ObjectType
from ramqp.moqp import PayloadType
from ramqp.moqp import RequestType
from ramqp.moqp import ServiceType
from ramqp.moqp import to_routing_key
from ramqp.utils import CallbackType
from ramqp.utils import function_to_name


def get_registry(moamqp_system: MOAMQPSystem) -> Dict[CallbackType, Set[str]]:
    """Extract the MOAMQPSystem callback registry.

    Args:
        moamqp_system: The system to extract the registry from

    Returns:
        The callback registry.
    """
    # pylint: disable=protected-access
    return moamqp_system._registry


def construct_adapter(
    moamqp_system: MOAMQPSystem, callback: MOCallbackType
) -> CallbackType:
    """Construct adapter function.

    Args:
        moamqp_system: The system to adapt the function for.
        callback: The callback to adapt.

    Returns:
        The callback registry.
    """
    # pylint: disable=protected-access
    return moamqp_system._construct_adapter(callback)


async def callback_func1(
    _1: ServiceType,
    _2: ObjectType,
    _3: RequestType,
    _4: PayloadType,
) -> None:
    """Dummy callback method."""


async def callback_func2(
    _1: ServiceType,
    _2: ObjectType,
    _3: RequestType,
    _4: PayloadType,
) -> None:
    """Dummy callback method."""


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


def test_construct_adapter(moamqp_system: MOAMQPSystem) -> None:
    """Test that _construct_adapter works as expected."""

    def get_adapter_map(
        moamqp_system: MOAMQPSystem,
    ) -> Dict[MOCallbackType, CallbackType]:
        return moamqp_system._adapter_map  # pylint: disable=protected-access

    assert get_adapter_map(moamqp_system) == {}

    # Adapt callback_func 1
    adapter1_1 = construct_adapter(moamqp_system, callback_func1)
    assert get_adapter_map(moamqp_system) == {callback_func1: adapter1_1}

    adapter1_2 = construct_adapter(moamqp_system, callback_func1)
    assert get_adapter_map(moamqp_system) == {callback_func1: adapter1_1}
    assert id(adapter1_1) == id(adapter1_2)

    # Adapt callback_func 2
    adapter2_1 = construct_adapter(moamqp_system, callback_func2)
    assert get_adapter_map(moamqp_system) == {
        callback_func1: adapter1_1,
        callback_func2: adapter2_1,
    }

    adapter2_2 = construct_adapter(moamqp_system, callback_func2)
    assert get_adapter_map(moamqp_system) == {
        callback_func1: adapter1_1,
        callback_func2: adapter2_1,
    }
    assert id(adapter2_1) == id(adapter2_2)

    # Assert adapter1 and adapter2 are different
    assert id(adapter1_1) != id(adapter2_1)


def test_register_multiple(moamqp_system: MOAMQPSystem) -> None:
    """Test that functions are added to the registry as expected."""
    # Prepare two routing-keys
    mo_routing_tuple1 = (ServiceType.EMPLOYEE, ObjectType.ADDRESS, RequestType.CREATE)
    routing_key1 = to_routing_key(*mo_routing_tuple1)

    mo_routing_tuple2 = (ServiceType.EMPLOYEE, ObjectType.IT, RequestType.EDIT)
    routing_key2 = to_routing_key(*mo_routing_tuple2)

    # Construct adapter functions
    adapter1 = construct_adapter(moamqp_system, callback_func1)
    adapter2 = construct_adapter(moamqp_system, callback_func2)
    assert get_registry(moamqp_system) == {}

    # Test that registering our callback, adds the adapter to the registry
    moamqp_system.register(*mo_routing_tuple1)(callback_func1)
    assert get_registry(moamqp_system) == {adapter1: {routing_key1}}

    # Test that adding the same entry multiple times only adds once
    moamqp_system.register(*mo_routing_tuple1)(callback_func1)
    assert get_registry(moamqp_system) == {adapter1: {routing_key1}}

    # Test that adding the same callback with another key expands the set
    moamqp_system.register(*mo_routing_tuple2)(callback_func1)
    assert get_registry(moamqp_system) == {adapter1: {routing_key1, routing_key2}}

    # Test that adding an unrelated callback adds another entry
    moamqp_system.register(*mo_routing_tuple1)(callback_func2)
    assert get_registry(moamqp_system) == {
        adapter1: {routing_key1, routing_key2},
        adapter2: {routing_key1},
    }

    # Test that all functions in the registry have unique names
    assert all_unique(map(function_to_name, get_registry(moamqp_system).keys()))
