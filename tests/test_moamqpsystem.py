# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the MOAMQPSystem."""
from asyncio import Event
from collections.abc import Callable
from typing import Any

import pytest
from more_itertools import all_unique

from .common import _test_context_manager
from .common import _test_run_forever_worker
from .common import random_string
from ramqp.config import ConnectionSettings
from ramqp.mo import MOAMQPSystem
from ramqp.mo.models import MOCallbackType
from ramqp.mo.models import MORoutingKey
from ramqp.mo.models import ObjectType
from ramqp.mo.models import PayloadType
from ramqp.mo.models import RequestType
from ramqp.mo.models import ServiceType
from ramqp.utils import CallbackType
from ramqp.utils import function_to_name


def get_registry(moamqp_system: MOAMQPSystem) -> dict[CallbackType, set[str]]:
    """Extract the MOAMQPSystem callback registry.

    Args:
        moamqp_system: The system to extract the registry from

    Returns:
        The callback registry.
    """
    return moamqp_system.router.registry


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
    return moamqp_system.router._construct_adapter(callback)


async def callback_func1(_1: MORoutingKey, _2: PayloadType, **__: Any) -> None:
    """Dummy callback method."""


async def callback_func2(_1: MORoutingKey, _2: PayloadType, **__: Any) -> None:
    """Dummy callback method."""


@pytest.mark.integrationtest
async def test_happy_path(moamqp_test: Callable) -> None:
    """Test that messages can flow through our AMQP system."""
    params: dict[str, Any] = {}

    async def callback(
        mo_routing_key: MORoutingKey, payload: PayloadType, **_: Any
    ) -> None:
        params["mo_routing_key"] = mo_routing_key
        params["payload"] = payload

    await moamqp_test(callback)

    assert list(params.keys()) == [
        "mo_routing_key",
        "payload",
    ]
    assert isinstance(params["mo_routing_key"], MORoutingKey)
    assert isinstance(params["payload"], PayloadType)

    routing_key = params["mo_routing_key"]
    assert routing_key.service_type == ServiceType.EMPLOYEE
    assert routing_key.object_type == ObjectType.ADDRESS
    assert routing_key.request_type == RequestType.CREATE


async def test_run_forever(moamqp_system: MOAMQPSystem) -> None:
    """Test that run_forever calls start, then stop."""
    await _test_run_forever_worker(moamqp_system)


async def test_context_manager(moamqp_system: MOAMQPSystem) -> None:
    """Test that the system is started, then stopped, as a context manager."""
    await _test_context_manager(moamqp_system)


async def test_cannot_publish_before_start(
    moamqp_system: MOAMQPSystem,
    mo_payload: PayloadType,
    mo_routing_key: MORoutingKey,
) -> None:
    """Test that messages cannot be published before system start."""
    with pytest.raises(ValueError):
        await moamqp_system.publish_message(mo_routing_key, mo_payload)


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
    ) -> dict[MOCallbackType, CallbackType]:
        return moamqp_system.router._adapter_map  # pylint: disable=protected-access

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
    routing_key1 = str(MORoutingKey.build(mo_routing_tuple1))

    mo_routing_tuple2 = (ServiceType.EMPLOYEE, ObjectType.IT, RequestType.EDIT)
    routing_key2 = str(MORoutingKey.build(*mo_routing_tuple2))

    # Construct adapter functions
    adapter1 = construct_adapter(moamqp_system, callback_func1)
    adapter2 = construct_adapter(moamqp_system, callback_func2)
    assert get_registry(moamqp_system) == {}

    # Test that registering our callback, adds the adapter to the registry
    moamqp_system.router.register(*mo_routing_tuple1)(callback_func1)
    assert get_registry(moamqp_system) == {adapter1: {routing_key1}}

    # Test that adding the same entry multiple times only adds once
    moamqp_system.router.register(*mo_routing_tuple1)(callback_func1)
    assert get_registry(moamqp_system) == {adapter1: {routing_key1}}

    # Test that adding the same callback with another key expands the set
    moamqp_system.router.register(*mo_routing_tuple2)(callback_func1)
    assert get_registry(moamqp_system) == {adapter1: {routing_key1, routing_key2}}

    # Test that adding an unrelated callback adds another entry
    moamqp_system.router.register(*mo_routing_tuple1)(callback_func2)
    assert get_registry(moamqp_system) == {
        adapter1: {routing_key1, routing_key2},
        adapter2: {routing_key1},
    }

    # Test that all functions in the registry have unique names
    assert all_unique(map(function_to_name, get_registry(moamqp_system).keys()))


@pytest.mark.integrationtest
async def test_handler_exclusivity(
    mo_payload: PayloadType, mo_routing_key: MORoutingKey
) -> None:
    """Test that messages are handled exclusively."""
    # Prepare handlers
    num_calls = 0
    set_event = Event()  # allows us to detect call
    wait_event = Event()  # allows us to block completion

    async def callback(*_: Any, **__: Any) -> None:
        nonlocal num_calls
        num_calls += 1
        set_event.set()
        await wait_event.wait()

    # Setup MO AMQP system
    test_id = random_string()
    queue_prefix = f"test_{test_id}"
    amqp_system = MOAMQPSystem(
        settings=ConnectionSettings(
            queue_prefix=queue_prefix,
            exchange=test_id,
        ),
    )
    amqp_system.router.register(mo_routing_key)(callback)

    async with amqp_system:
        # Publish message twice
        await amqp_system.publish_message(mo_routing_key, mo_payload)
        await amqp_system.publish_message(mo_routing_key, mo_payload)

        # Wait for a handler to receive the call
        await set_event.wait()
        # and assert that only one did
        assert num_calls == 1

        # Allow that handler to finish
        set_event.clear()
        wait_event.set()
        # and wait for the second one to receive the call
        await set_event.wait()
        assert num_calls == 2
