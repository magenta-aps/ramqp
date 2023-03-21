# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the MOAMQPSystem."""
from collections.abc import Callable
from typing import Any

import pytest

from .common import _test_context_manager
from .common import _test_run_forever_worker
from ramqp.mo import _PayloadType
from ramqp.mo import MOAMQPSystem
from ramqp.mo import MORoutingKey
from ramqp.mo import PayloadType
from ramqp.utils import CallbackType


def get_registry(moamqp_system: MOAMQPSystem) -> dict[CallbackType, set[str]]:
    """Extract the MOAMQPSystem callback registry.

    Args:
        moamqp_system: The system to extract the registry from

    Returns:
        The callback registry.
    """
    return moamqp_system.router.registry


@pytest.mark.integrationtest
async def test_happy_path(moamqp_test: Callable) -> None:
    """Test that messages can flow through our AMQP system."""
    params: dict[str, Any] = {}

    async def callback(mo_routing_key: MORoutingKey, payload: PayloadType) -> None:
        params["mo_routing_key"] = mo_routing_key
        params["payload"] = payload

    await moamqp_test(callback)

    assert list(params.keys()) == [
        "mo_routing_key",
        "payload",
    ]
    assert isinstance(params["payload"], _PayloadType)

    routing_key = params["mo_routing_key"]
    assert routing_key == "employee.address.create"


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
