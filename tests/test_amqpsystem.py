# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the AMQPSystem.start, stop and run-forever methods."""
import pytest
from ra_utils.attrdict import attrdict

from .common import _test_context_manager
from .common import _test_run_forever_worker
from ramqp import AMQPSystem


def test_run_forever(amqp_system: AMQPSystem) -> None:
    """Test that run_forever calls start, then stop."""
    _test_run_forever_worker(amqp_system)


async def test_context_manager(amqp_system: AMQPSystem) -> None:
    """Test that the system is started, then stopped, as a context manager."""
    await _test_context_manager(amqp_system)


async def test_cannot_publish_before_start(amqp_system: AMQPSystem) -> None:
    """Test that messages cannot be published before system start."""
    with pytest.raises(ValueError):
        await amqp_system.publish_message("test.routing.key", {"key": "value"})


def test_has_started_and_health(amqp_system: AMQPSystem) -> None:
    """Test the started property and healthcheck method."""
    # Fake that the system has started
    assert amqp_system.started is False
    assert amqp_system.healthcheck() is False
    # pylint: disable=protected-access
    amqp_system._connection = attrdict({"is_closed": False})  # type: ignore
    assert amqp_system.started is True
    assert amqp_system.healthcheck() is False

    amqp_system._channel = attrdict(
        {"is_closed": False, "is_initialized": True}
    )  # type: ignore
    assert amqp_system.started is True
    assert amqp_system.healthcheck() is True
