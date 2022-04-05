# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the AMQPSystem.start, stop and run-forever methods."""
import pytest

from .common import _test_run_forever_worker
from ramqp import AMQPSystem


def test_run_forever(amqp_system: AMQPSystem) -> None:
    """Test that run_forever calls start, then stop."""
    _test_run_forever_worker(amqp_system)


async def test_cannot_publish_before_start(amqp_system: AMQPSystem) -> None:
    """Test that messages cannot be published before system start."""
    with pytest.raises(ValueError):
        await amqp_system.publish_message("test.routing.key", {"key": "value"})


def test_has_started(amqp_system: AMQPSystem) -> None:
    """Test the started property."""
    # Fake that the system has started
    assert amqp_system.started is False
    # pylint: disable=protected-access
    amqp_system._connection = {}  # type: ignore
    assert amqp_system.started is True