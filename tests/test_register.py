# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the AMQPSystem.register decorator method."""
from typing import Dict
from typing import Set

import pytest
from structlog.testing import LogCapture

from .common import callback_func1
from ramqp import AMQPSystem
from ramqp.utils import CallbackType


def get_registry(amqp_system: AMQPSystem) -> Dict[CallbackType, Set[str]]:
    """Extract the AMQPSystem callback registry.

    Args:
        amqp_system: The system to extract the registry from

    Returns:
        The callback registry.
    """
    # pylint: disable=protected-access
    return amqp_system._registry


def test_register(amqp_system: AMQPSystem, log_output: LogCapture) -> None:
    """Happy-path test.

    Tests that:
    * The decorator does not modify the callback.
    * The callback is added to the callback registry
    * Log outputs are as expected
    """
    # Call decorator on our function, and check that the function is not modified
    # The decorator should purely register the callback, not modify our function
    decorated_func = amqp_system.register("test.routing.key")(callback_func1)
    assert id(callback_func1) == id(decorated_func)

    # Check that the amqp system did not start, and that our function has been added
    assert amqp_system.started is False
    registry = get_registry(amqp_system)
    assert len(registry) == 1
    routing_keys = registry[callback_func1]
    assert routing_keys == {"test.routing.key"}

    # Test that the call was logged
    assert log_output.entries == [
        {
            "routing_key": "test.routing.key",
            "function": "callback_func1",
            "event": "Register called",
            "log_level": "info",
        }
    ]


def test_register_after_start(amqp_system: AMQPSystem, log_output: LogCapture) -> None:
    """Test that a callbacks can only be registered before system starts running."""
    # Fake that the system has started
    assert amqp_system.started is False
    # pylint: disable=protected-access
    amqp_system._connection = {}  # type: ignore
    assert amqp_system.started is True

    # Cannot call register after system has started
    with pytest.raises(ValueError):
        amqp_system.register("test.routing.key")(callback_func1)

    # Test that the call was logged
    assert log_output.entries == [
        {
            "routing_key": "test.routing.key",
            "function": "callback_func1",
            "event": "Register called",
            "log_level": "info",
        },
        {
            "routing_key": "test.routing.key",
            "function": "callback_func1",
            "event": "Cannot register callback after run() has been called!",
            "log_level": "error",
        },
    ]


def test_register_invalid_routing_key(amqp_system: AMQPSystem) -> None:
    """Test that you cannot call register with an empty routing key."""
    # Cannot call register with empty routing key
    with pytest.raises(AssertionError):
        amqp_system.register("")(callback_func1)
