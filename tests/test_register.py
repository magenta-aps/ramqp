# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import pytest
from more_itertools import one

from .common import callback_func
from ramqp import InvalidRegisterCallException


def test_register(amqp_system, log_output):
    # Call decorator on our function, and check that the function is not modified
    # The decorator should purely register the callback, not modify our function
    decorated_func = amqp_system.register("test.routing.key")(callback_func)
    assert callback_func == decorated_func

    # Check that the amqp system did not start, and that our function has been added
    assert amqp_system.has_started() == False
    assert len(amqp_system._registry) == 1
    routing_keys = amqp_system._registry[callback_func]
    assert routing_keys == {"test.routing.key"}

    # Test that the call was logged
    assert log_output.entries == [
        {
            "routing_key": "test.routing.key",
            "function": "callback_func",
            "event": "Register called",
            "log_level": "info",
        }
    ]


def test_register_after_start(amqp_system, log_output):
    # Fake that the system has started
    assert amqp_system.has_started() == False
    amqp_system._started = True
    assert amqp_system.has_started() == True

    # Cannot call register after system has started
    with pytest.raises(InvalidRegisterCallException):
        amqp_system.register("test.routing.key")(callback_func)

    # Test that the call was logged
    assert log_output.entries == [
        {
            "routing_key": "test.routing.key",
            "function": "callback_func",
            "event": "Register called",
            "log_level": "info",
        },
        {
            "routing_key": "test.routing.key",
            "function": "callback_func",
            "event": "Cannot register callback after run() has been called!",
            "log_level": "error",
        },
    ]


def test_register_invalid_routing_key(amqp_system):
    # Cannot call register with empty routing key
    with pytest.raises(AssertionError):
        amqp_system.register("")(callback_func)
