# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from more_itertools import one

from .common import callback_func
from .common import callback_func2
from ramqp.amqpsystem import callbacks_registered


async def test_register_metrics(amqp_system):
    """Test that reigster() metrics behave as expected."""
    # Check that our processing_metric is empty
    assert set(callbacks_registered._metrics.keys()) == set()

    # Register callback
    amqp_system.register("test.routing.key")(callback_func)

    # Test that callback counter has gone up
    assert set(callbacks_registered._metrics.keys()) == {("test.routing.key",)}
    register_metric = callbacks_registered._metrics[("test.routing.key",)]
    assert register_metric._value.get() == 1.0

    # Register another callback
    amqp_system.register("test.routing.key")(callback_func2)

    # Test that callback counter has gone up
    assert set(callbacks_registered._metrics.keys()) == {("test.routing.key",)}
    register_metric = callbacks_registered._metrics[("test.routing.key",)]
    assert register_metric._value.get() == 2.0

    # Register our first function with another routing key
    decorated_func = amqp_system.register("test.another.routing.key")(callback_func)
    assert set(callbacks_registered._metrics.keys()) == {
        ("test.routing.key",),
        ("test.another.routing.key",),
    }
    another_register_metric = callbacks_registered._metrics[
        ("test.another.routing.key",)
    ]
    assert register_metric._value.get() == 2.0
    assert another_register_metric._value.get() == 1.0
