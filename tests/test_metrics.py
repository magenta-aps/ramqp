# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from typing import cast

from .common import callback_func
from .common import callback_func2
from ramqp import AMQPSystem
from ramqp.metrics import callbacks_registered


def get_callback_metric_value(routing_key: str) -> float:
    metric = callbacks_registered._metrics[(routing_key,)]._value  # type: ignore
    return cast(float, metric.get())


async def test_register_metrics(amqp_system: AMQPSystem) -> None:
    """Test that reigster() metrics behave as expected."""
    # Check that our processing_metric is empty
    assert set(callbacks_registered._metrics.keys()) == set()

    # Register callback
    amqp_system.register("test.routing.key")(callback_func)

    # Test that callback counter has gone up
    assert set(callbacks_registered._metrics.keys()) == {("test.routing.key",)}
    assert get_callback_metric_value("test.routing.key") == 1.0

    # Register another callback
    amqp_system.register("test.routing.key")(callback_func2)

    # Test that callback counter has gone up
    assert set(callbacks_registered._metrics.keys()) == {("test.routing.key",)}
    assert get_callback_metric_value("test.routing.key") == 2.0

    # Register our first function with another routing key
    amqp_system.register("test.another.routing.key")(callback_func)
    assert set(callbacks_registered._metrics.keys()) == {
        ("test.routing.key",),
        ("test.another.routing.key",),
    }
    assert get_callback_metric_value("test.routing.key") == 2.0
    assert get_callback_metric_value("test.another.routing.key") == 1.0
