# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests that metrics are updated as expected."""
from functools import partial
from typing import Any
from typing import cast

import pytest

from .common import callback_func1
from .common import callback_func2
from ramqp import AMQPSystem
from ramqp.metrics import callbacks_registered


@pytest.fixture(autouse=True)
def clear_register_metrics() -> None:
    """Clear the metric for registered callbacks"""
    callbacks_registered.clear()


def get_metric_value(metric: Any, labels: tuple[str]) -> float:
    """Get the value of a given metric with the given label-set.

    Args:
        metric: The metric to query.
        labels: The label-set to query with.

    Returns:
        The metric value.
    """
    # pylint: disable=protected-access
    metric = metric._metrics[labels]._value
    return cast(float, metric.get())


def get_metric_labels(metric: Any) -> set[tuple[str]]:
    """Get the label-set for a given metric.

    Args:
        metric: The metric to query.

    Returns:
        The label-set.
    """
    # pylint: disable=protected-access
    return set(metric._metrics.keys())


async def test_register_metrics(amqp_system: AMQPSystem) -> None:
    """Test that register() metrics behave as expected."""

    def get_callback_metric_value(routing_key: str) -> float:
        return get_metric_value(callbacks_registered, (routing_key,))

    get_callback_metric_labels = partial(get_metric_labels, callbacks_registered)

    # Check that our processing_metric is empty
    assert get_callback_metric_labels() == set()

    # Register callback
    amqp_system.router.register("test.routing.key")(callback_func1)

    # Test that callback counter has gone up
    assert get_callback_metric_labels() == {("test.routing.key",)}
    assert get_callback_metric_value("test.routing.key") == 1.0

    # Register another callback
    amqp_system.router.register("test.routing.key")(callback_func2)

    # Test that callback counter has gone up
    assert get_callback_metric_labels() == {("test.routing.key",)}
    assert get_callback_metric_value("test.routing.key") == 2.0

    # Register our first function with another routing key
    amqp_system.router.register("test.another.routing.key")(callback_func1)
    assert get_callback_metric_labels() == {
        ("test.routing.key",),
        ("test.another.routing.key",),
    }
    assert get_callback_metric_value("test.routing.key") == 2.0
    assert get_callback_metric_value("test.another.routing.key") == 1.0
