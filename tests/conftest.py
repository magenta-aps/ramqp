# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from typing import Callable
from typing import Iterator

import pytest
import structlog
from structlog.testing import LogCapture

from ramqp import AMQPSystem


@pytest.fixture
def log_output() -> LogCapture:
    return LogCapture()


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output) -> None:
    structlog.configure(processors=[log_output])


@pytest.fixture
def amqp_system_creator() -> Callable:
    def make_amqp_system(*args, **kwargs) -> AMQPSystem:
        amqp_system = AMQPSystem(*args, **kwargs)
        # Assert initial configuration
        assert amqp_system.has_started() is False
        assert amqp_system._registry == {}
        return amqp_system

    return make_amqp_system


@pytest.fixture
def amqp_system(amqp_system_creator):
    return amqp_system_creator()


def has_elements(iterator: Iterator) -> bool:
    return any(True for _ in iterator)


def pytest_collection_modifyitems(config, items):
    for item in items:
        integration_test = has_elements(item.iter_markers("integrationtest"))
        if not integration_test:
            item.add_marker("unittest")
