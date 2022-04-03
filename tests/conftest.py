# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import pytest
import structlog
from structlog.testing import LogCapture

from ramqp import AMQPSystem


@pytest.fixture
def log_output():
    return LogCapture()


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output):
    structlog.configure(processors=[log_output])


@pytest.fixture
def amqp_system_creator():
    def make_amqp_system(*args, **kwargs) -> AMQPSystem:
        amqp_system = AMQPSystem(*args, **kwargs)
        # Assert initial configuration
        assert amqp_system.has_started() is False
        assert amqp_system._registry == {}
        return amqp_system

    return make_amqp_system


@pytest.fixture
def amqp_system(amqp_system_creator):
    return amqp_system_creator(queue_name="test_queue")
