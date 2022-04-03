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
def amqp_system():
    amqp_system = AMQPSystem(queue_name="test_queue")
    # Assert initial configuration
    assert amqp_system.has_started() == False
    assert amqp_system._registry == {}
    return amqp_system
