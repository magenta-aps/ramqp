# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from typing import Any
from typing import Callable
from typing import cast
from typing import Iterator
from typing import Optional
from typing import Type

import pytest
import structlog
from structlog.testing import LogCapture

from ramqp import AMQPSystem
from ramqp.abstract_amqpsystem import AbstractAMQPSystem
from ramqp.moqp import MOAMQPSystem


@pytest.fixture
def log_output() -> LogCapture:
    return LogCapture()


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output: LogCapture) -> None:
    structlog.configure(processors=[log_output])


@pytest.fixture
def amqp_system_creator() -> Callable[..., AbstractAMQPSystem]:
    def make_amqp_system(
        amqp_system_class: Optional[Type[AbstractAMQPSystem]] = None,
    ) -> AbstractAMQPSystem:
        amqp_system_class = amqp_system_class or AMQPSystem
        amqp_system = amqp_system_class()
        # Assert initial configuration
        assert amqp_system.started is False
        assert amqp_system._registry == {}
        return amqp_system

    return make_amqp_system


@pytest.fixture
def amqp_system(amqp_system_creator: Callable[..., AMQPSystem]) -> AMQPSystem:
    return cast(AMQPSystem, amqp_system_creator(AMQPSystem))


@pytest.fixture
def moamqp_system(
    amqp_system_creator: Callable[..., AbstractAMQPSystem]
) -> MOAMQPSystem:
    return cast(MOAMQPSystem, amqp_system_creator(MOAMQPSystem))


def has_elements(iterator: Iterator) -> bool:
    return any(True for _ in iterator)


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    for item in items:
        integration_test = has_elements(item.iter_markers("integrationtest"))
        if not integration_test:
            item.add_marker("unittest")
