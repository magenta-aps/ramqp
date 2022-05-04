# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name,protected-access
"""This module contains pytest specific code, fixtures and helpers."""
import asyncio
import json
from datetime import datetime
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Type
from uuid import uuid4

import pytest
import structlog
from aio_pika import DeliveryMode
from aio_pika import IncomingMessage
from aio_pika import Message
from aiormq.abc import DeliveredMessage
from pamqp.commands import Basic
from pydantic import parse_obj_as
from ra_utils.attrdict import attrdict
from structlog.testing import LogCapture

from .common import random_string
from ramqp import AMQPSystem
from ramqp.abstract_amqpsystem import AbstractAMQPSystem
from ramqp.mo_models import MOCallbackType
from ramqp.mo_models import MORoutingKey
from ramqp.mo_models import ObjectType
from ramqp.mo_models import PayloadType
from ramqp.mo_models import RequestType
from ramqp.mo_models import ServiceType
from ramqp.moqp import MOAMQPSystem


@pytest.fixture
def log_output() -> LogCapture:
    """Pytest fixture to construct an LogCapture."""
    return LogCapture()


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output: LogCapture) -> None:
    """Pytest autofixture to capture all logs."""
    structlog.configure(processors=[log_output])


@pytest.fixture
def _amqp_system_creator() -> Callable[..., AbstractAMQPSystem]:
    def make_amqp_system(
        amqp_system_class: Optional[Type[AbstractAMQPSystem]] = None,
    ) -> AbstractAMQPSystem:
        """Pytest fixture to construct an XAMQPSystem (defaults to AMQPSystem."""
        amqp_system_class = amqp_system_class or AMQPSystem
        amqp_system = amqp_system_class()
        # Assert initial configuration
        assert amqp_system.started is False
        assert amqp_system._registry == {}
        return amqp_system

    return make_amqp_system


@pytest.fixture
def amqp_system(_amqp_system_creator: Callable[..., AMQPSystem]) -> AMQPSystem:
    """Pytest fixture to construct an AMQPSystem."""
    return cast(AMQPSystem, _amqp_system_creator(AMQPSystem))


@pytest.fixture
def moamqp_system(
    _amqp_system_creator: Callable[..., AbstractAMQPSystem]
) -> MOAMQPSystem:
    """Pytest fixture to construct an MOAMQPSystem."""
    return cast(MOAMQPSystem, _amqp_system_creator(MOAMQPSystem))


@pytest.fixture
def aio_pika_message() -> Message:
    """Pytest fixture to construct a aio_pika Message."""
    payload = {"key": "value"}
    return Message(body=json.dumps(payload).encode("utf-8"))


@pytest.fixture
def amqp_test(amqp_system: AMQPSystem) -> Callable:
    """Return an integration-test callable."""

    async def make_amqp_test(callback: Callable) -> None:
        """Setup an integration-test AMQPSystem, send a message to the callback."""
        test_id = random_string()
        queue_prefix = f"test_{test_id}"
        routing_key = "test.routing.key"
        payload = {"value": test_id}
        event = asyncio.Event()

        async def callback_wrapper(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
            await callback(*args, **kwargs)
            event.set()

        amqp_system.register(routing_key)(callback_wrapper)  # type: ignore
        await amqp_system.start(
            queue_prefix=queue_prefix,  # type: ignore
            amqp_exchange=test_id,  # type: ignore
        )
        await amqp_system.publish_message(routing_key, payload)
        await event.wait()
        await amqp_system.stop()

    return make_amqp_test


@pytest.fixture
def mo_payload() -> PayloadType:
    """Pytest fixture to construct a MO PayloadType."""
    return parse_obj_as(
        PayloadType,
        {
            "uuid": uuid4(),
            "object_uuid": uuid4(),
            "time": datetime.now().isoformat(),
        },
    )


@pytest.fixture
def mo_routing_key() -> MORoutingKey:
    """Pytest fixture to construct a MO routing tuple."""
    return MORoutingKey(
        service_type=ServiceType.EMPLOYEE,
        object_type=ObjectType.ADDRESS,
        request_type=RequestType.CREATE,
    )


@pytest.fixture
def moamqp_test(
    moamqp_system: MOAMQPSystem,
    mo_payload: PayloadType,
    mo_routing_key: MORoutingKey,
) -> Callable:
    """Return an integration-test callable."""

    async def make_amqp_test(callback: MOCallbackType) -> None:
        """Setup an integration-test MOAMQPSystem, send a message to the callback."""
        test_id = random_string()
        queue_prefix = f"test_{test_id}"
        event = asyncio.Event()

        async def callback_wrapper(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
            await callback(*args, **kwargs)  # type: ignore
            event.set()

        amqp_system = moamqp_system
        amqp_system.register(mo_routing_key)(callback_wrapper)
        await amqp_system.start(
            queue_prefix=queue_prefix,  # type: ignore
            amqp_exchange=test_id,  # type: ignore
        )
        await amqp_system.publish_message(mo_routing_key, mo_payload)
        await event.wait()
        await amqp_system.stop()

    return make_amqp_test


@pytest.fixture
def aio_pika_delivered_message(aio_pika_message: Message) -> DeliveredMessage:
    """Pytest fixture to construct a aiormq DeliveredMessage."""
    return DeliveredMessage(
        # channel should be an AbstractChannel
        channel=None,  # type: ignore
        header=attrdict(
            {
                "properties": attrdict(
                    {
                        "expiration": None,
                        "content_type": None,
                        "content_encoding": None,
                        "delivery_mode": DeliveryMode.NOT_PERSISTENT,
                        "headers": {},
                        "priority": 0,
                        "correlation_id": None,
                        "reply_to": None,
                        "message_id": "6800cb934bf94cc68009fe04ac91c972",
                        "timestamp": None,
                        "message_type": None,
                        "user_id": None,
                        "app_id": None,
                        "cluster_id": "",
                    }
                )
            }
        ),
        body=aio_pika_message.body,
        delivery=Basic.GetOk(
            delivery_tag=1,
            redelivered=False,
            exchange="9t6wzzmlBcaopTLF1aOPgnnd8szMSU",
            routing_key="test.routing.key",
            message_count=None,
        ),
    )


@pytest.fixture
def aio_pika_incoming_message(
    aio_pika_delivered_message: DeliveredMessage,
) -> IncomingMessage:
    """Pytest fixture to construct a aio_pika IncomingMessage."""
    return IncomingMessage(aio_pika_delivered_message)


def has_elements(iterator: Iterator) -> bool:
    """Check (destructively) if the iterator has any elements.

    Args:
        iterator: The iterator to check for elements

    Returns:
        Whether the iterator has any elements or not.
    """
    return any(True for _ in iterator)


def pytest_collection_modifyitems(items: Any) -> None:
    """Mark all non-integration tests with unittest."""
    for item in items:
        integration_test = has_elements(item.iter_markers("integrationtest"))
        if not integration_test:
            item.add_marker("unittest")
