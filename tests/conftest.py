# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name,not-callable
"""This module contains pytest specific code, fixtures and helpers."""
import asyncio
import json
from collections.abc import Callable
from collections.abc import Iterator
from datetime import datetime
from typing import Any
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
from ramqp import Router
from ramqp.config import ConnectionSettings
from ramqp.mo import MOAMQPSystem
from ramqp.mo import MORouter
from ramqp.mo.models import MOCallbackType
from ramqp.mo.models import MORoutingKey
from ramqp.mo.models import ObjectType
from ramqp.mo.models import PayloadType
from ramqp.mo.models import RequestType
from ramqp.mo.models import ServiceType


@pytest.fixture
def log_output() -> LogCapture:
    """Pytest fixture to construct an LogCapture."""
    return LogCapture()


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output: LogCapture) -> None:
    """Pytest autofixture to capture all logs."""
    structlog.configure(processors=[log_output])


@pytest.fixture
def amqp_system() -> AMQPSystem:
    """Pytest fixture to construct an AMQPSystem."""
    return AMQPSystem()


@pytest.fixture
def moamqp_system() -> MOAMQPSystem:
    """Pytest fixture to construct an MOAMQPSystem."""
    return MOAMQPSystem()


@pytest.fixture
def amqp_router() -> Router:
    """Pytest fixture to construct an Router."""
    return Router()


@pytest.fixture
def moamqp_router() -> MORouter:
    """Pytest fixture to construct an MORouter."""
    return MORouter()


@pytest.fixture
def aio_pika_message() -> Message:
    """Pytest fixture to construct a aio_pika Message."""
    payload = {"key": "value"}
    return Message(body=json.dumps(payload).encode("utf-8"))


@pytest.fixture
def amqp_test() -> Callable:
    """Return an integration-test callable."""

    async def make_amqp_test(
        callback: Callable,
        post_start: Callable[[AMQPSystem], None] | None = None,
        num_messages: int = 1,
    ) -> AMQPSystem:
        """Setup an integration-test AMQPSystem, send a message to the callback."""
        test_id = random_string()
        queue_prefix = f"test_{test_id}"
        routing_key = "test.routing.key"
        payload = {"value": test_id}
        message_blocker = asyncio.Semaphore(0)

        async def callback_wrapper(*args: Any, **kwargs: Any) -> None:
            try:
                await callback(*args, **kwargs)
            finally:
                message_blocker.release()

        amqp_system = AMQPSystem(
            settings=ConnectionSettings(
                queue_prefix=queue_prefix,
                exchange=test_id,
            ),
        )
        amqp_system.router.register(routing_key)(callback_wrapper)
        async with amqp_system:
            if post_start is not None:
                post_start(amqp_system)
            await amqp_system.publish_message(routing_key, payload)
            tasks = [message_blocker.acquire() for _ in range(num_messages)]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
        return amqp_system

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
    mo_payload: PayloadType,
    mo_routing_key: MORoutingKey,
) -> Callable:
    """Return an integration-test callable."""

    async def make_amqp_test(
        callback: MOCallbackType,
        post_start: Callable[[MOAMQPSystem], None] | None = None,
        num_messages: int = 1,
    ) -> None:
        """Setup an integration-test MOAMQPSystem, send a message to the callback."""
        test_id = random_string()
        queue_prefix = f"test_{test_id}"
        message_blocker = asyncio.Semaphore(0)

        async def callback_wrapper(*args: Any, **kwargs: Any) -> None:
            try:
                await callback(*args, **kwargs)  # type: ignore
            finally:
                message_blocker.release()

        amqp_system = MOAMQPSystem(
            settings=ConnectionSettings(
                queue_prefix=queue_prefix,
                exchange=test_id,
            ),
        )
        amqp_system.router.register(mo_routing_key)(callback_wrapper)
        async with amqp_system:
            if post_start is not None:
                post_start(amqp_system)
            await amqp_system.publish_message(mo_routing_key, mo_payload)
            tasks = [message_blocker.acquire() for _ in range(num_messages)]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

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
