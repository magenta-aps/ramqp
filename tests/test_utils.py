# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import json
from typing import Any
from typing import Dict

import pytest
from aio_pika import DeliveryMode
from aio_pika import IncomingMessage
from aio_pika import Message
from ra_utils.attrdict import attrdict

from ramqp.utils import pass_arguments


@pytest.fixture
def aio_pika_message() -> Message:
    payload = {"key": "value"}
    message = Message(body=json.dumps(payload).encode("utf-8"))
    message.channel = None
    message.header = attrdict(
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
    )
    message.consumer_tag = "ctag1.6e914f0225204a178566370cd93c8b40"
    message.delivery_tag = 1
    message.exchange = "9t6wzzmlBcaopTLF1aOPgnnd8szMSU"
    message.message_count = None
    message.redelivered = False
    message.routing_key = "test.routing.key"
    return message


@pytest.fixture
def aio_pika_incoming_message(aio_pika_message: Message) -> IncomingMessage:
    # TODO: We should convert Message to DeliveredMessage first
    return IncomingMessage(aio_pika_message)  # type: ignore


async def test_message_mock(aio_pika_incoming_message: IncomingMessage) -> None:
    params = {}

    async def callback(message: IncomingMessage) -> None:
        params["callback_called"] = True

    await callback(aio_pika_incoming_message)
    assert params["callback_called"] is True


async def test_pass_arguments_message(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    params = {}

    @pass_arguments
    async def callback(message: IncomingMessage) -> None:
        params["message"] = message

    await callback(aio_pika_incoming_message)
    assert params["message"].priority == 0


async def test_pass_arguments_message_id(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    params = {}

    @pass_arguments
    async def callback(message_id: str) -> None:
        params["message_id"] = message_id

    await callback(aio_pika_incoming_message)
    assert params["message_id"] == "6800cb934bf94cc68009fe04ac91c972"


async def test_pass_arguments_payload(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    params = {}

    @pass_arguments
    async def callback(payload: dict) -> None:
        params["payload"] = payload

    await callback(aio_pika_incoming_message)
    assert params["payload"] == {"key": "value"}


async def test_pass_arguments_routing_key(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    params = {}

    @pass_arguments
    async def callback(routing_key: str) -> None:
        params["routing_key"] = routing_key

    await callback(aio_pika_incoming_message)
    assert params["routing_key"] == "test.routing.key"


async def test_pass_arguments_body(aio_pika_incoming_message: IncomingMessage) -> None:
    params = {}

    @pass_arguments
    async def callback(body: str) -> None:
        params["body"] = body

    await callback(aio_pika_incoming_message)
    assert params["body"] == '{"key": "value"}'


async def test_pass_arguments_multiple(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    params: Dict[str, Any] = {}

    @pass_arguments
    async def callback(body: str, routing_key: str, payload: dict) -> None:
        params["body"] = body
        params["routing_key"] = routing_key
        params["payload"] = payload

    await callback(aio_pika_incoming_message)
    assert params["body"] == '{"key": "value"}'
    assert params["routing_key"] == "test.routing.key"
    assert params["payload"] == {"key": "value"}


async def test_pass_arguments_multiple_reorder(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    params: Dict[str, Any] = {}

    @pass_arguments
    async def callback(payload: dict, body: str, routing_key: str) -> None:
        params["body"] = body
        params["routing_key"] = routing_key
        params["payload"] = payload

    await callback(aio_pika_incoming_message)
    assert params["body"] == '{"key": "value"}'
    assert params["routing_key"] == "test.routing.key"
    assert params["payload"] == {"key": "value"}
