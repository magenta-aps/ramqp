# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the pass_arguments decorator."""
from typing import Any
from typing import Dict

from aio_pika import IncomingMessage

from ramqp.utils import pass_arguments


async def test_message_mock(aio_pika_incoming_message: IncomingMessage) -> None:
    """Test that our message fixture works."""
    params = {}

    async def callback(_: IncomingMessage) -> None:
        params["callback_called"] = True

    await callback(aio_pika_incoming_message)
    assert params["callback_called"] is True


async def test_pass_arguments_message(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    """Test that pass_arguments can pass/noop the message."""
    params = {}

    @pass_arguments
    async def callback(message: IncomingMessage) -> None:
        params["message"] = message

    await callback(aio_pika_incoming_message)
    assert params["message"].priority == 0


async def test_pass_arguments_message_id(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    """Test that pass_arguments can extract the message id."""
    params = {}

    @pass_arguments
    async def callback(message_id: str) -> None:
        params["message_id"] = message_id

    await callback(aio_pika_incoming_message)
    assert params["message_id"] == "6800cb934bf94cc68009fe04ac91c972"


async def test_pass_arguments_payload(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    """Test that pass_arguments can extract and parse the payload."""
    params = {}

    @pass_arguments
    async def callback(payload: dict) -> None:
        params["payload"] = payload

    await callback(aio_pika_incoming_message)
    assert params["payload"] == {"key": "value"}


async def test_pass_arguments_routing_key(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    """Test that pass_arguments can extract the routing key."""
    params = {}

    @pass_arguments
    async def callback(routing_key: str) -> None:
        params["routing_key"] = routing_key

    await callback(aio_pika_incoming_message)
    assert params["routing_key"] == "test.routing.key"


async def test_pass_arguments_body(aio_pika_incoming_message: IncomingMessage) -> None:
    """Test that pass_arguments can extract the message body."""
    params = {}

    @pass_arguments
    async def callback(body: str) -> None:
        params["body"] = body

    await callback(aio_pika_incoming_message)
    assert params["body"] == '{"key": "value"}'


async def test_pass_arguments_multiple(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    """Test that pass_arguments can handle multiple parameters."""
    params: Dict[str, Any] = {}

    @pass_arguments
    async def callback(body: str, routing_key: str, payload: dict) -> None:
        params["body"] = body
        params["routing_key"] = routing_key
        params["payload"] = payload

    # pylint does not understand that pass_arguments changes the signature
    await callback(aio_pika_incoming_message)  # pylint: disable=no-value-for-parameter
    assert params["body"] == '{"key": "value"}'
    assert params["routing_key"] == "test.routing.key"
    assert params["payload"] == {"key": "value"}


async def test_pass_arguments_multiple_reorder(
    aio_pika_incoming_message: IncomingMessage,
) -> None:
    """Test that pass_arguments can handle parameters in any order."""
    params: Dict[str, Any] = {}

    @pass_arguments
    async def callback(payload: dict, body: str, routing_key: str) -> None:
        params["body"] = body
        params["routing_key"] = routing_key
        params["payload"] = payload

    # pylint does not understand that pass_arguments changes the signature
    await callback(aio_pika_incoming_message)  # pylint: disable=no-value-for-parameter
    assert params["body"] == '{"key": "value"}'
    assert params["routing_key"] == "test.routing.key"
    assert params["payload"] == {"key": "value"}
