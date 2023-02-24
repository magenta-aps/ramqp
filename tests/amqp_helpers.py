# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""AMQP Helper methods."""
import json

from aio_pika import DeliveryMode
from aio_pika import IncomingMessage
from aio_pika import Message
from aiormq.abc import DeliveredMessage
from pamqp.commands import Basic
from ra_utils.attrdict import attrdict


def json2raw(payload: dict) -> bytes:
    """Convert a Python dictionary to bytes.

    Args:
        payload: Python dictionary to get converted.

    Returns:
        Byte representation.
    """
    return json.dumps(payload).encode("utf-8")


def raw2message(raw: bytes) -> Message:
    """Wrap a raw bytes body into an aio_pika.Message.

    Args:
        raw: Raw bytes body.

    Returns:
        aio_pika.Message wrapping the raw body.
    """
    return Message(body=raw)


def message2delivered(message: Message) -> DeliveredMessage:
    """Wrap an aio_pika.Message inside an aio_pika.DeliveredMessage.

    Args:
        message: aio_pika.Message.

    Returns:
        aio_pika.DeliveredMessage wrapping the aio_pika.Message.
    """
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
        body=message.body,
        delivery=Basic.GetOk(
            delivery_tag=1,
            redelivered=False,
            exchange="9t6wzzmlBcaopTLF1aOPgnnd8szMSU",
            routing_key="test.routing.key",
            message_count=None,
        ),
    )


def delivered2incoming(message: DeliveredMessage) -> IncomingMessage:
    """Wrap an aio_pika.DeliveredMessage inside an aio_pika.IncomingMessage.

    Args:
        message: aio_pika.DeliveredMessage.

    Returns:
        aio_pika.IncomingMessage wrapping the aio_pika.DeliveredMessage.
    """
    return IncomingMessage(message)


def raw2incoming(raw: bytes) -> IncomingMessage:
    """Wrap a raw bytes body into an aio_pika.IncomingMessage.

    Args:
        raw: Raw bytes body.

    Returns:
        aio_pika.IncomingMessage wrapping the aio_pika.DeliveredMessage wrapping the
        aio_pika.Message wrapping the raw body.
    """
    return delivered2incoming(message2delivered(raw2message(raw)))


def payload2incoming(payload: dict) -> IncomingMessage:
    """Convert and wrap a Python dictionary into an aio_pika.IncomingMessage.

    Args:
        payload: Python dictionary to get converted.

    Returns:
        aio_pika.IncomingMessage wrapping the aio_pika.DeliveredMessage wrapping the
        aio_pika.Message wrapping the converted payload as a raw body.
    """
    return raw2incoming(json2raw(payload))
