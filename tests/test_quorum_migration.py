# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the migration to quorum queues."""
import pytest
from aio_pika import connect
from aio_pika import Message
from pydantic import AmqpDsn
from pydantic import parse_obj_as

from ramqp import AMQPSystem
from ramqp.config import AMQPConnectionSettings
from tests.common import random_string


@pytest.mark.integrationtest
async def test_quorum_queue_migration() -> None:
    """Test that classic queues are 'converted' to quorum queues."""
    url = "amqp://guest:guest@localhost:5672"
    queue_prefix = random_string()
    connection = await connect(url)
    channel = await connection.channel()

    amqp_system = AMQPSystem(
        settings=AMQPConnectionSettings(
            url=parse_obj_as(AmqpDsn, url),
            queue_prefix=queue_prefix,
            exchange=random_string(),
        ),
    )

    # Create classic queue
    queue_name = f"{queue_prefix}_noop_handler"
    await channel.declare_queue(queue_name, durable=True)

    # Define handler for the same queue name
    async def noop_handler() -> None:
        pass

    amqp_system.router.register(routing_key="foo")(noop_handler)

    async with amqp_system:
        pass

    # Assert that the channel exists as quorum
    assert await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
        },
    )


@pytest.mark.integrationtest
async def test_quorum_queue_already_migrated() -> None:
    """Test that the migration works if it is already migrated."""
    url = "amqp://guest:guest@localhost:5672"
    queue_prefix = random_string()
    connection = await connect(url)
    channel = await connection.channel()

    amqp_system = AMQPSystem(
        settings=AMQPConnectionSettings(
            url=parse_obj_as(AmqpDsn, url),
            queue_prefix=queue_prefix,
            exchange=random_string(),
        ),
    )

    # Create quorum queue
    queue_name = f"{queue_prefix}_noop_handler"
    await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
        },
    )

    # Define handler for the same queue name
    async def noop_handler() -> None:
        pass

    amqp_system.router.register(routing_key="foo")(noop_handler)

    async with amqp_system:
        pass

    # Assert that the channel exists as quorum
    assert await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
        },
    )


@pytest.mark.integrationtest
async def test_quorum_queue_migration_fails_if_not_empty() -> None:
    """Test that the migration fails if the queue isn't empty."""
    url = "amqp://guest:guest@localhost:5672"
    queue_prefix = random_string()
    exchange = random_string()
    connection = await connect(url)
    channel = await connection.channel()

    amqp_system = AMQPSystem(
        settings=AMQPConnectionSettings(
            url=parse_obj_as(AmqpDsn, url),
            queue_prefix=queue_prefix,
            exchange=exchange,
        ),
    )

    # Create classic queue
    queue_name = f"{queue_prefix}_noop_handler"
    await channel.declare_queue(queue_name, durable=True)
    await channel.default_exchange.publish(Message(body=b"lol"), queue_name)

    # Define handler for the same queue name
    async def noop_handler() -> None:
        pass

    amqp_system.router.register(routing_key="foo")(noop_handler)

    async with amqp_system:
        pass

    # Assert that the channel exists as non-quorum/classic
    assert await channel.declare_queue(queue_name, durable=True)
