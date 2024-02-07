# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module contains commonly used test utilities."""
import asyncio
import random
import string
from typing import Any
from unittest.mock import AsyncMock

from aio_pika import IncomingMessage

from ramqp.abstract import AbstractAMQPSystem


async def callback_func1(_: IncomingMessage, **__: Any) -> None:
    """Dummy callback method."""


async def callback_func2(_: IncomingMessage, **__: Any) -> None:
    """Dummy callback method."""


def random_string(length: int = 30) -> str:
    """Generate a random string of characters.

    Args:
        length: The desired length of the string

    Returns:
        A string of random numbers, upper- and lower-case letters.
    """
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(length)
    )


async def _test_run_forever_worker(amqp_system: AbstractAMQPSystem) -> None:
    params: dict[str, Any] = {}

    async def start(*_: Any, **__: Any) -> None:
        # Instead of starting, cancel all tasks, thus closing the event loop
        for task in asyncio.all_tasks():
            task.cancel()

        params["start"] = True

    async def stop(*_: Any, **__: Any) -> None:
        params["stop"] = True

    # mypy says: Cannot assign to a method, we ignore it
    amqp_system.start = start  # type: ignore
    amqp_system.stop = stop  # type: ignore
    async with amqp_system:
        await amqp_system.run_forever()

    assert list(params.keys()) == ["start", "stop"]


async def _test_context_manager(amqp_system: AbstractAMQPSystem) -> None:
    start = AsyncMock()
    stop = AsyncMock()

    # mypy says: Cannot assign to a method, we ignore it
    amqp_system.start = start  # type: ignore
    amqp_system.stop = stop  # type: ignore

    async with amqp_system:
        start.assert_awaited_once()
    stop.assert_awaited_once()
