# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the context handling of the AMQP systems."""
from typing import Callable
from unittest.mock import AsyncMock

import pytest

from ramqp import AMQPSystem
from ramqp.mo import MOAMQPSystem


@pytest.mark.integrationtest
async def test_context_amqp(amqp_test: Callable) -> None:
    """Test that AMQP handlers are passed the context object."""
    context = {"foo": "bar"}
    callback = AsyncMock()

    def post_start(amqp_system: AMQPSystem) -> None:
        amqp_system.context = context

    await amqp_test(callback, post_start=post_start)
    callback.assert_awaited_once()
    assert callback.await_args.kwargs["context"] is context  # type: ignore


@pytest.mark.integrationtest
async def test_context_moamqp(moamqp_test: Callable) -> None:
    """Test that MOAMQP handlers are passed the context object."""
    context = {"foo": "bar"}
    callback = AsyncMock()

    def post_start(amqp_system: MOAMQPSystem) -> None:
        amqp_system.context = context

    await moamqp_test(callback, post_start=post_start)
    callback.assert_awaited_once()
    assert callback.await_args.kwargs["context"] is context  # type: ignore
