# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""This module tests the context handling of the AMQP system."""
from typing import Callable
from unittest.mock import AsyncMock

import pytest


@pytest.mark.integrationtest
async def test_context(amqp_test: Callable) -> None:
    """Test that handlers are passed the context object."""
    callback = AsyncMock()
    amqp_system = await amqp_test(callback)
    callback.assert_awaited_once()
    assert callback.await_args.kwargs["context"] is amqp_system.context  # type: ignore
