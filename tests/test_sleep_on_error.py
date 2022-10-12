# SPDX-FileCopyrightText: 2022 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=invalid-name,unused-argument
"""This module tests the util.sleep_on_error decorator."""
import asyncio
from asyncio import Event
from typing import Any

import pytest
from pytest import MonkeyPatch

from ramqp.utils import sleep_on_error


async def test_sleep_on_error(monkeypatch: MonkeyPatch) -> None:
    """Test that the decorator sleeps if an error is thrown."""

    @sleep_on_error()
    async def f() -> None:
        raise ValueError("no thanks")

    sleep_event = Event()

    async def fake_sleep(*args: Any, **kwargs: Any) -> None:
        sleep_event.set()

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(ValueError, match="no thanks"):
        await f()
    assert sleep_event.is_set()


async def test_dont_sleep_on_success(monkeypatch: MonkeyPatch) -> None:
    """Test that the decorator does not sleep if there are no errors."""

    @sleep_on_error()
    async def f() -> None:
        pass

    sleep_event = Event()

    async def fake_sleep(*args: Any, **kwargs: Any) -> None:
        sleep_event.set()

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await f()
    assert not sleep_event.is_set()
