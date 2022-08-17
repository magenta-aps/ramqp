# SPDX-FileCopyrightText: 2022 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=invalid-name,unused-argument
"""This module tests the util.handle_exclusively decorator."""
import asyncio
from asyncio import Event

from ramqp.utils import handle_exclusively


async def test_unrelated_concurrently() -> None:
    """Test that two unrelated calls work concurrently."""

    @handle_exclusively(key=lambda **kwargs: (kwargs["x"], kwargs["y"]))
    async def f(x: int, y: int, z: int, event: Event) -> None:
        event.set()
        await Event().wait()  # wait forever

    # Prepare calls
    e1 = Event()
    e2 = Event()
    f1 = f(x=1, y=2, z=3, event=e1)
    f2 = f(x=9, y=2, z=3, event=e2)

    # Call
    t1 = asyncio.create_task(f1)
    t2 = asyncio.create_task(f2)

    await asyncio.wait_for(e1.wait(), timeout=1)
    await asyncio.wait_for(e2.wait(), timeout=1)
    assert not t1.done()  # assert that the two calls were indeed concurrent
    assert not t2.done()
    t1.cancel()
    t2.cancel()


async def test_blocking() -> None:
    """Test that the second call is blocked."""

    @handle_exclusively(key=lambda **kwargs: (kwargs["x"], kwargs["y"]))
    async def f(x: int, y: int, z: int, set_event: Event, wait_event: Event) -> None:
        set_event.set()
        await wait_event.wait()

    # Prepare calls
    e1_set = Event()
    e1_wait = Event()
    e2_set = Event()
    e2_wait = Event()
    f1 = f(x=1, y=2, z=3, set_event=e1_set, wait_event=e1_wait)
    f2 = f(x=1, y=2, z=9, set_event=e2_set, wait_event=e2_wait)  # blocked

    # Call
    t1 = asyncio.create_task(f1)
    t2 = asyncio.create_task(f2)

    # Allow t1 to run
    await asyncio.wait_for(e1_set.wait(), timeout=1)
    # t2 shouldn't run
    assert not e2_set.is_set()

    # Allow t1 to finish
    e1_wait.set()
    await asyncio.wait_for(t1, timeout=1)
    assert t1.done()
    # t2 should finish now
    await asyncio.wait_for(e2_set.wait(), timeout=1)
    e2_wait.set()
    await asyncio.wait_for(t2, timeout=1)
    assert t2.done()
