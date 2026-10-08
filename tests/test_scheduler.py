"""Scheduler: short/long delays, wheel path, repeating, cancellation."""

from __future__ import annotations

import asyncio
import time

import pytest

from pyline.core.scheduler import Scheduler


async def test_short_delay_fires() -> None:
    sched = Scheduler(loop=asyncio.get_running_loop())
    fired = asyncio.Event()
    sched.call_after(0.02, fired.set, label="t")
    await asyncio.wait_for(fired.wait(), 1.0)
    await sched.close()


async def test_long_delay_via_wheel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pyline.core.scheduler.SHORT_DELAY", 0.3)
    sched = Scheduler(loop=asyncio.get_running_loop())
    fired = asyncio.Event()
    sched.call_after(0.8, fired.set, label="wheel")  # > SHORT_DELAY -> wheel path
    assert sched.pending_count() == 1
    start = time.monotonic()
    await asyncio.wait_for(fired.wait(), 4.0)
    elapsed = time.monotonic() - start
    assert 0.5 <= elapsed <= 3.5
    assert sched.pending_count() == 0
    await sched.close()


async def test_wheel_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pyline.core.scheduler.SHORT_DELAY", 0.3)
    sched = Scheduler(loop=asyncio.get_running_loop())
    fired = asyncio.Event()
    handle = sched.call_after(0.8, fired.set, label="wheel")
    handle.cancel()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fired.wait(), 1.5)
    await sched.close()


async def test_repeating_and_cancel() -> None:
    sched = Scheduler(loop=asyncio.get_running_loop())
    counter = {"n": 0}
    handle = sched.call_repeating(0.05, lambda: counter.__setitem__("n", counter["n"] + 1))
    await asyncio.sleep(0.25)
    handle.cancel()
    n_after_cancel = counter["n"]
    await asyncio.sleep(0.2)
    assert counter["n"] >= 3
    assert counter["n"] == n_after_cancel  # stopped after cancel
    await sched.close()


async def test_exception_isolated() -> None:
    sched = Scheduler(loop=asyncio.get_running_loop())

    def boom() -> None:
        raise RuntimeError("boom")

    ok = asyncio.Event()
    sched.call_after(0.05, boom, label="bad")
    sched.call_after(0.05, ok.set, label="good")
    await asyncio.wait_for(ok.wait(), 1.0)
    await sched.close()
