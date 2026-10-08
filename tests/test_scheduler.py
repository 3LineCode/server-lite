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


async def test_repeating_reschedules_on_grid_not_on_fire_time() -> None:
    # regression: re-arming from the actual fire time let a slow callback
    # accumulate drift; the next deadline must stay on the original grid.
    sched = Scheduler(loop=asyncio.get_running_loop())
    fires: list[float] = []
    interval = 0.05

    def job() -> None:
        fires.append(time.monotonic())
        time.sleep(0.03)  # simulate slow work inside the tick

    start = time.monotonic()
    handle = sched.call_repeating(interval, job)
    await asyncio.sleep(0.25)
    handle.cancel()
    await sched.close()
    assert len(fires) >= 2
    for i, ts in enumerate(fires):
        expected = start + interval * (i + 1)
        # On-grid (allowing tick jitter), NOT at fire_time + interval which
        # would drift by the 30ms work time every round.
        assert ts - expected < 0.045, f"fire {i} drifted: {ts - expected:.3f}s"


async def test_close_cancels_running_coroutine_callbacks() -> None:
    # regression: close() used to drop coroutine tasks without cancelling
    # them, leaving them running against torn-down services.
    sched = Scheduler(loop=asyncio.get_running_loop())
    inside = asyncio.Event()
    cancelled = asyncio.Event()

    async def hang() -> None:
        inside.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    sched.call_after(0.01, hang)
    await asyncio.wait_for(inside.wait(), 1.0)
    await sched.close()
    await asyncio.wait_for(cancelled.wait(), 1.0)


async def test_exception_isolated() -> None:
    sched = Scheduler(loop=asyncio.get_running_loop())

    def boom() -> None:
        raise RuntimeError("boom")

    ok = asyncio.Event()
    sched.call_after(0.05, boom, label="bad")
    sched.call_after(0.05, ok.set, label="good")
    await asyncio.wait_for(ok.wait(), 1.0)
    await sched.close()
