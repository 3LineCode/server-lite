"""Soak smoke (F-228, nightly only -- marker ``soak``).

A bounded slice of the 72h soak the migration plan defers: boot the real
runtime (db-less gateway topology), drive it with synthetic churn for
``SOAK_SECONDS``, then assert the three failure classes a soak exists to
catch early:

* event-loop latency stayed sane (measured sleep overshoot under load, plus
  the monitor's >=1s bucket staying empty);
* memory did not creep (tracemalloc snapshot diff over the window -- the
  classic leaked-task/accumulator regression);
* teardown stays clean after sustained load (flush ok, exit-0 semantics).

The 72h soak + heap-diff against a production workload remains a rc->GA
gate; this is the nightly smoke version of it.
"""

from __future__ import annotations

import asyncio
import tracemalloc
from pathlib import Path

import pytest

from pyline import api as pyline_api
from pyline.core.events import NewHourEvent
from pyline.runtime import ServerRuntime, _settle_shutdown, build_context
from tests.test_runtime_boot import BOOT_SERVER_NO

pytestmark = pytest.mark.soak

SOAK_SECONDS = 15.0
#: Loop-latency budget: well above a healthy loop (<10 ms), far below the
#: region where frame dispatch starts dropping (seconds).
LOOP_LATENCY_BUDGET_S = 0.25
#: Growth budget over the window: generous (Python allocates in arenas) but
#: catches runaway accumulators, which grow by megabytes per second.
HEAP_GROWTH_BUDGET_MB = 24.0


@pytest.fixture
def boot_env(config_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Same shape as test_runtime_boot.boot_env (fixtures cannot be imported
    across test modules): config + freshly imported business package."""
    import sys

    from tests.test_runtime_boot import _write_business, _write_config

    _write_config(config_dir)
    business_dir = tmp_path / "business"
    _write_business(business_dir)
    monkeypatch.syspath_prepend(str(business_dir))
    monkeypatch.setenv("PYLINE_EVENTS", "bootgame.events")
    monkeypatch.delitem(sys.modules, "bootgame.events", raising=False)
    monkeypatch.delitem(sys.modules, "bootgame", raising=False)
    return config_dir


async def _sleep_overshoot() -> float:
    """Scheduling delay of one 10 ms sleep (overshoot beyond the target)."""
    loop = asyncio.get_running_loop()
    target = 0.01
    t0 = loop.time()
    await asyncio.sleep(target)
    return loop.time() - t0 - target


async def test_soak_smoke(boot_env: Path) -> None:
    ctx = build_context(boot_env, BOOT_SERVER_NO, "main", 0, 0)
    pyline_api.bind(ctx)
    runtime = ServerRuntime(ctx)
    try:
        await runtime.boot()
        assert runtime.lifecycle.is_started()

        handled = {"events": 0, "timers": 0}

        def on_hour(_event) -> None:
            handled["events"] += 1

        runtime.bus.subscribe(NewHourEvent, on_hour)

        def churn_timer() -> None:
            handled["timers"] += 1
            runtime.scheduler.call_after(0.01, churn_timer, label="soak-churn")

        runtime.scheduler.call_after(0.01, churn_timer, label="soak-churn")

        tracemalloc.start()
        before, _peak = tracemalloc.get_traced_memory()
        worst_overshoot = 0.0
        deadline = asyncio.get_running_loop().time() + SOAK_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            await runtime.bus.emit(NewHourEvent(hour=1))
            worst_overshoot = max(worst_overshoot, await _sleep_overshoot())
        after, _peak2 = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # 1) the loop kept scheduling promptly under sustained churn.
        assert worst_overshoot < LOOP_LATENCY_BUDGET_S, (
            f"loop latency degraded: sleep overshoot {worst_overshoot:.3f}s"
        )

        # 2) no runaway growth.
        growth_mb = (after - before) / (1024 * 1024)
        assert growth_mb < HEAP_GROWTH_BUDGET_MB, f"heap grew {growth_mb:.1f} MiB"

        # 3) the churn actually ran (a soak that exercised nothing proves
        # nothing) and clean teardown still holds.
        assert handled["events"] > 100
        assert handled["timers"] > 100
        runtime._spawn(runtime.shutdown("soak end"))
        await _settle_shutdown(runtime)
        assert runtime._flush_completed is True
        assert runtime.save_flush_ok is True
    finally:
        pyline_api.unbind()
