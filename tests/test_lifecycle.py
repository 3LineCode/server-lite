"""Lifecycle: transitions, gates, watchdog, shutdown sequencing."""

from __future__ import annotations

import asyncio

import pytest

from pyline.core.lifecycle import (
    BOOT_STEPS,
    LifecycleManager,
    LifecycleState,
    StartupStuckError,
)


async def test_full_boot_sequence() -> None:
    manager = LifecycleManager(startup_timeout=10.0)
    visited: list[LifecycleState] = []
    for state in BOOT_STEPS:

        async def action(state: LifecycleState = state) -> None:
            visited.append(state)

        manager.on_step(state, action)
    await manager.run_boot()
    assert manager.state == LifecycleState.FINISHED
    assert visited == list(BOOT_STEPS)


async def test_start_wait_blocks_until_released() -> None:
    manager = LifecycleManager(startup_timeout=10.0)
    entered: list[LifecycleState] = []
    manager.on_step(LifecycleState.FRAME_INIT, lambda: entered.append(LifecycleState.FRAME_INIT))

    async def frame_init() -> None:
        entered.append(LifecycleState.FRAME_INIT)
        release = manager.add_start_wait("load-tables")
        # release after 50ms
        asyncio.get_running_loop().call_later(0.05, release)

    manager.on_step(LifecycleState.FRAME_INIT, frame_init)
    await manager.run_boot()
    assert manager.state == LifecycleState.FINISHED


async def test_duplicate_start_wait_rejected() -> None:
    manager = LifecycleManager()
    manager.add_start_wait("x")
    with pytest.raises(ValueError, match="already exists"):
        manager.add_start_wait("x")


async def test_watchdog_names_pending_flag() -> None:
    manager = LifecycleManager(startup_timeout=0.01)

    async def stall() -> None:
        manager.add_start_wait("forever-wait")

    manager.on_step(LifecycleState.LOOP_INIT, stall)
    with pytest.raises(StartupStuckError, match="forever-wait"):
        await manager.run_boot()


async def test_illegal_transition_rejected() -> None:
    manager = LifecycleManager()
    with pytest.raises(RuntimeError, match="illegal lifecycle transition"):
        await manager._enter(LifecycleState.BASE_INIT)


async def test_shutdown_runs_hooks_and_quits_tasks() -> None:
    manager = LifecycleManager(quit_timeout=1.0)
    hooks: list[str] = []
    manager.on_shutdown(lambda: hooks.append("h1") or asyncio.sleep(0))

    async def slow_quit() -> None:
        await asyncio.sleep(0.05)
        hooks.append("quit-task")

    async def coro() -> None:
        await asyncio.sleep(10)

    task = asyncio.get_running_loop().create_task(coro())
    await manager.request_shutdown("test")
    manager.track_quit_task(task)
    task.cancel()
    assert manager.state == LifecycleState.QUIT


class TestBootHardeningF19:
    async def test_boot_fails_when_start_task_fails(self) -> None:
        """F-19: a failed startup task aborts the boot (was only logged)."""
        manager = LifecycleManager(startup_timeout=10.0)

        async def failing() -> None:
            raise ConnectionError("db warmup died")

        async def loop_init() -> None:
            task = asyncio.get_running_loop().create_task(failing())
            manager.track_start_task(task)

        manager.on_step(LifecycleState.LOOP_INIT, loop_init)
        with pytest.raises(StartupStuckError, match="db warmup died"):
            await asyncio.wait_for(manager.run_boot(), 5.0)

    async def test_shutdown_during_boot_exits_cleanly(self) -> None:
        """F-19: a shutdown request mid-boot no longer hangs the boot loop."""
        manager = LifecycleManager(startup_timeout=10.0)
        cancelled: list[bool] = []

        async def parked() -> None:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        async def loop_init() -> None:
            task = asyncio.get_running_loop().create_task(parked())
            manager.track_start_task(task)

        manager.on_step(LifecycleState.LOOP_INIT, loop_init)
        boot = asyncio.get_running_loop().create_task(manager.run_boot())
        await asyncio.sleep(0.15)  # boot is now gating on the parked task
        await manager.request_shutdown("test")
        await asyncio.wait_for(boot, 3.0)  # used to hang forever
        assert cancelled == [True]
        assert manager.state == LifecycleState.QUIT
