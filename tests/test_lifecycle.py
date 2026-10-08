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
