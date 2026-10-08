"""Supervisor: spawn, death detection, graceful/stubborn termination (F-21)."""

from __future__ import annotations

import asyncio

import pytest

from pyline.core.supervisor import ChildDiedError, ProcessSupervisor, _pid_alive


async def _quiet_child(process_type: str, index: int) -> None:
    await asyncio.sleep(3600)


async def _stubborn_child(process_type: str, index: int) -> None:
    while True:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            continue  # refuse cancellation until killed


async def _dying_child(process_type: str, index: int) -> None:
    raise SystemExit(7)


@pytest.mark.integration
class TestProcessSupervisor:
    async def test_terminate_children_graceful(self) -> None:
        sup = ProcessSupervisor(_quiet_child)
        sup.spawn_subprocesses(("tq",), main_pid=0)
        await asyncio.sleep(1.0)  # let the child boot
        await asyncio.wait_for(sup.terminate_children(grace=5.0), 20.0)
        assert not sup._children

    async def test_terminate_children_kills_stubborn(self) -> None:
        sup = ProcessSupervisor(_stubborn_child)
        sup.spawn_subprocesses(("st",), main_pid=0)
        await asyncio.sleep(1.0)
        await asyncio.wait_for(sup.terminate_children(grace=0.5), 20.0)
        assert not sup._children

    async def test_child_death_detected(self) -> None:
        sup = ProcessSupervisor(_dying_child)
        sup.spawn_subprocesses(("dy",), main_pid=0)
        seen: list[tuple[str, int | None]] = []
        done = asyncio.Event()

        async def on_died(process_type: str, exitcode: int | None) -> None:
            seen.append((process_type, exitcode))
            done.set()

        sup.start_child_watch(on_died)
        await asyncio.wait_for(done.wait(), 20.0)
        assert seen and seen[0][0] == "dy"
        assert seen[0][1] == 7

    async def test_child_death_without_callback_raises(self) -> None:
        sup = ProcessSupervisor(_dying_child)
        sup.spawn_subprocesses(("dx",), main_pid=0)
        with pytest.raises(ChildDiedError, match="dx"):
            await asyncio.wait_for(sup._watch_children(None), 20.0)

    def test_pid_alive_self(self) -> None:
        import os

        assert _pid_alive(os.getpid()) is True
        assert _pid_alive(0x7FFFFFFF) is False
