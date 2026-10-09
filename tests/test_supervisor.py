"""Supervisor: spawn, death detection, graceful/stubborn termination (F-21)."""

from __future__ import annotations

import asyncio
import logging

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


class _FakeChild:
    """Duck-typed stand-in for mp.Process (fast unit path for watch logic)."""

    def __init__(self, exitcode: int | None) -> None:
        self.exitcode = exitcode
        self.pid = 4242
        self.terminated = False

    def is_alive(self) -> bool:
        return self.exitcode is None

    def terminate(self) -> None:
        self.terminated = True
        self.exitcode = -15


class TestDuplicateProcessTypeF81:
    def test_spawn_rejects_duplicate_types(self) -> None:
        """F-81: a duplicated process_type silently overwrote the children
        slot -- both were spawned, only the last was watched/terminated."""
        sup = ProcessSupervisor(_quiet_child)
        with pytest.raises(ValueError, match=r"duplicate sub-process types \['db'\]"):
            sup.spawn_subprocesses(("db", "game", "db"), main_pid=0)
        assert not sup._children  # rejected BEFORE any spawn: no orphans


class TestWatchSemanticsF82:
    async def test_raising_callback_falls_back_to_fail_fast(self) -> None:
        """F-82: a buggy on_child_died used to kill the watch task silently,
        leaving the remaining children unmanaged."""
        sup = ProcessSupervisor(_quiet_child)
        sup._children = {"dead": _FakeChild(7), "sibling": _FakeChild(None)}
        calls: list[str] = []

        async def bad_callback(process_type: str, exitcode: int | None) -> None:
            calls.append(process_type)
            raise RuntimeError("callback bug")

        with pytest.raises(ChildDiedError, match="dead"):
            await sup._watch_children(bad_callback)
        assert calls == ["dead"]  # callback was invoked exactly once
        # the sibling was terminated by the fail-fast fallback, not leaked
        sibling = sup._children["sibling"]
        assert isinstance(sibling, _FakeChild)
        assert sibling.terminated is True

    async def test_raising_callback_logged_as_critical(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        sup = ProcessSupervisor(_quiet_child)
        sup._children = {"dead": _FakeChild(1)}

        async def bad_callback(process_type: str, exitcode: int | None) -> None:
            raise RuntimeError("callback bug")

        with (
            caplog.at_level(logging.CRITICAL, logger="pyline.core.supervisor"),
            pytest.raises(ChildDiedError),
        ):
            task = asyncio.get_running_loop().create_task(sup._watch_children(bad_callback))
            await asyncio.wait_for(task, 5.0)
        assert any("on_child_died callback failed" in r.message for r in caplog.records)

    async def test_watch_task_death_is_observed_not_silent(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """F-82: via start_child_watch (fire-and-forget), the ChildDiedError
        used to die unretrieved inside the task; the done-callback must
        retrieve AND escalate it."""
        sup = ProcessSupervisor(_quiet_child)
        sup._children = {"dead": _FakeChild(9)}
        with caplog.at_level(logging.CRITICAL, logger="pyline.core.supervisor"):
            sup.start_child_watch(None)
            assert sup._watch_task is not None
            await asyncio.wait_for(_task_done(sup._watch_task), 5.0)
            # done-callbacks run on the next loop tick after completion
            await asyncio.sleep(0.05)
        assert any("child watch task died" in r.message for r in caplog.records)


async def _task_done(task: asyncio.Task[None]) -> None:
    while not task.done():
        await asyncio.sleep(0.01)
    await asyncio.sleep(0)  # let the done-callbacks run


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
