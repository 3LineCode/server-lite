"""Process supervision: spawn sub-processes, mutual liveness watching,
graceful teardown -- without psutil (stdlib multiprocessing only).

Roles:

* The main process spawns one child per ``entry.sub_process`` and watches
  them; a dead child aborts the server (fail-fast, same as prototype).
* Each child polls ``multiprocessing.parent_process().is_alive()`` and shuts
  itself down when the parent dies (no orphaned game processes).
* Shutdown order: main finishes its own lifecycle teardown, then terminates
  children with a deadline, then kills survivors.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import multiprocessing as mp
import os
from collections.abc import Awaitable, Callable
from multiprocessing.synchronize import Event as MpEvent

logger = logging.getLogger(__name__)

ChildMain = Callable[[str, int], Awaitable[None]]


class ProcessSupervisor:
    def __init__(self, child_main: ChildMain) -> None:
        self._child_main = child_main
        self._children: dict[str, mp.process.BaseProcess] = {}
        self._watch_task: asyncio.Task[None] | None = None
        self._shutdown_event: MpEvent | None = None

    # ------------------------------------------------------------------ #
    # Main-process side
    # ------------------------------------------------------------------ #

    def spawn_subprocesses(self, sub_process: tuple[str, ...], main_pid: int) -> None:
        ctx = mp.get_context("spawn")
        self._shutdown_event = ctx.Event()
        for index, process_type in enumerate(sub_process, start=1):
            child = ctx.Process(
                target=_child_entry,
                name=f"pyline-{process_type}",
                args=(self._child_main, process_type, index, main_pid, self._shutdown_event),
                daemon=False,
            )
            child.start()
            self._children[process_type] = child
            logger.info("spawned sub-process %s (index=%d, pid=%d)", process_type, index, child.pid)

    def start_child_watch(
        self, on_child_died: Callable[[str, int | None], Awaitable[None]] | None = None
    ) -> None:
        """Watch children; on death, notify ``on_child_died`` (type, exitcode)."""
        self._watch_task = asyncio.get_running_loop().create_task(
            self._watch_children(on_child_died)
        )

    async def _watch_children(
        self, on_child_died: Callable[[str, int | None], Awaitable[None]] | None
    ) -> None:
        while self._children:
            for process_type, child in list(self._children.items()):
                if not child.is_alive():
                    code = child.exitcode
                    logger.fatal("sub-process %s died (exitcode=%s)", process_type, code)
                    if on_child_died is not None:
                        await on_child_died(process_type, code)
                    else:
                        for other in self._children.values():
                            if other.is_alive():
                                other.terminate()
                        raise ChildDiedError(process_type, code)
                    return
            await asyncio.sleep(1.0)

    async def terminate_children(self, *, grace: float = 10.0) -> None:
        """Signal children to stop, wait ``grace`` seconds, then kill."""
        if self._shutdown_event is not None:
            self._shutdown_event.set()
        children = [c for c in self._children.values() if c.is_alive()]
        if not children:
            return
        deadline = asyncio.get_running_loop().time() + grace
        for child in children:
            child.join(timeout=0)  # non-blocking probe; real wait below
        while children and asyncio.get_running_loop().time() < deadline:
            children = [c for c in children if c.is_alive()]
            if not children:
                break
            await asyncio.sleep(0.2)
        for child in children:
            if child.is_alive():
                logger.warning("killing unresponsive child pid=%d", child.pid)
                child.kill()
        # async join: the old blocking child.join(timeout=2.0) stalled the
        # event loop up to 2s per child during shutdown (F-21)
        join_deadline = asyncio.get_running_loop().time() + 3.0
        for child in self._children.values():
            while child.is_alive() and asyncio.get_running_loop().time() < join_deadline:
                await asyncio.sleep(0.05)
            if child.is_alive():
                child.join(timeout=0)  # non-blocking reap after kill
        self._children.clear()

    # ------------------------------------------------------------------ #
    # Child-process side
    # ------------------------------------------------------------------ #

    async def watch_parent(self, main_pid: int, on_parent_gone: Callable[[], None]) -> None:
        """Run inside children: exit when the parent process disappears."""
        parent = mp.parent_process()
        while True:
            alive = parent.is_alive() if parent is not None else _pid_alive(main_pid)
            if not alive:
                logger.warning("parent process %d is gone; shutting down", main_pid)
                on_parent_gone()
                return
            await asyncio.sleep(1.0)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False
    return True


def _child_entry(
    child_main: ChildMain,
    process_type: str,
    index: int,
    main_pid: int,
    shutdown_event: MpEvent,
) -> None:
    """Trampoline executed inside the spawned child process."""
    from pyline.net.loop_policy import install_loop_policy

    install_loop_policy()
    asyncio.run(_child_loop(child_main, process_type, index, main_pid, shutdown_event))


async def _child_loop(
    child_main: ChildMain,
    process_type: str,
    index: int,
    main_pid: int,
    shutdown_event: MpEvent,
) -> None:
    stop = asyncio.Event()

    def parent_gone() -> None:
        stop.set()

    watcher = asyncio.get_running_loop().create_task(
        ProcessSupervisor(child_main).watch_parent(main_pid, parent_gone)
    )
    signal_task = asyncio.get_running_loop().create_task(_wait_shutdown_event(shutdown_event, stop))
    runner: asyncio.Task[None] = asyncio.ensure_future(child_main(process_type, index))
    stop_wait: asyncio.Task[None] = asyncio.ensure_future(_wait_stop(stop))
    try:
        await asyncio.wait({runner, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
        if stop.is_set():
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
        else:
            await runner
    finally:
        watcher.cancel()
        signal_task.cancel()
        stop_wait.cancel()


async def _wait_stop(stop: asyncio.Event) -> None:
    await stop.wait()


async def _wait_shutdown_event(event: MpEvent, stop: asyncio.Event) -> None:
    """Bridge the mp Event (set by the parent) into the child loop."""
    while not event.is_set():
        await asyncio.sleep(0.5)
    stop.set()


class ChildDiedError(RuntimeError):
    def __init__(self, process_type: str, exitcode: int | None) -> None:
        super().__init__(f"sub-process {process_type!r} died, exitcode={exitcode}")
        self.process_type = process_type
        self.exitcode = exitcode
