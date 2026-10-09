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
import sys
from collections.abc import Awaitable, Callable
from multiprocessing.synchronize import Event as MpEvent

from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

ChildMain = Callable[[str, int], Awaitable[None]]


def _log_watch_death(task: asyncio.Task[None]) -> None:
    """Done-callback for the fire-and-forget child watch task (F-82)."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.critical("child watch task died: %r", exc, exc_info=exc)


class ProcessSupervisor:
    def __init__(self, child_main: ChildMain) -> None:
        self._child_main = child_main
        self._children: dict[str, mp.process.BaseProcess] = {}
        self._watch_task: asyncio.Task[None] | None = None
        self._shutdown_event: MpEvent | None = None
        self._metrics = get_metrics()

    # ------------------------------------------------------------------ #
    # Main-process side
    # ------------------------------------------------------------------ #

    def spawn_subprocesses(self, sub_process: tuple[str, ...], main_pid: int) -> None:
        # F-81: duplicate process types used to overwrite the ``_children``
        # slot -- BOTH children were spawned but only the last one was
        # watched and terminated, leaving an orphan. Config models keep the
        # tuple permissive (order is meaningful), so the supervisor is the
        # single choke point that rejects duplicates. Validating BEFORE any
        # spawn means no half-started children need reaping on failure.
        if len(set(sub_process)) != len(sub_process):
            duplicates = sorted({t for t in sub_process if sub_process.count(t) > 1})
            raise ValueError(
                f"duplicate sub-process types {duplicates} in sub_process={sub_process}; "
                "each process type may be spawned at most once per server"
            )
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
            self._metrics.children_alive.set(len(self._children))
            logger.info("spawned sub-process %s (index=%d, pid=%d)", process_type, index, child.pid)

    def start_child_watch(
        self,
        on_child_died: Callable[[str, int | None], Awaitable[None]] | None = None,
        *,
        on_unhandled_death: Callable[[str, int | None], Awaitable[None]] | None = None,
    ) -> None:
        """Watch children; on death, notify ``on_child_died`` (type, exitcode).

        ``on_unhandled_death`` (F-160) is the escalation hook for the paths
        the callback cannot cover: no callback registered at all, or the
        callback itself failed. Terminating the siblings used to be the end
        of the fail-fast path -- the ChildDiedError only reached the watch
        task's done-callback log, and the main runtime kept running with its
        db/proxy process gone. The hook (in production: runtime shutdown) is
        invoked BEFORE the raise so the host still gets the failure even
        when the raise dies in this fire-and-forget task.
        """
        self._watch_task = asyncio.get_running_loop().create_task(
            self._watch_children(on_child_died, on_unhandled_death)
        )
        # F-82: the watch task is fire-and-forget. Without this done-callback
        # any exception it raises died unretrieved (only the GC-time
        # "exception was never retrieved" warning, which nobody reads) and
        # every child kept running unmanaged. The callback both retrieves the
        # exception and escalates it to CRITICAL so the failure is visible.
        self._watch_task.add_done_callback(_log_watch_death)

    async def _watch_children(
        self,
        on_child_died: Callable[[str, int | None], Awaitable[None]] | None,
        on_unhandled_death: Callable[[str, int | None], Awaitable[None]] | None = None,
    ) -> None:
        while self._children:
            for process_type, child in list(self._children.items()):
                if not child.is_alive():
                    code = child.exitcode
                    clean = code == 0
                    # F-153: exit 0 during a planned shutdown is the normal
                    # path -- the old logger.fatal (a deprecated alias) made
                    # every clean teardown read like a crash in the logs.
                    if clean:
                        logger.info("sub-process %s exited cleanly", process_type)
                    else:
                        logger.critical("sub-process %s died (exitcode=%s)", process_type, code)
                    # Observable exit: 0 = clean (shutdown path), anything
                    # else = crash; the gauge keeps the live count honest.
                    self._metrics.child_exits.labels(
                        process_type=process_type, reason="clean" if clean else "crash"
                    ).inc()
                    self._children.pop(process_type, None)
                    self._metrics.children_alive.set(len(self._children))
                    if on_child_died is not None:
                        try:
                            await on_child_died(process_type, code)
                        except Exception:
                            # F-82: a buggy callback used to kill the watch
                            # task mid-notification, leaving the remaining
                            # children unmanaged. The callback owns shutdown;
                            # when it fails we cannot trust it to have torn
                            # anything down, so we fall through to the same
                            # fail-fast path as the no-callback branch
                            # (escalate, terminate siblings, raise) instead of
                            # continuing without supervision.
                            logger.critical(
                                "on_child_died callback failed after sub-process %s "
                                "died (exitcode=%s); falling back to fail-fast teardown",
                                process_type,
                                code,
                                exc_info=True,
                            )
                            await self._fail_fast(process_type, code, escalate=on_unhandled_death)
                            return
                        if clean:
                            # F-153: a clean exit inside a planned shutdown
                            # (the callback requested/observed the teardown)
                            # keeps watching -- the siblings exit cleanly too
                            # and each deserves its own log/metric line
                            # instead of one line and silence.
                            continue
                        # The callback owns shutdown for this death.
                        return
                    await self._fail_fast(process_type, code, escalate=on_unhandled_death)
                    return
            await asyncio.sleep(1.0)

    async def _fail_fast(
        self,
        process_type: str,
        code: int | None,
        *,
        escalate: Callable[[str, int | None], Awaitable[None]] | None = None,
    ) -> None:
        """No-callback / callback-failed death handling (F-82, F-160): stop
        the siblings, escalate to the host, then raise.

        There is no runtime object to hand the failure to in this branch, so
        the supervisor guarantees what it can: no child outlives a dead
        sibling (the whole server is one fail-fast unit), and -- F-160 -- the
        escalation hook runs BEFORE the raise, because via ``start_child_watch``
        the ChildDiedError is only retrieved and logged by
        ``_log_watch_death``; without the hook the main runtime used to keep
        running with a dead db/proxy process. It propagates to callers that
        awaited ``_watch_children`` directly (tests, alternative hosts).
        """
        for other in self._children.values():
            if other.is_alive():
                other.terminate()
        if escalate is not None:
            try:
                await escalate(process_type, code)
            except Exception:
                logger.critical(
                    "escalation hook failed after sub-process %s died "
                    "(exitcode=%s); the main process may still be running",
                    process_type,
                    code,
                    exc_info=True,
                )
        raise ChildDiedError(process_type, code)

    async def terminate_children(self, *, grace: float = 10.0) -> None:
        """Signal children to stop, wait ``grace`` seconds, then kill."""
        if self._shutdown_event is not None:
            self._shutdown_event.set()
        children = [c for c in self._children.values() if c.is_alive()]
        if not children:
            self._children.clear()
            self._metrics.children_alive.set(0)
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
        self._metrics.children_alive.set(0)

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
    if sys.platform == "win32":
        # os.kill(pid, 0) on Windows TERMINATES the target process (any
        # non-CTRL signal maps to TerminateProcess); probe via
        # OpenProcess/GetExitCodeProcess instead.
        import ctypes

        windll = getattr(ctypes, "windll", None)
        if windll is None:  # pragma: no cover - non-CPython/broken ctypes
            return True
        kernel32 = windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except PermissionError:
        # F-160: the process EXISTS but is owned by another user; treating
        # this as "dead" made a child suicide on a false negative.
        return True
    except (ProcessLookupError, OSError):
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
