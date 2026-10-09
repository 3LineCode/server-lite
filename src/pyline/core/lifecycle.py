"""Boot state machine and shutdown sequencing.

Ten states, mirroring the validated prototype flow::

    PREPARE -> LOOP_INIT -> FRAME_INIT -> CONN_DB -> BASE_INIT
            -> FUNC_INIT -> FUNC_DONE -> OPEN_LOGIN -> FINISHED -> QUIT

Improvements over the prototype:

* States are an explicit enum with a declared transition table; illegal
  transitions raise instead of silently no-op'ing.
* ``add_start_wait`` gates have a startup watchdog: if the sequence stalls
  longer than ``startup_timeout`` seconds the process aborts, naming the
  pending wait flags (the prototype could hang forever with no diagnostics).
* Shutdown tracks quit tasks with a hard deadline, then force-exits.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import logging
import time
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


class LifecycleState(enum.Enum):
    PREPARE = enum.auto()
    LOOP_INIT = enum.auto()
    FRAME_INIT = enum.auto()
    CONN_DB = enum.auto()
    BASE_INIT = enum.auto()
    FUNC_INIT = enum.auto()
    FUNC_DONE = enum.auto()
    OPEN_LOGIN = enum.auto()
    FINISHED = enum.auto()
    QUIT = enum.auto()


_TRANSITIONS: dict[LifecycleState, LifecycleState] = {
    LifecycleState.PREPARE: LifecycleState.LOOP_INIT,
    LifecycleState.LOOP_INIT: LifecycleState.FRAME_INIT,
    LifecycleState.FRAME_INIT: LifecycleState.CONN_DB,
    LifecycleState.CONN_DB: LifecycleState.BASE_INIT,
    LifecycleState.BASE_INIT: LifecycleState.FUNC_INIT,
    LifecycleState.FUNC_INIT: LifecycleState.FUNC_DONE,
    LifecycleState.FUNC_DONE: LifecycleState.OPEN_LOGIN,
    LifecycleState.OPEN_LOGIN: LifecycleState.FINISHED,
    LifecycleState.FINISHED: LifecycleState.QUIT,
}

#: Steps of the boot sequence that run before steady state.
BOOT_STEPS: tuple[LifecycleState, ...] = (
    LifecycleState.LOOP_INIT,
    LifecycleState.FRAME_INIT,
    LifecycleState.CONN_DB,
    LifecycleState.BASE_INIT,
    LifecycleState.FUNC_INIT,
    LifecycleState.FUNC_DONE,
    LifecycleState.OPEN_LOGIN,
    LifecycleState.FINISHED,
)


class StartupStuckError(RuntimeError):
    """Boot sequence stalled; pending wait flags are attached."""


class LifecycleManager:
    """Owns the boot sequence, start gates and shutdown sequencing.

    ``step_timeout`` bounds each boot step ACTION (the watchdog can only
    observe a stalled sequence; without this a hung network connect parks
    the process forever). ``None`` disables the bound.
    """

    def __init__(
        self,
        *,
        startup_timeout: float = 300.0,
        quit_timeout: float = 30.0,
        step_timeout: float | None = None,
    ) -> None:
        self.state = LifecycleState.PREPARE
        self._startup_timeout = startup_timeout
        self._quit_timeout = quit_timeout
        self._step_timeout = step_timeout
        self._pending_waits: dict[str, float] = {}
        self._start_tasks: set[asyncio.Task[object]] = set()
        self._quit_tasks: set[asyncio.Task[object]] = set()
        self._step_actions: dict[LifecycleState, Callable[[], Awaitable[None]]] = {}
        self._last_advance = time.monotonic()
        self._watchdog_task: asyncio.Task[None] | None = None
        self._shutdown_hooks: list[Callable[[], Awaitable[None]]] = []
        self._shutdown_requested = False
        # F-58: resolved only after request_shutdown() has fully finished
        # (hooks + quit-task drain), so the parking loop can join a
        # fire-and-forget teardown instead of racing it.
        self._shutdown_done: asyncio.Future[None] | None = None
        self.stuck_error: StartupStuckError | None = None

    # ------------------------------------------------------------------ #
    # Boot sequencing
    # ------------------------------------------------------------------ #

    def on_step(self, state: LifecycleState, action: Callable[[], Awaitable[None]]) -> None:
        """Register the action executed when the boot sequence enters ``state``."""
        if state not in BOOT_STEPS:
            raise ValueError(f"cannot register action for state {state}")
        self._step_actions[state] = action

    def is_started(self) -> bool:
        return self.state == LifecycleState.FINISHED

    def in_quit(self) -> bool:
        return self.state == LifecycleState.QUIT

    def start_watchdog(self) -> None:
        if self._watchdog_task is None:
            self._watchdog_task = asyncio.get_running_loop().create_task(self._watchdog())

    async def _cancel_watchdog(self) -> None:
        """Stop the watchdog on a boot-failure exit.

        The old failure paths nulled the reference without cancelling: the
        watchdog only exits on FINISHED/QUIT, so a host that catches
        ``StartupStuckError`` and keeps its loop alive (tests, embedding)
        leaked a task spinning at 20 Hz forever."""
        task = self._watchdog_task
        self._watchdog_task = None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _watchdog(self) -> None:
        while self.state != LifecycleState.FINISHED and self.state != LifecycleState.QUIT:
            await asyncio.sleep(0.05)
            if self.state in (LifecycleState.FINISHED, LifecycleState.QUIT):
                return
            stuck_for = time.monotonic() - self._last_advance
            if stuck_for > self._startup_timeout:
                pending = ", ".join(f"{flag} ({stuck_for:.0f}s)" for flag in self._pending_waits)
                running = len(self._start_tasks)
                self.stuck_error = StartupStuckError(
                    f"startup stuck {stuck_for:.0f}s in {self.state.name}; "
                    f"pending waits: [{pending}]; running start tasks: {running}"
                )
                logger.error("startup watchdog fired: %s", self.stuck_error)
                return

    async def run_boot(self) -> None:
        """Drive the sequence through all boot steps, honouring gates."""
        self.start_watchdog()
        try:
            completed = await self._run_boot_steps()
        except BaseException:
            # Any failure exit (stuck watchdog, step timeout, hook exception)
            # must stop the watchdog: it only self-exits at FINISHED/QUIT, so
            # a host that catches the error and keeps its loop alive (tests,
            # embedding) would otherwise leak a task spinning at 20 Hz.
            await self._cancel_watchdog()
            raise
        if self.stuck_error is not None:
            await self._cancel_watchdog()
            raise self.stuck_error
        if completed:
            logger.info("startup finished: %s", self.state.name)

    async def _run_boot_steps(self) -> bool:
        for state in BOOT_STEPS:
            if self.in_quit():
                # Same F-19 semantics, covering the window where the shutdown
                # lands between two steps (or before the first one): _enter
                # would otherwise face QUIT, which is deliberately absent
                # from _TRANSITIONS, and die with a bare KeyError instead of
                # tearing down. in_quit() rather than a bare comparison --
                # another task may flip the state at any await point.
                logger.warning("boot aborted by shutdown request before %s", state.name)
                return False
            await self._enter(state)
            while True:
                if self.stuck_error is not None:
                    raise self.stuck_error
                if self.state == LifecycleState.QUIT:
                    # F-19: shutdown requested mid-boot -- stop gating, cancel
                    # the remaining start tasks and let teardown run (this
                    # loop previously never exited and hung the process).
                    for task in list(self._start_tasks):
                        task.cancel()
                    self._pending_waits.clear()
                    logger.warning("boot aborted by shutdown request in %s", state.name)
                    return False
                if not self._pending_waits and not self._start_tasks:
                    break
                await asyncio.sleep(0.05)
        return True

    async def _enter(self, state: LifecycleState) -> None:
        if self.state is LifecycleState.QUIT:
            # QUIT is reachable from any state and intentionally not in the
            # transition table; raising the table's KeyError here would crash
            # the boot task with no diagnostics (run_boot checks for this
            # first, this guard covers direct callers).
            raise RuntimeError(f"lifecycle already QUIT; cannot enter {state.name}")
        expected = _TRANSITIONS[self.state]
        if state is not expected:
            raise RuntimeError(f"illegal lifecycle transition {self.state.name} -> {state.name}")
        self.state = state
        self._last_advance = time.monotonic()
        action = self._step_actions.get(state)
        if action is not None:
            from pyline.obs.metrics import get_metrics

            started = time.monotonic()
            try:
                if self._step_timeout is None:
                    await action()
                    return
                try:
                    await asyncio.wait_for(action(), timeout=self._step_timeout)
                except TimeoutError as exc:
                    # The watchdog only records a stall and run_boot can only see
                    # it BETWEEN steps; a hung action (e.g. a connect without its
                    # own timeout) needs a hard bound or the process parks forever.
                    raise StartupStuckError(
                        f"boot step {state.name} exceeded {self._step_timeout:.0f}s"
                    ) from exc
            finally:
                # Which boot phase ate the time used to be log-only; a slow
                # step (schema migration, business init) is now a Prometheus
                # histogram sample too.
                get_metrics().boot_phase_seconds.labels(phase=state.name).observe(
                    time.monotonic() - started
                )

    # ------------------------------------------------------------------ #
    # Start gates
    # ------------------------------------------------------------------ #

    def track_start_task(self, task: asyncio.Task[object]) -> asyncio.Task[object]:
        """Track a startup task; the sequence waits for all of them."""
        self._start_tasks.add(task)
        task.add_done_callback(self._start_task_done)
        return task

    def _start_task_done(self, task: asyncio.Task[object]) -> None:
        self._start_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            # F-19: a failed startup task aborts the boot -- the runner sees
            # the failure instead of a "successfully" started half-server
            # (the comment used to promise this but the code only logged).
            if self.stuck_error is None:
                self.stuck_error = StartupStuckError(
                    f"start task failed during {self.state.name}: {exc!r}"
                )
            logger.error("start task failed: %r", task)

    def add_start_wait(self, flag: str) -> Callable[[], None]:
        """Block the boot sequence until the returned release() is called."""
        if self.state == LifecycleState.FINISHED:
            raise RuntimeError("cannot add start wait after startup finished")
        if flag in self._pending_waits:
            raise ValueError(f"start wait <{flag}> already exists")
        self._pending_waits[flag] = time.monotonic()

        def release() -> None:
            self._pending_waits.pop(flag, None)

        return release

    def pending_waits(self) -> list[str]:
        return list(self._pending_waits)

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #

    def on_shutdown(self, hook: Callable[[], Awaitable[None]]) -> None:
        self._shutdown_hooks.append(hook)

    def track_quit_task(self, task: asyncio.Task[object]) -> asyncio.Task[object]:
        self._quit_tasks.add(task)
        task.add_done_callback(self._quit_tasks.discard)
        return task

    async def request_shutdown(self, reason: str) -> None:
        """Begin graceful shutdown: run hooks and quit tasks under a deadline.

        QUIT is the ONE transition allowed from any state (a half-booted
        server must still be able to tear down); that is why it bypasses
        ``_TRANSITIONS`` -- the table models the linear boot chain only.

        The state flips to QUIT *before* the hooks run so a half-booted
        server can always tear down; callers that park on ``in_quit()`` must
        then join the teardown via ``wait_shutdown_complete()`` before
        returning, or their own cleanup (e.g. asyncio.run's task
        cancellation) will cut the hooks -- flushes included -- mid-flight.
        """
        if self._shutdown_requested:
            return
        self._shutdown_requested = True
        self._shutdown_done = asyncio.get_running_loop().create_future()
        logger.info("shutdown requested: %s", reason)
        self.state = LifecycleState.QUIT
        try:
            for hook in self._shutdown_hooks:
                try:
                    await hook()
                except Exception:
                    logger.exception("shutdown hook %r failed", hook)
            if self._quit_tasks:
                logger.info("waiting for %d quit task(s)...", len(self._quit_tasks))
                done, pending = await asyncio.wait(
                    set(self._quit_tasks), timeout=self._quit_timeout
                )
                for task in pending:
                    task.cancel()
                    logger.warning("quit task cancelled after deadline: %r", task)
                for task in done:
                    if not task.cancelled() and task.exception() is not None:
                        logger.error("quit task failed: %r", task)
        finally:
            # F-58: resolve even if a hook raised BaseException or the task
            # itself was cancelled -- wait_shutdown_complete() must never hang.
            if not self._shutdown_done.done():
                self._shutdown_done.set_result(None)

    async def wait_shutdown_complete(self) -> None:
        """Wait until a started request_shutdown() has fully finished.

        No-op when shutdown was never requested (the caller then owns calling
        ``request_shutdown`` itself)."""
        if self._shutdown_done is not None:
            await self._shutdown_done
