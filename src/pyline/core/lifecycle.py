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
    """Owns the boot sequence, start gates and shutdown sequencing."""

    def __init__(self, *, startup_timeout: float = 300.0, quit_timeout: float = 30.0) -> None:
        self.state = LifecycleState.PREPARE
        self._startup_timeout = startup_timeout
        self._quit_timeout = quit_timeout
        self._pending_waits: dict[str, float] = {}
        self._start_tasks: set[asyncio.Task[object]] = set()
        self._quit_tasks: set[asyncio.Task[object]] = set()
        self._step_actions: dict[LifecycleState, Callable[[], Awaitable[None]]] = {}
        self._last_advance = time.monotonic()
        self._watchdog_task: asyncio.Task[None] | None = None
        self._shutdown_hooks: list[Callable[[], Awaitable[None]]] = []
        self._shutdown_requested = False
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
        for state in BOOT_STEPS:
            await self._enter(state)
            while self._pending_waits or self._start_tasks:
                if self.stuck_error is not None:
                    self._watchdog_task = None
                    raise self.stuck_error
                await asyncio.sleep(0.05)
        logger.info("startup finished: %s", self.state.name)

    async def _enter(self, state: LifecycleState) -> None:
        expected = _TRANSITIONS[self.state]
        if state is not expected:
            raise RuntimeError(f"illegal lifecycle transition {self.state.name} -> {state.name}")
        self.state = state
        self._last_advance = time.monotonic()
        action = self._step_actions.get(state)
        if action is not None:
            await action()

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
        if not task.cancelled() and task.exception() is not None:
            # Startup coroutines fail fast: surface the exception to the runner.
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
        """Begin graceful shutdown: run hooks and quit tasks under a deadline."""
        if self._shutdown_requested:
            return
        self._shutdown_requested = True
        logger.info("shutdown requested: %s", reason)
        self.state = LifecycleState.QUIT
        for hook in self._shutdown_hooks:
            try:
                await hook()
            except Exception:
                logger.exception("shutdown hook %r failed", hook)
        if self._quit_tasks:
            logger.info("waiting for %d quit task(s)...", len(self._quit_tasks))
            done, pending = await asyncio.wait(set(self._quit_tasks), timeout=self._quit_timeout)
            for task in pending:
                task.cancel()
                logger.warning("quit task cancelled after deadline: %r", task)
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    logger.error("quit task failed: %r", task)
