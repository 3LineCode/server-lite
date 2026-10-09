"""Interactive console (develop servers).

Built-in commands::

    help                 -- list commands
    exit | quit | q      -- graceful shutdown
    kill | stop          -- immediate process kill
    clear | cls          -- clear screen
    update <mod,...>     -- hot-reload modules
    $ <text> | ￥ <text> -- forward to the business ConsoleCommandEvent

Arbitrary ``eval``/``exec`` of typed lines is **disabled by default**
(prototype issue #11); pass ``unsafe=True`` (wired to ``--unsafe-console``)
to enable it, and never enable it outside a development server.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import signal
import sys
from collections.abc import Callable

import aioconsole

from pyline.core.events import ConsoleCommandEvent, EventBus

logger = logging.getLogger(__name__)


class Console:
    def __init__(
        self,
        bus: EventBus,
        *,
        unsafe: bool = False,
        reload_hook: Callable[[str], object] | None = None,
        shutdown_hook: Callable[[str], object] | None = None,
        kill_hook: Callable[[str], object] | None = None,
    ) -> None:
        self._bus = bus
        self._unsafe = unsafe
        self._reload_hook = reload_hook
        self._shutdown_hook = shutdown_hook
        self._kill_hook = kill_hook
        self._task: asyncio.Task[None] | None = None
        self._emit_tasks: set[asyncio.Task[object]] = set()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())
            self._task.add_done_callback(self._console_task_done)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        for task in self._emit_tasks:
            task.cancel()
        if self._emit_tasks:
            await asyncio.gather(*self._emit_tasks, return_exceptions=True)
        self._emit_tasks.clear()

    def _console_task_done(self, task: asyncio.Task[None]) -> None:
        """F-97: the console task must never die silently.

        SystemExit/KeyboardInterrupt that escaped _run's routing (no shutdown
        hook configured) are re-raised from this done-callback: exceptions
        from callbacks propagate out of ``run_forever`` and exit the process,
        preserving F-41's "the reload chain may abort the process" semantics."""
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:  # pragma: no cover - race with cancel
            return
        if exc is None:
            return
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            raise exc
        logger.critical("console task died unexpectedly: %r", exc)

    async def _run(self) -> None:
        logger.info("console ready (unsafe_eval=%s)", self._unsafe)
        while True:
            try:
                line = await aioconsole.ainput()
            except (EOFError, asyncio.CancelledError):
                return
            except Exception:
                logger.exception("console input failed")
                await asyncio.sleep(0.5)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                self.execute(line)
            except (SystemExit, KeyboardInterrupt) as exc:
                # F-97: a command (typically a reload, whose F-41 semantics
                # re-raise SystemExit from the reloaded top level) asks for
                # process exit; route it into the graceful shutdown when the
                # runtime provided a hook, otherwise let it propagate -- the
                # old ``except Exception`` let it kill this task with the
                # exception never retrieved.
                if self._shutdown_hook is not None:
                    logger.warning("console command raised %r; requesting shutdown", exc)
                    self._shutdown_hook(f"console command raised {exc!r}")
                else:
                    raise
            except Exception:
                logger.exception("console command failed: %s", line)
            except BaseException:
                logger.critical(
                    "console command raised a non-Exception BaseException (console survives): %s",
                    line,
                    exc_info=True,
                )

    def execute(self, line: str) -> None:
        command, _, rest = line.partition(" ")
        match command:
            case "help":
                print(
                    "commands: help | exit/quit/q | kill/stop | clear/cls | "
                    "update <mod,...> | $ <business-cmd>"
                    + (" | <python expr> (unsafe)" if self._unsafe else "")
                )
            case "exit" | "quit" | "q":
                if self._shutdown_hook is not None:
                    self._shutdown_hook("console exit")
            case "kill" | "stop" | "k":
                if self._kill_hook is not None:
                    self._kill_hook("console kill")
                else:
                    kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
                    os.kill(os.getpid(), kill_signal)
            case "clear" | "cls":
                os.system("cls" if sys.platform == "win32" else "clear")
            case "update":
                if self._reload_hook is None:
                    print("reload not available in this process")
                    return
                for module in rest.replace(" ", "").split(","):
                    if module:
                        # F-91: the reload hook is async in production (its
                        # PreReload emit must complete before the code swap);
                        # execute() itself stays synchronous, so the coroutine
                        # is scheduled as a guarded, tracked task. Sync hooks
                        # (tests, bare reload_module) keep their old
                        # semantics and are covered by _run's handlers.
                        result = self._reload_hook(module)
                        if inspect.isawaitable(result):
                            task = asyncio.get_running_loop().create_task(
                                self._guard_reload(result, module)
                            )
                            self._emit_tasks.add(task)
                            task.add_done_callback(self._emit_tasks.discard)
                            task.add_done_callback(self._reload_task_done)
            case "$" | "￥":  # fullwidth variant, prototype parity
                task = asyncio.get_running_loop().create_task(
                    self._bus.emit(ConsoleCommandEvent(command=rest))
                )
                # Strong ref: a bare create_task can be GC'd before emitting.
                self._emit_tasks.add(task)
                task.add_done_callback(self._emit_tasks.discard)
            case _:
                if self._unsafe:
                    self._eval(line)
                else:
                    print(
                        f"unknown command {command!r} (eval disabled; start with "
                        "--unsafe-console to enable)"
                    )

    async def _guard_reload(self, coro: object, module: str) -> None:
        """Catch SystemExit/KeyboardInterrupt INSIDE the coroutine (F-97).

        asyncio re-raises those two straight out of the event loop (the task
        machinery will not store them), exiting the process with no graceful
        shutdown. Catching them here routes them into the shutdown hook; with
        no hook the re-raise keeps the interpreter-level "process exits"
        semantics."""
        try:
            await coro  # type: ignore[misc]
        except (SystemExit, KeyboardInterrupt) as exc:
            if self._shutdown_hook is not None:
                logger.warning("console reload of %s raised %r; requesting shutdown", module, exc)
                self._shutdown_hook(f"console reload of {module} raised {exc!r}")
            else:
                raise

    def _reload_task_done(self, task: asyncio.Task[object]) -> None:
        """F-97: retrieve failures of scheduled reload tasks -- they would
        otherwise die unretrieved inside _emit_tasks and only surface as a GC
        warning. (The SystemExit path is normally handled by _guard_reload
        before the exception ever lands on the task.)"""
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:  # pragma: no cover - race with cancel
            return
        if exc is None:
            return
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            # Defensive double net (same policy as _guard_reload).
            if self._shutdown_hook is not None:
                self._shutdown_hook(f"console reload raised {exc!r}")
            else:
                raise exc
        else:
            logger.error("console reload failed: %r", exc)

    def _eval(self, line: str) -> None:
        try:
            print(eval(line))
        except SyntaxError:
            namespace: dict[str, object] = {}
            exec(line, namespace)
            print(namespace)
