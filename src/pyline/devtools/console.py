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

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

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
            except Exception:
                logger.exception("console command failed: %s", line)

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
                        self._reload_hook(module)
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

    def _eval(self, line: str) -> None:
        try:
            print(eval(line))
        except SyntaxError:
            namespace: dict[str, object] = {}
            exec(line, namespace)
            print(namespace)
