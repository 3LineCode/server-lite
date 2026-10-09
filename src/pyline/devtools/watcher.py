"""Source file watcher: auto hot-reload on change (develop servers only)."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from watchfiles import awatch

from pyline.reload.inplace import ReloadError, reload_module

logger = logging.getLogger(__name__)

# F-96: when the watcher must fall back to the whole cwd, everything that is
# NOT the business tree is excluded -- reloading tests/build/tooling modules
# inside the live server executes their import side effects there.
DEFAULT_IGNORED_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".aiolog",
    "docs",
    "tests",
    "build",
    ".pytest_cache",
    "aioconfig",
}


class FileWatcher:
    def __init__(
        self,
        watch_dirs: list[Path],
        *,
        reload_hook: Callable[[str], object] = reload_module,
        ignored_dirs: set[str] | None = None,
        shutdown_hook: Callable[[str], object] | None = None,
    ) -> None:
        self._dirs = watch_dirs
        self._reload = reload_hook
        self._ignored = ignored_dirs if ignored_dirs is not None else set(DEFAULT_IGNORED_DIRS)
        # F-97: a reload that re-raises SystemExit/KeyboardInterrupt (F-41
        # keeps those escaping ``reload_module`` on purpose) must turn into a
        # graceful shutdown, not a dead watcher task with an unretrieved
        # exception.
        self._shutdown_hook = shutdown_hook
        self._task: asyncio.Task[None] | None = None
        self._reload_tasks: set[asyncio.Task[object]] = set()
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())
            self._task.add_done_callback(self._task_done)

    async def stop(self) -> None:
        # Signal awatch's stop_event so the watch loop exits cleanly; the
        # cancel below is belt-and-braces for a mid-callback stall.
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        # In-flight async reloads (F-91) must not outlive the watcher: the
        # teardown sequence closes the watcher before the save-flush, and a
        # reload landing mid-flush would swap code under the drain.
        for task in self._reload_tasks:
            task.cancel()
        if self._reload_tasks:
            await asyncio.gather(*self._reload_tasks, return_exceptions=True)
        self._reload_tasks.clear()

    async def _run(self) -> None:
        async for changes in awatch(
            *[str(d) for d in self._dirs],
            stop_event=self._stop_event,
            debounce=300,
            step=200,
        ):
            for _, path_str in changes:
                self._handle(Path(path_str))

    def _task_done(self, task: asyncio.Task[None]) -> None:
        """F-97: the watcher task must never die silently.

        If it ends with SystemExit/KeyboardInterrupt despite _handle's
        routing (no shutdown hook configured), re-raise here: an exception
        raised from a done-callback propagates out of ``run_forever`` and
        exits the process -- which is exactly the semantics F-41 chose for
        ``reload_module`` re-raising SystemExit."""
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
        logger.critical("watcher task died unexpectedly: %r", exc)

    def _handle(self, path: Path) -> None:
        if not str(path).endswith(".py"):
            return
        parts = set(path.parts)
        if parts & self._ignored:
            return
        module = self._module_name(path)
        if module is None:
            return
        try:
            # F-91: the production reload hook is async (its PreReload emit
            # must complete before the code swap); bare ``reload_module``
            # stays a legal sync hook, so accept both shapes.
            result = self._reload(module)
            if inspect.isawaitable(result):
                self._spawn_reload(result, module)
        except ReloadError as exc:
            logger.error("auto-reload of %s rejected: %s", module, exc)
        except (SystemExit, KeyboardInterrupt) as exc:
            # F-41 lets the reloaded top level abort the process via
            # SystemExit; F-97 turns that into a graceful shutdown when the
            # runtime provided a hook, otherwise the exception must keep
            # escaping (see _task_done) instead of dying inside this task.
            if self._shutdown_hook is not None:
                logger.warning("auto-reload of %s raised %r; requesting shutdown", module, exc)
                self._shutdown_hook(f"reload of {module} raised {exc!r}")
            else:
                raise
        except Exception:
            logger.exception("auto-reload of %s crashed", module)
        except BaseException:
            # Non-Exception BaseExceptions (e.g. GeneratorExit) used to kill
            # the watcher task with no retrieval; log loud and survive.
            logger.critical(
                "auto-reload of %s raised a non-Exception BaseException (watcher survives)",
                module,
                exc_info=True,
            )

    def _spawn_reload(self, coro: object, module: str) -> None:
        """Run the async half of a reload as a tracked, guarded task
        (F-91/F-97)."""
        task = asyncio.get_running_loop().create_task(self._guard_reload(coro, module))
        self._reload_tasks.add(task)
        task.add_done_callback(self._reload_tasks.discard)
        task.add_done_callback(self._reload_task_done)

    async def _guard_reload(self, coro: object, module: str) -> None:
        """Catch SystemExit/KeyboardInterrupt INSIDE the coroutine.

        asyncio re-raises those two straight out of the event loop (the task
        machinery will not store them), which would exit the process with no
        graceful shutdown at all. Catching them here lets F-97 route them
        into the shutdown hook; with no hook the re-raise keeps the
        interpreter-level "process exits" semantics."""
        try:
            await coro  # type: ignore[misc]
        except (SystemExit, KeyboardInterrupt) as exc:
            if self._shutdown_hook is not None:
                logger.warning("auto-reload of %s raised %r; requesting shutdown", module, exc)
                self._shutdown_hook(f"reload of {module} raised {exc!r}")
            else:
                raise

    def _reload_task_done(self, task: asyncio.Task[object]) -> None:
        """Retrieval/logging for finished reload tasks: a failure dying
        unretrieved inside ``_reload_tasks`` used to surface only as a GC
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
            # Defensive double net (see _guard_reload / _task_done).
            if self._shutdown_hook is not None:
                self._shutdown_hook(f"reload raised {exc!r}")
            else:
                raise exc
        elif isinstance(exc, ReloadError):
            logger.error("auto-reload rejected: %s", exc)
        else:
            logger.error("auto-reload task failed: %r", exc)

    def _module_name(self, path: Path) -> str | None:
        abs_path = path.resolve()
        # The DEEPEST matching sys.path entry wins: with shadowed paths (cwd
        # plus a package root both on sys.path) the shallowest base maps the
        # file onto the wrong module name and triggers a surprise import.
        # Deepest base == shortest relative path, so the comparison keeps the
        # SMALLEST part count (F-57: it kept the largest, which preferred the
        # shallowest base -- the exact mapping the comment forbids).
        best: tuple[int, list[str]] | None = None
        for base in (Path(p).resolve() for p in sys.path if p):
            try:
                relative = abs_path.relative_to(base)
            except ValueError:
                continue
            parts = list(relative.with_suffix("").parts)
            if not parts:
                continue
            if parts[-1] == "__init__":
                parts = parts[:-1]
            if not parts:
                continue
            depth = len(relative.parts)
            if best is None or depth < best[0]:
                best = (depth, parts)
        if best is None:
            return None
        return ".".join(best[1])
