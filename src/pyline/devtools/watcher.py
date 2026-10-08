"""Source file watcher: auto hot-reload on change (develop servers only)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from watchfiles import awatch

from pyline.reload.inplace import ReloadError, reload_module

logger = logging.getLogger(__name__)


class FileWatcher:
    def __init__(
        self,
        watch_dirs: list[Path],
        *,
        reload_hook: Callable[[str], object] = reload_module,
        ignored_dirs: set[str] | None = None,
    ) -> None:
        self._dirs = watch_dirs
        self._reload = reload_hook
        self._ignored = ignored_dirs or {".git", ".venv", "__pycache__", ".aiolog", "docs"}
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        # Signal awatch's stop_event so the watch loop exits cleanly; the
        # cancel below is belt-and-braces for a mid-callback stall.
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        async for changes in awatch(
            *[str(d) for d in self._dirs],
            stop_event=self._stop_event,
            debounce=300,
            step=200,
        ):
            for _, path_str in changes:
                self._handle(Path(path_str))

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
            self._reload(module)
        except ReloadError as exc:
            logger.error("auto-reload of %s rejected: %s", module, exc)
        except Exception:
            logger.exception("auto-reload of %s crashed", module)

    def _module_name(self, path: Path) -> str | None:
        abs_path = path.resolve()
        # Longest matching sys.path entry wins: with shadowed paths (cwd plus
        # a package root both on sys.path) the shortest prefix can map the
        # file onto the wrong module name and trigger a surprise import.
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
            if best is None or depth > best[0]:
                best = (depth, parts)
        if best is None:
            return None
        return ".".join(best[1])
