"""Task facade (old CreateTask/CreateStartTask/AddStartWait/QuitTask)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from pyline import api

logger = logging.getLogger(__name__)

_bg: set[asyncio.Task[object]] = set()


def _bg_done(task: asyncio.Task[object]) -> None:
    _bg.discard(task)
    if not task.cancelled() and task.exception() is not None:
        # Same discipline as the scheduler: a fire-and-forget failure must be
        # observed, not discovered as "exception was never retrieved" at GC.
        logger.error("spawned task failed: %r", task, exc_info=task.exception())


def spawn(coro: Coroutine[Any, Any, object], *, name: str = "") -> asyncio.Task[object]:
    """Fire-and-forget task with a kept reference (no GC of pending work)."""
    task = asyncio.get_running_loop().create_task(coro, name=name or None)
    _bg.add(task)
    task.add_done_callback(_bg_done)
    return task


def start_task(coro: Coroutine[Any, Any, object]) -> asyncio.Task[object]:
    """Startup task: the boot sequence waits for it (failures abort boot)."""
    lifecycle = api.ctx().lifecycle
    if lifecycle is None:
        raise RuntimeError("lifecycle not initialised")
    task = asyncio.get_running_loop().create_task(coro)
    return lifecycle.track_start_task(task)


def add_start_wait(flag: str) -> Callable[[], None]:
    """Block boot until the returned release() is called."""
    lifecycle = api.ctx().lifecycle
    if lifecycle is None:
        raise RuntimeError("lifecycle not initialised")
    return lifecycle.add_start_wait(flag)


async def on_quit(coro: Coroutine[Any, Any, object]) -> asyncio.Task[object]:
    """Task tracked through shutdown (old QuitTask semantics)."""
    task = asyncio.get_running_loop().create_task(coro)
    lifecycle = api.ctx().lifecycle
    if lifecycle is not None:
        return lifecycle.track_quit_task(task)
    # F-85: without the lifecycle wired (early boot, unit tests) the task
    # had NO strong reference -- a pending task can be garbage-collected
    # before it ever runs, the exact hazard spawn() guards against. Reuse
    # spawn's discipline: background set + done-callback that also observes
    # failures.
    _bg.add(task)
    task.add_done_callback(_bg_done)
    return task
