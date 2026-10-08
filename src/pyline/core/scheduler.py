"""Unified timer scheduler: short delays on the event loop, long delays on a
1-second timing wheel driven by a single tick task.

Merges the prototype's two overlapping components (per-task ``asyncio.sleep``
Timer and the 5s-granularity TimeWheel) into one API with one implementation
strategy chosen by delay size:

* ``delay <= SHORT_DELAY`` (default 2s) -> ``loop.call_later`` (precise);
* ``delay > SHORT_DELAY``               -> wheel bucket keyed by second
  (cheap to hold hundreds of thousands of pending timers).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

SHORT_DELAY = 2.0
_TICK = 1.0


@dataclass(slots=True)
class _WheelEntry:
    seq: int
    deadline: float
    bucket: int
    func: Callable[..., object]
    args: tuple[object, ...]
    label: str


class TimerHandle:
    """Cancellation handle returned by every scheduling call."""

    __slots__ = ("_cancel_fn", "_cancelled")

    def __init__(self, cancel_fn: Callable[[], None]) -> None:
        self._cancelled = False
        self._cancel_fn = cancel_fn

    def cancel(self) -> None:
        if not self._cancelled:
            self._cancelled = True
            self._cancel_fn()

    @property
    def cancelled(self) -> bool:
        return self._cancelled


class Scheduler:
    def __init__(self, *, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._loop = loop
        self._wheel: dict[int, list[_WheelEntry]] = {}
        self._entries: dict[int, _WheelEntry] = {}
        self._seq = itertools.count(1)
        self._tick_task: asyncio.Task[None] | None = None
        self._closed = False

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("scheduler already bound to a different loop")
        self._loop = loop

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_event_loop_policy().get_event_loop()
        return self._loop

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def call_after(
        self, delay: float, func: Callable[..., object], *args: object, label: str = ""
    ) -> TimerHandle:
        if self._closed:
            raise RuntimeError("scheduler is closed")
        delay = max(0.0, delay)
        deadline = time.monotonic() + delay
        if delay <= SHORT_DELAY:
            return self._call_later(delay, func, args, label)
        return self._schedule_wheel(deadline, func, args, label)

    def call_repeating(
        self, interval: float, func: Callable[..., object], *args: object, label: str = ""
    ) -> TimerHandle:
        """Repeat ``func`` every ``interval`` seconds until cancelled."""
        stopped = {"v": False}

        def run_once() -> None:
            if stopped["v"]:
                return
            try:
                func(*args)
            except Exception:
                logger.exception("repeating timer %r failed", label or func)
            if not stopped["v"]:
                self.call_after(interval, run_once, label=label)

        self.call_after(interval, run_once, label=label)
        return TimerHandle(lambda: stopped.__setitem__("v", True))

    def soon(self, func: Callable[..., object], *args: object) -> None:
        self.loop.call_soon(func, *args)

    def pending_count(self) -> int:
        return len(self._entries)

    async def close(self) -> None:
        self._closed = True
        if self._tick_task is not None:
            self._tick_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tick_task
            self._tick_task = None
        self._wheel.clear()
        self._entries.clear()

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _call_later(
        self,
        delay: float,
        func: Callable[..., object],
        args: tuple[object, ...],
        label: str,
    ) -> TimerHandle:
        loop_timer = self.loop.call_later(delay, self._fire, func, args, label)
        return TimerHandle(loop_timer.cancel)

    def _fire(self, func: Callable[..., object], args: tuple[object, ...], label: str) -> None:
        try:
            result = func(*args)
            if asyncio.iscoroutine(result):
                self.loop.create_task(result)
        except Exception:
            logger.exception("timer %r failed", label or func)

    def _schedule_wheel(
        self,
        deadline: float,
        func: Callable[..., object],
        args: tuple[object, ...],
        label: str,
    ) -> TimerHandle:
        if self._tick_task is None:
            self._tick_task = self.loop.create_task(self._tick_loop())
        # Ceil: never fire early. An entry lands in the first tick bucket
        # whose time is >= its deadline.
        bucket = math.ceil(deadline / _TICK)
        entry = _WheelEntry(
            seq=next(self._seq),
            deadline=deadline,
            bucket=bucket,
            func=func,
            args=args,
            label=label,
        )
        self._wheel.setdefault(bucket, []).append(entry)
        self._entries[entry.seq] = entry
        return TimerHandle(lambda: self._remove(entry))

    def _remove(self, entry: _WheelEntry) -> None:
        if self._entries.pop(entry.seq, None) is not None:
            entries = self._wheel.get(entry.bucket)
            if entries and entry in entries:
                entries.remove(entry)
                if not entries:
                    self._wheel.pop(entry.bucket, None)

    async def _tick_loop(self) -> None:
        while not self._closed:
            now_bucket = math.floor(time.monotonic() / _TICK)
            for bucket in [b for b in self._wheel if b <= now_bucket]:
                entries = self._wheel.pop(bucket, [])
                for entry in entries:
                    self._entries.pop(entry.seq, None)
                    self._fire(entry.func, entry.args, entry.label)
            await asyncio.sleep(_TICK)
