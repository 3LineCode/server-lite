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
    """Cancellation handle returned by every scheduling call; ``left()``
    exposes the remaining seconds so callers can tell "almost due" from
    "gone" (the prototype's ``left`` always returned 0)."""

    __slots__ = ("_cancel_fn", "_cancelled", "_deadline")

    def __init__(
        self,
        cancel_fn: Callable[[], None],
        deadline: float | Callable[[], float | None] | None = None,
    ) -> None:
        self._cancelled = False
        self._cancel_fn = cancel_fn
        # F-83: a repeating timer's "deadline" moves with every beat, so it
        # is supplied as a callable; one-shot timers pass the fixed float.
        self._deadline = deadline

    def cancel(self) -> None:
        if not self._cancelled:
            self._cancelled = True
            self._cancel_fn()

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def left(self) -> float:
        """Seconds until the scheduled deadline (0.0 when unknown/past).

        For repeating timers this is the distance to the NEXT beat, not the
        first one (F-83: it used to be frozen at the first beat, reporting
        0.0 forever after the first tick)."""
        deadline = self._deadline
        if callable(deadline):
            deadline = deadline()
        if deadline is None:
            return 0.0
        return max(0.0, deadline - time.monotonic())


class Scheduler:
    def __init__(self, *, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._loop = loop
        self._wheel: dict[int, list[_WheelEntry]] = {}
        self._entries: dict[int, _WheelEntry] = {}
        self._seq = itertools.count(1)
        self._tick_task: asyncio.Task[None] | None = None
        self._closed = False
        # F-23: short-path loop timers are tracked so close() can cancel them
        # (they used to keep firing -- and erroring -- after shutdown), and
        # coroutine callbacks keep a strong ref + done-callback.
        self._short_timers: set[asyncio.TimerHandle] = set()
        self._async_tasks: set[asyncio.Task[object]] = set()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("scheduler already bound to a different loop")
        self._loop = loop

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            # Explicit binding only (F-23): the lazy deprecated
            # get_event_loop() fallback explodes when called off-loop and
            # hides missing-bind bugs.
            raise RuntimeError("scheduler is not bound; call bind_loop() inside the loop")
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
            return self._call_later(delay, func, args, label, deadline)
        return self._schedule_wheel(deadline, func, args, label)

    def call_repeating(
        self, interval: float, func: Callable[..., object], *args: object, label: str = ""
    ) -> TimerHandle:
        """Repeat ``func`` every ``interval`` seconds until cancelled.

        Re-arms from the ORIGINAL deadline grid, not the actual fire time:
        a slow callback or a busy loop must not accumulate drift. Missed
        ticks (pause longer than one interval) are skipped, never replayed.
        """
        if interval <= 0:
            raise ValueError(f"repeating interval must be > 0, got {interval}")
        if self._closed:
            raise RuntimeError("scheduler is closed")
        stopped = False
        next_deadline = time.monotonic() + interval
        pending: TimerHandle | None = None

        def run_once() -> None:
            nonlocal stopped, next_deadline, pending
            if stopped:
                return
            try:
                func(*args)
            except Exception:
                logger.exception("repeating timer %r failed", label or func)
            if stopped:
                return
            next_deadline += interval
            now = time.monotonic()
            if next_deadline <= now:
                missed = math.ceil((now - next_deadline) / interval)
                next_deadline += missed * interval
            pending = self.call_after(next_deadline - now, run_once, label=label)

        def cancel() -> None:
            # F-83: cancelling used to only flip the stopped flag -- the
            # pending entry kept occupying the wheel (or a call_later slot)
            # until its deadline came round and run_once no-op'd. Cancel the
            # inner handle too so the timer frees its slot immediately
            # (visible in pending_count()) instead of idling to expiry.
            nonlocal stopped
            stopped = True
            if pending is not None:
                pending.cancel()

        pending = self.call_after(interval, run_once, label=label)
        # F-83: left() must track the NEXT beat, not stay frozen at the
        # first deadline (it read 0.0 forever after the first tick).
        return TimerHandle(cancel, lambda: next_deadline)

    def soon(self, func: Callable[..., object], *args: object) -> None:
        self.loop.call_soon(func, *args)

    def pending_count(self) -> int:
        """Total pending timers: wheel entries + live short-path timers."""
        return len(self._entries) + len(self._short_timers)

    async def close(self) -> None:
        self._closed = True
        if self._tick_task is not None:
            self._tick_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tick_task
            self._tick_task = None
        for timer in self._short_timers:
            timer.cancel()
        self._short_timers.clear()
        self._wheel.clear()
        self._entries.clear()
        # A closed scheduler must not leave coroutine callbacks running
        # against services that teardown closes next (mysql/zmq handles).
        if self._async_tasks:
            for task in self._async_tasks:
                task.cancel()
            # Cancel only requests delivery on the next loop tick; without
            # this join a callback could still run after close() returned,
            # one iteration from touching a handle the next teardown step
            # closes -- the exact hazard the cancel exists for.
            await asyncio.gather(*self._async_tasks, return_exceptions=True)
            self._async_tasks.clear()

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _call_later(
        self,
        delay: float,
        func: Callable[..., object],
        args: tuple[object, ...],
        label: str,
        deadline: float,
    ) -> TimerHandle:
        holder: dict[str, asyncio.TimerHandle | None] = {"h": None}

        def fire() -> None:
            handle = holder["h"]
            if handle is not None:
                self._short_timers.discard(handle)
            self._fire(func, args, label)

        loop_timer = self.loop.call_later(delay, fire)
        holder["h"] = loop_timer
        self._short_timers.add(loop_timer)

        def cancel() -> None:
            loop_timer.cancel()
            self._short_timers.discard(loop_timer)

        return TimerHandle(cancel, deadline)

    def _fire(self, func: Callable[..., object], args: tuple[object, ...], label: str) -> None:
        try:
            result = func(*args)
            if asyncio.iscoroutine(result):
                # F-23: hold the reference and observe the outcome (a bare
                # create_task died with "exception was never retrieved").
                task = self.loop.create_task(result)
                self._async_tasks.add(task)
                task.add_done_callback(self._async_task_done)
        except Exception:
            logger.exception("timer %r failed", label or func)

    def _async_task_done(self, task: asyncio.Task[object]) -> None:
        self._async_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("timer coroutine failed: %r", task)

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
        return TimerHandle(lambda: self._remove(entry), deadline)

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
