"""Auto-save scheduler: batched dirty flushing with unbounded retry.

Dirty savers are never dropped (migration plan F-01/F-02): a failed flush
stays queued and is retried with exponential backoff (``cooldown * 2**(n-1)``,
capped at ``retry_cap``).  Shutdown draining retries within a bounded
deadline and reports every saver still dirty when the deadline passes, so
data at risk is named in the logs instead of silently lost.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from pyline.db.orm import DataSaver
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

AlarmCallback = Callable[[str, dict[str, Any]], None]


class SaveScheduler:
    """Batched flusher for dirty :class:`DataSaver` objects.

    Membership in ``_dirty`` means "has unflushed data"; ``_deferred`` only
    gates *when* a dirty saver is retried.  Re-marking an already-queued
    saver does not shorten its backoff: while the database is down the
    queue must not turn into a retry storm.
    """

    def __init__(
        self,
        *,
        interval: float = 5.0,
        batch_size: int = 50,
        retry_cooldown: float = 15.0,
        retry_cap: float = 300.0,
        alarm_threshold: int = 3,
        shutdown_flush_timeout: float = 60.0,
        on_alarm: AlarmCallback | None = None,
    ) -> None:
        self._interval = interval
        self._batch_size = batch_size
        self._retry_cooldown = retry_cooldown
        self._retry_cap = retry_cap
        self._alarm_threshold = alarm_threshold
        self._shutdown_flush_timeout = shutdown_flush_timeout
        self._on_alarm = on_alarm
        self._dirty: dict[DataSaver, int] = {}  # saver -> consecutive failures
        self._deferred: dict[DataSaver, float] = {}  # saver -> retry-not-before
        self._inflight: set[DataSaver] = set()
        self._task: asyncio.Task[None] | None = None
        self._quitting = False
        # metrics / stats
        self.saved_total = 0
        self.failed_total = 0
        self._metrics = get_metrics()

    # ------------------------------ alarms ------------------------------ #

    def _alarm(self, kind: str, payload: dict[str, Any]) -> None:
        if self._on_alarm is None:
            return
        try:
            self._on_alarm(kind, payload)
        except Exception:
            logger.exception("save alarm callback failed (%s)", kind)

    def _sync_metrics(self) -> None:
        self._metrics.save_queue.set(self.queue_depth())

    # ------------------------------ queue ------------------------------- #

    def mark(self, saver: DataSaver) -> None:
        if self._quitting:
            raise OSError(f"cannot mark {saver!r} dirty while quitting")
        self._dirty.setdefault(saver, 0)
        self._sync_metrics()

    def queue_depth(self) -> int:
        return len(self._dirty)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self, *, timeout: float | None = None) -> bool:
        """Stop the loop, then bounded-retry drain of everything dirty.

        Returns True when the queue fully drained.  False means the deadline
        passed with dirty savers remaining -- each one is logged CRITICAL
        and callers should treat the result as data at risk (the runtime
        exits non-zero).
        """
        self._quitting = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        return await self.flush_all(timeout=timeout)

    # ------------------------------ loop -------------------------------- #

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self.flush_batch()

    def _next_due(self, now: float) -> DataSaver | None:
        for saver in self._dirty:
            due = self._deferred.get(saver)
            if due is None or due <= now:
                return saver
        return None

    async def flush_batch(self) -> None:
        for _ in range(self._batch_size):
            saver = self._next_due(time.monotonic())
            if saver is None:
                break
            failures = self._dirty.pop(saver)
            self._deferred.pop(saver, None)
            self._inflight.add(saver)
            try:
                await self._flush_one(saver, failures)
            except asyncio.CancelledError:
                # Cancellation mid-flush (shutdown racing the loop task):
                # the upsert may not have landed, so requeue for flush_all.
                self._dirty.setdefault(saver, failures)
                raise
            finally:
                self._inflight.discard(saver)
        self._sync_metrics()

    async def _flush_one(self, saver: DataSaver, failures: int) -> None:
        try:
            await saver.flush()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.failed_total += 1
            self._metrics.save_failures.inc()
            failures += 1
            self._dirty[saver] = failures
            backoff = min(self._retry_cooldown * 2 ** (failures - 1), self._retry_cap)
            self._deferred[saver] = time.monotonic() + backoff
            logger.error(
                "saver %r flush failed (attempt %d); retrying in %.0fs",
                saver,
                failures,
                backoff,
            )
            if failures >= self._alarm_threshold:
                self._alarm("save_retry", {"saver": repr(saver), "failures": failures})
        else:
            self.saved_total += 1
            self._metrics.save_flushed.inc()

    # ---------------------------- shutdown ------------------------------ #

    async def flush_all(self, *, timeout: float | None = None) -> bool:
        """Drain every dirty saver, retrying within a bounded deadline."""
        limit = self._shutdown_flush_timeout if timeout is None else timeout
        deadline = time.monotonic() + limit
        self._deferred.clear()
        while self._dirty:
            saver, failures = next(iter(self._dirty.items()))
            try:
                await saver.flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.failed_total += 1
                self._metrics.save_failures.inc()
                # F-33: delete-then-reinsert moves the saver to the tail.  A
                # dict assignment on an existing key keeps its head position,
                # so one persistently failing saver owned the queue head and
                # starved every other dirty saver out of the whole deadline.
                del self._dirty[saver]
                self._dirty[saver] = failures + 1
                logger.exception("shutdown flush failed for %r (attempt %d)", saver, failures + 1)
                if time.monotonic() >= deadline:
                    self._report_unflushed()
                    return False
                await asyncio.sleep(min(0.5, max(0.0, deadline - time.monotonic())))
            else:
                self._dirty.pop(saver)
                self.saved_total += 1
                self._metrics.save_flushed.inc()
        self._sync_metrics()
        logger.info("all pending saves flushed (saved=%d)", self.saved_total)
        return True

    def _report_unflushed(self) -> None:
        for saver, failures in self._dirty.items():
            logger.critical("UNFLUSHED DATA AT SHUTDOWN: %r (attempts=%d)", saver, failures)
        logger.critical(
            "shutdown flush deadline exceeded; %d saver(s) still dirty -- data loss possible",
            len(self._dirty),
        )
        self._alarm("save_unflushed", {"count": len(self._dirty)})
