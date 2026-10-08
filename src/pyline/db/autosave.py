"""Auto-save scheduler: batched dirty flushing with retry and shutdown flush."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from pyline.db.orm import DataSaver

logger = logging.getLogger(__name__)


class SaveScheduler:
    def __init__(
        self,
        *,
        interval: float = 5.0,
        batch_size: int = 50,
        retry_cooldown: float = 15.0,
        max_attempts: int = 3,
    ) -> None:
        self._interval = interval
        self._batch_size = batch_size
        self._retry_cooldown = retry_cooldown
        self._max_attempts = max_attempts
        self._dirty: dict[DataSaver, int] = {}  # saver -> consecutive failures
        self._deferred: dict[DataSaver, float] = {}  # saver -> retry-not-before
        self._task: asyncio.Task[None] | None = None
        self._quitting = False
        # metrics
        self.saved_total = 0
        self.failed_total = 0
        self.dropped_total = 0

    def mark(self, saver: DataSaver) -> None:
        if self._quitting:
            raise OSError(f"cannot mark {saver!r} dirty while quitting")
        self._dirty[saver] = 0

    def queue_depth(self) -> int:
        return len(self._dirty) + len(self._deferred)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        """Flush everything then stop the loop (shutdown path)."""
        self._quitting = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self.flush_all()

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self.flush_batch()
            now = time.monotonic()
            for saver in [s for s, t in self._deferred.items() if t <= now]:
                self._deferred.pop(saver, None)
                self._dirty[saver] = self._dirty.get(saver, 0)

    async def flush_batch(self) -> None:
        for _ in range(self._batch_size):
            saver = next(iter(self._dirty), None)
            if saver is None:
                break
            failures = self._dirty.pop(saver)
            await self._flush_one(saver, failures)

    async def _flush_one(self, saver: DataSaver, failures: int) -> None:
        try:
            await saver.flush()
            self.saved_total += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            self.failed_total += 1
            failures += 1
            if failures >= self._max_attempts:
                self.dropped_total += 1
                self._deferred.pop(saver, None)  # never resurrect a dropped saver
                logger.critical(
                    "saver %r failed %d times; dropping from save queue "
                    "(total dropped=%d) -- data loss possible!",
                    saver,
                    failures,
                    self.dropped_total,
                )
                return
            self._dirty[saver] = failures
            self._deferred[saver] = time.monotonic() + self._retry_cooldown
            logger.error("saver %r flush failed (attempt %d), deferred", saver, failures)

    async def flush_all(self) -> None:
        """Best-effort flush of everything pending (used on shutdown)."""
        self._deferred.clear()
        while self._dirty:
            saver = next(iter(self._dirty))
            self._dirty.pop(saver)
            try:
                await saver.flush()
                self.saved_total += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                self.failed_total += 1
                logger.exception("shutdown flush failed for %r", saver)
        logger.info("all pending saves flushed (saved=%d)", self.saved_total)
