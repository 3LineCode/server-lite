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
from pyline.db.transaction import current_flush_journal
from pyline.log.ratelimit import WindowLogLimiter
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

AlarmCallback = Callable[[str, dict[str, Any]], None]

# F-42 coalescing caps: chunk a group's rows so one statement stays well
# inside the 16 MiB RPC frame limit even for large blobs.
_COALESCE_MAX_ROWS = 32
_COALESCE_MAX_BYTES = 4 * 1024 * 1024


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
        queue_alarm_threshold: int = 1000,
        on_alarm: AlarmCallback | None = None,
    ) -> None:
        self._interval = interval
        self._batch_size = batch_size
        self._retry_cooldown = retry_cooldown
        self._retry_cap = retry_cap
        self._alarm_threshold = alarm_threshold
        self._shutdown_flush_timeout = shutdown_flush_timeout
        self._queue_alarm_threshold = queue_alarm_threshold
        self._on_alarm = on_alarm
        self._dirty: dict[DataSaver, int] = {}  # saver -> consecutive failures
        self._deferred: dict[DataSaver, float] = {}  # saver -> retry-not-before
        self._inflight: set[DataSaver] = set()
        self._task: asyncio.Task[None] | None = None
        self._quitting = False
        self._queue_alarm_active = False
        # F-186: visibility for dirty marks accepted during the drain window.
        self._late_marks = 0
        self._late_mark_log = WindowLogLimiter()
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
        self._check_queue_alarm()

    def _check_queue_alarm(self) -> None:
        """F-42: edge-triggered alarm when the dirty queue keeps growing.

        A long database outage turns the never-drop queue into an unbounded
        backlog (strong refs to savers and their blobs) racing the 60 s
        shutdown drain -- ops needs to see that forming, not discover it at
        shutdown.
        """
        depth = self.queue_depth()
        if depth >= self._queue_alarm_threshold:
            if not self._queue_alarm_active:
                self._queue_alarm_active = True
                self._alarm(
                    "save_queue_depth", {"depth": depth, "threshold": self._queue_alarm_threshold}
                )
        else:
            self._queue_alarm_active = False

    # ------------------------------ queue ------------------------------- #

    def mark(self, saver: DataSaver) -> None:
        """F-186: during the shutdown drain a late dirty mark is ACCEPTED.

        The old behaviour raised OSError from ``mark`` once ``_quitting`` was
        set -- a business ``d[k] = v`` after the drain started therefore
        raised out of a plain container mutation AND left the new data
        unmarked (the assignment had already landed in memory), i.e. the
        worst of both worlds: an exception the caller cannot handle at that
        call site and silent data loss.  The mutation already exists in
        memory; never-drop says the drain should try to flush it: the mark is
        queued like any other, a warning makes the late mutation visible, and
        ``flush_all``'s deadline (now also checked on the success path)
        bounds how long late arrivals can extend the drain.
        """
        if self._quitting:
            self._dirty.setdefault(saver, 0)
            if self._late_mark_log.allow():
                logger.warning(
                    "late dirty mark for %r during shutdown drain (queued; "
                    "late marks=%d, suppressed=%d)",
                    saver,
                    self._late_marks,
                    self._late_mark_log.take_suppressed(),
                )
            self._late_marks += 1
            self._sync_metrics()
            return
        self._dirty.setdefault(saver, 0)
        self._sync_metrics()

    def requeue(self, saver: DataSaver) -> None:
        """Queue a saver without the late-mark warning -- framework recovery
        paths only (journal release when a transaction ends, rollback
        re-marks).

        Since F-186 ``mark`` also accepts late arrivals during the shutdown
        drain (never-drop beats atomicity when the process is going down, and
        the data is already in memory either way); the two paths differ only
        in that ``requeue`` is expected during the drain and stays quiet, so
        routine transaction endings do not spam the late-mark warning.
        ``flush_all`` iterates ``_dirty`` until it is empty or its deadline,
        so a requeue landing inside the drain window is still flushed.
        """
        self._dirty.setdefault(saver, 0)
        self._sync_metrics()

    def queue_depth(self) -> int:
        return len(self._dirty)

    def forget(self, saver: DataSaver) -> None:
        """F-151: dequeue ``saver`` -- an explicit :meth:`DataSaver.flush`
        that succeeded has already persisted what the queued entry would
        redundantly re-upsert on the next round. The caller (DataSaver)
        guards this with its dirty-generation counter so a mark that landed
        *during* the flush keeps the saver queued.
        """
        if self._dirty.pop(saver, None) is not None:
            self._deferred.pop(saver, None)
            self._sync_metrics()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())
            # F-46: if the loop ever exits unexpectedly (guard bug, subclass
            # override), auto-save silently stops -- that must be loud.
            self._task.add_done_callback(self._on_loop_done)

    def _on_loop_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.critical("auto-save loop died: %r", exc, exc_info=exc)
            self._alarm("save_loop_died", {"error": repr(exc)})

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
            try:
                await self.flush_batch()
            except asyncio.CancelledError:
                raise
            except Exception:
                # F-46: one poison row (a blob that cannot be encoded, a bug
                # in a flush path) used to kill this task -- every later mark
                # stayed in memory, unsaved and unalarmed, until shutdown.
                self.failed_total += 1
                self._metrics.save_failures.inc()
                self._sync_metrics()
                logger.critical("auto-save flush round failed; savers requeued", exc_info=True)
                self._alarm("save_loop_error", {"queue_depth": self.queue_depth()})

    def _pick_batch(self, now: float) -> list[tuple[DataSaver, int]]:
        """Up to ``batch_size`` flushable ``(saver, failures)`` pairs (F-181).

        One pass over ``_dirty`` in queue order.  The old helper re-scanned
        the whole queue per pick (O(batch x queue)): during a long outage the
        queue grows without bound while every round still paid the full scan
        per selected saver.  Semantics are identical -- the per-pick loop
        popped each selection before the next scan, and nothing can mutate
        the queue between picks (no await inside), so a single ordered pass
        yields the same savers.
        """
        batch: list[tuple[DataSaver, int]] = []
        for saver, failures in self._dirty.items():
            if len(batch) >= self._batch_size:
                break
            if saver in self._inflight:
                # F-151: a saver re-marked while its flush is still running
                # is NOT re-pickable -- the running flush owns its lock, so
                # picking it merely blocked on begin_flush_row. It stays
                # queued here (the re-mark re-added it) and is picked next
                # round once the in-flight flush releases.
                continue
            if saver.held_by_journal is not None:
                # F-63: an open transaction owns this saver's writes;
                # flushing it here would autocommit outside the unit.  Leave
                # it queued -- the journal re-marks it when the unit ends.
                continue
            due = self._deferred.get(saver)
            if due is not None and due > now:
                continue
            batch.append((saver, failures))
        return batch

    async def flush_batch(self) -> None:
        batch = self._pick_batch(time.monotonic())
        for saver, _failures in batch:
            self._dirty.pop(saver, None)
            self._deferred.pop(saver, None)
            self._inflight.add(saver)
        if not batch:
            return
        # Savers whose outcome is not yet resolved; on cancellation they must
        # go back to the queue (the upsert may not have landed).
        unresolved: set[DataSaver] = {saver for saver, _ in batch}
        # F-59: every encoded saver's flush lock, held until the SQL carrying
        # its row has executed (or its row was handed to a path that re-takes
        # the lock itself).  _flush_group drains this set; the finally block
        # is the cancellation/bug safety net -- an unbalanced hold would
        # deadlock the next delete()/flush() on that saver forever.
        held: set[DataSaver] = set()
        try:
            # F-42: rows sharing (executor, table, column) coalesce into one
            # multi-row upsert -- one round-trip per table per batch instead
            # of one per saver (~batch_size/interval upserts/s before).
            groups: dict[tuple[int, str, str], list[tuple[DataSaver, int, Any, bytes]]] = {}
            for saver, failures in batch:
                try:
                    row = await saver.begin_flush_row()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # F-46: an unencodable row is quarantined exactly like a
                    # failing flush (F-33 semantics for encode errors) --
                    # requeued with backoff instead of aborting the batch and
                    # starving every saver queued behind it.
                    unresolved.discard(saver)
                    self._record_flush_failure(saver, failures)
                    continue
                if row is None:
                    # deleted while waiting: nothing to persist, same as the
                    # old flush() no-op path
                    unresolved.discard(saver)
                    self.saved_total += 1
                    self._metrics.save_flushed.inc()
                    continue
                held.add(saver)  # F-59: lock stays taken until the SQL lands
                groups.setdefault((id(saver.executor), saver.table, saver.column), []).append(
                    (saver, failures, row[0], row[1])
                )
            for members in groups.values():
                resolved = await self._flush_group(members, held)
                unresolved -= resolved
        except asyncio.CancelledError:
            # Cancellation mid-flush (shutdown racing the loop task): requeue
            # everything unresolved for flush_all.
            self._requeue_unresolved(batch, unresolved, backoff=False)
            raise
        except Exception:
            # F-46: an unexpected error mid-flush must neither drop the
            # already-popped savers (never-drop holds for bugs, not just DB
            # outages) nor let the caller hot-loop on the same poison row.
            self._requeue_unresolved(batch, unresolved, backoff=True)
            raise
        finally:
            for saver, _ in batch:
                self._inflight.discard(saver)
            for saver in held:
                saver.end_flush_row()
        self._sync_metrics()

    def _release_held(self, held: set[DataSaver], saver: DataSaver) -> None:
        """Give back one F-59 lock; removed from ``held`` first so a bug in
        end_flush_row cannot trigger a double release from the safety net."""
        held.discard(saver)
        saver.end_flush_row()

    def _requeue_unresolved(
        self,
        batch: list[tuple[DataSaver, int]],
        unresolved: set[DataSaver],
        *,
        backoff: bool,
    ) -> None:
        for saver, failures in batch:
            if saver in unresolved:
                self._dirty.setdefault(saver, failures)
                if backoff and saver not in self._deferred:
                    self._deferred[saver] = time.monotonic() + self._retry_cooldown

    async def _flush_group(
        self, members: list[tuple[DataSaver, int, Any, bytes]], held: set[DataSaver]
    ) -> set[DataSaver]:
        """Coalesced multi-row upsert for one (executor, table, column) group.

        Returns the savers whose outcome was resolved (saved or requeued for
        retry). One poisoned row fails the whole statement, so on failure the
        group falls back to per-saver flushes -- preserving F-33's
        isolate-the-poison-row semantics and per-saver backoff accounting.

        F-59: the members' flush locks (taken in flush_batch) are held for the
        duration of the multi-row statement so a racing delete() can only land
        strictly before or strictly after it -- never in between (which
        resurrected the row).  The locks are released before any fallback that
        calls saver.flush(), because flush() takes each lock itself.
        """
        resolved: set[DataSaver] = set()

        if len(members) == 1:
            # One row: the per-saver path IS the coalesced path (and a failure
            # must not be counted twice by a pointless multi->single retry).
            # The lock goes back first: flush() re-takes it and keeps the F-34
            # serialization for its own statement -- a delete() slipping in
            # between can only win by making that flush a no-op, never by
            # being overtaken by it.
            saver, failures, _k, _b = members[0]
            self._release_held(held, saver)
            await self._flush_one(saver, failures)
            return {saver}

        chunk: list[tuple[DataSaver, int, Any, bytes]] = []
        chunk_bytes = 0

        async def flush_chunk() -> None:
            nonlocal chunk, chunk_bytes
            if not chunk:
                return
            head = chunk[0][0]
            members_done = chunk  # chunk is rebound below; keep this batch
            params: list[Any] = []
            for _saver, _failures, key, blob in members_done:
                params.append(key)
                params.append(blob)
            try:
                await head.executor.execute(head.upsert_many_sql(len(members_done)), tuple(params))
                # F-50: a coalesced flush that joined an ambient transaction
                # (flush_batch called inside `async with db.transaction():`)
                # must re-mark its savers dirty if that unit rolls back -- the
                # savers were already popped off the dirty queue.
                journal = current_flush_journal()
                for saver, _f, _k, _b in members_done:
                    resolved.add(saver)
                    self.saved_total += 1
                    self._metrics.save_flushed.inc()
                    if journal is not None:
                        # F-158: mark_journal_flushed (not a bare note_flush)
                        # -- the saver's F-63 deferral hold must drop with it,
                        # or held_by_journal skips this saver forever.
                        saver.mark_journal_flushed(journal)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "coalesced upsert into %s.%s failed; falling back to per-saver "
                    "flushes to isolate the failing row",
                    head.table,
                    head.column,
                    exc_info=True,
                )
                # F-59: hand the locks back before the fallback -- flush()
                # re-takes each one itself (holding them here would deadlock).
                for saver, _f, _k, _b in members_done:
                    self._release_held(held, saver)
                for saver, failures, _k, _b in members_done:
                    await self._flush_one(saver, failures)
                    resolved.add(saver)
            else:
                # F-59: the SQL carrying these rows has landed; a delete()
                # blocked on any of these locks now runs strictly after it.
                for saver, _f, _k, _b in members_done:
                    self._release_held(held, saver)
            chunk = []
            chunk_bytes = 0

        for member in members:
            blob_size = len(member[3])
            if chunk and (
                len(chunk) >= _COALESCE_MAX_ROWS or chunk_bytes + blob_size > _COALESCE_MAX_BYTES
            ):
                await flush_chunk()
            chunk.append(member)
            chunk_bytes += blob_size
        await flush_chunk()
        return resolved

    async def _flush_one(self, saver: DataSaver, failures: int) -> None:
        try:
            await saver.flush()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._record_flush_failure(saver, failures)
        else:
            self.saved_total += 1
            self._metrics.save_flushed.inc()

    def _record_flush_failure(self, saver: DataSaver, failures: int) -> None:
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

    # ---------------------------- shutdown ------------------------------ #

    async def flush_all(self, *, timeout: float | None = None) -> bool:
        """Drain every dirty saver, retrying within a bounded deadline."""
        # F-63 note: unlike flush_batch's selection, the shutdown drain does
        # NOT skip journal-held savers -- the process is going down, the
        # owning transaction will never commit, and never-drop beats atomicity
        # for data that would otherwise be lost with the process.
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
                self._dirty.pop(saver, None)  # F-151: forget() may have removed it
                self.saved_total += 1
                self._metrics.save_flushed.inc()
                # F-186: marks accepted during the drain (never-drop) can
                # keep feeding the loop; the deadline bounds them too --
                # without this check only the FAILURE path ever noticed it.
                if self._dirty and time.monotonic() >= deadline:
                    self._report_unflushed()
                    return False
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
