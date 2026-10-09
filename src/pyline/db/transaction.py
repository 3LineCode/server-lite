"""Cross-statement / cross-saver transactions (F-43).

Business code gets one atomic unit::

    async with db.transaction():
        await db.execute("UPDATE gold SET amount = amount - 10 WHERE id = %s", (pid,))
        await gold_saver.flush()
        await inventory_saver.flush()

Every ``query``/``execute`` issued inside the block -- including
:class:`~pyline.db.orm.DataSaver` flushes, whose executor is the shared
:class:`~pyline.db.service.DatabaseAccess` -- joins the transaction through a
context-local binding. Anything that leaves the block with an exception rolls
the whole unit back; a logical save spanning two savers can no longer persist
half (the pool itself is autocommit-per-statement otherwise).

Two implementations share this contract: the local pool (dedicated pooled
connection) and the DB process reached over RPC (session-scoped dedicated
connection, reaped by TTL when a caller dies mid-transaction).
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Protocol

__all__ = [
    "TransactionError",
    "TransactionExecutor",
    "TransactionJournal",
    "current_flush_journal",
    "current_transaction",
]

logger = logging.getLogger(__name__)


class TransactionError(Exception):
    """Misuse of the transaction API (nesting, unknown session...)."""


class TransactionExecutor(Protocol):
    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int: ...

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]: ...


@dataclass
class TransactionJournal:
    """Savers whose persistence is coupled to this ambient transaction.

    ``flushed`` (F-50): savers whose upsert joined the unit.  A rollback
    discards their writes while memory keeps the new data -- each is
    re-marked dirty so the auto-save queue retries it outside the dead unit
    (without this, the row silently kept its old value).

    ``deferred`` (F-63): savers *marked dirty* inside the unit.  Their
    auto-save marking is postponed until the unit ends, otherwise the 5 s
    background flush (which runs without this journal in its context) would
    autocommit them mid-transaction and survive a later rollback.
    """

    flushed: list[Any] = field(default_factory=list)
    deferred: list[Any] = field(default_factory=list)

    def note_flush(self, saver: Any) -> None:
        if saver not in self.flushed:  # identity semantics: DataSaver has no __eq__
            self.flushed.append(saver)
        # F-63: an explicit flush() inside the unit supersedes an earlier
        # deferral -- the row is part of the unit now, and a rollback re-marks
        # it through ``flushed``.  The saver clears its own journal hold.
        if saver in self.deferred:
            self.deferred.remove(saver)

    def note_deferred(self, saver: Any) -> None:
        if saver not in self.deferred:
            self.deferred.append(saver)

    def remark_rolled_back(self) -> None:
        while self.flushed:
            saver = self.flushed.pop()
            try:
                saver.remark_dirty_after_rollback()
            except Exception:
                logger.exception("re-marking saver dirty after transaction rollback failed")
        self.release_deferred()

    def release_deferred(self) -> None:
        """F-63: the unit has ended (committed or rolled back) -- re-queue the
        deferred savers.  Deferred rows were never written inside the unit, so
        their in-memory data must be persisted outside it either way."""
        while self.deferred:
            saver = self.deferred.pop()
            try:
                saver.requeue_deferred()
            except Exception:
                logger.exception("re-queuing deferred saver after transaction end failed")


_current_tx: ContextVar[TransactionExecutor | None] = ContextVar("pyline_current_tx", default=None)
_current_journal: ContextVar[TransactionJournal | None] = ContextVar(
    "pyline_current_flush_journal", default=None
)


def current_transaction() -> TransactionExecutor | None:
    """The transaction the current async context is bound to, if any."""
    return _current_tx.get()


def current_flush_journal() -> TransactionJournal | None:
    """F-50: where a DataSaver flush inside the ambient transaction registers."""
    return _current_journal.get()


@contextlib.asynccontextmanager
async def bind_transaction(
    executor: TransactionExecutor, *, journal: TransactionJournal | None = None
) -> AsyncIterator[TransactionExecutor]:
    """Bind ``executor`` as the ambient transaction for the duration of the
    block, refusing nesting (no savepoints in v1).

    The caller owns the underlying session lifecycle (begin/commit/rollback);
    this helper only owns the context-local routing and the nesting guard.
    An exception leaving the block means both implementations roll the unit
    back, so savers flushed into it are re-marked dirty on the way out (F-50).

    ``journal`` (F-60): the caller may supply a journal that outlives this
    block.  Both implementations run their COMMIT *after* this context exits
    cleanly (the pool commits from its own ``__aexit__``, the remote path
    issues an explicit rpc) -- a journal created here would already be gone
    when that COMMIT fails, leaving flushed savers popped off the dirty queue
    while the database rolled their rows back.
    """
    if _current_tx.get() is not None:
        raise TransactionError("nested transactions are not supported")
    journal = TransactionJournal() if journal is None else journal
    journal_token = _current_journal.set(journal)
    token = _current_tx.set(executor)
    try:
        yield executor
    except BaseException:
        journal.remark_rolled_back()
        raise
    finally:
        # F-63: deferred savers re-enter the auto-save queue no matter how the
        # unit ended; cleared before the contextvars reset so a re-queue cannot
        # be re-deferred into this dying journal.
        journal.release_deferred()
        _current_tx.reset(token)
        _current_journal.reset(journal_token)
