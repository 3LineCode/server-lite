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
from collections.abc import AsyncIterator
from contextvars import ContextVar
from typing import Any, Protocol

__all__ = ["TransactionError", "TransactionExecutor", "current_transaction"]


class TransactionError(Exception):
    """Misuse of the transaction API (nesting, unknown session...)."""


class TransactionExecutor(Protocol):
    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int: ...

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]: ...


_current_tx: ContextVar[TransactionExecutor | None] = ContextVar("pyline_current_tx", default=None)


def current_transaction() -> TransactionExecutor | None:
    """The transaction the current async context is bound to, if any."""
    return _current_tx.get()


@contextlib.asynccontextmanager
async def bind_transaction(executor: TransactionExecutor) -> AsyncIterator[TransactionExecutor]:
    """Bind ``executor`` as the ambient transaction for the duration of the
    block, refusing nesting (no savepoints in v1).

    The caller owns the underlying session lifecycle (begin/commit/rollback);
    this helper only owns the context-local routing and the nesting guard.
    """
    if _current_tx.get() is not None:
        raise TransactionError("nested transactions are not supported")
    token = _current_tx.set(executor)
    try:
        yield executor
    finally:
        _current_tx.reset(token)
