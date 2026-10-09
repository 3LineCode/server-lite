"""Database process service and access facade.

Only the DB process (or a single-process server) holds real MySQL/Redis
connections. Every other process reaches the database through RPC --
``DatabaseAccess`` hides the difference so business code always sees the same
``await db.query(...)`` interface. The dedicated keepalive runs inside the DB
process; non-DB processes simply see RPC timeouts if the DB process dies.

Transactions (F-43) span both paths: locally a dedicated pooled connection
runs the whole unit; remotely ``transaction()`` opens a session in the DB
process (dedicated out-of-pool connection) addressed by an opaque id, reaped
by TTL if the caller dies mid-transaction.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any

from pyline.config.errors import ConfigError
from pyline.db.mysql import MySQLPool, MySQLSession
from pyline.db.redis import RedisClient
from pyline.db.transaction import (
    TransactionExecutor,
    TransactionJournal,
    bind_transaction,
    current_transaction,
)
from pyline.net.rpc import RpcManager, RpcTimeoutError

logger = logging.getLogger(__name__)

RPC_QUERY = "pyline.db.query"
RPC_EXECUTE = "pyline.db.execute"
RPC_REDIS_GET = "pyline.db.redis_get"
RPC_REDIS_SET = "pyline.db.redis_set"
RPC_REDIS_DEL = "pyline.db.redis_del"
RPC_TX_BEGIN = "pyline.db.tx_begin"
RPC_TX_EXECUTE = "pyline.db.tx_execute"
RPC_TX_QUERY = "pyline.db.tx_query"
RPC_TX_COMMIT = "pyline.db.tx_commit"
RPC_TX_ROLLBACK = "pyline.db.tx_rollback"
RPC_TX_STATUS = "pyline.db.tx_status"


class TransactionGoneError(Exception):
    """The RPC transaction session is unknown, expired, or already finished."""


class TransactionLimitError(Exception):
    """Too many concurrent remote transactions."""


class NullPool:
    """Placeholder pool for servers that enable redis but not mysql."""

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        raise ConnectionError("mysql not enabled on this server")

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        raise ConnectionError("mysql not enabled on this server")

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[TransactionExecutor]:
        raise ConnectionError("mysql not enabled on this server")
        yield  # pragma: no cover - unreachable, shapes the return type


class NullRedis:
    """Placeholder redis for servers that enable mysql but not redis."""

    async def get(self, key: str) -> str | None:
        raise ConnectionError("redis not enabled on this server")

    async def set(self, key: str, value: str) -> None:
        raise ConnectionError("redis not enabled on this server")

    async def delete(self, *keys: str) -> int:
        raise ConnectionError("redis not enabled on this server")


PoolLike = MySQLPool | NullPool
RedisLike = RedisClient | NullRedis


class _TxRecord:
    """One live remote-transaction session in the DB process."""

    def __init__(self, session: MySQLSession) -> None:
        self.session = session
        self.last_active = time.monotonic()
        # One connection, potentially many coroutines: the ambient-transaction
        # ContextVar is inherited by tasks spawned inside the block, so two of
        # them can issue statements on the SAME session concurrently. Without
        # serialization the statements interleave on one socket -- asyncmy
        # connections are not safe for concurrent cursors -- corrupting the
        # protocol stream. The lock also covers commit/rollback, so an
        # in-flight statement finishes before the unit ends.
        self.lock = asyncio.Lock()

    def touch(self) -> None:
        """F-47: the TTL bounds *idle* time -- an active transaction must not
        be rolled back for simply being long."""
        self.last_active = time.monotonic()

    async def dispose(self, *, rollback: bool) -> None:
        """Roll back (optionally) and close the session.

        F-142: takes the lock itself. The reaper and close() call this while
        statements may still be in flight on the session (the TTL bounds idle
        time, and two queued 30s-read-timeout statements can outlive it from
        the first touch()); disposing without the lock used to race an
        in-flight statement on the same asyncmy connection -- exactly the
        protocol-stream corruption the lock exists to prevent. Commit paths
        that already hold the lock must not call this (deadlock: the lock is
        not reentrant).
        """
        async with self.lock:
            if rollback:
                with contextlib.suppress(Exception):
                    await self.session.rollback()
            await self.session.close()


class DatabaseService:
    """Runs in the DB process: owns pools and exposes them via RPC."""

    def __init__(
        self,
        pool: PoolLike,
        redis: RedisLike,
        *,
        tx_ttl: float = 60.0,
        max_transactions: int = 32,
        outcome_ttl: float = 300.0,
    ) -> None:
        self._pool = pool
        self._redis = redis
        # Remote transaction sessions (F-43): opaque id -> dedicated
        # connection. Bounded by max_transactions so callers cannot open an
        # unbounded number of out-of-pool connections; reaped lazily by TTL
        # so a caller that dies mid-transaction cannot pin one forever.
        self._tx: dict[str, _TxRecord] = {}
        self._tx_ttl = tx_ttl
        self._max_transactions = max_transactions
        # F-209: high-water warning latch (see rpc_tx_begin).
        self._tx_watermark_warned = False
        self._dispose_tasks: set[asyncio.Task[None]] = set()
        # F-61: how finished transactions ended, so a caller whose COMMIT rpc
        # timed out can reconcile the ambiguous outcome.  Entries expire on
        # their own; past the ttl the answer decays to "unknown", which the
        # caller already treats as failure + retry (upserts are idempotent).
        self._outcomes: dict[str, tuple[str, float]] = {}
        self._outcome_ttl = outcome_ttl
        self._closed = False
        # Periodic sweep: the lazy reap in begin/execute/query never runs when
        # the DB process is fully idle, so an abandoned session (and its
        # locks) used to survive until close() on an otherwise idle process.
        self._sweep_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Start the periodic TTL sweep (idempotent; stopped by close())."""
        if self._sweep_task is not None and not self._sweep_task.done():
            return
        self._sweep_task = asyncio.get_running_loop().create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        interval = max(1.0, self._tx_ttl / 4)
        while True:
            await asyncio.sleep(interval)
            self._reap_expired()
            self._prune_outcomes()  # F-182: bound the outcome table without status rpcs

    def expose(self, rpc: RpcManager) -> None:
        rpc.register(RPC_QUERY, self.rpc_query)
        rpc.register(RPC_EXECUTE, self.rpc_execute)
        rpc.register(RPC_REDIS_GET, self.rpc_redis_get)
        rpc.register(RPC_REDIS_SET, self.rpc_redis_set)
        rpc.register(RPC_REDIS_DEL, self.rpc_redis_del)
        rpc.register(RPC_TX_BEGIN, self.rpc_tx_begin)
        rpc.register(RPC_TX_EXECUTE, self.rpc_tx_execute)
        rpc.register(RPC_TX_QUERY, self.rpc_tx_query)
        rpc.register(RPC_TX_COMMIT, self.rpc_tx_commit)
        rpc.register(RPC_TX_ROLLBACK, self.rpc_tx_rollback)
        rpc.register(RPC_TX_STATUS, self.rpc_tx_status)

    async def rpc_query(self, sql: str, args: list[Any]) -> list[list[Any]]:
        rows = await self._pool.query(sql, tuple(args))
        return [list(row) for row in rows]

    async def rpc_execute(self, sql: str, args: list[Any]) -> int:
        return await self._pool.execute(sql, tuple(args))

    async def rpc_redis_get(self, key: str) -> str | None:
        return await self._redis.get(key)

    async def rpc_redis_set(self, key: str, value: str) -> None:
        await self._redis.set(key, value)

    async def rpc_redis_del(self, keys: list[str]) -> int:
        return await self._redis.delete(*keys)

    # --------------------------- transactions --------------------------- #

    def active_transactions(self) -> int:
        return len(self._tx)

    async def rpc_tx_begin(self) -> str:
        if self._closed:
            raise ConnectionError("database service is closed")
        self._reap_expired()
        if isinstance(self._pool, NullPool):
            raise ConnectionError("mysql not enabled on this server")
        if len(self._tx) >= self._max_transactions:
            raise TransactionLimitError(
                f"remote transaction limit reached ({self._max_transactions})"
            )
        # F-209: the limit is global to every business process combined, and
        # each session is a dedicated out-of-pool connection -- a burst that
        # trends toward the cap deserves a warning BEFORE callers start
        # eating TransactionLimitError (75% high-water, once until it drops
        # back under).
        if len(self._tx) >= self._max_transactions * 3 // 4 and not self._tx_watermark_warned:
            self._tx_watermark_warned = True
            logger.warning(
                "remote transactions at %d/%d (each holds a dedicated connection; "
                "budget against mysql.max_connections)",
                len(self._tx),
                self._max_transactions,
            )
        elif len(self._tx) < self._max_transactions // 2:
            self._tx_watermark_warned = False
        session = await self._pool.open_session()
        try:
            await session.begin()
        except BaseException:
            await session.close()
            raise
        tx_id = uuid.uuid4().hex
        self._tx[tx_id] = _TxRecord(session)
        return tx_id

    def _live_tx(self, tx_id: str) -> _TxRecord:
        record = self._tx.get(tx_id)
        if record is None:
            raise TransactionGoneError(
                f"transaction {tx_id} is unknown, expired (ttl {self._tx_ttl:.0f}s), "
                "or already finished"
            )
        return record

    async def rpc_tx_execute(self, tx_id: str, sql: str, args: list[Any]) -> int:
        self._reap_expired()
        record = self._live_tx(tx_id)
        record.touch()
        async with record.lock:
            # F-180: the lookup above ran OUTSIDE the lock.  commit/rollback/
            # TTL-reap pop the record and then take this same lock -- if one
            # of them won the pop while we waited here, executing now would
            # run a statement on a session that commit already closed (or a
            # rollback is about to).  The dict-membership re-check under the
            # lock closes that window: popped means finished, answer Gone.
            if self._tx.get(tx_id) is not record:
                raise TransactionGoneError(
                    f"transaction {tx_id} finished while the statement waited; retry"
                )
            return await record.session.execute(sql, tuple(args))

    async def rpc_tx_query(self, tx_id: str, sql: str, args: list[Any]) -> list[list[Any]]:
        self._reap_expired()
        record = self._live_tx(tx_id)
        record.touch()
        async with record.lock:
            if self._tx.get(tx_id) is not record:  # F-180: see rpc_tx_execute
                raise TransactionGoneError(
                    f"transaction {tx_id} finished while the query waited; retry"
                )
            rows = await record.session.query(sql, tuple(args))
        return [list(row) for row in rows]

    async def rpc_tx_commit(self, tx_id: str) -> None:
        record = self._tx.pop(tx_id, None)
        if record is None:
            raise TransactionGoneError(f"transaction {tx_id} already finished")
        # F-47: a failed COMMIT must not leak the dedicated connection -- the
        # record is already popped, so the caller's rollback remedy would
        # only see TransactionGoneError and the session would stay open.
        try:
            # F-142: the lock spans commit AND close -- the record is popped,
            # but a statement that already looked the record up can still be
            # queued on the lock; closing outside it raced that statement.
            async with record.lock:
                try:
                    await record.session.commit()
                finally:
                    await record.session.close()
        except BaseException:
            # F-61: a failed COMMIT leaves the server-side outcome genuinely
            # unknown (the connection may have died mid-commit), never
            # "committed".
            self._record_outcome(tx_id, "unknown")
            raise
        self._record_outcome(tx_id, "committed")

    async def rpc_tx_rollback(self, tx_id: str) -> None:
        record = self._tx.pop(tx_id, None)
        if record is None:
            raise TransactionGoneError(f"transaction {tx_id} already finished")
        # F-61: recorded before the dispose so the outcome is visible even if
        # the rollback itself then hangs or fails.
        self._record_outcome(tx_id, "rolled_back")
        # F-142: dispose takes the lock itself (serializes with in-flight
        # statements), so it must not be called under it.
        await record.dispose(rollback=True)

    async def rpc_tx_status(self, tx_id: str) -> str:
        """F-61: ``'committed' | 'rolled_back' | 'unknown'`` for a transaction.

        ``unknown`` covers both expiry of the recorded outcome and a COMMIT
        still in flight on another coroutine -- callers treat it as failure
        plus retry, which upsert idempotency makes safe.
        """
        self._prune_outcomes()
        entry = self._outcomes.get(tx_id)
        if entry is not None:
            return entry[0]
        return "unknown"

    def _record_outcome(self, tx_id: str, outcome: str) -> None:
        # F-182 companion: a plain assignment plus a size-triggered prune.
        # The old version rebuilt the whole dict (TTL filter) on EVERY record
        # -- O(n) per finished transaction and O(n**2) across a burst of
        # commits; pruning only past a size threshold keeps recording O(1)
        # amortized while the table stays bounded.
        self._outcomes[tx_id] = (outcome, time.monotonic())
        if len(self._outcomes) > 512:
            self._prune_outcomes()

    def _prune_outcomes(self) -> None:
        now = time.monotonic()
        self._outcomes = {
            tx: (o, ts) for tx, (o, ts) in self._outcomes.items() if now - ts <= self._outcome_ttl
        }

    def _reap_expired(self) -> None:
        """Lazy TTL sweep on begin/execute/query: abandoned sessions are
        rolled back.

        A caller that dies between BEGIN and COMMIT would otherwise pin a
        dedicated connection (and its locks) forever. The TTL bounds idle
        time (F-47): activity via execute/query pushes ``last_active``
        forward, so a legitimately long transaction is not reaped.
        """
        now = time.monotonic()
        for tx_id, record in list(self._tx.items()):
            if now - record.last_active > self._tx_ttl:
                self._tx.pop(tx_id, None)
                self._record_outcome(tx_id, "rolled_back")  # F-61
                logger.error(
                    "remote transaction %s idle over ttl %.0fs; rolling back and "
                    "closing its session",
                    tx_id,
                    self._tx_ttl,
                )
                task = asyncio.get_running_loop().create_task(record.dispose(rollback=True))
                self._dispose_tasks.add(task)
                task.add_done_callback(self._dispose_tasks.discard)

    async def close(self, *, timeout: float = 10.0) -> None:
        """F-101: shutdown hook for the DB process.

        Roll back and close every live remote-transaction session (the
        process used to just drop them and hope the TCP peer dying would
        clean up), wait for the lazy TTL reapers so no dispose task outlives
        this await, and refuse new transaction begins afterwards.  The pools
        themselves stay owned by their wiring layer.
        """
        self._closed = True
        loop = asyncio.get_running_loop()
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweep_task
            self._sweep_task = None
        disposes: list[asyncio.Task[None]] = []
        for tx_id, record in list(self._tx.items()):
            self._tx.pop(tx_id, None)
            self._record_outcome(tx_id, "rolled_back")  # F-61
            disposes.append(loop.create_task(record.dispose(rollback=True)))
        disposes.extend(self._dispose_tasks)
        if not disposes:
            return
        done, pending = await asyncio.wait(disposes, timeout=timeout)
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                logger.error("session dispose failed during close: %r", task.exception())
        if pending:
            logger.error(
                "database service close(): %d session dispose(s) did not finish "
                "within %.1fs; cancelling them",
                len(pending),
                timeout,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task


class _RemoteTxExecutor:
    """TransactionExecutor view of one remote session (F-43)."""

    def __init__(self, access: DatabaseAccess, tx_id: str) -> None:
        self._access = access
        self.tx_id = tx_id

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        return int(await self._access._remote_call(RPC_TX_EXECUTE, self.tx_id, sql, list(args)))

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        rows = await self._access._remote_call(RPC_TX_QUERY, self.tx_id, sql, list(args))
        return [tuple(row) for row in rows]


@contextlib.asynccontextmanager
async def _remote_transaction(access: DatabaseAccess) -> AsyncIterator[TransactionExecutor]:
    tx_id = str(await access._remote_call(RPC_TX_BEGIN))
    executor = _RemoteTxExecutor(access, tx_id)
    # F-60: the journal must outlive the bind -- the COMMIT rpc runs after
    # the block body, outside bind_transaction, and its failure must still
    # re-mark the savers that already flushed into the unit.
    journal = TransactionJournal()
    try:
        async with bind_transaction(executor, journal=journal):
            yield executor
    except BaseException:
        journal.remark_rolled_back()
        with contextlib.suppress(Exception):
            await access._remote_call(RPC_TX_ROLLBACK, tx_id)
        raise
    try:
        await access._remote_call(RPC_TX_COMMIT, tx_id)
    except RpcTimeoutError:
        # F-61: the rpc timed out, but the DB process may have committed
        # anyway -- reconcile the outcome once instead of guessing.  A
        # reconciled success returns normally; anything else re-marks the
        # flushed savers (idempotent upserts make a false failure safe) and
        # re-raises.
        if await access._tx_outcome(tx_id) == "committed":
            return
        journal.remark_rolled_back()
        raise
    except BaseException:
        journal.remark_rolled_back()  # F-60: commit failed on the server side
        raise


@contextlib.asynccontextmanager
async def _local_transaction(pool: PoolLike) -> AsyncIterator[TransactionExecutor]:
    # F-60: keep the journal alive across the pool context's COMMIT.  The
    # pool commits (and raises on failure) from its own __aexit__, which runs
    # AFTER bind_transaction has exited cleanly -- without this wrapper a
    # failed COMMIT left the flushed savers popped off the dirty queue while
    # the database rolled their rows back (silent divergence).
    journal = TransactionJournal()
    try:
        async with pool.transaction() as session, bind_transaction(session, journal=journal):
            yield session
    except BaseException:
        journal.remark_rolled_back()
        raise


class DatabaseAccess:
    """Uniform DB facade: local pool when this process owns it, RPC otherwise."""

    def __init__(
        self,
        *,
        local: DatabaseService | None = None,
        remote: RpcManager | None = None,
        db_service_no: int | None = None,
    ) -> None:
        if local is None and remote is None:
            raise ValueError("DatabaseAccess needs a local service or remote rpc target")
        self._local = local
        self._remote = remote
        # F-104: ``db_service_no=None`` means this server has no db process at
        # all (pure gateway: use_mysql off and no 'db' sub_process).  Boot
        # must succeed for such a server, so the value is only checked when
        # the database is actually touched -- loudly.
        self._db_service_no = db_service_no

    def _remote_call(self, func_path: str, *args: Any) -> Any:
        if self._remote is None or self._db_service_no is None:
            raise ConfigError(
                "this server has no db process (enable use_mysql or add a 'db' sub_process)"
            )
        return self._remote.call(self._db_service_no, func_path, *args)

    async def _tx_outcome(self, tx_id: str) -> str:
        """F-61: best-effort outcome reconciliation; a failed status rpc
        counts as unknown (the caller treats that as failure + retry)."""
        try:
            return str(await self._remote_call(RPC_TX_STATUS, tx_id))
        except Exception:
            return "unknown"

    @property
    def _local_pool(self) -> PoolLike:
        assert self._local is not None
        return self._local._pool

    @property
    def _local_redis(self) -> RedisLike:
        assert self._local is not None
        return self._local._redis

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        tx = current_transaction()
        if tx is not None:
            return await tx.query(sql, args)
        if self._local is not None:
            rows = await self._local_pool.query(sql, args)
            return [tuple(row) for row in rows]
        rows = await self._remote_call(RPC_QUERY, sql, list(args))
        return [tuple(row) for row in rows]

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        tx = current_transaction()
        if tx is not None:
            return await tx.execute(sql, args)
        if self._local is not None:
            return int(await self._local_pool.execute(sql, args))
        return int(await self._remote_call(RPC_EXECUTE, sql, list(args)))

    def transaction(self) -> AbstractAsyncContextManager[TransactionExecutor]:
        """``async with db.transaction():`` -- one atomic unit (F-43).

        Statements and DataSaver flushes issued inside the block join the
        transaction via the context-local binding; leaving the block with an
        exception rolls everything back. Local callers run on a dedicated
        pooled connection; remote callers open a TTL-bounded session in the
        DB process.
        """
        if self._local is not None:
            return _local_transaction(self._local_pool)
        return _remote_transaction(self)

    async def redis_get(self, key: str) -> str | None:
        if self._local is not None:
            return await self._local_redis.get(key)
        result: str | None = await self._remote_call(RPC_REDIS_GET, key)
        return result

    async def redis_set(self, key: str, value: str) -> None:
        if self._local is not None:
            await self._local_redis.set(key, value)
            return
        await self._remote_call(RPC_REDIS_SET, key, value)

    async def redis_del(self, *keys: str) -> int:
        if self._local is not None:
            return int(await self._local_redis.delete(*keys))
        return int(await self._remote_call(RPC_REDIS_DEL, list(keys)))
