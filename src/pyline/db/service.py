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

from pyline.db.mysql import MySQLPool, MySQLSession
from pyline.db.redis import RedisClient
from pyline.db.transaction import (
    TransactionExecutor,
    bind_transaction,
    current_transaction,
)
from pyline.net.rpc import RpcManager

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
        self.created = time.monotonic()

    async def dispose(self, *, rollback: bool) -> None:
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
        self._dispose_tasks: set[asyncio.Task[None]] = set()

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
        self._reap_expired()
        if isinstance(self._pool, NullPool):
            raise ConnectionError("mysql not enabled on this server")
        if len(self._tx) >= self._max_transactions:
            raise TransactionLimitError(
                f"remote transaction limit reached ({self._max_transactions})"
            )
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
        return await self._live_tx(tx_id).session.execute(sql, tuple(args))

    async def rpc_tx_query(self, tx_id: str, sql: str, args: list[Any]) -> list[list[Any]]:
        rows = await self._live_tx(tx_id).session.query(sql, tuple(args))
        return [list(row) for row in rows]

    async def rpc_tx_commit(self, tx_id: str) -> None:
        record = self._tx.pop(tx_id, None)
        if record is None:
            raise TransactionGoneError(f"transaction {tx_id} already finished")
        await record.session.commit()
        await record.session.close()

    async def rpc_tx_rollback(self, tx_id: str) -> None:
        record = self._tx.pop(tx_id, None)
        if record is None:
            raise TransactionGoneError(f"transaction {tx_id} already finished")
        await record.dispose(rollback=True)

    def _reap_expired(self) -> None:
        """Lazy TTL sweep on every begin: abandoned sessions are rolled back.

        A caller that dies between BEGIN and COMMIT would otherwise pin a
        dedicated connection (and its locks) forever.
        """
        now = time.monotonic()
        for tx_id, record in list(self._tx.items()):
            if now - record.created > self._tx_ttl:
                self._tx.pop(tx_id, None)
                logger.error(
                    "remote transaction %s exceeded ttl %.0fs; rolling back and "
                    "closing its session",
                    tx_id,
                    self._tx_ttl,
                )
                task = asyncio.get_running_loop().create_task(record.dispose(rollback=True))
                self._dispose_tasks.add(task)
                task.add_done_callback(self._dispose_tasks.discard)


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
    try:
        async with bind_transaction(executor):
            yield executor
    except BaseException:
        with contextlib.suppress(Exception):
            await access._remote_call(RPC_TX_ROLLBACK, tx_id)
        raise
    await access._remote_call(RPC_TX_COMMIT, tx_id)


@contextlib.asynccontextmanager
async def _local_transaction(pool: PoolLike) -> AsyncIterator[TransactionExecutor]:
    async with pool.transaction() as session, bind_transaction(session):
        yield session


class DatabaseAccess:
    """Uniform DB facade: local pool when this process owns it, RPC otherwise."""

    def __init__(
        self,
        *,
        local: DatabaseService | None = None,
        remote: RpcManager | None = None,
        db_service_no: int | None = None,
    ) -> None:
        if local is None and (remote is None or db_service_no is None):
            raise ValueError("DatabaseAccess needs a local service or remote rpc target")
        self._local = local
        self._remote = remote
        self._db_service_no = db_service_no

    def _remote_call(self, func_path: str, *args: Any) -> Any:
        assert self._remote is not None and self._db_service_no is not None
        return self._remote.call(self._db_service_no, func_path, *args)

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
