"""F-43: cross-statement / cross-saver transactions.

Local path (ambient routing, rollback, saver flushes joining the unit),
remote path (RPC sessions through a DatabaseService stand-in), TTL reaping
and the concurrency cap.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest

from pyline.db.orm import DataSaver
from pyline.db.service import (
    RPC_TX_BEGIN,
    RPC_TX_COMMIT,
    RPC_TX_EXECUTE,
    RPC_TX_QUERY,
    RPC_TX_ROLLBACK,
    DatabaseAccess,
    DatabaseService,
    NullPool,
    NullRedis,
    TransactionGoneError,
    TransactionLimitError,
)
from pyline.db.transaction import TransactionError, current_transaction
from test_orm_autosave import make_schema


class RecordingExecutor:
    def __init__(self, log: list[Any]) -> None:
        self._log = log

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        self._log.append(("EXEC", sql, args))
        return 1

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        self._log.append(("QUERY", sql, args))
        return [(1,)]


class FakeTxPool:
    """MySQLPool stand-in: one recorded transaction context at a time."""

    def __init__(self) -> None:
        self.log: list[Any] = []
        self.plain_execute_calls = 0

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        return []

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        self.plain_execute_calls += 1
        return 0

    @contextlib.asynccontextmanager
    async def transaction(self):
        self.log.append("BEGIN")
        try:
            yield RecordingExecutor(self.log)
        except BaseException:
            self.log.append("ROLLBACK")
            raise
        self.log.append("COMMIT")


def local_access() -> tuple[DatabaseAccess, FakeTxPool]:
    pool = FakeTxPool()
    access = DatabaseAccess(local=DatabaseService(pool, NullRedis()))
    return access, pool


class TestLocalTransactions:
    async def test_statements_join_the_transaction(self) -> None:
        access, pool = local_access()
        async with access.transaction():
            assert current_transaction() is not None
            await access.execute("UPDATE t SET a = 1 WHERE id = %s", (5,))
            await access.query("SELECT a FROM t WHERE id = %s", (5,))
        assert current_transaction() is None
        assert pool.log == [
            "BEGIN",
            ("EXEC", "UPDATE t SET a = 1 WHERE id = %s", (5,)),
            ("QUERY", "SELECT a FROM t WHERE id = %s", (5,)),
            "COMMIT",
        ]
        assert pool.plain_execute_calls == 0  # nothing bypassed the session

    async def test_exception_rolls_back(self) -> None:
        access, pool = local_access()
        with pytest.raises(RuntimeError, match="business failure"):
            async with access.transaction():
                await access.execute("UPDATE t SET a = 1")
                raise RuntimeError("business failure")
        assert pool.log == ["BEGIN", ("EXEC", "UPDATE t SET a = 1", ()), "ROLLBACK"]
        assert current_transaction() is None

    async def test_saver_flushes_join_the_unit(self) -> None:
        """F-43's headline: two savers flushed inside one block are one save."""
        access, pool = local_access()
        gold = DataSaver(access, make_schema(), "tbl_player", "data", 1)
        gold.set_data({"gold": 100})
        inv = DataSaver(access, make_schema(), "tbl_player", "data", 2)
        inv.set_data({"items": []})
        async with access.transaction():
            await gold.flush()
            await inv.flush()
        upserts = [e for e in pool.log if isinstance(e, tuple) and e[0] == "EXEC"]
        assert len(upserts) == 2  # both inside BEGIN..COMMIT
        assert pool.log[0] == "BEGIN" and pool.log[-1] == "COMMIT"

    async def test_nested_transactions_rejected(self) -> None:
        access, _pool = local_access()
        with pytest.raises(TransactionError, match="nested"):
            async with access.transaction(), access.transaction():
                pass

    async def test_null_pool_transaction_fails_loudly(self) -> None:
        access = DatabaseAccess(local=DatabaseService(NullPool(), NullRedis()))
        with pytest.raises(ConnectionError, match="mysql not enabled"):
            async with access.transaction():
                pass


class FakeSession:
    """MySQLSession stand-in recording its lifecycle."""

    def __init__(self, log: list[Any]) -> None:
        self._log = log

    async def begin(self) -> None:
        self._log.append("BEGIN")

    async def commit(self) -> None:
        self._log.append("COMMIT")

    async def rollback(self) -> None:
        self._log.append("ROLLBACK")

    async def close(self) -> None:
        self._log.append("CLOSE")

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        self._log.append(("EXEC", sql, args))
        return 1

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        self._log.append(("QUERY", sql, args))
        return [(7,)]


class FakeSessionPool(FakeTxPool):
    """Pool stand-in that also serves open_session() for remote transactions."""

    async def open_session(self) -> FakeSession:
        return FakeSession(self.log)


class FakeRpc:
    """RpcManager stand-in routing pyline.db.tx_* to a DatabaseService."""

    def __init__(self, service: DatabaseService) -> None:
        self._service = service
        self.calls: list[str] = []

    async def call(self, target: int, path: str, *args: Any, timeout: float = 10.0) -> Any:
        self.calls.append(path)
        handlers = {
            RPC_TX_BEGIN: self._service.rpc_tx_begin,
            RPC_TX_EXECUTE: self._service.rpc_tx_execute,
            RPC_TX_QUERY: self._service.rpc_tx_query,
            RPC_TX_COMMIT: self._service.rpc_tx_commit,
            RPC_TX_ROLLBACK: self._service.rpc_tx_rollback,
        }
        return await handlers[path](*args)


def remote_stack(*, tx_ttl: float = 60.0, max_transactions: int = 32):
    pool = FakeSessionPool()
    service = DatabaseService(pool, NullRedis(), tx_ttl=tx_ttl, max_transactions=max_transactions)
    access = DatabaseAccess(remote=FakeRpc(service), db_service_no=9)
    return access, service, pool


class TestRemoteTransactions:
    async def test_commit_path(self) -> None:
        access, service, pool = remote_stack()
        async with access.transaction():
            await access.execute("UPDATE t SET a = 1")
            rows = await access.query("SELECT a FROM t")
        assert rows == [(7,)]
        assert pool.log == [
            "BEGIN",
            ("EXEC", "UPDATE t SET a = 1", ()),
            ("QUERY", "SELECT a FROM t", ()),
            "COMMIT",
            "CLOSE",
        ]
        assert service.active_transactions() == 0

    async def test_exception_rolls_back_remote_unit(self) -> None:
        access, _service, pool = remote_stack()
        with pytest.raises(ValueError):
            async with access.transaction():
                await access.execute("UPDATE t SET a = 1")
                raise ValueError("rollback me")
        assert pool.log == ["BEGIN", ("EXEC", "UPDATE t SET a = 1", ()), "ROLLBACK", "CLOSE"]

    async def test_expired_session_is_reaped(self) -> None:
        _access, service, _pool = remote_stack(tx_ttl=0.0)
        stale_id = await service.rpc_tx_begin()
        await service.rpc_tx_execute(stale_id, "SELECT 1", [])  # still usable now
        # force the record past its ttl
        service._tx[stale_id].created -= 1.0
        fresh_id = await service.rpc_tx_begin()  # triggers the lazy sweep
        await asyncio.sleep(0)  # let the dispose task run
        with pytest.raises(TransactionGoneError, match="expired"):
            await service.rpc_tx_execute(stale_id, "SELECT 1", [])
        with pytest.raises(TransactionGoneError):
            await service.rpc_tx_commit(stale_id)
        await service.rpc_tx_commit(fresh_id)

    async def test_concurrent_transaction_cap(self) -> None:
        access, _service, _pool = remote_stack(max_transactions=1)
        async with access.transaction():
            with pytest.raises(TransactionLimitError, match="limit reached"):
                async with access.transaction():
                    pass
