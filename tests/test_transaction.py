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

from pyline.config.errors import ConfigError
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import DataSaver
from pyline.db.service import (
    RPC_TX_BEGIN,
    RPC_TX_COMMIT,
    RPC_TX_EXECUTE,
    RPC_TX_QUERY,
    RPC_TX_ROLLBACK,
    RPC_TX_STATUS,
    DatabaseAccess,
    DatabaseService,
    NullPool,
    NullRedis,
    TransactionGoneError,
    TransactionLimitError,
)
from pyline.db.transaction import TransactionError, current_transaction
from pyline.net.rpc import RpcTimeoutError
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
            RPC_TX_STATUS: self._service.rpc_tx_status,
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
        _access, service, _pool = remote_stack(tx_ttl=0.05)
        stale_id = await service.rpc_tx_begin()
        await service.rpc_tx_execute(stale_id, "SELECT 1", [])  # still usable now
        # force the record past its ttl
        service._tx[stale_id].last_active -= 1.0
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


class TestRemoteTransactionRobustnessF47:
    async def test_ttl_renews_on_activity(self) -> None:
        """F-47: the ttl bounds idle time, not total age -- a transaction that
        keeps issuing statements survives past the ttl it would previously
        have been rolled back at."""
        _access, service, _pool = remote_stack(tx_ttl=0.12)
        tx_id = await service.rpc_tx_begin()
        for _ in range(6):  # 6 * 0.04 = 0.24s total, never idle over 0.12s
            await asyncio.sleep(0.04)
            await service.rpc_tx_execute(tx_id, "SELECT 1", [])
        await service.rpc_tx_commit(tx_id)
        assert service.active_transactions() == 0

    async def test_idle_session_reaped_on_execute(self) -> None:
        """F-47: the sweep also runs on execute/query, so an abandoned session
        is reaped by any traffic -- not only by the next begin."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis(), tx_ttl=0.05)
        tx_id = await service.rpc_tx_begin()
        await asyncio.sleep(0.12)
        with pytest.raises(TransactionGoneError, match="expired"):
            await service.rpc_tx_execute(tx_id, "SELECT 1", [])
        await asyncio.sleep(0)  # let the dispose task run
        assert pool.log == ["BEGIN", "ROLLBACK", "CLOSE"]

    async def test_commit_failure_still_closes_session(self) -> None:
        """F-47: a failed COMMIT used to leak the dedicated connection -- the
        record was already popped, so the caller's rollback remedy only saw
        TransactionGoneError and the session stayed open forever."""
        pool = _CommitFailPool()
        service = DatabaseService(pool, NullRedis())
        access = DatabaseAccess(remote=FakeRpc(service), db_service_no=9)
        with pytest.raises(ConnectionError, match="commit lost"):
            async with access.transaction():
                await access.execute("UPDATE t SET a = 1")
        assert pool.log == ["BEGIN", ("EXEC", "UPDATE t SET a = 1", ()), "COMMIT", "CLOSE"]
        assert service.active_transactions() == 0


class _CommitFailSession(FakeSession):
    async def commit(self) -> None:
        await super().commit()
        raise ConnectionError("commit lost")


class _CommitFailPool(FakeSessionPool):
    async def open_session(self) -> _CommitFailSession:
        return _CommitFailSession(self.log)


class TestRollbackRemarksDirtyF50:
    async def test_coalesced_flush_rollback_remarks_dirty(self) -> None:
        """F-50: flush_batch inside a rolled-back transaction popped the
        savers off the dirty queue before their upsert -- the rollback
        discarded the row while nothing would ever retry it."""
        access, _pool = local_access()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        with pytest.raises(RuntimeError, match="rollback me"):
            async with access.transaction():
                await scheduler.flush_batch()
                raise RuntimeError("rollback me")
        assert scheduler.queue_depth() == 1  # re-marked after the rollback
        # and it flushes cleanly once retried outside the dead unit
        await scheduler.flush_batch()
        assert scheduler.queue_depth() == 0

    async def test_coalesced_flush_commit_keeps_queue_drained(self) -> None:
        """The happy path is unchanged: committed savers stay off the queue."""
        access, _pool = local_access()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        async with access.transaction():
            await scheduler.flush_batch()
        assert scheduler.queue_depth() == 0

    async def test_direct_flush_without_scheduler_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A saver with no auto-save scheduler cannot be re-queued on rollback
        -- the divergence is at least named in the log."""
        access, _pool = local_access()
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1)
        saver.set_data({"x": 1})
        with (
            pytest.raises(RuntimeError, match="rollback me"),
            caplog.at_level("WARNING", logger="pyline.db.orm"),
        ):
            async with access.transaction():
                await saver.flush()
                raise RuntimeError("rollback me")
        assert any("no auto-save scheduler" in r.message for r in caplog.records)


class _CommitFailTxPool(FakeTxPool):
    """Local pool whose COMMIT fails after a clean body (F-60)."""

    @contextlib.asynccontextmanager
    async def transaction(self):
        self.log.append("BEGIN")
        try:
            yield RecordingExecutor(self.log)
        except BaseException:
            self.log.append("ROLLBACK")
            raise
        self.log.append("COMMIT")
        raise ConnectionError("commit lost")


class TestCommitFailureRemarksDirtyF60:
    async def test_local_commit_failure_remarks_dirty(self) -> None:
        """F-60: the local COMMIT runs in pool.transaction().__aexit__, i.e.
        AFTER bind_transaction has exited cleanly -- the F-50 remark only
        covered body exceptions, so a failed COMMIT left the flushed savers
        popped off the dirty queue while the database rolled their rows back
        (silent divergence)."""
        pool = _CommitFailTxPool()
        access = DatabaseAccess(local=DatabaseService(pool, NullRedis()))
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        with pytest.raises(ConnectionError, match="commit lost"):
            async with access.transaction():
                await saver.flush()
        upserts = [e for e in pool.log if isinstance(e, tuple) and e[0] == "EXEC"]
        assert len(upserts) == 1  # the flush ran inside the unit
        assert pool.log[0] == "BEGIN" and pool.log[-1] == "COMMIT"  # commit attempted last
        assert scheduler.queue_depth() == 1  # re-marked after the failed COMMIT
        await scheduler.flush_batch()  # and it flushes cleanly outside the dead unit
        assert scheduler.queue_depth() == 0

    async def test_remote_commit_failure_remarks_dirty(self) -> None:
        """F-60 (remote): the RPC_TX_COMMIT raise used to escape outside any
        journal semantics -- same silent divergence as the local path."""
        pool = _CommitFailPool()
        service = DatabaseService(pool, NullRedis())
        access = DatabaseAccess(remote=FakeRpc(service), db_service_no=9)
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        with pytest.raises(ConnectionError, match="commit lost"):
            async with access.transaction():
                await saver.flush()
        assert pool.log[0] == "BEGIN"
        assert isinstance(pool.log[1], tuple) and pool.log[1][0] == "EXEC"
        assert pool.log[1][1].startswith("INSERT INTO `tbl_player`")  # flush inside the unit
        assert pool.log[2:] == ["COMMIT", "CLOSE"]  # commit attempted, session closed
        assert scheduler.queue_depth() == 1  # re-marked after the failed COMMIT


class TestJournalRequeueDuringShutdownDrain:
    """A transaction ending inside the shutdown drain window used to hit
    ``SaveScheduler.mark``'s quitting guard: the journal caught the OSError,
    logged it, and dropped the saver -- ``flush_all`` then reported a clean
    drain while the deferred/rolled-back data was lost. Framework requeues
    bypass the guard (never-drop beats atomicity at shutdown)."""

    async def test_deferred_requeue_during_drain_is_not_dropped(self) -> None:
        access, _pool = local_access()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        async with access.transaction():
            await scheduler.flush_batch()  # the unit takes the queued row
            assert scheduler.queue_depth() == 0
            saver.set_data({"gold": 200})  # new mutation defers into the journal
            assert scheduler.queue_depth() == 0
            scheduler._quitting = True  # the shutdown drain begins mid-unit
        # the unit ended inside the drain window: the requeue must survive
        assert scheduler.queue_depth() == 1
        assert await scheduler.flush_all(timeout=1.0) is True

    async def test_rollback_remark_during_drain_is_not_dropped(self) -> None:
        access, _pool = local_access()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        with pytest.raises(RuntimeError, match="rollback me"):
            async with access.transaction():
                await saver.flush()
                scheduler._quitting = True  # drain begins before the rollback
                raise RuntimeError("rollback me")
        assert scheduler.queue_depth() == 1
        assert await scheduler.flush_all(timeout=1.0) is True

    async def test_mark_still_refuses_after_quit_but_requeue_does_not(self) -> None:
        """The loud guard on NEW business mutations stays: only the framework
        recovery paths may bypass it."""
        access, _pool = local_access()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 100})
        scheduler._quitting = True
        with pytest.raises(OSError, match="quitting"):
            saver.mark_dirty()
        scheduler.requeue(saver)
        assert scheduler.queue_depth() == 1


class TestTxStatusF61:
    async def test_outcome_lifecycle(self) -> None:
        """F-61: the db process records how each remote transaction ended;
        still-open or expired-outcome ids answer 'unknown'."""
        _access, service, _pool = remote_stack()
        tx_id = await service.rpc_tx_begin()
        assert await service.rpc_tx_status(tx_id) == "unknown"  # still open
        await service.rpc_tx_commit(tx_id)
        assert await service.rpc_tx_status(tx_id) == "committed"
        tx2 = await service.rpc_tx_begin()
        await service.rpc_tx_rollback(tx2)
        assert await service.rpc_tx_status(tx2) == "rolled_back"

    async def test_reaped_session_answers_rolled_back(self) -> None:
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis(), tx_ttl=0.05)
        stale = await service.rpc_tx_begin()
        await asyncio.sleep(0.1)
        await service.rpc_tx_begin()  # triggers the lazy sweep
        await asyncio.sleep(0)  # let the dispose task run
        assert await service.rpc_tx_status(stale) == "rolled_back"

    async def test_outcome_expires_to_unknown(self) -> None:
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis(), outcome_ttl=0.0)
        tx_id = await service.rpc_tx_begin()
        await service.rpc_tx_commit(tx_id)
        await asyncio.sleep(0.01)
        assert await service.rpc_tx_status(tx_id) == "unknown"


class _TimeoutCommitRpc(FakeRpc):
    """The db process commits successfully, but the caller's rpc times out on
    the way back (slow COMMIT past the rpc deadline)."""

    async def call(self, target: int, path: str, *args: Any, timeout: float = 10.0) -> Any:
        if path == RPC_TX_COMMIT:
            await self._service.rpc_tx_commit(*args)  # the work lands...
            raise RpcTimeoutError("rpc call 'pyline.db.tx_commit' timed out")
        return await super().call(target, path, *args, timeout=timeout)


class _TimeoutCommitNoLandRpc(FakeRpc):
    """The commit rpc times out and the outcome stays unresolved; status is
    answered by the (real) service or a canned value."""

    def __init__(self, service: DatabaseService, status: str) -> None:
        super().__init__(service)
        self._status = status

    async def call(self, target: int, path: str, *args: Any, timeout: float = 10.0) -> Any:
        if path == RPC_TX_COMMIT:
            raise RpcTimeoutError("rpc call 'pyline.db.tx_commit' timed out")
        if path == RPC_TX_STATUS:
            return self._status
        return await super().call(target, path, *args, timeout=timeout)


class TestCommitTimeoutReconciliationF61:
    def _saver(self, access: DatabaseAccess) -> tuple[SaveScheduler, DataSaver]:
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(access, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        return scheduler, saver

    async def test_committed_after_timeout_counts_as_success(self) -> None:
        """F-61: a COMMIT whose rpc timed out may have landed anyway; the
        reconciled 'committed' answer must surface as success -- no raise, no
        spurious re-mark of the flushed savers."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis())
        rpc = _TimeoutCommitRpc(service)
        access = DatabaseAccess(remote=rpc, db_service_no=9)
        scheduler, saver = self._saver(access)
        async with access.transaction():  # must not raise despite the timeout
            saver.set_data({"gold": 5})  # deferred (F-63)
            await saver.flush()  # explicit flush joins the unit (F-50)
        assert "COMMIT" in pool.log and pool.log[-1] == "CLOSE"
        assert RPC_TX_STATUS in rpc.calls  # the outcome was reconciled
        assert scheduler.queue_depth() == 0  # committed: not re-marked

    @pytest.mark.parametrize("status", ["unknown", "rolled_back"])
    async def test_unresolved_outcome_treated_as_failure(self, status: str) -> None:
        """F-61: unknown and rolled_back both count as failure -- the flushed
        savers are re-marked (upsert idempotency makes the retry safe) and
        the timeout error propagates."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis())
        access = DatabaseAccess(remote=_TimeoutCommitNoLandRpc(service, status), db_service_no=9)
        scheduler, saver = self._saver(access)
        with pytest.raises(RpcTimeoutError):
            async with access.transaction():
                saver.set_data({"gold": 5})
                await saver.flush()
        assert scheduler.queue_depth() == 1  # re-marked for retry outside the unit


class TestServiceCloseF69:
    async def test_close_rolls_back_and_closes_live_sessions(self) -> None:
        """F-101: the DB process used to drop live remote-transaction sessions
        on shutdown and rely on the TCP peer dying; close() now rolls back
        and closes each one deterministically and refuses new begins."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis())
        tx1 = await service.rpc_tx_begin()
        tx2 = await service.rpc_tx_begin()
        await service.close()
        assert service.active_transactions() == 0
        assert pool.log.count("ROLLBACK") == 2
        assert pool.log.count("CLOSE") == 2
        assert await service.rpc_tx_status(tx1) == "rolled_back"
        assert await service.rpc_tx_status(tx2) == "rolled_back"
        with pytest.raises(ConnectionError, match="closed"):
            await service.rpc_tx_begin()

    async def test_close_waits_for_pending_dispose_tasks(self) -> None:
        """F-101: the lazy TTL reapers were fire-and-forget tasks nobody
        awaited; close() gathers and awaits them."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis(), tx_ttl=0.05)
        stale = await service.rpc_tx_begin()
        await asyncio.sleep(0.1)
        await service.rpc_tx_begin()  # sweep arms a dispose task for `stale`
        await service.close()
        assert pool.log.count("CLOSE") == 2  # stale's session + the live one
        assert pool.log.count("ROLLBACK") == 2
        assert await service.rpc_tx_status(stale) == "rolled_back"


class TestGatewayAccessF72:
    async def test_no_db_process_fails_loudly_on_use(self) -> None:
        """F-104: a pure gateway server (no db sub-process, use_mysql off)
        boots fine -- but touching the database fails loudly at call time."""
        service = DatabaseService(FakeTxPool(), NullRedis())
        access = DatabaseAccess(remote=FakeRpc(service), db_service_no=None)
        for call in (
            lambda: access.query("SELECT 1"),
            lambda: access.execute("SELECT 1"),
            lambda: access.redis_get("k"),
            lambda: access.redis_set("k", "v"),
            lambda: access.redis_del("k"),
        ):
            with pytest.raises(ConfigError, match="no db process"):
                await call()
        with pytest.raises(ConfigError, match="no db process"):
            async with access.transaction():
                pass

    def test_constructor_still_requires_some_backend(self) -> None:
        with pytest.raises(ValueError, match="local service or remote rpc"):
            DatabaseAccess()
