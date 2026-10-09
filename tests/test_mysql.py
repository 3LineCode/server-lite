"""MySQLPool: bootstrap ordering, dedicated keepalive, settings plumbing."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import pyline.db.mysql as mysql_mod
from pyline.config.models import MySQLSettings
from pyline.db.mysql import MySQLPool, PoolAcquireTimeoutError, ensure_database


class FakeCursor:
    def __init__(self, exc: BaseException | None = None, row: tuple = (1,)) -> None:
        self._exc = exc
        self._row = row
        self.rowcount = 1
        self._sql = ""

    async def __aenter__(self) -> FakeCursor:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, args: tuple = ()) -> None:
        if self._exc is not None:
            raise self._exc
        self._sql = sql

    async def fetchone(self) -> tuple:
        return self._row

    async def fetchall(self) -> list[tuple]:
        return [self._row]


class FakeConn:
    def __init__(self, exc: BaseException | None = None, row: tuple = (1,)) -> None:
        self.executed: list[str] = []
        self._exc = exc
        self._row = row

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._exc, self._row)

    async def ensure_closed(self) -> None:
        pass

    async def commit(self) -> None:
        pass


class FakeAsyncmyPool:
    def __init__(self) -> None:
        self.closed = False
        self.acquired = 0

    def acquire(self) -> Any:
        self.acquired += 1
        raise AssertionError("business pool must not be used by keepalive or bootstrap")

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        pass


class TestConnectBootstrap:
    async def test_connect_creates_database_before_pool(self, monkeypatch: Any) -> None:
        """F-05: bootstrap (db-less CREATE DATABASE) runs before create_pool."""
        order: list[tuple[str, str | None]] = []
        conn = FakeConn()

        async def fake_connect(**kwargs: Any) -> FakeConn:
            order.append(("connect", kwargs.get("db")))
            return conn

        async_pool = FakeAsyncmyPool()

        async def fake_create_pool(**kwargs: Any) -> FakeAsyncmyPool:
            order.append(("create_pool", kwargs.get("db")))
            return async_pool

        monkeypatch.setattr(mysql_mod.asyncmy, "connect", fake_connect)
        monkeypatch.setattr(mysql_mod.asyncmy, "create_pool", fake_create_pool)

        settings = MySQLSettings(
            user="root", password="x", db_name="fresh_db", keepalive_interval=60.0
        )
        pool = MySQLPool(settings)
        await pool.connect()
        await pool.close()

        steps = [step for step, _ in order]
        # bootstrap connection (no db) -> pool (target db) -> keepalive conn
        assert steps == ["connect", "create_pool", "connect"]
        assert order[0][1] is None
        assert order[1][1] == "fresh_db"
        assert order[2][1] == "fresh_db"

    async def test_ensure_database_rejects_bad_name(self, monkeypatch: Any) -> None:
        async def fail_connect(**kwargs: Any) -> Any:
            raise AssertionError("must not connect for an invalid db name")

        monkeypatch.setattr(mysql_mod.asyncmy, "connect", fail_connect)
        settings = MySQLSettings(user="root", password="x", db_name="bad-name; drop")
        try:
            await ensure_database(settings)
        except mysql_mod.MySQLError as exc:
            assert "invalid database name" in str(exc)
        else:
            raise AssertionError("expected MySQLError for invalid database name")


class TestKeepalive:
    def _patch(self, monkeypatch: Any, keepalive_conn: FakeConn) -> FakeAsyncmyPool:
        async def fake_connect(**kwargs: Any) -> FakeConn:
            if kwargs.get("db") is None:
                return FakeConn()  # bootstrap
            return keepalive_conn

        pool = FakeAsyncmyPool()

        async def fake_create_pool(**kwargs: Any) -> FakeAsyncmyPool:
            return pool

        monkeypatch.setattr(mysql_mod.asyncmy, "connect", fake_connect)
        monkeypatch.setattr(mysql_mod.asyncmy, "create_pool", fake_create_pool)
        return pool

    async def test_keepalive_uses_dedicated_connection_and_alarms(self, monkeypatch: Any) -> None:
        """F-10: heartbeat on its own connection; lost -> flag + on_lost."""
        dead_conn = FakeConn(exc=ConnectionError("server gone"))
        async_pool = self._patch(monkeypatch, dead_conn)
        lost_calls: list[int] = []
        settings = MySQLSettings(
            user="root",
            password="x",
            db_name="d",
            keepalive_interval=0.01,
            keepalive_miss_limit=2,
        )
        pool = MySQLPool(settings, on_lost=lambda: lost_calls.append(1))
        await pool.connect()
        for _ in range(200):
            if pool.lost:
                break
            await asyncio.sleep(0.01)
        assert pool.lost
        assert lost_calls == [1]
        assert async_pool.acquired == 0  # never touched the business pool
        await pool.close()

    async def test_keepalive_healthy_stays_alive(self, monkeypatch: Any) -> None:
        healthy = FakeConn(row=(1,))
        self._patch(monkeypatch, healthy)
        settings = MySQLSettings(
            user="root",
            password="x",
            db_name="d",
            keepalive_interval=0.01,
            keepalive_miss_limit=2,
        )
        pool = MySQLPool(settings)
        await pool.connect()
        await asyncio.sleep(0.1)
        assert not pool.lost
        await pool.close()


class TestRecovery:
    async def test_pool_rebuilds_after_loss(self, monkeypatch: Any) -> None:
        """F-32: keepalive loss arms a backoff recovery loop; the pool
        rebuilds itself instead of running dead until a human restarts."""
        monkeypatch.setattr(mysql_mod, "_RECOVER_BACKOFF_MIN", 0.01)
        monkeypatch.setattr(mysql_mod, "_RECOVER_BACKOFF_MAX", 0.05)
        settings = MySQLSettings(user="root", password="x", db_name="d")
        pool = MySQLPool(settings)
        builds = {"n": 0}

        async def fake_build() -> None:
            builds["n"] += 1
            if builds["n"] == 1:
                raise RuntimeError("still down")

        async def fake_teardown() -> None:
            pass

        monkeypatch.setattr(pool, "_build_pool", fake_build)
        monkeypatch.setattr(pool, "_teardown_pool", fake_teardown)
        pool.lost = True
        pool._start_recovery()
        for _ in range(300):
            if not pool.lost:
                break
            await asyncio.sleep(0.01)
        assert builds["n"] == 2  # one failed attempt, then success
        assert not pool.lost
        await pool.close()

    async def test_on_lost_fires_once_per_incident(self, monkeypatch: Any) -> None:
        """The on_lost callback must fire on each False->True transition but
        not re-fire while the pool is already declared lost."""
        calls: list[int] = []
        settings = MySQLSettings(user="root", password="x", db_name="d")
        pool = MySQLPool(settings, on_lost=lambda: calls.append(1))

        async def noop() -> None:
            pass

        async def boom() -> None:
            raise ConnectionError("server gone")

        monkeypatch.setattr(pool, "_build_pool", noop)
        monkeypatch.setattr(pool, "_teardown_pool", noop)
        task = asyncio.get_running_loop().create_task(boom())
        await asyncio.sleep(0)  # let the task complete WITH its exception
        pool._keepalive_done(task)  # first death: fires on_lost, arms recovery
        pool._keepalive_done(task)  # repeat while already lost: no re-fire
        assert calls == [1]
        await pool.close()


class _StuckPool:
    """asyncmy pool stand-in whose connections never come free (F-62)."""

    def acquire(self) -> Any:
        async def stuck() -> Any:
            await asyncio.sleep(3600)
            raise AssertionError("unreachable")

        return stuck()

    def release(self, conn: Any) -> Any:
        async def noop() -> None:
            return None

        return noop()

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


class TestPoolAcquireTimeoutF62:
    async def test_query_times_out_when_no_connection_free(self) -> None:
        """F-62: pool.acquire() waits forever; a half-dead server used to
        park every caller past max_conn with no error at all."""
        settings = MySQLSettings(user="root", password="x", db_name="d", acquire_timeout=0.05)
        pool = MySQLPool(settings)
        pool._pool = _StuckPool()  # type: ignore[assignment]
        with pytest.raises(PoolAcquireTimeoutError, match="no mysql connection free"):
            await pool.query("SELECT 1")
        await pool.close()

    async def test_transaction_acquire_times_out(self) -> None:
        settings = MySQLSettings(user="root", password="x", db_name="d", acquire_timeout=0.05)
        pool = MySQLPool(settings)
        pool._pool = _StuckPool()  # type: ignore[assignment]
        with pytest.raises(PoolAcquireTimeoutError, match="no mysql connection free"):
            async with pool.transaction():
                pass
        await pool.close()

    async def test_acquired_connection_is_released_after_use(self) -> None:
        """The rewrite from `async with acquire()` to acquire/try/finally
        must not leak the connection on either the query or the error path."""
        released: list[Any] = []

        class FreePool:
            def acquire(self) -> Any:
                async def get() -> Any:
                    return FakeConn()

                return get()

            def release(self, conn: Any) -> Any:
                released.append(conn)

                async def noop() -> None:
                    return None

                return noop()

            def close(self) -> None:
                pass

            async def wait_closed(self) -> None:
                pass

        settings = MySQLSettings(user="root", password="x", db_name="d")
        pool = MySQLPool(settings)
        pool._pool = FreePool()  # type: ignore[assignment]
        rows = await pool.query("SELECT 1")
        assert rows == [(1,)]
        affected = await pool.execute("UPDATE t SET a = 1")
        assert affected == 1
        assert len(released) == 2

        class BrokenConn(FakeConn):
            def cursor(self) -> FakeCursor:
                return FakeCursor(exc=ConnectionError("socket gone"))

        class BrokenPool(FreePool):
            def acquire(self) -> Any:
                async def get() -> Any:
                    return BrokenConn()

                return get()

        pool._pool = BrokenPool()  # type: ignore[assignment]
        with pytest.raises(ConnectionError, match="socket gone"):
            await pool.query("SELECT 1")
        assert len(released) == 3  # released even when the statement failed
        await pool.close()


class _ScriptedCursor(FakeCursor):
    """Fails the statements it is scripted to fail (F-68)."""

    def __init__(self, fail_on: tuple[str, ...]) -> None:
        super().__init__()
        self._fail_on = fail_on

    async def execute(self, sql: str, args: tuple = ()) -> None:
        head = sql.strip().split(None, 1)[0].upper() if sql.strip() else ""
        if head in self._fail_on:
            raise ConnectionError(f"{head} failed")
        self._sql = sql


class _ScriptedConn(FakeConn):
    def __init__(self, fail_on: tuple[str, ...]) -> None:
        super().__init__()
        self._fail_on = fail_on
        self.ensure_closed_calls = 0

    def cursor(self) -> FakeCursor:
        return _ScriptedCursor(self._fail_on)

    async def ensure_closed(self) -> None:
        self.ensure_closed_calls += 1


class _RecordingPool:
    """Records release order relative to ensure_closed (F-68)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.events: list[str] = []

    def acquire(self) -> Any:
        async def get() -> Any:
            return self._conn

        return get()

    def release(self, conn: Any) -> Any:
        self.events.append("RELEASE")

        async def noop() -> None:
            return None

        return noop()

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


class TestDoubleFailureDiscardsConnectionF68:
    def _pool(self, fail_on: tuple[str, ...]) -> tuple[MySQLPool, _RecordingPool, _ScriptedConn]:
        settings = MySQLSettings(user="root", password="x", db_name="d")
        conn = _ScriptedConn(fail_on)
        inner = _RecordingPool(conn)
        pool = MySQLPool(settings)
        pool._pool = inner  # type: ignore[assignment]
        return pool, inner, conn

    async def test_commit_and_rollback_both_failing_closes_connection(self) -> None:
        """F-68: when COMMIT *and* the rescue ROLLBACK both fail, the
        connection's transaction state is unknowable -- it used to go back
        into the pool anyway, handing the next acquirer a live grenade."""
        pool, inner, conn = self._pool(("COMMIT", "ROLLBACK"))
        with pytest.raises(ConnectionError, match="COMMIT failed"):
            async with pool.transaction():
                pass
        assert conn.ensure_closed_calls == 1  # closed outright...
        assert inner.events == ["RELEASE"]  # ...before the release (pool drops it)
        await pool.close()

    async def test_commit_failure_with_successful_rollback_keeps_connection(self) -> None:
        """The rescue rollback still works: the connection is clean and may
        return to the pool (no ensure_closed)."""
        pool, inner, conn = self._pool(("COMMIT",))
        with pytest.raises(ConnectionError, match="COMMIT failed"):
            async with pool.transaction():
                pass
        assert conn.ensure_closed_calls == 0  # not discarded
        assert inner.events == ["RELEASE"]  # recycled normally
        await pool.close()

    async def test_body_exception_with_successful_rollback_keeps_connection(self) -> None:
        pool, inner, conn = self._pool(())
        with pytest.raises(RuntimeError, match="business failure"):
            async with pool.transaction():
                raise RuntimeError("business failure")
        assert conn.ensure_closed_calls == 0
        assert inner.events == ["RELEASE"]
        await pool.close()

    async def test_body_exception_with_failing_rollback_discards_connection(self) -> None:
        """F-68 applied symmetrically: a broken body-side rollback must not
        recycle the connection either."""
        pool, inner, conn = self._pool(("ROLLBACK",))
        with pytest.raises(RuntimeError, match="business failure"):
            async with pool.transaction():
                raise RuntimeError("business failure")
        assert conn.ensure_closed_calls == 1
        assert inner.events == ["RELEASE"]
        await pool.close()


class TestSessionIsolationF130:
    """The dedicated transaction session runs the CONFIGURED isolation level
    (it used to silently run the server default), and every connect path
    passes a bounded connect_timeout."""

    async def test_open_session_sets_isolation_and_connect_timeout(self, monkeypatch: Any) -> None:
        captured: dict[str, Any] = {}
        executed: list[str] = []

        class Cursor(FakeCursor):
            async def execute(self, sql: str, args: tuple = ()) -> None:
                executed.append(sql)

        class Conn(FakeConn):
            def cursor(self) -> FakeCursor:
                return Cursor()

        async def fake_connect(**kwargs: Any) -> FakeConn:
            captured.update(kwargs)
            return Conn()

        monkeypatch.setattr(mysql_mod.asyncmy, "connect", fake_connect)
        from pydantic import SecretStr as _SecretStr

        settings = MySQLSettings(user="u", password=_SecretStr("x"), db_name="db")
        assert settings.isolation_level == "READ COMMITTED"
        session = mysql_mod.MySQLSession(settings)
        await session.open()
        assert captured.get("connect_timeout") == 5, "session connect must be bounded"
        assert any("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED" in s for s in executed)

    async def test_pool_passes_recycle_and_bootstrap_timeout(self, monkeypatch: Any) -> None:
        connects: list[dict[str, Any]] = []
        pool_kwargs: dict[str, Any] = {}

        class Conn(FakeConn):
            pass

        async def fake_connect(**kwargs: Any) -> FakeConn:
            connects.append(kwargs)
            return Conn()

        async def fake_create_pool(**kwargs: Any) -> FakeAsyncmyPool:
            pool_kwargs.update(kwargs)
            return FakeAsyncmyPool()

        monkeypatch.setattr(mysql_mod.asyncmy, "connect", fake_connect)
        monkeypatch.setattr(mysql_mod.asyncmy, "create_pool", fake_create_pool)
        from pydantic import SecretStr as _SecretStr

        settings = MySQLSettings(
            user="u", password=_SecretStr("x"), db_name="db", pool_recycle=1800
        )
        pool = MySQLPool(settings)
        await pool.connect()
        try:
            assert pool_kwargs.get("pool_recycle") == 1800
            assert pool_kwargs.get("connect_timeout") == 5
            assert connects and all(c.get("connect_timeout") == 5 for c in connects), (
                "ensure_database/keepalive connects must be bounded too"
            )
        finally:
            await pool.close()
