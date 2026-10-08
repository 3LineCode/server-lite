"""MySQLPool: bootstrap ordering, dedicated keepalive, settings plumbing."""

from __future__ import annotations

import asyncio
from typing import Any

import pyline.db.mysql as mysql_mod
from pyline.config.models import MySQLSettings
from pyline.db.mysql import MySQLPool, ensure_database


class FakeCursor:
    def __init__(self, exc: BaseException | None = None, row: tuple = (1,)) -> None:
        self._exc = exc
        self._row = row

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


class FakeConn:
    def __init__(self, exc: BaseException | None = None, row: tuple = (1,)) -> None:
        self.executed: list[str] = []
        self._exc = exc
        self._row = row

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._exc, self._row)

    async def ensure_closed(self) -> None:
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
