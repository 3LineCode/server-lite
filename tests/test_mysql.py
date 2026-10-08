"""MySQLPool: bootstrap ordering and settings plumbing (no live server)."""

from __future__ import annotations

from typing import Any

import pyline.db.mysql as mysql_mod
from pyline.config.models import MySQLSettings
from pyline.db.mysql import MySQLPool, ensure_database


class _Cursor:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, args: tuple = ()) -> None:
        self._log.append(sql)


class FakeBootstrapConn:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.executed)

    async def ensure_closed(self) -> None:
        pass


class FakeAsyncmyPool:
    def __init__(self) -> None:
        self.closed = False

    async def acquire(self) -> Any:
        raise AssertionError("not used in this test")

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        pass


class TestConnectBootstrap:
    async def test_connect_creates_database_before_pool(self, monkeypatch: Any) -> None:
        """F-05: bootstrap (db-less CREATE DATABASE) runs before create_pool."""
        order: list[tuple[str, str | None]] = []
        conn = FakeBootstrapConn()

        async def fake_connect(**kwargs: Any) -> FakeBootstrapConn:
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

        assert [step for step, _ in order] == ["connect", "create_pool"]
        assert order[0][1] is None  # bootstrap connection selects no database
        assert order[1][1] == "fresh_db"  # the pool connects to the target db
        assert any("CREATE DATABASE IF NOT EXISTS `fresh_db`" in sql for sql in conn.executed)

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
