"""MySQL access: asyncmy pool, fully parameterized statements, dedicated
keepalive connection.

Prototype fixes baked in:

* Every statement goes through bound parameters (``%s``); the driver layer
  offers no string-interpolation path (prototype issue #9).
* Keepalive runs on its own connection with a tolerant miss limit instead of
  sharing the business pool with a 1-second kill timer (prototype issue #4).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Sequence
from typing import Any, cast

import asyncmy

from pyline.config.models import MySQLSettings

logger = logging.getLogger(__name__)

_DB_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


async def ensure_database(settings: MySQLSettings) -> None:
    """Create the target database if absent, using a db-less connection.

    Must run before the pool is created: ``create_pool(db=...)`` fails with
    "Unknown database" on a fresh server, which used to make first boot
    crash before ``CREATE DATABASE`` could ever execute (migration plan F-05).
    """
    if not _DB_NAME_RE.fullmatch(settings.db_name):
        raise MySQLError(f"invalid database name: {settings.db_name!r}")
    conn = await asyncmy.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        charset=settings.charset,
        autocommit=True,
    )
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{settings.db_name}` "
                "DEFAULT CHARACTER SET utf8mb4"
            )
    finally:
        with contextlib.suppress(Exception):
            await conn.ensure_closed()


class MySQLError(Exception):
    pass


class MySQLLostError(MySQLError):
    """Keepalive missed its limit; the pool is considered dead."""


class MySQLPool:
    def __init__(self, settings: MySQLSettings) -> None:
        self._settings = settings
        self._pool: asyncmy.Pool | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._configured_conns: set[int] = set()
        self._closed = False

    @property
    def connected(self) -> bool:
        return self._pool is not None and not self._closed

    async def connect(self) -> None:
        s = self._settings
        await ensure_database(s)  # F-05: fresh servers get the DB before pooling
        self._pool = await asyncmy.create_pool(
            host=s.host,
            port=s.port,
            user=s.user,
            password=s.password,
            db=s.db_name,
            charset=s.charset,
            minsize=s.min_conn,
            maxsize=s.max_conn,
            autocommit=True,
            connect_timeout=5,
        )
        logger.info("mysql connected: %s:%d/%s", s.host, s.port, s.db_name)
        self._keepalive_task = asyncio.get_running_loop().create_task(self._keepalive())

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> int:
        """Run a statement; returns affected row count."""
        return cast(int, await self._run("execute", sql, args))

    async def query(self, sql: str, args: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        rows = cast(list[tuple[Any, ...]], await self._run("query", sql, args))
        return [tuple(row) for row in rows]

    async def _run(self, mode: str, sql: str, args: Sequence[Any]) -> object:
        if self._pool is None or self._closed:
            raise MySQLError("mysql pool is not connected")
        async with self._pool.acquire() as conn:
            await self._ensure_isolation(conn)
            async with conn.cursor() as cursor:
                await cursor.execute(sql, tuple(args))
                if mode == "query":
                    return [tuple(row) for row in await cursor.fetchall()]
                await conn.commit()
                return cursor.rowcount

    async def _ensure_isolation(self, conn: Any) -> None:
        conn_id = id(conn)
        if conn_id in self._configured_conns:
            return
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"SET SESSION TRANSACTION ISOLATION LEVEL {self._settings.isolation_level}"
            )
        self._configured_conns.add(conn_id)

    async def _keepalive(self) -> None:
        """Dedicated connection; misses are tolerated up to the configured limit."""
        assert self._pool is not None
        s = self._settings
        misses = 0
        while not self._closed:
            await asyncio.sleep(s.keepalive_interval)
            try:
                async with self._pool.acquire() as conn, conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    row = await cursor.fetchone()
                if row and row[0] == 1:
                    misses = 0
                    continue
            except Exception:
                misses += 1
                logger.warning("mysql keepalive missed %d/%d", misses, s.keepalive_miss_limit)
                if misses >= s.keepalive_miss_limit:
                    logger.error("mysql keepalive exceeded miss limit; pool declared dead")
                    raise MySQLLostError("mysql connection lost (keepalive)") from None

    async def close(self) -> None:
        self._closed = True
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._keepalive_task
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
