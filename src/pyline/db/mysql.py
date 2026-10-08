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
import weakref
from collections.abc import AsyncIterator, Callable, Sequence
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
                f"CREATE DATABASE IF NOT EXISTS `{settings.db_name}` DEFAULT CHARACTER SET utf8mb4"
            )
    finally:
        with contextlib.suppress(Exception):
            await conn.ensure_closed()


class MySQLError(Exception):
    pass


class MySQLLostError(MySQLError):
    """Keepalive missed its limit; the pool is considered dead."""


class _TxSession:
    """Statement pair running on one already-acquired connection (F-43)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, tuple(args))
            return cast(int, cursor.rowcount)

    async def query(self, sql: str, args: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, tuple(args))
            rows = await cursor.fetchall()
            return [tuple(row) for row in rows]


class MySQLSession:
    """Dedicated out-of-pool connection for a multi-statement transaction.

    Used by the DB process to serve remote transactions over RPC (F-43): the
    session must outlive each individual RPC round-trip, so it cannot ride
    the per-statement pool. Concurrency is capped by the service layer.
    """

    def __init__(self, settings: MySQLSettings) -> None:
        self._settings = settings
        self._conn: Any = None

    async def open(self) -> None:
        s = self._settings
        self._conn = await asyncmy.connect(
            host=s.host,
            port=s.port,
            user=s.user,
            password=s.password,
            db=s.db_name,
            charset=s.charset,
            autocommit=True,
            read_timeout=s.read_timeout,
        )

    async def begin(self) -> None:
        async with self._conn.cursor() as cursor:
            await cursor.execute("BEGIN")

    async def commit(self) -> None:
        async with self._conn.cursor() as cursor:
            await cursor.execute("COMMIT")

    async def rollback(self) -> None:
        async with self._conn.cursor() as cursor:
            await cursor.execute("ROLLBACK")

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> int:
        return await _TxSession(self._conn).execute(sql, args)

    async def query(self, sql: str, args: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        return await _TxSession(self._conn).query(sql, args)

    async def close(self) -> None:
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.ensure_closed()
            self._conn = None


# Recovery retries back off exponentially between these bounds so a long
# outage does not hammer the server, while a blip is re-absorbed quickly.
_RECOVER_BACKOFF_MIN = 1.0
_RECOVER_BACKOFF_MAX = 30.0


class MySQLPool:
    def __init__(
        self,
        settings: MySQLSettings,
        *,
        on_lost: Callable[[], None] | None = None,
    ) -> None:
        self._settings = settings
        self._pool: asyncmy.Pool | None = None
        self._keepalive_conn: Any = None  # dedicated, outside the business pool
        self._keepalive_task: asyncio.Task[None] | None = None
        # WeakSet keyed on the connection objects themselves: entries vanish
        # when a pooled connection is GC'd, and id()-reuse cannot resurrect one.
        self._configured_conns: Any = weakref.WeakSet()
        self._on_lost = on_lost
        self.lost = False
        self._closed = False
        # F-32: recovery runs as a single background task; the lock makes the
        # rebuild itself single-flight no matter who arms it.  asyncio.Lock
        # no longer binds to a loop at creation (3.10+), so building it here
        # is safe even before any loop runs.
        self._recover_task: asyncio.Task[None] | None = None
        self._recover_lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._pool is not None and not self._closed

    async def connect(self) -> None:
        await self._build_pool()

    async def _build_pool(self) -> None:
        """(Re)create the business pool plus its dedicated keepalive connection."""
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
            # F-31: asyncmy.connect has no write_timeout parameter (verified
            # against its signature); read_timeout is the only socket deadline
            # it honors -- without it a half-dead server parks every pooled
            # query forever.  Symmetric with the redis socket_timeout fix (F-11).
            read_timeout=s.read_timeout,
        )
        # F-10: heartbeat traffic never competes with business queries for
        # pool slots -- the keepalive runs on its own connection.
        self._keepalive_conn = await asyncmy.connect(
            host=s.host,
            port=s.port,
            user=s.user,
            password=s.password,
            db=s.db_name,
            charset=s.charset,
            autocommit=True,
            read_timeout=s.read_timeout,
        )
        logger.info("mysql connected: %s:%d/%s", s.host, s.port, s.db_name)
        self._keepalive_task = asyncio.get_running_loop().create_task(self._keepalive())
        self._keepalive_task.add_done_callback(self._keepalive_done)

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

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[_TxSession]:
        """One transaction on a dedicated pooled connection (F-43).

        The connection is held for the whole block and returned to the pool
        afterwards; an exception body-side rolls back before propagating, and
        a failed COMMIT also attempts a rollback so the connection cannot go
        back into the pool with an open transaction.
        """
        if self._pool is None or self._closed:
            raise MySQLError("mysql pool is not connected")
        async with self._pool.acquire() as conn:
            await self._ensure_isolation(conn)
            session = _TxSession(conn)
            await session.execute("BEGIN")
            try:
                yield session
            except BaseException:
                with contextlib.suppress(Exception):
                    await session.execute("ROLLBACK")
                raise
            try:
                await session.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):
                    await session.execute("ROLLBACK")
                raise

    async def open_session(self) -> MySQLSession:
        """Open a dedicated transaction session (F-43); see :class:`MySQLSession`."""
        if self._pool is None or self._closed:
            raise MySQLError("mysql pool is not connected")
        session = MySQLSession(self._settings)
        await session.open()
        return session

    async def _ensure_isolation(self, conn: Any) -> None:
        if conn in self._configured_conns:
            return
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"SET SESSION TRANSACTION ISOLATION LEVEL {self._settings.isolation_level}"
            )
        self._configured_conns.add(conn)

    def _keepalive_done(self, task: asyncio.Task[None]) -> None:
        """Surface keepalive termination (F-10): the exception was previously
        raised into a task nobody awaited.

        F-32: declaring the loss is no longer the end of the story -- the
        background recovery task is armed here, so the process rebuilds the
        pool instead of running dead until manually restarted.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        first_loss = not self.lost
        self.lost = True
        logger.critical("mysql keepalive ended: %r -- pool declared dead", exc)
        if first_loss and self._on_lost is not None:
            # Fire only on the False->True transition: a recovered pool that
            # dies again is a fresh incident, but re-arming recovery while
            # already lost must not re-notify (the runtime acts once per loss).
            try:
                self._on_lost()
            except Exception:
                logger.exception("mysql on_lost callback failed")
        self._start_recovery()

    def _start_recovery(self) -> None:
        """Arm the background recovery task; a no-op if one is already alive."""
        if self._closed:
            return
        if self._recover_task is None or self._recover_task.done():
            self._recover_task = asyncio.get_running_loop().create_task(self._recover())

    async def _recover(self) -> None:
        """Rebuild the pool after keepalive declared it dead (F-32).

        Retries forever with exponential backoff (1s -> 30s) so a long outage
        is eventually absorbed; returns as soon as one rebuild succeeds.
        """
        backoff = _RECOVER_BACKOFF_MIN
        while not self._closed and self.lost:
            async with self._recover_lock:  # single-flight: one rebuild at a time
                if self._closed or not self.lost:
                    return  # someone else already recovered / shutting down
                try:
                    await self._teardown_pool()
                    await self._build_pool()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("mysql recovery attempt failed: %r; retrying", exc)
                else:
                    self.lost = False
                    logger.info("mysql pool recovered after connection loss")
                    return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, _RECOVER_BACKOFF_MAX)

    async def _teardown_pool(self) -> None:
        """Best-effort close of the (presumed dead) pool and keepalive socket.

        Called from recovery and from close(); errors are suppressed because
        a socket that is already gone must not abort the shutdown/rebuild.
        """
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            # a finished keepalive task re-raises its MySQLLostError on await;
            # the done-callback has already observed and logged it
            with contextlib.suppress(asyncio.CancelledError, MySQLLostError):
                await self._keepalive_task
            self._keepalive_task = None
        if self._keepalive_conn is not None:
            with contextlib.suppress(Exception):
                await self._keepalive_conn.ensure_closed()
            self._keepalive_conn = None
        if self._pool is not None:
            with contextlib.suppress(Exception):
                self._pool.close()
                await self._pool.wait_closed()
            self._pool = None

    async def _keepalive(self) -> None:
        """Dedicated connection; misses are tolerated up to the configured limit."""
        conn = self._keepalive_conn
        s = self._settings
        misses = 0
        while not self._closed:
            await asyncio.sleep(s.keepalive_interval)
            healthy = False
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    row = await cursor.fetchone()
                healthy = bool(row) and row[0] == 1
            except asyncio.CancelledError:
                raise
            except Exception:
                healthy = False
            if healthy:
                misses = 0
                continue
            misses += 1
            logger.warning("mysql keepalive missed %d/%d", misses, s.keepalive_miss_limit)
            if misses >= s.keepalive_miss_limit:
                raise MySQLLostError("mysql connection lost (keepalive)")

    async def close(self) -> None:
        self._closed = True
        if self._recover_task is not None:
            # F-32: a pending recovery must not outlive the pool it serves
            # (it would rebuild connections right after shutdown).
            self._recover_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recover_task
            self._recover_task = None
        await self._teardown_pool()
