"""Direct coverage for the api facade submodules business code touches most
(api/db, api/rpc, api/orm, api/log -- 0% via the lazy-import design, F-88)
and the db/redis.py unit paths that the live-redis marker hides from the
coverage job."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pyline import api
from pyline.config.models import RedisSettings
from pyline.core.scheduler import Scheduler
from pyline.db.redis import RedisClient
from pyline.db.service import DatabaseAccess, DatabaseService
from pyline.net.rpc import RpcManager
from pyline.runtime import build_context
from tests.test_transaction import FakeSessionPool


@pytest.fixture()
async def bound_ctx(config_dir, tmp_path):
    ctx = build_context(config_dir, 10001, "main", 0, 0)
    ctx.scheduler = Scheduler(loop=asyncio.get_running_loop())
    api.bind(ctx)
    yield ctx
    api.unbind()


# --------------------------------------------------------------------------- #
# api/db -- through a REAL DatabaseAccess on the local path (the typed
# service accessor rejects hand-rolled stand-ins, by design)
# --------------------------------------------------------------------------- #


class _RecordingRedis:
    def __init__(self) -> None:
        self.log: list[Any] = []

    async def get(self, key: str) -> str | None:
        return "v" if key == "known" else None

    async def set(self, key: str, value: str) -> None:
        self.log.append(("set", key, value))

    async def delete(self, *keys: str) -> int:
        return len(keys)


@pytest.fixture()
def db_access() -> tuple[DatabaseAccess, FakeSessionPool, _RecordingRedis]:
    pool = FakeSessionPool()
    redis = _RecordingRedis()
    return DatabaseAccess(local=DatabaseService(pool, redis)), pool, redis  # type: ignore[arg-type]


class TestApiDbFacade:
    async def test_query_execute_delegate_with_varargs(self, bound_ctx, db_access) -> None:
        access, pool, _redis = db_access
        bound_ctx.services["db"] = access
        assert await api.db.query("SELECT %s", 5) == []
        assert await api.db.execute("UPDATE t SET a=%s WHERE id=%s", 1, 2) == 0
        assert pool.plain_execute_calls == 1

    async def test_transaction_context_delegates(self, bound_ctx, db_access) -> None:
        access, pool, _redis = db_access
        bound_ctx.services["db"] = access
        async with api.db.transaction():
            await api.db.execute("INSERT INTO t VALUES (%s)", 9)
        assert pool.log == ["BEGIN", ("EXEC", "INSERT INTO t VALUES (%s)", (9,)), "COMMIT"]

    async def test_redis_family(self, bound_ctx, db_access) -> None:
        access, _pool, redis = db_access
        bound_ctx.services["db"] = access
        await api.db.redis_set("k", "v")
        assert await api.db.redis_get("known") == "v"
        assert await api.db.redis_get("missing") is None
        assert await api.db.redis_get("missing", "dflt") == "dflt"
        assert await api.db.redis_delete("a", "b") == 2
        assert ("set", "k", "v") in redis.log

    async def test_constants_exported(self, bound_ctx) -> None:
        assert api.db.MYSQL_INT == "BIGINT"
        assert api.db.MYSQL_DATA == "MEDIUMBLOB"


# --------------------------------------------------------------------------- #
# api/rpc
# --------------------------------------------------------------------------- #


class _RecordingRpc(RpcManager):
    """Only the delegated surface is under test; __init__ is skipped."""

    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def call(self, target: int, path: str, *args: Any, timeout: float = 10.0) -> Any:
        self.calls.append(("call", target, path, args, timeout))
        return "ok"

    def notify(self, target: int, path: str, *args: Any) -> None:
        self.calls.append(("notify", target, path, args))


class TestApiRpcFacade:
    async def test_call_and_notify_delegate(self, bound_ctx) -> None:
        fake = _RecordingRpc()
        bound_ctx.services["rpc"] = fake
        assert await api.rpc.call(5, "pyline.db.query", "SELECT 1", timeout=3.0) == "ok"
        api.rpc.notify(5, "probe", 1, 2)
        assert fake.calls == [
            ("call", 5, "pyline.db.query", ("SELECT 1",), 3.0),
            ("notify", 5, "probe", (1, 2)),
        ]

    async def test_current_caller_is_zero_outside_a_handler(self, bound_ctx) -> None:
        assert api.rpc.current_caller() == 0


# --------------------------------------------------------------------------- #
# api/orm
# --------------------------------------------------------------------------- #


class TestApiOrmFacade:
    async def test_make_saver_uses_the_bound_factory(self, bound_ctx) -> None:
        made: list[Any] = []

        def factory(table: str, column: str, key: object, codec: object = None, **kw: object):
            made.append((table, column, key, codec, kw))
            return "SAVER"

        bound_ctx.services["make_saver"] = factory
        result = api.orm.make_saver("tbl_player", "data", 7, None, auto_save=True)
        assert result == "SAVER"
        assert made == [("tbl_player", "data", 7, None, {"auto_save": True})]

    async def test_reexports(self, bound_ctx) -> None:
        from pyline.db.orm import DataSaver, TrackableModel
        from pyline.db.tracked import TrackedDict, TrackedList

        assert api.orm.DataSaver is DataSaver
        assert api.orm.TrackableModel is TrackableModel
        assert api.orm.TrackedDict is TrackedDict
        assert api.orm.TrackedList is TrackedList


# --------------------------------------------------------------------------- #
# api/log
# --------------------------------------------------------------------------- #


class TestApiLogFacade:
    async def test_file_channel_writes_under_log_dir(
        self, bound_ctx, tmp_path, monkeypatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".aiolog").mkdir()  # boot's log setup owns this in production
        logger = api.log.file("audit")
        logger.info("hello facade")
        for handler in logger.handlers:
            handler.flush()
        target = tmp_path / ".aiolog" / "audit.log"
        assert target.is_file()
        assert "hello facade" in target.read_text(encoding="utf-8")

    async def test_debug_channel_lands_in_debug_subdir(
        self, bound_ctx, tmp_path, monkeypatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".aiolog" / "debug").mkdir(parents=True)
        logger = api.log.file_debug("trace")
        logger.info("dbg")
        for handler in logger.handlers:
            handler.flush()
        assert (tmp_path / ".aiolog" / "debug" / "trace.log").is_file()


# --------------------------------------------------------------------------- #
# db/redis.py unit paths (the round-trip tests carry the `redis` marker and
# are excluded from the coverage run)
# --------------------------------------------------------------------------- #


class _FakeRedisCore:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.store: dict[str, str] = {}
        self.closed = False

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self.closed = True

    async def set(self, key: str, value: Any) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, *keys: str) -> int:
        removed = sum(1 for k in keys if self.store.pop(k, None) is not None)
        return removed


class TestRedisClientUnit:
    async def test_not_connected_fails_loudly(self) -> None:
        client = RedisClient(RedisSettings())
        assert not client.connected
        with pytest.raises(ConnectionError, match="not connected"):
            await client.get("k")
        with pytest.raises(ConnectionError, match="not connected"):
            await client.set("k", "v")

    async def test_connect_constructs_hardened_client(self, monkeypatch) -> None:
        created: dict[str, _FakeRedisCore] = {}

        def factory(**kwargs: Any) -> _FakeRedisCore:
            core = _FakeRedisCore(**kwargs)
            created["core"] = core
            return core

        monkeypatch.setattr("pyline.db.redis.aioredis.Redis", factory)
        settings = RedisSettings()
        client = RedisClient(settings)
        await client.connect()
        assert client.connected
        # F-11 lineage: the socket timeouts must reach the real client.
        assert created["core"].kwargs["socket_timeout"] == settings.socket_timeout
        assert created["core"].kwargs["socket_connect_timeout"] == settings.socket_timeout
        assert created["core"].kwargs["health_check_interval"] == settings.health_check_interval
        assert created["core"].kwargs["decode_responses"] is True
        assert created["core"].kwargs["max_connections"] == settings.conn_cnt

    async def test_round_trip_and_close(self, monkeypatch) -> None:
        monkeypatch.setattr("pyline.db.redis.aioredis.Redis", _FakeRedisCore)
        client = RedisClient(RedisSettings())
        await client.connect()
        await client.set("a", "1")
        assert await client.get("a") == "1"
        assert await client.get("missing") is None
        assert await client.get("missing", "dflt") == "dflt"
        assert await client.delete("a", "nope") == 1
        await client.close()
        assert not client.connected
        with pytest.raises(ConnectionError):
            await client.get("a")
