"""Database process service and access facade.

Only the DB process (or a single-process server) holds real MySQL/Redis
connections. Every other process reaches the database through RPC --
``DatabaseAccess`` hides the difference so business code always sees the same
``await db.query(...)`` interface. The dedicated keepalive runs inside the DB
process; non-DB processes simply see RPC timeouts if the DB process dies.
"""

from __future__ import annotations

import logging
from typing import Any

from pyline.db.mysql import MySQLPool
from pyline.db.redis import RedisClient
from pyline.net.rpc import RpcManager

logger = logging.getLogger(__name__)

RPC_QUERY = "pyline.db.query"
RPC_EXECUTE = "pyline.db.execute"
RPC_REDIS_GET = "pyline.db.redis_get"
RPC_REDIS_SET = "pyline.db.redis_set"
RPC_REDIS_DEL = "pyline.db.redis_del"


class NullPool:
    """Placeholder pool for servers that enable redis but not mysql."""

    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        raise ConnectionError("mysql not enabled on this server")

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        raise ConnectionError("mysql not enabled on this server")


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


class DatabaseService:
    """Runs in the DB process: owns pools and exposes them via RPC."""

    def __init__(self, pool: PoolLike, redis: RedisLike) -> None:
        self._pool = pool
        self._redis = redis

    def expose(self, rpc: RpcManager) -> None:
        rpc.register(RPC_QUERY, self.rpc_query)
        rpc.register(RPC_EXECUTE, self.rpc_execute)
        rpc.register(RPC_REDIS_GET, self.rpc_redis_get)
        rpc.register(RPC_REDIS_SET, self.rpc_redis_set)
        rpc.register(RPC_REDIS_DEL, self.rpc_redis_del)

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
        if self._local is not None:
            rows = await self._local_pool.query(sql, args)
            return [tuple(row) for row in rows]
        rows = await self._remote_call(RPC_QUERY, sql, list(args))
        return [tuple(row) for row in rows]

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        if self._local is not None:
            return int(await self._local_pool.execute(sql, args))
        return int(await self._remote_call(RPC_EXECUTE, sql, list(args)))

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
