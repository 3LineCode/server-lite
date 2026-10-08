"""Database facade: unified local/remote access (old MysqlExecute/MysqlQuery
and the Redis family, with async semantics instead of callbacks)."""

from __future__ import annotations

from typing import Any, cast

from pyline import api
from pyline.db.service import DatabaseAccess

MYSQL_INT = "BIGINT"
MYSQL_STR = "VARCHAR"
MYSQL_TEXT = "MEDIUMTEXT"
MYSQL_DATA = "MEDIUMBLOB"

__all__ = [
    "MYSQL_DATA",
    "MYSQL_INT",
    "MYSQL_STR",
    "MYSQL_TEXT",
    "execute",
    "query",
    "redis_delete",
    "redis_delete_many",
    "redis_get",
    "redis_set",
]


def _db() -> DatabaseAccess:
    return cast(DatabaseAccess, api.service("db"))


async def execute(sql: str, *args: Any) -> int:
    """Run a statement (auto-committed); returns affected row count.

    Routes to the local pool or the DB process transparently.
    """
    return await _db().execute(sql, args)


async def query(sql: str, *args: Any) -> list[tuple[Any, ...]]:
    """Run a query; returns the fetched rows."""
    return await _db().query(sql, args)


async def redis_set(key: str, value: Any) -> None:
    await _db().redis_set(key, value)


async def redis_get(key: str, default: str | None = None) -> str | None:
    value = await _db().redis_get(key)
    return default if value is None else value


async def redis_delete(*keys: str) -> int:
    return await _db().redis_del(*keys)


async def redis_delete_many(keys: list[str]) -> int:
    """Old RedisMultiDel."""
    if not keys:
        return 0
    return int(await _db().redis_del(*keys))
