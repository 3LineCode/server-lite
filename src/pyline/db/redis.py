"""Redis access on the official redis.asyncio client (prototype issue #15)."""

from __future__ import annotations

import logging
from typing import Any, cast

from redis import asyncio as aioredis

from pyline.config.models import RedisSettings

logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    @property
    def connected(self) -> bool:
        return self._client is not None

    async def connect(self) -> None:
        s = self._settings
        self._client = aioredis.Redis(
            host=s.host,
            port=s.port,
            password=s.password.get_secret_value() if s.password else None,
            db=s.db_index,
            max_connections=s.conn_cnt,
            decode_responses=True,
            socket_timeout=s.socket_timeout,
            socket_connect_timeout=s.socket_timeout,
            health_check_interval=s.health_check_interval,
        )
        await self._client.ping()
        logger.info("redis connected: %s:%d db=%d", s.host, s.port, s.db_index)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _require(self) -> aioredis.Redis:
        if self._client is None:
            raise ConnectionError("redis client is not connected")
        return self._client

    async def set(self, key: str, value: Any) -> None:
        await self._require().set(key, value)

    async def get(self, key: str, default: str | None = None) -> str | None:
        # decode_responses=True: values arrive as str | None, never bytes
        # (the stub type is the union because the flag is per-instance).
        raw = await self._require().get(key)
        if raw is None:
            return default
        return cast("str", raw)

    async def delete(self, *keys: str) -> int:
        return int(await self._require().delete(*keys))
