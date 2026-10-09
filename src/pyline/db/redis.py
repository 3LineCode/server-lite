"""Redis access on the official redis.asyncio client (prototype issue #15).

F-213: the client also runs a dedicated liveness probe (the mysql-keepalive
analogue). redis-py silently reconnects on the next command after an outage,
which is exactly why the outage needs its own signal: without a probe, a dead
Redis produced no alarm, no metric and no recovery event -- the first
evidence was a business call failing at a random later time.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Any, cast

from redis import asyncio as aioredis

from pyline.config.models import RedisSettings
from pyline.obs.metrics import shared_counter

logger = logging.getLogger(__name__)

# F-213: transitions into the lost state (not per-miss -- one outage is one
# sample, mirroring the mysql_lost alarm semantics).
_REDIS_LOST = shared_counter(
    "pyline_redis_lost_total", "Redis outages detected by the keepalive probe"
)


class RedisClient:
    def __init__(
        self,
        settings: RedisSettings,
        *,
        on_lost: Callable[[], None] | None = None,
    ) -> None:
        self._settings = settings
        self._on_lost = on_lost
        self._client: aioredis.Redis | None = None
        self._keepalive_task: asyncio.Task[None] | None = None

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
        # F-213: start/replace the liveness probe with the live connection.
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
        self._keepalive_task = asyncio.get_running_loop().create_task(self._keepalive())
        logger.info("redis connected: %s:%d db=%d", s.host, s.port, s.db_index)

    async def _keepalive(self) -> None:
        """PING probe; sustained misses raise the lost alarm (F-213).

        Recovery is self-service (redis-py reconnects on the next command),
        so unlike the mysql pool there is nothing to rebuild -- the alarm is
        the deliverable, and one transition per outage (not one per miss)."""
        s = self._settings
        misses = 0
        lost_announced = False
        while self._client is not None:
            await asyncio.sleep(s.keepalive_interval)
            healthy = False
            try:
                client = self._require()
                healthy = await client.ping() is True
            except asyncio.CancelledError:
                raise
            except Exception:
                healthy = False
            if healthy:
                misses = 0
                lost_announced = False
                continue
            misses += 1
            logger.warning("redis keepalive missed %d/%d", misses, s.keepalive_miss_limit)
            if misses >= s.keepalive_miss_limit and not lost_announced:
                lost_announced = True
                _REDIS_LOST.inc()
                logger.critical(
                    "redis lost: %d consecutive keepalive misses (%s:%d db=%d)",
                    misses,
                    s.host,
                    s.port,
                    s.db_index,
                )
                if self._on_lost is not None:
                    try:
                        self._on_lost()
                    except Exception:
                        logger.exception("redis on_lost callback failed")

    async def close(self) -> None:
        task, self._keepalive_task = self._keepalive_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
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
