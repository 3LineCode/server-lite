"""Integration tests against a live Redis (CI service or local dev).

Env overrides: PYLINE_TEST_REDIS_HOST / _PORT / _PASSWORD.
Skipped automatically when no usable server is reachable.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from pyline.config.models import RedisSettings
from pyline.db.redis import RedisClient

pytestmark = pytest.mark.redis


def _redis_host_port() -> tuple[str, int]:
    return (
        os.environ.get("PYLINE_TEST_REDIS_HOST", "127.0.0.1"),
        int(os.environ.get("PYLINE_TEST_REDIS_PORT", "6379")),
    )


def _redis_reachable() -> bool:
    host, port = _redis_host_port()
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def _settings() -> RedisSettings:
    host, port = _redis_host_port()
    password = os.environ.get("PYLINE_TEST_REDIS_PASSWORD")
    return RedisSettings(
        host=host,
        port=port,
        password=password,
        socket_timeout=3.0,
    )


@pytest.mark.skipif(not _redis_reachable(), reason="no reachable Redis")
class TestRedisRoundtrip:
    async def test_set_get_delete_multidel(self) -> None:
        client = RedisClient(_settings())
        await client.connect()
        try:
            prefix = f"pyline-it-{uuid.uuid4().hex[:8]}"
            await client.set(f"{prefix}:a", "1")
            await client.set(f"{prefix}:b", "2")
            assert await client.get(f"{prefix}:a") == "1"
            assert await client.get(f"{prefix}:missing", default="x") == "x"
            assert await client.delete(f"{prefix}:a") == 1
            assert await client.delete(f"{prefix}:a", f"{prefix}:b") == 1  # multi-del
            assert await client.get(f"{prefix}:b") is None
        finally:
            await client.close()

    async def test_unconnected_rejected(self) -> None:
        client = RedisClient(_settings())
        with pytest.raises(ConnectionError):
            await client.get("k")
