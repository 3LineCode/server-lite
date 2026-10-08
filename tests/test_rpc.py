"""RPC: in-process loopback via a fake router.

Exercises result/timeout/error paths and -- critically -- that the pending
table never leaks (prototype bug #1).
"""

from __future__ import annotations

import asyncio

import pytest

from pyline.net.gateway import ProtocolGateway
from pyline.net.rpc import (
    RpcManager,
    RpcRemoteError,
    RpcTimeoutError,
    RpcUnknownFunctionError,
)


class LoopbackRouter:
    """Delivers every send straight back into the local rpc manager."""

    def __init__(self) -> None:
        self.rpc: RpcManager | None = None
        self.sent: list[tuple[int, str, bytes]] = []

    def route(self, flag: str, payload: bytes, target_service_no: int) -> None:
        self.sent.append((target_service_no, flag, payload))
        assert self.rpc is not None
        self.rpc.handle_message(flag, payload)


@pytest.fixture()
def loopback():
    router = LoopbackRouter()
    gateway = ProtocolGateway()
    rpc = RpcManager(gateway, router, own_service_no=1)
    router.rpc = rpc
    return rpc, router


async def test_call_and_result(loopback) -> None:
    rpc, _ = loopback

    @rpc.expose
    async def add(a: int, b: int) -> int:
        return a + b

    assert await rpc.call(1, add, 2, 3) == 5
    assert rpc.pending_count() == 0


async def test_sync_function(loopback) -> None:
    rpc, _ = loopback
    rpc.register("sync.echo", lambda x: f"echo:{x}")
    assert await rpc.call(1, "sync.echo", "hi") == "echo:hi"


async def test_remote_exception_surfaces(loopback) -> None:
    rpc, _ = loopback

    @rpc.expose
    def boom() -> None:
        raise ValueError("kaputt")

    with pytest.raises(RpcRemoteError, match="kaputt"):
        await rpc.call(1, boom)


async def test_unknown_function(loopback) -> None:
    rpc, _ = loopback
    with pytest.raises(RpcUnknownFunctionError):
        await rpc.call(1, "no.such.func")


async def test_timeout_cleans_pending(loopback) -> None:
    rpc, _ = loopback

    async def hang() -> str:
        await asyncio.sleep(10)
        return "never"

    rpc.register("hang", hang)
    with pytest.raises(RpcTimeoutError):
        await rpc.call(1, "hang", timeout=0.05)
    assert rpc.pending_count() == 0  # <-- the prototype leaked here


async def test_caller_cancellation_cleans_pending(loopback) -> None:
    rpc, _ = loopback

    async def hang() -> str:
        await asyncio.sleep(10)
        return "never"

    rpc.register("hang2", hang)
    task = asyncio.get_running_loop().create_task(rpc.call(1, "hang2", timeout=10))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rpc.pending_count() == 0


async def test_notify_fire_and_forget(loopback) -> None:
    rpc, _router = loopback
    hits: list[str] = []

    def note(x: str) -> None:
        hits.append(x)

    rpc.register("note", note)
    rpc.notify(1, "note", "n1")
    await asyncio.sleep(0.05)
    assert hits == ["n1"]
    assert rpc.pending_count() == 0


async def test_expose_duplicate_rejected(loopback) -> None:
    rpc, _ = loopback

    @rpc.expose
    def one() -> int:
        return 1

    with pytest.raises(ValueError, match="already registered"):
        rpc.register(one.__module__ + "." + one.__qualname__, one)
