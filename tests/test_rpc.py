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
    """Delivers every send straight back into the local rpc manager.

    Models message origin faithfully (F-40): a CALL is dispatched as coming
    from this service (the caller), a RESULT/CANCEL as coming from the service
    the message was routed to (the callee that replied). ``spoof_from``
    overrides the delivered origin to simulate injection attempts; set it to
    ``0`` to simulate an unknown-origin delivery.
    """

    def __init__(self, own_service_no: int = 1) -> None:
        self.rpc: RpcManager | None = None
        self.own_service_no = own_service_no
        self.sent: list[tuple[int, str, bytes]] = []
        self.spoof_from: int | None = None
        # the callee identity results claim to come from: the target of the
        # most recent CALL this router delivered
        self.last_call_target: int = own_service_no

    def route(self, flag: str, payload: bytes, target_service_no: int) -> None:
        import msgpack

        self.sent.append((target_service_no, flag, payload))
        assert self.rpc is not None
        kind = msgpack.unpackb(payload, raw=False)[0]
        if kind == 1:
            self.last_call_target = target_service_no
        if self.spoof_from is not None:
            origin = self.spoof_from
        else:
            origin = self.own_service_no if kind == 1 else self.last_call_target
        self.rpc.handle_message(flag, payload, origin)


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


class TestHardeningF13F14F18:
    async def test_rpc_message_arity_validation(self, loopback) -> None:
        """F-13: malformed messages are counted and dropped, never raised."""
        import msgpack

        rpc, _ = loopback
        rpc.handle_message("@rpc", msgpack.packb([1, 2]), from_service=5)  # CALL arity 2
        rpc.handle_message("@rpc", msgpack.packb([2, 9]), from_service=5)  # RESULT arity 2
        rpc.handle_message("@rpc", msgpack.packb([3]), from_service=5)  # CANCEL arity 1
        rpc.handle_message("@rpc", msgpack.packb([1, "x", "y", "z", []]), from_service=5)
        rpc.handle_message("@rpc", msgpack.packb([9]), from_service=5)  # unknown kind
        assert rpc.pending_count() == 0  # nothing exploded, nothing leaked

    async def test_rpc_send_failure_cleans_pending(self, loopback) -> None:
        """F-14: a send exception fails the call now, not via a stray timer."""

        class BrokenRouter(LoopbackRouter):
            def route(self, flag: str, payload: bytes, target_service_no: int) -> None:
                raise ConnectionError("bus gone")

        router = BrokenRouter()
        rpc = RpcManager(ProtocolGateway(), router, own_service_no=1)
        router.rpc = rpc

        @rpc.expose
        def hello() -> str:
            return "hi"

        with pytest.raises(ConnectionError):
            await rpc.call(1, hello)
        assert rpc.pending_count() == 0

    async def test_rpc_unserializable_args_fail_fast(self, loopback) -> None:
        """F-14: bad arguments raise TypeError immediately (was fake timeout)."""
        rpc, _ = loopback

        @rpc.expose
        def show(x: object) -> object:
            return x

        with pytest.raises(TypeError, match="not serializable"):
            await rpc.call(1, show, {1, 2, 3})  # set is not msgpack-compatible
        assert rpc.pending_count() == 0

    async def test_rpc_unserializable_result_raises_rpcerror(self, loopback) -> None:
        """F-14: the caller sees a remote error, not a timeout."""
        rpc, _ = loopback

        @rpc.expose
        def weird() -> object:
            return {"unserializable": {1, 2}}

        with pytest.raises(RpcRemoteError, match="not serializable"):
            await rpc.call(1, weird)

    async def test_timeout_sends_cancel_to_remote(self, loopback) -> None:
        """F-18: a timed-out call best-effort cancels remote execution."""
        import msgpack

        rpc, router = loopback
        started = asyncio.Event()

        @rpc.expose
        async def hang() -> str:
            started.set()
            await asyncio.sleep(5)
            return "late"

        task = asyncio.get_running_loop().create_task(rpc.call(1, hang, timeout=0.1))
        await asyncio.wait_for(started.wait(), 1)
        with pytest.raises(RpcTimeoutError):
            await task
        cancels = [
            msgpack.unpackb(p)
            for (_t, f, p) in router.sent
            if f == "@rpc"
            and isinstance(msgpack.unpackb(p), list)
            and msgpack.unpackb(p)[:1] == [3]
        ]
        assert cancels, "expected a MSG_CANCEL after timeout"

    async def test_current_caller(self, loopback) -> None:
        """Old-repo RpcFromServer equivalent: caller visible inside handler."""
        from pyline.net.rpc import current_caller

        rpc, _ = loopback
        seen: list[int] = []

        @rpc.expose
        def who_calls() -> int:
            seen.append(current_caller())
            return current_caller()

        # loopback delivers back to self, so the caller is own service no 1
        assert await rpc.call(7, who_calls) == 1
        assert seen == [1]
        assert current_caller() == 0  # reset after the call


class TestInflightLimitF19:
    async def test_busy_call_rejected_not_executed(self) -> None:
        """F-19: with one execution slot, a CALL arriving while another still
        runs waits ``inflight_wait`` and then gets a busy error -- the
        function is never executed."""
        router = LoopbackRouter()
        rpc = RpcManager(
            ProtocolGateway(),
            router,
            own_service_no=1,
            max_inflight=1,
            inflight_wait=0.1,
        )
        router.rpc = rpc

        started = asyncio.Event()
        release = asyncio.Event()

        @rpc.expose
        async def blocker() -> str:
            started.set()
            await release.wait()
            return "done"

        executed: list[bool] = []

        @rpc.expose
        async def quick() -> str:
            executed.append(True)
            return "quick"

        first = asyncio.get_running_loop().create_task(rpc.call(1, blocker, timeout=10))
        await asyncio.wait_for(started.wait(), 1.0)  # slot is now held

        with pytest.raises(RpcRemoteError, match="busy"):
            await rpc.call(1, quick, timeout=10)
        assert executed == []  # rejected before execution
        assert rpc.busy_rejects == 1

        release.set()
        assert await first == "done"
        # the slot was released: subsequent calls execute normally again
        assert await rpc.call(1, quick, timeout=10) == "quick"
        assert executed == [True]
        assert rpc.pending_count() == 0


class TestOriginValidationF40:
    async def test_result_from_wrong_origin_times_out(self, loopback) -> None:
        """F-40: a result arriving from a service other than the called target
        is an injection attempt -- dropped, the caller's own timeout fires."""
        rpc, router = loopback

        @rpc.expose
        def echo(x: str) -> str:
            return x

        router.spoof_from = 99
        with pytest.raises(RpcTimeoutError):
            await rpc.call(1, echo, "hi", timeout=0.1)
        assert rpc.pending_count() == 0

    async def test_result_with_unknown_origin_times_out(self, loopback) -> None:
        """F-40: from_service=0 (no validated origin) is untrusted."""
        rpc, router = loopback

        @rpc.expose
        def echo(x: str) -> str:
            return x

        router.spoof_from = 0
        with pytest.raises(RpcTimeoutError):
            await rpc.call(1, echo, "hi", timeout=0.1)
        assert rpc.pending_count() == 0

    async def test_call_with_unknown_origin_dropped(self, loopback) -> None:
        """F-40: a CALL without a validated origin never executes."""
        import msgpack

        rpc, router = loopback
        executed: list[bool] = []
        rpc.register("probe.f40", lambda: executed.append(True))
        router.spoof_from = 0
        rpc.handle_message("@rpc", msgpack.packb([1, 1, 1, "probe.f40", []]), from_service=0)
        await asyncio.sleep(0.05)
        assert executed == []

    async def test_validated_origin_overrides_body_claim(self, loopback) -> None:
        """F-40/F-39: when the transport-validated origin disagrees with the
        body's ``from`` claim, the validated origin wins."""
        from pyline.net.rpc import current_caller

        rpc, router = loopback
        seen: list[int] = []

        @rpc.expose
        async def who() -> int:
            seen.append(current_caller())
            router.spoof_from = None  # reply delivers with the honest origin
            return 7

        router.spoof_from = 42  # CALL arrives validated from 42, body claims 1
        assert await rpc.call(1, who, timeout=5) == 7
        assert seen == [42]

    async def test_cancel_from_non_caller_ignored(self, loopback) -> None:
        """F-40: a forged CANCEL from a service that did not issue the call
        cannot kill the running task."""
        import msgpack

        rpc, _router = loopback
        started = asyncio.Event()
        state = {"cancelled": False}

        @rpc.expose
        async def hang() -> str:
            started.set()
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                state["cancelled"] = True
                raise
            return "done"

        task = asyncio.get_running_loop().create_task(rpc.call(1, hang, timeout=10))
        await asyncio.wait_for(started.wait(), 1.0)
        # forged CANCEL for call_id 1, claiming to be (and validated as) 99
        rpc.handle_message("@rpc", msgpack.packb([3, 1, 99]), from_service=99)
        await asyncio.sleep(0.05)
        assert state["cancelled"] is False
        # the genuine caller cancelling still works
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.05)
        assert state["cancelled"] is True
