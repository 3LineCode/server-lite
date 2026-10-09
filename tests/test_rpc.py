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

    def route(
        self,
        flag: str,
        payload: bytes,
        target_service_no: int,
        *,
        raise_on_drop: bool = False,
    ) -> None:
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
            def route(
                self,
                flag: str,
                payload: bytes,
                target_service_no: int,
                *,
                raise_on_drop: bool = False,
            ) -> None:
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


class TestRunningKeyCollisionF69:
    """F-69: ``_running`` is keyed by (caller, call_id). Every process numbers
    its calls from 1, so callers A and C both issuing call_id 1 to the same
    server used to collide: the second entry overwrote the first, the first
    task's done-callback popped the second's entry, and a CANCEL for either
    silently matched nothing while the remote task ran on forever."""

    async def test_same_call_id_different_callers_coexist(self, loopback) -> None:
        import msgpack

        rpc, _ = loopback
        states = {"a": False, "c": False}
        started_by = {"a": asyncio.Event(), "c": asyncio.Event()}
        released = {"a": asyncio.Event(), "c": asyncio.Event()}

        @rpc.expose(path="hang")
        async def hang(tag: str) -> str:
            started_by[tag].set()
            await released[tag].wait()
            states[tag] = True
            return tag

        # two distinct callers (11 and 22) both use call_id 7
        rpc.handle_message("@rpc", msgpack.packb([1, 7, 11, "hang", ["a"]]), from_service=11)
        rpc.handle_message("@rpc", msgpack.packb([1, 7, 22, "hang", ["c"]]), from_service=22)
        await asyncio.wait_for(asyncio.gather(started_by["a"].wait(), started_by["c"].wait()), 2.0)
        assert len(rpc._running) == 2, "one call overwrote the other's table entry"

        # CANCEL from caller 22 must kill ONLY caller 22's task
        rpc.handle_message("@rpc", msgpack.packb([3, 7, 22]), from_service=22)
        await asyncio.sleep(0.05)
        assert (22, 7) not in rpc._running
        assert (11, 7) in rpc._running, "caller 11's entry was collateral damage"

        # caller 11's task still runs to completion
        released["a"].set()
        await asyncio.sleep(0.05)
        assert states["a"] is True
        assert states["c"] is False  # 22's was cancelled, never completed

    async def test_cancel_from_wrong_caller_matches_nothing(self, loopback) -> None:
        import msgpack

        rpc, _ = loopback
        started = asyncio.Event()

        @rpc.expose(path="hang")
        async def hang() -> str:
            started.set()
            await asyncio.sleep(5)
            return "never"

        rpc.handle_message("@rpc", msgpack.packb([1, 3, 33, "hang", []]), from_service=33)
        await asyncio.wait_for(started.wait(), 2.0)
        # caller 44 sends a CANCEL for call_id 3 (a call it never made): the
        # composite key means there is no (44, 3) entry -- nothing cancelled
        rpc.handle_message("@rpc", msgpack.packb([3, 3, 44]), from_service=44)
        await asyncio.sleep(0.05)
        assert (33, 3) in rpc._running


class TestAcquireSlotF77:
    """F-77: the inflight slot acquisition cannot leak permits through the
    cancellation window around a grant."""

    def _manager(self, max_inflight: int = 1, inflight_wait: float = 0.2) -> RpcManager:
        router = LoopbackRouter()
        rpc = RpcManager(
            ProtocolGateway(),
            router,
            own_service_no=1,
            max_inflight=max_inflight,
            inflight_wait=inflight_wait,
        )
        router.rpc = rpc
        return rpc

    async def test_cancel_while_parked_leaks_nothing(self) -> None:
        rpc = self._manager()
        await rpc._inflight.acquire()  # the only slot is held

        waiter = asyncio.get_running_loop().create_task(rpc._acquire_slot())
        await asyncio.sleep(0.05)  # parked, no grant possible
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        rpc._inflight.release()  # the holder finishes
        # the slot must still be usable: a fresh acquire succeeds immediately
        assert await asyncio.wait_for(rpc._acquire_slot(), 0.5) is True
        rpc._inflight.release()

    async def test_cancel_racing_the_grant_hands_permit_back(self) -> None:
        """The exact leak window: the grant lands in the same loop iteration
        in which the caller is cancelled. Before F-77 the permit was consumed
        by the never-resumed caller and the server went busy forever."""
        rpc = self._manager()
        await rpc._inflight.acquire()

        waiter = asyncio.get_running_loop().create_task(rpc._acquire_slot())
        await asyncio.sleep(0.05)  # parked on the semaphore
        rpc._inflight.release()  # grant assigned to the waiter's future...
        waiter.cancel()  # ...but the caller is cancelled before it resumes
        with pytest.raises(asyncio.CancelledError):
            await waiter
        await asyncio.sleep(0)  # let the reclaim path settle

        # the raced-in permit must be back: a new acquire succeeds at once
        assert await asyncio.wait_for(rpc._acquire_slot(), 0.5) is True
        rpc._inflight.release()

    async def test_timeout_racing_the_grant_hands_permit_back(self) -> None:
        """Same window on the busy-timeout path (wait_for expiry)."""
        rpc = self._manager(inflight_wait=0.05)
        await rpc._inflight.acquire()

        waiter = asyncio.get_running_loop().create_task(rpc._acquire_slot())
        await asyncio.sleep(0.02)  # parked
        rpc._inflight.release()  # grant lands...
        # ...right at the 50ms busy deadline; either outcome (granted->True
        # or busy->False) is fine, but a grant consumed by a busy return
        # would strand the permit
        granted = await waiter
        if not granted:
            # the permit must have been reclaimed
            assert await asyncio.wait_for(rpc._acquire_slot(), 0.5) is True
            rpc._inflight.release()
        else:
            rpc._inflight.release()

    async def test_execute_cancellation_leaks_no_slot(self) -> None:
        """End-to-end: MSG_CANCEL during the inflight wait leaves the server
        fully serviceable afterwards (used to leak one permit per hit)."""
        import msgpack

        rpc = self._manager(max_inflight=1, inflight_wait=5.0)
        started = asyncio.Event()

        @rpc.expose(path="blocker")
        async def blocker() -> str:
            started.set()
            await asyncio.sleep(10)
            return "never"

        # hold the slot with a blocker
        rpc.handle_message("@rpc", msgpack.packb([1, 1, 11, "blocker", []]), from_service=11)
        await asyncio.wait_for(started.wait(), 2.0)

        # a second CALL parks on the semaphore, then its caller cancels it
        second = asyncio.get_running_loop().create_task(rpc.call(1, blocker, timeout=0.5))
        # the second execution task lands in _running beside the blocker
        # (F-69: same caller, its own call_id -- the loopback router's own
        # call counter also starts at 1, so both coexist under (11, 1))
        for _ in range(100):
            if len(rpc._running) == 2:
                break
            await asyncio.sleep(0.05)
        assert len(rpc._running) == 2
        # the blocker is the injected (11, 1) entry; the parked waiter is the
        # other one
        parked_key = next(k for k in rpc._running if k != (11, 1))
        parked_task = rpc._running[parked_key][0]
        await asyncio.sleep(0.05)  # parked inside _acquire_slot
        parked_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parked_task

        # cancel the caller side too so no pending entry leaks
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert rpc.pending_count() == 0
        assert rpc._inflight._value == 0, "a permit leaked through the cancel window"
        # cleanup: release the blocker that still holds the slot legitimately
        blocker_task = rpc._running.get((11, 1))
        if blocker_task is not None:
            blocker_task[0].cancel()
            await asyncio.sleep(0)


class TestMalformedPayloadF78:
    async def test_deeply_nested_payload_dropped_not_raised(self, loopback) -> None:
        """F-78: adversarially deep msgpack nesting raises RecursionError
        (not ValueError) while unpacking -- it used to escape handle_message."""
        rpc, _ = loopback
        deep = b"\x91" * 100_000 + b"\xc0"  # 100k nested 1-element arrays
        rpc.handle_message("@rpc", deep, from_service=5)  # must not raise
        assert rpc.malformed_messages == 1
        assert rpc.pending_count() == 0

    async def test_busy_notify_dropped_is_counted(self) -> None:
        """F-77: a notify-style CALL (call_id 0) rejected as busy used to
        vanish with no reply and no trace."""
        import msgpack

        router = LoopbackRouter()
        rpc = RpcManager(
            ProtocolGateway(), router, own_service_no=1, max_inflight=1, inflight_wait=0.05
        )
        router.rpc = rpc
        started = asyncio.Event()
        release = asyncio.Event()

        @rpc.expose(path="blocker")
        async def blocker() -> str:
            started.set()
            await release.wait()
            return "done"

        executed: list[bool] = []

        @rpc.expose(path="quick")
        async def quick() -> str:
            executed.append(True)
            return "q"

        rpc.handle_message("@rpc", msgpack.packb([1, 1, 9, "blocker", []]), from_service=9)
        await asyncio.wait_for(started.wait(), 2.0)
        before = len(router.sent)
        rpc.handle_message("@rpc", msgpack.packb([1, 0, 9, "quick", []]), from_service=9)
        for _ in range(50):
            if rpc.busy_notify_drops == 1:
                break
            await asyncio.sleep(0.05)
        assert rpc.busy_notify_drops == 1
        assert executed == []
        assert len(router.sent) == before  # no RESULT for a notify
        release.set()


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


class TestBusOverflowFastFailF128:
    async def test_call_raises_overflow_instead_of_timing_out(self) -> None:
        """A CALL refused by a full bus queue fails at the send site (the
        F-14 path) instead of surfacing as a 10 s fake timeout."""

        from pyline.net.ipc import BusOverflowError

        class OverflowRouter(LoopbackRouter):
            def route(
                self,
                flag: str,
                payload: bytes,
                target_service_no: int,
                *,
                raise_on_drop: bool = False,
            ) -> None:
                if raise_on_drop:
                    raise BusOverflowError("queue full")
                super().route(flag, payload, target_service_no)

        router = OverflowRouter()
        rpc = RpcManager(ProtocolGateway(), router, own_service_no=1)
        router.rpc = rpc

        @rpc.expose
        def hello() -> str:
            return "hi"

        with pytest.raises(BusOverflowError):
            await rpc.call(1, hello)
        assert rpc.pending_count() == 0
