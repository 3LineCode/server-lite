"""ZMQ bus: dealer->router->dealer routing over a real TCP transport."""

from __future__ import annotations

import asyncio

import pytest

from pyline.config.loader import load_project_settings, load_server_registry, load_table_defs
from pyline.core.context import PROCESS_DB, PROCESS_MAIN, Context
from pyline.net.gateway import ProtocolGateway
from pyline.net.ipc import ZmqBus
from pyline.net.network import Network, pack_call


class _CaptureNet(Network):
    flag = "test"

    def __init__(self, gateway, box: dict) -> None:
        super().__init__(gateway)
        self.box = box
        self.subscribe(1, self.on_ping)

    async def on_ping(self, value: str) -> None:
        self.box["got"] = value
        self.box["event"].set()


class TestNetworkInflightCap:
    async def test_overflow_drops_and_counts(self) -> None:
        """A plain network used to spawn one task per inbound frame with no
        bound -- a token-holding client could stack unlimited handler tasks
        (F-19 bounded only the RPC face). The cap drops and counts; it must
        never block the dispatch path."""
        gw = ProtocolGateway()

        class SlowNet(Network):
            flag = "slow"

            def __init__(self, gateway) -> None:
                super().__init__(gateway, max_inflight=2)
                self.started = asyncio.Event()
                self.ran = 0
                self.subscribe(1, self.on_slow)

            async def on_slow(self) -> None:
                self.ran += 1
                await self.started.wait()

        net = SlowNet(gw)
        for _ in range(4):
            net.handle_message("slow", pack_call(1), from_service=7)
        await asyncio.sleep(0.01)  # let the two admitted tasks start
        assert net.ran == 2  # the cap admitted two handlers
        assert net._overflowed == 2  # the rest were dropped and counted
        net.started.set()
        await asyncio.sleep(0.01)
        assert net.ran == 2  # the dropped ones never ran


def free_tcp_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_ctx(config_dir, tmp_path, *, main: bool, index: int, port: int) -> Context:
    settings = load_project_settings(config_dir)
    settings = settings.model_copy(
        update={
            "zeromq": settings.zeromq.model_copy(update={"bind_host": f"tcp://127.0.0.1:{port}"})
        }
    )
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=registry.entry(10001),
        process_type=PROCESS_MAIN if main else PROCESS_DB,
        process_index=index,
        main_pid=0,
    )


@pytest.mark.integration
async def test_dealer_to_router_and_back(config_dir, tmp_path) -> None:
    port = free_tcp_port()
    ctx_main = make_ctx(config_dir, tmp_path, main=True, index=0, port=port)
    ctx_sub = make_ctx(config_dir, tmp_path, main=False, index=1, port=port)

    gw_main = ProtocolGateway()
    gw_sub = ProtocolGateway()

    bus_main = ZmqBus(ctx_main, gw_main)
    bus_sub = ZmqBus(ctx_sub, gw_sub)

    box_main: dict = {"event": asyncio.Event()}
    box_sub: dict = {"event": asyncio.Event()}
    _CaptureNet(gw_main, box_main)
    _CaptureNet(gw_sub, box_sub)

    await bus_main.start()  # ROUTER binds
    await bus_sub.start()  # DEALER connects

    try:
        await asyncio.sleep(0.3)  # let the connection establish

        # sub -> main
        bus_sub.send(ctx_main.service_no, "test", pack_call(1, "hello-main"))
        await asyncio.wait_for(box_main["event"].wait(), 5.0)
        assert box_main["got"] == "hello-main"

        # main -> sub
        bus_main.send(ctx_sub.service_no, "test", pack_call(1, "hello-sub"))
        await asyncio.wait_for(box_sub["event"].wait(), 5.0)
        assert box_sub["got"] == "hello-sub"

        # local loopback on main
        box_main["event"].clear()
        bus_main.send(10001, "test", pack_call(1, "self"))
        await asyncio.wait_for(box_main["event"].wait(), 5.0)
        assert box_main["got"] == "self"
    finally:
        await bus_sub.close()
        await bus_main.close()


@pytest.mark.integration
async def test_ipc_send_order_preserved(config_dir, tmp_path) -> None:
    """F-15: the single writer per destination keeps per-peer FIFO order."""
    port = free_tcp_port()
    ctx_main = make_ctx(config_dir, tmp_path, main=True, index=0, port=port)
    ctx_sub = make_ctx(config_dir, tmp_path, main=False, index=1, port=port)
    gw_main, gw_sub = ProtocolGateway(), ProtocolGateway()
    bus_main, bus_sub = ZmqBus(ctx_main, gw_main), ZmqBus(ctx_sub, gw_sub)

    received: list[int] = []
    done = asyncio.Event()

    class OrderNet(Network):
        flag = "order"

        def __init__(self, gateway: ProtocolGateway) -> None:
            super().__init__(gateway)
            self.subscribe(1, self.on_seq)

        def on_seq(self, seq: int) -> None:
            received.append(seq)
            if seq == 199:
                done.set()

    OrderNet(gw_sub)
    await bus_main.start()
    await bus_sub.start()
    try:
        await asyncio.sleep(0.3)
        for i in range(200):
            bus_main.send(ctx_sub.service_no, "order", pack_call(1, i))
        await asyncio.wait_for(done.wait(), 10.0)
        assert len(received) == 200
        assert received == sorted(received), "per-destination order broken"
    finally:
        await bus_sub.close()
        await bus_main.close()


@pytest.mark.integration
async def test_ipc_destination_cap(config_dir, tmp_path) -> None:
    """F-24: the ROUTER caps tracked destinations -- a third unknown target is
    dropped and counted, never allocated a queue or writer task."""
    port = free_tcp_port()
    ctx_main = make_ctx(config_dir, tmp_path, main=True, index=0, port=port)
    ctx_sub = make_ctx(config_dir, tmp_path, main=False, index=1, port=port)
    gw_main, gw_sub = ProtocolGateway(), ProtocolGateway()
    bus_main = ZmqBus(ctx_main, gw_main, max_destinations=2)
    bus_sub = ZmqBus(ctx_sub, gw_sub)
    await bus_main.start()
    await bus_sub.start()
    try:
        await asyncio.sleep(0.3)
        # three same-machine targets no DEALER ever registered for
        unknown = [5 * 100_000 + 10001, 6 * 100_000 + 10001, 7 * 100_000 + 10001]
        for target in unknown:
            bus_sub.send(target, "test", pack_call(1, "nobody-home"))
        for _ in range(100):
            if bus_main.dest_overflow >= 1:
                break
            await asyncio.sleep(0.05)
        assert bus_main.dest_overflow == 1, "overflow message was not counted"
        assert set(bus_main._peer_queues) == set(unknown[:2]), "third target allocated a queue"
        assert len(bus_main._peer_tasks) == 2
    finally:
        await bus_sub.close()
        await bus_main.close()


@pytest.mark.integration
async def test_ipc_slow_dealer_does_not_block_bus(config_dir, tmp_path) -> None:
    """F-15: a DEALER that never reads must not stall the ROUTER's recv loop
    (the old inline-forward design blocked the whole bus on one slow peer)."""
    port = free_tcp_port()
    ctx_main = make_ctx(config_dir, tmp_path, main=True, index=0, port=port)
    ctx_sub = make_ctx(config_dir, tmp_path, main=False, index=1, port=port)
    gw_main, gw_sub = ProtocolGateway(), ProtocolGateway()
    bus_main, bus_sub = ZmqBus(ctx_main, gw_main), ZmqBus(ctx_sub, gw_sub)

    got_fast = asyncio.Event()

    class FastNet(Network):
        flag = "fast"

        def __init__(self, gateway: ProtocolGateway) -> None:
            super().__init__(gateway)
            self.subscribe(1, lambda: got_fast.set())

    FastNet(gw_sub)

    import zmq
    import zmq.asyncio

    slow_no = 3 * 100_000 + 10001  # a subprocess index 3 that never starts
    slow = zmq.asyncio.Context().socket(zmq.DEALER)
    slow.setsockopt(zmq.IDENTITY, slow_no.to_bytes(4, "big"))
    slow.set_hwm(1)
    slow.connect(f"tcp://127.0.0.1:{port}")

    await bus_main.start()
    await bus_sub.start()
    try:
        await asyncio.sleep(0.3)
        # spam the vanished-but-connected slow peer far past its HWM
        for _ in range(200):
            bus_main.send(slow_no, "fast", pack_call(1))
        # the fast sub must still receive promptly while the slow peer is stuck
        bus_main.send(ctx_sub.service_no, "fast", pack_call(1))
        await asyncio.wait_for(got_fast.wait(), 3.0)
        assert bus_main.unroutable_sends > 0  # slow peer's HWM rejections counted
    finally:
        slow.close(0)
        await bus_sub.close()
        await bus_main.close()


@pytest.mark.integration
async def test_router_drops_spoofed_from(config_dir, tmp_path) -> None:
    """F-39: a DEALER claiming to be a different service in the ``from`` field
    is dropped at the ROUTER; a truthful sender still gets through."""
    import zmq
    import zmq.asyncio

    from pyline.net.ipc import service_no_bytes

    port = free_tcp_port()
    ctx_main = make_ctx(config_dir, tmp_path, main=True, index=0, port=port)
    gw_main = ProtocolGateway()
    bus_main = ZmqBus(ctx_main, gw_main)
    box: dict = {"event": asyncio.Event()}
    _CaptureNet(gw_main, box)

    await bus_main.start()

    zctx = zmq.asyncio.Context()
    rogue = zctx.socket(zmq.DEALER)
    rogue.setsockopt(zmq.IDENTITY, service_no_bytes(424242))
    rogue.connect(f"tcp://127.0.0.1:{port}")
    try:
        await asyncio.sleep(0.3)
        # socket identity is 424242 but the message claims from=777777
        await rogue.send_multipart(
            [
                service_no_bytes(ctx_main.service_no),
                service_no_bytes(777777),
                b"test",
                pack_call(1, "spoofed"),
            ]
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(box["event"].wait(), 1.0)
        assert bus_main.spoofed_messages == 1
        assert box.get("got") is None

        # the same socket telling the truth is delivered
        await rogue.send_multipart(
            [
                service_no_bytes(ctx_main.service_no),
                service_no_bytes(424242),
                b"test",
                pack_call(1, "honest"),
            ]
        )
        await asyncio.wait_for(box["event"].wait(), 5.0)
        assert box["got"] == "honest"
    finally:
        rogue.close(0)
        zctx.term()
        await bus_main.close()


class _DrainSocket:
    """Stand-in socket: the writer tasks' sends succeed (and are recorded)."""

    def __init__(self) -> None:
        self.sent: list[list[bytes]] = []

    async def send_multipart(self, frames: list[bytes]) -> None:
        self.sent.append(frames)

    def close(self, linger: int = 0) -> None:
        pass


def _frames(target: int, from_no: int) -> list[bytes]:
    from pyline.net.ipc import service_no_bytes

    return [service_no_bytes(target), service_no_bytes(from_no), b"test", b"x"]


class TestDestinationReclamationF49:
    async def test_idle_slots_reclaimed_when_table_full(self, config_dir, tmp_path) -> None:
        """F-49: vanished peers used to occupy their destination slot forever;
        once the table filled, every new destination was permanently refused."""
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway(), max_destinations=2, peer_idle_ttl=0.05)
        bus._socket = _DrainSocket()  # type: ignore[assignment]
        try:
            bus._enqueue(20001, _frames(20001, ctx.service_no))
            bus._enqueue(20002, _frames(20002, ctx.service_no))
            assert len(bus._peer_queues) == 2
            # both busy (just used): a third destination is refused
            bus._enqueue(20003, _frames(20003, ctx.service_no))
            assert bus.dest_overflow == 1
            assert 20003 not in bus._peer_queues

            await asyncio.sleep(0.1)  # both peers go silent past the idle ttl
            bus._enqueue(20003, _frames(20003, ctx.service_no))
            assert 20003 in bus._peer_queues  # idle slot reclaimed for it
            assert bus.dest_overflow == 1  # no further drop
        finally:
            await bus.close()

    async def test_busy_peers_are_not_evicted(self, config_dir, tmp_path) -> None:
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway(), max_destinations=1, peer_idle_ttl=60.0)
        bus._socket = _DrainSocket()  # type: ignore[assignment]
        try:
            bus._enqueue(20001, _frames(20001, ctx.service_no))
            bus._enqueue(20002, _frames(20002, ctx.service_no))
            assert 20001 in bus._peer_queues  # recently active: kept
            assert 20002 not in bus._peer_queues
            assert bus.dest_overflow == 1
        finally:
            await bus.close()

    async def test_router_peer_slot_never_evicted(self, config_dir, tmp_path) -> None:
        """A DEALER's only destination (the ROUTER) must survive reclamation."""
        from pyline.net.ipc import _ROUTER_PEER

        ctx = make_ctx(config_dir, tmp_path, main=False, index=1, port=0)
        bus = ZmqBus(ctx, ProtocolGateway(), max_destinations=1, peer_idle_ttl=0.0)
        bus._socket = _DrainSocket()  # type: ignore[assignment]
        try:
            bus._enqueue(_ROUTER_PEER, _frames(10001, ctx.service_no))
            await asyncio.sleep(0.02)
            assert bus._evict_idle_peers() is False
            assert _ROUTER_PEER in bus._peer_queues
        finally:
            await bus.close()


class _BlockingSocket:
    """Stand-in socket whose sends block until released (F-74 setup)."""

    def __init__(self) -> None:
        self.sent: list[list[bytes]] = []
        self.gate = asyncio.Event()
        self.in_send = asyncio.Event()

    async def send_multipart(self, frames: list[bytes]) -> None:
        self.in_send.set()
        await self.gate.wait()
        self.sent.append(frames)

    def close(self, linger: int = 0) -> None:
        pass


class _CrashingSocket:
    """Stand-in socket whose first send raises a non-ZMQ error (F-76 setup)."""

    def __init__(self) -> None:
        self.sent: list[list[bytes]] = []
        self.calls = 0

    async def send_multipart(self, frames: list[bytes]) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("encoder exploded")  # not a zmq.ZMQError
        self.sent.append(frames)

    def close(self, linger: int = 0) -> None:
        pass


class TestSendOriginPreservationF70:
    async def test_router_send_stamps_relaid_origin(self, config_dir, tmp_path) -> None:
        """F-70: bus.send(from_service=...) puts the ORIGINAL sender in the
        wire frames -- the ROUTER used to rewrite it to its own number, so
        RPC replies to cross-machine callers left with a forged origin."""
        from pyline.net.ipc import service_no_bytes

        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway())
        bus._socket = _DrainSocket()  # type: ignore[assignment]
        try:
            remote_origin = 9 * 100_000 + ctx.service_no
            target = 1 * 100_000 + ctx.service_no
            bus.send(target, "f", b"x", from_service=remote_origin)
            await asyncio.sleep(0.05)
            frames = bus._socket.sent[0]  # type: ignore[attr-defined]
            assert frames[0] == service_no_bytes(target)
            assert frames[1] == service_no_bytes(remote_origin)  # origin kept
        finally:
            await bus.close()

    async def test_dealer_send_defaults_to_own_identity(self, config_dir, tmp_path) -> None:
        from pyline.net.ipc import service_no_bytes

        ctx = make_ctx(config_dir, tmp_path, main=False, index=2, port=0)
        bus = ZmqBus(ctx, ProtocolGateway())
        bus._socket = _DrainSocket()  # type: ignore[assignment]
        try:
            bus.send(10001, "f", b"x")
            await asyncio.sleep(0.05)
            frames = bus._socket.sent[0]  # type: ignore[attr-defined]
            assert frames[1] == service_no_bytes(ctx.service_no)
        finally:
            await bus.close()

    async def test_send_to_self_dispatches_with_origin(self, config_dir, tmp_path) -> None:
        """F-70: the local-dispatch leg of send() keeps the relayed origin."""
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        got: list[tuple[str, bytes, int]] = []

        class OriginNet(Network):
            flag = "orig"

            def __init__(self, gateway: ProtocolGateway) -> None:
                super().__init__(gateway)

            def handle_message(self, flag: str, payload: bytes, from_service: int = 0) -> None:
                got.append((flag, payload, from_service))

        gateway = ProtocolGateway()
        OriginNet(gateway)
        bus = ZmqBus(ctx, gateway)
        remote_origin = 9 * 100_000 + ctx.service_no
        bus.send(ctx.service_no, "orig", b"x", from_service=remote_origin)
        assert got == [("orig", b"x", remote_origin)]


class TestMidSendEvictionF74:
    async def test_writer_inside_send_is_not_reclaimed(self, config_dir, tmp_path) -> None:
        """F-74: a writer that dequeued its message and blocks inside
        send_multipart (HWM full, peer gone) must NOT have its slot
        reclaimed -- cancelling it there tears a multipart message in half
        on the shared socket and the peer misframes everything after it."""
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway(), max_destinations=1, peer_idle_ttl=0.0)
        sock = _BlockingSocket()
        bus._socket = sock  # type: ignore[assignment]
        try:
            bus._enqueue(20001, _frames(20001, ctx.service_no))
            await asyncio.wait_for(sock.in_send.wait(), 2.0)  # mid-send now
            assert bus._evict_idle_peers() is False  # idle ttl 0 but BUSY
            assert 20001 in bus._peer_queues
            # the table is full and the busy slot cannot be stolen
            bus._enqueue(20002, _frames(20002, ctx.service_no))
            assert bus.dest_overflow == 1
            assert 20002 not in bus._peer_queues

            # once the send completes the slot is reclaimable again
            sock.gate.set()
            await asyncio.sleep(0.05)
            assert bus._evict_idle_peers() is True
            assert 20001 not in bus._peer_queues
        finally:
            await bus.close()

    async def test_parked_writer_with_drained_queue_still_evictable(
        self, config_dir, tmp_path
    ) -> None:
        """F-74 sanity: the guard covers ONLY the mid-send window. A writer
        parked back on queue.get() with an empty queue (send completed) is
        reclaimed exactly as before (F-49)."""
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway(), max_destinations=1, peer_idle_ttl=0.0)
        sock = _BlockingSocket()
        sock.gate.set()  # sends complete immediately
        bus._socket = sock  # type: ignore[assignment]
        try:
            bus._enqueue(20001, _frames(20001, ctx.service_no))
            for _ in range(100):
                if sock.sent:
                    break
                await asyncio.sleep(0.05)
            assert sock.sent, "message never sent"
            await asyncio.sleep(0.05)  # writer back on queue.get()
            assert bus._evict_idle_peers() is True
            assert 20001 not in bus._peer_queues
        finally:
            await bus.close()


class TestPeerWriterRobustnessF76:
    async def test_non_zmq_send_error_survives_writer(self, config_dir, tmp_path) -> None:
        """F-76: a non-ZMQ exception in send_multipart used to kill the
        writer task; every subsequent message for that destination then sat
        in a queue nobody drained. The writer now survives, counts, and the
        NEXT message still goes out."""
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway())
        sock = _CrashingSocket()
        bus._socket = sock  # type: ignore[assignment]
        try:
            bus._enqueue(20001, _frames(20001, ctx.service_no))
            await asyncio.sleep(0.05)
            assert bus.writer_errors == 1
            assert bus.unroutable_sends == 0  # not miscounted as routing failure
            # writer still alive: the next send is attempted and delivered
            bus._enqueue(20001, _frames(20001, ctx.service_no))
            for _ in range(100):
                if sock.sent:
                    break
                await asyncio.sleep(0.05)
            assert sock.sent, "writer died after a non-ZMQ error"
            assert bus.writer_errors == 1
        finally:
            await bus.close()


class TestIpcEndpointPermissionsF75:
    def test_ipc_file_path_extraction(self) -> None:
        from pyline.net.ipc import ipc_file_path

        assert ipc_file_path("ipc:///tmp/pyline.ipc") == "/tmp/pyline.ipc"
        assert ipc_file_path("ipc://rel.ipc") == "rel.ipc"
        assert ipc_file_path("unix:///tmp/x.sock") == "/tmp/x.sock"
        assert ipc_file_path("/tmp/pyline.ipc") == "/tmp/pyline.ipc"
        assert ipc_file_path("tcp://127.0.0.1:2918") is None
        assert ipc_file_path("ipc://") is None
        assert ipc_file_path("inproc://bus") is None

    def test_secure_chmods_only_ipc_paths(self, monkeypatch) -> None:
        """F-75: exactly the ipc file gets chmod 0600; tcp/named-pipe
        endpoints are untouched and chmod failures never raise."""
        import os

        import pyline.net.ipc as ipc_mod

        calls: list[tuple[str, int]] = []
        monkeypatch.setattr(
            os, "chmod", lambda path, mode: calls.append((str(path), int(mode))), raising=True
        )
        ipc_mod.secure_ipc_endpoint("ipc:///tmp/pyline.ipc")
        ipc_mod.secure_ipc_endpoint("/tmp/pyline.ipc")
        assert calls == [("/tmp/pyline.ipc", 0o600), ("/tmp/pyline.ipc", 0o600)]

        calls.clear()
        ipc_mod.secure_ipc_endpoint("tcp://127.0.0.1:2918")
        assert calls == []

        def boom(path: object, mode: object) -> None:
            raise OSError("read-only")

        monkeypatch.setattr(os, "chmod", boom)
        ipc_mod.secure_ipc_endpoint("ipc:///tmp/pyline.ipc")  # logged, not raised


class TestEndpointNormalizationF105:
    """F-105: a bare path is not a valid zmq address; the scheme picks the
    transport. Old configs with bare-path bind_file keep working."""

    def test_bare_path_gets_ipc_scheme(self) -> None:
        from pyline.net.ipc import normalize_endpoint

        assert normalize_endpoint("/tmp/pyline.ipc") == "ipc:///tmp/pyline.ipc"

    def test_schemed_addresses_unchanged(self) -> None:
        from pyline.net.ipc import normalize_endpoint

        assert normalize_endpoint("ipc:///tmp/x.sock") == "ipc:///tmp/x.sock"
        assert normalize_endpoint("tcp://127.0.0.1:2918") == "tcp://127.0.0.1:2918"
