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
