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
