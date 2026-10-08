"""Router local routing rules and loop-latency monitor."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from pyline.net.gateway import ProtocolGateway
from pyline.net.router import CrossServerError, MessageRouter


class FakeBus:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str, bytes]] = []

    def send(self, target: int, flag: str, payload: bytes) -> None:
        self.sent.append((target, flag, payload))


class FakeProxyClient:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str, bytes]] = []

    def send_to_service(self, target: int, flag: str, payload: bytes) -> None:
        self.sent.append((target, flag, payload))


@pytest.fixture()
def router_ctx(config_dir):
    from pyline.config.loader import load_project_settings, load_server_registry, load_table_defs
    from pyline.core.context import PROCESS_MAIN, Context

    settings = load_project_settings(config_dir)
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=registry.entry(10001),
        process_type=PROCESS_MAIN,
        process_index=0,
        main_pid=0,
    )


async def test_local_and_bus_routing(router_ctx) -> None:
    gateway = ProtocolGateway()
    bus = FakeBus()
    proxy = FakeProxyClient()
    router = MessageRouter(router_ctx, gateway, bus)
    router.attach_proxy_client(proxy)  # type: ignore[arg-type]

    # same service -> local dispatch through gateway (unknown flag counted)
    router.route("nope", b"x", router_ctx.service_no)
    assert gateway.unknown_dispatches == 1

    # same machine, other process -> bus
    target = 1 * 100_000 + router_ctx.server_no
    router.route("flag", b"payload", target)
    assert bus.sent == [(target, "flag", b"payload")]
    assert proxy.sent == []


async def test_cross_server_from_main_goes_to_proxy(router_ctx) -> None:
    gateway = ProtocolGateway()
    bus = FakeBus()
    proxy = FakeProxyClient()
    router = MessageRouter(router_ctx, gateway, bus)
    router.attach_proxy_client(proxy)  # type: ignore[arg-type]

    router.route("flag", b"payload", 2 * 100_000 + 999)
    assert len(proxy.sent) == 1
    assert bus.sent == []


async def test_cross_server_from_subprocess_rejected(router_ctx) -> None:
    sub_ctx = replace(router_ctx, process_type="game", process_index=1)
    gateway = ProtocolGateway()
    router = MessageRouter(sub_ctx, gateway, FakeBus())
    with pytest.raises(CrossServerError, match="cannot send cross-server"):
        router.route("flag", b"x", 9 * 100_000 + 1)


async def test_loop_latency_monitor() -> None:
    from pyline.obs.metrics import LoopLatencyMonitor

    alerts: list[float] = []
    monitor = LoopLatencyMonitor(interval=0.02, alert_threshold=0.05, on_alert=alerts.append)
    monitor.start()
    await asyncio.sleep(0.1)
    await monitor.stop()
    assert monitor.last_delay >= 0
    # healthy fast loop should not alert
    assert alerts == []
