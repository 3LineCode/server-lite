"""Router local routing rules and loop-latency monitor."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import msgpack
import pytest

from pyline.net.gateway import ProtocolGateway
from pyline.net.router import RELAY_FLAG, MessageRouter


class FakeBus:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str, bytes, int | None]] = []

    def send(
        self,
        target: int,
        flag: str,
        payload: bytes,
        *,
        from_service: int | None = None,
        raise_on_drop: bool = False,
    ) -> None:
        self.sent.append((target, flag, payload, from_service))


class FakeProxyClient:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str, bytes, int]] = []
        self.hops: list[int] = []

    def send_to_service(
        self,
        target: int,
        flag: str,
        payload: bytes,
        *,
        from_service: int | None = None,
        hops: int = 0,
    ) -> None:
        self.sent.append((target, flag, payload, from_service if from_service is not None else -1))
        self.hops.append(hops)


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

    # same machine, other process -> bus (F-70: own identity by default)
    target = 1 * 100_000 + router_ctx.server_no
    router.route("flag", b"payload", target)
    assert bus.sent == [(target, "flag", b"payload", None)]
    assert proxy.sent == []


async def test_bus_routing_preserves_relayed_origin(router_ctx) -> None:
    """F-70: a message this main relays for a remote caller keeps the remote
    origin on the local bus leg -- the stamp used to be rewritten to the
    local main's number, so RPC replies to cross-machine callers of a local
    sub-process arrived with a forged origin and were rejected (F-40)."""
    gateway = ProtocolGateway()
    bus = FakeBus()
    router = MessageRouter(router_ctx, gateway, bus)
    target = 1 * 100_000 + router_ctx.server_no
    remote_origin = 2 * 100_000 + router_ctx.server_no + 1
    router.route("flag", b"payload", target, from_service=remote_origin)
    assert bus.sent == [(target, "flag", b"payload", remote_origin)]


async def test_cross_server_from_main_goes_to_proxy(router_ctx) -> None:
    gateway = ProtocolGateway()
    bus = FakeBus()
    proxy = FakeProxyClient()
    router = MessageRouter(router_ctx, gateway, bus)
    router.attach_proxy_client(proxy)  # type: ignore[arg-type]

    router.route("flag", b"payload", 2 * 100_000 + 999)
    assert len(proxy.sent) == 1
    assert bus.sent == []


async def test_cross_server_from_main_preserves_relayed_origin(router_ctx) -> None:
    """F-70: the proxy envelope carries the original sender, not the local
    main's number, when the main forwards relayed traffic."""
    gateway = ProtocolGateway()
    proxy = FakeProxyClient()
    router = MessageRouter(router_ctx, gateway, FakeBus())
    router.attach_proxy_client(proxy)  # type: ignore[arg-type]

    remote_target = 3 * 100_000 + 999
    local_sub = 1 * 100_000 + router_ctx.server_no
    router.route("flag", b"payload", remote_target, from_service=local_sub)
    assert proxy.sent == [(remote_target, "flag", b"payload", local_sub)]


async def test_cross_server_from_subprocess_relays_via_main(router_ctx) -> None:
    """F-70: a sub-process no longer fails loudly (CrossServerError) -- it
    wraps the message in an @relay envelope addressed to the local main,
    carrying itself as the envelope origin, and sends it over the bus."""
    sub_ctx = replace(router_ctx, process_type="game", process_index=1)
    gateway = ProtocolGateway()
    bus = FakeBus()
    router = MessageRouter(sub_ctx, gateway, bus)

    target = 9 * 100_000 + 1  # another machine entirely
    router.route("flag", b"x", target)

    assert len(bus.sent) == 1
    bus_target, bus_flag, bus_payload, bus_from = bus.sent[0]
    assert bus_target == sub_ctx.main_service_no  # addressed to the local main
    assert bus_flag == RELAY_FLAG
    assert bus_from is None  # bus frames stamp the sub-process's own identity
    envelope = msgpack.unpackb(bus_payload, raw=False)
    # [target, from_service, flag, payload, hops]
    assert envelope[0] == target
    assert envelope[1] == sub_ctx.service_no
    assert envelope[2] == "flag"
    assert envelope[3] == b"x"
    assert envelope[4] == 0


def _relay_router(router_ctx) -> tuple[MessageRouter, FakeBus]:
    """A main-process router whose gateway dispatches @relay into it."""
    bus = FakeBus()
    return MessageRouter(router_ctx, ProtocolGateway(), bus), bus


class TestRelayOriginBindingF70:
    """The main-side @relay handler binds the envelope's claimed origin to
    the local machine's mesh, mirroring the proxy's first-hop binding."""

    def test_local_origin_forwarded_cross_machine(self, router_ctx) -> None:
        router, _bus = _relay_router(router_ctx)
        proxy = FakeProxyClient()
        router.attach_proxy_client(proxy)  # type: ignore[arg-type]
        sub = 1 * 100_000 + router_ctx.server_no
        remote_target = 5 * 100_000 + 7
        router._on_relay(msgpack.packb([remote_target, sub, "f", b"x", 0]), from_service=sub)
        assert router.relayed_messages == 1
        assert proxy.sent == [(remote_target, "f", b"x", sub)]

    def test_relayed_hops_accumulate_across_the_router(self, router_ctx) -> None:
        """A relayed @fwd that re-enters the router (registry disagreement,
        the classic forwarding loop) used to reset its hop count to 0 at
        every ProxyClient send, so MAX_HOPS never bounded the loop. The
        envelope's hops must ride along and the onward send must add one."""
        router, _bus = _relay_router(router_ctx)
        proxy = FakeProxyClient()
        router.attach_proxy_client(proxy)  # type: ignore[arg-type]
        sub = 1 * 100_000 + router_ctx.server_no
        remote_target = 5 * 100_000 + 7
        router._on_relay(msgpack.packb([remote_target, sub, "f", b"x", 6]), from_service=sub)
        assert proxy.hops == [6]  # +1 is stamped by send_to_service's envelope
        envelope_hops = 7  # what build_forward wrote into the outbound frame
        # and a second relay round keeps accumulating from the carried value
        router._on_relay(
            msgpack.packb([remote_target, sub, "f", b"x", envelope_hops]), from_service=sub
        )
        assert proxy.hops == [6, 7]

    def test_remote_origin_claim_dropped(self, router_ctx) -> None:
        """A sub-process stamping a DIFFERENT machine's service number on its
        envelope is spoofing (it never could have received that origin here)
        -- dropped and counted, not forwarded."""
        router, bus = _relay_router(router_ctx)
        sub = 1 * 100_000 + router_ctx.server_no
        victim = (router_ctx.main_service_no + 1) + 2 * 100_000  # machine B's process
        remote_target = 5 * 100_000 + 7
        router._on_relay(msgpack.packb([remote_target, victim, "f", b"x", 0]), from_service=sub)
        assert router.relayed_messages == 0
        assert router.relay_spoofed == 1
        assert bus.sent == []

    def test_local_target_routes_via_bus_with_origin(self, router_ctx) -> None:
        """An @relay envelope whose target turns out to be local is delivered
        through the bus with the preserved origin (defensive branch)."""
        router, bus = _relay_router(router_ctx)
        sub = 1 * 100_000 + router_ctx.server_no
        sibling = 2 * 100_000 + router_ctx.server_no
        router._on_relay(msgpack.packb([sibling, sub, "f", b"x", 0]), from_service=sub)
        assert router.relayed_messages == 1
        assert bus.sent == [(sibling, "f", b"x", sub)]

    def test_malformed_envelope_dropped(self, router_ctx) -> None:
        router, _bus = _relay_router(router_ctx)
        router._on_relay(msgpack.packb([1, 2, 3], use_bin_type=True), from_service=1)
        router._on_relay(msgpack.packb("scalar", use_bin_type=True), from_service=1)
        router._on_relay(b"\x91" * 100_000 + b"\xc0", from_service=1)  # RecursionError
        assert router.relayed_messages == 0
        assert router.relay_spoofed == 3


async def test_loop_latency_monitor() -> None:
    from pyline.obs.metrics import LoopLatencyMonitor

    alerts: list[float] = []
    monitor = LoopLatencyMonitor(interval=0.02, alert_threshold=0.05, on_alert=alerts.append)
    monitor.start()
    await asyncio.sleep(0.1)
    await monitor.stop()
    # Windows timer granularity can wake the sleep marginally EARLY (a
    # slightly negative measured delay); the assertion is about the monitor
    # having measured, not about clock perfection.
    assert monitor.last_delay >= -0.1
    # healthy fast loop should not alert
    assert alerts == []


class TestRebindF30:
    def test_gateway_rebind_module(self, config_dir) -> None:
        """F-30: after a hot reload the gateway asks that module's networks
        to re-register handlers (prototype ReInitHandlers equivalent)."""
        import sys
        import textwrap
        from pathlib import Path

        from pyline.net.gateway import ProtocolGateway

        gateway = ProtocolGateway()
        reload_module = pytest.importorskip("pyline.reload").reload_module

        source_v1 = textwrap.dedent(
            """
            from pyline.net.network import Network

            class GameNet(Network):
                flag = "game"

                def __init__(self, gateway):
                    super().__init__(gateway)
                    self._extra = None

                def rebind_handlers(self):
                    if self._extra is None:
                        self._extra = "bound"
            """
        )
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            monkey = pytest.MonkeyPatch()
            monkey.syspath_prepend(str(tmp_path))
            monkey.delitem(sys.modules, "gamenet", raising=False)
            try:
                (tmp_path / "gamenet.py").write_text(source_v1, encoding="utf-8")
                import gamenet

                net = gamenet.GameNet(gateway)
                assert net._extra is None
                source_v2 = source_v1.replace('"bound"', '"rebound"')
                (tmp_path / "gamenet.py").write_text(source_v2, encoding="utf-8")
                import os
                import time as _t

                os.utime(tmp_path / "gamenet.py", (_t.time() + 5, _t.time() + 5))
                reload_module("gamenet")
                assert gateway.rebind_module("gamenet") == 1
                assert net._extra == "rebound"
            finally:
                monkey.undo()
                sys.modules.pop("gamenet", None)


# --------------------------------------------------------------------- #
# F-70: cross-machine relay end-to-end -- two simulated machines, each
# with its own zmq bus / router / proxy pair, in one process.
# --------------------------------------------------------------------- #


@pytest.mark.integration
async def test_cross_machine_rpc_to_sub_process_round_trip(config_dir, tmp_path) -> None:
    """F-70 regression: machine B's main calls a function on machine A's
    sub-process. The CALL travels B-main -> proxy @fwd -> A-main -> local
    bus -> A-sub, and the RESULT travels back A-sub -> @relay -> A-main ->
    proxy @fwd -> B-main. Before F-70 the reply left A stamped with A-main's
    identity, B rejected it ("unknown/expired call_id" upstream, F-40
    mismatch) and the caller burned its whole 10s timeout."""
    import socket

    from pyline.config.loader import (
        load_project_settings,
        load_server_registry,
        load_table_defs,
    )
    from pyline.core.context import PROCESS_DB, Context
    from pyline.net.ipc import ZmqBus
    from pyline.net.proxy import ProxyClient, ProxyServer
    from pyline.net.rpc import RpcManager, current_caller

    def free_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    # Two proxy machines; advertise_ip must be unique registry-wide, so
    # machine B advertises a fictional address. That is fine for this
    # topology: B's client dials A (loopback, reachable) and A's replies ride
    # B's own inbound connection back -- A never needs to dial B. The
    # loopback peer-IP bypass in ProxyServer accepts both IDENTs.
    (tmp_path / "servers.json5").write_text(
        """
{
    "normal": {"sub_process": [], "use_mysql": true, "use_redis": true},
    "20001": {
        "base": "normal", "name": "machine-a", "advertise_ip": "127.0.0.1",
        "client_port": 15201, "server_port": 25201, "is_proxy": true,
    },
    "20002": {
        "base": "normal", "name": "machine-b", "advertise_ip": "10.9.0.2",
        "bind_ip": "127.0.0.1",
        "client_port": 15202, "server_port": 25202, "is_proxy": true,
    },
}
""",
        encoding="utf-8",
    )
    (tmp_path / "tables.json5").write_text("{}", encoding="utf-8")

    settings = load_project_settings(config_dir)
    registry = load_server_registry(tmp_path)
    tables = load_table_defs(tmp_path)

    def make_ctx(server_no: int, *, main: bool, index: int, zmq_port: int) -> Context:
        # A distinct zmq bus endpoint per machine's main process.
        s = settings.model_copy(
            update={
                "zeromq": settings.zeromq.model_copy(
                    update={"bind_host": f"tcp://127.0.0.1:{zmq_port}"}
                )
            }
        )
        return Context(
            settings=s,
            registry=registry,
            tables=tables,
            entry=registry.entry(server_no),
            process_type="main" if main else PROCESS_DB,
            process_index=index,
            main_pid=0,
        )

    A, B = 20001, 20002
    # Both of machine A's processes point at A's ROUTER endpoint (the DEALER
    # connects to it; only the main binds).
    a_bus_port = free_port()
    a_main_ctx = make_ctx(A, main=True, index=0, zmq_port=a_bus_port)
    a_sub_ctx = make_ctx(A, main=False, index=1, zmq_port=a_bus_port)
    b_main_ctx = make_ctx(B, main=True, index=0, zmq_port=free_port())
    a_sub_no = a_sub_ctx.service_no

    # --- machine A ---
    gw_a_main = ProtocolGateway()
    bus_a_main = ZmqBus(a_main_ctx, gw_a_main)
    router_a_main = MessageRouter(a_main_ctx, gw_a_main, bus_a_main)
    proxy_client_a = ProxyClient(a_main_ctx, router_a_main)
    router_a_main.attach_proxy_client(proxy_client_a)
    proxy_server_a = ProxyServer(a_main_ctx, router_a_main)
    router_a_main.attach_proxy_server(proxy_server_a)
    gw_a_sub = ProtocolGateway()
    bus_a_sub = ZmqBus(a_sub_ctx, gw_a_sub)
    router_a_sub = MessageRouter(a_sub_ctx, gw_a_sub, bus_a_sub)
    rpc_a_sub = RpcManager(gw_a_sub, router_a_sub, own_service_no=a_sub_no)

    # --- machine B ---
    gw_b_main = ProtocolGateway()
    bus_b_main = ZmqBus(b_main_ctx, gw_b_main)
    router_b_main = MessageRouter(b_main_ctx, gw_b_main, bus_b_main)
    proxy_client_b = ProxyClient(b_main_ctx, router_b_main)
    router_b_main.attach_proxy_client(proxy_client_b)
    proxy_server_b = ProxyServer(b_main_ctx, router_b_main)
    router_b_main.attach_proxy_server(proxy_server_b)
    rpc_b = RpcManager(gw_b_main, router_b_main, own_service_no=b_main_ctx.service_no)

    @rpc_a_sub.expose
    async def who_is_calling(x: int) -> dict:
        return {"echo": x, "caller": current_caller()}

    # start() only binds/connects -- no proxy handshake -- so plain awaits
    # suffice; the maintain loops run as their own background tasks.
    await bus_a_main.start()
    await bus_a_sub.start()
    await bus_b_main.start()
    await proxy_server_a.start()
    await proxy_server_b.start()
    await proxy_client_a.start()
    await proxy_client_b.start()
    # wait for binds, dealer connects and B's proxy link + IDENT
    await asyncio.sleep(1.5)
    try:
        # B's link to A is the one the CALL rides on (A cannot dial B's
        # fictional address; replies use B's inbound connection).
        assert proxy_client_b._proxies, "machine B never linked to machine A's proxy"

        # B's main calls A's sub-process (the previously broken path).
        result = await asyncio.wait_for(rpc_b.call(a_sub_no, who_is_calling, 42), timeout=10)
        assert result["echo"] == 42
        # the caller identity survived the whole relay chain: B's main number
        assert result["caller"] == b_main_ctx.service_no
        # and A's relay handler actually participated in the reply leg
        assert router_a_main.relayed_messages >= 1

        # sanity: the pending table drained on both sides
        assert rpc_b.pending_count() == 0
        assert rpc_a_sub.pending_count() == 0
    finally:
        for closer in (
            proxy_client_a.close,
            proxy_client_b.close,
            proxy_server_a.close,
            proxy_server_b.close,
            bus_a_sub.close,
            bus_a_main.close,
            bus_b_main.close,
        ):
            await closer()
