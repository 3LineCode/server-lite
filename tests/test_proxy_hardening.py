"""F-16/F-48: proxy hardening -- IDENT validation, reconnect guard, inter-token,
@fwd origin binding."""

from __future__ import annotations

import asyncio
import dataclasses

import pytest

import pyline.net.connection as conn_mod
import pyline.net.proxy as proxy_mod
from pyline.net.proxy import FWD_FLAG, IDENT_FLAG, ProxyClient, ProxyServer, build_forward
from pyline.obs.metrics import get_metrics
from test_ipc import make_ctx  # reuse the config-based Context builder

KW = {
    "handshake_timeout": 1.0,
    "idle_timeout": 30.0,
    "send_queue_limit": 64,
}


class _StubRouter:
    def route(self, flag: str, payload: bytes, target_service_no: int) -> None:
        pass


class _RecordingRouter:
    def __init__(self) -> None:
        self.routed: list[tuple[str, bytes, int, int]] = []

    def route(
        self, flag: str, payload: bytes, target_service_no: int, *, from_service: int = 0
    ) -> None:
        self.routed.append((flag, payload, target_service_no, from_service))


@dataclasses.dataclass(eq=False)  # identity-hashed: used as a dict key
class _FakeConn:
    peer: tuple[str, int] = ("127.0.0.1", 54321)
    closed: bool = False


def nodes(server: ProxyServer) -> dict:
    return server._nodes  # type: ignore[no-any-return]


async def wait_closed(conn: conn_mod.Connection, timeout: float = 2.0) -> None:
    for _ in range(int(timeout / 0.05)):
        if conn.closed:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("connection was not closed in time")


async def start_proxy_server(ctx) -> tuple[ProxyServer, asyncio.AbstractServer, int]:
    server = ProxyServer(ctx, _StubRouter())  # type: ignore[arg-type]
    srv = await conn_mod.serve(
        "127.0.0.1",
        0,
        token=proxy_mod.inter_token(ctx),
        on_message=lambda f, p: None,
        on_connected=server._on_connected,
        **KW,  # type: ignore[arg-type]
    )
    return server, srv, srv.sockets[0].getsockname()[1]


class TestIdentValidation:
    async def test_ident_unknown_machine_rejected(self, config_dir, tmp_path) -> None:
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        server, srv, port = await start_proxy_server(ctx)
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=proxy_mod.inter_token(ctx),
            on_message=lambda f, p: None,
            **KW,  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        client.send_message(IDENT_FLAG, (99999).to_bytes(4, "big"))
        await wait_closed(client)
        assert not nodes(server)
        await conn_mod.close_server(srv)

    async def test_ident_duplicate_claim_rejected(self, config_dir, tmp_path) -> None:
        """A second live connection claiming the same machine is refused."""
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        server, srv, port = await start_proxy_server(ctx)
        first = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=proxy_mod.inter_token(ctx),
            on_message=lambda f, p: None,
            **KW,  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        first.send_message(IDENT_FLAG, (10001).to_bytes(4, "big"))  # loopback allowed
        await asyncio.sleep(0.1)
        assert 10001 in nodes(server)
        second = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=proxy_mod.inter_token(ctx),
            on_message=lambda f, p: None,
            **KW,  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        second.send_message(IDENT_FLAG, (10001).to_bytes(4, "big"))  # hijack attempt
        await wait_closed(second)
        assert not first.closed, "the original registration survives"
        assert 10001 in nodes(server), "original registration kept"
        await first.close("done")
        await conn_mod.close_server(srv)

    def test_inter_token_falls_back(self, config_dir, tmp_path) -> None:
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        assert proxy_mod.inter_token(ctx) == ctx.settings.socket.token.get_secret_value()
        from pydantic import SecretStr

        patched_socket = ctx.settings.socket.model_copy(
            update={"inter_token": SecretStr("sec-ret")}
        )
        patched = dataclasses.replace(
            ctx, settings=ctx.settings.model_copy(update={"socket": patched_socket})
        )
        assert proxy_mod.inter_token(patched) == "sec-ret"

    def test_inter_token_fallback_warns_once(
        self, config_dir, tmp_path, monkeypatch, caplog
    ) -> None:
        """F-73: the fallback warning fires once per process, not on every
        reconnect cycle of every proxy link (a flapping peer used to re-log
        the same config fact every second)."""
        import logging

        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        monkeypatch.setattr(proxy_mod, "_inter_token_warned", False)
        with caplog.at_level(logging.WARNING, logger="pyline.net.proxy"):
            for _ in range(5):  # five "reconnects"
                proxy_mod.inter_token(ctx)
        warnings = [r for r in caplog.records if "inter_token not set" in r.message]
        assert len(warnings) == 1, f"expected exactly one warning, got {len(warnings)}"


@pytest.mark.integration
async def test_proxy_reconnect_survives_immediate_close(config_dir, tmp_path) -> None:
    """F-16: the reconnect task outlives a server that closes every link
    the instant after handshake (used to kill the task with an unguarded
    ConnectionClosedError from the IDENT send)."""
    ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
    srv = await conn_mod.serve(
        "127.0.0.1",
        0,
        token=proxy_mod.inter_token(ctx),
        on_message=lambda f, p: None,
        on_connected=lambda conn: asyncio.get_running_loop().create_task(
            conn.close("instant close")
        ),
        **KW,  # type: ignore[arg-type]
    )
    port = srv.sockets[0].getsockname()[1]

    client = ProxyClient(ctx, _StubRouter())  # type: ignore[arg-type]
    task = asyncio.get_running_loop().create_task(client._maintain(10009, "127.0.0.1", port))
    await asyncio.sleep(1.5)
    assert not task.done(), "reconnect task must survive immediate-close servers"
    task.cancel()
    await conn_mod.close_server(srv)


class TestForwardEnvelopeF39:
    def test_forward_envelope_carries_original_sender(self) -> None:
        from pyline.net.proxy import build_forward, parse_forward

        data = build_forward(10001, 20002, "game", b"payload", 3)
        assert parse_forward(data) == (10001, 20002, "game", b"payload", 3)
        assert isinstance(data, bytes)

    def test_legacy_four_field_envelope_parses_as_unknown_origin(self) -> None:
        import msgpack

        from pyline.net.proxy import parse_forward

        legacy = msgpack.packb([10001, "game", b"payload", 0], use_bin_type=True)
        assert parse_forward(legacy) == (10001, 0, "game", b"payload", 0)


class TestParseForwardHardeningF78:
    """F-78: parse_forward used to ``len()`` whatever msgpack produced -- a
    scalar payload escaped as TypeError and crashed the frame handler's
    caller; adversarially deep nesting escaped as RecursionError."""

    def _bad(self, value: object) -> None:
        import msgpack
        import pytest as _pytest

        from pyline.net.proxy import parse_forward

        with _pytest.raises((ValueError, RecursionError)):
            parse_forward(msgpack.packb(value, use_bin_type=True))

    def test_scalar_payload_rejected(self) -> None:
        self._bad(5)
        self._bad("just-a-string")
        self._bad(b"raw-bytes")

    def test_wrong_shape_rejected(self) -> None:
        self._bad([1, 2, 3])  # 3 fields
        self._bad([1, 2, 3, 4, 5, 6])  # 6 fields
        self._bad({})  # not a list at all

    def test_wrong_field_types_rejected(self) -> None:
        self._bad(["target", "from", "flag", b"payload", 1])  # target not int
        self._bad([10001, "from", "flag", "payload", 1])  # payload not bytes
        self._bad([10001, 1, 2, b"payload", 1])  # flag not str

    def test_deeply_nested_payload_rejected(self) -> None:
        """Adversarially deep nesting: msgpack's C unpacker raises StackError
        (a ValueError subclass); the extended classification in F-78 also
        covers a plain RecursionError from other unpacker builds."""
        import pytest

        from pyline.net.proxy import parse_forward

        deep = b"\x91" * 100_000 + b"\xc0"  # nested 1-element arrays
        with pytest.raises((ValueError, RecursionError)):
            parse_forward(deep)

    async def test_malformed_fwd_on_server_counted_not_raised(self, config_dir, tmp_path) -> None:
        """End-to-end over the parse sites: scalar, wrong-shape and
        deeply-nested @fwd payloads are dropped and counted on both the
        ProxyServer and ProxyClient paths."""
        import msgpack

        from pyline.net.proxy import FWD_FLAG as FWD

        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        server, router = self._make_server(ctx)
        sender = self._register(server, server._ctx.main_service_no)
        server._on_frame(sender, FWD, msgpack.packb(5))
        server._on_frame(sender, FWD, msgpack.packb([1, 2, 3]))
        server._on_frame(sender, FWD, b"\x91" * 100_000 + b"\xc0")
        assert server.malformed_fwd == 3
        assert router.routed == []

        client = ProxyClient(ctx, router)  # type: ignore[arg-type]
        client._on_frame(FWD, msgpack.packb("scalar"))
        client._on_frame(FWD, b"\x91" * 100_000 + b"\xc0")
        assert client.malformed_fwd == 2
        assert router.routed == []

    def _make_server(self, ctx) -> tuple[ProxyServer, _RecordingRouter]:
        router = _RecordingRouter()
        return ProxyServer(ctx, router), router  # type: ignore[arg-type]

    def _register(self, server: ProxyServer, machine: int) -> _FakeConn:
        conn = _FakeConn()
        server._nodes[machine] = conn  # type: ignore[assignment]
        server._machines[conn] = machine  # type: ignore[index]
        return conn


class TestForwardOriginBindingF48:
    """F-48: the first proxy hop binds the envelope's claimed origin to the
    IDENT-registered machine of the sending connection."""

    def _server(self, config_dir, tmp_path) -> tuple[ProxyServer, _RecordingRouter]:
        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        router = _RecordingRouter()
        return ProxyServer(ctx, router), router  # type: ignore[arg-type]

    def _register(self, server: ProxyServer, machine: int) -> _FakeConn:
        conn = _FakeConn()
        server._nodes[machine] = conn  # type: ignore[assignment]
        server._machines[conn] = machine  # type: ignore[index]
        return conn

    async def test_matching_origin_is_routed_locally(self, config_dir, tmp_path) -> None:
        server, router = self._server(config_dir, tmp_path)
        local_machine = server._ctx.main_service_no
        sender = self._register(server, local_machine)
        sub_service = local_machine + 100_000  # a process on the same machine
        server._on_frame(sender, FWD_FLAG, build_forward(local_machine, sub_service, "f", b"x"))
        assert len(router.routed) == 1
        assert router.routed[0][3] == sub_service  # origin preserved for F-40 checks

    async def test_foreign_machine_claim_dropped(self, config_dir, tmp_path) -> None:
        """The @fwd equivalent of F-39: machine A stamps machine B's service
        number on its envelope -- dropped and counted instead of sailing
        through to the RPC origin checks, which only compare claims."""
        server, router = self._server(config_dir, tmp_path)
        local_machine = server._ctx.main_service_no
        sender = self._register(server, local_machine)
        victim = local_machine + 1  # a different machine's number
        spoofed = victim + 2 * 100_000  # a sub-service on the victim machine
        before = get_metrics().proxy_spoofed._value.get()
        server._on_frame(sender, FWD_FLAG, build_forward(local_machine, spoofed, "f", b"x"))
        assert router.routed == []
        assert get_metrics().proxy_spoofed._value.get() == before + 1

    async def test_unregistered_connection_cannot_forward(self, config_dir, tmp_path) -> None:
        """A peer that never sent IDENT has no verifiable origin: a nonzero
        claim is dropped (the client sends IDENT before any @fwd, so a legit
        sender is never in this state)."""
        server, router = self._server(config_dir, tmp_path)
        local_machine = server._ctx.main_service_no
        stranger = _FakeConn()
        server._on_frame(stranger, FWD_FLAG, build_forward(local_machine, local_machine, "f", b"x"))
        assert router.routed == []

    async def test_legacy_zero_origin_flows_through(self, config_dir, tmp_path) -> None:
        """from_service == 0 (legacy 4-field envelope) stays routable -- it is
        treated as untrusted downstream by F-39/F-40, not dropped here."""
        server, router = self._server(config_dir, tmp_path)
        local_machine = server._ctx.main_service_no
        sender = self._register(server, local_machine)
        import msgpack

        legacy = msgpack.packb([local_machine, "f", b"x", 0], use_bin_type=True)
        server._on_frame(sender, FWD_FLAG, legacy)
        assert len(router.routed) == 1
        assert router.routed[0][3] == 0

    async def test_relayed_third_party_origin_passes_first_hop_only(
        self, config_dir, tmp_path
    ) -> None:
        """Relay semantics preserved: the proxy re-emits an envelope claiming
        a third-party origin on the TARGET's connection, and the receiving
        ProxyClient (not a ProxyServer) accepts it -- validation exists only
        at the first hop."""
        server, router = self._server(config_dir, tmp_path)
        local_machine = server._ctx.main_service_no
        sender = self._register(server, local_machine)
        other_machine = local_machine + 1
        server._on_frame(sender, FWD_FLAG, build_forward(other_machine, local_machine, "f", b"x"))
        assert router.routed == []  # not local: relay branch taken
        assert other_machine not in server._nodes  # unknown machine: dropped with warning
