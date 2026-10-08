"""F-16: proxy hardening -- IDENT validation, reconnect guard, inter-token."""

from __future__ import annotations

import asyncio
import dataclasses

import pytest

import pyline.net.connection as conn_mod
import pyline.net.proxy as proxy_mod
from pyline.net.proxy import IDENT_FLAG, ProxyClient, ProxyServer
from test_ipc import make_ctx  # reuse the config-based Context builder

KW = {
    "handshake_timeout": 1.0,
    "idle_timeout": 30.0,
    "send_queue_limit": 64,
}


class _StubRouter:
    def route(self, flag: str, payload: bytes, target_service_no: int) -> None:
        pass


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
        assert proxy_mod.inter_token(ctx) == ctx.settings.socket.token
        patched_socket = ctx.settings.socket.model_copy(update={"inter_token": "sec-ret"})
        patched = dataclasses.replace(
            ctx, settings=ctx.settings.model_copy(update={"socket": patched_socket})
        )
        assert proxy_mod.inter_token(patched) == "sec-ret"


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
