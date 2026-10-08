"""Connection: handshake accept/reject, round-trip, heartbeat, close hooks."""

from __future__ import annotations

import asyncio

import pytest

import pyline.net.connection as conn_mod

TOKEN = "test-token"
KW = {
    "handshake_timeout": 1.0,
    "idle_timeout": 30.0,
    "send_queue_limit": 64,
}


async def start_echo_server(got: dict) -> tuple[asyncio.AbstractServer, int]:
    def on_message(flag: str, payload: bytes) -> None:
        got.setdefault(flag, []).append(payload)

    def on_connected(connection: conn_mod.Connection) -> None:
        got.setdefault("connected", []).append(connection.peer)
        connection.send_message("welcome", b"hi")

    server = await conn_mod.serve(
        "127.0.0.1", 0, token=TOKEN, on_message=on_message, on_connected=on_connected, **KW
    )
    return server, server.sockets[0].getsockname()[1]


async def test_verified_round_trip() -> None:
    got: dict = {}
    server, port = await start_echo_server(got)
    client = await conn_mod.open_connection(
        "127.0.0.1",
        port,
        token=TOKEN,
        on_message=lambda f, p: got.setdefault("c:" + f, []).append(p),
        **KW,
    )
    await asyncio.sleep(0.1)
    assert client.verified
    client.send_message("chat", b"ping")
    await asyncio.sleep(0.1)
    assert got.get("chat") == [b"ping"]
    assert got.get("c:welcome") == [b"hi"]
    await client.close("done")
    server.close()
    await server.wait_closed()


async def test_wrong_token_rejected() -> None:
    got: dict = {}
    server, port = await start_echo_server(got)
    client = await conn_mod.open_connection(
        "127.0.0.1", port, token="WRONG", on_message=lambda f, p: None, **KW
    )
    for _ in range(50):
        if client.closed:
            break
        await asyncio.sleep(0.05)
    assert client.closed
    assert "handshake rejected" in client.close_reason or "read eof" in client.close_reason
    server.close()
    await server.wait_closed()


async def test_close_hooks_fire() -> None:
    got: dict = {}
    server, port = await start_echo_server(got)
    client = await conn_mod.open_connection(
        "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
    )
    hooked: list[str] = []
    client.add_close_hook(lambda c: hooked.append(c.close_reason))
    await client.close("manual")
    assert hooked == ["manual"]
    # double close is a no-op
    await client.close("again")
    assert hooked == ["manual"]
    server.close()
    await server.wait_closed()


async def test_send_on_closed_raises() -> None:
    got: dict = {}
    server, port = await start_echo_server(got)
    client = await conn_mod.open_connection(
        "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
    )
    await client.close("bye")
    with pytest.raises(conn_mod.ConnectionClosedError):
        client.send_message("x", b"y")
    server.close()
    await server.wait_closed()
