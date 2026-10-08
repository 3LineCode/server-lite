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
    await conn_mod.close_server(server)


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
    await conn_mod.close_server(server)


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
    await conn_mod.close_server(server)


async def test_send_on_closed_raises() -> None:
    got: dict = {}
    server, port = await start_echo_server(got)
    client = await conn_mod.open_connection(
        "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
    )
    await client.close("bye")
    with pytest.raises(conn_mod.ConnectionClosedError):
        client.send_message("x", b"y")
    await conn_mod.close_server(server)


class TestDispatchIsolationF13:
    async def test_handler_exception_keeps_connection(self) -> None:
        """F-13: a crashing handler drops frames; the connection survives."""
        got: dict = {}

        def on_message(flag: str, payload: bytes) -> None:
            if flag == "boom":
                raise ValueError("handler bug")
            got.setdefault(flag, []).append(payload)

        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=on_message,
            on_connected=lambda c: None,
            **KW,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
        )
        await asyncio.sleep(0.1)
        for _ in range(3):
            client.send_message("boom", b"x")  # used to tear the connection down
        client.send_message("chat", b"alive")
        await asyncio.sleep(0.2)
        assert not client.closed
        assert got.get("chat") == [b"alive"]
        await client.close("done")
        await conn_mod.close_server(server)

    async def test_malformed_rpc_frame_does_not_kill_connection(self) -> None:
        """F-13 end-to-end: a hand-crafted @rpc frame with wrong arity is
        dropped by the RPC layer; the TCP connection stays up."""
        got: dict = {}
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: got.setdefault(f, []).append(p),
            on_connected=lambda c: None,
            **KW,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
        )
        await asyncio.sleep(0.1)
        import msgpack

        client.send_message("@rpc", msgpack.packb([1, 2]))  # arity 2, not 5
        client.send_message("chat", b"still-alive")
        await asyncio.sleep(0.2)
        assert not client.closed
        assert got.get("chat") == [b"still-alive"]
        await client.close("done")
        await conn_mod.close_server(server)


class TestLifecycleF17:
    async def test_close_flushes_pending_writes(self) -> None:
        """F-17: messages queued right before close() still reach the peer."""
        got: dict = {}
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: got.setdefault(f, []).append(p),
            on_connected=lambda c: None,
            **KW,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
        )
        await asyncio.sleep(0.1)
        for i in range(40):  # under the queue limit: all buffered at once
            client.send_message("burst", str(i).encode())
        await client.close("done")  # must drain the 40 queued frames first
        await asyncio.sleep(0.3)
        assert len(got.get("burst", [])) == 40
        await conn_mod.close_server(server)

    async def test_client_verified_requires_welcome(self) -> None:
        """F-17: a server that never confirms the handshake times the client out."""
        async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await asyncio.sleep(10)  # never reply, never close

        server = await asyncio.start_server(silent, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            handshake_timeout=0.3,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        assert not client.verified
        for _ in range(40):
            if client.closed:
                break
            await asyncio.sleep(0.05)
        assert client.closed
        assert "handshake timeout" in client.close_reason
        await conn_mod.close_server(server)

    async def test_server_probes_idle_client(self) -> None:
        """F-17: the server side pings quiet links (raw client, no pyline)."""
        from pyline.net.protocol import FrameDecoder, encode_message

        got_pings = 0
        seen = asyncio.Event()

        async def raw_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal got_pings
            # handshake: send @auth
            writer.write(b"".join(encode_message("@auth", TOKEN.encode())))
            await writer.drain()
            decoder = FrameDecoder()
            while True:
                data = await reader.read(4096)
                if not data:
                    return
                for frame in decoder.feed(data):
                    if frame.flag == "@ping":
                        got_pings += 1
                        seen.set()

        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: None,
            on_connected=lambda c: None,
            handshake_timeout=1.0,
            idle_timeout=3.0,  # probes at max(3/3,1)=1s once the link goes quiet
            send_queue_limit=64,
        )
        port = server.sockets[0].getsockname()[1]
        raw = await asyncio.open_connection("127.0.0.1", port)
        # run the raw client loop over the already-open streams
        raw_task = asyncio.get_running_loop().create_task(raw_client(raw[0], raw[1]))
        await asyncio.wait_for(seen.wait(), 5.0)
        assert got_pings >= 1
        raw_task.cancel()
        await conn_mod.close_server(server)
