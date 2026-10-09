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
    with pytest.raises(conn_mod.ConnectionClosedError):
        await conn_mod.open_connection(
            "127.0.0.1", port, token="WRONG", on_message=lambda f, p: None, **KW
        )
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


class TestSendQueueBytesF21:
    async def test_byte_overflow_closes_connection(self) -> None:
        """F-21: a peer that accepts but never reads trips the BYTE cap (the
        message-count cap stays far away) and the sender closes with an
        error instead of buffering gigabytes."""

        async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            # complete the server side of the handshake, then never read again
            import hashlib
            import hmac as hmac_mod
            import secrets as secrets_mod

            from pyline.net.protocol import FrameDecoder, encode_message

            nonce = secrets_mod.token_bytes(16)
            writer.write(b"".join(encode_message("@challenge", nonce)))
            await writer.drain()
            decoder = FrameDecoder()
            while True:
                data = await reader.read(4096)
                if not data:
                    return
                frames = decoder.feed(data)
                done = False
                for frame in frames:
                    if frame.flag == "@auth":
                        expected = hmac_mod.new(TOKEN.encode(), nonce, hashlib.sha256).digest()
                        assert hmac_mod.compare_digest(frame.payload, expected)
                        writer.write(b"".join(encode_message("@welcome", b"")))
                        await writer.drain()
                        done = True
                        break
                if done:
                    break
            await asyncio.sleep(10)  # accept, never read again, never close

        server = await asyncio.start_server(silent, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            handshake_timeout=5.0,
            idle_timeout=30.0,
            send_queue_limit=4096,  # count guard must never fire first
            send_queue_bytes=8 * 1024,
        )
        overflowed = False
        blob = b"x" * (64 * 1024)
        # 8 MiB total: OS socket buffers absorb the first chunk (drain returns
        # and bytes are released), then drain blocks and the next send sees
        # queued bytes far past the 8 KiB budget.
        for _ in range(128):
            try:
                client.send_message("flood", blob)
            except conn_mod.ConnectionClosedError:
                overflowed = True
                break
        assert overflowed, "byte cap never fired"
        for _ in range(100):
            if client.closed:
                break
            await asyncio.sleep(0.05)
        assert client.closed
        assert "bytes" in client.close_reason
        await conn_mod.close_server(server)

    async def test_small_traffic_within_budget_survives(self) -> None:
        """F-21 sanity: with a small-but-sane byte budget, ordinary traffic
        (written out continuously) never trips the cap."""
        got: dict = {}
        server, port = await start_echo_server(got)
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            send_queue_bytes=16 * 1024,
            **KW,
        )
        await asyncio.sleep(0.1)
        for i in range(50):
            client.send_message("chat", str(i).encode())
        await asyncio.sleep(0.3)
        assert not client.closed
        assert len(got.get("chat", [])) == 50
        await client.close("done")
        await conn_mod.close_server(server)


class TestSendSideFrameCapF71:
    async def test_oversized_payload_rejected_immediately(self) -> None:
        """F-71: a payload larger than max_frame raises on the SEND side
        before any byte hits the wire. Each chunk used to be a legal frame
        inside the byte budget, so the stream went out and only the peer's
        16 MiB reassembly cap caught it -- minutes later, as a disconnect."""
        from pyline.net.protocol import ProtocolError

        got: dict = {}
        server, port = await start_echo_server(got)
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            max_frame=64 * 1024,
            chunk_size=1024,  # every chunk individually tiny and legal
            **KW,
        )
        await asyncio.sleep(0.1)
        with pytest.raises(ProtocolError, match="max frame"):
            client.send_message("flood", b"x" * (64 * 1024 + 1))
        # nothing was queued: the connection itself is untouched
        assert not client.closed
        client.send_message("chat", b"still-fine")
        await asyncio.sleep(0.2)
        assert got.get("chat") == [b"still-fine"]
        await client.close("done")
        await conn_mod.close_server(server)

    async def test_exact_max_frame_is_allowed(self) -> None:
        """F-71 boundary sanity: a payload of exactly max_frame passes (the
        decoder accepts a reassembled message of exactly max_frame)."""
        got: dict = {}
        server, port = await start_echo_server(got)
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            max_frame=8 * 1024,
            chunk_size=1024,
            **KW,
        )
        await asyncio.sleep(0.1)
        blob = b"y" * (8 * 1024)
        client.send_message("big", blob)
        await asyncio.sleep(0.3)
        assert got.get("big") == [blob]
        await client.close("done")
        await conn_mod.close_server(server)


class TestConnectionCapsF72:
    async def test_global_cap_rejects_beyond_limit(self) -> None:
        """F-72: with a 2-connection cap the third accept is refused
        pre-handshake (no tasks, no decode buffer) and counted."""
        from prometheus_client import REGISTRY

        conns: list[conn_mod.Connection] = []
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: None,
            on_connected=lambda c: None,
            max_connections=2,
            **KW,
        )
        port = server.sockets[0].getsockname()[1]
        before = (
            REGISTRY.get_sample_value("pyline_connections_rejected_total", {"reason": "global"})
            or 0.0
        )
        try:
            for _ in range(2):
                conns.append(
                    await conn_mod.open_connection(
                        "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
                    )
                )
            await asyncio.sleep(0.1)
            assert all(c.verified for c in conns)

            # the refused accept drops the socket immediately: the dial fails
            # fast instead of returning a dying connection
            with pytest.raises(conn_mod.ConnectionClosedError):
                await conn_mod.open_connection(
                    "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
                )
            after = (
                REGISTRY.get_sample_value("pyline_connections_rejected_total", {"reason": "global"})
                or 0.0
            )
            assert after == before + 1
            # the first two are unaffected
            assert all(not c.closed for c in conns)
        finally:
            for conn in conns:
                await conn.close("done")
            await conn_mod.close_server(server)

    async def test_per_ip_cap_rejects_second_from_same_ip(self) -> None:
        """F-72: the per-IP cap fires while the global cap is still far away
        (one connection allowed total, two per IP)."""
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: None,
            on_connected=lambda c: None,
            max_connections=10,
            max_connections_per_ip=1,
            **KW,
        )
        port = server.sockets[0].getsockname()[1]
        try:
            first = await conn_mod.open_connection(
                "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
            )
            await asyncio.sleep(0.1)
            assert first.verified
            with pytest.raises(conn_mod.ConnectionClosedError):
                await conn_mod.open_connection(
                    "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
                )
            assert not first.closed
        finally:
            if first is not None:
                await first.close("done")
            await conn_mod.close_server(server)

    async def test_slot_released_on_close_allows_reconnect(self) -> None:
        """F-72: the cap counts live connections, not lifetime accepts -- a
        closed connection returns its slot via the close hook."""
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: None,
            on_connected=lambda c: None,
            max_connections=1,
            **KW,
        )
        port = server.sockets[0].getsockname()[1]
        try:
            first = await conn_mod.open_connection(
                "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
            )
            await asyncio.sleep(0.05)
            await first.close("done")
            await asyncio.sleep(0.1)
            second = await conn_mod.open_connection(
                "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
            )
            await asyncio.sleep(0.1)
            assert second.verified, "slot was not returned after close"
            await second.close("done")
        finally:
            await conn_mod.close_server(server)


class TestAuthCompareF22:
    async def test_near_miss_tokens_rejected(self) -> None:
        """F-22: constant-time comparison must still reject wrong tokens --
        including same-length off-by-one and prefix/suffix variants (an
        implementation that compared lengths or prefixes would accept)."""
        for bad in ("test-tokenX", "test", "test-token-extra", ""):
            got: dict = {}
            server, port = await start_echo_server(got)
            with pytest.raises(conn_mod.ConnectionClosedError):
                await conn_mod.open_connection(
                    "127.0.0.1", port, token=bad, on_message=lambda f, p: None, **KW
                )
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
        with pytest.raises(conn_mod.ConnectionClosedError, match="handshake timeout"):
            await conn_mod.open_connection(
                "127.0.0.1",
                port,
                token=TOKEN,
                on_message=lambda f, p: None,
                handshake_timeout=0.3,
                idle_timeout=30.0,
                send_queue_limit=64,
            )
        server.close()

    async def test_server_probes_idle_client(self) -> None:
        """F-17: the server side pings quiet links (raw client, no pyline)."""
        import hashlib
        import hmac as hmac_mod

        from pyline.net.protocol import FrameDecoder, encode_message

        got_pings = 0
        seen = asyncio.Event()

        async def raw_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal got_pings
            decoder = FrameDecoder()
            while True:
                data = await reader.read(4096)
                if not data:
                    return
                for frame in decoder.feed(data):
                    if frame.flag == "@challenge":
                        # challenge-response: answer HMAC(token, nonce)
                        digest = hmac_mod.new(
                            TOKEN.encode(), frame.payload, hashlib.sha256
                        ).digest()
                        writer.write(b"".join(encode_message("@auth", digest)))
                        await writer.drain()
                    elif frame.flag == "@ping":
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


class TestChallengeResponseF123:
    """The token never crosses the wire; digests are per-connection bound."""

    async def test_auth_payload_is_hmac_not_token(self) -> None:
        """The @auth frame carries HMAC-SHA256(token, nonce) -- the plaintext
        token appearing on the wire is exactly what this finds."""
        import hashlib
        import hmac as hmac_mod

        captured: dict[str, list[bytes]] = {}
        nonce_holder: dict[str, bytes] = {}

        async def probe_server(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            import secrets as secrets_mod

            from pyline.net.protocol import FrameDecoder, encode_message

            nonce = secrets_mod.token_bytes(16)
            nonce_holder["nonce"] = nonce
            writer.write(b"".join(encode_message("@challenge", nonce)))
            await writer.drain()
            decoder = FrameDecoder()
            while True:
                data = await reader.read(4096)
                if not data:
                    return
                for frame in decoder.feed(data):
                    captured.setdefault(frame.flag, []).append(frame.payload)
                    if frame.flag == "@auth":
                        writer.write(b"".join(encode_message("@welcome", b"")))
                        await writer.drain()
                        return

        server = await asyncio.start_server(probe_server, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            client = await conn_mod.open_connection(
                "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
            )
            assert client.verified
            digests = captured.get("@auth", [])
            assert len(digests) == 1
            assert digests[0] != TOKEN.encode(), "plaintext token crossed the wire"
            assert digests[0] != TOKEN.encode() * 4
            expected = hmac_mod.new(TOKEN.encode(), nonce_holder["nonce"], hashlib.sha256).digest()
            assert hmac_mod.compare_digest(digests[0], expected)
            await client.close("done")
        finally:
            server.close()

    async def test_replayed_digest_rejected_on_new_connection(self) -> None:
        """A sniffed @auth digest is bound to the first connection's nonce:
        replaying it against a fresh connection (fresh nonce) fails."""
        got: dict = {}
        server, port = await start_echo_server(got)
        # connection 1: capture its digest
        client1 = await conn_mod.open_connection(
            "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
        )
        assert client1.verified
        await client1.close("done")
        # connection 2 with the WRONG token: its digest is an HMAC under a
        # different key, rejected just like a replayed foreign digest would be
        # (the server has no way to accept it: the nonce differs).
        with pytest.raises(conn_mod.ConnectionClosedError):
            await conn_mod.open_connection(
                "127.0.0.1", port, token="attacker-guess", on_message=lambda f, p: None, **KW
            )
        await conn_mod.close_server(server)

    async def test_preauth_frame_cap_rejects_oversized_frames(self) -> None:
        """F-124: before verification the decoder accepts only
        preauth_max_frame bytes; a big frame pre-auth kills the link instead
        of parking max_frame (16 MiB) in the decode buffer."""
        got: dict = {}
        server, port = await start_echo_server(got)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            from pyline.net.protocol import FrameDecoder, encode_message

            # consume the @challenge
            decoder = FrameDecoder()
            while "@challenge" not in [f.flag for f in decoder.feed(await reader.read(65536))]:
                pass
            # a 256 KiB frame: legal post-auth (max_frame default 16 MiB) but
            # far past the 64 KiB pre-auth cap
            writer.write(b"".join(encode_message("flood", b"x" * (256 * 1024))))
            await writer.drain()
            try:
                data = await asyncio.wait_for(reader.read(65536), 5.0)
            except (ConnectionResetError, ConnectionAbortedError):
                data = b""  # Windows: the abort arrives as a RST, not clean EOF
            assert data == b"", "server must drop the link on a pre-auth oversized frame"
        finally:
            writer.close()
            await conn_mod.close_server(server)
