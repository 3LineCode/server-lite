"""TCP connection with challenge-response handshake, heartbeat, idle timeout
and send backpressure.

Protocol names starting with ``@`` are reserved for connection control:

* ``@challenge``   -- first frame from the server: 16 random bytes (nonce).
* ``@auth``        -- client's reply: HMAC-SHA256(token, nonce) digest.
* ``@welcome``     -- server's confirmation once the digest verified.
* ``@ping``/``@pong`` -- heartbeat probes (client probes, server answers).

The token never crosses the wire: only keyed hashes of per-connection random
nonces do, so a sniffed handshake is useless for replay (a new connection
gets a fresh nonce) and leaks nothing about the token. The decode buffer is
capped at ``preauth_max_frame`` (kilobytes) until the handshake verifies --
an unauthenticated connection cannot park ``max_frame`` (16 MiB) of frames in
the decoder. Everything else is dispatched to the application ``on_message``
callback after verification. One reader task owns the stream (handshake is
enforced inside the read loop plus a deadline timer), one writer task drains
a bounded send queue; when the peer stops reading and the queue fills up --
by message count OR queued bytes -- the connection is closed -- slow-consumer
protection the prototype lacked.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import secrets
import time
from collections import deque
from collections.abc import Callable

from prometheus_client import Counter

from pyline.net.protocol import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_FRAME,
    Frame,
    FrameDecoder,
    ProtocolError,
    encode_message,
)
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

AUTH_FLAG = "@auth"
WELCOME_FLAG = "@welcome"
CHALLENGE_FLAG = "@challenge"
PING_FLAG = "@ping"
PONG_FLAG = "@pong"

#: Length of the per-connection challenge nonce (bytes).
CHALLENGE_NONCE_LEN = 16

DEFAULT_PREAUTH_MAX_FRAME = 64 * 1024

MessageCallback = Callable[[str, bytes], None]

# F-13: application handler exceptions drop the frame, never the connection;
# but a peer spraying malformed messages must not turn that into a log flood.
DISPATCH_ERROR_LIMIT = 10
DISPATCH_ERROR_WINDOW = 1.0

# F-72: connection attempts refused by the accept-path caps. Declared here
# rather than obs.metrics because this module must stay independently
# importable; the pyline_* namespace is shared.
_CONNECTIONS_REJECTED = Counter(
    "pyline_connections_rejected_total",
    "Incoming connections refused by the global/per-IP accept caps",
    ("reason",),
)

# F-72: defaults mirror SocketSettings.max_connections / .max_connections_per_ip
# so a caller that passes nothing (e.g. today's runtime wiring) still gets the
# documented caps enforced.
DEFAULT_MAX_CONNECTIONS = 4096
DEFAULT_MAX_CONNECTIONS_PER_IP = 256


class ConnectionClosedError(ConnectionError):
    pass


class Connection:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        peer: tuple[str, int],
        token: str,
        handshake_timeout: float,
        idle_timeout: float,
        send_queue_limit: int,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_frame: int = DEFAULT_MAX_FRAME,
        send_queue_bytes: int = 64 * 1024 * 1024,
        preauth_max_frame: int = DEFAULT_PREAUTH_MAX_FRAME,
        is_server_side: bool,
        on_message: MessageCallback,
        on_verified: Callable[[Connection], None] | None = None,
    ) -> None:
        self.peer = peer
        self.is_server_side = is_server_side
        self.verified = False
        self.closed = False
        self.close_reason = ""
        self._reader = reader
        self._writer = writer
        self._token = token
        self._token_bytes = token.encode("utf-8")
        self._handshake_timeout = handshake_timeout
        self._idle_timeout = idle_timeout
        self._chunk_size = chunk_size
        # F-71: kept for the send-side reassembled-size check.
        self._max_frame = max_frame
        self._on_message = on_message
        self._on_verified = on_verified
        # Decode-buffer cap starts at the (tiny) pre-auth limit and widens to
        # the full frame budget once the handshake verifies the peer: the
        # accept caps count connections, not bytes, so without this each
        # unauthenticated connection could park ``max_frame`` in the decoder.
        self._decoder = FrameDecoder(max_frame=min(preauth_max_frame, max_frame))
        self._challenge_nonce: bytes | None = None
        self._send_queue: asyncio.Queue[list[bytes]] = asyncio.Queue(send_queue_limit)
        self._send_queue_bytes = send_queue_bytes
        # F-21: bytes queued for the write loop. ``send_queue_limit`` counts
        # messages only, so worst case ``limit * max_frame`` bytes (~16 GiB)
        # could pile up before the count guard fired; this closes that hole.
        # Incremented on enqueue, decremented once the write loop handed the
        # frames to the transport (after drain returns).
        self._queued_bytes = 0
        self._close_hooks: list[Callable[[Connection], None]] = []
        self._last_recv = time.monotonic()
        self._tasks: list[asyncio.Task[None]] = []
        self._dispatch_errors = 0
        self._error_times: deque[float] = deque()
        self._write_failed = False
        self._closing = False
        # F-78: strong references to fire-and-forget close() tasks. A bare
        # create_task result can be garbage-collected mid-run (the library's
        # own F-20 discipline); a lost close task used to leave the socket
        # open until the idle timeout.
        self._bg_close_tasks: set[asyncio.Task[None]] = set()
        # Handshake/lifecycle events: wait_verified() blocks until the
        # handshake completes or the connection dies, restoring the
        # open_connection contract that callers may send immediately after
        # it returns (with the challenge-response flow the first outbound
        # frame must not race the @challenge -> @auth exchange).
        self._verified_event: asyncio.Event = asyncio.Event()
        self._closed_event: asyncio.Event = asyncio.Event()
        self._metrics = get_metrics()
        self._metrics.connections.inc()

    def _spawn_close(self, reason: str) -> None:
        """Schedule close() and keep the reference (F-78)."""
        task = asyncio.get_running_loop().create_task(self.close(reason))
        self._bg_close_tasks.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            self._bg_close_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.error("deferred close of %s failed: %r", self, t.exception())

        task.add_done_callback(_done)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        # Both sides enforce the deadline (F-17): a client that never hears
        # @welcome must not hang forever either.
        self._tasks.append(loop.create_task(self._handshake_deadline()))
        if self.is_server_side:
            # Challenge first: the peer must prove it holds the token by
            # keyed-hashing THIS connection's random nonce, so the secret
            # never crosses the wire and a captured digest cannot be replayed
            # against any other connection.
            self._challenge_nonce = secrets.token_bytes(CHALLENGE_NONCE_LEN)
            self.send_message(CHALLENGE_FLAG, self._challenge_nonce)
        self._tasks.append(loop.create_task(self._read_loop()))
        self._tasks.append(loop.create_task(self._write_loop()))
        self._tasks.append(loop.create_task(self._idle_watch()))

    async def _handshake_deadline(self) -> None:
        await asyncio.sleep(self._handshake_timeout)
        if not self.verified and not self.closed:
            await self.close("handshake timeout")

    async def _read_loop(self) -> None:
        try:
            while not self.closed:
                data = await self._reader.read(65536)
                if not data:
                    break
                self._last_recv = time.monotonic()
                for frame in self._decoder.feed(data):
                    if self._handle_frame(frame) is False:
                        return
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:
            logger.exception("read loop crashed for %s", self)
        finally:
            await self.close("read eof")

    def _handle_frame(self, frame: Frame) -> bool:
        """Route one decoded frame; returns False to stop the read loop."""
        if not self.verified:
            if self.is_server_side:
                if frame.flag == AUTH_FLAG and self._challenge_nonce is not None:
                    # F-22 lineage: constant-time comparison of the digest.
                    # The expected value is HMAC(token, this connection's
                    # nonce); a sniffed digest from another connection fails
                    # because nonces never repeat in practice (2^-128).
                    expected = hmac.new(
                        self._token_bytes, self._challenge_nonce, hashlib.sha256
                    ).digest()
                    if hmac.compare_digest(frame.payload, expected):
                        self._verified()
                        return True
                logger.warning("rejecting unauthenticated frame %r from %s", frame.flag, self)
                self._spawn_close("handshake rejected")
                return False
            if frame.flag == CHALLENGE_FLAG:
                if len(frame.payload) != CHALLENGE_NONCE_LEN:
                    logger.warning("bad challenge length %d from %s", len(frame.payload), self)
                    self._spawn_close("handshake rejected")
                    return False
                digest = hmac.new(self._token_bytes, frame.payload, hashlib.sha256).digest()
                # verified stays False until the server's @welcome confirms the
                # digest was accepted (F-17; it used to be set unconditionally).
                self.send_message(AUTH_FLAG, digest)
                return True
            if frame.flag == WELCOME_FLAG:
                self._verified()
                return True
            logger.warning("client ignoring pre-welcome frame %r from %s", frame.flag, self)
            return True
        if frame.flag == PING_FLAG:
            self.send_message(PONG_FLAG, b"")
            return True
        if frame.flag == PONG_FLAG:
            return True
        if frame.flag == AUTH_FLAG or frame.flag == WELCOME_FLAG:
            return True
        # F-13: dispatch isolation -- an application exception drops this one
        # frame; only a sustained storm of them takes the connection down.
        try:
            self._on_message(frame.flag, frame.payload)
        except Exception:
            self._dispatch_errors += 1
            self._metrics.dispatch_errors.inc()
            now = time.monotonic()
            self._error_times.append(now)
            while self._error_times and now - self._error_times[0] > DISPATCH_ERROR_WINDOW:
                self._error_times.popleft()
            logger.exception(
                "message handler crashed for %s flag=%r (dropping frame; errors=%d)",
                self,
                frame.flag,
                self._dispatch_errors,
            )
            if len(self._error_times) >= DISPATCH_ERROR_LIMIT:
                self._spawn_close("dispatch error storm")
                return False
        return True

    def _verified(self) -> None:
        """Mark the handshake complete: full frame budget, @welcome, hook."""
        self.verified = True
        self._decoder.set_max_frame(self._max_frame)
        if self.is_server_side:
            self.send_message(WELCOME_FLAG, b"")
        self._verified_event.set()
        if self._on_verified is not None:
            try:
                self._on_verified(self)
            except Exception:
                logger.exception("on_verified hook failed for %s", self)

    async def wait_verified(self) -> None:
        """Block until the handshake completes; raise if the link dies first.

        The handshake deadline task bounds the wait on both sides, so this
        never parks forever."""
        if self.verified:
            return
        verified = asyncio.ensure_future(self._verified_event.wait())
        closed = asyncio.ensure_future(self._closed_event.wait())
        try:
            await asyncio.wait({verified, closed}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            verified.cancel()
            closed.cancel()
        if self.verified:
            return
        raise ConnectionClosedError(
            f"connection {self} closed before the handshake completed ({self.close_reason})"
        )

    async def _write_loop(self) -> None:
        try:
            while not self.closed:
                frames = await self._send_queue.get()
                for chunk in frames:
                    self._writer.write(chunk)
                await self._writer.drain()
                # F-21: the frames reached the transport -- release their
                # bytes back to the budget. Bytes counted here (instead of at
                # dequeue time) stay accounted while drain() is blocked on a
                # peer that stopped reading, which is exactly the case the
                # byte cap exists for.
                self._queued_bytes -= sum(len(chunk) for chunk in frames)
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            self._write_failed = True
            await self.close("write error")
        except Exception:
            self._write_failed = True
            logger.exception("write loop crashed for %s", self)
            await self.close("write error")

    async def _idle_watch(self) -> None:
        ping_interval = max(self._idle_timeout / 3, 1.0)
        while not self.closed:
            await asyncio.sleep(ping_interval)
            if self.closed:
                return
            quiet_for = time.monotonic() - self._last_recv
            if quiet_for > self._idle_timeout:
                await self.close("idle timeout")
                return
            if quiet_for >= ping_interval:
                # Link has been quiet: probe from EITHER side (F-17) -- a
                # server that never probed used to idle-kill third-party
                # clients that only answer pings. Replies refresh _last_recv
                # on both sides, so mutual probes do not loop.
                self.send_message(PING_FLAG, b"")

    # ------------------------------------------------------------------ #
    # Sending
    # ------------------------------------------------------------------ #

    def set_message_handler(self, handler: MessageCallback) -> None:
        """Swap the application message handler (e.g. once the connection is
        identified after handshake). Safe because pre-verification frames
        never reach the handler."""
        self._on_message = handler

    def send_message(self, flag: str, payload: bytes) -> None:
        """Queue one message (non-blocking). Raises ConnectionClosedError if
        the connection is closed (or closing) or the peer consumes too slowly
        (send queue full by message count or queued bytes). Raises
        ProtocolError if the payload alone exceeds ``max_frame`` (F-71)."""
        if self.closed or self._closing:
            raise ConnectionClosedError(f"connection {self} closed ({self.close_reason})")
        # F-71: fail BEFORE encoding. Every individual chunk of an oversized
        # message is a legal frame inside the byte budget, so the old path
        # happily streamed it -- the peer then accumulated the chunks until
        # its 16 MiB reassembly cap killed the connection minutes later (or
        # never, for chunk sizes above the cap). The reassembled size is just
        # len(payload); rejecting here turns a delayed disconnect into an
        # immediate, local error.
        if len(payload) > self._max_frame:
            raise ProtocolError(
                f"payload of {len(payload)} bytes exceeds max frame size "
                f"{self._max_frame}; refusing to send"
            )
        frames = encode_message(flag, payload, chunk_size=self._chunk_size)
        # F-21: budget check on the encoded wire size (frames include headers)
        # before anything is queued -- the same close-and-raise path as the
        # count-based limit below.
        frame_bytes = sum(len(chunk) for chunk in frames)
        if self._queued_bytes + frame_bytes > self._send_queue_bytes:
            self._spawn_close(f"send queue overflow (bytes > {self._send_queue_bytes})")
            raise ConnectionClosedError(
                f"send queue bytes exceeded for {self}; closing "
                f"(queued={self._queued_bytes}, message={frame_bytes})"
            ) from None
        try:
            self._send_queue.put_nowait(frames)
        except asyncio.QueueFull:
            self._spawn_close("send queue overflow")
            raise ConnectionClosedError(f"send queue full for {self}; closing") from None
        self._queued_bytes += frame_bytes

    # ------------------------------------------------------------------ #
    # Closing
    # ------------------------------------------------------------------ #

    def add_close_hook(self, hook: Callable[[Connection], None]) -> None:
        if self.closed:
            hook(self)
            return
        self._close_hooks.append(hook)

    async def close(self, reason: str, *, flush_timeout: float = 2.0) -> None:
        """Close the connection, giving the writer a bounded chance to drain
        queued frames first (F-17: graceful-close traffic used to race the
        write-loop cancellation and could be lost).

        Two-phase: ``_closing`` rejects new sends while the writer keeps
        flushing; ``closed`` lands only once the queue drained (or the
        deadline passed), then the transport is torn down.
        """
        if self.closed or self._closing:
            return
        self._closing = True
        self._metrics.connections.dec()
        self.close_reason = reason
        if flush_timeout > 0 and not self._write_failed:
            deadline = time.monotonic() + flush_timeout
            while self._send_queue.qsize() and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
        self.closed = True
        self._closed_event.set()
        try:
            self._writer.close()
            # wait_closed can block forever while the peer refuses to read
            # (transport still flushing) -- bound it, then abort hard.
            with contextlib.suppress(Exception, TimeoutError):
                await asyncio.wait_for(self._writer.wait_closed(), timeout=flush_timeout)
            self._writer.transport.abort()
        except Exception:
            logger.debug("error closing writer for %s", self, exc_info=True)
        for task in self._tasks:
            task.cancel()
        for hook in self._close_hooks:
            try:
                hook(self)
            except Exception:
                logger.exception("close hook failed for %s", self)
        logger.info("connection %s closed: %s", self, reason)

    def __repr__(self) -> str:
        return f"Connection({self.peer[0]}:{self.peer[1]}, server={self.is_server_side})"


async def open_connection(
    host: str,
    port: int,
    *,
    token: str,
    on_message: MessageCallback,
    **kwargs: object,
) -> Connection:
    """Dial a server and return a started, VERIFIED client-side Connection.

    Blocks until the challenge-response handshake completes (or fails: a
    wrong token closes the link and surfaces as ConnectionClosedError), so
    callers may send immediately -- with the challenge-response flow the
    first caller frame must not race the @challenge -> @auth exchange."""
    reader, writer = await asyncio.open_connection(host, port)
    conn = Connection(
        reader,
        writer,
        peer=writer.get_extra_info("peername") or (host, port),
        token=token,
        is_server_side=False,
        on_message=on_message,
        **kwargs,  # type: ignore[arg-type]
    )
    await conn.start()
    await conn.wait_verified()
    return conn


class _ConnectionLimiter:
    """F-72: global + per-peer-IP accept caps for one listening server.

    Each accepted socket costs a file descriptor, three tasks and up to
    ``max_frame`` of decode buffer BEFORE the token handshake proves the
    peer is legitimate -- unbounded accepts are therefore a cheap resource
    exhaustion attack. Slots are taken at accept time (not after handshake)
    and returned from the connection's close hook, which every Connection
    runs exactly once via its read loop's finally.
    """

    def __init__(self, max_connections: int, max_per_ip: int) -> None:
        self._max = max_connections
        self._max_per_ip = max_per_ip
        self._active = 0
        self._per_ip: dict[str, int] = {}

    def try_acquire(self, ip: str) -> str | None:
        """Reserve a slot for ``ip``; returns the rejection reason or None."""
        if self._max > 0 and self._active >= self._max:
            return "global"
        if self._max_per_ip > 0 and self._per_ip.get(ip, 0) >= self._max_per_ip:
            return "per_ip"
        self._active += 1
        self._per_ip[ip] = self._per_ip.get(ip, 0) + 1
        return None

    def release(self, ip: str) -> None:
        count = self._per_ip.get(ip)
        if count is not None:
            if count <= 1:
                self._per_ip.pop(ip, None)
            else:
                self._per_ip[ip] = count - 1
        self._active = max(self._active - 1, 0)


async def serve(
    host: str,
    port: int,
    *,
    token: str,
    on_message: MessageCallback,
    on_connected: Callable[[Connection], None],
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
    max_connections_per_ip: int = DEFAULT_MAX_CONNECTIONS_PER_IP,
    **kwargs: object,
) -> asyncio.AbstractServer:
    """Listen for verified connections; ``on_connected`` fires post-handshake.

    F-72: connections beyond ``max_connections`` (global) or
    ``max_connections_per_ip`` (same peer IP) are refused at accept time and
    counted in ``pyline_connections_rejected_total{reason}``. The defaults
    mirror SocketSettings; the runtime forwards the configured values."""

    limiter = _ConnectionLimiter(max_connections, max_connections_per_ip)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername") or ("?", 0)
        reason = limiter.try_acquire(peer[0])
        if reason is not None:
            # No Connection is created: no tasks, no decode buffer, no token
            # comparison -- the socket is dropped immediately.
            _CONNECTIONS_REJECTED.labels(reason=reason).inc()
            logger.warning(
                "refusing connection from %s:%s (%s cap reached)",
                peer[0],
                peer[1],
                "per-IP" if reason == "per_ip" else "global",
            )
            writer.close()
            with contextlib.suppress(Exception, TimeoutError):
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            return

        def verified_hook(conn: Connection) -> None:
            on_connected(conn)

        conn = Connection(
            reader,
            writer,
            peer=peer,
            token=token,
            is_server_side=True,
            on_message=on_message,
            on_verified=verified_hook,
            **kwargs,  # type: ignore[arg-type]
        )

        def release_slot(_conn: Connection, ip: str = peer[0]) -> None:
            limiter.release(ip)

        conn.add_close_hook(release_slot)
        await conn.start()

    return await asyncio.start_server(handle, host, port)


async def close_server(server: asyncio.AbstractServer, *, timeout: float = 5.0) -> None:
    """Close a listening server and bounded-wait for it (F-17).

    On 3.12 ``Server.wait_closed()`` can hang forever while client handler
    tasks linger (3.13 adds close_clients/abort_clients for exactly this);
    bound the wait and use the 3.13 helpers when available.
    """
    server.close()
    if hasattr(server, "close_clients"):
        server.close_clients()
    try:
        await asyncio.wait_for(server.wait_closed(), timeout)
    except TimeoutError:
        if hasattr(server, "abort_clients"):
            server.abort_clients()
        logger.warning("server wait_closed timed out after %.1fs; aborting clients", timeout)
