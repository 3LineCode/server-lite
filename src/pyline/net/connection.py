"""TCP connection with challenge-response handshake, heartbeat, idle timeout
and send backpressure.

Protocol names starting with ``@`` are reserved for connection control:

* ``@challenge``   -- first frame from the server: 16 random bytes (nonce).
* ``@auth``        -- client's reply: HMAC-SHA256(token, server_nonce) digest
  (32 bytes) followed by the client's OWN 16-byte nonce (F-147).
* ``@welcome``     -- server's confirmation: HMAC-SHA256(token, client_nonce)
  (32 bytes) -- the server's proof that IT also holds the token.
* ``@ping``/``@pong`` -- heartbeat probes (client probes, server answers).

The handshake is MUTUAL (F-147): the client proves it holds the token and the
server proves the same back, so neither direction trusts an unauthenticated
peer. The token never crosses the wire: only keyed hashes of per-connection
random nonces do, so a sniffed handshake is useless for replay (a new
connection gets fresh nonces) and leaks nothing about the token. The decode
buffer is capped at ``preauth_max_frame`` (kilobytes) until the handshake
verifies -- an unauthenticated connection cannot park ``max_frame`` (16 MiB)
of frames in the decoder. Everything else is dispatched to the application
``on_message`` callback after verification. One reader task owns the stream
(handshake is enforced inside the read loop plus a deadline timer), one
writer task drains a bounded send queue; when the peer stops reading and the
queue fills up -- by message count OR queued bytes -- the connection is
closed -- slow-consumer protection the prototype lacked.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import secrets
import ssl
import sys
import time
from collections import deque
from collections.abc import Callable

from pyline.config.errors import ConfigError
from pyline.net.protocol import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_FRAME,
    Frame,
    FrameDecoder,
    ProtocolError,
    encode_message,
)
from pyline.net.session import set_current_connection
from pyline.obs.metrics import get_metrics, shared_counter

logger = logging.getLogger(__name__)

AUTH_FLAG = "@auth"
WELCOME_FLAG = "@welcome"
CHALLENGE_FLAG = "@challenge"
PING_FLAG = "@ping"
PONG_FLAG = "@pong"

#: Length of the per-connection challenge nonce (bytes).
CHALLENGE_NONCE_LEN = 16

#: Length of an HMAC-SHA256 digest (the auth/proof payloads' fixed size).
AUTH_DIGEST_LEN = 32

DEFAULT_PREAUTH_MAX_FRAME = 64 * 1024

MessageCallback = Callable[[str, bytes], None]

# F-13: application handler exceptions drop the frame, never the connection;
# but a peer spraying malformed messages must not turn that into a log flood.
DISPATCH_ERROR_LIMIT = 10
DISPATCH_ERROR_WINDOW = 1.0

# F-72: connection attempts refused by the accept-path caps. Created through
# obs.metrics.shared_counter (F-184) so a re-import of this module cannot
# collide with the process-global registry; the pyline_* namespace is shared.
_CONNECTIONS_REJECTED = shared_counter(
    "pyline_connections_rejected_total",
    "Incoming connections refused by the global/per-IP accept caps",
    ("reason",),
)

# F-72: defaults mirror SocketSettings.max_connections / .max_connections_per_ip
# so a caller that passes nothing (e.g. today's runtime wiring) still gets the
# documented caps enforced.
DEFAULT_MAX_CONNECTIONS = 4096
DEFAULT_MAX_CONNECTIONS_PER_IP = 256

# F-161: CPython's win32 select() watches at most ~512 fds. The selector
# loop pyline installs for pyzmq (net.loop_policy) inherits that ceiling for
# EVERY socket it registers -- the client listener, proxy links, the bus.
# docs/deployment.md documented the limit as advice; it is enforced now: a
# config whose accept cap cannot fit under the ceiling fails at bind time
# instead of crashing the loop with "too many file descriptors in select()"
# under load. The reserve leaves room for non-listener sockets (bus DEALER,
# outbound proxy links, stdio).
_WINDOWS_SELECT_FDS = 512
_WINDOWS_RESERVED_FDS = 64

# F-219: the ceiling is PER PROCESS, not per listener: the client listener
# and the proxy server each used to be checked against the full budget, so
# together they could promise the selector loop ~2x what it can watch. Each
# listener label commits its cap to this ledger; the SUM must fit.
_FD_BUDGET_LEDGER: dict[str, int] = {}


def reset_fd_budget_ledger() -> None:
    """Test helper: forget committed reservations from a previous server."""
    _FD_BUDGET_LEDGER.clear()


def check_windows_fd_budget(max_connections: int, *, label: str = "client") -> None:
    """Refuse accept caps the Windows selector loop cannot honour (F-161),
    summed across every listener in this process (F-219)."""
    if sys.platform != "win32":
        return
    _FD_BUDGET_LEDGER[label] = max_connections
    ceiling = _WINDOWS_SELECT_FDS - _WINDOWS_RESERVED_FDS
    committed = sum(_FD_BUDGET_LEDGER.values())
    if committed > ceiling:
        raise ConfigError(
            f"socket.max_connections across this process's listeners "
            f"({_FD_BUDGET_LEDGER!r}) totals {committed} and exceeds the Windows "
            f"selector-loop budget (~{_WINDOWS_SELECT_FDS} fds incl. a "
            f"{_WINDOWS_RESERVED_FDS}-fd reserve for the bus/proxy/stdio): "
            f"lower the caps to a total <= {ceiling} or deploy on POSIX "
            "(see docs/deployment.md, 'Windows fd ceiling')"
        )


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
        max_inflight: int = 0,
    ) -> None:
        self.peer = peer
        self.is_server_side = is_server_side
        self.verified = False
        self.closed = False
        self.close_reason = ""
        # F-225: registry-assigned id (0 = not registered -- proxy links and
        # client-side connections). Assigned by ClientSessionRegistry.register.
        self.conn_id = 0
        # F-224: per-connection handler-task budget (0 = uncapped). Admitted
        # by Network.handle_message through the dispatch contextvar; released
        # by the handler task's done callback -- a frame storm from ONE
        # connection used to be able to occupy the process-global handler
        # pool and starve every other client.
        self.max_inflight = max_inflight
        self._inflight = 0
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
        # F-147: the client-side nonce for the mutual handshake. The client
        # challenges the server inside its @auth reply and verifies the
        # server's @welcome proof against this value; None until the server's
        # @challenge arrives.
        self._client_nonce: bytes | None = None
        # F-147: the proof the server will send with @welcome (computed from
        # the client nonce received in @auth). Server side only.
        self._welcome_proof: bytes | None = None
        self._send_queue: asyncio.Queue[list[bytes]] = asyncio.Queue(send_queue_limit)
        self._send_queue_bytes = send_queue_bytes
        # F-21: bytes queued for the write loop. ``send_queue_limit`` counts
        # messages only, so worst case ``limit * max_frame`` bytes (~16 GiB)
        # could pile up before the count guard fired; this closes that hole.
        # Incremented on enqueue, decremented once the write loop handed the
        # frames to the transport (after drain returns).
        self._queued_bytes = 0
        # F-178: set whenever the send queue holds nothing, cleared by every
        # successful enqueue -- close() waits on it instead of the old 10 ms
        # busy-poll (up to 200 loop wakeups per close for a queue that was
        # already empty). The write loop re-sets it after each dequeue when
        # the queue has drained.
        self._queue_drained = asyncio.Event()
        self._queue_drained.set()
        self._close_hooks: list[Callable[[Connection], None]] = []
        self._last_recv = time.monotonic()
        self._tasks: list[asyncio.Task[None]] = []
        self._dispatch_errors = 0
        self._error_times: deque[float] = deque()
        self._write_failed = False
        self._closing = False
        # F-147: reason claimed synchronously by a rejection path. The read
        # loop's finally-close runs before any spawned close task gets a loop
        # tick, so without this the generic "read eof" always overwrote the
        # diagnostic reason ("handshake rejected") the rejection produced.
        self._claimed_reason: str | None = None
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
        """Schedule close() and keep the reference (F-78).

        The reason is claimed synchronously (F-147): a caller that returns
        False out of the read loop right after this would otherwise have its
        own generic finally-close ("read eof") win the name."""
        self._claimed_reason = self._claimed_reason or reason
        task = asyncio.get_running_loop().create_task(self.close(reason))
        self._bg_close_tasks.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            self._bg_close_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.error("deferred close of %s failed: %r", self, t.exception())

        task.add_done_callback(_done)

    # ------------------------------------------------------------------ #
    # Per-connection handler budget (F-224)
    # ------------------------------------------------------------------ #

    def admit_inflight(self) -> bool:
        """Reserve one handler slot; False when this connection is at its cap."""
        if self.max_inflight <= 0:
            return True
        if self._inflight >= self.max_inflight:
            return False
        self._inflight += 1
        return True

    def release_inflight(self) -> None:
        if self.max_inflight > 0 and self._inflight > 0:
            self._inflight -= 1

    @property
    def inflight(self) -> int:
        return self._inflight

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
        except ProtocolError as exc:
            # F-223: a protocol violation (bad magic/version, oversize) used
            # to land in the catch-all below and close with the generic
            # "read eof" -- the diagnostic only ever lived in the log line.
            self._claimed_reason = self._claimed_reason or f"protocol error: {exc}"
            logger.warning("protocol error on %s: %s", self, exc)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:
            logger.exception("read loop crashed for %s", self)
        finally:
            await self.close(self._claimed_reason or "read eof")

    def _handle_frame(self, frame: Frame) -> bool:
        """Route one decoded frame; returns False to stop the read loop."""
        if not self.verified:
            if self.is_server_side:
                if frame.flag == AUTH_FLAG and self._challenge_nonce is not None:
                    # F-147: @auth carries HMAC(token, server_nonce) || the
                    # client's own nonce. The digest halves stay
                    # constant-time compared (F-22 lineage); a sniffed
                    # digest from another connection fails because nonces
                    # never repeat in practice (2^-128).
                    if len(frame.payload) != AUTH_DIGEST_LEN + CHALLENGE_NONCE_LEN:
                        logger.warning(
                            "bad @auth payload length %d from %s", len(frame.payload), self
                        )
                        self._spawn_close("handshake rejected")
                        return False
                    claimed, client_nonce = (
                        frame.payload[:AUTH_DIGEST_LEN],
                        frame.payload[AUTH_DIGEST_LEN:],
                    )
                    expected = hmac.new(
                        self._token_bytes, self._challenge_nonce, hashlib.sha256
                    ).digest()
                    if hmac.compare_digest(claimed, expected):
                        # The server's @welcome proof: HMAC(token, the
                        # client's nonce) -- the mutual half of F-147.
                        self._welcome_proof = hmac.new(
                            self._token_bytes, client_nonce, hashlib.sha256
                        ).digest()
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
                # F-147: challenge the server back -- @auth carries our own
                # fresh nonce so @welcome must prove the server holds the
                # token too (the client used to accept any @welcome, which a
                # machine-in-the-middle could send while relaying cleartext
                # application frames).
                self._client_nonce = secrets.token_bytes(CHALLENGE_NONCE_LEN)
                self.send_message(AUTH_FLAG, digest + self._client_nonce)
                return True
            if frame.flag == WELCOME_FLAG:
                if self._client_nonce is None or len(frame.payload) != AUTH_DIGEST_LEN:
                    logger.warning("malformed @welcome from %s; closing", self)
                    self._spawn_close("handshake rejected")
                    return False
                expected = hmac.new(self._token_bytes, self._client_nonce, hashlib.sha256).digest()
                if not hmac.compare_digest(frame.payload, expected):
                    logger.error(
                        "@welcome proof failed for %s; server does not hold the token", self
                    )
                    self._spawn_close("handshake rejected")
                    return False
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
        # F-225: the dispatch runs inside the connection context so handlers
        # (and the handler tasks Network spawns from here -- asyncio copies
        # the context at task creation) can ask session.current() which
        # client connection this frame arrived on.
        token = set_current_connection(self)
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
        finally:
            token.var.reset(token)
        return True

    def _verified(self) -> None:
        """Mark the handshake complete: full frame budget, @welcome, hook."""
        self.verified = True
        self._decoder.set_max_frame(self._max_frame)
        if self.is_server_side:
            # F-147: the welcome carries the server's proof over the client's
            # nonce (set while verifying @auth); the empty-payload welcome is
            # gone -- the client now rejects it.
            assert self._welcome_proof is not None
            self.send_message(WELCOME_FLAG, self._welcome_proof)
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
                # F-178: everything dequeued -- wake any close() waiting on
                # the drain (the transport itself is flushed by
                # StreamWriter.close() below).
                if self._send_queue.empty():
                    self._queue_drained.set()
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
                # F-148: an overflowing/already-closed send path raises out
                # of send_message; the close it spawns is the correct
                # outcome, but the escape used to kill this watcher with an
                # unretrieved-exception warning one loop tick later.
                try:
                    self.send_message(PING_FLAG, b"")
                except ConnectionClosedError:
                    return

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
        self._queue_drained.clear()  # F-178: something to drain again

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
            # F-178: event-driven drain wait (was a 10 ms poll loop). The
            # event is set by the write loop once the queue is empty and by
            # nothing else, so a closed/dead writer cannot stall close()
            # past the bound: ``wait_for`` enforces it either way.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._queue_drained.wait(), timeout=flush_timeout)
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
    connect_timeout: float = 10.0,
    ssl_context: ssl.SSLContext | None = None,
    **kwargs: object,
) -> Connection:
    """Dial a server and return a started, VERIFIED client-side Connection.

    Blocks until the challenge-response handshake completes (or fails: a
    wrong token closes the link and surfaces as ConnectionClosedError), so
    callers may send immediately -- with the challenge-response flow the
    first caller frame must not race the @challenge -> @auth exchange.

    F-175: the TCP dial itself is bounded by ``connect_timeout``. A dropped
    SYN used to park ``asyncio.open_connection`` on the OS timeout (roughly
    21 s on Windows, ~2 min on Linux) -- the proxy maintainer's backoff
    ladder never ran, and reconnect cadence was owned by the kernel. The
    timeout raises TimeoutError, which the maintainer's except-all already
    treats as a link error.

    F-188: ``ssl_context`` (from :func:`pyline.net.tls.build_client_context`)
    wraps the dial in TLS; the HMAC handshake then runs inside the tunnel,
    unchanged.
    """
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=ssl_context), timeout=connect_timeout
    )
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
    on_disconnected: Callable[[Connection], None] | None = None,
    max_connections: int = DEFAULT_MAX_CONNECTIONS,
    max_connections_per_ip: int = DEFAULT_MAX_CONNECTIONS_PER_IP,
    max_inflight_per_connection: int = 0,
    label: str = "client",
    ssl_context: ssl.SSLContext | None = None,
    **kwargs: object,
) -> asyncio.AbstractServer:
    """Listen for verified connections; ``on_connected`` fires post-handshake.

    F-72: connections beyond ``max_connections`` (global) or
    ``max_connections_per_ip`` (same peer IP) are refused at accept time and
    counted in ``pyline_connections_rejected_total{reason}``. The defaults
    mirror SocketSettings; the runtime forwards the configured values.

    F-187/F-188: ``ssl_context`` (from
    :func:`pyline.net.tls.build_server_context`) wraps the listener in TLS;
    the challenge-response handshake runs inside the tunnel, unchanged.

    F-225: ``on_disconnected`` (when given) runs from the connection's close
    hook -- exactly once per connection, any close reason. F-219:
    ``label`` names this listener in the process-wide Windows fd budget
    ledger. F-224: ``max_inflight_per_connection`` bounds how many handler
    tasks ONE connection may have running (0 = uncapped)."""

    # F-161: fail at bind time on Windows instead of crashing the selector
    # loop under load; F-219: the budget is shared by every listener in the
    # process (client + proxy), each committing under its own label.
    check_windows_fd_budget(max_connections, label=label)

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
            max_inflight=max_inflight_per_connection,
            **kwargs,  # type: ignore[arg-type]
        )

        def release_slot(_conn: Connection, ip: str = peer[0]) -> None:
            limiter.release(ip)
            if on_disconnected is not None:
                try:
                    on_disconnected(_conn)
                except Exception:
                    logger.exception("on_disconnected hook failed for %s", _conn)

        conn.add_close_hook(release_slot)
        await conn.start()

    return await asyncio.start_server(handle, host, port, ssl=ssl_context)


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
