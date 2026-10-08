"""TCP connection with token handshake, heartbeat, idle timeout and send
backpressure.

Protocol names starting with ``@`` are reserved for connection control:

* ``@auth``        -- first frame from the client, payload = token string.
* ``@ping``/``@pong`` -- heartbeat probes (client probes, server answers).
* ``@bye``         -- graceful close notice (payload = reason).

Everything else is dispatched to the application ``on_message`` callback, but
only after the server side has verified the handshake. One reader task owns
the stream (handshake is enforced inside the read loop plus a deadline timer),
one writer task drains a bounded send queue; when the peer stops reading and
the queue fills up, the connection is closed -- slow-consumer protection the
prototype lacked.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Callable

from pyline.net.protocol import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_FRAME,
    Frame,
    FrameDecoder,
    encode_message,
)
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

AUTH_FLAG = "@auth"
WELCOME_FLAG = "@welcome"
PING_FLAG = "@ping"
PONG_FLAG = "@pong"
BYE_FLAG = "@bye"

MessageCallback = Callable[[str, bytes], None]

# F-13: application handler exceptions drop the frame, never the connection;
# but a peer spraying malformed messages must not turn that into a log flood.
DISPATCH_ERROR_LIMIT = 10
DISPATCH_ERROR_WINDOW = 1.0


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
        self._handshake_timeout = handshake_timeout
        self._idle_timeout = idle_timeout
        self._chunk_size = chunk_size
        self._on_message = on_message
        self._on_verified = on_verified
        self._decoder = FrameDecoder(max_frame=max_frame)
        self._send_queue: asyncio.Queue[list[bytes]] = asyncio.Queue(send_queue_limit)
        self._close_hooks: list[Callable[[Connection], None]] = []
        self._last_recv = time.monotonic()
        self._tasks: list[asyncio.Task[None]] = []
        self._dispatch_errors = 0
        self._error_times: deque[float] = deque()
        self._write_failed = False
        self._closing = False
        self._metrics = get_metrics()
        self._metrics.connections.inc()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        # Both sides enforce the deadline (F-17): a client that never hears
        # @welcome must not hang forever either.
        self._tasks.append(loop.create_task(self._handshake_deadline()))
        if not self.is_server_side:
            # verified stays False until the server's @welcome confirms the
            # token was accepted (F-17; it used to be set unconditionally).
            self.send_message(AUTH_FLAG, self._token.encode("utf-8"))
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
                if (
                    frame.flag == AUTH_FLAG
                    and self._decode_text(frame.payload) == self._token
                ):
                    self.verified = True
                    self.send_message(WELCOME_FLAG, b"")
                    if self._on_verified is not None:
                        try:
                            self._on_verified(self)
                        except Exception:
                            logger.exception("on_verified hook failed for %s", self)
                    return True
                logger.warning("rejecting unauthenticated frame %r from %s", frame.flag, self)
                asyncio.get_running_loop().create_task(self.close("handshake rejected"))
                return False
            if frame.flag == WELCOME_FLAG:
                self.verified = True
                if self._on_verified is not None:
                    try:
                        self._on_verified(self)
                    except Exception:
                        logger.exception("on_verified hook failed for %s", self)
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
        if frame.flag == BYE_FLAG:
            reason = self._decode_text(frame.payload)
            asyncio.get_running_loop().create_task(self.close(f"peer bye: {reason}"))
            return False
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
                asyncio.get_running_loop().create_task(self.close("dispatch error storm"))
                return False
        return True

    @staticmethod
    def _decode_text(payload: bytes) -> str:
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError:
            return payload.decode("utf-8", "replace")

    async def _write_loop(self) -> None:
        try:
            while not self.closed:
                frames = await self._send_queue.get()
                for chunk in frames:
                    self._writer.write(chunk)
                await self._writer.drain()
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
        the connection is closed (or closing) or the peer consumes too slowly."""
        if self.closed or self._closing:
            raise ConnectionClosedError(f"connection {self} closed ({self.close_reason})")
        frames = encode_message(flag, payload, chunk_size=self._chunk_size)
        try:
            self._send_queue.put_nowait(frames)
        except asyncio.QueueFull:
            asyncio.get_running_loop().create_task(self.close("send queue overflow"))
            raise ConnectionClosedError(f"send queue full for {self}; closing") from None

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
        queued frames first (F-17: graceful-close traffic like ``@bye`` used
        to race the write-loop cancellation and could be lost).

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
    """Dial a server and return a started client-side Connection."""
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
    return conn


async def serve(
    host: str,
    port: int,
    *,
    token: str,
    on_message: MessageCallback,
    on_connected: Callable[[Connection], None],
    **kwargs: object,
) -> asyncio.AbstractServer:
    """Listen for verified connections; ``on_connected`` fires post-handshake."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername") or ("?", 0)

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
