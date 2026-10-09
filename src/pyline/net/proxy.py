"""Cross-server proxy: machines without direct connectivity relay through a
designated proxy server.

Topology mirrors the prototype: each machine's main process keeps one
connection per configured proxy server (excluding itself); a proxy machine's
main process additionally accepts connections from every other machine and
forwards ``@fwd`` frames.

``@fwd`` payload is ``msgpack([target_service, from_service, inner_flag,
inner_payload, hops])``. ``from_service`` is the ORIGINAL sender (F-39) so the
receiving side can validate RPC result origins; a legacy 4-field envelope
without it parses with ``from=0`` (origin unknown). The hop counter bounds
forwarding loops (max 8) -- misconfigured proxy rings drop frames with an
error instead of looping forever.

The proxy validates peers by IP allowlist (every configured ``advertise_ip``)
plus the connection token handshake.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
from typing import cast

import msgpack

import pyline.net.connection as conn_mod
from pyline.core.context import Context
from pyline.net.auth import inter_token as _inter_token_impl
from pyline.net.ipc import main_service_no, service_no_bytes
from pyline.net.protocol import decode_payload
from pyline.net.router import MessageRouter, NoProxyAvailableError
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

FWD_FLAG = "@fwd"
IDENT_FLAG = "@ident"
MAX_HOPS = 8


def build_forward(
    target: int, from_service: int, flag: str, payload: bytes, hops: int = 0
) -> bytes:
    return cast(
        bytes, msgpack.packb([target, from_service, flag, payload, hops], use_bin_type=True)
    )


def parse_forward(data: bytes) -> tuple[int, int, str, bytes, int]:
    """Return ``(target, from_service, flag, payload, hops)``.

    Accepts the legacy 4-field form (no ``from_service``) for mixed-version
    clusters; those parse with ``from_service=0`` = origin unknown, which the
    RPC layer treats as untrusted (F-39/F-40).

    F-78: a non-envelope payload (scalar, string, wrong element types) used
    to escape as TypeError from the ``len()`` below and crash the receiving
    loop; malformed envelopes now raise ValueError, which every caller
    already treats as a drop-and-count condition.
    """
    fields = decode_payload(data)
    if not isinstance(fields, list) or len(fields) not in (4, 5):
        raise ValueError(f"@fwd envelope must be a 4/5-field list, got {type(fields).__name__}")
    if len(fields) == 4:
        target, flag, payload, hops = fields
        from_service = 0
    else:
        target, from_service, flag, payload, hops = fields
    if (
        not isinstance(target, int)
        or not isinstance(from_service, int)
        or not isinstance(flag, str)
        or not isinstance(payload, bytes)
        or not isinstance(hops, int)
    ):
        raise ValueError("@fwd envelope has wrong field types")
    return target, from_service, flag, payload, hops


def inter_token(ctx: Context) -> str:
    """Server-to-server token; see :func:`pyline.net.auth.inter_token`.

    Kept as a re-export: callers (tests included) import it from this module
    historically. The implementation and its once-per-process fallback
    warning live in :mod:`pyline.net.auth`, shared with the ZMQ bus so both
    server-to-server planes derive the same secret."""
    return _inter_token_impl(ctx)


class _CloseSpawner:
    """F-78: fire-and-forget ``connection.close()`` with a kept reference.

    The library's own discipline (F-20): a bare ``create_task`` result can be
    garbage-collected mid-run, silently skipping the close. Small mixin so
    ProxyServer and ProxyClient share one implementation."""

    def __init__(self) -> None:
        self._close_tasks: set[asyncio.Task[None]] = set()

    def _spawn_close(self, connection: conn_mod.Connection, reason: str) -> None:
        task = asyncio.get_running_loop().create_task(connection.close(reason))
        self._close_tasks.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            self._close_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.error("deferred close of %s failed: %r", connection, t.exception())

        task.add_done_callback(_done)


class ProxyServer(_CloseSpawner):
    """Accepts proxy connections on the proxy machine's main process."""

    def __init__(self, ctx: Context, router: MessageRouter) -> None:
        super().__init__()
        self._ctx = ctx
        self._router = router
        self._nodes: dict[int, conn_mod.Connection] = {}  # main service no -> conn
        self._machines: dict[conn_mod.Connection, int] = {}  # F-48: reverse of _nodes
        self._server: asyncio.AbstractServer | None = None
        # F-78: malformed @fwd payloads (bad shape or adversarial nesting).
        self.malformed_fwd = 0
        self._metrics = get_metrics()

    def _inter_token(self) -> str:
        return inter_token(self._ctx)

    async def start(self) -> None:
        entry = self._ctx.entry
        s = self._ctx.settings.socket
        self._server = await conn_mod.serve(
            entry.bind_host(),
            entry.process_port(process_index=0),
            token=self._inter_token(),
            handshake_timeout=s.handshake_timeout,
            idle_timeout=s.idle_timeout,
            send_queue_limit=s.send_queue_limit,
            send_queue_bytes=s.send_queue_bytes,
            max_frame=s.max_frame_size,
            preauth_max_frame=s.preauth_max_frame,
            max_connections=s.max_connections,
            max_connections_per_ip=s.max_connections_per_ip,
            on_message=lambda flag, payload: None,
            on_connected=self._on_connected,
        )
        logger.info("proxy server listening on %s:%d", entry.bind_host(), entry.process_port(0))

    def _on_connected(self, connection: conn_mod.Connection) -> None:
        peer_ip = connection.peer[0]
        known = any(entry.advertise_ip == peer_ip for entry in self._ctx.registry.entries())
        if not known and peer_ip not in ("127.0.0.1", "::1"):
            logger.warning("proxy connection from unconfigured IP %s; closing", peer_ip)
            self._spawn_close(connection, "ip not allowed")
            return
        connection.set_message_handler(
            lambda flag, payload: self._on_frame(connection, flag, payload)
        )

    def _register_ident(self, connection: conn_mod.Connection, machine: int) -> None:
        """Validate an IDENT claim (F-16): the machine must exist, its
        advertised IP must match the peer, and no live connection may already
        claim it (a duplicate claim is treated as hijacking)."""
        try:
            entry = self._ctx.registry.entry(machine)
        except KeyError:
            logger.warning(
                "proxy IDENT for unknown machine %d from %s; closing", machine, connection
            )
            self._spawn_close(connection, "unknown machine")
            return
        peer_ip = connection.peer[0]
        if entry.advertise_ip != peer_ip and peer_ip not in ("127.0.0.1", "::1"):
            logger.warning(
                "proxy IDENT machine %d claims ip %s but connects from %s; closing",
                machine,
                entry.advertise_ip,
                peer_ip,
            )
            self._spawn_close(connection, "machine/ip mismatch")
            return
        existing = self._nodes.get(machine)
        if existing is not None and existing is not connection and not existing.closed:
            logger.critical(
                "proxy IDENT conflict: machine %d already registered by %s; "
                "rejecting new claim from %s",
                machine,
                existing.peer,
                connection.peer,
            )
            self._spawn_close(connection, "duplicate machine claim")
            return
        self._nodes[machine] = connection
        self._machines[connection] = machine

        def drop_node(_conn: conn_mod.Connection, m: int = machine) -> None:
            if self._nodes.get(m) is _conn:
                self._nodes.pop(m, None)
                self._machines.pop(_conn, None)

        connection.add_close_hook(drop_node)
        logger.info("proxy peer registered: machine %d (%s)", machine, connection.peer)

    def _on_frame(self, connection: conn_mod.Connection, flag: str, payload: bytes) -> None:
        if flag == IDENT_FLAG and len(payload) == 4:
            self._register_ident(connection, int.from_bytes(payload, "big"))
            return
        if flag == FWD_FLAG:
            self._on_forward(connection, payload)
            return
        logger.debug("proxy ignoring unknown flag %r", flag)

    def _on_forward(self, connection: conn_mod.Connection, payload: bytes) -> None:
        try:
            target, from_service, inner_flag, inner_payload, hops = parse_forward(payload)
        except (ValueError, msgpack.exceptions.ExtraData, RecursionError):
            # F-78: RecursionError = adversarially deep msgpack nesting; a
            # scalar payload now fails inside parse_forward as ValueError
            # instead of a TypeError from len(). Both used to crash the frame
            # handler's caller.
            self.malformed_fwd += 1
            logger.warning(
                "malformed @fwd payload on proxy from %s (total=%d)",
                connection.peer,
                self.malformed_fwd,
            )
            return
        # F-48: bind the claimed origin to the sender's IDENT-registered
        # machine. Without this, any connected machine could stamp a victim's
        # service number on its envelope and sail past the F-39/F-40 origin
        # checks (which compare against the *claim*, not the wire). This is
        # the first proxy hop, so the claim is checkable; relay hops carry a
        # validated third-party origin and are received by ProxyClient, which
        # does not re-check. ``from_service == 0`` is the legacy no-origin
        # form: it flows on, untrusted, exactly as before.
        if from_service != 0:
            machine = self._machines.get(connection)
            if machine is None or main_service_no(from_service) != machine:
                self._metrics.proxy_spoofed.inc()
                logger.error(
                    "@fwd from %s claims origin %d but connection is registered as "
                    "machine %s; dropping",
                    connection.peer,
                    from_service,
                    machine,
                )
                return
        if main_service_no(target) == self._ctx.main_service_no:
            self._router.route(
                inner_flag, inner_payload, target, from_service=from_service, hops=hops
            )
            return
        if hops >= MAX_HOPS:
            logger.error("@fwd exceeded max hops (%d); dropping message to %d", hops, target)
            return
        node = self._nodes.get(main_service_no(target))
        if node is None:
            logger.warning(
                "proxy has no connection to machine %d (target=%d)", main_service_no(target), target
            )
            return
        node.send_message(
            FWD_FLAG, build_forward(target, from_service, inner_flag, inner_payload, hops + 1)
        )

    def forward(
        self, target: int, from_service: int, flag: str, payload: bytes, hops: int = 0
    ) -> bool:
        """Direct forwarding via this proxy's node table (local machine only).

        ``hops`` is the number of proxy hops the message has already
        traversed; the outbound envelope carries ``hops + 1`` so MAX_HOPS
        still bounds loops that bounce through the router."""
        node = self._nodes.get(main_service_no(target))
        if node is None:
            return False
        node.send_message(FWD_FLAG, build_forward(target, from_service, flag, payload, hops + 1))
        return True

    async def close(self) -> None:
        if self._server is not None:
            await conn_mod.close_server(self._server)
            self._server = None
        for task in list(self._close_tasks):  # F-78: settle deferred closes
            with contextlib.suppress(asyncio.CancelledError):
                await task


class ProxyClient(_CloseSpawner):
    """Connects to all configured proxy servers (excluding self), with
    exponential-backoff reconnect; used for cross-server sends."""

    def __init__(self, ctx: Context, router: MessageRouter) -> None:
        super().__init__()
        self._ctx = ctx
        self._router = router
        self._proxies: dict[int, conn_mod.Connection] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._failed_sends = 0
        self._seq = itertools.count(1)
        # F-78: malformed inbound @fwd payloads.
        self.malformed_fwd = 0

    async def start(self) -> None:
        for proxy_no in self._ctx.registry.proxy_list():
            if proxy_no == self._ctx.server_no:
                continue
            entry = self._ctx.registry.entry(proxy_no)
            self._tasks.append(
                asyncio.get_running_loop().create_task(
                    self._maintain(proxy_no, entry.advertise_ip, entry.process_port(0))
                )
            )

    async def _maintain(self, proxy_no: int, host: str, port: int) -> None:
        backoff = 1.0
        s = self._ctx.settings.socket
        while True:
            try:
                connection = await conn_mod.open_connection(
                    host,
                    port,
                    token=inter_token(self._ctx),
                    handshake_timeout=s.handshake_timeout,
                    idle_timeout=s.idle_timeout,
                    send_queue_limit=s.send_queue_limit,
                    send_queue_bytes=s.send_queue_bytes,
                    max_frame=s.max_frame_size,
                    preauth_max_frame=s.preauth_max_frame,
                    on_message=self._on_frame,
                )
                backoff = 1.0  # connected: reset the reconnect ladder
                self._proxies[proxy_no] = connection

                def drop_hook(conn: conn_mod.Connection, p: int = proxy_no) -> None:
                    self._drop_proxy(conn, p)

                connection.add_close_hook(drop_hook)
                # F-16: this send is inside the guard -- when the server closes
                # the connection in the instant after handshake, the raised
                # ConnectionClosedError used to kill the reconnect task and
                # this proxy stayed offline forever.
                connection.send_message(IDENT_FLAG, service_no_bytes(self._ctx.main_service_no))
                logger.info("connected to proxy %d at %s:%d", proxy_no, host, port)
                await self._wait_closed(connection)
                logger.warning("proxy %d disconnected; reconnecting", proxy_no)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.info("proxy %d link error (%s); retrying in %.1fs", proxy_no, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _drop_proxy(self, connection: conn_mod.Connection, proxy_no: int) -> None:
        """Identity-checked deregistration (same pattern as ProxyServer's
        drop_node): the close hook of a connection that was already replaced
        by a reconnect must not evict its live replacement."""
        if self._proxies.get(proxy_no) is connection:
            self._proxies.pop(proxy_no, None)

    async def _wait_closed(self, connection: conn_mod.Connection) -> None:
        """Park until the connection closes (F-78: event, not a 1s poll).

        The old polling loop added up to a second of dead time to every
        reconnect cycle and spun forever if a connection object leaked; the
        close hook fires exactly once, from the connection itself. A
        connection that is already closed runs the hook inline (no await in
        between, so there is no missed-event window)."""
        closed = asyncio.Event()
        connection.add_close_hook(lambda _conn: closed.set())
        await closed.wait()

    def _on_frame(self, flag: str, payload: bytes) -> None:
        if flag != FWD_FLAG:
            return
        try:
            target, from_service, inner_flag, inner_payload, _hops = parse_forward(payload)
        except (ValueError, msgpack.exceptions.ExtraData, RecursionError):
            # F-78: RecursionError = adversarially deep msgpack nesting (the
            # old classification let it escape and kill the maintain loop's
            # frame handling); scalars now fail inside parse_forward.
            self.malformed_fwd += 1
            logger.warning("malformed @fwd payload (total=%d)", self.malformed_fwd)
            return
        self._router.route(inner_flag, inner_payload, target, from_service=from_service, hops=_hops)

    def send_to_service(
        self,
        target: int,
        flag: str,
        payload: bytes,
        *,
        from_service: int | None = None,
        hops: int = 0,
    ) -> None:
        """Send cross-server via any connected proxy; raises if none.

        F-70: ``from_service`` is the validated original sender when the main
        process forwards a relayed message (``@relay`` from a sub-process or
        an ``@fwd`` received here); ``None`` keeps the historical behaviour
        of stamping this process. The receiving proxy binds the claimed
        origin to the sending connection's machine (F-48), so the origin
        must always belong to this machine when relaying.

        ``hops`` is the number of proxy hops already traversed; stamping 0
        here (as before) meant a registry disagreement bouncing a message
        through the router never accumulated toward MAX_HOPS -- the
        documented loop bound only worked along ProxyServer forwards.
        """
        if not self._proxies:
            self._failed_sends += 1
            raise NoProxyAvailableError(
                f"no proxy connected (dropped sends so far: {self._failed_sends})"
            )
        # Prefer a proxy located on the target machine when available.
        proxy = self._proxies.get(main_service_no(target))
        if proxy is None:
            proxy = next(iter(self._proxies.values()))
        origin = self._ctx.service_no if from_service is None else from_service
        proxy.send_message(FWD_FLAG, build_forward(target, origin, flag, payload, hops + 1))

    async def close(self) -> None:
        """Shut down the client: stop the maintain loops AND close every
        established proxy connection (cancelling the tasks alone used to leak
        the sockets -- the connections only died on idle timeout)."""
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for connection in list(self._proxies.values()):
            await connection.close("proxy client shutdown")
        self._proxies.clear()
        for task in list(self._close_tasks):  # F-78: settle deferred closes
            with contextlib.suppress(asyncio.CancelledError):
                await task


__all__ = ["FWD_FLAG", "IDENT_FLAG", "MAX_HOPS", "ProxyClient", "ProxyServer"]
