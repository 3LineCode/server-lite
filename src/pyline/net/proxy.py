"""Cross-server proxy: machines without direct connectivity relay through a
designated proxy server.

Topology mirrors the prototype: each machine's main process keeps one
connection per configured proxy server (excluding itself); a proxy machine's
main process additionally accepts connections from every other machine and
forwards ``@fwd`` frames.

``@fwd`` payload is ``msgpack([target_service, inner_flag, inner_payload,
hops])``. The hop counter bounds forwarding loops (max 8) -- misconfigured
proxy rings drop frames with an error instead of looping forever.

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
from pyline.net.ipc import main_service_no, service_no_bytes
from pyline.net.router import MessageRouter, NoProxyAvailableError

logger = logging.getLogger(__name__)

FWD_FLAG = "@fwd"
IDENT_FLAG = "@ident"
MAX_HOPS = 8


def build_forward(target: int, flag: str, payload: bytes, hops: int = 0) -> bytes:
    return cast(bytes, msgpack.packb([target, flag, payload, hops], use_bin_type=True))


def parse_forward(data: bytes) -> tuple[int, str, bytes, int]:
    target, flag, payload, hops = msgpack.unpackb(data, raw=False, strict_map_key=False)
    return target, flag, payload, hops


def inter_token(ctx: Context) -> str:
    """Server-to-server token; falls back to the client token (F-16).

    The fallback keeps single-token deployments working but widens the blast
    radius of a client-token leak to the inter-server plane -- warn so ops
    can see it in the log instead of discovering it during an incident."""
    s = ctx.settings.socket
    if s.inter_token is None:
        logger.warning(
            "socket.inter_token not set; server-to-server links reuse the CLIENT "
            "token -- configure a separate $env: reference for production"
        )
        return s.token
    return s.inter_token


class ProxyServer:
    """Accepts proxy connections on the proxy machine's main process."""

    def __init__(self, ctx: Context, router: MessageRouter) -> None:
        self._ctx = ctx
        self._router = router
        self._nodes: dict[int, conn_mod.Connection] = {}  # main service no -> conn
        self._server: asyncio.AbstractServer | None = None

    def _inter_token(self) -> str:
        return inter_token(self._ctx)

    async def start(self) -> None:
        entry = self._ctx.entry
        self._server = await conn_mod.serve(
            entry.bind_host(),
            entry.process_port(process_index=0),
            token=self._inter_token(),
            handshake_timeout=self._ctx.settings.socket.handshake_timeout,
            idle_timeout=self._ctx.settings.socket.idle_timeout,
            send_queue_limit=self._ctx.settings.socket.send_queue_limit,
            max_frame=self._ctx.settings.socket.max_frame_size,
            on_message=lambda flag, payload: None,
            on_connected=self._on_connected,
        )
        logger.info("proxy server listening on %s:%d", entry.bind_host(), entry.process_port(0))

    def _on_connected(self, connection: conn_mod.Connection) -> None:
        peer_ip = connection.peer[0]
        known = any(entry.advertise_ip == peer_ip for entry in self._ctx.registry.entries())
        if not known and peer_ip not in ("127.0.0.1", "::1"):
            logger.warning("proxy connection from unconfigured IP %s; closing", peer_ip)
            asyncio.get_running_loop().create_task(connection.close("ip not allowed"))
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
            asyncio.get_running_loop().create_task(connection.close("unknown machine"))
            return
        peer_ip = connection.peer[0]
        if entry.advertise_ip != peer_ip and peer_ip not in ("127.0.0.1", "::1"):
            logger.warning(
                "proxy IDENT machine %d claims ip %s but connects from %s; closing",
                machine,
                entry.advertise_ip,
                peer_ip,
            )
            asyncio.get_running_loop().create_task(connection.close("machine/ip mismatch"))
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
            asyncio.get_running_loop().create_task(connection.close("duplicate machine claim"))
            return
        self._nodes[machine] = connection

        def drop_node(_conn: conn_mod.Connection, m: int = machine) -> None:
            if self._nodes.get(m) is _conn:
                self._nodes.pop(m, None)

        connection.add_close_hook(drop_node)
        logger.info("proxy peer registered: machine %d (%s)", machine, connection.peer)

    def _on_frame(self, connection: conn_mod.Connection, flag: str, payload: bytes) -> None:
        if flag == IDENT_FLAG and len(payload) == 4:
            self._register_ident(connection, int.from_bytes(payload, "big"))
            return
        if flag == FWD_FLAG:
            self._on_forward(payload)
            return
        logger.debug("proxy ignoring unknown flag %r", flag)

    def _on_forward(self, payload: bytes) -> None:
        try:
            target, inner_flag, inner_payload, hops = parse_forward(payload)
        except (ValueError, msgpack.exceptions.ExtraData):
            logger.exception("malformed @fwd payload on proxy")
            return
        if main_service_no(target) == self._ctx.main_service_no:
            self._router.route(inner_flag, inner_payload, target)
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
        node.send_message(FWD_FLAG, build_forward(target, inner_flag, inner_payload, hops + 1))

    def forward(self, target: int, flag: str, payload: bytes) -> bool:
        """Direct forwarding via this proxy's node table (local machine only)."""
        node = self._nodes.get(main_service_no(target))
        if node is None:
            return False
        node.send_message(FWD_FLAG, build_forward(target, flag, payload, 1))
        return True

    async def close(self) -> None:
        if self._server is not None:
            await conn_mod.close_server(self._server)
            self._server = None


class ProxyClient:
    """Connects to all configured proxy servers (excluding self), with
    exponential-backoff reconnect; used for cross-server sends."""

    def __init__(self, ctx: Context, router: MessageRouter) -> None:
        self._ctx = ctx
        self._router = router
        self._proxies: dict[int, conn_mod.Connection] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._failed_sends = 0
        self._seq = itertools.count(1)

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
        while True:
            try:
                connection = await conn_mod.open_connection(
                    host,
                    port,
                    token=inter_token(self._ctx),
                    handshake_timeout=self._ctx.settings.socket.handshake_timeout,
                    idle_timeout=self._ctx.settings.socket.idle_timeout,
                    send_queue_limit=self._ctx.settings.socket.send_queue_limit,
                    max_frame=self._ctx.settings.socket.max_frame_size,
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
        while not connection.closed:
            await asyncio.sleep(1.0)

    def _on_frame(self, flag: str, payload: bytes) -> None:
        if flag != FWD_FLAG:
            return
        try:
            target, inner_flag, inner_payload, _hops = parse_forward(payload)
        except (ValueError, msgpack.exceptions.ExtraData):
            logger.exception("malformed @fwd payload")
            return
        self._router.route(inner_flag, inner_payload, target)

    def send_to_service(self, target: int, flag: str, payload: bytes) -> None:
        """Send cross-server via any connected proxy; raises if none."""
        if not self._proxies:
            self._failed_sends += 1
            raise NoProxyAvailableError(
                f"no proxy connected (dropped sends so far: {self._failed_sends})"
            )
        # Prefer a proxy located on the target machine when available.
        proxy = self._proxies.get(main_service_no(target))
        if proxy is None:
            proxy = next(iter(self._proxies.values()))
        proxy.send_message(FWD_FLAG, build_forward(target, flag, payload, 0))

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


__all__ = ["FWD_FLAG", "IDENT_FLAG", "MAX_HOPS", "ProxyClient", "ProxyServer"]
