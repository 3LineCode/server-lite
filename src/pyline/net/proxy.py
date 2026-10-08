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


class ProxyServer:
    """Accepts proxy connections on the proxy machine's main process."""

    def __init__(self, ctx: Context, router: MessageRouter) -> None:
        self._ctx = ctx
        self._router = router
        self._nodes: dict[int, conn_mod.Connection] = {}  # main service no -> conn
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        entry = self._ctx.entry
        self._server = await conn_mod.serve(
            entry.bind_host(),
            entry.process_port(process_index=0),
            token=self._ctx.settings.socket.token,
            handshake_timeout=self._ctx.settings.socket.handshake_timeout,
            idle_timeout=self._ctx.settings.socket.idle_timeout,
            send_queue_limit=self._ctx.settings.socket.send_queue_limit,
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

    def _on_frame(self, connection: conn_mod.Connection, flag: str, payload: bytes) -> None:
        if flag == IDENT_FLAG and len(payload) == 4:
            machine = int.from_bytes(payload, "big")
            self._nodes[machine] = connection

            def drop_node(_conn: conn_mod.Connection, m: int = machine) -> None:
                self._nodes.pop(m, None)

            connection.add_close_hook(drop_node)
            logger.info("proxy peer registered: machine %d (%s)", machine, connection.peer)
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
            self._server.close()
            await self._server.wait_closed()


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
                    token=self._ctx.settings.socket.token,
                    handshake_timeout=self._ctx.settings.socket.handshake_timeout,
                    idle_timeout=self._ctx.settings.socket.idle_timeout,
                    send_queue_limit=self._ctx.settings.socket.send_queue_limit,
                    on_message=self._on_frame,
                )
            except (TimeoutError, ConnectionError, OSError) as exc:
                logger.info("proxy %d unreachable (%s); retrying in %.1fs", proxy_no, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            backoff = 1.0
            self._proxies[proxy_no] = connection

            def drop_proxy(_conn: conn_mod.Connection, p: int = proxy_no) -> None:
                self._proxies.pop(p, None)

            connection.add_close_hook(drop_proxy)
            connection.send_message(IDENT_FLAG, service_no_bytes(self._ctx.main_service_no))
            logger.info("connected to proxy %d at %s:%d", proxy_no, host, port)
            await self._wait_closed(connection)
            logger.warning("proxy %d disconnected; reconnecting", proxy_no)
            await asyncio.sleep(1.0)

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
        for task in self._tasks:
            task.cancel()


__all__ = ["FWD_FLAG", "IDENT_FLAG", "MAX_HOPS", "ProxyClient", "ProxyServer"]
