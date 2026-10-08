"""Inter-process message bus on ZeroMQ (ROUTER/DEALER star topology).

The main process owns the ROUTER; every process (including main) has a DEALER
whose ZMQ identity is its 4-byte service number. All payloads travel inside
ZMQ itself -- the prototype's shared-memory single-slot buffer (which silently
overwrote in-flight data) is gone entirely.

Wire format (after ZMQ identity handling)::

    DEALER -> ROUTER:  [target(4B), from(4B), flag, payload]
    ROUTER -> DEALER:  [from(4B), flag, payload]

``flag`` is the utf-8 protocol name. The ROUTER inspects ``target``: own
service number dispatches locally, anything else is forwarded to the DEALER
with that identity. ROUTER_MANDATORY is enabled so sends to vanished peers
raise instead of silently dropping (counted in ``unroutable_sends``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys

import zmq
import zmq.asyncio

from pyline.config.models import ZeroMQSettings
from pyline.core.context import SERVICE_NO_STRIDE, Context
from pyline.net.gateway import ProtocolGateway

logger = logging.getLogger(__name__)


def service_no_bytes(service_no: int) -> bytes:
    """Fixed-width encoding (prototype bug #7: bare int.to_bytes())."""
    return service_no.to_bytes(4, "big", signed=False)


def main_service_no(service_no: int) -> int:
    """Physical-server part of a service number (strip process stride)."""
    return service_no % SERVICE_NO_STRIDE


class ZmqBus:
    def __init__(self, ctx: Context, gateway: ProtocolGateway) -> None:
        self._ctx = ctx
        self._gateway = gateway
        self._settings: ZeroMQSettings = ctx.settings.zeromq
        self._zctx = zmq.asyncio.Context()
        self._socket: zmq.asyncio.Socket | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self.sent_messages = 0
        self.recv_messages = 0
        self.unroutable_sends = 0

    async def start(self) -> None:
        if self._ctx.is_main_process:
            self._socket = self._zctx.socket(zmq.ROUTER)
            self._socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
            self._socket.set_hwm(self._settings.hwm)
            endpoint = (
                self._settings.bind_host if sys.platform == "win32" else self._settings.bind_file
            )
            self._socket.bind(endpoint)
            logger.info("zmq ROUTER bound to %s", endpoint)
        else:
            self._socket = self._zctx.socket(zmq.DEALER)
            self._socket.setsockopt(zmq.IDENTITY, service_no_bytes(self._ctx.service_no))
            self._socket.set_hwm(self._settings.hwm)
            self._socket.setsockopt(zmq.RECONNECT_IVL, self._settings.reconnect_min_ms)
            self._socket.setsockopt(zmq.RECONNECT_IVL_MAX, self._settings.reconnect_max_ms)
            endpoint = (
                self._settings.bind_host if sys.platform == "win32" else self._settings.bind_file
            )
            self._socket.connect(endpoint)
            logger.info("zmq DEALER connected to %s (identity=%d)", endpoint, self._ctx.service_no)
        self._recv_task = asyncio.get_running_loop().create_task(self._recv_loop())

    @property
    def is_router(self) -> bool:
        return self._ctx.is_main_process

    def send(self, target_service_no: int, flag: str, payload: bytes) -> None:
        """Send to another service on this physical server (or self)."""
        if target_service_no == self._ctx.service_no:
            self._dispatch_local(flag, payload)
            return
        self.sent_messages += 1
        asyncio.get_running_loop().create_task(self._async_send(target_service_no, flag, payload))

    async def _async_send(self, target_service_no: int, flag: str, payload: bytes) -> None:
        assert self._socket is not None
        # Same multipart layout for both roles: ROUTER treats frame 0 as the
        # destination identity; a DEALER's frames reach the ROUTER as
        # [identity, target, from, flag, payload].
        message = [
            service_no_bytes(target_service_no),
            service_no_bytes(self._ctx.service_no),
            flag.encode("utf-8"),
            payload,
        ]
        try:
            await self._socket.send_multipart(message)
        except zmq.ZMQError:
            self.unroutable_sends += 1
            logger.warning(
                "zmq cannot route to service %d (unroutable=%d)",
                target_service_no,
                self.unroutable_sends,
            )

    def _dispatch_local(self, flag: str, payload: bytes) -> None:
        self._gateway.dispatch(flag, payload)

    async def _recv_loop(self) -> None:
        assert self._socket is not None
        while True:
            try:
                parts = await self._socket.recv_multipart()
            except zmq.ContextTerminated:
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("zmq recv failed")
                await asyncio.sleep(0.1)
                continue
            try:
                await self._on_recv(parts)
            except Exception:
                logger.exception("zmq message handling failed (parts=%d)", len(parts))

    async def _on_recv(self, parts: list[bytes]) -> None:
        assert self._socket is not None
        self.recv_messages += 1
        if self.is_router:
            # [sender_identity, target, from, flag, payload]
            if len(parts) != 5:
                logger.warning("router got malformed message with %d parts", len(parts))
                return
            _, target_b, from_b, flag_b, payload = parts
            target = int.from_bytes(target_b, "big")
            if target == self._ctx.service_no:
                flag = flag_b.decode("utf-8")
                self._dispatch_local(flag, payload)
            else:
                # Forward: identity = target, then [from, flag, payload].
                await self._socket.send_multipart(
                    [service_no_bytes(target), from_b, flag_b, payload]
                )
        else:
            # [from, flag, payload]
            if len(parts) != 3:
                logger.warning("dealer got malformed message with %d parts", len(parts))
                return
            _, flag_b, payload = parts
            self._dispatch_local(flag_b.decode("utf-8"), payload)

    async def close(self) -> None:
        if self._recv_task is not None:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
        if self._socket is not None:
            self._socket.close(1)
        self._zctx.term()
