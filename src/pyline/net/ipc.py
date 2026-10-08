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

Send architecture (F-15, per the ZeroMQ guide's queue-broker pattern): the
recv loop never sends. Every outbound destination has its own bounded queue
and one writer task, so (a) a slow DEALER whose HWM is full stalls only its
own queue -- never the whole bus -- and (b) one writer per destination
serializes sends, preserving per-destination FIFO order. Overflowing a
queue drops the message and counts it (``dropped_sends``), mirroring ZMQ's
own HWM semantics for fire-and-forget traffic. New destinations beyond
``max_destinations`` are refused the same way (``dest_overflow``) -- a rogue
DEALER must not grow the ROUTER's queue table without bound.
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
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

# Identity key for a DEALER's single destination (the ROUTER).
_ROUTER_PEER = -1


def service_no_bytes(service_no: int) -> bytes:
    """Fixed-width encoding (prototype bug #7: bare int.to_bytes())."""
    return service_no.to_bytes(4, "big", signed=False)


def main_service_no(service_no: int) -> int:
    """Physical-server part of a service number (strip process stride)."""
    return service_no % SERVICE_NO_STRIDE


class ZmqBus:
    def __init__(
        self,
        ctx: Context,
        gateway: ProtocolGateway,
        *,
        max_destinations: int | None = None,
    ) -> None:
        self._ctx = ctx
        self._gateway = gateway
        self._settings: ZeroMQSettings = ctx.settings.zeromq
        self._zctx = zmq.asyncio.Context()
        self._socket: zmq.asyncio.Socket | None = None
        self._recv_task: asyncio.Task[None] | None = None
        # Per-destination outbound queues + their single writer tasks.
        self._peer_queues: dict[int, asyncio.Queue[list[bytes]]] = {}
        self._peer_tasks: dict[int, asyncio.Task[None]] = {}
        # F-24: cap on tracked destinations. Any DEALER can name any ``target``
        # value; without a cap each new one permanently allocated a queue plus
        # a writer task (unbounded memory growth). ``None`` defers to the
        # zeromq settings (which default to 256).
        self._max_destinations = (
            max_destinations if max_destinations is not None else self._settings.max_destinations
        )
        self.sent_messages = 0
        self.recv_messages = 0
        self.unroutable_sends = 0
        self.dropped_sends = 0
        self.dest_overflow = 0
        self._metrics = get_metrics()

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
        # Same multipart layout for both roles: ROUTER treats frame 0 as the
        # destination identity; a DEALER's frames reach the ROUTER as
        # [identity, target, from, flag, payload].
        self._enqueue(
            target_service_no if self.is_router else _ROUTER_PEER,
            [
                service_no_bytes(target_service_no),
                service_no_bytes(self._ctx.service_no),
                flag.encode("utf-8"),
                payload,
            ],
        )

    # ---------------------------- outbound queues ---------------------------- #

    def _enqueue(self, peer: int, frames: list[bytes]) -> None:
        queue = self._peer_queues.get(peer)
        if queue is None:
            # F-24: never allocate a queue+writer for target values beyond the
            # destination cap -- drop and count instead (mirrors the
            # dropped_sends semantics for full queues).
            if len(self._peer_queues) >= self._max_destinations:
                self.dest_overflow += 1
                self._metrics.ipc_dest_overflow.inc()
                logger.warning(
                    "zmq destination table full (%d peers); dropping message to %s "
                    "(dest_overflow=%d)",
                    self._max_destinations,
                    "router" if peer == _ROUTER_PEER else peer,
                    self.dest_overflow,
                )
                return
            queue = asyncio.Queue(maxsize=self._settings.queue_bound)
            self._peer_queues[peer] = queue
            self._peer_tasks[peer] = asyncio.get_running_loop().create_task(
                self._peer_writer(peer, queue)
            )
        try:
            queue.put_nowait(frames)
        except asyncio.QueueFull:
            self.dropped_sends += 1
            self._metrics.ipc_dropped.inc()
            logger.warning(
                "zmq send queue for %s full; dropping message (dropped=%d)",
                "router" if peer == _ROUTER_PEER else peer,
                self.dropped_sends,
            )

    async def _peer_writer(self, peer: int, queue: asyncio.Queue[list[bytes]]) -> None:
        assert self._socket is not None
        while True:
            frames = await queue.get()
            try:
                await self._socket.send_multipart(frames)
            except zmq.ZMQError:
                self.unroutable_sends += 1
                self._metrics.ipc_unroutable.inc()
                logger.warning(
                    "zmq cannot route to peer %s (unroutable=%d)",
                    "router" if peer == _ROUTER_PEER else peer,
                    self.unroutable_sends,
                )

    def queue_depth(self) -> int:
        """Total depth across per-destination outbound queues."""
        return sum(q.qsize() for q in self._peer_queues.values())

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
                # Enqueue, never send inline -- a slow DEALER must not stall
                # the ROUTER's recv loop for every other peer (F-15).
                self._enqueue(target, [service_no_bytes(target), from_b, flag_b, payload])
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
        for task in self._peer_tasks.values():
            task.cancel()
        for task in self._peer_tasks.values():
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._peer_tasks.clear()
        self._peer_queues.clear()
        if self._socket is not None:
            self._socket.close(1)
        self._zctx.term()
