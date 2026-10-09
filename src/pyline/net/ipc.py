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
DEALER must not grow the ROUTER's queue table without bound. Slots of peers
that stay silent past an idle ttl are reclaimed when the table is full, so
vanished peers do not permanently occupy it (F-49).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import time

import zmq
import zmq.asyncio
from prometheus_client import Counter

from pyline.config.models import ZeroMQSettings
from pyline.core.context import SERVICE_NO_STRIDE, Context
from pyline.net.gateway import ProtocolGateway
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

# Identity key for the DEALER's single destination (the ROUTER).
_ROUTER_PEER = -1

# F-49: how long a destination must stay silent (with an empty queue) before
# its slot is reclaimable when the table is under pressure.
_PEER_IDLE_TTL = 300.0

# F-76: writer failures that are not ZMQ routing errors (a crashing encoder,
# a closed socket in an unexpected state...). The metrics registry lives in
# obs.metrics, which this module cannot grow from here, so the counter is
# declared locally in the same pyline_* namespace.
_WRITER_ERRORS = Counter("pyline_ipc_writer_errors_total", "ZMQ peer writer tasks: crashes")

# F-75: permissions for a POSIX ipc:// endpoint. The bind file sits in a
# world-writable directory (/tmp) by default; 0600 restricts it to the
# owning uid so local users cannot snoop or inject inter-process traffic.
_IPC_ENDPOINT_MODE = 0o600


def service_no_bytes(service_no: int) -> bytes:
    """Fixed-width encoding (prototype bug #7: bare int.to_bytes())."""
    return service_no.to_bytes(4, "big", signed=False)


def main_service_no(service_no: int) -> int:
    """Physical-server part of a service number (strip process stride)."""
    return service_no % SERVICE_NO_STRIDE


def ipc_file_path(endpoint: str) -> str | None:
    """Extract the filesystem path of an ipc endpoint, else ``None``."""
    if endpoint.startswith("ipc://"):
        return endpoint[len("ipc://") :] or None
    if endpoint.startswith("unix://"):
        return endpoint[len("unix://") :]
    if endpoint.startswith("/") and "://" not in endpoint:
        return endpoint
    return None


def normalize_endpoint(endpoint: str) -> str:
    """F-105: a bare filesystem path is not a valid zmq address -- prefix it.

    zmq.bind("/tmp/x.ipc") fails with "invalid address" because the scheme
    decides the transport. Configs written against the old bare-path default
    keep working instead of failing at startup.
    """
    if endpoint.startswith("/") and "://" not in endpoint:
        return f"ipc://{endpoint}"
    return endpoint


def secure_ipc_endpoint(endpoint: str) -> None:
    """F-75: restrict a POSIX IPC socket file to its owner (chmod 0600).

    tcp:// endpoints and Windows named pipes return unchanged. A chmod
    failure is logged, never fatal -- an unwritable mount must not take the
    bus down, but the operator sees it in the log.
    """
    path = ipc_file_path(endpoint)
    if path is None:
        return
    try:
        os.chmod(path, _IPC_ENDPOINT_MODE)
        logger.info("restricted ipc endpoint %s to mode %o", path, _IPC_ENDPOINT_MODE)
    except OSError as exc:
        logger.warning("could not restrict ipc endpoint %s permissions: %s", path, exc)


class ZmqBus:
    def __init__(
        self,
        ctx: Context,
        gateway: ProtocolGateway,
        *,
        max_destinations: int | None = None,
        peer_idle_ttl: float | None = None,
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
        self._peer_last_used: dict[int, float] = {}
        # F-74: peers whose writer is between queue.get() and the completion
        # of send_multipart. Cancelling a writer inside send_multipart can
        # tear a multipart message in half on the socket, after which the
        # peer misframes everything that follows -- idle-slot reclamation
        # must never touch these.
        self._sending_peers: set[int] = set()
        # F-49: cancelled writers held until they finish (no GC'd-task races).
        self._dying_tasks: set[asyncio.Task[None]] = set()
        # F-24: cap on tracked destinations. Any DEALER can name any ``target``
        # value; without a cap each new one permanently allocated a queue plus
        # a writer task (unbounded memory growth). ``None`` defers to the
        # zeromq settings (which default to 256).
        self._max_destinations = (
            max_destinations if max_destinations is not None else self._settings.max_destinations
        )
        # F-49: slots idle longer than this (queue empty) are evictable when
        # the destination table is full -- vanished peers no longer occupy
        # the table forever.
        self._peer_idle_ttl = _PEER_IDLE_TTL if peer_idle_ttl is None else peer_idle_ttl
        self.sent_messages = 0
        self.recv_messages = 0
        self.unroutable_sends = 0
        self.dropped_sends = 0
        self.dest_overflow = 0
        self.spoofed_messages = 0
        # F-76: writer crashes that were not ZMQ routing errors.
        self.writer_errors = 0
        self._metrics = get_metrics()

    async def start(self) -> None:
        if self._ctx.is_main_process:
            self._socket = self._zctx.socket(zmq.ROUTER)
            self._socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
            self._socket.set_hwm(self._settings.hwm)
            endpoint = normalize_endpoint(
                self._settings.bind_host if sys.platform == "win32" else self._settings.bind_file
            )
            self._socket.bind(endpoint)
            # F-75: the bind file of an ipc:// endpoint sits in a world-writable
            # directory by default; lock it to the owning uid right after bind
            # (before any peer can connect). No-op for tcp:// and on Windows.
            if sys.platform != "win32":
                secure_ipc_endpoint(endpoint)
            logger.info("zmq ROUTER bound to %s", endpoint)
        else:
            self._socket = self._zctx.socket(zmq.DEALER)
            self._socket.setsockopt(zmq.IDENTITY, service_no_bytes(self._ctx.service_no))
            self._socket.set_hwm(self._settings.hwm)
            self._socket.setsockopt(zmq.RECONNECT_IVL, self._settings.reconnect_min_ms)
            self._socket.setsockopt(zmq.RECONNECT_IVL_MAX, self._settings.reconnect_max_ms)
            endpoint = normalize_endpoint(
                self._settings.bind_host if sys.platform == "win32" else self._settings.bind_file
            )
            self._socket.connect(endpoint)
            logger.info("zmq DEALER connected to %s (identity=%d)", endpoint, self._ctx.service_no)
        self._recv_task = asyncio.get_running_loop().create_task(self._recv_loop())

    @property
    def is_router(self) -> bool:
        return self._ctx.is_main_process

    def send(
        self,
        target_service_no: int,
        flag: str,
        payload: bytes,
        *,
        from_service: int | None = None,
    ) -> None:
        """Send to another service on this physical server (or self).

        F-70: ``from_service`` is the validated ORIGINAL sender when this
        process relays a message that arrived from elsewhere (proxy ``@fwd``
        delivery); ``None`` (the historical behaviour) stamps this process's
        own service number. Without the parameter the ROUTER used to rewrite
        every relayed frame's origin to the local main process, so an RPC
        reply to a cross-machine caller of a local sub-process reached the
        wrong service and the caller timed out.

        Note a DEALER may only ever name its own number here: the ROUTER
        verifies ``from`` against the sender's ZMQ identity (F-39). Only the
        main process's ROUTER may send third-party origins, which it does
        solely for origins validated by the proxy/@relay paths.
        """
        origin = self._ctx.service_no if from_service is None else from_service
        if target_service_no == self._ctx.service_no:
            self._dispatch_local(flag, payload, origin)
            return
        self.sent_messages += 1
        # Same multipart layout for both roles: ROUTER treats frame 0 as the
        # destination identity; a DEALER's frames reach the ROUTER as
        # [identity, target, from, flag, payload].
        self._enqueue(
            target_service_no if self.is_router else _ROUTER_PEER,
            [
                service_no_bytes(target_service_no),
                service_no_bytes(origin),
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
            # dropped_sends semantics for full queues). F-49: before dropping,
            # try to reclaim slots of peers that went silent -- a vanished
            # DEALER (or a bogus target some rogue DEALER named once) used to
            # occupy its slot forever, so a churned cluster eventually could
            # not talk to anyone new.
            if len(self._peer_queues) >= self._max_destinations and not self._evict_idle_peers():
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
        self._peer_last_used[peer] = time.monotonic()
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

    def _evict_idle_peers(self) -> bool:
        """F-49: drop queue+writer of destinations that are silent past the
        idle ttl with nothing queued. Returns True when at least one slot was
        freed (the caller may then allocate for the new destination).

        F-74: a writer that has dequeued a message and is inside
        send_multipart (queue empty, peer marked in ``_sending_peers``) is
        NOT reclaimable -- cancelling it mid-send can strand half a multipart
        message on the shared socket, after which the peer misframes every
        subsequent message. Such a slot becomes reclaimable again once the
        send completes or errors."""
        now = time.monotonic()
        evicted = False
        for peer in list(self._peer_queues):
            if peer == _ROUTER_PEER:
                continue  # the DEALER's only destination is never churned
            if peer in self._sending_peers:
                continue  # F-74: mid-send writer; see docstring
            if not self._peer_queues[peer].empty():
                continue
            if now - self._peer_last_used.get(peer, 0.0) < self._peer_idle_ttl:
                continue
            del self._peer_queues[peer]
            self._peer_last_used.pop(peer, None)
            task = self._peer_tasks.pop(peer, None)
            if task is not None:
                task.cancel()
                self._dying_tasks.add(task)
                task.add_done_callback(self._dying_tasks.discard)
            evicted = True
            logger.info("zmq destination table: reclaimed idle peer slot %d", peer)
        return evicted

    async def _peer_writer(self, peer: int, queue: asyncio.Queue[list[bytes]]) -> None:
        assert self._socket is not None
        while True:
            frames = await queue.get()
            # F-74: from dequeue until send_multipart returns, this peer's
            # slot must not be reclaimed (see _evict_idle_peers). There is no
            # await between get() and the mark, so the flag is exact.
            self._sending_peers.add(peer)
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
            except asyncio.CancelledError:
                raise
            except Exception:
                # F-76: any non-ZMQ failure used to escape the writer, which
                # killed its task silently -- every later message for that
                # destination piled up in a queue nobody drained. The writer
                # survives, the error is counted and logged.
                self.writer_errors += 1
                _WRITER_ERRORS.inc()
                logger.exception(
                    "zmq peer writer for %s crashed on send (errors=%d)",
                    "router" if peer == _ROUTER_PEER else peer,
                    self.writer_errors,
                )
            finally:
                self._sending_peers.discard(peer)

    def queue_depth(self) -> int:
        """Total depth across per-destination outbound queues."""
        return sum(q.qsize() for q in self._peer_queues.values())

    def _dispatch_local(self, flag: str, payload: bytes, from_service: int) -> None:
        self._gateway.dispatch(flag, payload, from_service)

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
            identity_b, target_b, from_b, flag_b, payload = parts
            # F-39: the claimed ``from`` must match the sender's actual ZMQ
            # identity. Without this check any bus-connected process could
            # impersonate any other service and, via RPC, inject results into
            # live calls. A mismatch drops the message and counts it.
            if identity_b != from_b:
                self.spoofed_messages += 1
                self._metrics.ipc_spoofed.inc()
                logger.warning(
                    "zmq message claims from=%d but sender identity is %d; dropped (spoofed=%d)",
                    int.from_bytes(from_b, "big"),
                    int.from_bytes(identity_b, "big"),
                    self.spoofed_messages,
                )
                return
            target = int.from_bytes(target_b, "big")
            if target == self._ctx.service_no:
                flag = flag_b.decode("utf-8")
                self._dispatch_local(flag, payload, int.from_bytes(from_b, "big"))
            else:
                # Forward: identity = target, then [from, flag, payload].
                # Enqueue, never send inline -- a slow DEALER must not stall
                # the ROUTER's recv loop for every other peer (F-15).
                self._enqueue(target, [service_no_bytes(target), from_b, flag_b, payload])
        else:
            # [from, flag, payload] -- ``from`` was validated against the
            # sender's identity by the ROUTER before forwarding (F-39).
            if len(parts) != 3:
                logger.warning("dealer got malformed message with %d parts", len(parts))
                return
            from_b, flag_b, payload = parts
            self._dispatch_local(flag_b.decode("utf-8"), payload, int.from_bytes(from_b, "big"))

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
        self._peer_last_used.clear()
        self._sending_peers.clear()
        for task in list(self._dying_tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._socket is not None:
            self._socket.close(1)
        self._zctx.term()
