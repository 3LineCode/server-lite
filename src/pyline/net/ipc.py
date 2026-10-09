"""Inter-process message bus on ZeroMQ (ROUTER/DEALER star topology).

The main process owns the ROUTER; every other process has a DEALER whose ZMQ
identity is its 4-byte service number. All payloads travel inside ZMQ itself
-- the prototype's shared-memory single-slot buffer (which silently
overwrote in-flight data) is gone entirely.

Wire format (after ZMQ identity handling)::

    DEALER -> ROUTER:  [target(4B), from(4B), flag, payload]
    ROUTER -> DEALER:  [from(4B), flag, payload]

``flag`` is the utf-8 protocol name. The ROUTER inspects ``target``: own
service number dispatches locally, anything else is forwarded to the DEALER
with that identity. ROUTER_MANDATORY is enabled so sends to vanished peers
raise instead of silently dropping (counted in ``unroutable_sends``).

Bus authentication (``@busauth0``/``@busauth1``/``@busauth2``): before any
data flows, every DEALER proves it holds the inter-server token with an
HMAC challenge-response whose nonces are random per connection. The ROUTER
drops all frames from identities that have not completed the handshake --
without this, the identity==from check (F-39) only constrained honest peers:
on the default Windows endpoint (loopback TCP, no filesystem permission to
gate it) any local process could set its DEALER identity to a victim's
service number and inject frames as that service. The token itself never
crosses the bus, so a sniffed digest from one connection is useless on any
other (fresh nonces).

Send architecture (F-15, per the ZeroMQ guide's queue-broker pattern): the
recv loop never sends. Every outbound destination has its own bounded queue
-- bounded by message count AND queued bytes (frames may be up to
``max_frame_size`` large, so a count-only bound let ~16 GiB pile up before
firing) -- and one writer task, so (a) a slow DEALER whose HWM is full
stalls only its own queue -- never the whole bus -- and (b) one writer per
destination serializes sends, preserving per-destination FIFO order.
Overflowing a queue drops the message and counts it (``dropped_sends``),
mirroring ZMQ's own HWM semantics for fire-and-forget traffic; callers that
need to know (RPC CALLs) pass ``raise_on_drop`` and get ``BusOverflowError``
instead of a timeout ten seconds later. New destinations beyond
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
import secrets
import sys
import time

import zmq
import zmq.asyncio
from prometheus_client import Counter

from pyline.config.models import ZeroMQSettings
from pyline.core.context import SERVICE_NO_STRIDE, Context
from pyline.net.auth import NONCE_LEN, digest_matches, hmac_digest, inter_token
from pyline.net.gateway import ProtocolGateway
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

# Identity key for the DEALER's single destination (the ROUTER).
_ROUTER_PEER = -1

# Target value of bus-control frames (the auth handshake): reserved, never a
# service number, so the ROUTER can intercept these before routing.
_BUS_CONTROL_TARGET = 0

BUS_AUTH0 = "@busauth0"  # DEALER -> ROUTER: client nonce
BUS_AUTH1 = "@busauth1"  # ROUTER -> DEALER: server nonce + HMAC(client||server)
BUS_AUTH2 = "@busauth2"  # DEALER -> ROUTER: HMAC(server)

# How long an incomplete handshake stays in the ROUTER's pending table before
# it is purgeable, and how many concurrent attempts the table tolerates (a
# rogue local process spamming @busauth0 with random identities must not grow
# it without bound).
_AUTH_PENDING_TTL = 10.0
_MAX_PENDING_AUTH = 64

# F-49: how long a destination must stay silent (with an empty queue) before
# its slot is reclaimable when the table is under pressure.
_PEER_IDLE_TTL = 300.0

# F-76: writer failures that are not ZMQ routing errors (a crashing encoder,
# a closed socket in an unexpected state...). The metrics registry lives in
# obs.metrics, which this module cannot grow from here, so the counter is
# declared locally in the same pyline_* namespace.
_WRITER_ERRORS = Counter("pyline_ipc_writer_errors_total", "ZMQ peer writer tasks: crashes")
_BUS_AUTH_REJECTS = Counter(
    "pyline_bus_auth_rejects_total",
    "ZMQ bus handshake attempts rejected (bad digest/shape/forged)",
)
_BUS_UNAUTHENTICATED = Counter(
    "pyline_bus_unauthenticated_total",
    "ZMQ bus data frames dropped from identities that never completed the handshake",
)

# F-75: permissions for a POSIX ipc:// endpoint. The bind file sits in a
# world-writable directory (/tmp) by default; 0600 restricts it to the
# owning uid so local users cannot snoop or inject inter-process traffic.
_IPC_ENDPOINT_MODE = 0o600


class BusAuthError(RuntimeError):
    """The bus HMAC handshake did not complete (wrong token or dead ROUTER)."""


class BusOverflowError(RuntimeError):
    """A per-destination queue (count or byte budget) refused the message."""


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
        self._token = inter_token(ctx)
        self._zctx = zmq.asyncio.Context()
        self._socket: zmq.asyncio.Socket | None = None
        self._recv_task: asyncio.Task[None] | None = None
        # Per-destination outbound queues + their single writer tasks.
        self._peer_queues: dict[int, asyncio.Queue[list[bytes]]] = {}
        self._peer_tasks: dict[int, asyncio.Task[None]] = {}
        self._peer_last_used: dict[int, float] = {}
        # Queued bytes per destination (the F-21 analogue for the bus): a
        # count-only bound lets ``queue_bound`` messages of up to
        # ``max_frame_size`` bytes each pile up before it fires.
        self._peer_queue_bytes: dict[int, int] = {}
        # Handshake state. ROUTER side: identities that completed the
        # challenge-response (``_auth_pending`` holds the in-flight ones).
        # DEALER side: the nonce we sent and the event set once OUR handshake
        # completed (start() blocks on it).
        self._authed: set[bytes] = set()
        self._auth_pending: dict[bytes, tuple[bytes, bytes, float]] = {}
        self._client_nonce: bytes | None = None
        self._authed_event = asyncio.Event()
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
        # the destination table is full -- vanished peers no longer occupy the
        # table forever.
        self._peer_idle_ttl = _PEER_IDLE_TTL if peer_idle_ttl is None else peer_idle_ttl
        self.sent_messages = 0
        self.recv_messages = 0
        self.unroutable_sends = 0
        self.dropped_sends = 0
        self.dest_overflow = 0
        self.spoofed_messages = 0
        self.auth_rejects = 0
        self.unauthenticated_drops = 0
        # F-76: writer crashes that were not ZMQ routing errors.
        self.writer_errors = 0
        self._metrics = get_metrics()

    async def start(self) -> None:
        if self._ctx.is_main_process:
            self._socket = self._zctx.socket(zmq.ROUTER)
            self._socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
            self._socket.setsockopt(zmq.SNDHWM, self._settings.hwm)
            self._socket.setsockopt(zmq.RCVHWM, self._settings.hwm)
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
            self._socket.setsockopt(zmq.SNDHWM, self._settings.hwm)
            self._socket.setsockopt(zmq.RCVHWM, self._settings.hwm)
            self._socket.setsockopt(zmq.RECONNECT_IVL, self._settings.reconnect_min_ms)
            self._socket.setsockopt(zmq.RECONNECT_IVL_MAX, self._settings.reconnect_max_ms)
            endpoint = normalize_endpoint(
                self._settings.bind_host if sys.platform == "win32" else self._settings.bind_file
            )
            self._socket.connect(endpoint)
            logger.info("zmq DEALER connected to %s (identity=%d)", endpoint, self._ctx.service_no)
        self._recv_task = asyncio.get_running_loop().create_task(self._recv_loop())
        if not self.is_router:
            # The handshake rides the same single-writer queue, so it is the
            # first frame the ROUTER sees from this identity -- data sent
            # after start() follows it in FIFO order and cannot be dropped
            # for arriving pre-authentication.
            self._client_nonce = secrets.token_bytes(NONCE_LEN)
            self._enqueue(
                _ROUTER_PEER,
                [
                    service_no_bytes(_BUS_CONTROL_TARGET),
                    service_no_bytes(self._ctx.service_no),
                    BUS_AUTH0.encode("utf-8"),
                    self._client_nonce,
                ],
            )
            try:
                await asyncio.wait_for(
                    self._authed_event.wait(), timeout=self._settings.auth_timeout
                )
            except TimeoutError as exc:
                raise BusAuthError(
                    "zmq bus handshake did not complete within "
                    f"{self._settings.auth_timeout:.0f}s (wrong inter_token, or the "
                    "main-process ROUTER never came up)"
                ) from exc
            logger.info("zmq bus handshake complete (identity=%d)", self._ctx.service_no)

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
        raise_on_drop: bool = False,
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

        ``raise_on_drop`` turns a refused enqueue (queue full by count or
        bytes, destination table full) into ``BusOverflowError`` instead of
        the historical drop-and-count: a caller waiting on a result (an RPC
        CALL) then fails fast instead of burning its full timeout.
        """
        origin = self._ctx.service_no if from_service is None else from_service
        if target_service_no == self._ctx.service_no:
            self._dispatch_local(flag, payload, origin)
            return
        self.sent_messages += 1
        # Same multipart layout for both roles: ROUTER treats frame 0 as the
        # destination identity; a DEALER's frames reach the ROUTER as
        # [identity, target, from, flag, payload].
        ok = self._enqueue(
            target_service_no if self.is_router else _ROUTER_PEER,
            [
                service_no_bytes(target_service_no),
                service_no_bytes(origin),
                flag.encode("utf-8"),
                payload,
            ],
        )
        if not ok and raise_on_drop:
            raise BusOverflowError(
                f"bus queue to service {target_service_no} refused the message "
                "(full by count/bytes, or destination table full)"
            )

    # ---------------------------- outbound queues ---------------------------- #

    def _enqueue(self, peer: int, frames: list[bytes]) -> bool:
        """Queue one message for ``peer``; False when it was refused."""
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
                return False
            queue = asyncio.Queue(maxsize=self._settings.queue_bound)
            self._peer_queues[peer] = queue
            self._peer_tasks[peer] = asyncio.get_running_loop().create_task(
                self._peer_writer(peer, queue)
            )
        self._peer_last_used[peer] = time.monotonic()
        size = sum(len(frame) for frame in frames)
        budget = self._settings.queue_bytes
        if self._peer_queue_bytes.get(peer, 0) + size > budget:
            self.dropped_sends += 1
            self._metrics.ipc_dropped.inc()
            logger.warning(
                "zmq send queue for %s over byte budget (%d + %d > %d); dropping "
                "message (dropped=%d)",
                "router" if peer == _ROUTER_PEER else peer,
                self._peer_queue_bytes.get(peer, 0),
                size,
                budget,
                self.dropped_sends,
            )
            return False
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
            return False
        self._peer_queue_bytes[peer] = self._peer_queue_bytes.get(peer, 0) + size
        return True

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
            self._peer_queue_bytes.pop(peer, None)
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
            size = sum(len(frame) for frame in frames)
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
                # Byte budget release: the frames left our accounting the
                # moment the socket took them (delivered or failed) -- the
                # budget bounds OUR queue memory, not ZMQ's internal buffers.
                self._peer_queue_bytes[peer] = max(self._peer_queue_bytes.get(peer, 0) - size, 0)

    def queue_depth(self) -> int:
        """Total depth across per-destination outbound queues."""
        return sum(q.qsize() for q in self._peer_queues.values())

    def queued_bytes(self) -> int:
        """Total queued bytes across per-destination outbound queues."""
        return sum(self._peer_queue_bytes.values())

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
            flag = flag_b.decode("utf-8")
            if flag in (BUS_AUTH0, BUS_AUTH2):
                self._router_auth_step(identity_b, flag, payload)
                return
            if identity_b not in self._authed:
                # No handshake, no data: without this gate the Windows
                # loopback endpoint (no filesystem permission to protect it)
                # let any local process speak as any service number.
                self.unauthenticated_drops += 1
                _BUS_UNAUTHENTICATED.inc()
                logger.warning(
                    "zmq data frame from unauthenticated identity %d; dropped (total=%d)",
                    int.from_bytes(identity_b, "big"),
                    self.unauthenticated_drops,
                )
                return
            target = int.from_bytes(target_b, "big")
            if target == self._ctx.service_no:
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
            flag = flag_b.decode("utf-8")
            if flag == BUS_AUTH1:
                self._dealer_auth_reply(payload)
                return
            self._dispatch_local(flag, payload, int.from_bytes(from_b, "big"))

    # ------------------------------ bus auth ------------------------------ #

    def _purge_stale_auth(self, now: float) -> None:
        for identity, (_cn, _sn, deadline) in list(self._auth_pending.items()):
            if now > deadline:
                del self._auth_pending[identity]

    def _router_auth_step(self, identity: bytes, flag: str, payload: bytes) -> None:
        """ROUTER side of the handshake: challenge AUTH0, verify AUTH2."""
        now = time.monotonic()
        self._purge_stale_auth(now)
        if flag == BUS_AUTH0:
            if identity in self._authed:
                return  # re-auth of a live peer: idempotent no-op
            if len(payload) != NONCE_LEN:
                self._reject_auth(identity, "bad auth0 nonce length")
                return
            if len(self._auth_pending) >= _MAX_PENDING_AUTH:
                self._reject_auth(identity, "pending handshake table full")
                return
            server_nonce = secrets.token_bytes(NONCE_LEN)
            self._auth_pending[identity] = (
                payload,
                server_nonce,
                now + _AUTH_PENDING_TTL,
            )
            # Reply [identity, from=router, flag, payload]: server nonce plus
            # HMAC(token, client_nonce || server_nonce) -- the DEALER can
            # verify it is talking to a ROUTER that holds the token, not a
            # local impostor that stole the endpoint after our crash.
            self._enqueue(
                int.from_bytes(identity, "big"),
                [
                    identity,
                    service_no_bytes(self._ctx.service_no),
                    BUS_AUTH1.encode("utf-8"),
                    server_nonce + hmac_digest(self._token, payload, server_nonce),
                ],
            )
            return
        # BUS_AUTH2: HMAC(token, server_nonce) -- proof the peer holds the
        # token; only valid against the nonce this ROUTER generated.
        entry = self._auth_pending.get(identity)
        if entry is None:
            self._reject_auth(identity, "auth2 without a pending handshake")
            return
        _client_nonce, server_nonce, _deadline = entry
        if not digest_matches(hmac_digest(self._token, server_nonce), payload):
            self._reject_auth(identity, "bad auth2 digest")
            return
        del self._auth_pending[identity]
        self._authed.add(identity)
        logger.info("zmq bus peer %d authenticated", int.from_bytes(identity, "big"))

    def _reject_auth(self, identity: bytes, reason: str) -> None:
        self._auth_pending.pop(identity, None)
        self.auth_rejects += 1
        _BUS_AUTH_REJECTS.inc()
        logger.warning(
            "zmq bus handshake from identity %d rejected: %s (total=%d)",
            int.from_bytes(identity, "big"),
            reason,
            self.auth_rejects,
        )

    def _dealer_auth_reply(self, payload: bytes) -> None:
        """DEALER side: verify the ROUTER's AUTH1 and answer with AUTH2."""
        if self._client_nonce is None:
            # A stray AUTH1 without our challenge (e.g. after a completed
            # handshake) -- nothing to do.
            return
        if len(payload) != NONCE_LEN + 32:  # server nonce + SHA-256 digest
            logger.warning("zmq bus auth reply has bad length %d; ignored", len(payload))
            return
        server_nonce, claimed = payload[:NONCE_LEN], payload[NONCE_LEN:]
        expected = hmac_digest(self._token, self._client_nonce, server_nonce)
        if not digest_matches(expected, claimed):
            # Wrong token on the ROUTER (or an impostor endpoint): never
            # answer, never authenticate -- start() fails on its deadline.
            logger.error("zmq bus auth reply failed digest verification")
            return
        self._client_nonce = None  # one-shot: a replayed AUTH1 is ignored
        self._enqueue(
            _ROUTER_PEER,
            [
                service_no_bytes(_BUS_CONTROL_TARGET),
                service_no_bytes(self._ctx.service_no),
                BUS_AUTH2.encode("utf-8"),
                hmac_digest(self._token, server_nonce),
            ],
        )
        self._authed_event.set()

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
        self._peer_queue_bytes.clear()
        self._sending_peers.clear()
        for task in list(self._dying_tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._socket is not None:
            self._socket.close(1)
        # zctx.term() blocks until every socket's linger drains -- up to the
        # 1s close linger per socket, ON the event loop. A teardown step that
        # parks here stalls the whole shutdown plan; drain it off-loop.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._zctx.term)
