"""Application network base: sub-protocol dispatch over msgpack payloads.

An inbound frame payload for a network is ``msgpack.packb([sub, *args])``;
sub-protocols are small ints declared per network. Handlers are registered
with :meth:`Network.subscribe` and may be sync or async::

    class GameNet(Network):
        flag = "game"

        def __init__(self, gateway):
            super().__init__(gateway)
            self.subscribe(1, self.on_login)

        async def on_login(self, player_id: int, token: str) -> None: ...
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, cast

import msgpack

from pyline.log.ratelimit import WindowLogLimiter
from pyline.net.gateway import ProtocolGateway
from pyline.net.protocol import decode_payload
from pyline.net.session import current_connection
from pyline.obs.metrics import get_metrics

if TYPE_CHECKING:
    from pyline.net.connection import Connection

logger = logging.getLogger(__name__)

Handler = Callable[..., Any]


def pack_call(sub: int, *args: Any) -> bytes:
    """Build an outbound network payload."""
    return cast(bytes, msgpack.packb([sub, *args], use_bin_type=True))


def unpack_call(payload: bytes) -> tuple[int, list[Any]]:
    """Split an inbound payload into ``(sub, args)``."""
    data = decode_payload(payload)
    # F-194: reject bool -- msgpack encodes it as an int subtype, so ``True``
    # used to dispatch as sub-protocol 1.
    if (
        not isinstance(data, list)
        or not data
        or not isinstance(data[0], int)
        or isinstance(data[0], bool)
    ):
        raise ValueError("malformed network payload: expected [sub, *args]")
    return data[0], list(data[1:])


class Network:
    """Base class for protocol handlers registered on the gateway."""

    flag: ClassVar[str] = ""

    #: F-19 analogue for plain networks: how many handler tasks may run
    #: concurrently before inbound messages are dropped and counted. The RPC
    #: face has its own bounded semaphore with busy replies; a plain network
    #: used to spawn one task per frame with no limit, so a token-holding
    #: client could stack unbounded handler tasks. Blocking the read loop
    #: instead would deadlock request/response handlers that await an RPC
    #: whose RESULT arrives on the same connection, so overflow drops.
    DEFAULT_MAX_INFLIGHT: ClassVar[int] = 256

    def __init__(self, gateway: ProtocolGateway, *, max_inflight: int | None = None) -> None:
        self._gateway = gateway
        self._handlers: dict[int, Handler] = {}
        self._unknown_subs = 0
        self._overflowed = 0
        self._max_inflight = self.DEFAULT_MAX_INFLIGHT if max_inflight is None else max_inflight
        self._metrics = get_metrics()
        # F-171: peer-triggerable noise (bad payloads, unknown subs) is
        # rate-limited; F-179: overflow drops additionally surface through
        # the optional on_overflow hook so a network can signal its peer
        # (close, busy frame) instead of a silent drop.
        self._noise_log = WindowLogLimiter()
        self._overflow_log = WindowLogLimiter()
        self._on_overflow: Callable[[int, int], None] | None = None
        # F-20: strong references to in-flight handler tasks. A bare
        # ``create_task`` result can be garbage-collected mid-run (a known
        # CPython pitfall) and its exception is then never observed; the set
        # plus done callback mirrors scheduler.py's ``_async_tasks`` pattern.
        self._inbound: set[asyncio.Task[None]] = set()
        gateway.register(self)

    def set_overflow_hook(self, hook: Callable[[int, int], None] | None) -> None:
        """F-179: observe every overflow drop with ``(sub, dropped_total)``.

        The RPC face answers a refused CALL with BUSY (rpc.py); a plain
        network used to drop-and-count silently -- for a request/response
        sub-protocol that is indistinguishable from a lost reply until the
        caller's own timeout fires. The hook lets the network decide the
        signal: close the connection, reply an application-level busy frame,
        or just alarm. Never called from the dispatch path's own stack in a
        way that can block -- it runs inline where the drop decision is
        made.
        """
        self._on_overflow = hook

    @property
    def gateway(self) -> ProtocolGateway:
        return self._gateway

    def rebind_handlers(self) -> None:
        """Re-run sub-protocol registration after a hot reload (F-30).

        Existing registrations keep working without this: handler functions
        are updated in place, so live bindings already run the new code.
        Override this to pick up *newly added* sub-protocols (the business
        equivalent of the prototype's automatic ReInitHandlers); call it
        from the module's ``__reload__`` hook or via
        ``gateway.rebind_module()``.
        """

    def subscribe(self, sub: int, handler: Handler) -> None:
        if sub in self._handlers:
            raise ValueError(f"{type(self).__qualname__}: sub-protocol {sub} already registered")
        self._handlers[sub] = handler

    def handle_message(self, flag: str, payload: bytes, from_service: int = 0) -> None:
        """Entry point from the gateway; schedule the matching handler.

        ``from_service`` is the validated origin (F-39), 0 when the dispatch
        path cannot establish one. Plain networks may ignore it; trust-sensitive
        networks (see RpcManager) must not.
        """
        try:
            sub, args = unpack_call(payload)
        except (ValueError, msgpack.exceptions.ExtraData, RecursionError):
            # F-78: RecursionError covers adversarially deep msgpack nesting
            # (msgpack builds containers recursively while unpacking); it is
            # not a ValueError, so one such payload used to escape this
            # handler and tear down the dispatch path.
            self._unknown_subs += 1
            if self._noise_log.allow():
                logger.warning(
                    "bad payload for flag %r (total=%d, suppressed=%d)",
                    flag,
                    self._unknown_subs,
                    self._noise_log.take_suppressed(),
                )
            return
        handler = self._handlers.get(sub)
        if handler is None:
            self._unknown_subs += 1
            if self._noise_log.allow():
                logger.warning(
                    "%s: unregistered sub-protocol %d (total=%d, suppressed=%d)",
                    type(self).__qualname__,
                    sub,
                    self._unknown_subs,
                    self._noise_log.take_suppressed(),
                )
            return
        if len(self._inbound) >= self._max_inflight:
            # Drop-and-count, mirroring the ZMQ destination overflow (F-24):
            # never block the read loop (see DEFAULT_MAX_INFLIGHT).
            self._overflowed += 1
            self._metrics.handler_overflow.inc()
            if self._overflow_log.allow():
                logger.warning(
                    "%s: handler concurrency cap reached (%d); dropped sub %d "
                    "(dropped total=%d, suppressed=%d)",
                    type(self).__qualname__,
                    self._max_inflight,
                    sub,
                    self._overflowed,
                    self._overflow_log.take_suppressed(),
                )
            # F-179: give the network a chance to signal its peer.
            hook = self._on_overflow
            if hook is not None:
                try:
                    hook(sub, self._overflowed)
                except Exception:
                    logger.exception("overflow hook failed for %s", type(self).__qualname__)
            return
        # F-224: per-CONNECTION fairness. The cap above is process-global:
        # one authenticated client could stack max_inflight frames of its own
        # and every other connection's dispatch would drop. The dispatch
        # contextvar names the client connection this frame arrived on (the
        # bus/RPC faces have none, so they are unaffected).
        source = current_connection()
        if source is not None and not source.admit_inflight():
            self._overflowed += 1
            self._metrics.handler_overflow.inc()
            if self._overflow_log.allow():
                logger.warning(
                    "%s: per-connection inflight cap (%d) reached for %r; dropped sub %d "
                    "(dropped total=%d, suppressed=%d)",
                    type(self).__qualname__,
                    source.max_inflight,
                    source,
                    sub,
                    self._overflowed,
                    self._overflow_log.take_suppressed(),
                )
            hook = self._on_overflow
            if hook is not None:
                try:
                    hook(sub, self._overflowed)
                except Exception:
                    logger.exception("overflow hook failed for %s", type(self).__qualname__)
            return
        task = asyncio.get_running_loop().create_task(self._run_handler(handler, sub, args))
        self._inbound.add(task)
        if source is not None:
            # Release the per-connection slot with the same done callback
            # that releases the set slot -- one exit path, both resources.
            def _done(t: asyncio.Task[None], conn: Connection = source) -> None:
                self._inbound_done(t)
                conn.release_inflight()

            task.add_done_callback(_done)
        else:
            task.add_done_callback(self._inbound_done)

    async def _run_handler(self, handler: Handler, sub: int, args: list[Any]) -> None:
        try:
            result = handler(*args)
            # isawaitable, not iscoroutine (F-162): a handler returning a
            # Task/Future/custom __await__ object used to have its result --
            # and exception -- silently dropped.
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception(
                "handler %r (sub=%d) failed", getattr(handler, "__qualname__", handler), sub
            )

    def _inbound_done(self, task: asyncio.Task[None]) -> None:
        self._inbound.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("inbound handler task failed: %r", task)

    def close(self) -> None:
        """Cancel still-running inbound handler tasks (shutdown path, F-20)."""
        for task in self._inbound:
            task.cancel()
        self._inbound.clear()
