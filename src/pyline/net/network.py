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
import logging
from collections.abc import Callable
from typing import Any, ClassVar, cast

import msgpack

from pyline.net.gateway import ProtocolGateway

logger = logging.getLogger(__name__)

Handler = Callable[..., Any]


def pack_call(sub: int, *args: Any) -> bytes:
    """Build an outbound network payload."""
    return cast(bytes, msgpack.packb([sub, *args], use_bin_type=True))


def unpack_call(payload: bytes) -> tuple[int, list[Any]]:
    """Split an inbound payload into ``(sub, args)``."""
    data = msgpack.unpackb(payload, raw=False, strict_map_key=False)
    if not isinstance(data, list) or not data or not isinstance(data[0], int):
        raise ValueError("malformed network payload: expected [sub, *args]")
    return data[0], list(data[1:])


class Network:
    """Base class for protocol handlers registered on the gateway."""

    flag: ClassVar[str] = ""

    def __init__(self, gateway: ProtocolGateway) -> None:
        self._gateway = gateway
        self._handlers: dict[int, Handler] = {}
        self._unknown_subs = 0
        # F-20: strong references to in-flight handler tasks. A bare
        # ``create_task`` result can be garbage-collected mid-run (a known
        # CPython pitfall) and its exception is then never observed; the set
        # plus done callback mirrors scheduler.py's ``_async_tasks`` pattern.
        self._inbound: set[asyncio.Task[None]] = set()
        gateway.register(self)

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
            logger.warning("bad payload for flag %r (total=%d)", flag, self._unknown_subs)
            return
        handler = self._handlers.get(sub)
        if handler is None:
            self._unknown_subs += 1
            logger.warning(
                "%s: unregistered sub-protocol %d (total=%d)",
                type(self).__qualname__,
                sub,
                self._unknown_subs,
            )
            return
        task = asyncio.get_running_loop().create_task(self._run_handler(handler, sub, args))
        self._inbound.add(task)
        task.add_done_callback(self._inbound_done)

    async def _run_handler(self, handler: Handler, sub: int, args: list[Any]) -> None:
        try:
            result = handler(*args)
            if asyncio.iscoroutine(result):
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
