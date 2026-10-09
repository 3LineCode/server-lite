"""Typed event bus with layered subscribers.

Three layers mirror the prototype's three hook classes:

* ``LAYER_FRAMEWORK`` -- framework internals (``aioevents`` equivalent);
* ``LAYER_PUBLIC``   -- shared business library (``pubevents`` equivalent);
* ``LAYER_BUSINESS`` -- per-game script code (``events`` equivalent).

Forward events dispatch framework -> public -> business; quit-style events
dispatch in reverse (business first, so business code tears down before the
framework services it depends on). Handlers may be sync or async; each handler
is isolated (an exception is logged with the handler name and does not break
the remaining handlers).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)

LAYER_FRAMEWORK = 0
LAYER_PUBLIC = 1
LAYER_BUSINESS = 2

# F-202: bound on the MRO->subscription-keys cache (see _subscription_keys).
_MRO_CACHE_MAX = 4096
_VALID_LAYERS = (LAYER_FRAMEWORK, LAYER_PUBLIC, LAYER_BUSINESS)

EventHandler = Callable[[Any], Any]

# --------------------------------------------------------------------------- #
# Framework-defined event types
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class EnvReadyEvent:
    """Config + Context are built; fires before any boot step runs.

    The async replacement for the prototype's pre-loop ``OnEnvInit`` hook.
    """


@dataclass(slots=True)
class FrameInitEvent:
    """Event loop is up, before DB connections."""


@dataclass(slots=True)
class BaseInitEvent:
    """Database connected; base systems may load."""


@dataclass(slots=True)
class FuncInitEvent:
    """Business systems init."""


@dataclass(slots=True)
class FuncDoneEvent:
    """Startup finished; server enters steady state."""


@dataclass(slots=True)
class FuncQuitEvent:
    """Server is shutting down; release resources and flush state."""


@dataclass(slots=True)
class HalfHourEvent:
    hour: int


@dataclass(slots=True)
class NewHourEvent:
    hour: int


@dataclass(slots=True)
class NewDayEvent:
    """A new game day started (midnight boundary). ``day`` is the 1-based
    game day number (same frozen base as ``GameClock.day_no``), NOT the
    day-of-month -- the two coincide only within the first month of the
    2024 epoch and confusing them corrupts persisted day-keyed data."""

    day: int


@dataclass(slots=True)
class NewWeekEvent:
    """A new ISO week started (Monday). ``week_no`` is the 1-based game week
    number (the prototype passed the constant 1 as ``week_day``)."""

    week_no: int


@dataclass(slots=True)
class NewMonthEvent:
    month: int


@dataclass(slots=True)
class NewYearEvent:
    year: int


@dataclass(slots=True)
class PreReloadEvent:
    module: ModuleType


@dataclass(slots=True)
class OnReloadEvent:
    module: ModuleType


@dataclass(slots=True)
class ClientConnectedEvent:
    peer: tuple[str, int]
    # F-225: registry-assigned connection id (monotonic, never reused). The
    # default keeps handlers written against the pre-F-225 shape working.
    conn_id: int = 0


@dataclass(slots=True)
class ClientDisconnectedEvent:
    """F-225: a client connection's close hook fired (any reason).

    The counterpart of ClientConnectedEvent: without it a
    connection<->player map could only grow, and logout/kick cleanup had no
    framework hook to ride. ``reason`` is the Connection's close reason
    (idle timeout, send queue overflow, closed by peer, ...)."""

    peer: tuple[str, int]
    conn_id: int
    reason: str = ""


@dataclass(slots=True)
class ConsoleCommandEvent:
    command: str


@dataclass
class StartupContextEvent:
    """Base class for events carrying the runtime context."""

    context: Any = field(default=None, repr=False)


class EventBus:
    """In-process, single-loop event bus.

    Subscribing a base class receives its subclasses too (F-24: subscription
    matching walks ``type(event).__mro__``); that is what makes
    ``StartupContextEvent``-style base subscriptions usable.
    """

    def __init__(self) -> None:
        # (event_type, layer) -> [handlers]
        self._handlers: dict[tuple[type, int], list[EventHandler]] = {}
        self._failure_counts: dict[str, int] = {}
        # full-MRO tuple -> subscription keys. Keyed by the MRO, not the
        # type: re-pointing a class's __bases__ (rejected by the reload
        # validator, but the bus cannot assume that) changes the answer
        # while the type object stays identical -- an MRO-keyed cache
        # recomputes instead of serving stale keys.
        self._mro_cache: dict[tuple[type, ...], tuple[tuple[type, int], ...]] = {}

    def subscribe(
        self,
        event_type: type,
        handler: EventHandler,
        *,
        layer: int = LAYER_BUSINESS,
    ) -> Callable[[], None]:
        """Register ``handler`` for ``event_type``; returns an unsubscribe fn.

        Re-subscribing the same handler is a no-op: a double registration
        (e.g. a module re-running its register() hook) must not double-deliver."""
        if layer not in _VALID_LAYERS:
            raise ValueError(f"invalid layer {layer!r}")
        handlers = self._handlers.setdefault((event_type, layer), [])
        if handler not in handlers:
            handlers.append(handler)
            self._mro_cache.clear()
        return lambda: self._unsubscribe(event_type, layer, handler)

    def _unsubscribe(self, event_type: type, layer: int, handler: EventHandler) -> None:
        handlers = self._handlers.get((event_type, layer))
        if handlers and handler in handlers:
            handlers.remove(handler)
            self._mro_cache.clear()

    def _subscription_keys(self, event_type: type) -> tuple[tuple[type, int], ...]:
        """Subscribed keys whose type is event_type or a base of it."""
        mro = tuple(event_type.__mro__)
        cached = self._mro_cache.get(mro)
        if cached is not None:
            return cached
        keys = [
            (klass, layer)
            for klass in event_type.__mro__
            if klass is not object
            for layer in _VALID_LAYERS
            if (klass, layer) in self._handlers
        ]
        result = tuple(keys)
        # F-202: every DISTINCT event type adds one permanent entry. A busy
        # bus with short-lived dynamically created event classes (reload
        # re-execution included) used to grow this map without bound; a
        # simple size cap trades a rare recompute for bounded memory.
        if len(self._mro_cache) >= _MRO_CACHE_MAX:
            self._mro_cache.clear()
        self._mro_cache[mro] = result
        return result

    async def emit(self, event: Any, *, reverse: bool = False) -> None:
        """Dispatch ``event`` to all layers, in layer order (or reverse).

        Serial dispatch is the CONTRACT, not an implementation detail
        (F-90a, documented decision -- no behavior change): handlers run
        strictly one after another, framework -> public -> business (or the
        reverse for quit-style events), and ``await`` inside a handler
        delays every later handler on the chain. This ordering guarantee is
        load-bearing for quit events: business teardown must finish before
        the framework services it calls into are closed, which rules out
        concurrent fan-out on this bus. Consequence: a slow handler stalls
        the whole dispatch -- heavy work belongs in a task
        (``loop.create_task`` / ``api.task.spawn``) scheduled from the
        handler, not awaited inline. A timeout here would be wrong: cutting
        a quit handler off mid-flush loses data, and there is no safe
        cutoff the bus could pick for arbitrary business code.
        """
        layers = reversed(_VALID_LAYERS) if reverse else iter(_VALID_LAYERS)
        keys = self._subscription_keys(type(event))
        for layer in layers:
            for key in keys:
                if key[1] != layer:
                    continue
                for handler in list(self._handlers.get(key, [])):
                    await self._invoke(event, handler)

    async def _invoke(self, event: Any, handler: EventHandler) -> None:
        name = getattr(handler, "__qualname__", repr(handler))
        try:
            result = handler(event)
            # isawaitable, not iscoroutine (F-162): a handler returning a
            # Task/Future/custom __await__ object used to have its result --
            # and exception -- silently dropped.
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            # F-150: a handler that raises CancelledError on its own (a
            # cancelled inner task awaited bare, a mis-coded timeout) used to
            # escape the Exception clause below and truncate the dispatch --
            # on a reverse-dispatch quit chain one such handler silently
            # skipped the teardown of every handler behind it. Only a
            # cancellation of THIS task (the await above was interrupted
            # from outside) propagates; otherwise the handler is isolated
            # like any other failure.
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise
            self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
            logger.exception(
                "event handler %s raised CancelledError (event=%s)", name, type(event).__name__
            )
        except Exception:
            self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
            logger.exception("event handler %s failed (event=%s)", name, type(event).__name__)

    def handler_failures(self) -> dict[str, int]:
        """Per-handler failure counts, for metrics/monitoring."""
        return dict(self._failure_counts)

    def clear(self) -> None:
        self._handlers.clear()
        self._mro_cache.clear()
