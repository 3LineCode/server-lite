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
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)

LAYER_FRAMEWORK = 0
LAYER_PUBLIC = 1
LAYER_BUSINESS = 2
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
        self._mro_cache: dict[type, tuple[tuple[type, int], ...]] = {}

    def subscribe(
        self,
        event_type: type,
        handler: EventHandler,
        *,
        layer: int = LAYER_BUSINESS,
    ) -> Callable[[], None]:
        """Register ``handler`` for ``event_type``; returns an unsubscribe fn."""
        if layer not in _VALID_LAYERS:
            raise ValueError(f"invalid layer {layer!r}")
        self._handlers.setdefault((event_type, layer), []).append(handler)
        self._mro_cache.clear()
        return lambda: self._unsubscribe(event_type, layer, handler)

    def _unsubscribe(self, event_type: type, layer: int, handler: EventHandler) -> None:
        handlers = self._handlers.get((event_type, layer))
        if handlers and handler in handlers:
            handlers.remove(handler)
            self._mro_cache.clear()

    def _subscription_keys(self, event_type: type) -> tuple[tuple[type, int], ...]:
        """Subscribed keys whose type is event_type or a base of it."""
        cached = self._mro_cache.get(event_type)
        if cached is not None:
            return cached
        keys = [
            (klass, layer)
            for klass in event_type.__mro__
            if klass is not object
            for layer in _VALID_LAYERS
            if (klass, layer) in self._handlers
        ]
        self._mro_cache[event_type] = tuple(keys)
        return self._mro_cache[event_type]

    async def emit(self, event: Any, *, reverse: bool = False) -> None:
        """Dispatch ``event`` to all layers, in layer order (or reverse)."""
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
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
            logger.exception("event handler %s failed (event=%s)", name, type(event).__name__)

    def handler_failures(self) -> dict[str, int]:
        """Per-handler failure counts, for metrics/monitoring."""
        return dict(self._failure_counts)

    def clear(self) -> None:
        self._handlers.clear()
        self._mro_cache.clear()


def describe_event_file(path: Path) -> str:
    """Human-readable helper used in diagnostics."""
    return str(path)
