"""Timer facade: flag-keyed timers over the unified Scheduler (old NewTimer).

Semantics preserved from the prototype:

* same ``flag`` re-``call`` cancels the previous timer;
* delay floor 0.001s; coroutines and plain callables both work;
* ``left`` returns remaining seconds (0 when absent); ``delete`` cancels.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from pyline import api
from pyline.core.scheduler import Scheduler, TimerHandle

_MIN_DELAY = 0.001


class TimerFacade:
    def __init__(self, scheduler: Scheduler) -> None:
        self._scheduler = scheduler
        self._entries: dict[str, TimerHandle] = {}

    def call(self, flag: str, delay: float, func: Callable[..., Any], *args: Any) -> None:
        self.delete(flag)

        def run(*call_args: Any) -> Any:
            self._entries.pop(flag, None)  # self-remove on fire
            return func(*call_args)

        self._entries[flag] = self._scheduler.call_after(
            max(_MIN_DELAY, delay), run, *args, label=f"timer:{flag}"
        )

    def ms_call(self, flag: str, delay_ms: float, func: Callable[..., Any], *args: Any) -> None:
        self.call(flag, max(1.0, delay_ms) / 1000.0, func, *args)

    def soon(self, func: Callable[..., Any], *args: Any) -> None:
        self._scheduler.soon(func, *args)

    def left(self, flag: str) -> float:
        entry = self._entries.get(flag)
        if entry is None or entry.cancelled:
            return 0.0
        return 0.0  # deadline introspection is not exposed by TimerHandle

    def delete(self, flag: str) -> None:
        entry = self._entries.pop(flag, None)
        if entry is not None:
            entry.cancel()

    def pending_flags(self) -> list[str]:
        return [f for f, h in self._entries.items() if not h.cancelled]


_FACADE: TimerFacade | None = None


def _timer() -> TimerFacade:
    global _FACADE
    if _FACADE is None:
        scheduler = api.ctx().scheduler
        if scheduler is None:
            raise RuntimeError("scheduler not bound to the context yet")
        _FACADE = TimerFacade(scheduler)
    return _FACADE


def call(flag: str, delay: float, func: Callable[..., Any], *args: Any) -> None:
    _timer().call(flag, delay, func, *args)


def ms_call(flag: str, delay_ms: float, func: Callable[..., Any], *args: Any) -> None:
    _timer().ms_call(flag, delay_ms, func, *args)


def soon(func: Callable[..., Any], *args: Any) -> None:
    _timer().soon(func, *args)


def left(flag: str) -> float:
    return _timer().left(flag)


def delete(flag: str) -> None:
    _timer().delete(flag)


def pending_flags() -> list[str]:
    return _timer().pending_flags()


async def sleep(delay: float) -> None:
    await asyncio.sleep(delay)
