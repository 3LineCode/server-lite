"""Clock facade (framework-side subset of old com_time; the full business
helper set ships in template/game/com_time.py)."""

from __future__ import annotations

from pyline import api
from pyline.core.clock import GameClock


def _clock() -> GameClock:
    # F-87: typed bag access instead of an unchecked cast.
    return api.ctx().service("clock", GameClock)


def now() -> float:
    return _clock().now()


def now_int() -> int:
    return _clock().now_int()


def day_no(timestamp: float | None = None) -> int:
    """1-based day number from the frozen 2024-01-01 anchor (persisted data
    depends on this numbering)."""
    return _clock().day_no(timestamp)


def week_no(timestamp: float | None = None) -> int:
    return _clock().week_no(timestamp)


def month_no(timestamp: float | None = None) -> int:
    """0-based month number (2024-01 == 0), matching persisted data."""
    return _clock().month_no(timestamp)


def hour(timestamp: float | None = None) -> int:
    return _clock().hour(timestamp)


def set_debug_time(timestamp: float) -> None:
    """Old SetTime: pin the logical time (0 resets to real time)."""
    _clock().set_time(timestamp)


def push_debug_time(delta: float) -> None:
    """Old PushTime: advance the logical time by ``delta`` seconds."""
    _clock().push_time(delta)
