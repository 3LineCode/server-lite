"""Windowed log-rate limiting for peer-triggerable log lines.

A hostile or misbehaving peer can make the framework *want* to log one line
per event (unauthenticated bus frames, unknown protocol flags, refused
handshakes).  Those lines are evidence on the first occurrence and a
log-flood denial of service at line 10,000 -- the same windowed-admission
idea ``net/connection.py`` applies per connection to dispatch errors, as a
shareable helper for module-level (multi-connection) noise sources.
"""

from __future__ import annotations

import time
from collections import deque


class WindowLogLimiter:
    """Sliding-window admission: at most ``limit`` admissions per ``window``.

    Usage::

        if limiter.allow():
            logger.warning("... (suppressed=%d)", limiter.take_suppressed())

    ``take_suppressed()`` reports how many calls were refused since the last
    admitted line, so the flood stays visible as a count instead of N lines.
    """

    def __init__(self, *, limit: int = 5, window: float = 10.0) -> None:
        self._limit = limit
        self._window = window
        self._times: deque[float] = deque()
        self._suppressed = 0

    def allow(self) -> bool:
        now = time.monotonic()
        while self._times and now - self._times[0] > self._window:
            self._times.popleft()
        if len(self._times) >= self._limit:
            self._suppressed += 1
            return False
        self._times.append(now)
        return True

    def take_suppressed(self) -> int:
        """Suppressed calls since the last admitted line (resets on read)."""
        count = self._suppressed
        self._suppressed = 0
        return count
