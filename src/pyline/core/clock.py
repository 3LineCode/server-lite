"""Game clock: real time with an adjustable offset, plus game-calendar helpers.

Mirrors the prototype's debug-time facility (``SetTime``/``PushTime``) but as
an injectable service instead of module globals. Day/week numbering counts
from a configurable epoch (default 2024-01-01 local time).
"""

from __future__ import annotations

import datetime as dt
import time


class GameClock:
    def __init__(self, *, epoch: dt.datetime | None = None) -> None:
        self._epoch = epoch or dt.datetime(2024, 1, 1)
        self._epoch_ts = int(time.mktime(self._epoch.timetuple()))
        self._offset = 0.0

    # ------------------------------ offset ------------------------------ #

    def set_time(self, timestamp: float) -> None:
        """Set the logical time; 0 resets to real time."""
        self._offset = 0.0 if timestamp == 0 else timestamp - time.time()

    def push_time(self, delta: float) -> None:
        self._offset += delta

    @property
    def offset(self) -> float:
        return self._offset

    # ------------------------------ reading ----------------------------- #

    def now(self) -> float:
        return time.time() + self._offset

    def now_int(self) -> int:
        return int(self.now())

    # --------------------------- game calendar -------------------------- #

    def day_no(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return int((ts - self._epoch_ts) // 86_400) + 1

    def week_no(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return int((ts - self._epoch_ts) // 604_800) + 1

    def month_no(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        local = dt.datetime.fromtimestamp(ts)
        base = self._epoch
        return (local.year - base.year) * 12 + (local.month - base.month)

    def hour(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return time.localtime(ts).tm_hour

    def next_halfhour_boundary(self) -> tuple[float, int]:
        """Return ``(deadline, hour_at_deadline)`` for the next :00/:30 boundary."""
        now = dt.datetime.fromtimestamp(self.now())
        minute = 30 if now.minute < 30 else 60
        nxt = now.replace(minute=0, second=0, microsecond=0) + dt.timedelta(minutes=minute)
        return nxt.timestamp(), nxt.hour
