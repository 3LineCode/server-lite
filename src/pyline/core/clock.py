"""Game clock: real time with an adjustable offset, plus game-calendar helpers.

Mirrors the prototype's debug-time facility (``SetTime``/``PushTime``) but as
an injectable service instead of module globals.

Numbering compatibility (F-25): day/week numbers are 1-based and month
numbers 0-based (2024-01 == 0), anchored at host-local midnight 2024-01-01 --
these values are persisted inside business data, so the bases are frozen.
``tz`` only pins the wall-clock derivations (hour, month label, boundaries);
with no tz the host's local time is used, exactly like the prototype.
"""

from __future__ import annotations

import datetime as dt
import time
import zoneinfo


class GameClock:
    def __init__(
        self,
        *,
        epoch: dt.datetime | None = None,
        tz: str | None = None,
    ) -> None:
        self._epoch = epoch or dt.datetime(2024, 1, 1)
        self._epoch_ts = int(time.mktime(self._epoch.timetuple()))
        self._offset = 0.0
        self._tz = zoneinfo.ZoneInfo(tz) if tz else None

    def _local(self, ts: float) -> dt.datetime:
        if self._tz is not None:
            return dt.datetime.fromtimestamp(ts, self._tz)
        return dt.datetime.fromtimestamp(ts)

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
        local = self._local(ts)
        base = self._epoch
        return (local.year - base.year) * 12 + (local.month - base.month)

    def hour(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return self._local(ts).hour

    def next_halfhour_after(self, ts: float) -> float:
        """Timestamp of the first :00/:30 boundary STRICTLY after ``ts``."""
        local = self._local(ts)
        minute = 30 if local.minute < 30 else 60
        nxt = local.replace(minute=0, second=0, microsecond=0) + dt.timedelta(minutes=minute)
        return nxt.timestamp()

    def next_halfhour_boundary(self) -> tuple[float, int]:
        """Return ``(deadline, hour_at_deadline)`` for the next :00/:30 boundary."""
        deadline = self.next_halfhour_after(self.now())
        return deadline, self._local(deadline).hour
