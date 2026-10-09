"""Game clock: real time with an adjustable offset, plus game-calendar helpers.

Mirrors the prototype's debug-time facility (``SetTime``/``PushTime``) but as
an injectable service instead of module globals.

Numbering compatibility (F-25): day/week numbers are 1-based and month
numbers 0-based (2024-01 == 0), anchored at host-local midnight 2024-01-01 --
these values are persisted inside business data, so the bases are frozen.
``tz`` only pins the wall-clock derivations (hour, month label, boundaries);
with no tz the host's local time is used, exactly like the prototype.

Day/week numbers derive from the *calendar* day in the effective timezone
(F-44): the prototype's fixed 86,400-second grid rolls at a constant UTC
instant, which under a DST timezone drifts an hour off local midnight -- the
frozen calendar and ``NewDayEvent`` (fired at local midnight) then disagreed,
splitting one local day into two day numbers. Calendar derivation keeps the
frozen anchors (2024-01-01, a Monday) and only changes values across DST
edges, where the old grid was already inconsistent.
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
        self._offset = 0.0
        self._tz = zoneinfo.ZoneInfo(tz) if tz else None

    def local(self, ts: float) -> dt.datetime:
        """Wall-clock datetime for a logical timestamp, honoring the clock's
        pinned tz (host-local when unset). Boundary derivations must go
        through this so they can never disagree with ``next_halfhour_after``."""
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

    def _calendar_days_since_epoch(self, ts: float) -> int:
        """Whole calendar days between the epoch date and ``ts``'s local date.

        Both the anchor (2024-01-01, a Monday) and the 1-basing are frozen
        (F-25); deriving through :meth:`local` keeps the grid aligned with
        actual local midnights across DST transitions (F-44).
        """
        return (self.local(ts).date() - self._epoch.date()).days

    def day_no(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return self._calendar_days_since_epoch(ts) + 1

    def week_no(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return self._calendar_days_since_epoch(ts) // 7 + 1

    def month_no(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        local = self.local(ts)
        base = self._epoch
        return (local.year - base.year) * 12 + (local.month - base.month)

    def hour(self, timestamp: float | None = None) -> int:
        ts = self.now() if timestamp is None else timestamp
        return self.local(ts).hour

    def next_halfhour_after(self, ts: float) -> float:
        """Timestamp of the first :00/:30 boundary STRICTLY after ``ts``.

        DST-safe (F-89): wall-clock arithmetic on the local datetime walks
        across DST edges the wrong way. On a fall-back day ``01:30 + 1h``
        produces wall 02:00 resolved with the PRE-transition offset,
        skipping the repeated 01:00/01:30 boundaries that really exist in
        UTC; on a spring-forward day the nonexistent 02:00/02:30 land on the
        right instants only by luck of fold resolution. Instead: enumerate
        the wall grid around ``ts`` (both folds of ambiguous times), map
        every candidate back to real instants, and take the earliest one
        strictly after ``ts``. Boundary instants stay 30 real minutes apart
        across every transition, so the resulting sequence is monotonic
        with no skipped or duplicated boundaries.
        """
        base = self.local(ts).replace(minute=0, second=0, microsecond=0)
        best: float | None = None
        # +-hours of wall candidates: a DST shift moves the wall clock by at
        # most a couple of hours, so this window always contains the next
        # boundary even when the wall clock jumps in either direction.
        for minutes in range(-120, 241, 30):
            candidate = base + dt.timedelta(minutes=minutes)
            for fold in (0, 1):
                instant = candidate.replace(fold=fold).timestamp()
                if instant > ts and (best is None or instant < best):
                    best = instant
        assert best is not None, "wall window must contain the next boundary"
        return best

    def next_halfhour_boundary(self) -> tuple[float, int]:
        """Return ``(deadline, hour_at_deadline)`` for the next :00/:30 boundary."""
        deadline = self.next_halfhour_after(self.now())
        return deadline, self.local(deadline).hour
