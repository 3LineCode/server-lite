"""Game clock: offset, day/week numbering, half-hour boundary."""

from __future__ import annotations

import datetime as dt
import zoneinfo

from pyline.core.clock import GameClock


def test_offset_push_and_set() -> None:
    clock = GameClock()
    base = clock.now()
    clock.push_time(100)
    assert abs(clock.now() - (base + 100)) < 0.01
    clock.set_time(0)
    assert abs(clock.now() - base) < 2.0  # reset to real time


def test_day_no_counts_from_epoch() -> None:
    clock = GameClock(epoch=dt.datetime(2024, 1, 1))
    # Epoch day 1 is 2024-01-01 itself.
    assert clock.day_no(dt.datetime(2024, 1, 2).timestamp()) == 2
    assert clock.week_no(dt.datetime(2024, 1, 8).timestamp()) == 2


def test_month_no() -> None:
    clock = GameClock(epoch=dt.datetime(2024, 1, 1))
    assert clock.month_no(dt.datetime(2024, 3, 1).timestamp()) == 2
    assert clock.month_no(dt.datetime(2025, 1, 1).timestamp()) == 12


def test_next_halfhour_boundary() -> None:
    clock = GameClock()
    deadline, hour = clock.next_halfhour_boundary()
    after = dt.datetime.fromtimestamp(deadline)
    assert after.minute in (0, 30)
    assert after.second == 0
    assert hour == after.hour
    assert deadline > clock.now()


class TestClockF25:
    def test_next_halfhour_after_strictly_increases(self) -> None:
        clock = GameClock()
        ts = clock.now()
        first = clock.next_halfhour_after(ts)
        assert first > ts
        assert clock.next_halfhour_after(first) > first

    def test_numbering_bases_frozen(self) -> None:
        """F-25 compat: day/week 1-based from 2024-01-01, month 0-based."""
        import time as _time

        clock = GameClock()
        epoch_ts = _time.mktime(__import__("datetime").datetime(2024, 1, 1).timetuple())
        assert clock.day_no(epoch_ts + 1) == 1
        assert clock.week_no(epoch_ts + 1) == 1
        assert clock.month_no(epoch_ts + 1) == 0
        assert clock.month_no(epoch_ts + 32 * 86400) == 1

    def test_tz_pinned_wall_clock(self) -> None:
        clock = GameClock(tz="Asia/Shanghai")
        # 2024-07-01 00:30 UTC == 08:30 in Shanghai
        assert clock.hour(1719793800) == 8


class TestDSTSafeCalendarF44:
    def test_day_no_rolls_exactly_at_local_midnight_across_dst(self) -> None:
        """F-44: with a DST tz, the old fixed-86400 grid drifted an hour off
        local midnight, splitting one local day into two day numbers and
        disagreeing with NewDayEvent (fired at local midnight)."""
        ny = zoneinfo.ZoneInfo("America/New_York")
        clock = GameClock(tz="America/New_York")
        # spring-forward day (23h), fall-back day (25h) and a year boundary
        for day in [(2024, 3, 10), (2024, 11, 3), (2024, 12, 31), (2025, 1, 1)]:
            midnight = dt.datetime(*day, tzinfo=ny).timestamp()
            just_before = midnight - 1
            assert clock.hour(just_before) == 23
            assert clock.hour(midnight) == 0
            assert clock.day_no(just_before) + 1 == clock.day_no(midnight), day
            assert clock.local(midnight).date() == dt.date(*day)

    def test_week_no_anchored_on_monday(self) -> None:
        clock = GameClock(tz="UTC")
        utc = zoneinfo.ZoneInfo("UTC")
        # epoch 2024-01-01 is a Monday: week 1 covers Jan 1-7, week 2 starts Jan 8
        assert clock.week_no(dt.datetime(2024, 1, 7, 23, 59, tzinfo=utc).timestamp()) == 1
        assert clock.week_no(dt.datetime(2024, 1, 8, 0, 0, tzinfo=utc).timestamp()) == 2

    def test_day_no_matches_local_calendar_date(self) -> None:
        clock = GameClock(tz="America/New_York")
        ny = zoneinfo.ZoneInfo("America/New_York")
        ts = dt.datetime(2024, 7, 4, 12, 0, tzinfo=ny).timestamp()
        expected = (dt.date(2024, 7, 4) - dt.date(2024, 1, 1)).days + 1
        assert clock.day_no(ts) == expected
