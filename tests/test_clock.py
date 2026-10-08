"""Game clock: offset, day/week numbering, half-hour boundary."""

from __future__ import annotations

import datetime as dt

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
