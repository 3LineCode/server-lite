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


class TestClockCatchupCapF52:
    async def test_surplus_boundaries_skipped_with_alarm(self) -> None:
        """F-52: after >2 days of downtime the 96-boundary catch-up cap must
        skip the surplus LOUDLY -- a NewDayEvent that never fired silently
        breaks daily-reset logic."""
        import asyncio
        from pathlib import Path

        from pyline.core.events import EventBus, NewDayEvent
        from pyline.core.scheduler import Scheduler
        from pyline.obs.metrics import AlarmHub
        from pyline.runtime_wiring import ClockEventEmitter

        clock = GameClock(epoch=dt.datetime(2024, 1, 1), tz="UTC")
        scheduler = Scheduler()
        bus = EventBus()
        seen: list[object] = []
        bus.subscribe(NewDayEvent, lambda event: seen.append(event))
        alarms = AlarmHub()
        alarm_log: list[tuple[str, dict]] = []
        alarms.register_all(lambda kind, payload: alarm_log.append((kind, payload)))
        emitter = ClockEventEmitter(
            clock,
            scheduler,
            bus,
            log_dir=Path("."),
            spawn=lambda coro: asyncio.get_running_loop().create_task(coro),
            alarms=alarms,
        )
        now_ts = clock.now()
        # simulate three days of downtime: 144 missed half-hour boundaries
        emitter._last_boundary = now_ts - 3 * 86400
        emitter.emit_missed_boundaries()
        # 96 boundaries fired (the cap), the remaining 48 skipped with an alarm
        skipped = [p for k, p in alarm_log if k == "clock_boundaries_skipped"]
        assert skipped and skipped[0]["skipped"] == 48

    async def test_short_downtime_catches_up_without_alarm(self) -> None:
        import asyncio
        from pathlib import Path

        from pyline.core.events import EventBus, NewHourEvent
        from pyline.core.scheduler import Scheduler
        from pyline.obs.metrics import AlarmHub
        from pyline.runtime_wiring import ClockEventEmitter

        clock = GameClock(epoch=dt.datetime(2024, 1, 1), tz="UTC")
        bus = EventBus()
        seen: list[object] = []
        bus.subscribe(NewHourEvent, lambda event: seen.append(event))
        alarms = AlarmHub()
        alarm_log: list[tuple[str, dict]] = []
        alarms.register_all(lambda kind, payload: alarm_log.append((kind, payload)))
        emitter = ClockEventEmitter(
            clock,
            Scheduler(),
            bus,
            log_dir=Path("."),
            spawn=lambda coro: asyncio.get_running_loop().create_task(coro),
            alarms=alarms,
        )
        now_ts = clock.now()
        emitter._last_boundary = now_ts - 5400  # 1.5 hours: three boundaries
        emitter.emit_missed_boundaries()
        assert alarm_log == []  # within the cap: nothing skipped
        await asyncio.sleep(0.01)  # spawned emit tasks run
        assert len(seen) >= 1  # caught-up boundaries fired


class TestNextHalfHourDSTF89:
    """F-89: wall-clock arithmetic across DST edges skipped boundaries
    (fall-back) or landed on instants only by luck (spring-forward)."""

    def _walk(self, clock: GameClock, start: float, steps: int) -> list[float]:
        boundaries: list[float] = []
        ts = start
        for _ in range(steps):
            ts = clock.next_halfhour_after(ts)
            boundaries.append(ts)
        return boundaries

    def test_spring_forward_chain_is_exact(self) -> None:
        clock = GameClock(tz="America/New_York")
        utc = zoneinfo.ZoneInfo("UTC")
        start = dt.datetime(2024, 3, 10, 4, 0, tzinfo=utc).timestamp()  # local midnight
        boundaries = self._walk(clock, start, 20)  # covers the 02:00 jump
        self._assert_even_grid(clock, start, boundaries)

    def test_fall_back_chain_is_exact(self) -> None:
        clock = GameClock(tz="America/New_York")
        utc = zoneinfo.ZoneInfo("UTC")
        start = dt.datetime(2024, 11, 3, 4, 0, tzinfo=utc).timestamp()  # local midnight
        boundaries = self._walk(clock, start, 20)  # covers the repeated hour
        self._assert_even_grid(clock, start, boundaries)

    @staticmethod
    def _assert_even_grid(clock: GameClock, start: float, boundaries: list[float]) -> None:
        prev = start
        for b in boundaries:
            assert b > prev
            assert abs((b - prev) - 1800.0) < 1e-6  # no skipped/duplicated beat
            wall = clock.local(b)
            assert wall.second == 0
            assert wall.minute in (0, 30)
            prev = b

    def test_repeated_hour_boundaries_not_skipped(self) -> None:
        clock = GameClock(tz="America/New_York")
        ny = zoneinfo.ZoneInfo("America/New_York")
        # first 01:30 (EDT) on the fall-back day
        ts = dt.datetime(2024, 11, 3, 1, 30, fold=0, tzinfo=ny).timestamp()
        nxt = clock.next_halfhour_after(ts)
        # the next REAL boundary is the SECOND 01:00 (EST), 30 real minutes on;
        # the old wall-arithmetic produced 02:00 EST -- 90 minutes on, skipping
        # two boundaries that really exist
        assert abs((nxt - ts) - 1800.0) < 1e-6
        wall = clock.local(nxt)
        assert (wall.hour, wall.minute) == (1, 0)

    def test_nonexistent_hour_collapses_to_real_boundary(self) -> None:
        clock = GameClock(tz="America/New_York")
        ny = zoneinfo.ZoneInfo("America/New_York")
        ts = dt.datetime(2024, 3, 10, 1, 30, tzinfo=ny).timestamp()  # 01:30 EST
        nxt = clock.next_halfhour_after(ts)
        assert abs((nxt - ts) - 1800.0) < 1e-6
        wall = clock.local(nxt)
        # 02:00/02:30 do not exist that day; the boundary lands on 03:00 EDT
        assert (wall.hour, wall.minute) == (3, 0)

    def test_boundary_exactly_on_ts_advances(self) -> None:
        clock = GameClock(tz="America/New_York")
        ny = zoneinfo.ZoneInfo("America/New_York")
        ts = dt.datetime(2024, 6, 1, 12, 0, tzinfo=ny).timestamp()  # on a boundary
        nxt = clock.next_halfhour_after(ts)
        assert abs((nxt - ts) - 1800.0) < 1e-6
        assert clock.local(nxt).minute == 30


class TestAlarmHubF90b:
    def test_register_all_returns_unsubscribe_and_dedupes(self) -> None:
        from pyline.obs.metrics import AlarmHub

        hub = AlarmHub()
        seen: list[str] = []

        def catch_all(kind: str, payload: dict) -> None:
            seen.append(kind)

        unsub = hub.register_all(catch_all)
        hub.register_all(catch_all)  # duplicate registration: no double delivery
        hub.emit("kind", {})
        assert seen == ["kind"]
        unsub()
        hub.emit("other", {})
        assert seen == ["kind"]  # actually unsubscribed

    def test_kind_register_dedupes(self) -> None:
        from pyline.obs.metrics import AlarmHub

        hub = AlarmHub()
        seen: list[dict] = []

        def on_kind(payload: dict) -> None:
            seen.append(payload)

        hub.register("kind", on_kind)
        hub.register("kind", on_kind)
        hub.emit("kind", {"a": 1})
        assert seen == [{"a": 1}]
