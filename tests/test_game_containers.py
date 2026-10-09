"""Template business layer: com_time + containers (M5, old-repo parity)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pyline import api
from pyline.core.clock import GameClock
from pyline.runtime import build_context

TEMPLATE = Path(__file__).parent.parent / "template"


@pytest.fixture()
async def game_layer(config_dir, tmp_path):
    ctx = build_context(config_dir, 10001, "main", 0, 0)
    ctx.services["clock"] = GameClock()
    api.bind(ctx)
    api.unbind()
    api.bind(ctx)
    sys.path.insert(0, str(TEMPLATE))
    sys.modules.pop("game", None)
    sys.modules.pop("game.containers", None)
    sys.modules.pop("game.com_time", None)
    import game.com_time as com_time
    import game.containers as containers

    yield com_time, containers
    sys.path.remove(str(TEMPLATE))
    for name in ("game", "game.containers", "game.com_time"):
        sys.modules.pop(name, None)
    api.unbind()


class TestComTime:
    def test_numbering_bases_match_old_repo(self, game_layer) -> None:
        com_time, _ = game_layer
        # day/week 1-based from the 2024 anchor; month 0-based (compat!)
        assert com_time.GetDayNo(com_time.MakeTime(2024, 1, 1, 12)) == 1
        assert com_time.GetWeekNo(com_time.MakeTime(2024, 1, 1, 12)) == 1
        assert com_time.GetMonthNo(com_time.MakeTime(2024, 1, 15)) == 0
        assert com_time.GetMonthNo(com_time.MakeTime(2024, 2, 1)) == 1
        assert com_time.GetMonthNo(com_time.MakeTime(2025, 1, 1)) == 12

    def test_weekday_and_formats(self, game_layer) -> None:
        com_time, _ = game_layer
        ts = com_time.MakeTime(2024, 1, 1)  # a Monday
        assert com_time.GetWeekDay(ts) == 1
        assert com_time.GetWeekDay(ts, iWeekStart=0) == 0
        assert com_time.TimeString(com_time.TIME_DAY + 3661) == "1天1小时1分1秒"
        assert "2024-01-01" in com_time.TimeFormat(ts)

    def test_timeformat_follows_clock_tz(self, game_layer) -> None:
        """TimeFormat/TimeFormatCN used datetime.fromtimestamp (HOST zone),
        contradicting the module's own "all wall-clock derivations go through
        the game clock" contract: with a pinned clock.tz the rendered date
        disagreed with the clock's day/week numbers."""
        com_time, _ = game_layer
        api.ctx().services["clock"] = GameClock(tz="UTC")
        ts = 1704069000  # 2024-01-01 00:30:00 UTC
        assert com_time.TimeFormat(ts) == "2024-01-01 00:30:00"
        assert "2024年01月01日" in com_time.TimeFormatCN(ts)
        assert com_time.GetDayNo(ts) == 1  # the rendering and the number agree

    def test_debug_time_offset(self, game_layer) -> None:
        com_time, _ = game_layer
        real = com_time.TrueTime()
        com_time.PushTime(120)
        assert com_time.GetTime() >= real + 119
        com_time.SetTime(0)  # reset


class TestDataOP:
    def test_day_rollover_keeps_one_layer_of_history(self, game_layer) -> None:
        _com_time, containers = game_layer

        class Op(containers.DataOP):
            def __init__(self) -> None:
                self._d_op_init()

        op = Op()
        op.DaySet("login", 3)
        op.DaySet("kills", 9)
        op.d_DayNo -= 1  # force a rollover on next access
        op.DayGet("x")
        assert op.LastDayGet("kills") == 9
        assert op.DayGet("login") == 0

    def test_timed_entries_expire(self, game_layer) -> None:
        _com_time, containers = game_layer

        class Op(containers.DataOP):
            def __init__(self) -> None:
                self._d_op_init()

        op = Op()
        op.TimeSet("buff", 5, duration=0)  # already expired
        assert op.TimeGet("buff", default="gone") == "gone"
        assert op.TimeLeft("buff") == 0
        op.TimeSet("shield", 1, duration=3600)
        assert 3500 < op.TimeLeft("shield") <= 3600

    def test_time_upset_updates_only_live_keys(self, game_layer) -> None:
        """The prototype compared the DURATION against keys (bug); the port
        updates the entry only when the KEY is alive."""
        _com_time, containers = game_layer

        class Op(containers.DataOP):
            def __init__(self) -> None:
                self._d_op_init()

        op = Op()
        op.TimeUpset("a", 1, duration=3600)
        op.TimeUpset("a", 2, duration=9999)
        assert op.TimeGet("a") == 2
        assert 3500 < op.TimeLeft("a") <= 3600  # expiry NOT refreshed by upset

    def test_save_load_roundtrip_excludes_temp(self, game_layer) -> None:
        _com_time, containers = game_layer

        class Op(containers.DataOP):
            def __init__(self) -> None:
                self._d_op_init()

        op = Op()
        op.Set("perm", 1)
        op.TempSet("tmp", 2)
        saved = op.DataOPSave()
        assert "tmp" not in str(saved)
        fresh = Op()
        fresh.DataOPLoad(saved)
        assert fresh.Get("perm") == 1

    def test_time_data_period_pairs(self, game_layer) -> None:
        _com_time, containers = game_layer
        day = containers.DayData()
        day.Set("v", 1)
        day.d_Time -= 1  # force rollover
        assert day.Get("v", default=-1) == -1
        assert day.LastGet("v") == 1
        assert day.Save() == {"T": day.d_Time, "D": {}, "L": {"v": 1}}
