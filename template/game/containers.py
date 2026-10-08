"""Business data containers -- port of the prototype's pubcom/com_db.py.

* ``TimeData``/``DayData``/``WeekData``: current/previous-period pairs that
  roll over automatically (``d_Time``/``d_Data``/``d_Last``).
* ``DataOP``: the day/week/timed/permanent/temporary KV families, one layer
  of history for day/week data.
* ``SaverDataOP``: mixin combining DataOP with an ORM-tracked model.

Serialization keeps the prototype's ``{"T": .., "D": .., "L": ..}`` /
``{"d__Last": ..}`` shapes so existing persisted blobs stay readable. The
prototype's TimeUpset key/duration confusion is fixed (duration now keyed
by the key, not compared against a duration value).
"""

from __future__ import annotations

from typing import Any

from pyline.api import clock

from .com_time import GetDayNo, GetTime, GetWeekNo


class TimeData:
    """Base period container: current data + previous data, rolled on tick."""

    def __init__(self) -> None:
        self.d_Time: int = self.GetTimeNo()
        self.d_Data: dict[str, Any] = {}
        self.d_Last: dict[str, Any] = {}

    def GetTimeNo(self) -> int:
        return 0  # subclasses pick the period (day/week)

    def TryNewTime(self) -> None:
        now_no = self.GetTimeNo()
        if now_no != self.d_Time:
            self.d_Last = self.d_Data
            self.d_Data = {}
            self.d_Time = now_no

    # current-period ops
    def Set(self, key: str, value: Any) -> None:
        self.TryNewTime()
        self.d_Data[key] = value

    def Get(self, key: str, default: Any = 0) -> Any:
        self.TryNewTime()
        return self.d_Data.get(key, default)

    def Add(self, key: str, value: Any = 1) -> None:
        self.TryNewTime()
        self.d_Data[key] = self.d_Data.get(key, 0) + value

    def Del(self, key: str) -> None:
        self.TryNewTime()
        self.d_Data.pop(key, None)

    # previous-period ops
    def LastSet(self, key: str, value: Any) -> None:
        self.d_Last[key] = value

    def LastGet(self, key: str, default: Any = 0) -> Any:
        return self.d_Last.get(key, default)

    def LastAdd(self, key: str, value: Any = 1) -> None:
        self.d_Last[key] = self.d_Last.get(key, 0) + value

    def LastDel(self, key: str) -> None:
        self.d_Last.pop(key, None)

    def Save(self) -> dict[str, Any]:
        return {"T": self.d_Time, "D": self.d_Data, "L": self.d_Last}

    def Load(self, data: dict[str, Any] | None) -> None:
        if not data:
            return
        self.d_Time = data.get("T", self.d_Time)
        self.d_Data = data.get("D", {})
        self.d_Last = data.get("L", {})


class DayData(TimeData):
    def GetTimeNo(self) -> int:
        return GetDayNo()


class WeekData(TimeData):
    def GetTimeNo(self) -> int:
        return GetWeekNo()


class DataOP:
    """Day/week/timed/permanent/temporary KV families with one-layer history.

    Combine with an ORM model: ``class PlayerData(DataOP, TrackableModel)``;
    call ``touch()`` (or mutate through tracked containers) to mark dirty.
    """

    def _d_op_init(self) -> None:
        self.d_WeekNo: int = GetWeekNo()
        self.d_DayNo: int = GetDayNo()
        self.d_Today: dict[str, Any] = {}
        self.d_Week: dict[str, Any] = {}
        self.d_LastToday: dict[str, Any] = {}
        self.d_LastWeek: dict[str, Any] = {}
        self.d_Time: dict[str, dict[str, Any]] = {}
        self.d_Data: dict[str, Any] = {}
        self.m_Temp: dict[str, Any] = {}

    # ------------------------------- day ------------------------------- #

    def try_newday(self) -> None:
        day_no = GetDayNo()
        if day_no != self.d_DayNo:
            self.d_LastToday = {"d__Last": self.d_Today}  # one layer of history
            self.d_Today = {}
            self.d_DayNo = day_no

    def DaySet(self, key: str, value: Any) -> None:
        self.try_newday()
        self.d_Today[key] = value

    def DayGet(self, key: str, default: Any = 0) -> Any:
        self.try_newday()
        return self.d_Today.get(key, default)

    def DayAdd(self, key: str, value: Any = 1) -> None:
        self.try_newday()
        self.d_Today[key] = self.d_Today.get(key, 0) + value

    def DayDel(self, key: str) -> None:
        self.try_newday()
        self.d_Today.pop(key, None)

    def LastDayGet(self, key: str, default: Any = 0) -> Any:
        return self.d_LastToday.get("d__Last", {}).get(key, default)

    # ------------------------------- week ------------------------------- #

    def try_newweek(self) -> None:
        week_no = GetWeekNo()
        if week_no != self.d_WeekNo:
            self.d_LastWeek = {"d__Last": self.d_Week}
            self.d_Week = {}
            self.d_WeekNo = week_no

    def WeekSet(self, key: str, value: Any) -> None:
        self.try_newweek()
        self.d_Week[key] = value

    def WeekGet(self, key: str, default: Any = 0) -> Any:
        self.try_newweek()
        return self.d_Week.get(key, default)

    def WeekAdd(self, key: str, value: Any = 1) -> None:
        self.try_newweek()
        self.d_Week[key] = self.d_Week.get(key, 0) + value

    def WeekDel(self, key: str) -> None:
        self.try_newweek()
        self.d_Week.pop(key, None)

    def LastWeekGet(self, key: str, default: Any = 0) -> Any:
        return self.d_LastWeek.get("d__Last", {}).get(key, default)

    # ------------------------------- timed ------------------------------- #

    def try_newtime(self) -> None:
        now = GetTime()
        expired = [k for k, v in self.d_Time.items() if v.get("t", 0) <= now]
        for key in expired:
            self.d_Time.pop(key, None)

    def TimeSet(self, key: str, value: Any, duration: int) -> None:
        self.d_Time[key] = {"t": GetTime() + duration, "v": value}

    def TimeGet(self, key: str, default: Any = 0) -> Any:
        self.try_newtime()
        entry = self.d_Time.get(key)
        return entry["v"] if entry is not None else default

    def TimeUpset(self, key: str, value: Any, duration: int) -> None:
        """Update an unexpired entry in place, else create a new one."""
        self.try_newtime()
        entry = self.d_Time.get(key)
        if entry is not None:
            entry["v"] = value
        else:
            self.TimeSet(key, value, duration)

    def TimeDel(self, key: str) -> None:
        self.d_Time.pop(key, None)

    def TimeLeft(self, key: str) -> int:
        self.try_newtime()
        entry = self.d_Time.get(key)
        return max(0, entry["t"] - GetTime()) if entry is not None else 0

    # ------------------------------ permanent ------------------------------ #

    def Set(self, key: str, value: Any) -> None:
        self.d_Data[key] = value

    def Get(self, key: str, default: Any = 0) -> Any:
        return self.d_Data.get(key, default)

    def Add(self, key: str, value: Any = 1) -> None:
        self.d_Data[key] = self.d_Data.get(key, 0) + value

    def Del(self, key: str) -> None:
        self.d_Data.pop(key, None)

    # ------------------------------ temporary ------------------------------ #

    def TempSet(self, key: str, value: Any) -> None:
        self.m_Temp[key] = value

    def TempGet(self, key: str, default: Any = 0) -> Any:
        return self.m_Temp.get(key, default)

    def TempAdd(self, key: str, value: Any = 1) -> None:
        self.m_Temp[key] = self.m_Temp.get(key, 0) + value

    def TempDel(self, key: str) -> None:
        self.m_Temp.pop(key, None)

    # ------------------------------ persistence ------------------------------ #

    def DataOPSave(self) -> dict[str, Any]:
        """Everything persisted: temporary data (m_Temp) is excluded."""
        return {
            "today": self.d_Today,
            "lastToday": self.d_LastToday,
            "week": self.d_Week,
            "lastWeek": self.d_LastWeek,
            "time": self.d_Time,
            "data": self.d_Data,
        }

    def DataOPLoad(self, data: dict[str, Any] | None) -> None:
        if not data:
            self._d_op_init()
            return
        self.d_Today = data.get("today", {})
        self.d_LastToday = data.get("lastToday", {})
        self.d_Week = data.get("week", {})
        self.d_LastWeek = data.get("lastWeek", {})
        self.d_Time = data.get("time", {})
        self.d_Data = data.get("data", {})
