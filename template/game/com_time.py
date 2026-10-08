"""Business time helpers -- faithful port of the prototype's pubcom/com_time.

Numbering compatibility is load-bearing: GetDayNo/GetWeekNo are 1-based and
GetMonthNo is 0-based (2024-01 == 0) from the 2024-01-01 anchor, exactly like
the old repo -- these values are persisted inside player data. Do not change.
"""

from __future__ import annotations

import time
from datetime import datetime

from pyline.api import clock

# The prototype's STANDARD_TIME anchor (2024, 1, 1, 0).
STANDARD_TIME = (2024, 1, 1, 0)

TIME_MINUTE = 60
TIME_HOUR = 3600
TIME_DAY = 86400
TIME_WEEK = 604800

__all__ = [
    "STANDARD_TIME",
    "TIME_DAY",
    "TIME_HOUR",
    "TIME_MINUTE",
    "TIME_WEEK",
    "GetDayNo",
    "GetDayHour",
    "GetMonthDay",
    "GetMonthNo",
    "GetMonthNum",
    "GetTime",
    "GetWeekDay",
    "GetWeekNo",
    "GetYearNum",
    "MakeTime",
    "PushTime",
    "SetTime",
    "TimeString",
    "TimeFormat",
    "TimeFormatCN",
    "TrueTime",
]


def TrueTime() -> int:
    """Real unix seconds -- no debug offset."""
    return int(time.time())


def GetTime() -> int:
    """Logical unix seconds (honours the debug offset). Use this everywhere."""
    return clock.now_int()


def TimeFormat(iTime: int = 0) -> str:
    return datetime.fromtimestamp(iTime or GetTime()).strftime("%Y-%m-%d %H:%M:%S")


def TimeFormatCN(iTime: int = 0) -> str:
    return datetime.fromtimestamp(iTime or GetTime()).strftime("%Y年%m月%d日 %H时%M分%S秒")


def TimeString(iTime: int) -> str:
    """Seconds -> 'x天x小时x分x秒' (empty string for 0)."""
    if iTime <= 0:
        return ""
    days, rem = divmod(iTime, TIME_DAY)
    hours, rem = divmod(rem, TIME_HOUR)
    minutes, seconds = divmod(rem, TIME_MINUTE)
    return f"{days}天{hours}小时{minutes}分{seconds}秒"


def GetDayNo(iTime: int = 0) -> int:
    return clock.day_no(iTime or GetTime())


def GetWeekNo(iTime: int = 0) -> int:
    return clock.week_no(iTime or GetTime())


def GetMonthNo(iTime: int = 0) -> int:
    return clock.month_no(iTime or GetTime())


def GetYearNum(iTime: int = 0) -> int:
    return time.localtime(iTime or GetTime()).tm_year


def GetMonthNum(iTime: int = 0) -> int:
    return time.localtime(iTime or GetTime()).tm_mon


def GetDayHour(iTime: int = 0) -> int:
    return time.localtime(iTime or GetTime()).tm_hour


def GetWeekDay(iTime: int = 0, iWeekStart: int = 1) -> int:
    """1..7 with a configurable first day (Monday==1 by default)."""
    return time.localtime(iTime or GetTime()).tm_wday + iWeekStart


def GetMonthDay(iTime: int = 0) -> int:
    return time.localtime(iTime or GetTime()).tm_mday


def MakeTime(
    iYear: int,
    iMonth: int = 1,
    iDay: int = 1,
    iHour: int = 0,
    iMinute: int = 0,
    iSecond: int = 0,
) -> int:
    return int(time.mktime((iYear, iMonth, iDay, iHour, iMinute, iSecond, 0, 0, -1)))


def PushTime(iTime: int) -> None:
    clock.push_debug_time(iTime)


def SetTime(iTime: int) -> None:
    """Pin the debug time (clamped to >= the 2024 anchor); 0 resets."""
    base = MakeTime(*STANDARD_TIME[:3])
    if iTime and iTime < base:
        iTime = base
    clock.set_debug_time(iTime)
