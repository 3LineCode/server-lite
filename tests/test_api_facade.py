"""pyline.api facade and tracked containers (M5)."""

from __future__ import annotations

import asyncio

import pytest

from pyline import api
from pyline.core.clock import GameClock
from pyline.core.scheduler import Scheduler
from pyline.db.tracked import TrackedDict, TrackedList
from pyline.runtime import build_context


@pytest.fixture()
async def bound_ctx(config_dir, tmp_path):
    ctx = build_context(config_dir, 10001, "main", 0, 0)
    ctx.scheduler = Scheduler(loop=asyncio.get_running_loop())
    ctx.services["clock"] = GameClock()
    api.bind(ctx)
    yield ctx
    api.unbind()


class TestFacade:
    async def test_env_and_registry(self, bound_ctx) -> None:
        assert api.env.service_no() == 10001
        assert api.env.server_no() == 10001
        assert api.env.is_main_process() is True
        assert api.env.is_develop() is True
        assert api.registry.server_list(exclude=[10009]) == [10001]
        assert api.registry.server_ip(10001) == "127.0.0.1"
        assert api.registry.proxy_list() == [10009]
        assert api.registry.is_proxy(10009) is True
        assert api.registry.server_by_ip("127.0.0.1") == 10001

    async def test_clock_helpers(self, bound_ctx) -> None:
        assert api.clock.day_no() >= 1
        assert api.clock.month_no() is not None
        before = api.clock.now()
        api.clock.push_debug_time(60)
        assert api.clock.now() >= before + 60
        api.clock.set_debug_time(0)  # reset

    async def test_timer_facade_semantics(self, bound_ctx) -> None:
        fired: list[int] = []
        api.timer.call("t", 0.05, fired.append, 1)
        api.timer.call("t", 0.05, fired.append, 2)  # same flag: replaces
        await asyncio.sleep(0.15)
        assert fired == [2]  # old timer cancelled, only the new one ran
        assert api.timer.pending_flags() == []

    async def test_timer_left_reports_remaining(self, bound_ctx) -> None:
        # regression: left() used to return a constant 0.0
        api.timer.call("later", 5.0, lambda: None)
        api.timer.call("soon", 0.05, lambda: None)
        remaining = api.timer.left("later")
        assert 4.0 < remaining <= 5.0
        assert 0.0 < api.timer.left("soon") <= 0.06  # 0.05 + float jitter
        assert api.timer.left("missing") == 0.0
        await asyncio.sleep(0.1)
        assert api.timer.left("soon") == 0.0  # fired: self-removed

    async def test_unbound_facade_fails_loudly(self) -> None:
        api.unbind()
        with pytest.raises(api.ApiUnboundError, match=r"api\.bind"):
            api.ctx()
        with pytest.raises(api.ApiUnboundError, match=r"api\.bind"):
            api.service("rpc")


class TestTrackedContainers:
    def test_tracked_dict_touches(self) -> None:
        touches = []
        data: TrackedDict[str, int] = TrackedDict(touch=lambda: touches.append(1))
        data["a"] = 1
        del data["a"]
        data.setdefault("b", 2)
        data.pop("b")
        assert len(touches) == 4

    def test_tracked_list_touches(self) -> None:
        touches = []
        items: TrackedList[int] = TrackedList([1], touch=lambda: touches.append(1))
        items.append(2)
        items.extend([3])
        items += [4]
        items[0] = 9
        items.remove(9)
        assert len(touches) == 5
        assert list(items) == [2, 3, 4]

    def test_tracked_dict_ior_touches(self) -> None:
        # regression: ``d |= {...}`` used to mutate via dict.__ior__ without touching
        touches = []
        data: TrackedDict[str, int] = TrackedDict({"a": 1}, touch=lambda: touches.append(1))
        data |= {"b": 2}
        assert dict(data) == {"a": 1, "b": 2}
        assert len(touches) == 1

    def test_tracked_list_imul_touches(self) -> None:
        # regression: ``items *= 2`` used to mutate via list.__imul__ without touching
        touches = []
        items: TrackedList[int] = TrackedList([1, 2], touch=lambda: touches.append(1))
        items *= 2
        assert list(items) == [1, 2, 1, 2]
        assert len(touches) == 1
