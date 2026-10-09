"""pyline.api facade and tracked containers (M5)."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap

import pytest

from pyline import api
from pyline.core.clock import GameClock
from pyline.core.context import ServiceNotAvailableError, ServiceTypeError
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


class TestTypedServiceBagF87:
    async def test_returns_narrowed_instance(self, bound_ctx) -> None:
        clock = bound_ctx.service("clock", GameClock)
        assert isinstance(clock, GameClock)
        assert clock is bound_ctx.services["clock"]

    async def test_missing_service_names_the_service(self, bound_ctx) -> None:
        with pytest.raises(ServiceNotAvailableError, match="'nope'"):
            bound_ctx.service("nope", GameClock)

    async def test_wrong_type_names_both_types(self, bound_ctx) -> None:
        bound_ctx.services["clock"] = "not-a-clock"
        with pytest.raises(ServiceTypeError, match="GameClock"):
            bound_ctx.service("clock", GameClock)
        bound_ctx.services["clock"] = GameClock()  # restore for other tests

    async def test_clock_facade_uses_typed_lookup(self, bound_ctx) -> None:
        bound_ctx.services.pop("clock", None)
        with pytest.raises(ServiceNotAvailableError, match="clock"):
            api.clock.now()
        bound_ctx.services["clock"] = GameClock()
        assert api.clock.now() > 0


class TestLazyFacadeImportsF88:
    def test_importing_api_does_not_pull_db_or_net(self) -> None:
        """F-88: ``from pyline import api`` used to drag in every facade and
        with them asyncmy (api.db) and zmq (api.rpc); the surface must stay
        importable without the heavy chain."""
        code = textwrap.dedent(
            """
            import sys
            from pyline import api
            leaked = [m for m in sys.modules
                      if m.startswith(("pyline.db", "pyline.net", "asyncmy", "zmq"))]
            assert not leaked, leaked
            # the public surface is unchanged: attribute access still works
            assert hasattr(api.timer, "call")
            assert hasattr(api.env, "service_no")
            assert "db" in dir(api)
            assert not hasattr(api, "no_such_facade")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            timeout=60,
        )
        assert result.returncode == 0, result.stderr


class TestTaskFacadeF85:
    async def test_on_quit_without_lifecycle_keeps_strong_ref(self, bound_ctx) -> None:
        """F-85: with no lifecycle wired, on_quit's task had no reference and
        could be garbage-collected before running."""
        bound_ctx.lifecycle = None
        ran = asyncio.Event()

        async def work() -> None:
            await asyncio.sleep(0.02)
            ran.set()

        task = await api.task.on_quit(work())
        import gc

        gc.collect()  # would drop an unreferenced pending task
        await asyncio.wait_for(ran.wait(), 2.0)
        assert task in api.task._bg  # the strong reference held it


class TestEnvShutdownFallbackF86:
    async def test_shutdown_without_lifecycle_hard_exits(self, bound_ctx, monkeypatch) -> None:
        """F-86: the fallback used os.kill(pid, 15), which on Windows is
        TerminateProcess anyway (see supervisor._pid_alive) -- a hard kill
        with no exit-code control. The fallback must be explicit about it."""
        bound_ctx.lifecycle = None
        exits: list[int] = []
        monkeypatch.setattr(os, "_exit", lambda code: exits.append(code))
        api.env.shutdown("test-boom")
        assert exits == [1]

    async def test_shutdown_with_lifecycle_requests_graceful(self, bound_ctx) -> None:
        reasons: list[str] = []

        class FakeLifecycle:
            async def request_shutdown(self, reason: str) -> None:
                reasons.append(reason)

        bound_ctx.lifecycle = FakeLifecycle()  # type: ignore[assignment]
        try:
            api.env.shutdown("graceful-please")
            await asyncio.sleep(0)  # let the spawned task run
            assert reasons == ["graceful-please"]
        finally:
            bound_ctx.lifecycle = None


class TestDebugExceptionCycleF90c:
    def test_cyclic_cause_chain_terminates(self) -> None:
        from pyline.api import debug

        a = RuntimeError("a")
        b = ValueError("b")
        a.__cause__ = b
        b.__cause__ = a
        text = debug.format_exception(a)  # used to recurse infinitely
        assert "cycle" in text
        assert "RuntimeError" in text and "ValueError" in text

    def test_normal_chain_still_rendered(self) -> None:
        from pyline.api import debug

        try:
            try:
                raise ValueError("inner-f90c")
            except ValueError as exc:
                raise RuntimeError("outer-f90c") from exc
        except RuntimeError as outer:
            text = debug.format_exception(outer)
        assert "outer-f90c" in text
        assert "caused by" in text
        assert "inner-f90c" in text


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
