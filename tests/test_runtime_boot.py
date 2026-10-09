"""Full-runtime boot/teardown wiring: F-92 (db-less boot), F-58 (shutdown
settle end-to-end), F-93 (teardown ordering), F-94 (early service
registration), F-95 (per-step teardown budget), F-91 (PreReload before the
swap), F-96 (watcher roots), F-97 (reload surfaces vs BaseException),
F-98 (SetTime setback), F-100 (clock tz wiring)."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import sys
from pathlib import Path

import pytest

from pyline import api as pyline_api
from pyline.config.errors import ConfigError
from pyline.core.events import (
    EventBus,
    FuncQuitEvent,
    OnReloadEvent,
    PreReloadEvent,
)
from pyline.core.lifecycle import LifecycleState
from pyline.core.scheduler import Scheduler
from pyline.devtools.console import Console
from pyline.devtools.watcher import FileWatcher
from pyline.runtime import ServerRuntime, _settle_shutdown, build_context
from pyline.runtime_wiring import ClockEventEmitter, TeardownPlan

# --------------------------------------------------------------------------- #
# Config / business-module helpers
# --------------------------------------------------------------------------- #

BOOT_SERVER_NO = 10002


def _write_config(
    config_dir: Path,
    *,
    srv_type: str = "production",
    clock_tz: str = "",
    metrics_port: int | None = None,
) -> None:
    """(Re)write the fixture's config with a db-less gateway server added."""
    project = {
        "project": "boot-test",
        "srv_type": srv_type,
        # null keeps the boot test free of the shared default metrics port
        "metrics_port": metrics_port,
        "zeromq": {"bind_host": "tcp://127.0.0.1:29417"},
        "socket": {"token": "$plain:unit-test-token", "client_port": 11520, "server_port": 12520},
        "mysql": {"user": "root", "password": "$plain:test", "db_name": "pyline_test"},
        "redis": {"password": "$plain:test"},
        "clock": {"tz": clock_tz},
    }
    (config_dir / "project.json5").write_text(json.dumps(project), encoding="utf-8")
    servers = {
        "normal": {"sub_process": [], "use_mysql": True, "use_redis": True},
        "10001": {
            "base": "normal",
            "name": "dev",
            "advertise_ip": "127.0.0.1",
            "client_port": 1520,
            "server_port": 2520,
        },
        # F-92: a pure gateway entry -- no mysql, no redis, no db sub-process.
        # advertise_ip must differ from 10001 (registry uniqueness); bind_ip
        # stays on loopback.
        str(BOOT_SERVER_NO): {
            "base": "normal",
            "name": "gateway",
            "advertise_ip": "127.0.0.2",
            "bind_ip": "127.0.0.1",
            "use_mysql": False,
            "use_redis": False,
            "client_port": 25331,
            "server_port": 25332,
        },
    }
    (config_dir / "servers.json5").write_text(json.dumps(servers), encoding="utf-8")


def _write_business(business_dir: Path) -> None:
    """A minimal bootable business package (the PYLINE_EVENTS target)."""
    (business_dir / "bootgame").mkdir(parents=True, exist_ok=True)
    (business_dir / "bootgame" / "__init__.py").write_text("", encoding="utf-8")
    (business_dir / "bootgame" / "events.py").write_text(
        '"""Boot-test business module (reload target for F-91)."""\n'
        "\n"
        "from pyline.core.events import BaseInitEvent, FuncQuitEvent\n"
        "\n"
        'STATE = {"base_init_services": [], "func_quit": False}\n'
        "\n"
        "\n"
        "def marker() -> int:\n"
        "    return 1\n"
        "\n"
        "\n"
        "async def on_base_init(event: BaseInitEvent) -> None:\n"
        "    from pyline import api\n"
        "\n"
        "    ok = []\n"
        '    for name in ("clock", "alarms", "db"):\n'
        "        try:\n"
        "            api.service(name)\n"
        "            ok.append(name)\n"
        "        except Exception:  # noqa: BLE001 - record, never crash boot\n"
        "            pass\n"
        '    STATE["base_init_services"] = ok\n'
        "\n"
        "\n"
        "async def on_func_quit(event: FuncQuitEvent) -> None:\n"
        '    STATE["func_quit"] = True\n'
        "\n"
        "\n"
        "def register(bus) -> None:\n"
        "    bus.subscribe(BaseInitEvent, on_base_init)\n"
        "    bus.subscribe(FuncQuitEvent, on_func_quit)\n",
        encoding="utf-8",
    )


@pytest.fixture()
def boot_env(config_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Config dir + freshly imported business package + PYLINE_EVENTS wired."""
    _write_config(config_dir)
    business_dir = tmp_path / "business"
    _write_business(business_dir)
    monkeypatch.syspath_prepend(str(business_dir))
    monkeypatch.setenv("PYLINE_EVENTS", "bootgame.events")
    # A cached import from a previous test's tmp_path would point reloads at
    # a deleted file; always start from this test's copy.
    monkeypatch.delitem(sys.modules, "bootgame.events", raising=False)
    monkeypatch.delitem(sys.modules, "bootgame", raising=False)
    import bootgame.events as boot_events

    return config_dir, boot_events


# --------------------------------------------------------------------------- #
# F-92 + F-58 + F-94: db-less full boot, services early, shutdown settles
# --------------------------------------------------------------------------- #


class TestDblessBootF92F58F94:
    async def test_gateway_boots_and_shuts_down_cleanly(self, boot_env) -> None:
        """use_mysql=false + use_redis=false + no db sub-process used to blow
        up in CONN_DB (db_service_no raised ValueError). Boot must succeed,
        expose the facades from the earliest hooks (F-94), and a spawned
        shutdown must be joined by _settle_shutdown with save_flush_ok True
        (the F-58 end-to-end path)."""
        config_dir, boot_events = boot_env
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        assert ctx.entry.use_mysql is False and ctx.entry.use_redis is False
        pyline_api.bind(ctx)
        try:
            runtime = ServerRuntime(ctx)
            await runtime.boot()
            assert runtime.lifecycle.state is LifecycleState.FINISHED
            from pyline.db.service import DatabaseAccess

            assert isinstance(ctx.services["db"], DatabaseAccess)
            # F-94: clock/alarms/db are usable from BaseInitEvent handlers
            # (which fire three boot steps before FUNC_DONE used to register
            # them).
            assert sorted(boot_events.STATE["base_init_services"]) == ["alarms", "clock", "db"]

            # shutdown spawned fire-and-forget, exactly like a signal handler
            runtime._spawn(runtime.shutdown("test end"))
            await _settle_shutdown(runtime)
            assert runtime._flush_completed is True
            assert runtime.save_flush_ok is True
            assert boot_events.STATE["func_quit"] is True
        finally:
            pyline_api.unbind()

    async def test_unknown_clock_tz_fails_fast(self, config_dir: Path) -> None:
        """F-100: a typo'd clock.tz must abort before any calendar number is
        derived under a wrong timezone."""
        _write_config(config_dir, clock_tz="Not/AZone")
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        with pytest.raises(ConfigError, match="Not/AZone"):
            ServerRuntime(ctx)

    async def test_clock_tz_is_wired_into_the_runtime_clock(self, config_dir: Path) -> None:
        """F-100: settings.clock.tz pins GameClock's timezone."""
        _write_config(config_dir, clock_tz="UTC")
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        runtime = ServerRuntime(ctx)
        local = runtime.clock.local(0)
        assert local.tzinfo is not None
        assert local.utcoffset() == dt.timedelta(0)


# --------------------------------------------------------------------------- #
# F-93: teardown order -- every ingress closes before the save-flush
# --------------------------------------------------------------------------- #


class TestTeardownOrderF93:
    async def test_ingress_closes_before_flush_and_zmq_after(self, boot_env) -> None:
        config_dir, _boot_events = boot_env
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        pyline_api.bind(ctx)
        try:
            runtime = ServerRuntime(ctx)
            await runtime.boot()
        finally:
            pyline_api.unbind()

        order: list[str] = []
        runtime.bus.subscribe(FuncQuitEvent, lambda _e: order.append("func-quit"))

        import pyline.runtime as runtime_mod

        orig_close_server = runtime_mod.close_server

        async def close_server_rec(server: object) -> None:
            order.append("client-listener")
            await orig_close_server(server)  # type: ignore[arg-type]

        runtime_mod.close_server = close_server_rec  # type: ignore[assignment]
        try:
            assert runtime._client_server is not None
            assert runtime.proxy_client is not None
            assert runtime.bus_zmq is not None

            def wrap(owner: object, attr: str, name: str) -> None:
                orig = getattr(owner, attr)

                async def step() -> object:
                    order.append(name)
                    return await orig()

                setattr(owner, attr, step)

            wrap(runtime.proxy_client, "close", "proxy-client")
            wrap(runtime.bus_zmq, "close", "zmq-bus")
            wrap(runtime.save_scheduler, "stop", "save-flush")
            assert runtime.devtools.monitor is not None
            wrap(runtime.devtools.monitor, "stop", "monitor")
            wrap(runtime.scheduler, "close", "scheduler")

            await runtime._shutdown_teardown()
        finally:
            runtime_mod.close_server = orig_close_server  # type: ignore[assignment]

        # Every mutation source is closed before the flush snapshot; the DB
        # path (zmq bus) closes only after the flush that may use it.
        assert order[0] == "func-quit"
        assert order.index("save-flush") > order.index("client-listener")
        assert order.index("save-flush") > order.index("proxy-client")
        assert order.index("zmq-bus") > order.index("save-flush")
        assert order.index("monitor") > order.index("save-flush")
        assert order[-1] == "scheduler"
        assert runtime.save_flush_ok is True


class TestDbServiceTeardownWiring:
    async def test_db_service_close_runs_before_the_pools(self, boot_env) -> None:
        """F-101 wiring: DatabaseService.close() (graceful rollback of the
        remote-transaction sessions on their out-of-pool connections) used
        to have no production caller -- the teardown plan closed only the
        pools, and live sessions were dropped for the OS to notice. It must
        run, and before redis/mysql close."""
        config_dir, _boot_events = boot_env
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        runtime = ServerRuntime(ctx)
        order: list[str] = []

        class FakeDbService:
            async def close(self) -> None:
                order.append("db-service")

        class FakeRedis:
            async def close(self) -> None:
                order.append("redis")

        class FakeMysql:
            async def close(self) -> None:
                order.append("mysql")

        runtime.db_layer.db_service = FakeDbService()  # type: ignore[assignment]
        runtime.db_layer.redis = FakeRedis()  # type: ignore[assignment]
        runtime.db_layer.mysql = FakeMysql()  # type: ignore[assignment]

        await runtime._shutdown_teardown()

        assert "db-service" in order
        assert order.index("db-service") < order.index("redis")
        assert order.index("db-service") < order.index("mysql")


# --------------------------------------------------------------------------- #
# F-95: TeardownPlan per-step budget
# --------------------------------------------------------------------------- #


class TestTeardownPlanStepBudgetF95:
    async def test_hung_step_times_out_and_next_step_still_runs(self) -> None:
        entered: list[str] = []

        async def hang() -> None:
            entered.append("hang")
            await asyncio.sleep(30)

        async def quick() -> None:
            entered.append("quick")

        plan = TeardownPlan(total_timeout=30.0, step_timeout=0.05)
        plan.add("hang", hang)
        plan.add("quick", quick)
        await plan.run()
        assert entered == ["hang", "quick"]  # the hung step could not starve it

    async def test_step_timeout_override_gets_the_remaining_total(self) -> None:
        """The save-flush step is exempt from the per-step cap: it may use
        the whole remaining total budget for its slow drain."""
        entered: list[str] = []

        async def slow_flush() -> None:
            entered.append("flush")
            await asyncio.sleep(0.15)  # far beyond the 0.05 step cap

        plan = TeardownPlan(total_timeout=10.0, step_timeout=0.05)
        plan.add("flush", slow_flush, timeout=10.0)
        await plan.run()
        assert entered == ["flush"]

    async def test_total_budget_exhaustion_skips_rest_loudly(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        completed: list[str] = []

        async def slow() -> None:
            await asyncio.sleep(0.2)  # pragma: no cover - always cancelled

        async def never() -> None:
            await asyncio.sleep(0.01)
            completed.append("never")  # pragma: no cover - must not finish

        plan = TeardownPlan(total_timeout=0.05, step_timeout=None)
        plan.add("slow", slow)
        plan.add("never", never)
        with caplog.at_level(logging.CRITICAL, logger="pyline.runtime_wiring"):
            await asyncio.wait_for(plan.run(), timeout=5.0)
        # Whatever the scheduler jitter, nothing behind the exhausted budget
        # may COMPLETE, and the exhaustion is logged loudly.
        assert completed == []
        assert any(
            "budget exhausted" in rec.message or "timed out" in rec.message
            for rec in caplog.records
        )

    async def test_step_failure_does_not_skip_the_rest(self) -> None:
        entered: list[str] = []

        async def boom() -> None:
            entered.append("boom")
            raise RuntimeError("step died")

        async def after() -> None:
            entered.append("after")

        plan = TeardownPlan()
        plan.add("boom", boom)
        plan.add("after", after)
        await plan.run()
        assert entered == ["boom", "after"]


# --------------------------------------------------------------------------- #
# F-91: PreReload handlers complete before the code swap
# --------------------------------------------------------------------------- #


class TestPreReloadOrderingF91:
    async def test_pre_handlers_see_old_code_and_finish_before_swap(self, boot_env) -> None:
        """The old fire-and-forget emit let the swap finish first; business
        code could never quiesce/serialize ahead of the new code."""
        config_dir, boot_events = boot_env
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        runtime = ServerRuntime(ctx)
        observed: list[tuple[str, int]] = []

        async def on_pre(event: PreReloadEvent) -> None:
            # Sleep past any scheduling race: if the reload were not awaited,
            # the marker would already read 2 by the time this handler runs.
            await asyncio.sleep(0.05)
            observed.append(("pre", boot_events.marker()))

        def on_post(event: OnReloadEvent) -> None:
            observed.append(("on", boot_events.marker()))

        runtime.bus.subscribe(PreReloadEvent, on_pre)
        runtime.bus.subscribe(OnReloadEvent, on_post)

        source = Path(boot_events.__file__)
        assert source.exists()
        source.write_text(
            source.read_text(encoding="utf-8").replace("return 1", "return 2"),
            encoding="utf-8",
        )
        await runtime._reload_and_rebind("bootgame.events")
        await asyncio.sleep(0.05)  # the OnReload emit is fire-and-forget
        assert observed == [("pre", 1), ("on", 2)]


# --------------------------------------------------------------------------- #
# F-98: debug SetTime setback must not re-fire calendar boundaries
# --------------------------------------------------------------------------- #


class TestClockSetbackF98:
    async def test_setback_and_restore_do_not_refire_boundaries(self) -> None:
        from pyline.core.clock import GameClock
        from pyline.core.events import NewHourEvent

        clock = GameClock(epoch=dt.datetime(2024, 1, 1), tz="UTC")
        bus = EventBus()
        seen: list[object] = []
        bus.subscribe(NewHourEvent, lambda event: seen.append(event))
        emitter = ClockEventEmitter(
            clock,
            Scheduler(),
            bus,
            log_dir=Path("."),
            spawn=lambda coro: asyncio.get_running_loop().create_task(coro),
        )
        t0 = clock.now()
        emitter._last_boundary = t0

        clock.set_time(t0 + 3700)  # jump past one hourly boundary
        emitter.emit_missed_boundaries()
        await asyncio.sleep(0.01)
        fired_after_jump = len(seen)
        assert fired_after_jump >= 1  # sanity: the boundary did fire

        clock.set_time(t0 + 3700 - 7200)  # SetTime two hours BACK
        emitter.emit_missed_boundaries()
        clock.set_time(t0 + 3700)  # restore the debug offset
        emitter.emit_missed_boundaries()
        await asyncio.sleep(0.01)
        assert len(seen) == fired_after_jump  # no boundary fired twice


class TestClockJumpCatchUpBound:
    async def test_multi_year_jump_skips_arithmetically_and_alarms(self) -> None:
        """A multi-year debug clock jump used to walk one loop iteration per
        30-minute boundary (~48 per jumped day, each enumerating ~50 wall-grid
        candidates) inside the synchronous scheduler callback -- a loop stall
        proportional to the jump. The skip count is arithmetic now, bounded,
        and still alarms."""
        import time

        from pyline.core.clock import GameClock
        from pyline.obs.metrics import AlarmHub

        clock = GameClock(epoch=dt.datetime(2024, 1, 1), tz="UTC")
        alarms = AlarmHub()
        seen: list[tuple[str, dict]] = []
        alarms.register_all(lambda kind, payload: seen.append((kind, payload)))
        emitter = ClockEventEmitter(
            clock,
            Scheduler(),
            EventBus(),
            log_dir=Path("."),
            spawn=lambda coro: asyncio.get_running_loop().create_task(coro),
            alarms=alarms,
        )
        t0 = clock.now()
        emitter._last_boundary = t0

        started = time.monotonic()
        clock.set_time(t0 + 400 * 86400)  # ~400 days: ~19200 boundaries
        emitter.emit_missed_boundaries()
        elapsed = time.monotonic() - started
        await asyncio.sleep(0.01)

        skips = [p for kind, p in seen if kind == "clock_boundaries_skipped"]
        assert skips and skips[0]["skipped"] > 19_000  # arithmetic count, not 0
        assert elapsed < 1.0  # no per-boundary iteration over 400 days


# --------------------------------------------------------------------------- #
# F-96: watcher root derivation (devtools layer)
# --------------------------------------------------------------------------- #


class TestWatcherRootsF96:
    def test_watch_roots_follow_business_package(self, boot_env) -> None:
        """The watcher must observe the business package directory, not the
        whole cwd (saving a stray tests/*.py must not reload anything)."""
        config_dir, _events = boot_env
        ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
        from pyline.runtime_wiring import DevtoolsLayer

        layer = DevtoolsLayer(ctx, None, EventBus())  # type: ignore[arg-type]
        roots = layer._watch_roots()
        assert len(roots) == 1
        assert roots[0].name == "bootgame"

    def test_watch_roots_fall_back_to_cwd_when_unresolvable(
        self, boot_env, caplog: pytest.LogCaptureFixture
    ) -> None:
        import os

        import pyline.runtime_wiring as wiring_mod

        config_dir, _events = boot_env
        old_events = os.environ.get("PYLINE_EVENTS")
        os.environ["PYLINE_EVENTS"] = "definitely.not.importable"
        try:
            ctx = build_context(config_dir, BOOT_SERVER_NO, "main", 0, 0)
            layer = wiring_mod.DevtoolsLayer(ctx, None, EventBus())  # type: ignore[arg-type]
            with caplog.at_level(logging.WARNING, logger="pyline.runtime_wiring"):
                roots = layer._watch_roots()
            assert roots == [Path.cwd()]
            assert any("falls back to cwd" in rec.message for rec in caplog.records)
        finally:
            if old_events is None:
                os.environ.pop("PYLINE_EVENTS", None)
            else:
                os.environ["PYLINE_EVENTS"] = old_events


# --------------------------------------------------------------------------- #
# F-91/F-97: async reload hooks through the console and watcher surfaces
# --------------------------------------------------------------------------- #


def _watcher_with_hook(hook, shutdown_hook=None) -> FileWatcher:
    watcher = FileWatcher([], reload_hook=hook, shutdown_hook=shutdown_hook)
    # The exception-policy tests target the hook invocation, not path
    # resolution; pin the module name directly.
    watcher._module_name = lambda _path: "game.mod"  # type: ignore[method-assign]
    return watcher


class TestReloadHookSurfaces:
    async def test_console_update_schedules_async_reload_hook(self) -> None:
        done: list[str] = []

        async def hook(module: str) -> None:
            await asyncio.sleep(0.01)
            done.append(module)

        console = Console(bus=EventBus(), reload_hook=hook)
        console.execute("update game.events, game.time")
        await asyncio.sleep(0.05)
        assert done == ["game.events", "game.time"]
        await console.stop()

    async def test_watcher_handles_async_reload_hook(self, tmp_path: Path) -> None:
        reloaded: list[str] = []

        async def hook(module: str) -> None:
            reloaded.append(module)

        watcher = FileWatcher([tmp_path], reload_hook=hook)
        sys.path.insert(0, str(tmp_path))
        try:
            watcher._handle(tmp_path / "game" / "mod.py")
            await asyncio.sleep(0.01)
            assert reloaded == ["game.mod"]
        finally:
            sys.path.remove(str(tmp_path))
        await watcher.stop()

    async def test_watcher_reload_systemexit_routes_to_shutdown_hook(self) -> None:
        calls: list[str] = []

        def hook(module: str) -> None:
            raise SystemExit(9)

        watcher = _watcher_with_hook(hook, shutdown_hook=calls.append)
        watcher._handle(Path("game/x.py"))
        assert calls and "SystemExit" in calls[0]
        watcher._handle(Path("game/y.py"))  # watcher survives and keeps working
        assert len(calls) == 2

    def test_watcher_reload_systemexit_without_hook_escalates(self) -> None:
        def hook(module: str) -> None:
            raise SystemExit(9)

        watcher = _watcher_with_hook(hook)
        with pytest.raises(SystemExit):
            watcher._handle(Path("game/x.py"))

    async def test_watcher_survives_non_exception_base_exception(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        class Weird(BaseException):
            pass

        def hook(module: str) -> None:
            raise Weird("generator-adjacent mishap")

        watcher = _watcher_with_hook(hook)
        with caplog.at_level(logging.CRITICAL, logger="pyline.devtools.watcher"):
            watcher._handle(Path("game/x.py"))
            watcher._handle(Path("game/y.py"))  # still alive
        assert any("BaseException" in rec.message for rec in caplog.records)

    async def test_watcher_task_done_escalates_system_exit(self) -> None:
        """A done-callback raise propagates out of run_forever and exits the
        process; the callback is unit-tested with a fake task (a REAL task
        raising SystemExit would kill the test's own event loop first --
        asyncio re-raises it from the loop, never storing it on the task)."""

        class _FakeTask:
            def cancelled(self) -> bool:
                return False

            def exception(self) -> BaseException:
                return SystemExit(3)

        watcher = FileWatcher([])
        with pytest.raises(SystemExit):
            watcher._task_done(_FakeTask())  # type: ignore[arg-type]

    async def test_watcher_async_reload_systemexit_routes_to_shutdown_hook(self) -> None:
        """F-97: the guard inside the reload coroutine catches the exit
        BEFORE asyncio would re-raise it out of the loop, so the runtime's
        shutdown hook gets a graceful-shutdown chance."""
        calls: list[str] = []

        async def hook(module: str) -> None:
            raise SystemExit(11)

        watcher = _watcher_with_hook(hook, shutdown_hook=calls.append)
        watcher._handle(Path("game/x.py"))
        await asyncio.sleep(0.05)  # the guarded reload task completes
        assert calls and "SystemExit" in calls[0]
        await watcher.stop()

    async def test_console_reload_task_exit_routes_to_shutdown_hook(self) -> None:
        calls: list[str] = []

        async def hook(module: str) -> None:
            raise SystemExit(7)

        console = Console(bus=EventBus(), reload_hook=hook, shutdown_hook=calls.append)
        console.execute("update game.events")
        await asyncio.sleep(0.05)
        assert calls and "SystemExit" in calls[0]
        await console.stop()

    async def test_console_reload_task_done_escalates_without_hook(self) -> None:
        class _FakeTask:
            def cancelled(self) -> bool:
                return False

            def exception(self) -> BaseException:
                return KeyboardInterrupt()

        console = Console(bus=EventBus())
        with pytest.raises(KeyboardInterrupt):
            console._reload_task_done(_FakeTask())  # type: ignore[arg-type]
