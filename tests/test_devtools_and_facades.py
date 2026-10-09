"""Coverage pass: console commands, watcher mapping, debug formatting, log
channels, task/env facades (F-56 -- restoring the CI coverage gate)."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import pytest

from pyline import api
from pyline.core.events import ConsoleCommandEvent, EventBus
from pyline.devtools.console import Console
from pyline.devtools.watcher import FileWatcher
from pyline.reload.inplace import ReloadError

# ------------------------------ debug facade ------------------------------ #


class TestDebugFacade:
    def test_format_exception_includes_frames_and_locals(self) -> None:
        from pyline.api.debug import format_exception

        def inner() -> None:
            balance = 42  # noqa: F841
            raise ValueError("boom")

        try:
            inner()
        except ValueError as exc:
            rendered = format_exception(exc)
        assert "ValueError: boom" in rendered
        assert "balance = 42" in rendered
        assert "inner" in rendered

    def test_format_exception_truncates_long_values(self) -> None:
        from pyline.api.debug import _fmt_value

        text = _fmt_value("x" * 500)
        assert text.endswith("chars]") and len(text) < 200

    def test_format_exception_chained_cause(self) -> None:
        from pyline.api.debug import format_exception

        try:
            try:
                raise KeyError("root")
            except KeyError as ke:
                raise RuntimeError("outer") from ke
        except RuntimeError as exc:
            rendered = format_exception(exc)
        assert "caused by:" in rendered
        assert "KeyError: 'root'" in rendered  # str(KeyError) quotes its arg

    def test_format_exception_instance_attributes(self) -> None:
        from pyline.api.debug import format_exception

        class Widget:
            def __init__(self) -> None:
                self.state = "broken"

            def fail(self) -> None:
                raise RuntimeError("nope")

        try:
            Widget().fail()
        except RuntimeError as exc:
            rendered = format_exception(exc)
        assert "self.state = 'broken'" in rendered

    def test_trace_lists_caller_frames(self) -> None:
        from pyline.api.debug import trace

        lines = trace("hello")
        assert lines[0] == "trace: hello"
        assert any("test_trace_lists_caller_frames" in line for line in lines)


# ------------------------------ console ------------------------------ #


class TestConsole:
    def _console(self, bus: EventBus, **kwargs: object) -> Console:
        return Console(bus=bus, **kwargs)  # type: ignore[arg-type]

    def test_help_lists_commands(self, capsys: pytest.CaptureFixture) -> None:
        console = self._console(EventBus())
        console.execute("help")
        assert "commands:" in capsys.readouterr().out

    def test_exit_invokes_shutdown_hook(self) -> None:
        calls: list[str] = []
        console = self._console(EventBus(), shutdown_hook=calls.append)
        console.execute("quit")
        console.execute("q")
        assert calls == ["console exit", "console exit"]

    def test_kill_invokes_hook(self) -> None:
        calls: list[str] = []
        console = self._console(EventBus(), kill_hook=calls.append)
        console.execute("kill")
        assert calls == ["console kill"]

    def test_update_reloads_each_module(self) -> None:
        reloaded: list[str] = []
        console = self._console(EventBus(), reload_hook=reloaded.append)
        console.execute("update game.events, game.time")
        assert reloaded == ["game.events", "game.time"]

    def test_update_without_hook_reports(self, capsys: pytest.CaptureFixture) -> None:
        console = self._console(EventBus())
        console.execute("update game.events")
        assert "not available" in capsys.readouterr().out

    async def test_business_command_emits_event(self) -> None:
        bus = EventBus()
        seen: list[ConsoleCommandEvent] = []
        bus.subscribe(ConsoleCommandEvent, seen.append)
        console = self._console(bus)
        console.execute("$ give gold 100")
        await asyncio.sleep(0.01)  # emit runs in a spawned task
        assert len(seen) == 1
        assert seen[0].command == "give gold 100"

    def test_unknown_command_refused_when_safe(self, capsys: pytest.CaptureFixture) -> None:
        console = self._console(EventBus())
        console.execute("2 + 2")
        assert "eval disabled" in capsys.readouterr().out

    def test_unsafe_eval_evaluates(self, capsys: pytest.CaptureFixture) -> None:
        console = self._console(EventBus(), unsafe=True)
        console.execute("1 + 1")
        assert "2" in capsys.readouterr().out

    def test_unsafe_eval_executes_statements(self, capsys: pytest.CaptureFixture) -> None:
        console = self._console(EventBus(), unsafe=True)
        console.execute("x = 40 + 2")
        assert "42" in capsys.readouterr().out


# ------------------------------ watcher ------------------------------ #


class TestWatcher:
    def test_module_name_deepest_sys_path_prefix_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """F-57: with both a shallow base (cwd) and a package root on
        sys.path, the package root must win -- the shallow one maps the file
        onto a wrong dotted name and surprises the import."""
        package_root = tmp_path / "outer"
        monkeypatch.setattr(sys, "path", [str(tmp_path), str(package_root)])
        watcher = FileWatcher([tmp_path])
        assert watcher._module_name(package_root / "game" / "mod.py") == "game.mod"
        assert watcher._module_name(package_root / "pkg" / "__init__.py") == "pkg"

    def test_module_name_unresolvable_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sys, "path", [str(tmp_path / "elsewhere")])
        watcher = FileWatcher([tmp_path])
        assert watcher._module_name(tmp_path / "game" / "mod.py") is None

    def test_handle_ignores_non_python_and_ignored_dirs(self) -> None:
        reloaded: list[str] = []
        watcher = FileWatcher([], reload_hook=reloaded.append)
        watcher._handle(Path("game/data.json"))
        watcher._handle(Path(".git/config.py"))
        watcher._handle(Path("__pycache__/mod.cpython-312.py"))
        assert reloaded == []

    def test_handle_reloads_and_survives_errors(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sys, "path", [str(tmp_path)])
        reloaded: list[str] = []

        def flaky(module: str) -> None:
            reloaded.append(module)
            if module == "game.bad":
                raise ReloadError("rejected")
            if module == "game.worse":
                raise RuntimeError("crash")

        watcher = FileWatcher([], reload_hook=flaky)
        watcher._handle(tmp_path / "game" / "good.py")
        watcher._handle(tmp_path / "game" / "bad.py")
        watcher._handle(tmp_path / "game" / "worse.py")
        assert reloaded == ["game.good", "game.bad", "game.worse"]  # loop survives

    async def test_stop_is_idempotent(self) -> None:
        watcher = FileWatcher([])
        await watcher.stop()  # never started: no error
        await watcher.stop()


# ------------------------------ log channels ------------------------------ #


class TestLogChannels:
    def test_file_logger_writes_and_caches_per_target(self, tmp_path: Path) -> None:
        from pyline import log as pyline_log

        pyline_log.clear_file_channels()
        channel = pyline_log.file_logger("audit-test", tmp_path)
        channel.info("hello %s", "audit")
        for handler in channel.handlers:
            handler.flush()
        assert "hello audit" in (tmp_path / "audit-test.log").read_text(encoding="utf-8")

        # F-22 semantics: same name + same dir -> cached channel
        assert pyline_log.file_logger("audit-test", tmp_path) is channel
        # same name + different dir -> not served from the stale cache: a
        # handler targeting the NEW dir is attached
        relocated = pyline_log.file_logger("audit-test", tmp_path / "other")
        targets = {str(h.baseFilename) for h in relocated.handlers if hasattr(h, "baseFilename")}
        assert str(tmp_path / "other" / "audit-test.log") in targets
        pyline_log.clear_file_channels()

    def test_get_logger_returns_structured_logger(self) -> None:
        from pyline import log as pyline_log

        logger = pyline_log.get_logger("test-channel")
        logger.info("structured", value="yes")  # structlog: kwargs, not %-args

    def test_setup_logging_installs_console_and_file(self, tmp_path: Path) -> None:
        from pyline import log as pyline_log
        from pyline.config.models import LogSettings

        root = logging.getLogger()
        before_handlers = list(root.handlers)
        before_level = root.level
        try:
            pyline_log.setup_logging(
                LogSettings(level="INFO", log_dir=str(tmp_path)),
                process_tag="test-proc",
                run_dir=tmp_path,
            )
            assert root.level == logging.INFO
            kinds = {type(h).__name__ for h in root.handlers}
            assert "StreamHandler" in kinds
            assert "TimedRotatingFileHandler" in kinds
        finally:
            for handler in list(root.handlers):
                if handler not in before_handlers:
                    root.removeHandler(handler)
                    handler.close()  # no transient ResourceWarning at GC
            root.setLevel(before_level)


# ------------------------------ task / env facades ------------------------------ #


@pytest.fixture()
async def bound_ctx(config_dir, tmp_path):
    from pyline.core.clock import GameClock
    from pyline.core.scheduler import Scheduler
    from pyline.runtime import build_context

    ctx = build_context(config_dir, 10001, "main", 0, 0)
    ctx.scheduler = Scheduler(loop=asyncio.get_running_loop())
    ctx.services["clock"] = GameClock()
    api.bind(ctx)
    yield ctx
    api.unbind()


class TestTaskFacade:
    async def test_spawn_keeps_reference_and_logs_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from pyline.api import task as task_api

        async def ok() -> object:
            return "done"

        async def boom() -> object:
            raise RuntimeError("task blew up")

        t1 = task_api.spawn(ok())
        task_api.spawn(boom())
        await asyncio.sleep(0.05)
        assert t1.result() == "done"
        assert any("spawned task failed" in r.message for r in caplog.records)

    async def test_start_task_without_lifecycle_fails_loudly(self, bound_ctx) -> None:
        from pyline.api import task as task_api

        async def work() -> None:
            return None

        assert api.ctx().lifecycle is None
        work_coro = work()
        with pytest.raises(RuntimeError, match="lifecycle not initialised"):
            task_api.start_task(work_coro)
        work_coro.close()  # never awaited: the guard raised before create_task
        with pytest.raises(RuntimeError, match="lifecycle not initialised"):
            task_api.add_start_wait("flag")

    async def test_on_quit_tracks_task_without_lifecycle(self, bound_ctx) -> None:
        from pyline.api import task as task_api

        async def cleanup() -> str:
            return "cleaned"

        task = await task_api.on_quit(cleanup())
        await asyncio.sleep(0.01)
        assert task.result() == "cleaned"


class TestEnvFacade:
    async def test_identity_predicates(self, bound_ctx) -> None:
        from pyline.api import env as env_api
        from pyline.core.context import SERVICE_NO_STRIDE

        assert env_api.service_no() == api.ctx().service_no
        assert env_api.server_no() == api.ctx().server_no
        assert env_api.service_name() == api.ctx().entry.name
        assert env_api.is_main_process()
        assert env_api.is_main_process(SERVICE_NO_STRIDE) is False
        assert env_api.is_sub_process(SERVICE_NO_STRIDE)
        assert env_api.is_develop() == api.ctx().is_develop
        assert env_api.on_windows() == sys.platform.startswith("win")
        assert env_api.on_linux() == sys.platform.startswith("linux")
        assert isinstance(env_api.main_pid(), int)
