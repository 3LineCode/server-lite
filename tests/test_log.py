"""Logging: per-process os log (F-80), channel retargeting (F-79)."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from pyline import log as pyline_log
from pyline.config.models import LogSettings


@pytest.fixture()
def _restore_root_logger():
    root = logging.getLogger()
    before_handlers = list(root.handlers)
    before_level = root.level
    pyline_log.clear_file_channels()
    yield
    pyline_log.clear_file_channels()
    for handler in list(root.handlers):
        if handler not in before_handlers:
            root.removeHandler(handler)
            handler.close()
    root.setLevel(before_level)


class TestSetupLoggingPerProcessF80:
    def test_process_tag_gets_its_own_file(self, tmp_path: Path, _restore_root_logger) -> None:
        """F-80: main + sub-processes all wrote the same os.log; concurrent
        writers interleave and the Windows midnight rename fails outright."""
        pyline_log.setup_logging(
            LogSettings(level="INFO", log_dir=str(tmp_path)),
            process_tag="db",
            run_dir=tmp_path,
        )
        logging.getLogger("pyline.test.f80").warning("hello-f80")
        for handler in logging.getLogger().handlers:
            handler.flush()
        text = (tmp_path / "os-db.log").read_text(encoding="utf-8")
        assert "hello-f80" in text
        assert not (tmp_path / "os.log").exists()

    def test_lines_carry_pid_and_logger_name(self, tmp_path: Path, _restore_root_logger) -> None:
        """F-80 (P3 attribution): without pid/logger name, a shared log line
        could not be attributed to a process or a component."""
        pyline_log.setup_logging(
            LogSettings(level="INFO", log_dir=str(tmp_path)),
            process_tag="main",
            run_dir=tmp_path,
        )
        logging.getLogger("pyline.test.f80b").warning("attributed-line")
        for handler in logging.getLogger().handlers:
            handler.flush()
        text = (tmp_path / "os-main.log").read_text(encoding="utf-8")
        assert f"pid={os.getpid()}" in text
        assert "pyline.test.f80b" in text
        assert "attributed-line" in text


class TestFileLoggerRetargetF79:
    def test_retargeting_same_name_does_not_double_write(self, tmp_path: Path) -> None:
        """F-79: the same channel name with a different run_dir used to
        ADD a handler to the singleton logger -- every record was then
        written to BOTH the old and the new file."""
        try:
            (tmp_path / "a").mkdir()
            (tmp_path / "b").mkdir()
            first = pyline_log.file_logger("dup-channel", tmp_path / "a")
            first.info("to-old")
            for handler in first.handlers:
                handler.flush()
            old_handlers = list(first.handlers)

            second = pyline_log.file_logger("dup-channel", tmp_path / "b")
            assert second is first  # stdlib logger singleton per name
            # exactly one live file handler, targeting the NEW dir
            file_handlers = [h for h in second.handlers if hasattr(h, "baseFilename")]
            assert len(file_handlers) == 1
            assert str(tmp_path / "b" / "dup-channel.log") in {
                str(h.baseFilename) for h in file_handlers
            }
            # the old handler was detached AND closed (releases the file):
            # FileHandler.close() drops the stream reference -- a stream that
            # is None or closed proves close() ran
            for old in old_handlers:
                assert old not in second.handlers
                stream = getattr(old, "stream", None)
                assert stream is None or stream.closed

            second.info("to-new")
            for handler in second.handlers:
                handler.flush()
            old_text = (tmp_path / "a" / "dup-channel.log").read_text(encoding="utf-8")
            new_text = (tmp_path / "b" / "dup-channel.log").read_text(encoding="utf-8")
            assert "to-old" in old_text
            assert "to-new" not in old_text  # the double-write regression
            assert "to-new" in new_text
            assert "to-old" not in new_text
        finally:
            pyline_log.clear_file_channels()

    def test_cache_hit_returns_same_channel(self, tmp_path: Path) -> None:
        try:
            channel = pyline_log.file_logger("cached", tmp_path)
            assert pyline_log.file_logger("cached", tmp_path) is channel
        finally:
            pyline_log.clear_file_channels()

    def test_clear_closes_handlers(self, tmp_path: Path) -> None:
        channel = pyline_log.file_logger("closeme", tmp_path)
        handler = channel.handlers[0]
        channel.info("open-the-stream")
        handler.flush()
        assert handler.stream is not None  # opened (delay=True opens on write)
        pyline_log.clear_file_channels()
        assert not channel.handlers
        # FileHandler.close() closes the stream and drops the reference:
        # stream back to None proves close() actually ran (F-79)
        assert handler.stream is None
