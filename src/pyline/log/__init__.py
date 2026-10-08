"""Logging setup: structlog + stdlib logging, per-channel rotating files.

No global hijacking: we never touch ``builtins.print``, ``warnings.warn`` or
the root logging manager. Components obtain loggers explicitly via
``get_logger()`` and file channels via ``file_logger(name)``.
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

import structlog

from pyline.config.models import LogSettings

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"


def setup_logging(settings: LogSettings, *, process_tag: str, run_dir: Path) -> None:
    """Configure the process-wide logging pipeline once at boot."""
    root = logging.getLogger()
    root.setLevel(settings.level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_LOG_FORMAT))
    _set_handlers(root, console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        run_dir / "os.log",
        when="midnight",
        backupCount=settings.retention_days,
        encoding="utf-8",
        delay=True,
    )
    file_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.addHandler(file_handler)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=False),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty()),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.level, logging.INFO)
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(process=process_tag)


def _set_handlers(root: logging.Logger, *handlers: logging.Handler) -> None:
    for old in list(root.handlers):
        root.removeHandler(old)
    for handler in handlers:
        root.addHandler(handler)


# Keyed by (name, target path, rotation): the same channel name requested
# with a different run_dir (multi-setup tests, re-init) used to silently
# return the OLD channel writing to the OLD file (F-22).
_file_channels: dict[tuple[str, str, int], logging.Logger] = {}


def file_logger(name: str, run_dir: Path, *, rotation_mb: int = 64) -> logging.Logger:
    """Get (or create) a dedicated rotating file channel, e.g. ``file_logger("database")``.

    Writes to ``<run_dir>/<name>.log`` with size-based rotation; used for
    operational channels like verify/audit/save in the prototype.
    """
    target = run_dir / f"{name}.log"
    key = (name, str(target), rotation_mb)
    channel = _file_channels.get(key)
    if channel is not None:
        return channel
    channel = logging.getLogger(f"pyline.channel.{name}")
    channel.setLevel(logging.INFO)
    channel.propagate = False
    handler = logging.handlers.RotatingFileHandler(
        run_dir / f"{name}.log",
        maxBytes=rotation_mb * 1024 * 1024,
        backupCount=7,
        encoding="utf-8",
        delay=True,
    )
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
    channel.addHandler(handler)
    _file_channels[key] = channel
    return channel


def clear_file_channels() -> None:
    """Drop cached channels (test isolation / re-setup)."""
    _file_channels.clear()


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger; extras (process tag, trace ids) bind automatically."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger
