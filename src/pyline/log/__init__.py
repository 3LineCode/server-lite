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

# F-80: pid + logger name in every line. Multi-process deployments share the
# run dir; without the pid the operator cannot tell which process wrote a
# line, and without the logger name every stdlib line looks identical.
_LOG_FORMAT = "%(asctime)s [%(levelname)s] pid=%(process)d %(name)s: %(message)s"


def setup_logging(settings: LogSettings, *, process_tag: str, run_dir: Path) -> None:
    """Configure the process-wide logging pipeline once at boot.

    F-80: the main process and every sub-process used to write the SAME
    ``os.log`` -- concurrent writers interleave under POSIX and on Windows
    the midnight rollover's ``os.rename`` fails outright (file open in
    another process). Each process now writes its own ``os-<tag>.log``
    (``process_tag`` is always provided by the runtime, so the plain
    ``os.log`` name survives only for an empty tag).
    """
    root = logging.getLogger()
    root.setLevel(settings.level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_LOG_FORMAT))
    _set_handlers(root, console)

    file_name = f"os-{process_tag}.log" if process_tag else "os.log"
    file_handler = logging.handlers.TimedRotatingFileHandler(
        run_dir / file_name,
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


# Keyed by channel name only, holding (target path, rotation, logger): the
# named logger is a stdlib singleton, so at most one live target may exist
# per name. The same channel name requested with a different run_dir
# (multi-setup tests, re-init) used to silently return the OLD channel
# writing to the OLD file (F-22), and simply adding another handler produced
# a double-writing logger: every record landed in BOTH the old and the new
# file (F-79). The cache now tracks the single live target per name.
_file_channels: dict[str, tuple[str, int, logging.Logger]] = {}


def file_logger(name: str, run_dir: Path, *, rotation_mb: int = 64) -> logging.Logger:
    """Get (or create) a dedicated rotating file channel, e.g. ``file_logger("database")``.

    Writes to ``<run_dir>/<name>.log`` with size-based rotation; used for
    operational channels like verify/audit/save in the prototype. Requesting
    the same name with a different ``run_dir``/``rotation_mb`` RETARGETS the
    channel: the previous handler is detached and closed first (F-79), so the
    name never writes to a stale file.
    """
    target = run_dir / f"{name}.log"
    cached = _file_channels.get(name)
    if cached is not None and cached[0] == str(target) and cached[1] == rotation_mb:
        return cached[2]

    channel = logging.getLogger(f"pyline.channel.{name}")
    # The logger object is a per-name singleton shared across callers: detach
    # whatever it currently holds before installing the new target, or the
    # old handler keeps writing (and holds the old file open on Windows).
    for old_handler in list(channel.handlers):
        channel.removeHandler(old_handler)
        old_handler.close()
    channel.setLevel(logging.INFO)
    channel.propagate = False
    handler = logging.handlers.RotatingFileHandler(
        target,
        maxBytes=rotation_mb * 1024 * 1024,
        backupCount=7,
        encoding="utf-8",
        delay=True,
    )
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
    channel.addHandler(handler)
    _file_channels[name] = (str(target), rotation_mb, channel)
    return channel


def clear_file_channels() -> None:
    """Drop cached channels (test isolation / re-setup).

    F-79: also closes the dropped handlers -- an open RotatingFileHandler
    pins the old file on Windows and blocks tmp_path cleanup.
    """
    for _target, _rotation, channel in _file_channels.values():
        for handler in list(channel.handlers):
            channel.removeHandler(handler)
            handler.close()
    _file_channels.clear()


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger; extras (process tag, trace ids) bind automatically."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger
