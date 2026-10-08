"""Log facade (old LogFile/LogDebug): per-channel files under the run dir."""

from __future__ import annotations

import logging
from pathlib import Path

from pyline import api
from pyline.log import file_logger


def _run_dir() -> Path:
    return Path(api.ctx().settings.log.log_dir)


def file(name: str) -> logging.Logger:
    """``<log_dir>/<name>.log`` channel (size-rotated)."""
    return file_logger(name, _run_dir(), rotation_mb=api.ctx().settings.log.rotation_mb)


def file_debug(name: str) -> logging.Logger:
    """``<log_dir>/debug/<name>.log`` channel."""
    return file_logger(f"debug/{name}", _run_dir(), rotation_mb=api.ctx().settings.log.rotation_mb)
