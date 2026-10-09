"""pyline: production-grade asyncio game-server framework.

Rebuilt from the ServerLite/frameaio prototype. See docs/plan.md for the
approved master plan and the known-issues -> fix mapping.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from pyline import config, core, log

try:
    # Single source of truth: the version lives in pyproject.toml and reaches
    # runtime through the installed package metadata (it used to drift -- the
    # literal here read 0.1.0 while pyproject said 1.0.0rc1).
    __version__ = _pkg_version("pyline")
except PackageNotFoundError:  # pragma: no cover - running from a source tree
    __version__ = "0.0.0+unknown"

__all__ = ["__version__", "config", "core", "log"]
