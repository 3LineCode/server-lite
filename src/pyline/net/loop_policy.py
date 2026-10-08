"""Event-loop policy selection.

zmq.asyncio requires a selector-based loop; on Windows the default is the
proactor loop, which pyzmq rejects at runtime. The framework therefore
installs ``WindowsSelectorEventLoopPolicy`` on win32 (uvloop on POSIX when
available) before any loop is created. Call :func:`install_loop_policy` once
at process start -- ``pyline.runtime.main`` and the supervisor's child
trampoline both do this.
"""

from __future__ import annotations

import asyncio
import sys


def install_loop_policy() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        return
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass  # plain asyncio is fine
