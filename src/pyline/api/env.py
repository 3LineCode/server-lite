"""Environment facade: identity, process predicates, shutdown (old aio_core +
aioinfo judgement family)."""

from __future__ import annotations

import asyncio
import os
import sys

from pyline import api
from pyline.core.context import PROCESS_MAIN, SERVICE_NO_STRIDE

# Shutdown requests fired from business code must keep a strong reference
# (a bare create_task can be garbage-collected before it runs).
_pending: set[asyncio.Task[object]] = set()


def service_no() -> int:
    return api.ctx().service_no


def server_no() -> int:
    return api.ctx().server_no


def service_name() -> str:
    return api.ctx().entry.name


def is_main_process(service: int | None = None) -> bool:
    if service is None:
        return api.ctx().process_type == PROCESS_MAIN
    # Main processes have process_index 0 -> service_no below one stride;
    # single source of truth with Context.service_no.
    return service < SERVICE_NO_STRIDE


def is_sub_process(service: int | None = None) -> bool:
    return not is_main_process(service)


def is_db_process() -> bool:
    return api.ctx().is_db_process


def is_single_process() -> bool:
    return not api.ctx().entry.sub_process


def is_develop() -> bool:
    return api.ctx().is_develop


def main_pid() -> int:
    return api.ctx().main_pid


def on_windows() -> bool:
    return sys.platform.startswith("win")


def on_linux() -> bool:
    return sys.platform.startswith("linux")


def process_index_of(process_type: str) -> int:
    return api.ctx().process_index_of(process_type)


def shutdown(reason: str) -> None:
    """Request graceful shutdown (old StopAio); schedules asynchronously."""
    lifecycle = api.ctx().lifecycle
    if lifecycle is not None:
        task = asyncio.get_running_loop().create_task(lifecycle.request_shutdown(reason))
        _pending.add(task)
        task.add_done_callback(_pending.discard)
        return
    # F-86: fallback when the lifecycle is not wired yet (early boot).
    # The old ``os.kill(os.getpid(), 15)`` LOOKS graceful but on Windows any
    # non-CTRL signal is TerminateProcess -- a hard kill with no flushes and
    # no exit code control (the very behavior documented in
    # supervisor._pid_alive, contradicting the graceful intent here). Be
    # honest about it: explicit hard exit, documented as such, with the
    # reason on stderr so operators can tell how the process died.
    sys.stderr.write(f"hard shutdown (lifecycle not initialised): {reason}\n")
    os._exit(1)


def kill(reason: str) -> None:
    """Immediate process termination (old KillProcess)."""
    import signal

    sys.stderr.write(f"process killed: {reason}\n")
    sig = getattr(signal, "SIGKILL", signal.SIGTERM)
    os.kill(os.getpid(), sig)
