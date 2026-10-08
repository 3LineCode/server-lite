"""Environment facade: identity, process predicates, shutdown (old aio_core +
aioinfo judgement family)."""

from __future__ import annotations

import os
import sys

from pyline import api
from pyline.core.context import PROCESS_MAIN


def service_no() -> int:
    return api.ctx().service_no


def server_no() -> int:
    return api.ctx().server_no


def service_name() -> str:
    return api.ctx().entry.name


def is_main_process(service: int | None = None) -> bool:
    if service is None:
        return api.ctx().process_type == PROCESS_MAIN
    return service < 100_000


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
    import asyncio

    lifecycle = api.ctx().lifecycle
    if lifecycle is None:
        os.kill(os.getpid(), 15)
        return
    asyncio.get_running_loop().create_task(lifecycle.request_shutdown(reason))


def kill(reason: str) -> None:
    """Immediate process termination (old KillProcess)."""
    import signal

    sys.stderr.write(f"process killed: {reason}\n")
    sig = getattr(signal, "SIGKILL", signal.SIGTERM)
    os.kill(os.getpid(), sig)
