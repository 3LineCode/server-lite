"""Runtime context: the explicit dependency container.

Replaces every module-level singleton of the prototype (``g_ServerInfo``,
``g_Transfer``, ``if not "g_X" in globals()`` guards...). Components receive
the :class:`Context` through construction; nothing imports globals.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

from pyline.config.models import ProjectSettings, ServerEntry, ServerRegistry, TableDef

if TYPE_CHECKING:
    from pyline.core.events import EventBus
    from pyline.core.lifecycle import LifecycleManager
    from pyline.core.scheduler import Scheduler

_T = TypeVar("_T")

#: Process type of the primary process (owns client/proxy listeners).
PROCESS_MAIN = "main"
#: Process type of the database proxy process (owns MySQL/Redis connections).
PROCESS_DB = "db"

#: service_no = process_index * SERVICE_NO_STRIDE + server_no
SERVICE_NO_STRIDE = 100_000


class ServiceNotAvailableError(RuntimeError):
    """A service name is not registered in the context's bag (wrong process,
    or the boot step that registers it has not run yet)."""


class ServiceTypeError(RuntimeError):
    """A registered service does not have the type the caller expects."""


@dataclass
class Context:
    """Everything a component may need at runtime."""

    settings: ProjectSettings
    registry: ServerRegistry
    tables: dict[str, TableDef]
    entry: ServerEntry
    process_type: str
    process_index: int
    main_pid: int

    loop: asyncio.AbstractEventLoop | None = None
    scheduler: Scheduler | None = None
    events: EventBus | None = None
    lifecycle: LifecycleManager | None = None
    #: Extension slot for later phases (net managers, db pools...).
    services: dict[str, object] = field(default_factory=dict)

    @property
    def server_no(self) -> int:
        return self.entry.server_no

    @property
    def service_no(self) -> int:
        return self.process_index * SERVICE_NO_STRIDE + self.entry.server_no

    @property
    def main_service_no(self) -> int:
        """Server number without the process-index stride."""
        return self.entry.server_no

    @property
    def is_main_process(self) -> bool:
        return self.process_type == PROCESS_MAIN

    @property
    def is_db_process(self) -> bool:
        return self.process_type == PROCESS_DB

    @property
    def is_sub_process(self) -> bool:
        return self.process_type != PROCESS_MAIN

    @property
    def is_develop(self) -> bool:
        return self.settings.srv_type == "develop"

    def db_service_no(self) -> int | None:
        """Service number of this server's DB proxy process; ``None`` when no
        ``db`` sub-process is configured.

        Cross-agent contract: single-process deployments own the pools
        locally, so there is no DB service to address -- callers
        (runtime wiring, DatabaseAccess) treat ``None`` as "local access
        only" instead of the old behavior where a missing ``db`` entry blew
        up with a ValueError naming an "unknown process type".
        """
        try:
            db_index = self.process_index_of(PROCESS_DB)
        except ValueError:
            return None
        return db_index * SERVICE_NO_STRIDE + self.entry.server_no

    def service(self, name: str, cls: type[_T]) -> _T:
        """Typed accessor for the service bag (F-87).

        ``services: dict[str, object]`` forced every consumer to ``cast()``
        -- an actual type mismatch then failed far away from the lookup, at
        the first attribute use. This accessor checks presence AND type at
        the boundary and returns the narrowed type, so facades lose their
        casts and mis-wirings fail with the service name in the message.
        """
        value = self.services.get(name)
        if value is None:
            raise ServiceNotAvailableError(
                f"service {name!r} is not available in this process (or not yet at this boot stage)"
            )
        if not isinstance(value, cls):
            raise ServiceTypeError(
                f"service {name!r} is {type(value).__qualname__}, expected {cls.__qualname__}"
            )
        return value

    def process_index_of(self, process_type: str) -> int:
        """Index of ``process_type`` among this server's processes (main == 0)."""
        if process_type == PROCESS_MAIN:
            return 0
        subs = list(self.entry.sub_process)
        if process_type not in subs:
            raise ValueError(
                f"unknown process type {process_type!r}; configured sub-processes: "
                f"{[*subs, PROCESS_MAIN]}"
            )
        return subs.index(process_type) + 1
