"""Collaborators extracted from ServerRuntime (F-45).

The composition root (runtime.py) used to own boot steps, DB topology
decisions, clock-event derivation, the operator surface and the entire
teardown ordering in one ~500-line class -- every new component had to touch
it in three places. Each concern now lives in its own wiring class:

* :class:`ClockEventEmitter` -- the half-hour boundary -> calendar event chain;
* :class:`DbLayer` -- owns-db vs remote-proxy decision and local pool setup;
* :class:`DevtoolsLayer` -- monitor, metrics endpoint, console, file watcher;
* :class:`TeardownPlan` -- the ordered, guarded, deadline-bounded shutdown.

ServerRuntime orchestrates these; behavior is unchanged.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

from pyline import log as pyline_log
from pyline.core.clock import GameClock
from pyline.core.context import Context
from pyline.core.events import (
    EventBus,
    HalfHourEvent,
    NewDayEvent,
    NewHourEvent,
    NewMonthEvent,
    NewWeekEvent,
    NewYearEvent,
)
from pyline.core.scheduler import Scheduler
from pyline.db.autosave import SaveScheduler
from pyline.db.mysql import MySQLPool
from pyline.db.orm import DataSaver
from pyline.db.redis import RedisClient
from pyline.db.schema import SchemaManager
from pyline.db.service import (
    DatabaseAccess,
    DatabaseService,
    NullPool,
    NullRedis,
    PoolLike,
    RedisLike,
)
from pyline.devtools.console import Console
from pyline.devtools.watcher import FileWatcher
from pyline.net.rpc import RpcManager
from pyline.obs import LoopLatencyMonitor
from pyline.obs.metrics import AlarmHub

logger = logging.getLogger(__name__)

ENV_UNSAFE_CONSOLE = "PYLINE_UNSAFE_CONSOLE"

SpawnFn = Callable[[Coroutine[Any, Any, object]], None]


class ClockEventEmitter:
    """Half-hour boundary tick -> calendar events.

    Derives every wall-clock field through the clock itself (F-44) and
    catches up missed boundaries instead of swallowing them (F-25). The
    calendar events themselves are framework-level (business code subscribes
    via the bus), so the chain lives here rather than inside GameClock.
    """

    def __init__(
        self,
        clock: GameClock,
        scheduler: Scheduler,
        bus: EventBus,
        *,
        log_dir: Path,
        spawn: SpawnFn,
    ) -> None:
        self._clock = clock
        self._scheduler = scheduler
        self._bus = bus
        self._spawn = spawn
        self._log_dir = log_dir
        self._last_boundary = 0.0
        self._channel = logging.getLogger("pyline.channel.clock")

    def start(self) -> None:
        self._last_boundary = self._clock.now()
        self._channel = pyline_log.file_logger("clock", self._log_dir)
        self._schedule_next()

    def _schedule_next(self) -> None:
        deadline = self._clock.next_halfhour_after(self._clock.now())
        self._scheduler.call_after(
            max(deadline - self._clock.now(), 0.1), self._on_boundary, label="clock-boundary"
        )

    def _on_boundary(self) -> None:
        self.emit_missed_boundaries()
        self._schedule_next()

    def emit_missed_boundaries(self) -> None:
        """Fire every :00/:30 boundary since the last one emitted."""
        now_ts = self._clock.now()
        cursor = self._last_boundary
        for _ in range(96):  # at most two days of catch-up per tick
            boundary = self._clock.next_halfhour_after(cursor)
            if boundary > now_ts:
                break
            self._fire_boundary_events(boundary)
            cursor = boundary
        self._last_boundary = now_ts

    def _fire_boundary_events(self, ts: float) -> None:
        local = self._clock.local(ts)
        events: list[object] = []
        if local.minute == 0:
            events.append(NewHourEvent(hour=local.hour))
            if local.hour == 0:
                events.append(NewDayEvent(day=self._clock.day_no(ts)))
                if local.day == 1:
                    events.append(NewMonthEvent(month=local.month))
                    if local.month == 1:
                        events.append(NewYearEvent(year=local.year))
                if local.weekday() == 0:
                    events.append(NewWeekEvent(week_no=self._clock.week_no(ts)))
        elif local.minute == 30:
            events.append(HalfHourEvent(hour=local.hour))
        for event in events:
            self._channel.info("event: %s", type(event).__name__)
            self._spawn(self._bus.emit(event))


class DbLayer:
    """CONN_DB wiring: decide who owns the database and build the access.

    A process owns the pools when it is the DB process (or the server has no
    sub-process split); everyone else gets an RPC-backed
    :class:`DatabaseAccess` to the DB process.
    """

    def __init__(self, ctx: Context, alarms: AlarmHub) -> None:
        self._ctx = ctx
        self._alarms = alarms
        self.mysql: MySQLPool | None = None
        self.redis: RedisClient | None = None
        self.db_service: DatabaseService | None = None

    async def connect(self, rpc: RpcManager) -> DatabaseAccess:
        entry = self._ctx.entry
        if not entry.use_mysql and not entry.use_redis:
            return DatabaseAccess(remote=rpc, db_service_no=self._ctx.db_service_no())
        owns_db = self._ctx.is_db_process or not entry.sub_process
        if not owns_db:
            return DatabaseAccess(remote=rpc, db_service_no=self._ctx.db_service_no())
        await self._connect_local(rpc)
        assert self.db_service is not None
        return DatabaseAccess(local=self.db_service)

    async def _connect_local(self, rpc: RpcManager) -> None:
        s = self._ctx.settings
        if self._ctx.entry.use_mysql:
            self.mysql = MySQLPool(
                s.mysql,
                # Pool loss raises the mysql_lost alarm (recovery itself runs
                # inside the pool); the hub is wired from __init__, so this
                # works even though CONN_DB precedes FUNC_DONE.
                on_lost=lambda: self._alarms.emit(
                    "mysql_lost", {"host": s.mysql.host, "port": s.mysql.port}
                ),
            )
            await self.mysql.connect()
            schema = SchemaManager(self.mysql, self._ctx.tables, s.mysql.db_name)
            await schema.ensure_all()
            self._ctx.services["schema"] = schema
        if self._ctx.entry.use_redis:
            self.redis = RedisClient(s.redis)
            await self.redis.connect()
        pool: PoolLike = self.mysql if self.mysql is not None else NullPool()
        redis: RedisLike = self.redis if self.redis is not None else NullRedis()
        self.db_service = DatabaseService(pool, redis)
        self.db_service.expose(rpc)

    def saver_factory(
        self, access: DatabaseAccess, scheduler: SaveScheduler
    ) -> Callable[..., DataSaver] | None:
        """Configured DataSaver factory for business code, or None when this
        process has no local schema to validate savers against."""
        schema = self._ctx.services.get("schema")
        if not isinstance(schema, SchemaManager):
            return None

        def make_saver(
            table: str,
            column: str,
            key: object,
            codec: object = None,
            **kwargs: object,
        ) -> DataSaver:
            return DataSaver(
                access,
                schema,
                table,
                column,
                key,
                codec=codec,  # type: ignore[arg-type]
                scheduler=scheduler,
                **kwargs,  # type: ignore[arg-type]
            )

        return make_saver


class DevtoolsLayer:
    """Operator surface: loop monitor, metrics endpoint, console, watcher."""

    def __init__(self, ctx: Context, alarms: AlarmHub, bus: EventBus) -> None:
        self._ctx = ctx
        self._alarms = alarms
        self._bus = bus
        self.monitor: LoopLatencyMonitor | None = None
        self.console: Console | None = None
        self.watcher: FileWatcher | None = None

    def start(
        self, *, reload_hook: Callable[[str], object], shutdown_hook: Callable[[str], object]
    ) -> None:
        self.monitor = LoopLatencyMonitor(
            on_alert=lambda delay: self._alarms.emit("loop_latency", {"delay": delay})
        )
        self.monitor.start()
        self._start_metrics_server()
        if not self._ctx.is_develop:
            return
        # Terminal console on the main process only (F-27): sub-processes
        # share one stdin and would fight over it.
        if self._ctx.is_main_process:
            self.console = Console(
                bus=self._bus,
                unsafe=os.environ.get(ENV_UNSAFE_CONSOLE, "") == "1",
                reload_hook=reload_hook,
                shutdown_hook=shutdown_hook,
            )
            self.console.start()
        self.watcher = FileWatcher([Path.cwd()])
        self.watcher.start()

    def _start_metrics_server(self) -> None:
        """Export Prometheus metrics on the main process (F-28)."""
        port = self._ctx.settings.metrics_port
        if port is None or not self._ctx.is_main_process:
            return
        from prometheus_client import start_http_server

        start_http_server(port)
        logger.info("prometheus metrics on :%d", port)


class TeardownPlan:
    """Ordered, guarded shutdown steps under one total deadline (F-45).

    A failure in one step logs and continues so the rest always get their
    chance (F-20); the caller bounds the whole plan with ``shutdown_timeout``
    and interprets the timeout (the flush bookkeeping stays with the runtime).
    """

    def __init__(self) -> None:
        self._steps: list[tuple[str, Callable[[], Awaitable[object]]]] = []

    def add(self, name: str, step: Callable[[], Awaitable[object]]) -> None:
        self._steps.append((name, step))

    async def run(self) -> None:
        for name, step in self._steps:
            try:
                await step()
            except Exception:
                logger.exception("shutdown step %r failed (continuing)", name)


__all__ = [
    "ENV_UNSAFE_CONSOLE",
    "ClockEventEmitter",
    "DbLayer",
    "DevtoolsLayer",
    "TeardownPlan",
]
