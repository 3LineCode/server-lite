"""Collaborators extracted from ServerRuntime (F-45).

The composition root (runtime.py) used to own boot steps, DB topology
decisions, clock-event derivation, the operator surface and the entire
teardown ordering in one ~500-line class -- every new component had to touch
it in three places. Each concern now lives in its own wiring class:

* :class:`ClockEventEmitter` -- the half-hour boundary -> calendar event chain;
* :class:`DbLayer` -- owns-db vs remote-proxy decision and local pool setup;
* :class:`DevtoolsLayer` -- monitor, metrics endpoint, console, file watcher;
* :class:`TeardownPlan` -- the ordered, guarded, deadline-bounded shutdown
  with per-step budgets (F-95).

ServerRuntime orchestrates these.
"""

from __future__ import annotations

import asyncio
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
from pyline.db.schema import SchemaManager, TableCatalog, TableProvider
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

# Returning the created task (so callers can keep references) is allowed;
# None-returning spawners stay compatible.
SpawnFn = Callable[[Coroutine[Any, Any, object]], object]


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
        alarms: AlarmHub | None = None,
        log_rotation_mb: int = 64,
    ) -> None:
        self._clock = clock
        self._scheduler = scheduler
        self._bus = bus
        self._spawn = spawn
        self._log_dir = log_dir
        self._alarms = alarms
        self._log_rotation_mb = log_rotation_mb
        self._last_boundary = 0.0
        self._channel = logging.getLogger("pyline.channel.clock")

    def start(self) -> None:
        self._last_boundary = self._clock.now()
        # Honour the configured log.rotation_mb: the clock channel used to
        # silently pin the 64 MB default while every other channel followed
        # the setting.
        self._channel = pyline_log.file_logger(
            "clock", self._log_dir, rotation_mb=self._log_rotation_mb
        )
        self._schedule_next()

    def _schedule_next(self) -> None:
        deadline = self._clock.next_halfhour_after(self._clock.now())
        self._scheduler.call_after(
            max(deadline - self._clock.now(), 0.1), self._on_boundary, label="clock-boundary"
        )

    def _on_boundary(self) -> None:
        # F-164: the boundary chain is one coroutine the scheduler awaits.
        # Per-event fire-and-forget spawns started in creation order but
        # interleaved at the first await -- an async NewHour handler could
        # run AFTER the NewDay/NewWeek handlers behind it, while the serial
        # bus contract promises NewHour -> NewDay -> NewMonth -> NewYear ->
        # NewWeek. Awaiting the chain also serializes catch-up boundaries
        # against each other (a skipped-boundary drain used to spawn one
        # interleaving task per boundary).
        self._spawn(self._on_boundary_async())

    async def _on_boundary_async(self) -> None:
        await self.emit_missed_boundaries()
        self._schedule_next()

    async def emit_missed_boundaries(self) -> None:
        """Fire every :00/:30 boundary since the last one emitted.

        More than 96 missed boundaries (two days of downtime) cannot be
        drained -- catching up would only match real time. The surplus is
        skipped, and the skip is alarmed instead of silent (F-52): daily-reset
        logic that missed its :NewDayEvent needs an operator to notice.
        """
        now_ts = self._clock.now()
        cursor = self._last_boundary
        for _ in range(96):  # at most two days of catch-up per tick
            boundary = self._clock.next_halfhour_after(cursor)
            if boundary > now_ts:
                break
            await self._fire_boundary_events(boundary)
            cursor = boundary
        # Arithmetic, not one iteration per boundary: a multi-year clock jump
        # (debug push_time) used to iterate ~48 times per jumped day, each
        # call enumerating ~50 wall-grid candidates -- a synchronous scheduler
        # callback stalled for hundreds of milliseconds. The estimate can
        # drift by a boundary per DST transition inside the window; it feeds
        # an operator alarm, not persisted state.
        last = self._clock.next_halfhour_after(now_ts - 1800)
        skipped = max(1, round((last - cursor) / 1800)) if last > cursor else 0
        if skipped:
            logger.error(
                "clock catch-up cap exceeded: %d half-hour boundary events skipped "
                "(downtime over two days); calendar resets inside the skipped "
                "window did NOT fire",
                skipped,
            )
            if self._alarms is not None:
                self._alarms.emit("clock_boundaries_skipped", {"skipped": skipped})
        # F-98: advance monotonically. A debug SetTime that moves the clock
        # BACKWARD used to drag _last_boundary down with it, so after the
        # debug offset was restored every boundary inside the setback window
        # (NewDayEvent included) fired a second time. max() keeps the
        # high-water mark: a setback emits nothing, and the restore resumes
        # exactly where the pre-setback stream stopped. The F-52 catch-up cap
        # above still sees the true cursor and is unaffected.
        self._last_boundary = max(self._last_boundary, now_ts)

    async def _fire_boundary_events(self, ts: float) -> None:
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
            # F-164: awaited in order, NOT one spawn per event -- spawned
            # tasks start in creation order but interleave at the first
            # await, so an async NewHour handler could run after the
            # NewDay/NewWeek handlers behind it while the serial bus
            # contract promises NewHour -> NewDay -> NewMonth -> NewYear
            # -> NewWeek (midnight-reset logic depends on it).
            await self._bus.emit(event)


class DbLayer:
    """CONN_DB wiring: decide who owns the database and build the access.

    A process owns the pools when it is the DB process (or the server has no
    sub-process split); everyone else gets an RPC-backed
    :class:`DatabaseAccess` to the DB process. Savers work in BOTH topologies
    (F-156): the owning process validates them against its ``SchemaManager``,
    remote business processes against a pool-less :class:`TableCatalog` built
    from the same tables config -- the DB process skips business init, so
    before F-156 the saver factory existed only in the one process that runs
    no business code.
    """

    def __init__(self, ctx: Context, alarms: AlarmHub) -> None:
        self._ctx = ctx
        self._alarms = alarms
        self.mysql: MySQLPool | None = None
        self.redis: RedisClient | None = None
        self.db_service: DatabaseService | None = None
        # F-156: spec source for saver construction -- the full SchemaManager
        # where this process owns the pools, a TableCatalog otherwise.
        self._specs: TableProvider | None = None

    async def connect(self, rpc: RpcManager) -> DatabaseAccess:
        entry = self._ctx.entry
        if not entry.use_mysql and not entry.use_redis:
            return self._remote_access(rpc)
        owns_db = self._ctx.is_db_process or not entry.sub_process
        if not owns_db:
            # F-156: a business process with no local pool still gets savers:
            # the catalog validates (table, column) against the same tables
            # config the DB process runs DDL from, and every statement rides
            # DatabaseAccess -> RPC -> the DB process.
            if entry.use_mysql:
                self._specs = TableCatalog(self._ctx.tables)
            return self._remote_access(rpc)
        await self._connect_local(rpc)
        assert self.db_service is not None
        return DatabaseAccess(local=self.db_service)

    def _remote_access(self, rpc: RpcManager) -> DatabaseAccess:
        """RPC-backed access to the db process (F-92).

        ``Context.db_service_no()`` returns ``None`` when the topology has no
        db sub-process; ``DatabaseAccess`` accepts that and only fails (with
        a loud ConfigError) if the database is actually touched -- the boot
        of a ``use_mysql=false use_redis=false`` pure-gateway server used to
        crash here instead."""
        return DatabaseAccess(remote=rpc, db_service_no=self._ctx.db_service_no())

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
            # The versioned-migration engine is reachable from production:
            # ``migrations_dir`` used to be a constructor-only parameter with
            # no config surface and no production call site, so no .sql script
            # could ever run and only table creation / drift detection worked.
            migrations_dir = Path(s.mysql.migrations_dir) if s.mysql.migrations_dir else None
            if migrations_dir is None:
                logger.info(
                    "mysql.migrations_dir not set; versioned migrations disabled "
                    "(table creation and drift detection still run)"
                )
            schema = SchemaManager(
                self.mysql, self._ctx.tables, s.mysql.db_name, migrations_dir=migrations_dir
            )
            await schema.ensure_all()
            self._ctx.services["schema"] = schema
            self._specs = schema
        if self._ctx.entry.use_redis:
            self.redis = RedisClient(s.redis)
            await self.redis.connect()
        pool: PoolLike = self.mysql if self.mysql is not None else NullPool()
        redis: RedisLike = self.redis if self.redis is not None else NullRedis()
        self.db_service = DatabaseService(pool, redis)
        self.db_service.expose(rpc)
        # The periodic TTL sweep frees abandoned remote-transaction sessions
        # even when the DB process is otherwise idle (lazy-only reaping used
        # to pin them -- and their locks -- until shutdown).
        self.db_service.start()

    def saver_factory(
        self, access: DatabaseAccess, scheduler: SaveScheduler
    ) -> Callable[..., DataSaver] | None:
        """Configured DataSaver factory for business code, or None when this
        process has no MySQL spec source at all (mysql disabled on a server
        with no db topology)."""
        specs = self._specs
        if specs is None:
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
                specs,
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
        # F-99: keep the server object alive and reachable; an untracked
        # ThreadingHTTPServer could never be closed and only died with the
        # process (its daemon thread silently holding the port).
        self.metrics_server: object | None = None

    def start(
        self,
        *,
        reload_hook: Callable[[str], Awaitable[None]],
        shutdown_hook: Callable[[str], object],
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
        # F-96: watch the BUSINESS package root, not the whole cwd. With the
        # repo root under watch, saving any tests/*.py or build script
        # auto-imported and executed its side effects inside the live server.
        self.watcher = FileWatcher(
            self._watch_roots(),
            reload_hook=reload_hook,
            # F-97: a reload that re-raises SystemExit must route into a
            # graceful shutdown instead of killing the watcher task.
            shutdown_hook=shutdown_hook,
        )
        self.watcher.start()

    def _watch_roots(self) -> list[Path]:
        """Directory roots the file watcher observes (F-96).

        The business module (PYLINE_EVENTS, default ``game.events``) names the
        business package; its filesystem root is the only tree whose modules
        the server may hot-reload. If the package cannot be located (a
        business module that is not part of a package), fall back to cwd with
        the extended ignore set (FileWatcher's own defaults cover tests/,
        build/, .venv/, ...)."""
        import importlib.util

        module_name = os.environ.get("PYLINE_EVENTS", "game.events")
        top_level = module_name.split(".")[0]
        try:
            spec = importlib.util.find_spec(top_level)
        except (ImportError, ValueError):
            spec = None
        if spec is not None and spec.submodule_search_locations:
            return [Path(next(iter(spec.submodule_search_locations)))]
        logger.warning(
            "business package %r not importable; file watcher falls back to "
            "cwd (changes outside the business tree are filtered by the "
            "ignore set)",
            top_level,
        )
        return [Path.cwd()]

    def _start_metrics_server(self) -> None:
        """Export Prometheus metrics (F-28, F-54).

        The main process binds ``metrics_port``. Sub-processes bind
        ``metrics_port + process_index`` when ``metrics_all_processes`` is set
        -- without it, sub-process autosave/RPC/loop metrics exist in-process
        but have no exporter, i.e. they are invisible to Prometheus (the
        documented "metrics gap" decision in docs/deployment.md; the per-
        process ports are its recommended resolution). Binds loopback by
        default; an optional bearer token (``metrics_token``) guards scrapes.
        """
        settings = self._ctx.settings
        port = settings.metrics_port
        if port is None:
            return
        if self._ctx.is_main_process:
            bind_port = port
        elif settings.metrics_all_processes:
            bind_port = port + self._ctx.process_index
        else:
            return
        token = settings.metrics_token
        self.metrics_server = start_metrics_endpoint(
            settings.metrics_bind,
            bind_port,
            token.get_secret_value() if token is not None else None,
        )

    async def stop_metrics(self) -> None:
        """Explicitly close the metrics endpoint (F-99): stop accepting,
        join the serve_forever loop and release the port instead of leaking
        the socket until process exit."""
        server, self.metrics_server = self.metrics_server, None
        if server is None:
            return
        shutdown = getattr(server, "shutdown", None)
        close = getattr(server, "server_close", None)
        if shutdown is not None:
            shutdown()
        if close is not None:
            close()


def start_metrics_endpoint(bind: str, port: int, token: str | None) -> object:
    """Serve Prometheus metrics on ``bind:port`` (F-54).

    With ``token``, every request must carry ``Authorization: Bearer <token>``
    (constant-time compared) or is answered with 401.
    """
    import hmac
    import threading
    from http.server import ThreadingHTTPServer

    from prometheus_client.exposition import MetricsHandler

    class _AuthMetricsHandler(MetricsHandler):
        def do_GET(self) -> None:
            if token is not None:
                supplied = self.headers.get("Authorization", "")
                # Bytes, not str: hmac.compare_digest raises TypeError on
                # non-ASCII str operands, so a non-ASCII token (or header)
                # used to 500 every scrape instead of answering 401.
                supplied_b = supplied.encode("utf-8")
                expected_b = f"Bearer {token}".encode()
                if not hmac.compare_digest(supplied_b, expected_b):
                    self.send_response(401)
                    self.end_headers()
                    return
            super().do_GET()

    server: ThreadingHTTPServer = ThreadingHTTPServer((bind, port), _AuthMetricsHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="pyline-metrics").start()
    if token is not None:
        logger.info("prometheus metrics on %s:%d (bearer token required)", bind, port)
    else:
        logger.info("prometheus metrics on %s:%d (no auth)", bind, port)
    return server


class TeardownPlan:
    """Ordered, guarded shutdown steps under one total deadline (F-45).

    A failure in one step logs and continues so the rest always get their
    chance (F-20). F-95: every step ALSO gets its own budget --
    ``min(step_timeout, remaining total)`` -- so a single hung step can no
    longer consume the entire ``total_timeout`` and silently skip everything
    behind it (mysql.close included). A per-step timeout is logged as
    critical (data-loss class event) but the plan moves on.
    """

    def __init__(
        self,
        *,
        total_timeout: float | None = None,
        step_timeout: float | None = 15.0,
    ) -> None:
        self._steps: list[tuple[str, Callable[[], Awaitable[object]], float | None]] = []
        self._total_timeout = total_timeout
        self._step_timeout = step_timeout

    def add(
        self, name: str, step: Callable[[], Awaitable[object]], *, timeout: float | None = None
    ) -> None:
        """Append ``step``; ``timeout`` overrides the per-step cap (use it for
        the one step the whole budget exists for, e.g. the save-flush: it
        then only shares the remaining total instead of a fixed slice)."""
        self._steps.append((name, step, timeout))

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._total_timeout if self._total_timeout is not None else None
        for index, (name, step, step_override) in enumerate(self._steps):
            budget = step_override if step_override is not None else self._step_timeout
            if deadline is not None:
                remaining = deadline - loop.time()
                budget = remaining if budget is None else min(budget, remaining)
                if budget <= 0:
                    logger.critical(
                        "shutdown total budget exhausted before step %r; %d step(s) skipped",
                        name,
                        len(self._steps) - index,
                    )
                    return
            try:
                if budget is None:
                    await step()
                else:
                    await asyncio.wait_for(step(), timeout=budget)
            except TimeoutError:
                # F-95: the step hung; it was cancelled and the plan moves on
                # so the remaining steps still run.
                logger.critical(
                    "shutdown step %r timed out after %.1fs (step cancelled; continuing)",
                    name,
                    budget,
                )
            except Exception:
                logger.exception("shutdown step %r failed (continuing)", name)


__all__ = [
    "ENV_UNSAFE_CONSOLE",
    "ClockEventEmitter",
    "DbLayer",
    "DevtoolsLayer",
    "TeardownPlan",
]
