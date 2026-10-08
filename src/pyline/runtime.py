"""Server runtime: assembles every component along the boot state machine.

This replaces the prototype's launch.py / aiolaunch.py / asyncos.py / flowctrl
wiring with one explicit, testable composition root.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import os
import signal
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from pyline import log as pyline_log
from pyline.config.loader import (
    load_project_settings,
    load_server_registry,
    load_table_defs,
)
from pyline.core.clock import GameClock
from pyline.core.context import PROCESS_MAIN, Context
from pyline.core.events import (
    BaseInitEvent,
    ClientConnectedEvent,
    EventBus,
    FrameInitEvent,
    FuncDoneEvent,
    FuncInitEvent,
    FuncQuitEvent,
    HalfHourEvent,
    NewDayEvent,
    NewHourEvent,
    NewMonthEvent,
    NewWeekEvent,
    NewYearEvent,
)
from pyline.core.lifecycle import LifecycleManager, LifecycleState
from pyline.core.scheduler import Scheduler
from pyline.core.supervisor import ProcessSupervisor
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
)
from pyline.devtools.console import Console
from pyline.devtools.watcher import FileWatcher
from pyline.net import (
    Connection,
    MessageRouter,
    ProtocolGateway,
    ProxyClient,
    ProxyServer,
    RpcManager,
    ZmqBus,
    serve,
)
from pyline.obs import LoopLatencyMonitor
from pyline.reload.inplace import reload_module

logger = logging.getLogger(__name__)

ENV_SERVER_NO = "PYLINE_SERVER"
ENV_UNSAFE_CONSOLE = "PYLINE_UNSAFE_CONSOLE"


class ServerRuntime:
    """Owns all components of one process."""

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.bus = EventBus()
        self.scheduler = Scheduler()
        self.clock = GameClock()
        self.lifecycle = LifecycleManager()
        self.gateway = ProtocolGateway()
        ctx.loop = asyncio.get_running_loop()
        ctx.scheduler = self.scheduler
        ctx.events = self.bus
        ctx.lifecycle = self.lifecycle
        # Filled during boot:
        self.bus_zmq: ZmqBus | None = None
        self.router: MessageRouter | None = None
        self.rpc: RpcManager | None = None
        self.proxy_client: ProxyClient | None = None
        self.proxy_server: ProxyServer | None = None
        self.db: DatabaseAccess | None = None
        self.db_service: DatabaseService | None = None
        self.mysql: MySQLPool | None = None
        self.redis: RedisClient | None = None
        self.save_scheduler = SaveScheduler()
        # Set False by teardown when the shutdown flush deadline passes with
        # dirty savers remaining; the process then exits non-zero (F-01).
        self.save_flush_ok = True
        self.monitor: LoopLatencyMonitor | None = None
        self.console: Console | None = None
        self.watcher: FileWatcher | None = None
        self._client_server: asyncio.AbstractServer | None = None
        self._client_control_rejected = 0
        self._bg_tasks: set[asyncio.Task[object]] = set()

    # ------------------------------------------------------------------ #
    # Boot
    # ------------------------------------------------------------------ #

    async def boot(self) -> None:
        self._register_boot_steps()
        await self.lifecycle.run_boot()

    def _register_boot_steps(self) -> None:
        steps = {
            LifecycleState.LOOP_INIT: self._step_loop_init,
            LifecycleState.FRAME_INIT: self._step_frame_init,
            LifecycleState.CONN_DB: self._step_conn_db,
            LifecycleState.BASE_INIT: self._step_base_init,
            LifecycleState.FUNC_INIT: self._step_func_init,
            LifecycleState.FUNC_DONE: self._step_func_done,
            LifecycleState.OPEN_LOGIN: self._step_open_login,
            LifecycleState.FINISHED: self._step_finished,
        }
        for state, action in steps.items():
            self.lifecycle.on_step(state, action)
        self.lifecycle.on_shutdown(self._shutdown_teardown)

    async def _step_loop_init(self) -> None:
        self.scheduler.bind_loop(asyncio.get_running_loop())
        self._start_clock_events()

    async def _step_frame_init(self) -> None:
        self.bus_zmq = ZmqBus(self.ctx, self.gateway)
        await self.bus_zmq.start()
        self.router = MessageRouter(self.ctx, self.gateway, self.bus_zmq)
        self.proxy_client = ProxyClient(self.ctx, self.router)
        self.router.attach_proxy_client(self.proxy_client)
        if self.ctx.is_main_process and self.ctx.entry.is_proxy:
            self.proxy_server = ProxyServer(self.ctx, self.router)
            await self.proxy_server.start()
            self.router.attach_proxy_server(self.proxy_server)
        if self.ctx.is_main_process:
            await self.proxy_client.start()
        self.rpc = RpcManager(self.gateway, self.router, own_service_no=self.ctx.service_no)
        self.ctx.services["rpc"] = self.rpc
        await self.bus.emit(FrameInitEvent())

    async def _step_conn_db(self) -> None:
        if not self.ctx.entry.use_mysql and not self.ctx.entry.use_redis:
            self.db = DatabaseAccess(remote=self.rpc, db_service_no=self.ctx.db_service_no())
            return
        owns_db = self.ctx.is_db_process or not self.ctx.entry.sub_process
        if owns_db:
            await self._connect_local_db()
        else:
            self.db = DatabaseAccess(remote=self.rpc, db_service_no=self.ctx.db_service_no())

    async def _connect_local_db(self) -> None:
        s = self.ctx.settings
        if self.ctx.entry.use_mysql:
            self.mysql = MySQLPool(s.mysql)
            await self.mysql.connect()
            schema = SchemaManager(self.mysql, self.ctx.tables, s.mysql.db_name)
            await schema.ensure_all()
            self.ctx.services["schema"] = schema
        if self.ctx.entry.use_redis:
            self.redis = RedisClient(s.redis)
            await self.redis.connect()
        self.db_service = DatabaseService(
            self.mysql if self.mysql is not None else NullPool(),
            self.redis if self.redis is not None else NullRedis(),
        )
        assert self.rpc is not None
        self.db_service.expose(self.rpc)
        self.db = DatabaseAccess(local=self.db_service)

    async def _step_base_init(self) -> None:
        if self.ctx.is_db_process:
            return
        await self.bus.emit(BaseInitEvent())

    async def _step_func_init(self) -> None:
        if self.ctx.is_db_process:
            return
        await self.bus.emit(FuncInitEvent())

    async def _step_func_done(self) -> None:
        self.save_scheduler.start()
        self._expose_saver_factory()
        self.monitor = LoopLatencyMonitor()
        self.monitor.start()
        if self.ctx.is_develop:
            self.console = Console(
                self.bus,
                unsafe=os.environ.get(ENV_UNSAFE_CONSOLE, "") == "1",
                reload_hook=reload_module,
                shutdown_hook=lambda reason: asyncio.get_running_loop().create_task(
                    self.shutdown(reason)
                ),
            )
            self.console.start()
            self.watcher = FileWatcher([Path.cwd()])
            self.watcher.start()
        await self.bus.emit(FuncDoneEvent())

    def _expose_saver_factory(self) -> None:
        """Provide a configured DataSaver factory for business code."""
        schema = self.ctx.services.get("schema")
        if isinstance(schema, SchemaManager) and self.db is not None:

            def make_saver(
                table: str,
                column: str,
                key: object,
                codec: object = None,
                **kwargs: object,
            ) -> DataSaver:
                return DataSaver(
                    self.db,  # type: ignore[arg-type]
                    schema,
                    table,
                    column,
                    key,
                    codec=codec,  # type: ignore[arg-type]
                    scheduler=self.save_scheduler,
                    **kwargs,  # type: ignore[arg-type]
                )

            self.ctx.services["make_saver"] = make_saver

    async def _step_open_login(self) -> None:
        if not self.ctx.is_main_process:
            return
        entry = self.ctx.entry
        self._client_server = await serve(
            entry.bind_host(),
            entry.client_listen_port(self.ctx.process_index),
            token=self.ctx.settings.socket.token,
            handshake_timeout=self.ctx.settings.socket.handshake_timeout,
            max_frame=self.ctx.settings.socket.max_frame_size,
            idle_timeout=self.ctx.settings.socket.idle_timeout,
            send_queue_limit=self.ctx.settings.socket.send_queue_limit,
            on_message=self._on_client_frame,
            on_connected=self._on_client_connected,
        )
        logger.info(
            "client listener on %s:%d",
            entry.bind_host(),
            entry.client_listen_port(self.ctx.process_index),
        )

    def _on_client_frame(self, flag: str, payload: bytes) -> None:
        """Client-facing dispatch (F-16): the client network never reaches the
        internal control surface -- ``@``-prefixed flags (@rpc/@fwd/...) are
        reserved for inter-server links and rejected here."""
        if flag.startswith("@"):
            self._client_control_rejected += 1
            logger.warning(
                "client connection sent reserved flag %r (dropped, total=%d)",
                flag,
                self._client_control_rejected,
            )
            return
        self.gateway.dispatch(flag, payload)

    def _on_client_connected(self, conn: Connection) -> None:
        self._spawn(self.bus.emit(ClientConnectedEvent(peer=conn.peer)))

    async def _step_finished(self) -> None:
        logger.info(
            "server %s (%s) up: service_no=%d",
            self.ctx.entry.name,
            self.ctx.process_type,
            self.ctx.service_no,
        )

    # ------------------------------------------------------------------ #
    # Clock events (half-hour / hour / day / week / month / year)
    # ------------------------------------------------------------------ #

    def _start_clock_events(self) -> None:
        def on_boundary() -> None:
            self._emit_clock_events()
            deadline, _ = self.clock.next_halfhour_boundary()
            self.scheduler.call_after(
                max(deadline - self.clock.now(), 0.1), on_boundary, label="clock-boundary"
            )

        deadline, _ = self.clock.next_halfhour_boundary()
        self.scheduler.call_after(
            max(deadline - self.clock.now(), 0.1), on_boundary, label="clock-boundary"
        )

    def _emit_clock_events(self) -> None:
        now = dt.datetime.fromtimestamp(self.clock.now())
        events: list[object] = []
        if now.minute == 0:
            events.append(NewHourEvent(hour=now.hour))
            if now.hour == 0:
                events.append(NewDayEvent(day=now.day))
                if now.day == 1:
                    events.append(NewMonthEvent(month=now.month))
                    if now.month == 1:
                        events.append(NewYearEvent(year=now.year))
                if now.weekday() == 0:
                    events.append(NewWeekEvent(week_day=1))
        elif now.minute == 30:
            events.append(HalfHourEvent(hour=now.hour))
        for event in events:
            self._spawn(self.bus.emit(event))

    def _spawn(self, coro: Coroutine[Any, Any, object]) -> None:
        """Fire-and-forget background work with a kept reference."""
        task = asyncio.get_running_loop().create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #

    async def shutdown(self, reason: str) -> None:
        await self.lifecycle.request_shutdown(reason)

    async def _shutdown_teardown(self) -> None:
        await self.bus.emit(FuncQuitEvent(), reverse=True)
        self.save_flush_ok = await self.save_scheduler.stop()
        if self.console is not None:
            await self.console.stop()
        if self.watcher is not None:
            await self.watcher.stop()
        if self.monitor is not None:
            await self.monitor.stop()
        if self._client_server is not None:
            self._client_server.close()
            await self._client_server.wait_closed()
        if self.proxy_server is not None:
            await self.proxy_server.close()
        if self.proxy_client is not None:
            await self.proxy_client.close()
        if self.bus_zmq is not None:
            await self.bus_zmq.close()
        if self.redis is not None:
            await self.redis.close()
        if self.mysql is not None:
            await self.mysql.close()
        await self.scheduler.close()


def build_context(
    config_dir: Path,
    server_no: int,
    process_type: str,
    process_index: int,
    main_pid: int,
) -> Context:
    """Load all config layers and assemble the runtime context."""
    settings = load_project_settings(config_dir)
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    entry = registry.entry(server_no)
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=entry,
        process_type=process_type,
        process_index=process_index,
        main_pid=main_pid,
    )


async def run_process(ctx: Context, config_dir: Path) -> None:
    """Single-process entry: build runtime, boot, park until shutdown."""
    pyline_log.setup_logging(
        ctx.settings.log,
        process_tag=ctx.process_type,
        run_dir=Path(ctx.settings.log.log_dir),
    )
    runtime = ServerRuntime(ctx)
    boot_task = asyncio.get_running_loop().create_task(runtime.boot())
    _install_signal_handlers(runtime)
    try:
        await boot_task
        while not runtime.lifecycle.in_quit():
            await asyncio.sleep(0.5)
    finally:
        if not runtime.lifecycle.in_quit():
            await runtime.shutdown("main loop exit")
    if not runtime.save_flush_ok:
        raise SystemExit(3)  # dirty data could not be flushed at shutdown


def _install_signal_handlers(runtime: ServerRuntime) -> None:
    loop = asyncio.get_running_loop()

    def handle_signal(sig: signal.Signals) -> None:
        logger.info("received signal %s", sig.name)
        runtime._spawn(runtime.shutdown(f"signal {sig.name}"))

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            # Windows selector loop: no add_signal_handler; KeyboardInterrupt
            # (raised inside asyncio.run) covers SIGINT instead.
            loop.add_signal_handler(sig, handle_signal, sig)


async def child_main(process_type: str, process_index: int) -> None:
    """Entry for sub-processes spawned by ProcessSupervisor."""
    server_no = int(os.environ.get(ENV_SERVER_NO, "0"))
    if not server_no:
        raise SystemExit(f"{ENV_SERVER_NO} not set; cannot start child process")
    config_dir = Path(os.environ.get("PYLINE_CONFIG_DIR", "aioconfig"))
    ctx = build_context(config_dir, server_no, process_type, process_index, os.getppid())
    await run_process(ctx, config_dir)


def main(argv: list[str] | None = None) -> None:
    """Console entry: ``python -m pyline [--config DIR] [--server N]``."""
    import argparse

    from pyline.net.loop_policy import install_loop_policy

    install_loop_policy()

    parser = argparse.ArgumentParser(prog="pyline")
    parser.add_argument("--config", default="aioconfig", help="config directory")
    parser.add_argument("--server", type=int, default=0, help="server number")
    parser.add_argument("--unsafe-console", action="store_true")
    args = parser.parse_args(argv)

    server_no = args.server or int(os.environ.get(ENV_SERVER_NO, "0"))
    if not server_no:
        raise SystemExit(
            f"select a server: --server N or ${ENV_SERVER_NO} (servers are listed in servers.json5)"
        )
    if args.unsafe_console:
        os.environ[ENV_UNSAFE_CONSOLE] = "1"
    os.environ[ENV_SERVER_NO] = str(server_no)
    os.environ.setdefault("PYLINE_CONFIG_DIR", args.config)

    config_dir = Path(args.config)
    registry = load_server_registry(config_dir)
    entry = registry.entry(server_no)
    main_pid = os.getpid()

    # Warm config validation before spawning children (fail fast).
    load_project_settings(config_dir)

    if entry.sub_process:
        supervisor = ProcessSupervisor(child_main)
        supervisor.spawn_subprocesses(entry.sub_process, main_pid)
        main_runtime: ServerRuntime | None = None

        async def on_child_died(process_type: str, exitcode: int | None) -> None:
            if main_runtime is not None:
                await main_runtime.shutdown(f"sub-process {process_type} died ({exitcode})")

        async def main_proc() -> None:
            nonlocal main_runtime
            supervisor.start_child_watch(on_child_died)
            ctx = build_context(config_dir, server_no, PROCESS_MAIN, 0, main_pid)
            pyline_log.setup_logging(
                ctx.settings.log,
                process_tag=PROCESS_MAIN,
                run_dir=Path(ctx.settings.log.log_dir),
            )
            main_runtime = ServerRuntime(ctx)
            await main_runtime.boot()
            while not main_runtime.lifecycle.in_quit():
                await asyncio.sleep(0.5)
            await supervisor.terminate_children()
            if not main_runtime.save_flush_ok:
                raise SystemExit(3)  # dirty data could not be flushed at shutdown

        asyncio.run(main_proc())
    else:
        ctx = build_context(config_dir, server_no, PROCESS_MAIN, 0, main_pid)
        asyncio.run(run_process(ctx, config_dir))


if __name__ == "__main__":
    main()
