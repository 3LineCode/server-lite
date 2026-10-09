"""Server runtime: assembles every component along the boot state machine.

This replaces the prototype's launch.py / aiolaunch.py / asyncos.py / flowctrl
wiring with one explicit, testable composition root. The heavy machineries
live in :mod:`pyline.runtime_wiring` (F-45): clock-event derivation, the DB
topology layer, the operator surface, and the declarative teardown plan;
ServerRuntime itself only orchestrates boot steps, business loading, the
client listener and reload-and-rebind.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import os
import signal
from collections.abc import Coroutine
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfoNotFoundError

from pyline import api as pyline_api
from pyline import log as pyline_log
from pyline.config.errors import ConfigError
from pyline.config.loader import (
    load_project_settings,
    load_server_registry,
    load_table_defs,
)
from pyline.core.clock import GameClock
from pyline.core.context import PROCESS_MAIN, SERVICE_NO_STRIDE, Context
from pyline.core.events import (
    BaseInitEvent,
    ClientConnectedEvent,
    ClientDisconnectedEvent,
    EnvReadyEvent,
    EventBus,
    FrameInitEvent,
    FuncDoneEvent,
    FuncInitEvent,
    FuncQuitEvent,
    OnReloadEvent,
    PreReloadEvent,
)
from pyline.core.lifecycle import LifecycleManager, LifecycleState
from pyline.core.scheduler import Scheduler
from pyline.core.supervisor import ProcessSupervisor
from pyline.db.autosave import SaveScheduler
from pyline.db.service import DatabaseAccess
from pyline.net import (
    Connection,
    MessageRouter,
    ProtocolGateway,
    ProxyClient,
    ProxyServer,
    RpcManager,
    ZmqBus,
    close_server,
    serve,
)
from pyline.net.session import ClientSessionRegistry
from pyline.net.tls import build_server_context
from pyline.obs.metrics import AlarmHub, get_metrics
from pyline.reload.inplace import reload_module
from pyline.runtime_wiring import (
    ENV_UNSAFE_CONSOLE,
    ClockEventEmitter,
    DbLayer,
    DevtoolsLayer,
    TeardownPlan,
)

logger = logging.getLogger(__name__)

ENV_SERVER_NO = "PYLINE_SERVER"


class ServerRuntime:
    """Owns all components of one process."""

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.bus = EventBus()
        self.scheduler = Scheduler()
        self.clock = self._build_clock(ctx.settings.clock.tz)
        self.lifecycle = LifecycleManager(step_timeout=ctx.settings.boot_step_timeout)
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
        self.alarms = AlarmHub()
        # F-94: clock and alarms must be service-visible from the earliest
        # hooks -- EnvReadyEvent fires before the first boot step and
        # BaseInitEvent/FuncInitEvent handlers legitimately call
        # api.service("clock") / ("alarms"). Registering them only at
        # FUNC_DONE used to raise ApiServiceUnavailableError inside those
        # handlers, i.e. before business code ever got a working facade.
        ctx.services["clock"] = self.clock
        ctx.services["alarms"] = self.alarms
        # F-45 wiring collaborators (clock chain, DB topology, operator
        # surface); ServerRuntime orchestrates them.
        self.db_layer = DbLayer(ctx, self.alarms)
        self.devtools = DevtoolsLayer(ctx, self.alarms, self.bus)
        self.clock_events = ClockEventEmitter(
            self.clock,
            self.scheduler,
            self.bus,
            log_dir=Path(ctx.settings.log.log_dir),
            spawn=self._spawn,
            alarms=self.alarms,
            log_rotation_mb=ctx.settings.log.rotation_mb,
        )
        self.save_scheduler = SaveScheduler(
            on_alarm=lambda kind, payload: self.alarms.emit(kind, payload)
        )  # queue-depth alarm (F-42) rides the same hub; thresholds stay at
        # scheduler defaults until a deployment needs to tune them
        # Set False by teardown when the shutdown flush deadline passes with
        # dirty savers remaining; the process then exits non-zero (F-01).
        self.save_flush_ok = True
        self._flush_completed = False
        self.shutdown_timeout = 90.0
        # F-95: per-step cap inside the total budget so one hung teardown
        # step cannot eat the whole ``shutdown_timeout`` and starve the
        # remaining ones (mysql.close included).
        self.shutdown_step_timeout = 15.0
        self._client_server: asyncio.AbstractServer | None = None
        self._client_control_rejected = 0
        self._bg_tasks: set[asyncio.Task[object]] = set()
        # F-225: client session registry -- conn_id -> live Connection. The
        # gauge rides register/unregister; the disconnect event fires from
        # each connection's close hook (exactly once per connection).
        self.sessions = ClientSessionRegistry()
        ctx.services["client_sessions"] = self.sessions
        # F-196: boot() registers the shutdown hook by APPENDING; a second
        # boot() used to run the whole teardown plan twice.
        self._boot_called = False

    # ------------------------------------------------------------------ #
    # Boot
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_clock(tz: str) -> GameClock:
        """F-100: wire the game-calendar timezone from ``settings.clock.tz``.

        Empty means host-local time (the frozen default the persisted
        day/week numbering anchors were defined against). An unknown zone
        must fail the boot before a single calendar number is derived under
        a wrong timezone."""
        if not tz:
            return GameClock()
        try:
            return GameClock(tz=tz)
        except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
            raise ConfigError(f"clock.tz {tz!r} is not a valid timezone: {exc}") from exc

    async def boot(self) -> None:
        # F-196: the shutdown hook list is appended to in _register_boot_steps;
        # a second boot() (programmer error, a re-hosting embedder) used to
        # double every hook -- the whole teardown plan ran twice.
        if self._boot_called:
            raise RuntimeError(
                "ServerRuntime.boot() called twice; build a new ServerRuntime "
                "for a second boot (shutdown hooks are not idempotent)"
            )
        self._boot_called = True
        self._register_boot_steps()
        await self.bus.emit(EnvReadyEvent())  # F-24: pre-boot hook point
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
        self.clock_events.start()

    async def _step_frame_init(self) -> None:
        # F-197: the DB process runs no business code (DbLayer's documented
        # contract) -- importing the business package there used to execute
        # its module side effects and register event handlers in the one
        # process that exists to serve RPC, and develop mode even gave it a
        # file watcher for modules it never dispatches.
        if not self.ctx.is_db_process:
            self._load_business()
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
        self.rpc = RpcManager(
            self.gateway,
            self.router,
            own_service_no=self.ctx.service_no,
            # Inbound CALL storm bound (F-19): authenticated peers must not
            # stack unbounded execution tasks.
            max_inflight=self.ctx.settings.socket.rpc_max_inflight,
            inflight_wait=self.ctx.settings.socket.rpc_inflight_wait,
        )
        self.ctx.services["rpc"] = self.rpc
        # F-220: a plaintext bus in production is authenticated but readable
        # by anything on the path; the trust model says configure CURVE -- a
        # deployment that forgot used to come up quietly exposed.
        if self.ctx.settings.srv_type == "production" and self.ctx.settings.zeromq.curve is None:
            logger.warning(
                "zeromq.curve is not configured: the ZMQ bus runs AUTHENTICATED "
                "but PLAINTEXT in production (see docs/deployment.md, trust model)"
            )
        if not self.ctx.is_db_process:
            await self.bus.emit(FrameInitEvent())

    async def _step_conn_db(self) -> None:
        assert self.rpc is not None  # FRAME_INIT runs before CONN_DB
        self.db = await self.db_layer.connect(self.rpc)
        # F-94: BaseInitEvent handlers run in the very next step and may
        # touch the DB facade or create savers; registration used to wait
        # for FUNC_DONE, so the first business hook that called
        # api.service("db") / make_saver blew up with
        # ApiServiceUnavailableError.
        self.ctx.services["db"] = self.db
        self._expose_saver_factory()

    async def _step_base_init(self) -> None:
        if self.ctx.is_db_process:
            return
        await self.bus.emit(BaseInitEvent())

    async def _step_func_init(self) -> None:
        if self.ctx.is_db_process:
            return
        await self.bus.emit(FuncInitEvent())

    def _load_business(self) -> None:
        """Import the business events module and let it subscribe (the
        prototype's sys.path-injected script/events flow, now explicit)."""
        module_name = os.environ.get("PYLINE_EVENTS", "game.events")
        try:
            business = importlib.import_module(module_name)
        except ImportError as exc:
            # A typo'd module used to boot a business-less server that looked
            # perfectly healthy; a server without its handlers must fail boot.
            logger.error("business module %r failed to import: %s", module_name, exc)
            raise ConfigError(f"business module {module_name!r} failed to import: {exc}") from exc
        register = getattr(business, "register", None)
        if register is not None:
            register(self.bus)
            logger.info("business module %s registered", module_name)

    async def _step_func_done(self) -> None:
        # F-197: no business code lives in the DB process, so there is
        # nothing to save-schedule, watch or notify -- starting the console/
        # watcher there used to race the operator surface with a process
        # nobody operates.
        if self.ctx.is_db_process:
            return
        self.save_scheduler.start()
        self.devtools.start(
            reload_hook=self._reload_and_rebind,
            # _spawn keeps the strong reference + observes failures; a bare
            # create_task here could be garbage-collected mid-shutdown.
            shutdown_hook=lambda reason: self._spawn(self.shutdown(reason)),
        )
        await self.bus.emit(FuncDoneEvent())

    def _expose_saver_factory(self) -> None:
        """Provide a configured DataSaver factory for business code."""
        if self.db is None:
            return
        make_saver = self.db_layer.saver_factory(self.db, self.save_scheduler)
        if make_saver is not None:
            self.ctx.services["make_saver"] = make_saver

    async def _step_open_login(self) -> None:
        if not self.ctx.is_main_process:
            return
        entry = self.ctx.entry
        # F-187: TLS on the client listener (server certificate; game
        # clients are authenticated by the in-tunnel HMAC handshake).
        tls = self.ctx.settings.socket.tls
        ssl_context = build_server_context(tls) if tls is not None else None
        if ssl_context is None and self.ctx.settings.srv_type == "production":
            # F-220: same reasoning as the bus warning above -- the listener
            # is authenticated but plaintext; say so instead of letting a
            # deployment discover it from a packet capture.
            logger.warning(
                "socket.tls is not configured: the client listener runs AUTHENTICATED "
                "but PLAINTEXT in production (see docs/deployment.md, trust model)"
            )
        self._client_server = await serve(
            entry.bind_host(),
            entry.client_listen_port(self.ctx.process_index),
            token=self.ctx.settings.socket.token.get_secret_value(),
            handshake_timeout=self.ctx.settings.socket.handshake_timeout,
            max_frame=self.ctx.settings.socket.max_frame_size,
            preauth_max_frame=self.ctx.settings.socket.preauth_max_frame,
            idle_timeout=self.ctx.settings.socket.idle_timeout,
            send_queue_limit=self.ctx.settings.socket.send_queue_limit,
            send_queue_bytes=self.ctx.settings.socket.send_queue_bytes,
            max_connections=self.ctx.settings.socket.max_connections,
            max_connections_per_ip=self.ctx.settings.socket.max_connections_per_ip,
            max_inflight_per_connection=self.ctx.settings.socket.max_inflight_per_connection,
            ssl_context=ssl_context,
            on_message=self._on_client_frame,
            on_connected=self._on_client_connected,
            on_disconnected=self._on_client_disconnected,
        )
        logger.info(
            "client listener on %s:%d (tls=%s)",
            entry.bind_host(),
            entry.client_listen_port(self.ctx.process_index),
            "on" if ssl_context is not None else "off",
        )

    async def _reload_and_rebind(self, module_name: str) -> None:
        """Reload one module and rebind its network handlers (F-91).

        ``PreReloadEvent`` handlers must FINISH before the code swap: business
        code subscribes to it to quiesce traffic or serialize state ahead of
        the new code taking effect. The old fire-and-forget spawn let
        ``reload_module`` run first in practice (observed order was
        reload-executed -> Pre handlers -> On handlers), which made the
        contract in docs/hot-reload.md impossible to honour. The bus isolates
        handler failures, so awaiting the emit cannot abort the reload."""
        import sys

        module = sys.modules.get(module_name)
        if module is not None:
            await self.bus.emit(PreReloadEvent(module=module))
        reload_module(module_name)
        self.gateway.rebind_module(module_name)
        if module is not None:
            # OnReload is a post-swap notification (restart tasks, refresh
            # caches): fire-and-forget keeps the reload chain synchronous,
            # matching the pre-F-91 behaviour for this half of the contract.
            self._spawn(self.bus.emit(OnReloadEvent(module=module)))

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
        # F-225: registry + id assignment happen synchronously in the
        # handshake's verified hook, so the very first frame from this
        # connection can already be answered with session.send(conn_id).
        conn_id = self.sessions.register(conn)
        get_metrics().client_sessions.set(self.sessions.count())
        self._spawn(self.bus.emit(ClientConnectedEvent(peer=conn.peer, conn_id=conn_id)))

    def _on_client_disconnected(self, conn: Connection) -> None:
        # F-225: runs from the connection's close hook (exactly once). The
        # event is the framework half of connection<->player bookkeeping:
        # business handlers drop their mapping here; the registry entry is
        # gone before the event fires, so a racing send() fails cleanly.
        self.sessions.unregister(conn)
        get_metrics().client_sessions.set(self.sessions.count())
        self._spawn(
            self.bus.emit(
                ClientDisconnectedEvent(
                    peer=conn.peer, conn_id=conn.conn_id, reason=conn.close_reason
                )
            )
        )

    async def _step_finished(self) -> None:
        logger.info(
            "server %s (%s) up: service_no=%d",
            self.ctx.entry.name,
            self.ctx.process_type,
            self.ctx.service_no,
        )

    def _spawn(self, coro: Coroutine[Any, Any, object]) -> asyncio.Task[object]:
        """Fire-and-forget background work with a kept reference.

        The done-callback logs unexpected exceptions: a silently dead
        background task (the shutdown teardown included) is exactly the kind
        of failure this project refuses to ship."""
        task = asyncio.get_running_loop().create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_task_done)
        return task

    def _bg_task_done(self, task: asyncio.Task[object]) -> None:
        self._bg_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:  # pragma: no cover - race with cancel
            return
        if exc is not None:
            logger.error("background task %r failed: %r", task, exc)

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #

    async def shutdown(self, reason: str) -> None:
        await self.lifecycle.request_shutdown(reason)

    async def _shutdown_teardown(self) -> None:
        """Tear down in order; every step is guarded so a failure in one
        cannot skip the rest (F-20: mysql.close must always get its chance),
        each step gets its own slice of the total budget (F-95: one hung step
        must not starve the remaining ones), and the whole sequence is
        bounded by ``shutdown_timeout``.

        Ordering (F-93):

        1. ``func-quit`` -- business handlers run first (reverse layer order):
           this is where business cancels its own timers/tasks and stops
           producing new work.
        2. every ingress (client listener, proxy links) -- frames arriving
           AFTER the save-flush snapshot would mutate data that never
           re-flushes; on a rolling restart that is silent data loss.
        3. watcher + console -- operator-triggered reloads/commands are the
           remaining mutation sources during the flush window; a mid-flush
           code swap or console command can dirty savers after their snapshot.
        4. ``save-flush`` -- the dirty-saver drain. It frequently goes over
           RPC -> zmq bus -> db process, so the bus (and the db-side pools)
           must stay alive until it completes; this is the one step the whole
           ``shutdown_timeout`` exists for, so it is exempt from the per-step
           cap (F-95) and simply gets the remaining total budget.
        5. monitor + metrics endpoint -- read-only observability; keep them
           up through the flush so the final scrape sees the drain.
        6. zmq bus, db-service, redis, mysql -- the DB path closes only
           after the flush that uses it (mysql.close still always runs:
           F-20). db-service rolls back the remote-transaction sessions on
           their out-of-pool connections (F-101), which the pool close
           cannot reach.
        7. scheduler last -- mirrors boot (loop bound first, released last);
           business timers were the caller's responsibility to cancel at
           func-quit (step 1)."""
        # F-195: a shutdown request that landed while a boot step action was
        # still inside an await used to build this plan against half-built
        # state -- a listener/pool the step assigns after its await returns
        # never entered the plan and leaked into asyncio.run's backstop.
        # Let the in-flight action finish assigning first (bounded; it has
        # its own step_timeout).
        await self.lifecycle.wait_boot_step_settled(self.shutdown_step_timeout)
        plan = TeardownPlan(
            total_timeout=self.shutdown_timeout, step_timeout=self.shutdown_step_timeout
        )

        async def flush_step() -> None:
            flush_ok = await self.save_scheduler.stop()
            self._flush_completed = True
            self.save_flush_ok = flush_ok

        plan.add("func-quit", lambda: self.bus.emit(FuncQuitEvent(), reverse=True))
        if self._client_server is not None:
            server, self._client_server = self._client_server, None
            plan.add("client-listener", lambda: close_server(server))
        if self.proxy_server is not None:
            plan.add("proxy-server", self.proxy_server.close)
        if self.proxy_client is not None:
            plan.add("proxy-client", self.proxy_client.close)
        if self.devtools.watcher is not None:
            plan.add("watcher", self.devtools.watcher.stop)
        if self.devtools.console is not None:
            plan.add("console", self.devtools.console.stop)
        # F-198: stop arming the clock boundary chain BEFORE the flush -- a
        # boundary firing mid-flush dirty data after its snapshot is the
        # same silent-loss class as an ingress frame, and a chain leg racing
        # the scheduler's close step used to raise "scheduler is closed"
        # out of a background task.
        plan.add("clock-events", self.clock_events.stop)
        plan.add("save-flush", flush_step, timeout=self.shutdown_timeout)
        if self.devtools.monitor is not None:
            plan.add("monitor", self.devtools.monitor.stop)
        if self.devtools.metrics_server is not None:
            plan.add("metrics-server", self.devtools.stop_metrics)
        # F-225: close client connections (bounded) before the transports
        # under them go away.
        if self.sessions.count():
            plan.add("client-sessions", self._drain_client_sessions)
        # F-198: cancel remaining background notification tasks (clock
        # chains, connect/disconnect event emits) instead of leaving them to
        # asyncio.run's loop-close backstop. The CURRENT task (this
        # teardown, possibly spawned through _spawn itself) is excluded.
        if self._bg_tasks:
            plan.add("bg-tasks", self._drain_bg_tasks)
        if self.bus_zmq is not None:
            plan.add("zmq-bus", self.bus_zmq.close)
        if self.db_layer.db_service is not None:
            # F-101 wiring: the db process's remote-transaction sessions live
            # on out-of-pool dedicated connections, so mysql.close() cannot
            # reach them -- without this step the graceful rollback F-101
            # wrote only ever ran in tests, and live sessions were dropped
            # for the OS to notice the dead socket.
            plan.add("db-service", self.db_layer.db_service.close)
        if self.db_layer.redis is not None:
            plan.add("redis", self.db_layer.redis.close)
        if self.db_layer.mysql is not None:
            plan.add("mysql", self.db_layer.mysql.close)
        plan.add("scheduler", self.scheduler.close)

        try:
            await asyncio.wait_for(plan.run(), timeout=self.shutdown_timeout)
        except TimeoutError:
            logger.critical(
                "shutdown teardown exceeded %.0fs; continuing exit", self.shutdown_timeout
            )
        if not self._flush_completed:
            # The flush step never finished (per-step timeout or total
            # deadline): dirty data may still be in memory, and the clean
            # exit-0 path must not claim otherwise (F-01 extends to teardown
            # timeouts; previously only the total-deadline branch set this).
            self.save_flush_ok = False
            logger.critical("save-flush did not complete before the teardown ended")

    async def _drain_client_sessions(self) -> None:
        """F-225: bounded close of every registered client connection."""
        await self.sessions.drain(timeout=self.shutdown_step_timeout)

    async def _drain_bg_tasks(self) -> None:
        """F-198: cancel outstanding background notification tasks.

        Excludes the CURRENT task: the teardown itself usually arrived here
        through ``_spawn(runtime.shutdown(...))`` (signal handler, console
        command, watcher hook), and cancelling our own task mid-plan is
        exactly the "teardown cut short" failure F-58 closed."""
        current = asyncio.current_task()
        pending = [t for t in self._bg_tasks if t is not current and not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def revoke_child_bus_identity(self, process_type: str) -> None:
        """F-190: revoke a dead child's bus identity immediately.

        The supervisor observes the process exit; the bus drops the identity
        from its authenticated table so whoever claims that service number
        next (a restarting child must, an impostor must not get to) starts
        from an unauthenticated state. Belt and braces over the auth-liveness
        TTL; harmless when the identity was already gone."""
        bus = self.bus_zmq
        if bus is None:
            return
        try:
            index = self.ctx.process_index_of(process_type)
        except ValueError:
            return
        bus.revoke_identity(index * SERVICE_NO_STRIDE + self.ctx.entry.server_no)


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


async def _settle_shutdown(runtime: ServerRuntime) -> None:
    """Make sure the teardown has actually finished before returning.

    F-58: every real shutdown path (signal handler, console command,
    child-death watch) spawns ``runtime.shutdown`` fire-and-forget, and
    ``request_shutdown`` flips the state to QUIT *before* running the
    teardown hooks. The parking loop therefore observes QUIT while the
    save-flush is still in flight; returning then lets ``asyncio.run``
    cancel the teardown task mid-flush while ``save_flush_ok`` still reads
    True -- exit 0 with dirty data lost, precisely what F-01's exit code
    guarantees were meant to prevent.
    """
    if not runtime.lifecycle.in_quit():
        await runtime.shutdown("main loop exit")
    else:
        await runtime.lifecycle.wait_shutdown_complete()


async def run_process(ctx: Context, config_dir: Path) -> None:
    """Single-process entry: build runtime, boot, park until shutdown."""
    pyline_api.bind(ctx)
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
        await _settle_shutdown(runtime)
    if not runtime.save_flush_ok:
        raise SystemExit(3)  # dirty data could not be flushed at shutdown


def _install_signal_handlers(runtime: ServerRuntime) -> None:
    loop = asyncio.get_running_loop()
    signal_count = {"n": 0}

    def handle_signal(sig: signal.Signals) -> None:
        signal_count["n"] += 1
        if signal_count["n"] >= 2:
            # F-19: one signal graceful, two signals immediate (industry
            # convention) -- the first shutdown may itself be stuck.
            logger.warning("second signal %s; forcing immediate exit", sig.name)
            os._exit(1)
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

    # Warm config validation before spawning children (fail fast). Everything
    # the MAIN process will parse at build_context time is validated here so a
    # config error surfaces BEFORE children exist -- previously a bad
    # tables.json5 or clock.tz crashed the main process after the spawn, and
    # teardown degraded to the children's parent-watch (no flush, exit-code
    # guarantees lost).
    warm_settings = load_project_settings(config_dir)
    load_table_defs(config_dir)
    if warm_settings.clock.tz:
        ServerRuntime._build_clock(warm_settings.clock.tz)

    if entry.sub_process:
        supervisor = ProcessSupervisor(child_main)
        supervisor.spawn_subprocesses(entry.sub_process, main_pid)
        main_runtime: ServerRuntime | None = None

        async def on_child_died(process_type: str, exitcode: int | None) -> None:
            if main_runtime is None:
                # Unreachable today: the runtime is created before the watch
                # starts. Keep the guard loud rather than silently ignoring.
                logger.critical(
                    "sub-process %s died (%s) before the main runtime was ready",
                    process_type,
                    exitcode,
                )
                return
            # F-190: the dead child's bus identity is revoked before anything
            # else -- the shutdown it triggers takes time, and in that window
            # a local process could otherwise inherit the identity's
            # authenticated status.
            main_runtime.revoke_child_bus_identity(process_type)
            await main_runtime.shutdown(f"sub-process {process_type} died ({exitcode})")

        async def on_unhandled_death(process_type: str, exitcode: int | None) -> None:
            # F-160: the no-callback / callback-failed fail-fast branch.
            # Terminating the siblings alone left the main runtime running
            # with its db/proxy process gone -- the escalation keeps the
            # whole server one fail-fast unit.
            target = main_runtime
            if target is None:
                logger.critical(
                    "sub-process %s died unhandled (%s) before the main runtime was ready",
                    process_type,
                    exitcode,
                )
                return
            target.revoke_child_bus_identity(process_type)  # F-190
            await target.shutdown(f"sub-process {process_type} died unhandled ({exitcode})")

        async def main_proc() -> None:
            nonlocal main_runtime
            runtime: ServerRuntime | None = None
            try:
                ctx = build_context(config_dir, server_no, PROCESS_MAIN, 0, main_pid)
                pyline_api.bind(ctx)
                pyline_log.setup_logging(
                    ctx.settings.log,
                    process_tag=PROCESS_MAIN,
                    run_dir=Path(ctx.settings.log.log_dir),
                )
                runtime = ServerRuntime(ctx)
                main_runtime = runtime
                _install_signal_handlers(runtime)  # F-20: main process too
                # Watch only after the runtime exists: a child that died while the
                # main process was still setting up must find a runtime to tear
                # down, otherwise the main process would run on without it.
                supervisor.start_child_watch(on_child_died, on_unhandled_death=on_unhandled_death)
                boot_task = asyncio.get_running_loop().create_task(runtime.boot())
                try:
                    await boot_task
                    while not runtime.lifecycle.in_quit():
                        await asyncio.sleep(0.5)
                finally:
                    await _settle_shutdown(runtime)
            finally:
                # Children must die with the main process even when boot OR THE
                # PRE-BOOT CONSTRUCTION fails: build_context/ServerRuntime used
                # to sit outside this guard, so a bad config file discovered
                # there crashed the main process with live children and no
                # graceful teardown (the children's parent-watch was the only
                # backstop -- crash-mode exits, no save flush).
                await supervisor.terminate_children()
            assert runtime is not None  # reached only on the success path
            if not runtime.save_flush_ok:
                raise SystemExit(3)  # dirty data could not be flushed at shutdown

        asyncio.run(main_proc())
    else:
        ctx = build_context(config_dir, server_no, PROCESS_MAIN, 0, main_pid)
        asyncio.run(run_process(ctx, config_dir))


if __name__ == "__main__":
    main()
