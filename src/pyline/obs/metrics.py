"""Prometheus metrics registry and the event-loop latency monitor.

The prototype's stall detector only printed to the console; here every
observation lands in a histogram (exportable via prometheus-client) and an
optional alert callback fires when the loop stays blocked beyond a threshold.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

# F-184: cache for shared counters (see shared_counter).
_SHARED_COUNTERS: dict[str, Counter] = {}


def shared_counter(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Counter:
    """Get-or-create a Counter in the default registry (F-184).

    Modules that must stay independently importable (net.connection,
    net.ipc) used to declare module-level ``Counter(...)`` objects -- an
    import side effect that registers into the process-global registry, so
    re-importing the module under a fresh identity (a hot reload of a
    consumer, a sys.path duplicate) raised "Duplicated timeseries" and took
    the importer down.  The factory caches by metric name; the cache lives in
    this module (stable across consumer reloads), and a registry collision
    beyond that (this module itself reloaded) falls back to the live
    collector instead of failing the import.
    """
    cached = _SHARED_COUNTERS.get(name)
    if cached is not None:
        return cached
    try:
        counter = Counter(name, documentation, labelnames)
    except ValueError:
        collector = REGISTRY._names_to_collectors.get(name)
        if not isinstance(collector, Counter):
            raise
        counter = collector
    _SHARED_COUNTERS[name] = counter
    return counter


class Metrics:
    """Framework metric handles; created once per process."""

    def __init__(self, *, namespace: str = "pyline") -> None:
        self.loop_latency = Histogram(
            f"{namespace}_loop_latency_seconds",
            "Event-loop scheduling delay observed by the monitor",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
        )
        self.connections = Gauge(f"{namespace}_connections", "Active connections")
        # F-225: registered client sessions (post-handshake, not yet closed)
        # -- the connection<->player bookkeeping base business layers build on.
        self.client_sessions = Gauge(
            f"{namespace}_client_sessions", "Registered client connections (session registry)"
        )
        self.rpc_latency = Histogram(
            f"{namespace}_rpc_seconds",
            "RPC round-trip latency",
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
        )
        self.rpc_timeouts = Counter(f"{namespace}_rpc_timeouts_total", "Timed-out RPC calls")
        self.dispatch_errors = Counter(
            f"{namespace}_dispatch_errors_total", "Handler exceptions during frame dispatch"
        )
        self.ipc_unroutable = Counter(
            f"{namespace}_ipc_unroutable_total", "ZMQ sends to unroutable peers"
        )
        self.ipc_dropped = Counter(
            f"{namespace}_ipc_dropped_total", "ZMQ messages dropped on full queues"
        )
        self.ipc_dest_overflow = Counter(
            f"{namespace}_ipc_dest_overflow_total",
            "ZMQ sends dropped: destination table at max_destinations",
        )
        self.ipc_spoofed = Counter(
            f"{namespace}_ipc_spoofed_total",
            "ZMQ messages dropped: claimed from-service != actual sender identity",
        )
        self.proxy_spoofed = Counter(
            f"{namespace}_proxy_spoofed_total",
            "Proxy @fwd messages dropped: claimed from-service machine != sending "
            "connection's registered machine",
        )
        self.rpc_origin_rejects = Counter(
            f"{namespace}_rpc_origin_rejects_total",
            "RPC messages dropped: result/call origin failed validation",
            ("reason",),
        )
        self.handler_overflow = Counter(
            f"{namespace}_handler_overflow_total",
            "Plain-network inbound messages dropped: handler concurrency cap reached",
        )
        self.save_queue = Gauge(f"{namespace}_save_queue", "Pending auto-save entries")
        self.save_flushed = Counter(f"{namespace}_save_flushed_total", "Flushed save entries")
        self.save_failures = Counter(f"{namespace}_save_failures_total", "Failed save flushes")
        # F-212: rows skipped because the saver was deleted while waiting for
        # its flush lock -- nothing was written, so counting them as flushed
        # overstated durability.
        self.save_skipped = Counter(
            f"{namespace}_save_skipped_total", "Save entries skipped (deleted before flush)"
        )
        self.reload_total = Counter(f"{namespace}_reload_total", "Hot reloads", ("result",))
        # --- kernel / lifecycle / db instrumentation ----------------------- #
        # These close the observability gap where a multi-process game server
        # needs them most: child deaths and restarts, timer backlog, pool
        # saturation, boot phase durations and migration outcomes were
        # previously log-only (invisible to Prometheus).
        self.children_alive = Gauge(f"{namespace}_children_alive", "Live supervised children")
        self.child_exits = Counter(
            f"{namespace}_child_exits_total",
            "Supervised child process exits",
            ("process_type", "reason"),
        )
        self.boot_phase_seconds = Histogram(
            f"{namespace}_boot_phase_seconds",
            "Duration of one boot-step action",
            ("phase",),
            buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300),
        )
        self.scheduler_pending = Gauge(
            f"{namespace}_scheduler_pending", "Timers pending in the scheduler"
        )
        self.mysql_pool_size = Gauge(f"{namespace}_mysql_pool_size", "MySQL pool size")
        self.mysql_pool_in_use = Gauge(
            f"{namespace}_mysql_pool_in_use", "MySQL pool connections in use"
        )
        self.mysql_acquire_timeouts = Counter(
            f"{namespace}_mysql_acquire_timeouts_total",
            "Pool acquisitions that exceeded acquire_timeout",
        )
        self.schema_migrations = Counter(
            f"{namespace}_schema_migrations_total",
            "Versioned schema migration files applied",
            ("result",),
        )


_METRICS: Metrics | None = None


def get_metrics() -> Metrics:
    global _METRICS
    if _METRICS is None:
        _METRICS = Metrics()
    return _METRICS


class LoopLatencyMonitor:
    """Measures event-loop blockage: sleep(k), measure the overshoot."""

    def __init__(
        self,
        *,
        interval: float = 0.5,
        alert_threshold: float = 1.0,
        on_alert: Callable[[float], None] | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._interval = interval
        self._alert_threshold = alert_threshold
        self._on_alert = on_alert
        self._metrics = metrics or get_metrics()
        self._task: asyncio.Task[None] | None = None
        self.healthy = True
        self.last_delay = 0.0

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while True:
            expected = self._interval
            started = time.monotonic()
            await asyncio.sleep(expected)
            delay = time.monotonic() - started - expected
            self.last_delay = delay
            self._metrics.loop_latency.observe(max(delay, 0.0))
            blocked = delay > self._alert_threshold
            if blocked and self.healthy:
                logger.error("event loop blocked for %.2fs", delay)
                if self._on_alert is not None:
                    try:
                        self._on_alert(delay)
                    except Exception:
                        logger.exception("loop-latency alert callback failed")
            self.healthy = not blocked


class AlarmHub:
    """Central alarm fan-out (F-28): producers emit (kind, payload), ops
    code subscribes -- the bridge between framework events and alerting."""

    def __init__(self) -> None:
        self._subs: dict[str, list[Callable[[dict[str, Any]], None]]] = {}
        self._all: list[Callable[[str, dict[str, Any]], None]] = []

    def register(self, kind: str, callback: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        # F-90b: same callback registered twice delivered every alarm twice
        # (the hub has no dedupe) -- register() now no-ops on re-register,
        # symmetric with EventBus.subscribe.
        handlers = self._subs.setdefault(kind, [])
        if callback not in handlers:
            handlers.append(callback)
        return lambda: self._unsubscribe(kind, callback)

    def register_all(self, callback: Callable[[str, dict[str, Any]], None]) -> Callable[[], None]:
        """Subscribe to every alarm kind; returns the unsubscribe fn (F-90b:
        the asymmetric ``-> None`` left catch-all subscribers permanently
        wedged, and duplicate registrations double-delivered)."""
        if callback not in self._all:
            self._all.append(callback)
        return lambda: self._unsubscribe_all(callback)

    def _unsubscribe(self, kind: str, callback: Callable[[dict[str, Any]], None]) -> None:
        handlers = self._subs.get(kind)
        if handlers and callback in handlers:
            handlers.remove(callback)

    def _unsubscribe_all(self, callback: Callable[[str, dict[str, Any]], None]) -> None:
        if callback in self._all:
            self._all.remove(callback)

    def emit(self, kind: str, payload: dict[str, Any]) -> None:
        for callback in self._subs.get(kind, []):
            try:
                callback(payload)
            except Exception:
                logger.exception("alarm callback failed (%s)", kind)
        for catch_all in self._all:
            try:
                catch_all(kind, payload)
            except Exception:
                logger.exception("alarm callback failed (%s)", kind)
