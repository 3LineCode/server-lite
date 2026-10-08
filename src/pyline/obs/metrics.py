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

from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)


class Metrics:
    """Framework metric handles; created once per process."""

    def __init__(self, *, namespace: str = "pyline") -> None:
        self.loop_latency = Histogram(
            f"{namespace}_loop_latency_seconds",
            "Event-loop scheduling delay observed by the monitor",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
        )
        self.connections = Gauge(f"{namespace}_connections", "Active connections")
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
        self.save_queue = Gauge(f"{namespace}_save_queue", "Pending auto-save entries")
        self.save_flushed = Counter(f"{namespace}_save_flushed_total", "Flushed save entries")
        self.save_failures = Counter(f"{namespace}_save_failures_total", "Failed save flushes")
        self.reload_total = Counter(f"{namespace}_reload_total", "Hot reloads", ("result",))


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
        self._subs.setdefault(kind, []).append(callback)
        return lambda: self._unsubscribe(kind, callback)

    def register_all(self, callback: Callable[[str, dict[str, Any]], None]) -> None:
        self._all.append(callback)

    def _unsubscribe(self, kind: str, callback: Callable[[dict[str, Any]], None]) -> None:
        handlers = self._subs.get(kind)
        if handlers and callback in handlers:
            handlers.remove(callback)

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
