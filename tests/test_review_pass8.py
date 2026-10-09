"""Eighth review pass (F-142..F-154): one regression test per finding.

The fixes span many subsystems, so the tests live in one module; each class
cites its finding id the same way the per-area suites do. F-147's mutual
handshake tests live in test_connection.py next to the handshake suite.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import os
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import msgpack
import pytest

from pyline.core.events import EventBus
from pyline.core.scheduler import Scheduler
from pyline.core.supervisor import ProcessSupervisor
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import DataSaver
from pyline.db.schema import _split_statements
from pyline.db.service import DatabaseService, NullRedis
from pyline.net.gateway import ProtocolGateway
from pyline.net.ipc import ZmqBus
from pyline.net.router import MessageRouter, NoProxyAvailableError
from pyline.reload import ReloadRejected, reload_module
from tests.test_ipc import _CaptureNet, free_tcp_port, make_ctx
from tests.test_orm_autosave import FakeDB, make_schema
from tests.test_supervisor import _FakeChild, _quiet_child
from tests.test_transaction import FakeSessionPool


@pytest.fixture()
def router_ctx(config_dir):
    """Same construction as test_router_metrics.router_ctx (local copy: the
    imported fixture collides with the parameter name under ruff F811)."""
    from pyline.config.loader import load_project_settings, load_server_registry, load_table_defs
    from pyline.core.context import PROCESS_MAIN, Context

    settings = load_project_settings(config_dir)
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=registry.entry(10001),
        process_type=PROCESS_MAIN,
        process_index=0,
        main_pid=0,
    )


# --------------------------------------------------------------------------- #
# F-142: the remote-transaction reaper/close dispose under the session lock
# --------------------------------------------------------------------------- #


class TestReaperDisposeLockF142:
    async def test_reap_dispose_waits_for_inflight_statement(self) -> None:
        """The TTL sweep must not roll back a session while a statement is
        still running on it (the protocol-stream corruption the lock exists
        to prevent)."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis(), tx_ttl=60.0)
        tx_id = await service.rpc_tx_begin()
        record = service._tx[tx_id]
        await record.lock.acquire()  # an in-flight statement holds the lock
        record.last_active = time.monotonic() - (service._tx_ttl + 1)
        service._reap_expired()  # schedules the dispose task
        assert tx_id not in service._tx  # reaped from the table immediately
        await asyncio.sleep(0.05)
        assert "ROLLBACK" not in pool.log and "CLOSE" not in pool.log, (
            "dispose touched the session without holding its lock (F-142)"
        )
        record.lock.release()  # the statement finished
        for _ in range(100):
            if "CLOSE" in pool.log:
                break
            await asyncio.sleep(0.02)
        assert pool.log[-2:] == ["ROLLBACK", "CLOSE"]

    async def test_close_dispose_also_serializes(self) -> None:
        """close() drives the same locked dispose path."""
        pool = FakeSessionPool()
        service = DatabaseService(pool, NullRedis())
        tx_id = await service.rpc_tx_begin()
        record = service._tx[tx_id]
        await record.lock.acquire()
        close_task = asyncio.get_running_loop().create_task(service.close(timeout=5.0))
        await asyncio.sleep(0.05)
        assert "CLOSE" not in pool.log  # still waiting for the lock
        record.lock.release()
        await asyncio.wait_for(close_task, 5.0)
        assert pool.log[-1] == "CLOSE"


# --------------------------------------------------------------------------- #
# F-145: raise_on_drop honoured on the proxy legs
# --------------------------------------------------------------------------- #


class TestRaiseOnDropProxyLegF145:
    async def test_no_proxy_connected_raises_instead_of_silent_drop(self, router_ctx) -> None:
        from tests.test_router_metrics import FakeBus

        gateway = ProtocolGateway()
        router = MessageRouter(router_ctx, gateway, FakeBus())  # type: ignore[arg-type]

        class DeadProxy:
            def send_to_service(self, *args: Any, **kwargs: Any) -> None:
                raise NoProxyAvailableError("no link")

        router.attach_proxy_client(DeadProxy())  # type: ignore[arg-type]
        remote = 50 * 100_000 + 999  # another physical server entirely
        with pytest.raises(NoProxyAvailableError):
            router.route("x", b"p", remote, raise_on_drop=True)
        # without raise_on_drop the drop stays quiet (fire-and-forget traffic)
        router.route("x", b"p", remote, raise_on_drop=False)

    async def test_missing_proxy_client_raises_with_raise_on_drop(self, router_ctx) -> None:
        from tests.test_router_metrics import FakeBus

        router = MessageRouter(router_ctx, ProtocolGateway(), FakeBus())  # type: ignore[arg-type]
        remote = 50 * 100_000 + 999
        with pytest.raises(NoProxyAvailableError):
            router.route("x", b"p", remote, raise_on_drop=True)


# --------------------------------------------------------------------------- #
# F-146: total (cross-container) msgpack decode budget
# --------------------------------------------------------------------------- #


class TestDecodeTotalBudgetF146:
    def test_dense_tiny_value_frame_rejected(self) -> None:
        """Per-container caps alone let many small arrays through; the
        running total must abort the decode."""
        from pyline.net.protocol import decode_payload

        payload = msgpack.packb([[0] * 300_000 for _ in range(8)], use_bin_type=True)
        with pytest.raises(ValueError, match="total element budget"):
            decode_payload(payload)

    def test_map_slots_counted_too(self) -> None:
        from pyline.net.protocol import decode_payload

        payload = msgpack.packb([{i: i for i in range(200_000)} for _ in range(12)])
        with pytest.raises(ValueError, match="total element budget"):
            decode_payload(payload)

    def test_legitimate_single_large_container_passes(self) -> None:
        from pyline.net.protocol import decode_payload

        out = decode_payload(msgpack.packb(list(range(1_000_000))))
        assert len(out) == 1_000_000

    def test_ordinary_payload_round_trips(self) -> None:
        from pyline.net.protocol import decode_payload

        data = {"rows": [[1, 2], [3, 4]], "blob": b"x" * 1000, "name": "ok"}
        assert decode_payload(msgpack.packb(data, use_bin_type=True)) == data


# --------------------------------------------------------------------------- #
# F-148: idle-probe send failure does not crash the watcher
# --------------------------------------------------------------------------- #


class TestIdleProbeGuardF148:
    async def test_probe_send_failure_ends_watcher_quietly(self) -> None:
        import pyline.net.connection as conn_mod
        from tests.test_connection import KW, TOKEN, start_echo_server

        got: dict = {}
        server, port = await start_echo_server(got)
        client = await conn_mod.open_connection(
            "127.0.0.1", port, token=TOKEN, on_message=lambda f, p: None, **KW
        )
        try:
            # A probe whose send path raises (queue overflow / already closed)
            # used to escape _idle_watch as an unretrieved task exception.
            def boom(flag: str, payload: bytes) -> None:
                raise conn_mod.ConnectionClosedError("queue full")

            client.send_message = boom  # type: ignore[method-assign]
            await asyncio.sleep(1.5)  # one probe round (ping_interval >= 1s)
            crashed = [
                t
                for t in client._tasks
                if t.done() and not t.cancelled() and t.exception() is not None
            ]
            assert not crashed, "idle watcher died on a failed probe send (F-148)"
        finally:
            await client.close("done")
            await conn_mod.close_server(server)


# --------------------------------------------------------------------------- #
# F-149 / F-154: hot-reload guard net
# --------------------------------------------------------------------------- #

_ASYNC_V1 = textwrap.dedent(
    """
    async def fetch(url: str) -> str:
        return "v1"

    class Client:
        async def call(self) -> int:
            return 1
    """
)

_keep_v1 = textwrap.dedent(
    """
    class Base:
        __reloadkeep__ = ("helper",)

        def helper(self) -> str:
            return "base"

    class Sub(Base):
        def helper(self) -> str:
            return "sub-v1"
    """
)

# Monotonically increasing future mtimes: same-second, same-size rewrites
# would otherwise hit Python's stale-bytecode cache during re-import.
_mtime_seq = itertools.count(int(time.time()) + 10)


def _write_module(tmp_path: Path, name: str, source: str, monkeypatch) -> Any:
    monkeypatch.delitem(sys.modules, name, raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    path = tmp_path / f"{name}.py"
    path.write_text(source, encoding="utf-8")
    stamp = next(_mtime_seq)
    os.utime(path, (stamp, stamp))
    module = __import__(name)
    return module


class TestReloadAsyncSyncSwapF149:
    def test_async_to_sync_rejected(self, tmp_path: Path, monkeypatch) -> None:
        _write_module(tmp_path, "asyncmod", _ASYNC_V1, monkeypatch)
        v2 = _ASYNC_V1.replace("async def fetch", "def fetch")
        (tmp_path / "asyncmod.py").write_text(v2, encoding="utf-8")
        with pytest.raises(ReloadRejected, match="kind changed"):
            reload_module("asyncmod")

    def test_sync_to_async_rejected(self, tmp_path: Path, monkeypatch) -> None:
        _write_module(
            tmp_path, "asyncmod", _ASYNC_V1.replace("async def fetch", "def fetch"), monkeypatch
        )
        (tmp_path / "asyncmod.py").write_text(_ASYNC_V1, encoding="utf-8")
        with pytest.raises(ReloadRejected, match="kind changed"):
            reload_module("asyncmod")

    def test_method_async_to_sync_rejected(self, tmp_path: Path, monkeypatch) -> None:
        _write_module(tmp_path, "asyncmod", _ASYNC_V1, monkeypatch)
        v2 = _ASYNC_V1.replace("async def call", "def call")
        (tmp_path / "asyncmod.py").write_text(v2, encoding="utf-8")
        with pytest.raises(ReloadRejected, match="kind changed"):
            reload_module("asyncmod")

    def test_async_to_async_is_fine(self, tmp_path: Path, monkeypatch) -> None:
        module = _write_module(tmp_path, "asyncmod", _ASYNC_V1, monkeypatch)
        (tmp_path / "asyncmod.py").write_text(
            _ASYNC_V1.replace('return "v1"', 'return "v2"'), encoding="utf-8"
        )
        reload_module("asyncmod")
        assert asyncio.run(module.fetch("u")) == "v2"  # type: ignore[attr-defined]


class TestReloadKeepOwnClassF154:
    def test_base_keep_list_not_inherited_by_subclass(self, tmp_path: Path, monkeypatch) -> None:
        """``__reloadkeep__`` is a per-class contract: a base's keep-list must
        not pin attribute names in the subclass diff too."""
        module = _write_module(tmp_path, "keepmod", _keep_v1, monkeypatch)
        v2 = _keep_v1.replace('return "sub-v1"', 'return "sub-v2"')
        (tmp_path / "keepmod.py").write_text(v2, encoding="utf-8")
        reload_module("keepmod")
        sub = module.Sub()
        assert sub.helper() == "sub-v2", "base __reloadkeep__ pinned the subclass attr (F-154)"


# --------------------------------------------------------------------------- #
# F-150: handler-raised CancelledError does not truncate the dispatch chain
# --------------------------------------------------------------------------- #


@dataclass
class _Evt:
    n: int


class TestCancelledIsolationF150:
    async def test_handler_raising_cancellederror_does_not_truncate(self) -> None:
        bus = EventBus()
        ran: list[str] = []

        async def bad(_: _Evt) -> None:
            raise asyncio.CancelledError()

        def after(_: _Evt) -> None:
            ran.append("after")

        bus.subscribe(_Evt, bad)
        bus.subscribe(_Evt, after)
        await bus.emit(_Evt(n=1), reverse=True)  # quit-style chain
        assert ran == ["after"]
        assert bus.handler_failures()

    async def test_real_cancellation_still_propagates(self) -> None:
        bus = EventBus()
        started = asyncio.Event()
        late: list[str] = []

        async def slow(_: _Evt) -> None:
            started.set()
            await asyncio.sleep(30)

        def after(_: _Evt) -> None:
            late.append("ran")

        bus.subscribe(_Evt, slow)
        bus.subscribe(_Evt, after)
        task = asyncio.get_running_loop().create_task(bus.emit(_Evt(n=1)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert late == []


# --------------------------------------------------------------------------- #
# F-151: autosave in-flight skip + explicit flush dequeue
# --------------------------------------------------------------------------- #


class _RemarkMidFlightDB(FakeDB):
    """Simulates a mutation landing while the flush's upsert is in flight."""

    saver: DataSaver | None = None

    async def execute(self, sql: str, args: tuple = ()) -> int:
        assert self.saver is not None
        self.saver.set_data({"gold": 2})  # re-mark during the SQL
        return await super().execute(sql, args)


class TestExplicitFlushDequeuesF151:
    async def test_flush_removes_redundant_queue_entry(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 1})
        assert scheduler.queue_depth() == 1
        await saver.flush()
        assert scheduler.queue_depth() == 0, "queued entry survived an explicit flush"

    async def test_remark_during_flush_stays_queued(self) -> None:
        db = _RemarkMidFlightDB()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        db.saver = saver
        saver.set_data({"gold": 1})
        await saver.flush()
        assert scheduler.queue_depth() == 1, (
            "a mark that landed mid-flush must keep the saver queued for the next round"
        )

    async def test_next_due_skips_inflight_saver(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 1})
        scheduler._inflight.add(saver)  # a flush is running on it
        assert scheduler._next_due(time.monotonic() + 999) is None


# --------------------------------------------------------------------------- #
# F-152: migration splitter honours ``#`` comments
# --------------------------------------------------------------------------- #


class TestHashCommentSplitF152:
    def test_semicolon_inside_hash_comment_does_not_split(self) -> None:
        sql = "INSERT INTO `t` VALUES (1); # seed; note\nSELECT 2;"
        assert _split_statements(sql) == ["INSERT INTO `t` VALUES (1)", "SELECT 2"]

    def test_hash_comment_without_newline_runs_to_eof(self) -> None:
        sql = "SELECT 1; # dangling ; comment"
        assert _split_statements(sql) == ["SELECT 1"]


# --------------------------------------------------------------------------- #
# F-153: shutdown log hygiene
# --------------------------------------------------------------------------- #


class TestRepeatingTimerAfterCloseF153:
    async def test_dispatched_beat_after_close_is_quiet(self, caplog) -> None:
        sched = Scheduler(loop=asyncio.get_running_loop())
        calls: list[int] = []
        handle = sched.call_repeating(60.0, lambda: calls.append(1), label="rep")
        entry = next(iter(sched._entries.values()))
        sched._closed = True  # a close() landed while this beat was dispatched
        with caplog.at_level(logging.ERROR, logger="pyline.core.scheduler"):
            sched._fire(entry.func, entry.args, entry.label)
        assert calls == [1]
        assert not [r for r in caplog.records if "failed" in r.getMessage()], (
            "post-close re-arm must not log a traceback (F-153)"
        )
        handle.cancel()
        await sched.close()


class TestCleanChildExitWatchF153:
    async def test_clean_exits_keep_watching_siblings(self) -> None:
        sup = ProcessSupervisor(_quiet_child)
        sup._children["a"] = _FakeChild(exitcode=None)  # type: ignore[assignment]
        sup._children["b"] = _FakeChild(exitcode=None)  # type: ignore[assignment]
        notified: list[tuple[str, int | None]] = []

        async def on_died(process_type: str, exitcode: int | None) -> None:
            notified.append((process_type, exitcode))

        watch = asyncio.get_running_loop().create_task(sup._watch_children(on_died))
        try:
            sup._children["a"].exitcode = 0  # type: ignore[index, union-attr]
            for _ in range(100):
                if notified:
                    break
                await asyncio.sleep(0.02)
            assert notified == [("a", 0)]
            assert not watch.done(), "watcher stopped after ONE clean exit (F-153)"
            sup._children["b"].exitcode = 0  # type: ignore[index, union-attr]
            for _ in range(100):
                if len(notified) == 2:
                    break
                await asyncio.sleep(0.02)
            assert notified == [("a", 0), ("b", 0)]
        finally:
            watch.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watch


# --------------------------------------------------------------------------- #
# F-143 / F-144: bus re-authentication and pre-auth slot eviction
# --------------------------------------------------------------------------- #


def make_ctx_fast(config_dir, tmp_path, *, main: bool, index: int, port: int):
    """make_ctx with a fast reconnect interval so restart tests stay quick."""
    from pyline.config.loader import load_project_settings, load_server_registry, load_table_defs
    from pyline.core.context import PROCESS_DB, PROCESS_MAIN, Context

    settings = load_project_settings(config_dir)
    settings = settings.model_copy(
        update={
            "zeromq": settings.zeromq.model_copy(
                update={
                    "bind_host": f"tcp://127.0.0.1:{port}",
                    "reconnect_min_ms": 100,
                    "reconnect_max_ms": 500,
                }
            )
        }
    )
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=registry.entry(10001),
        process_type=PROCESS_MAIN if main else PROCESS_DB,
        process_index=index,
        main_pid=0,
    )


@pytest.mark.integration
class TestBusRehandshakeF143:
    async def test_dealer_reauthenticates_after_router_restart(self, config_dir, tmp_path) -> None:
        """A restarted main-process ROUTER starts with an empty ``_authed``
        set; the DEALER must re-run the handshake on reconnect or every frame
        it sends is dropped forever."""
        from pyline.net.network import pack_call

        port = free_tcp_port()
        ctx_main = make_ctx_fast(config_dir, tmp_path, main=True, index=0, port=port)
        ctx_sub = make_ctx_fast(config_dir, tmp_path, main=False, index=1, port=port)
        gw_main, gw_sub = ProtocolGateway(), ProtocolGateway()
        bus_main = ZmqBus(ctx_main, gw_main)
        bus_sub = ZmqBus(ctx_sub, gw_sub)
        box_main: dict = {"event": asyncio.Event()}
        _CaptureNet(gw_main, box_main)

        await bus_main.start()
        await bus_sub.start()
        try:
            bus_sub.send(ctx_main.service_no, "test", pack_call(1, "before"))
            await asyncio.wait_for(box_main["event"].wait(), 5.0)
            assert box_main["got"] == "before"

            # The ROUTER dies and comes back as a fresh socket/process: its
            # authentication table is empty.
            await bus_main.close()
            await asyncio.sleep(0.3)  # let the DEALER see the disconnect
            bus_main2 = ZmqBus(ctx_main, gw_main)
            await bus_main2.start()
            try:
                box_main["event"].clear()
                got_it = False
                for _ in range(10):
                    bus_sub.send(ctx_main.service_no, "test", pack_call(1, "after"))
                    try:
                        await asyncio.wait_for(box_main["event"].wait(), 1.5)
                        got_it = True
                        break
                    except TimeoutError:
                        continue
                assert got_it, "DEALER never re-authenticated after the ROUTER restart (F-143)"
                assert box_main["got"] == "after"
                assert bus_sub._auth_ok
            finally:
                await bus_main2.close()
        finally:
            await bus_sub.close()


@pytest.mark.integration
class TestPreauthEvictionF144:
    async def test_unauthenticated_slots_do_not_pin_the_table(self, config_dir, tmp_path) -> None:
        """Junk identities from local AUTH0 spam must not starve legitimate
        destinations out of the (bounded) table."""
        port = free_tcp_port()
        ctx_main = make_ctx(config_dir, tmp_path, main=True, index=0, port=port)
        ctx_sub = make_ctx(config_dir, tmp_path, main=False, index=1, port=port)
        gw_main, gw_sub = ProtocolGateway(), ProtocolGateway()
        bus_main = ZmqBus(ctx_main, gw_main, max_destinations=3)
        bus_sub = ZmqBus(ctx_sub, gw_sub)
        await bus_main.start()
        await bus_sub.start()
        try:
            await asyncio.sleep(0.3)
            # the authenticated DEALER's slot plus two junk targets fill the table
            junk1, junk2 = 5 * 100_000 + 10001, 6 * 100_000 + 10001
            bus_main.send(junk1, "test", b"x")
            bus_main.send(junk2, "test", b"x")
            await asyncio.sleep(0.2)  # let their (unroutable) writers drain
            assert len(bus_main._peer_queues) == 3
            # a fourth destination evicts a worthless pre-auth slot
            fourth = 7 * 100_000 + 10001
            bus_main.send(fourth, "test", b"x")
            assert bus_main.dest_overflow == 0, "pre-auth slot pinned the table (F-144)"
            assert fourth in bus_main._peer_queues
            assert ctx_sub.service_no in bus_main._peer_queues  # authed slot survives
        finally:
            await bus_sub.close()
            await bus_main.close()
