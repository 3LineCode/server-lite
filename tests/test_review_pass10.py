"""Tenth review pass (F-165..F-189): one regression test per finding.

The fixes span tracked containers, hot-reload closures, the ZMQ bus auth
state machine, proxy routing, TCP timeouts/TLS, CURVE+ZAP, and the data
layer's races and performance cliffs; each test cites its finding id the
same way the per-area suites do.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

import pyline.net.connection as conn_mod
from pyline.config.errors import ConfigError
from pyline.config.loader import load_project_settings, load_server_registry, load_table_defs
from pyline.core.context import PROCESS_MAIN, Context
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import MAX_BLOB_BYTES, DataSaver
from pyline.db.service import TransactionGoneError
from pyline.db.transaction import TransactionJournal
from pyline.log.ratelimit import WindowLogLimiter
from pyline.net.gateway import ProtocolGateway
from pyline.net.ipc import BusAuthError, ZmqBus, curve_public_of, service_no_bytes, z85_encode
from pyline.net.network import Network, pack_call
from pyline.net.proxy import FWD_FLAG, MAX_HOPS, ProxyClient, ProxyServer
from pyline.net.tls import build_client_context, build_server_context
from pyline.obs.metrics import shared_counter
from pyline.reload import reload_module
from tests.test_orm_autosave import FakeDB, make_schema

_mtimes = itertools.count(int(time.time()) + 10)
TOKEN = "unit-test-token"


def write_module(tmp_path: Path, source: str, name: str = "hotmod10") -> str:
    path = tmp_path / f"{name}.py"
    path.write_text(source, encoding="utf-8")
    stamp = next(_mtimes)
    os.utime(path, (stamp, stamp))
    return name


def free_tcp_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_bus_ctx(config_dir: Path, tmp_path: Path, *, main: bool, index: int, port: int) -> Context:
    """Bus context bound to a unique endpoint (TCP on win32, ipc elsewhere)."""
    settings = load_project_settings(config_dir)
    zeromq = settings.zeromq.model_copy(
        update={
            "bind_host": f"tcp://127.0.0.1:{port}",
            "bind_file": f"ipc://{tmp_path}/bus-{port}.ipc",
        }
    )
    settings = settings.model_copy(update={"zeromq": zeromq})
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=registry.entry(10001),
        process_type=PROCESS_MAIN if main else "db",
        process_index=index,
        main_pid=0,
    )


# --------------------------------------------------------------------------- #
# F-165: tracked containers wrap inserted values
# --------------------------------------------------------------------------- #


class TestTrackedInsertsF165:
    async def test_setitem_wraps_nested_plain_containers(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"items": {}})
        assert scheduler.queue_depth() == 1
        await scheduler.flush_all(timeout=1.0)

        # THE regression: inserting a plain dict used to leave it untracked,
        # so the inner mutation below never marked the saver dirty.
        saver.data["items"]["sword"] = {"level": 1}
        assert scheduler.queue_depth() == 1, "nested mutation after insert must mark dirty"

    async def test_list_append_wraps_and_deep_mutation_notifies(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"bag": []})
        await scheduler.flush_all(timeout=1.0)
        saver.data["bag"].append({"slot": 3})
        assert scheduler.queue_depth() == 1
        await scheduler.flush_all(timeout=1.0)
        saver.data["bag"][0]["slot"] = 9  # deep mutation inside the appended item
        assert scheduler.queue_depth() == 1

    def test_touchless_copy_does_not_wrap(self) -> None:
        """asdict-style copies (touch=None) keep plain values plain."""
        from pyline.db.tracked import TrackedDict

        td = TrackedDict({"a": {"b": 1}}, touch=None)
        assert isinstance(td["a"], dict) and not isinstance(td["a"], TrackedDict)

    def test_update_and_setdefault_wrap(self) -> None:
        from pyline.db.tracked import TrackedDict, TrackedList

        marks: list[str] = []
        td = TrackedDict(touch=lambda: marks.append("x"))
        td.update(one={"deep": 1})
        td.setdefault("two", [1, 2])
        assert isinstance(td["one"], TrackedDict)
        assert isinstance(td["two"], TrackedList)
        td["one"]["deep"] = 2  # notifies through the wrapped child
        assert marks


# --------------------------------------------------------------------------- #
# F-166/F-168: hot reload closure refresh + module-value visibility
# --------------------------------------------------------------------------- #

DECORATED_V1 = textwrap.dedent(
    """
    import functools

    def deco(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper

    @deco
    def business(x):
        return x + 1

    TUNING = 10
    """
)
DECORATED_V2 = DECORATED_V1.replace("return x + 1", "return x + 100").replace(
    "TUNING = 10", "TUNING = 20"
)


class TestReloadClosuresF166:
    def test_decorated_function_picks_up_new_inner(self, tmp_path, monkeypatch) -> None:
        sys.path.insert(0, str(tmp_path))
        monkeypatch.syspath_prepend(str(tmp_path))
        name = write_module(tmp_path, DECORATED_V1)
        try:
            import importlib

            module = importlib.import_module(name)
            assert module.business(1) == 2
            write_module(tmp_path, DECORATED_V2)
            reload_module(name)
            # F-166: the wrapper's cell still points at the OLD inner
            # function object -- but that object's code must now be the NEW
            # one. Before the fix this kept returning 2 (silent no-op edit).
            assert module.business(1) == 101
        finally:
            sys.modules.pop(name, None)

    def test_state_cells_keep_old_value(self, tmp_path, monkeypatch) -> None:
        """Non-function cells (state) keep the old value: documented."""
        v1 = textwrap.dedent(
            """
            def make():
                factor = 2
                def scaled(x):
                    return x * factor
                return scaled
            scaled = make()
            """
        )
        sys.path.insert(0, str(tmp_path))
        monkeypatch.syspath_prepend(str(tmp_path))
        name = write_module(tmp_path, v1)
        try:
            import importlib

            module = importlib.import_module(name)
            write_module(tmp_path, v1.replace("factor = 2", "factor = 3"))
            reload_module(name)
            assert module.scaled(5) == 10  # state preserved, not re-bound
        finally:
            sys.modules.pop(name, None)


class TestReloadVisibilityF168:
    def test_changed_module_value_is_logged(self, tmp_path, monkeypatch, caplog) -> None:
        sys.path.insert(0, str(tmp_path))
        monkeypatch.syspath_prepend(str(tmp_path))
        name = write_module(tmp_path, DECORATED_V1)
        try:
            import importlib

            module = importlib.import_module(name)
            assert module.TUNING == 10
            with caplog.at_level(logging.INFO, logger="pyline.reload.inplace"):
                write_module(tmp_path, DECORATED_V2)
                reload_module(name)
            assert module.TUNING == 10  # runtime state: not clobbered
            assert any(
                "kept the live value" in r.message and "TUNING" in r.message for r in caplog.records
            ), "F-168: the kept value must be visible in the log"
        finally:
            sys.modules.pop(name, None)


# --------------------------------------------------------------------------- #
# F-169/F-170: bus auth ack + idempotent close
# --------------------------------------------------------------------------- #


class TestBusAuthAckF169:
    async def test_reply_alone_does_not_complete_handshake(self, config_dir, tmp_path):
        """F-169: the DEALER must not mark itself authenticated on its own
        AUTH2 send -- only the ROUTER's AUTH3 confirmation counts."""
        ctx = make_bus_ctx(config_dir, tmp_path, main=False, index=1, port=free_tcp_port())
        bus = ZmqBus(ctx, ProtocolGateway())
        try:
            from pyline.net.auth import NONCE_LEN, hmac_digest

            client_nonce = b"\x01" * NONCE_LEN
            server_nonce = b"\x02" * NONCE_LEN
            token = ctx.settings.socket.token.get_secret_value()
            bus._client_nonce = client_nonce
            payload = server_nonce + hmac_digest(token, client_nonce, server_nonce)
            bus._dealer_auth_reply(payload)
            assert not bus._auth_ok, "AUTH2 enqueued must NOT self-complete the handshake"
            assert not bus._authed_event.is_set()
            assert bus._peer_queues[-1].qsize() == 1  # the AUTH2 was queued
            bus._dealer_auth_ack()  # the ROUTER confirmed
            assert bus._auth_ok and bus._authed_event.is_set()
        finally:
            await bus.close()

    async def test_control_frames_bypass_queue_bounds(self, config_dir, tmp_path):
        """F-169: a saturated data queue must not refuse a handshake frame."""
        ctx = make_bus_ctx(config_dir, tmp_path, main=False, index=1, port=free_tcp_port())
        bus = ZmqBus(ctx, ProtocolGateway())
        try:
            assert bus._enqueue(-1, [b"", b"", b"f", b"x"])  # create the peer queue
            # Saturate the byte budget: data frames are refused from here on.
            bus._peer_queue_bytes[-1] = bus._settings.queue_bytes
            assert not bus._enqueue(-1, [b"", b"", b"f", b"x"])
            bus._send_auth0()  # forced: admitted past the bound
            assert bus._peer_queues[-1].qsize() == 2
        finally:
            await bus.close()

    @pytest.mark.integration
    async def test_full_handshake_round_trip_with_auth3(self, config_dir, tmp_path):
        port = free_tcp_port()
        ctx_main = make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=port)
        ctx_sub = make_bus_ctx(config_dir, tmp_path, main=False, index=1, port=port)
        bus_main = ZmqBus(ctx_main, ProtocolGateway())
        bus_sub = ZmqBus(ctx_sub, ProtocolGateway())
        try:
            await bus_main.start()
            await bus_sub.start()  # would raise BusAuthError without AUTH3
            assert bus_sub._auth_ok
            assert service_no_bytes(ctx_sub.service_no) in bus_main._authed
        finally:
            await bus_sub.close()
            await bus_main.close()


class TestBusCloseIdempotentF170:
    @pytest.mark.integration
    async def test_double_close_is_a_noop(self, config_dir, tmp_path):
        ctx = make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=free_tcp_port())
        bus = ZmqBus(ctx, ProtocolGateway())
        await bus.start()
        await bus.close()
        await bus.close()  # F-170: used to explode on the second term()
        assert bus._closed


# --------------------------------------------------------------------------- #
# F-171: windowed log limiting
# --------------------------------------------------------------------------- #


class TestRateLimitedLoggingF171:
    def test_window_limiter_admits_burst_then_suppresses(self) -> None:
        limiter = WindowLogLimiter(limit=3, window=10.0)
        assert [limiter.allow() for _ in range(3)] == [True, True, True]
        assert limiter.allow() is False
        assert limiter.take_suppressed() == 1
        assert limiter.take_suppressed() == 0  # reset on read

    async def test_gateway_unknown_flag_flood_is_capped(self, caplog) -> None:
        gw = ProtocolGateway()
        with caplog.at_level(logging.WARNING, logger="pyline.net.gateway"):
            for _ in range(50):
                gw.dispatch("nope", b"x")
        lines = [r for r in caplog.records if "no network registered" in r.message]
        assert 0 < len(lines) <= 5, f"log flood: {len(lines)} lines for 50 bad frames"
        assert gw.unknown_dispatches == 50  # every frame still counted


# --------------------------------------------------------------------------- #
# F-172/F-173/F-174: proxy hop bound, deterministic relay pick, pre-IDENT
# --------------------------------------------------------------------------- #


class _RecordingConn:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    def send_message(self, flag: str, payload: bytes) -> None:
        self.sent.append((flag, payload))


class TestProxyHardeningF172F174:
    async def test_server_forward_checks_hops_at_send(self, config_dir, tmp_path):
        ctx = make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=free_tcp_port())
        server = ProxyServer(ctx, SimpleNamespace())  # router unused on this path
        conn = _RecordingConn()
        server._nodes[10009] = conn  # type: ignore[assignment]
        assert server.forward(110009, 0, "f", b"x", hops=MAX_HOPS - 1) is True
        # F-172: the send side now refuses instead of stamping an unbounded
        # counter that only the receive side validated.
        assert server.forward(110009, 0, "f", b"x", hops=MAX_HOPS) is False
        assert server._hop_drops == 1
        assert len(conn.sent) == 1

    async def test_client_send_checks_hops(self, config_dir, tmp_path):
        from pyline.net.router import NoProxyAvailableError

        ctx = make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=free_tcp_port())
        client = ProxyClient(ctx, SimpleNamespace())
        client._proxies[10009] = _RecordingConn()  # type: ignore[assignment]
        with pytest.raises(NoProxyAvailableError, match="max hops"):
            client.send_to_service(110009, "f", b"x", hops=MAX_HOPS)

    async def test_indirect_relay_is_deterministic_and_counted(self, config_dir, tmp_path):
        """F-173: with no proxy on the target machine, the LOWEST-numbered
        proxy is picked deterministically and the send is counted."""
        ctx = make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=free_tcp_port())
        client = ProxyClient(ctx, SimpleNamespace())
        proxy5 = _RecordingConn()
        proxy2 = _RecordingConn()
        client._proxies[5] = proxy5  # type: ignore[assignment]
        client._proxies[2] = proxy2  # type: ignore[assignment]
        client.send_to_service(999001, "f", b"x")  # machine 999 has no proxy
        assert len(proxy2.sent) == 1 and not proxy5.sent
        assert client.indirect_sends == 1

    async def test_pre_ident_frames_are_counted(self, config_dir, tmp_path, caplog):
        ctx = make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=free_tcp_port())
        server = ProxyServer(ctx, SimpleNamespace())
        with caplog.at_level(logging.WARNING, logger="pyline.net.proxy"):
            server._on_pre_ident_frame(FWD_FLAG, b"junk")
            server._on_pre_ident_frame(FWD_FLAG, b"junk")
        assert server.pre_ident_frames == 2
        assert any("before IDENT" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# F-175/F-176/F-177/F-178/F-179: connection/rpc/network behaviour
# --------------------------------------------------------------------------- #


class TestConnectTimeoutF175:
    async def test_dial_to_blackhole_is_bounded(self) -> None:
        # TEST-NET-1: no route in test environments; the dial must fail at
        # connect_timeout, not at the OS timeout (~21 s on Windows).
        started = time.monotonic()
        with pytest.raises((TimeoutError, OSError)):
            await conn_mod.open_connection(
                "10.255.255.1",
                9,
                token=TOKEN,
                on_message=lambda f, p: None,
                connect_timeout=0.3,
            )
        assert time.monotonic() - started < 3.0


class TestRpcSendPathsF176F177:
    class _RaisingSender:
        def route(self, flag: str, payload: bytes, target: int, **_: object) -> None:
            raise ConnectionError("proxy link closed")

    def test_reply_send_failure_is_contained_and_counted(self) -> None:
        from pyline.net.rpc import RpcManager

        rpc = RpcManager(ProtocolGateway(), self._RaisingSender(), own_service_no=1)
        rpc._send(2, [2, 1, 1, "ok"])  # must not raise (F-177)
        assert rpc.result_send_failures == 1

    def test_send_packed_reuses_payload(self) -> None:
        """F-176: the success path packs once and sends those exact bytes."""
        import msgpack

        from pyline.net.rpc import RpcManager

        sent: list[bytes] = []

        class Sender:
            def route(self, flag: str, payload: bytes, target: int, **_: object) -> None:
                sent.append(payload)

        rpc = RpcManager(ProtocolGateway(), Sender(), own_service_no=1)
        payload = msgpack.packb([2, 7, 1, {"big": b"y" * 4096}], use_bin_type=True)
        rpc._send_packed(2, payload)
        assert sent == [payload]


class TestCloseFlushEventF178:
    async def test_queue_drained_event_tracks_queue_state(self) -> None:
        got: dict = {}
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: got.setdefault(f, []).append(p),
            on_connected=lambda c: None,
            max_connections=16,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        try:
            await asyncio.sleep(0.05)
            assert client._queue_drained.is_set()
            client.send_message("chat", b"hello")
            assert not client._queue_drained.is_set()
            for _ in range(50):
                if got.get("chat"):
                    break
                await asyncio.sleep(0.02)
            for _ in range(50):
                if client._queue_drained.is_set():
                    break
                await asyncio.sleep(0.02)
            assert client._queue_drained.is_set(), "event must fire once the writer drains"
        finally:
            await client.close("done")
            await conn_mod.close_server(server)

    async def test_close_bounded_when_peer_never_reads(self) -> None:
        """A stuck flush must still terminate within flush_timeout."""
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: None,
            on_connected=lambda c: None,
            max_connections=16,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        try:
            await asyncio.sleep(0.05)
            client.send_message("chat", b"x" * 64)
            started = time.monotonic()
            await client.close("done", flush_timeout=0.2)
            assert time.monotonic() - started < 2.0
        finally:
            await conn_mod.close_server(server)


class TestOverflowHookF179:
    async def test_overflow_hook_fires_with_sub_and_count(self) -> None:
        gw = ProtocolGateway()

        class HookedNet(Network):
            flag = "hooked"

            def __init__(self, gateway) -> None:
                super().__init__(gateway, max_inflight=1)
                self.drops: list[tuple[int, int]] = []
                self.gate = asyncio.Event()
                self.subscribe(1, self.on_slow)
                self.set_overflow_hook(self.on_overflow)

            async def on_slow(self) -> None:
                await self.gate.wait()

            def on_overflow(self, sub: int, total: int) -> None:
                self.drops.append((sub, total))

        net = HookedNet(gw)
        net.handle_message("hooked", pack_call(1), from_service=7)
        await asyncio.sleep(0.01)
        net.handle_message("hooked", pack_call(1), from_service=7)
        net.handle_message("hooked", pack_call(1), from_service=7)
        assert net.drops == [(1, 1), (1, 2)]  # F-179: visible, in order
        net.gate.set()


# --------------------------------------------------------------------------- #
# F-180: remote transaction lookup race
# --------------------------------------------------------------------------- #


class TestTxLockRaceF180:
    async def test_execute_rechecks_liveness_under_the_lock(self) -> None:
        from tests.test_transaction import remote_stack

        _access, service, _pool = remote_stack()
        tx_id = await service.rpc_tx_begin()
        record = service._tx[tx_id]
        real_lock = record.lock

        class PopOnEnterLock:
            """Simulates commit/reap popping the record in the window between
            rpc_tx_execute's dict lookup and its lock acquisition."""

            async def __aenter__(self) -> None:
                service._tx.pop(tx_id, None)
                await real_lock.acquire()

            async def __aexit__(self, *exc: object) -> None:
                real_lock.release()

        record.lock = PopOnEnterLock()  # type: ignore[assignment]
        with pytest.raises(TransactionGoneError, match="retry"):
            await service.rpc_tx_execute(tx_id, "SELECT 1", [])
        # restore a clean record so the rollback below disposes the session
        record.lock = real_lock
        service._tx[tx_id] = record
        await service.rpc_tx_rollback(tx_id)


# --------------------------------------------------------------------------- #
# F-181/F-182/F-183: performance-shape regressions
# --------------------------------------------------------------------------- #


class TestBatchPickF181:
    async def test_pick_batch_matches_documented_selection(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)
        savers = [
            DataSaver(db, make_schema(), "tbl_player", "data", i, scheduler=scheduler)
            for i in (1, 2, 3)
        ]
        for s in savers:
            s.set_data({"g": 1})
        await scheduler.flush_all(timeout=1.0)
        for s in savers:
            s.set_data({"g": 2})
        scheduler._inflight.add(savers[0])  # F-151: skipped
        scheduler._deferred[savers[2]] = time.monotonic() + 999  # backed off
        batch = scheduler._pick_batch(time.monotonic())
        assert [s for s, _f in batch] == [savers[1]]


class TestJournalDedupF182:
    def test_note_and_release_are_dedup_and_ordered(self) -> None:
        journal = TransactionJournal()
        marks: list[int] = []

        def make(no: int) -> SimpleNamespace:
            return SimpleNamespace(
                remark_dirty_after_rollback=lambda: marks.append(no),
                requeue_deferred=lambda: marks.append(100 + no),
            )

        a, b = make(1), make(2)
        journal.note_deferred(a)
        journal.note_deferred(a)  # duplicate: one entry only
        journal.note_deferred(b)
        journal.note_flush(a)  # flush supersedes the deferral (F-63)
        assert len(journal.deferred) == 1 and len(journal.flushed) == 1
        journal.remark_rolled_back()
        assert marks == [1, 102]  # rolled-back flush first, then the deferred
        assert not journal.flushed and not journal.deferred


class TestSchedulerBucketsF183:
    async def test_mass_cancel_is_exact_and_buckets_are_dicts(self) -> None:
        from pyline.core.scheduler import Scheduler

        scheduler = Scheduler()
        scheduler.bind_loop(asyncio.get_running_loop())
        handles = [scheduler.call_after(60.0, lambda: None) for _ in range(2000)]
        assert scheduler.pending_count() == 2000
        for handle in handles[:1000]:
            handle.cancel()
        await asyncio.sleep(0.05)
        assert scheduler.pending_count() == 1000  # F-183: exact O(1) removal
        assert all(isinstance(bucket, dict) for bucket in scheduler._wheel.values())
        await scheduler.close()


# --------------------------------------------------------------------------- #
# F-184: shared metric factory
# --------------------------------------------------------------------------- #


class TestSharedCounterF184:
    def test_same_name_returns_same_counter(self) -> None:
        c1 = shared_counter("pyline_test_shared_total", "doc")
        c2 = shared_counter("pyline_test_shared_total", "doc")
        assert c1 is c2

    def test_registry_collision_falls_back_to_live_collector(self) -> None:
        from prometheus_client import Counter

        direct = Counter("pyline_test_collide_total", "doc")
        again = shared_counter("pyline_test_collide_total", "doc")
        assert again is direct


# --------------------------------------------------------------------------- #
# F-185: blob size cap at encode time
# --------------------------------------------------------------------------- #


class TestBlobCapF185:
    async def test_oversized_blob_refused_before_sql(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1)
        saver.set_data({"hole": b"x" * (MAX_BLOB_BYTES + 1)})
        with pytest.raises(ValueError, match="MEDIUMBLOB"):
            await saver.flush()
        assert db.executed == [], "the oversized blob must never reach SQL"


# --------------------------------------------------------------------------- #
# F-186: late dirty marks during the drain are queued
# --------------------------------------------------------------------------- #


class TestLateMarkF186:
    async def test_late_mark_queues_warns_and_drains(self, caplog) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"gold": 1})
        scheduler._quitting = True  # drain begins
        with caplog.at_level(logging.WARNING, logger="pyline.db.autosave"):
            saver.set_data({"gold": 2})  # plain mutation: must not raise
        assert scheduler.queue_depth() == 1
        assert any("late dirty mark" in r.message for r in caplog.records)
        assert await scheduler.flush_all(timeout=1.0) is True


# --------------------------------------------------------------------------- #
# F-187/F-188: TLS on the TCP planes
# --------------------------------------------------------------------------- #


def _generate_pki(tmp_path: Path) -> dict[str, str]:
    """CA + machine cert (CA-signed) + rogue cert (self-signed)."""
    import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "pyline-test-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=1), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    files: dict[str, str] = {}

    def write_pair(name: str, key: Any, cert: x509.Certificate) -> None:
        pem = tmp_path / f"{name}.pem"
        pem.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        keyfile = tmp_path / f"{name}.key"
        keyfile.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            )
        )
        files[f"{name}.pem"] = str(pem)
        files[f"{name}.key"] = str(keyfile)

    (tmp_path / "ca.pem").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    files["ca.pem"] = str(tmp_path / "ca.pem")

    def signed(name: str, by_key: Any, by_name: x509.Name) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .issuer_name(by_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=365))
            .sign(by_key, hashes.SHA256())
        )
        write_pair(name, key, cert)

    signed("machine", ca_key, ca_name)  # legitimate mesh member
    rogue_key = ec.generate_private_key(ec.SECP256R1())
    rogue_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "machine-rogue")])
    rogue_cert = (
        x509.CertificateBuilder()
        .subject_name(rogue_name)
        .issuer_name(rogue_name)  # self-signed: not the CA
        .public_key(rogue_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(rogue_key, hashes.SHA256())
    )
    write_pair("rogue", rogue_key, rogue_cert)
    return files


def _tls_settings(files: dict[str, str], *, cert: str = "machine", mutual: bool = False):
    from pyline.config.models import TlsSettings

    return TlsSettings(
        cert_file=files[f"{cert}.pem"],
        key_file=files[f"{cert}.key"],
        ca_file=files["ca.pem"],
        require_client_cert=mutual,
    )


class TestTlsPlanesF187F188:
    async def test_tls_round_trip_on_listener(self, tmp_path) -> None:
        files = _generate_pki(tmp_path)
        server_ctx = build_server_context(_tls_settings(files))
        client_ctx = build_client_context(_tls_settings(files))
        got: dict = {}
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: got.setdefault(f, []).append(p),
            on_connected=lambda c: None,
            ssl_context=server_ctx,
            max_connections=16,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
            ssl_context=client_ctx,
        )
        try:
            assert client.verified
            client.send_message("chat", b"secret")
            for _ in range(50):
                if got.get("chat"):
                    break
                await asyncio.sleep(0.02)
            assert got.get("chat") == [b"secret"]  # HMAC auth ran inside TLS
        finally:
            await client.close("done")
            await conn_mod.close_server(server)

    async def test_mutual_tls_rejects_untrusted_client_cert(self, tmp_path) -> None:
        files = _generate_pki(tmp_path)
        server_ctx = build_server_context(_tls_settings(files, mutual=True))
        rogue_client_ctx = build_client_context(_tls_settings(files, cert="rogue"))
        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=lambda f, p: None,
            on_connected=lambda c: None,
            ssl_context=server_ctx,
            max_connections=16,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=64,
        )
        port = server.sockets[0].getsockname()[1]
        try:
            with pytest.raises((conn_mod.ConnectionClosedError, OSError)):
                await conn_mod.open_connection(
                    "127.0.0.1",
                    port,
                    token=TOKEN,
                    on_message=lambda f, p: None,
                    handshake_timeout=2.0,
                    idle_timeout=30.0,
                    send_queue_limit=64,
                    ssl_context=rogue_client_ctx,
                )
        finally:
            await conn_mod.close_server(server)

    def test_client_context_without_ca_is_refused(self, tmp_path) -> None:
        from pyline.config.models import TlsSettings

        files = _generate_pki(tmp_path)
        settings = TlsSettings(
            cert_file=files["machine.pem"], key_file=files["machine.key"], ca_file=None
        )
        with pytest.raises(ConfigError, match="ca_file"):
            build_client_context(settings)

    def test_missing_cert_file_fails_fast(self, tmp_path) -> None:
        from pyline.config.models import TlsSettings

        settings = TlsSettings(
            cert_file=str(tmp_path / "nope.pem"), key_file=str(tmp_path / "nope.key")
        )
        with pytest.raises(ConfigError, match="does not exist"):
            build_server_context(settings)

    async def test_config_loader_resolves_tls_paths(self, tmp_path) -> None:
        cfg = tmp_path / "aioconfig"
        cfg.mkdir()
        certs = tmp_path / "certs"
        certs.mkdir()
        (certs / "machine.pem").write_text("placeholder", encoding="utf-8")
        (certs / "machine.key").write_text("placeholder", encoding="utf-8")
        (cfg / "project.json5").write_text(
            textwrap.dedent(
                """
                {
                    "project": "tls-test",
                    "srv_type": "develop",
                    "socket": {
                        "token": "$plain:t",
                        "client_port": 11520,
                        "server_port": 12520,
                        "max_connections": 32,
                        "tls": {"cert_file": "certs/machine.pem",
                                "key_file": "certs/machine.key"},
                    },
                    "mysql": {"user": "root", "password": "$plain:t",
                              "db_name": "d"},
                    "redis": {"password": "$plain:t"},
                }
                """
            ),
            encoding="utf-8",
        )
        settings = load_project_settings(cfg)
        assert settings.socket.tls is not None
        resolved = Path(settings.socket.tls.cert_file)
        assert resolved.is_absolute() and resolved == (certs / "machine.pem").resolve()


# --------------------------------------------------------------------------- #
# F-189: CURVE + ZAP on the ZMQ bus
# --------------------------------------------------------------------------- #


def _curve_pair() -> tuple[str, str]:
    import zmq

    public, secret = zmq.curve_keypair()
    return public.decode("ascii"), secret.decode("ascii")


class TestZ85F189:
    def test_z85_known_vectors(self) -> None:
        assert z85_encode(b"\x00\x00\x00\x00") == "00000"
        assert z85_encode(bytes.fromhex("864FD26FB559F75B")) == "HelloWorld"

    def test_curve_public_of_roundtrip(self) -> None:
        public, secret = _curve_pair()
        assert curve_public_of(secret) == public


def _curved_copy(
    base: Context, curve: object, *, main: bool, index: int, port: int, tmp_path: Path
) -> Context:
    from pyline.config.models import CurveSettings

    assert isinstance(curve, CurveSettings)
    settings = base.settings.model_copy(
        update={
            "zeromq": base.settings.zeromq.model_copy(
                update={
                    "bind_host": f"tcp://127.0.0.1:{port}",
                    "bind_file": f"ipc://{tmp_path}/bus-{port}.ipc",
                    "curve": curve,
                    "auth_timeout": 1.5,
                }
            )
        }
    )
    return Context(
        settings=settings,
        registry=base.registry,
        tables=base.tables,
        entry=base.entry,
        process_type=PROCESS_MAIN if main else "db",
        process_index=index,
        main_pid=0,
    )


class TestCurveBusF189:
    @pytest.mark.integration
    async def test_full_curve_mesh_round_trip(self, config_dir, tmp_path) -> None:
        import zmq

        if not zmq.has("curve"):
            pytest.skip("libzmq without CURVE support")
        from pyline.config.models import CurveSettings

        _server_pub, server_sec = _curve_pair()
        _client_pub, client_sec = _curve_pair()
        port = free_tcp_port()
        curve = CurveSettings(
            server_secret=SecretStr(server_sec), client_secret=SecretStr(client_sec)
        )
        ctx_main = _curved_copy(
            make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=port),
            curve,
            main=True,
            index=0,
            port=port,
            tmp_path=tmp_path,
        )
        ctx_sub = _curved_copy(
            make_bus_ctx(config_dir, tmp_path, main=False, index=1, port=port),
            curve,
            main=False,
            index=1,
            port=port,
            tmp_path=tmp_path,
        )
        bus_main = ZmqBus(ctx_main, ProtocolGateway())
        bus_sub = ZmqBus(ctx_sub, ProtocolGateway())
        try:
            await bus_main.start()
            await asyncio.sleep(0.1)
            await bus_sub.start()  # CURVE + ZAP + HMAC all pass
            assert bus_sub._auth_ok

            box: dict = {}

            class PingNet(Network):
                flag = "curve-test"

                def __init__(self, gateway) -> None:
                    super().__init__(gateway)
                    self.subscribe(1, self.on_ping)

                async def on_ping(self, value: str) -> None:
                    box["got"] = value

            PingNet(bus_main._gateway)
            bus_sub.send(ctx_main.service_no, "curve-test", pack_call(1, "through-curve"))
            for _ in range(100):
                if box.get("got"):
                    break
                await asyncio.sleep(0.02)
            assert box.get("got") == "through-curve"
        finally:
            await bus_sub.close()
            await bus_main.close()

    @pytest.mark.integration
    async def test_unlisted_client_key_cannot_authenticate(self, config_dir, tmp_path) -> None:
        import zmq

        if not zmq.has("curve"):
            pytest.skip("libzmq without CURVE support")
        from pyline.config.models import CurveSettings

        _server_pub, server_sec = _curve_pair()
        _listed_pub, listed_sec = _curve_pair()
        _rogue_pub, rogue_sec = _curve_pair()  # NOT in the allowlist
        port = free_tcp_port()

        bus_main = ZmqBus(
            _curved_copy(
                make_bus_ctx(config_dir, tmp_path, main=True, index=0, port=port),
                CurveSettings(
                    server_secret=SecretStr(server_sec), client_secret=SecretStr(listed_sec)
                ),
                main=True,
                index=0,
                port=port,
                tmp_path=tmp_path,
            ),
            ProtocolGateway(),
        )
        bus_rogue = ZmqBus(
            _curved_copy(
                make_bus_ctx(config_dir, tmp_path, main=False, index=1, port=port),
                CurveSettings(
                    server_secret=SecretStr(server_sec), client_secret=SecretStr(rogue_sec)
                ),
                main=False,
                index=1,
                port=port,
                tmp_path=tmp_path,
            ),
            ProtocolGateway(),
        )
        try:
            await bus_main.start()
            await asyncio.sleep(0.1)
            with pytest.raises(BusAuthError):
                await bus_rogue.start()  # ZAP 400 -> handshake never completes
            assert bus_main._zap_rejects >= 1
        finally:
            await bus_rogue.close()
            await bus_main.close()

    def test_bad_key_length_fails_validation(self) -> None:
        from pyline.config.models import CurveSettings

        with pytest.raises(ConfigError, match="40-char z85"):
            CurveSettings(server_secret=SecretStr("short"), client_secret=SecretStr("k" * 40))
