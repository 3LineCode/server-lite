"""Eleventh review pass (F-190..F-228): regression tests.

Every behavioural fix from the eleventh pass has at least one test here;
declarative/config-only changes (pyproject floor, doc alignment) are
exercised through their load paths instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets as pysecrets
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

import msgpack
import pytest

import pyline.net.connection as conn_mod
from pyline.config.errors import ConfigError
from pyline.core.events import ClientConnectedEvent, ClientDisconnectedEvent, EventBus
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import DataSaver, SaverDeletedError, SaverHeldByTransactionError
from pyline.db.schema import TableCatalog, _parse_type
from pyline.db.tracked import TrackedDict, bind_tracking
from pyline.net.gateway import ProtocolGateway
from pyline.net.network import Network, pack_call
from pyline.net.protocol import encode_message
from tests.test_ipc import _DrainSocket, make_ctx
from tests.test_orm_autosave import FakeDB, make_schema

TOKEN = "unit-test-token"


# --------------------------------------------------------------------------- #
# F-190: bus identity-inheritance / authenticated liveness
# --------------------------------------------------------------------------- #


class TestBusIdentityLivenessF190:
    def _router_bus(self, config_dir, tmp_path) -> tuple[Any, _DrainSocket]:
        from pyline.net.ipc import ZmqBus

        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway())
        socket = _DrainSocket()
        bus._socket = socket  # type: ignore[assignment]
        return bus, socket

    async def test_auth0_revokes_and_rechallenges(self, config_dir, tmp_path) -> None:
        """The heart of F-190: an AUTH0 from an authenticated identity no
        longer no-ops -- it demotes and forces a fresh proof. An impostor
        that inherited the identity's ROUTING cannot answer the challenge,
        so its data frames hit the unauthenticated gate."""
        from pyline.net.auth import hmac_digest, inter_token
        from pyline.net.ipc import BUS_AUTH0, BUS_AUTH2, NONCE_LEN

        bus, socket = self._router_bus(config_dir, tmp_path)
        try:
            identity = (424242).to_bytes(4, "big")  # service 424242
            token = inter_token(bus._ctx)

            async def handshake() -> None:
                bus._router_auth_step(identity, BUS_AUTH0, pysecrets.token_bytes(NONCE_LEN))
                await asyncio.sleep(0.02)
                server_nonce = socket.sent[-1][3][:NONCE_LEN]
                bus._router_auth_step(identity, BUS_AUTH2, hmac_digest(token, server_nonce))

            await handshake()
            assert bus._authed  # authenticated with a live deadline

            # An AUTH0 from "the same identity" (which ZMQ may have re-routed
            # to a whole different process) must revoke, not no-op.
            bus._router_auth_step(identity, BUS_AUTH0, pysecrets.token_bytes(NONCE_LEN))
            await asyncio.sleep(0.02)
            assert identity not in bus._authed
            assert socket.sent[-1][2] == b"@busauth1"  # a fresh challenge went out

            # The impostor cannot answer: data frames are dropped unread.
            await bus._on_recv([identity, service_no_bytes_router(bus), identity, b"test", b"x"])
            assert bus.unauthenticated_drops == 1
        finally:
            await bus.close()

    async def test_liveness_expiry_demotes(self, config_dir, tmp_path) -> None:
        """F-190: an authenticated entry whose deadline passed is treated
        exactly like a never-authenticated identity (silent inheritors get
        nothing)."""
        bus, _socket = self._router_bus(config_dir, tmp_path)
        try:
            identity = (424242).to_bytes(4, "big")
            bus._authed[identity] = time.monotonic() - 1.0  # already expired
            await bus._on_recv([identity, service_no_bytes_router(bus), identity, b"test", b"x"])
            assert bus.unauthenticated_drops == 1
        finally:
            await bus.close()

    async def test_revoke_identity_drops_auth(self, config_dir, tmp_path) -> None:
        """F-190: the supervisor-facing revoke (child death) clears both the
        authenticated entry and any in-flight handshake."""
        bus, _socket = self._router_bus(config_dir, tmp_path)
        try:
            identity = (424242).to_bytes(4, "big")
            bus._authed[identity] = time.monotonic() + 60.0
            bus.revoke_identity(424242)
            assert identity not in bus._authed
        finally:
            await bus.close()

    async def test_dealer_answers_unsolicited_probe(self, config_dir, tmp_path) -> None:
        """F-190: the ROUTER's liveness probes arrive with no AUTH0 of ours
        outstanding; the DEALER must still answer (refusing would let our
        entry expire)."""
        from pyline.net.auth import hmac_digest, inter_token
        from pyline.net.ipc import NONCE_LEN, ZmqBus

        dealer_ctx = make_ctx(config_dir, tmp_path, main=False, index=1, port=0)
        dealer = ZmqBus(dealer_ctx, ProtocolGateway())
        drain = _DrainSocket()
        dealer._socket = drain  # type: ignore[assignment]
        try:
            dealer._client_nonce = None  # steady state: no outstanding AUTH0
            server_nonce = pysecrets.token_bytes(NONCE_LEN)
            digest = hmac_digest(inter_token(dealer_ctx), b"", server_nonce)
            dealer._dealer_auth_reply(server_nonce + digest)
            await asyncio.sleep(0.02)
            # AUTH2 went back covering exactly the probe's server nonce.
            assert drain.sent[-1][2] == b"@busauth2"
            assert drain.sent[-1][3] == hmac_digest(inter_token(dealer_ctx), server_nonce)
        finally:
            await dealer.close()


def service_no_bytes_router(bus: Any) -> bytes:
    from pyline.net.ipc import service_no_bytes

    return service_no_bytes(bus._ctx.service_no)


# --------------------------------------------------------------------------- #
# F-191: forwarding-leg drop receipts
# --------------------------------------------------------------------------- #


class TestForwardDropReceiptF191:
    async def test_router_nacks_refused_forward(self, config_dir, tmp_path) -> None:
        """A frame the ROUTER accepted but could not enqueue for its target
        DEALER produces a @busnack to the ORIGINAL sender (bounded to small
        payloads)."""
        from pyline.net.ipc import ZmqBus
        from tests.test_ipc import _BlockingSocket

        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        from dataclasses import replace as _replace

        ctx = _replace(
            ctx,
            settings=ctx.settings.model_copy(
                update={"zeromq": ctx.settings.zeromq.model_copy(update={"queue_bound": 1})}
            ),
        )
        bus = ZmqBus(ctx, ProtocolGateway())
        blocked = _BlockingSocket()
        blocked.gate.clear()  # writer parks mid-send: the queue stays full
        bus._socket = blocked  # type: ignore[assignment]
        try:
            sender = (424243).to_bytes(4, "big")
            target = (424244).to_bytes(4, "big")
            bus._authed[sender] = time.monotonic() + 60.0
            frame = [sender, target, sender, b"game", b"payload"]
            await bus._on_recv(frame)  # dequeued by the writer, held mid-send
            await blocked.in_send.wait()
            await bus._on_recv(frame)  # queued behind the blocked writer
            assert bus.nacks_sent == 0  # bound=1: one QUEUED message fits
            await bus._on_recv(frame)  # refused -> receipt for the sender
            assert bus.nacks_sent == 1
        finally:
            blocked.gate.set()
            await bus.close()

    async def test_rpc_pending_fails_on_receipt(self) -> None:
        """The source RpcManager fails the matching pending CALL immediately
        instead of burning its timeout."""
        from pyline.net.rpc import RpcError, RpcManager, _PendingCall

        gw = ProtocolGateway()
        rpc = RpcManager(gw, _NullSender(), own_service_no=100001)
        call_body = msgpack.packb([1, 7, 100001, "game.fn", [1]], use_bin_type=True)
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        rpc._pending[7] = _PendingCall(
            call_id=7,
            target=200001,
            func="game.fn",
            future=fut,
            timer=loop.call_later(10.0, lambda: None),
            started=time.monotonic(),
        )
        nack = msgpack.packb([200001, 100001, "@rpc", call_body], use_bin_type=True)
        rpc.on_forward_dropped(nack, from_service=100001)
        assert rpc.dropped_forward_calls == 1
        assert fut.done()
        with pytest.raises(RpcError, match="dropped at the bus router"):
            fut.result()


class _NullSender:
    def route(self, flag: str, payload: bytes, target: int, *, raise_on_drop: bool = False):
        raise AssertionError("not used in this test")


# --------------------------------------------------------------------------- #
# F-192/F-194: malformed flags, bool-typed wire ints
# --------------------------------------------------------------------------- #


class TestMalformedAndBoolsF192F194:
    async def test_undecodable_flag_dropped_not_raised(self, config_dir, tmp_path) -> None:
        from pyline.net.ipc import ZmqBus

        ctx = make_ctx(config_dir, tmp_path, main=True, index=0, port=0)
        bus = ZmqBus(ctx, ProtocolGateway())
        bus._socket = _DrainSocket()  # type: ignore[assignment]
        try:
            identity = (424242).to_bytes(4, "big")
            # "\xff\xff" is not utf-8.
            await bus._on_recv(
                [identity, service_no_bytes_router(bus), identity, b"\xff\xff", b"x"]
            )
            assert bus.recv_messages == 1  # consumed, not crashed
        finally:
            await bus.close()

    def test_bool_sub_rejected_by_network(self) -> None:
        from pyline.net.network import unpack_call

        with pytest.raises(ValueError):
            unpack_call(msgpack.packb([True, 1], use_bin_type=True))

    def test_bool_hops_rejected_by_parse_forward(self) -> None:
        from pyline.net.proxy import parse_forward

        envelope = msgpack.packb([1, 1, "g", b"p", True], use_bin_type=True)
        with pytest.raises(ValueError, match="wrong field types"):
            parse_forward(envelope)

    async def test_bool_call_id_rejected_by_rpc(self) -> None:
        from pyline.net.rpc import RpcManager

        gw = ProtocolGateway()
        rpc = RpcManager(gw, _NullSender(), own_service_no=100001)
        message = msgpack.packb([1, True, 100001, "f", []], use_bin_type=True)
        rpc.handle_message("@rpc", message, from_service=200001)
        assert not rpc._inbound  # dropped without spawning anything


# --------------------------------------------------------------------------- #
# F-195/F-196/F-197/F-198: lifecycle and runtime wiring
# --------------------------------------------------------------------------- #


class TestLifecycleGuardsF195F196:
    async def test_wait_boot_step_settled_blocks_until_step_ends(self) -> None:
        from pyline.core.lifecycle import LifecycleManager

        lm = LifecycleManager(step_timeout=5.0)
        lm._boot_step_active = True
        lm._boot_step_done = asyncio.get_running_loop().create_future()
        done = asyncio.Event()

        async def waiter() -> None:
            await lm.wait_boot_step_settled(timeout=5.0)
            done.set()

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.05)
        assert not done.is_set()  # still waiting for the step
        lm._boot_step_done.set_result(None)
        lm._boot_step_active = False
        await asyncio.wait_for(task, timeout=1.0)

    async def test_wait_boot_step_settled_no_step(self) -> None:
        from pyline.core.lifecycle import LifecycleManager

        lm = LifecycleManager()
        await asyncio.wait_for(lm.wait_boot_step_settled(timeout=0.1), timeout=1.0)

    async def test_boot_twice_rejected(self, config_dir, tmp_path) -> None:
        from pyline.runtime import ServerRuntime, build_context

        ctx = build_context(config_dir, 10001, "main", 0, 0)
        runtime = ServerRuntime(ctx)
        runtime._boot_called = True  # a completed boot() claims the runtime
        with pytest.raises(RuntimeError, match="called twice"):
            await runtime.boot()


class TestDbProcessGuardsF197:
    async def test_db_process_skips_business_and_devtools(
        self, config_dir, tmp_path, monkeypatch
    ) -> None:
        import pyline.runtime as runtime_mod
        from pyline.runtime import ServerRuntime, build_context

        ctx = build_context(config_dir, 10001, "db", 1, 0)
        runtime = ServerRuntime(ctx)
        loaded: list[str] = []
        started: list[str] = []

        class _StubBus:
            def __init__(self, ctx, gateway) -> None: ...

            async def start(self) -> None: ...

            async def close(self) -> None: ...

        monkeypatch.setattr(runtime_mod, "ZmqBus", _StubBus)
        monkeypatch.setattr(
            runtime_mod,
            "ProxyClient",
            lambda ctx, router: type("P", (), {"close": lambda self: None})(),
        )
        runtime._load_business = lambda: loaded.append("business")  # type: ignore[method-assign]
        runtime.save_scheduler.start = lambda: started.append("save")  # type: ignore[method-assign]
        runtime.devtools.start = lambda **kw: started.append("devtools")  # type: ignore[method-assign]
        events: list[Any] = []
        runtime.bus.subscribe(object, lambda e: events.append(e), layer=0)
        await runtime._step_frame_init()
        assert loaded == []  # F-197: no business import in the DB process
        assert not any(type(e).__name__ == "FrameInitEvent" for e in events)
        await runtime._step_func_done()
        assert started == []  # no save scheduler / devtools / FuncDone fanout


class TestClockChainStopF198:
    async def test_stop_prevents_rearm(self) -> None:
        from pyline.core.clock import GameClock
        from pyline.core.scheduler import Scheduler
        from pyline.runtime_wiring import ClockEventEmitter

        scheduler = Scheduler()
        scheduler.bind_loop(asyncio.get_running_loop())
        emitter = ClockEventEmitter(
            GameClock(), scheduler, EventBus(), log_dir=_tmp_log_dir(), spawn=_spawn_noop
        )
        emitter.start()
        assert scheduler.pending_count() == 1
        await emitter.stop()
        # The already-armed timer stays (scheduler.close owns cancelling it);
        # what stop() guarantees is that no FURTHER boundary is armed.
        emitter._schedule_next()
        emitter._schedule_next()
        assert scheduler.pending_count() == 1

    async def test_drain_bg_tasks_excludes_current(self, config_dir) -> None:
        from pyline.runtime import ServerRuntime, build_context

        ctx = build_context(config_dir, 10001, "main", 0, 0)
        runtime = ServerRuntime(ctx)
        flag = asyncio.Event()
        victim = asyncio.get_running_loop().create_task(flag.wait())
        runtime._bg_tasks.add(victim)
        inside = asyncio.Event()

        async def teardown_path() -> None:
            inside.set()
            await runtime._drain_bg_tasks()
            assert victim.cancelled() or victim.done()

        task = asyncio.get_running_loop().create_task(teardown_path())
        await inside.wait()
        await asyncio.sleep(0.05)
        assert not task.cancelled()  # draining did not cancel the caller
        flag.set()
        await asyncio.wait_for(task, timeout=1.0)


def _tmp_log_dir() -> Path:
    import tempfile

    return Path(tempfile.mkdtemp())


def _spawn_noop(coro):
    return asyncio.get_running_loop().create_task(coro)


# --------------------------------------------------------------------------- #
# F-202: MRO cache bound
# --------------------------------------------------------------------------- #


class TestMroCacheBoundF202:
    async def test_cache_capped(self) -> None:
        from pyline.core import events as events_mod

        bus = EventBus()
        bus.subscribe(events_mod.NewHourEvent, lambda e: None)
        for i in range(events_mod._MRO_CACHE_MAX + 10):
            event_type = type(f"E{i}", (object,), {})
            bus._subscription_keys(event_type)
        assert len(bus._mro_cache) <= events_mod._MRO_CACHE_MAX


# --------------------------------------------------------------------------- #
# F-205: transaction-hold guard on explicit flush/delete
# --------------------------------------------------------------------------- #


class TestTransactionHoldGuardF205:
    async def test_flush_outside_unit_rejected(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1)
        saver.set_data({"x": 1})
        from pyline.db.transaction import TransactionJournal

        journal = TransactionJournal()
        saver._pending_journal = journal  # deferred by an open transaction
        with pytest.raises(SaverHeldByTransactionError):
            await saver.flush()
        assert db.rows == {}  # nothing autocommitted

    async def test_flush_with_force_is_the_drain_path(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1)
        saver.set_data({"x": 1})
        from pyline.db.transaction import TransactionJournal

        saver._pending_journal = TransactionJournal()
        await saver.flush(force=True)  # shutdown drain semantics
        assert db.rows.get(("tbl_player", 1)) is not None

    async def test_delete_outside_unit_rejected(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1)
        saver.set_data({"x": 1})
        await saver.flush()
        from pyline.db.transaction import TransactionJournal

        saver._pending_journal = TransactionJournal()
        with pytest.raises(SaverHeldByTransactionError):
            await saver.delete()


# --------------------------------------------------------------------------- #
# F-206: flush-round deadline
# --------------------------------------------------------------------------- #


class _SlowDB(FakeDB):
    """The DB-down shape: the first (coalesced) statement fails instantly,
    every fallback leg costs `delay` seconds."""

    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay
        self.first_done = False

    async def execute(self, sql: str, args: tuple = ()) -> int:
        if not self.first_done and "INSERT" in sql and "VALUES" in sql:
            self.first_done = True
            raise ConnectionError("db down")
        await asyncio.sleep(self.delay)
        return await super().execute(sql, args)


class TestFlushRoundDeadlineF206:
    async def test_deadline_requeues_rest(self) -> None:
        db = _SlowDB(delay=0.3)
        scheduler = SaveScheduler(interval=60.0, batch_size=10, flush_round_timeout=0.4)
        savers = []
        for key in range(4):
            saver = DataSaver(db, make_schema(), "tbl_player", "data", key, scheduler=scheduler)
            saver.set_data({"k": key})
            savers.append(saver)
        started = time.monotonic()
        await scheduler.flush_batch()
        elapsed = time.monotonic() - started
        # The first coalesced statement (0.3s) plus one fallback leg (0.3s)
        # crosses the 0.4s deadline; the remaining rows must be requeued
        # instead of each burning another full leg (~1.2s+ total).
        assert elapsed < 0.9, f"round ran {elapsed:.2f}s; deadline not enforced"
        assert scheduler.queue_depth() >= 1  # never dropped
        assert scheduler.failed_total + scheduler.saved_total >= 1


# --------------------------------------------------------------------------- #
# F-207: type grammar + primary-key drift
# --------------------------------------------------------------------------- #


class TestTypeGrammarF207:
    def test_unsigned_and_multiwidth(self) -> None:
        assert _parse_type("INT UNSIGNED") == ("INT UNSIGNED", None)
        assert _parse_type("DECIMAL(10,2)") == ("DECIMAL", "10,2")
        assert _parse_type("BIGINT") == ("BIGINT", None)

    def test_enum_values_verbatim(self) -> None:
        name, length = _parse_type("ENUM('a','b')")
        assert name == "ENUM"
        assert length == "'a','b'"

    def test_injection_refused(self) -> None:
        with pytest.raises(Exception, match=r"parenthesised|unparseable"):
            _parse_type("INT(11) DROP TABLE x--")

    async def test_drift_flags_missing_primary(self) -> None:
        from pyline.config.models import TableDef, TableFieldDef
        from pyline.db.schema import SchemaManager
        from tests.test_schema import SchemaFakePool

        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={
                # id exists but is NOT the live primary key anymore.
                "tbl_player": [("id", "bigint", "NO", ""), ("data", "mediumblob", "YES", "")]
            },
        )
        manager = SchemaManager(
            pool,
            {
                "tbl_player": TableDef(
                    fields={
                        "id": TableFieldDef(type="BIGINT", primary=True),
                        "data": TableFieldDef(type="MEDIUMBLOB"),
                    }
                )
            },
            "test_db",
        )
        with pytest.raises(Exception, match="PRIMARY"):
            await manager.ensure_all()


# --------------------------------------------------------------------------- #
# F-211: TableCatalog ODKU selector
# --------------------------------------------------------------------------- #


class TestCatalogOdkuF211:
    def test_alias_form(self, config_dir) -> None:
        from pyline.config.loader import load_table_defs

        tables = load_table_defs(config_dir)
        catalog = TableCatalog(tables, odku_alias=True)
        sql = catalog.table("tbl_player").upsert_sql("data")
        assert "AS `_new`" in sql and "`_new`.`data`" in sql

    def test_default_legacy(self, config_dir) -> None:
        from pyline.config.loader import load_table_defs

        catalog = TableCatalog(load_table_defs(config_dir))
        sql = catalog.table("tbl_player").upsert_sql("data")
        assert "VALUES(`data`)" in sql


# --------------------------------------------------------------------------- #
# F-213: redis liveness probe
# --------------------------------------------------------------------------- #


class _FakeRedis:
    def __init__(self, fail: bool) -> None:
        self.fail = fail

    async def ping(self) -> bool:
        if self.fail:
            raise ConnectionError("dead")
        return True


class TestRedisKeepaliveF213:
    async def test_lost_alarm_after_misses(self, config_dir, tmp_path, monkeypatch) -> None:
        from pyline.config.loader import load_project_settings
        from pyline.db.redis import RedisClient

        settings = load_project_settings(config_dir).redis.model_copy(
            update={"keepalive_interval": 0.05, "keepalive_miss_limit": 3}
        )
        lost: list[str] = []
        client = RedisClient(settings, on_lost=lambda: lost.append("lost"))
        client._client = _FakeRedis(fail=True)
        task = asyncio.get_running_loop().create_task(client._keepalive())
        try:
            await asyncio.sleep(0.4)
            assert lost == ["lost"]  # exactly one alarm per outage
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_recovery_rearms_alarm(self, config_dir, monkeypatch) -> None:
        from pyline.config.loader import load_project_settings
        from pyline.db.redis import RedisClient

        settings = load_project_settings(config_dir).redis.model_copy(
            update={"keepalive_interval": 0.05, "keepalive_miss_limit": 2}
        )
        lost: list[str] = []
        client = RedisClient(settings, on_lost=lambda: lost.append("lost"))
        fake = _FakeRedis(fail=True)
        client._client = fake
        task = asyncio.get_running_loop().create_task(client._keepalive())
        try:
            await asyncio.sleep(0.2)
            assert lost == ["lost"]
            fake.fail = False  # redis-py "reconnected"
            await asyncio.sleep(0.1)
            fake.fail = True
            await asyncio.sleep(0.2)
            assert lost == ["lost", "lost"]  # second outage alarms again
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# --------------------------------------------------------------------------- #
# F-215/F-216/F-217/F-218: codec and container edges
# --------------------------------------------------------------------------- #


class TestCodecAndContainersF215toF218:
    def test_unknown_keys_warned(self, caplog) -> None:
        from dataclasses import dataclass

        from pyline.db.orm import dataclass_codec

        @dataclass
        class Model:
            a: int = 0

        codec = dataclass_codec(Model)
        # A blob written by an older shape of the model (extra key) --
        # injected at the msgpack layer, exactly like a stale row.
        blob = codec._inner.encode({"a": 1, "gone": 2})
        with caplog.at_level(logging.WARNING):
            out = codec.decode(blob)
        assert out.a == 1
        assert any("gone" in r.message for r in caplog.records)

    def test_subclass_downgrade_warned_once(self, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            first = bind_tracking(OrderedDict({"a": 1}), lambda: None)
            second = bind_tracking(OrderedDict({"b": 2}), lambda: None)
        assert isinstance(first, TrackedDict) and isinstance(second, TrackedDict)
        warnings = [r for r in caplog.records if "OrderedDict" in r.message]
        assert len(warnings) == 1  # once per type, not per instance

    async def test_deleted_mutation_raises_named_error(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1)
        data: dict[str, int] = {}
        saver.set_data(data)
        wrapped = saver.data  # tracked container
        assert isinstance(wrapped, TrackedDict)
        await saver.delete()
        with pytest.raises(SaverDeletedError):
            wrapped["x"] = 1
        assert issubclass(SaverDeletedError, OSError)  # back-compat

    def test_touch_leaves_no_dead_flag(self) -> None:
        from dataclasses import dataclass

        from pyline.db.orm import TrackableModel

        @dataclass
        class M(TrackableModel):
            x: int = 0

        m = M()
        m.x = 5
        assert not hasattr(m, "_dirty")  # F-218: the write-only flag is gone


# --------------------------------------------------------------------------- #
# F-219: process-wide Windows fd budget
# --------------------------------------------------------------------------- #


class TestFdBudgetF219:
    def test_labels_sum_against_one_ceiling(self, monkeypatch) -> None:
        import pyline.net.connection as c

        monkeypatch.setattr(sys, "platform", "win32")
        c.reset_fd_budget_ledger()
        c.check_windows_fd_budget(384, label="client")
        c.check_windows_fd_budget(64, label="proxy")  # 448 == ceiling: fits
        with pytest.raises(ConfigError, match="totals"):
            c.check_windows_fd_budget(64, label="other")  # 512 > 448
        c.reset_fd_budget_ledger()


# --------------------------------------------------------------------------- #
# F-223: protocol-error close reason
# --------------------------------------------------------------------------- #


class TestProtocolErrorReasonF223:
    async def test_bad_magic_names_the_reason(self) -> None:
        import asyncio

        class _BadReader:
            async def read(self, n: int) -> bytes:
                return b"XX-not-the-magic"

        class _Writer:
            def close(self) -> None: ...
            async def wait_closed(self) -> None: ...
            def transport(self):
                class _T:
                    def abort(self) -> None: ...

                return _T()

        class _NoTimer:
            pass

        conn = conn_mod.Connection(
            _BadReader(),  # type: ignore[arg-type]
            _Writer(),  # type: ignore[arg-type]
            peer=("127.0.0.1", 1),
            token=TOKEN,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=4,
            is_server_side=True,
            on_message=lambda f, p: None,
        )
        await asyncio.wait_for(conn._read_loop(), timeout=2.0)
        assert conn.closed
        assert "protocol error" in conn.close_reason


# --------------------------------------------------------------------------- #
# F-224/F-225: per-connection fairness + session layer
# --------------------------------------------------------------------------- #


class _FakeClientConnection:
    """The slice of Connection that Network's fairness accounting touches."""

    def __init__(self, cap: int) -> None:
        self.max_inflight = cap
        self._inflight = 0
        self.peer = ("127.0.0.1", 9)
        self.conn_id = 0

    def admit_inflight(self) -> bool:
        if self._inflight >= self.max_inflight:
            return False
        self._inflight += 1
        return True

    def release_inflight(self) -> None:
        self._inflight = max(0, self._inflight - 1)

    def __repr__(self) -> str:
        return "FakeClientConnection"


class TestPerConnectionFairnessF224:
    async def test_one_connection_cannot_starve_the_pool(self) -> None:
        from pyline.net.session import set_current_connection

        class SlowNet(Network):
            flag = "fair"

            def __init__(self, gateway) -> None:
                super().__init__(gateway, max_inflight=8)
                self.ran = 0
                self.gate = asyncio.Event()
                self.subscribe(1, self.on_slow)

            async def on_slow(self) -> None:
                self.ran += 1
                await self.gate.wait()

        gw = ProtocolGateway()
        net = SlowNet(gw)
        conn = _FakeClientConnection(cap=2)
        token = set_current_connection(conn)  # type: ignore[arg-type]
        try:
            for _ in range(4):
                net.handle_message("fair", pack_call(1))
        finally:
            token.var.reset(token)
        await asyncio.sleep(0.05)
        assert net.ran == 2  # the connection's own cap admitted two
        assert net._overflowed == 2  # the rest dropped, not pooled globally
        net.gate.set()
        await asyncio.sleep(0.05)
        assert conn._inflight == 0  # done callbacks released the slots


class TestSessionLayerF225:
    async def test_registry_lifecycle_and_events(self) -> None:
        from pyline.net.session import ClientSessionRegistry

        registry = ClientSessionRegistry()
        conn = _FakeClientConnection(cap=0)
        conn_id = registry.register(conn)  # type: ignore[arg-type]
        assert conn.conn_id == conn_id == 1
        assert registry.get(conn_id) is conn
        second = registry.register(_FakeClientConnection(cap=0))  # type: ignore[arg-type]
        assert second == 2  # monotonic, no reuse
        registry.unregister(conn)  # type: ignore[arg-type]
        assert registry.get(conn_id) is None
        assert registry.count() == 1

    async def test_serve_disconnect_event_and_context(self) -> None:
        """End-to-end on a real loopback socket: the contextvar reaches the
        handler, the disconnect hook fires exactly once, api.session sees the
        connection while it lives."""
        from pyline.net.session import ClientSessionRegistry, current_connection

        registry = ClientSessionRegistry()
        seen: dict[str, Any] = {}
        disconnected: list[int] = []

        def on_message(flag: str, payload: bytes) -> None:
            seen["flag"] = flag
            seen["conn"] = current_connection()

        def on_connected(conn: conn_mod.Connection) -> None:
            registry.register(conn)

        def on_disconnected(conn: conn_mod.Connection) -> None:
            disconnected.append(conn.conn_id)
            registry.unregister(conn)  # what runtime._on_client_disconnected does

        server = await conn_mod.serve(
            "127.0.0.1",
            0,
            token=TOKEN,
            on_message=on_message,
            on_connected=on_connected,
            on_disconnected=on_disconnected,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=8,
            max_connections=8,
        )
        port = server.sockets[0].getsockname()[1]
        client = await conn_mod.open_connection(
            "127.0.0.1",
            port,
            token=TOKEN,
            on_message=lambda f, p: None,
            handshake_timeout=1.0,
            idle_timeout=30.0,
            send_queue_limit=8,
        )
        await asyncio.sleep(0.1)
        client.send_message("hello", b"world")
        await asyncio.sleep(0.2)
        assert seen["flag"] == "hello"
        assert seen["conn"] is not None and seen["conn"].conn_id == 1
        assert registry.count() == 1
        await client.close("bye")
        await asyncio.sleep(0.2)
        assert disconnected == [1]  # F-225: the disconnect counterpart fired
        assert registry.count() == 0
        await conn_mod.close_server(server)

    async def test_client_connected_event_carries_conn_id(self) -> None:
        event = ClientConnectedEvent(peer=("127.0.0.1", 1), conn_id=7)
        assert event.conn_id == 7
        legacy = ClientConnectedEvent(peer=("127.0.0.1", 2))
        assert legacy.conn_id == 0  # pre-F-225 shape keeps working
        drop = ClientDisconnectedEvent(peer=("127.0.0.1", 2), conn_id=7, reason="idle")
        assert drop.reason == "idle"


# --------------------------------------------------------------------------- #
# F-226: production inter_token must not be $plain:
# --------------------------------------------------------------------------- #


class TestProductionPlainTokenF226:
    def test_plain_inter_token_refused(self, config_dir) -> None:
        import json

        (config_dir / "project.json5").write_text(
            json.dumps(
                {
                    "project": "t",
                    "srv_type": "production",
                    "socket": {
                        "token": "$plain:t",
                        "inter_token": "$plain:oops",
                        "client_port": 11521,
                        "server_port": 12521,
                    },
                    "mysql": {"user": "u", "password": "$plain:p", "db_name": "d"},
                    "redis": {"password": "$plain:r"},
                }
            ),
            encoding="utf-8",
        )
        from pyline.config.loader import load_project_settings

        with pytest.raises(ConfigError, match="inter_token"):
            load_project_settings(config_dir)


# --------------------------------------------------------------------------- #
# F-206 aux: encode_message still fine after the F-221 comment (sanity)
# --------------------------------------------------------------------------- #


def test_encode_message_roundtrip_f221() -> None:
    from pyline.net.protocol import FrameDecoder

    payload = b"x" * 300_000
    frames = encode_message("big", payload, chunk_size=64 * 1024)
    decoder = FrameDecoder(max_frame=16 * 1024 * 1024)
    out: list[Any] = []
    for frame in frames:
        out.extend(decoder.feed(frame))
    assert len(out) == 1 and out[0].payload == payload


class TestApiSessionFacadeF225:
    async def test_facade_roundtrip(self, config_dir, monkeypatch) -> None:
        """api.session: bind, current(), get/send/kick through the registry
        service -- the business-facing half of F-225."""
        from pyline import api as pyline_api
        from pyline.api import session as api_session
        from pyline.runtime import ServerRuntime, build_context

        ctx = build_context(config_dir, 10001, "main", 0, 0)
        runtime = ServerRuntime(ctx)
        pyline_api.bind(ctx)
        try:
            assert api_session.current() is None  # outside dispatch

            class _Conn:
                conn_id = 0
                peer = ("127.0.0.1", 5)
                closed = False

                def send_message(self, flag: str, payload: bytes) -> None:
                    self.sent = (flag, payload)

                async def close(self, reason: str) -> None:
                    self.closed = True
                    self.close_reason = reason

            conn = _Conn()
            registry = runtime.sessions
            registry.register(conn)  # type: ignore[arg-type]
            assert api_session.get(conn.conn_id) is conn
            assert api_session.count() == 1
            assert api_session.all_ids() == [conn.conn_id]
            assert api_session.send(conn.conn_id, "push", b"x")
            assert conn.sent == ("push", b"x")  # type: ignore[attr-defined]
            assert not api_session.send(999, "push", b"x")  # gone -> False
            assert api_session.kick(conn.conn_id, "test-kick")
            await asyncio.sleep(0.05)
            assert conn.closed  # type: ignore[attr-defined]
            registry.unregister(conn)  # type: ignore[arg-type]
            assert not api_session.kick(conn.conn_id)  # gone -> False
        finally:
            pyline_api.unbind()
