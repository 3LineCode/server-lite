"""Ninth review pass (F-155..F-164): one regression test per finding.

The fixes span many subsystems, so the tests live in one module; each class
cites its finding id the same way the per-area suites do.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import itertools
import os
import sys
import time
from pathlib import Path
from typing import Any

import msgpack
import pytest
from pydantic import SecretStr

from pyline.config.errors import ConfigError
from pyline.config.loader import load_project_settings, load_server_registry, load_table_defs
from pyline.config.models import ServerEntry
from pyline.core.context import PROCESS_MAIN, Context
from pyline.core.events import (
    LAYER_BUSINESS,
    LAYER_FRAMEWORK,
    EventBus,
    NewHourEvent,
)
from pyline.core.supervisor import ChildDiedError, ProcessSupervisor, _pid_alive
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import DataSaver, SaveState
from pyline.db.schema import SchemaError
from pyline.db.serialization import BlobFormatError, dumps
from pyline.db.transaction import bind_transaction
from pyline.net.auth import inter_token
from pyline.net.gateway import ProtocolGateway
from pyline.net.rpc import BUSY_MESSAGE, MSG_RESULT, RpcManager
from pyline.obs.metrics import AlarmHub
from pyline.reload import ReloadRejected, reload_module
from pyline.runtime_wiring import ClockEventEmitter, DbLayer
from tests.test_orm_autosave import FakeDB, make_schema
from tests.test_supervisor import _FakeChild, _quiet_child

_mtime_seq = itertools.count(1)


def write_module(tmp_path: Path, source: str) -> None:
    """Same discipline as tests.test_reload.write_module (mtime bump so the
    source cache cannot serve the old bytes on coarse-mtime filesystems)."""
    path = tmp_path / "hotmod9.py"
    path.write_text(source, encoding="utf-8")
    stamp = next(_mtime_seq)
    os.utime(path, (stamp, stamp))


@pytest.fixture()
def hotmod9(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delitem(sys.modules, "hotmod9", raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(tmp_path, "def f(a, b=1):\n    return (a, b)\n")
    import hotmod9

    return hotmod9


# --------------------------------------------------------------------------- #
# F-155: reload signature guard -- default POSITIONS, not counts
# --------------------------------------------------------------------------- #


class TestReloadDefaultsPositionF155:
    def test_rejects_default_moved_off_shared_param(self, hotmod9, tmp_path: Path) -> None:
        """``def f(a, b=1)`` -> ``def f(a, b, c=1)``: prefix matches, default
        COUNT matches (1 == 1), yet every old ``f(1)`` caller TypeErrors
        because ``b`` lost its default. Positions are what callers depend on."""
        write_module(tmp_path, "def f(a, b, c=1):\n    return (a, b, c)\n")
        with pytest.raises(ReloadRejected, match="lost their defaults"):
            reload_module("hotmod9")
        assert hotmod9.f(1) == (1, 1)  # old code untouched

    def test_rejects_plain_default_removal(self, hotmod9, tmp_path: Path) -> None:
        write_module(tmp_path, "def f(a, b):\n    return (a, b)\n")
        with pytest.raises(ReloadRejected, match="lost their defaults"):
            reload_module("hotmod9")

    def test_accepts_compatible_extra_and_gained_default(self, hotmod9, tmp_path: Path) -> None:
        """``f(a, b=1)`` -> ``f(a, b=1, c=2)`` is backward compatible, and so
        is a shared parameter GAINING a default -- neither may be rejected."""
        write_module(tmp_path, "def f(a, b=1, c=2):\n    return (a, b, c)\n")
        reload_module("hotmod9")
        assert hotmod9.f(1) == (1, 1, 2)
        write_module(tmp_path, "def f(a, b=1, c=2):\n    return (a, b, c)\n")
        reload_module("hotmod9")  # stable

    def test_rejects_new_extra_without_default(self, hotmod9, tmp_path: Path) -> None:
        """``def f(a)`` -> ``def f(a, b)``: the extra positional has no
        default, so old callers passing one argument break."""
        sys.modules.pop("hotmod9", None)
        write_module(tmp_path, "def f(a):\n    return (a,)\n")
        # Windows flake discipline (see test_reload._rewire): drop the cached
        # FileFinder so the reimport re-lists the rewritten module.
        sys.path_importer_cache.pop(str(tmp_path), None)
        import hotmod9 as mod

        assert mod.f(1) == (1,)
        write_module(tmp_path, "def f(a, b):\n    return (a, b)\n")
        with pytest.raises(ReloadRejected, match="must have defaults"):
            reload_module("hotmod9")


# --------------------------------------------------------------------------- #
# F-156/F-160: savers work in the sub-process topology + supervisor escalation
# --------------------------------------------------------------------------- #


def _split_ctx(config_dir: Path, *, process_type: str = "game") -> Context:
    settings = load_project_settings(config_dir)
    registry = load_server_registry(config_dir)
    tables = load_table_defs(config_dir)
    entry = ServerEntry(
        server_no=10001,
        name="split",
        advertise_ip="127.0.0.1",
        client_port=1520,
        server_port=2520,
        sub_process=("db", "game"),
    )
    return Context(
        settings=settings,
        registry=registry,
        tables=tables,
        entry=entry,
        process_type=process_type,
        process_index=2,
        main_pid=0,
    )


class TestRemoteSaverFactoryF156:
    async def test_business_process_gets_saver_factory(self, config_dir: Path) -> None:
        """A non-owns-db business process used to have no ``schema`` service,
        so ``api.orm.make_saver`` raised ApiServiceUnavailableError in exactly
        the processes where business code runs. The pool-less TableCatalog
        now backs the factory; statements ride the RPC-backed access."""
        ctx = _split_ctx(config_dir)
        assert not ctx.is_db_process
        layer = DbLayer(ctx, AlarmHub())
        access = await layer.connect(object())  # type: ignore[arg-type]
        factory = layer.saver_factory(access, SaveScheduler())
        assert factory is not None
        saver = factory("tbl_player", "data", 7)
        assert isinstance(saver, DataSaver)
        assert saver.executor is access
        with pytest.raises(KeyError):
            factory("tbl_player", "no_such_column", 7)
        with pytest.raises(SchemaError):
            factory("no_such_table", "data", 7)

    async def test_mysql_disabled_process_gets_no_factory(self, config_dir: Path) -> None:
        ctx = _split_ctx(config_dir)
        ctx.entry = ctx.entry.model_copy(update={"use_mysql": False})
        layer = DbLayer(ctx, AlarmHub())
        await layer.connect(object())  # type: ignore[arg-type]
        assert layer.saver_factory(object(), SaveScheduler()) is None


class TestSupervisorEscalationF160:
    async def test_no_callback_death_escalates_before_raise(self) -> None:
        """The no-callback fail-fast used to terminate the siblings and raise
        ChildDiedError inside the fire-and-forget watch task -- the main
        runtime was never told to stop. The escalation hook runs first."""
        sup = ProcessSupervisor(_quiet_child)
        sup._children = {"db": _FakeChild(7), "game": _FakeChild(None)}
        escalated: list[tuple[str, int | None]] = []

        async def hook(process_type: str, exitcode: int | None) -> None:
            escalated.append((process_type, exitcode))

        with pytest.raises(ChildDiedError):
            await sup._watch_children(None, hook)
        assert escalated == [("db", 7)]
        assert sup._children["game"].terminated  # siblings die with it

    async def test_failed_callback_escalates(self) -> None:
        sup = ProcessSupervisor(_quiet_child)
        sup._children = {"db": _FakeChild(1)}
        escalated: list[tuple[str, int | None]] = []

        async def broken_callback(process_type: str, exitcode: int | None) -> None:
            raise RuntimeError("callback bug")

        async def hook(process_type: str, exitcode: int | None) -> None:
            escalated.append((process_type, exitcode))

        with pytest.raises(ChildDiedError):
            await sup._watch_children(broken_callback, hook)
        assert escalated == [("db", 1)]

    async def test_escalation_failure_does_not_mask_the_death(self) -> None:
        sup = ProcessSupervisor(_quiet_child)
        sup._children = {"db": _FakeChild(3)}

        async def broken_hook(process_type: str, exitcode: int | None) -> None:
            raise RuntimeError("hook bug")

        with pytest.raises(ChildDiedError):
            await sup._watch_children(None, broken_hook)

    def test_pid_alive_permission_error_means_alive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """POSIX: os.kill(pid, 0) raising PermissionError means the process
        EXISTS under another user; treating it as dead made a child suicide
        on a false negative."""

        def deny(pid: int, sig: int) -> None:
            raise PermissionError()

        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(os, "kill", deny)
        assert _pid_alive(123) is True


# --------------------------------------------------------------------------- #
# F-157: a decode failure during load must resolve the joiners
# --------------------------------------------------------------------------- #


class TestDecodeFailureResolvesJoinersF157:
    async def test_corrupt_blob_fails_both_callers_and_allows_retry(self) -> None:
        db = FakeDB()
        db.rows[("tbl_player", 9)] = {"data": b"garbage-not-a-pld1-blob"}
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 9)

        t1 = asyncio.create_task(saver.load())
        t2 = asyncio.create_task(saver.load())
        with pytest.raises(BlobFormatError):
            await t1
        with pytest.raises(BlobFormatError):
            await t2  # the joiner used to hang here forever
        assert saver.state == SaveState.NEW
        assert saver._load_future is None

        db.rows[("tbl_player", 9)]["data"] = dumps({"ok": 1})
        data = await saver.load()
        assert data["ok"] == 1


# --------------------------------------------------------------------------- #
# F-158: the coalesced flush must drop the journal hold
# --------------------------------------------------------------------------- #


class TestCoalescedJournalFlushF158:
    async def test_batch_flush_inside_transaction_clears_hold(self) -> None:
        """A coalesced multi-row upsert inside a transaction used to
        note_flush without clearing ``_pending_journal`` -- when a deferral
        landed between the batch's selection and the upsert (the saver was
        already popped off the queue, so the journal's requeue could not see
        it either) the auto-save scheduler then skipped this saver forever.
        Its data was only rescued by the shutdown drain."""
        from tests.test_orm_autosave import FakeDB as _FakeDB

        class _HookDB(_FakeDB):
            """Fires a hook mid-upsert: the deferral lands while the row is
            already in flight, exactly the F-158 interleaving."""

            def __init__(self) -> None:
                super().__init__()
                self.hook: Any = None

            async def execute(self, sql: str, args: tuple = ()) -> int:
                if self.hook is not None:
                    hook, self.hook = self.hook, None
                    hook()
                return await super().execute(sql, args)

        db = _HookDB()
        schema = make_schema()
        sched = SaveScheduler()
        s1 = DataSaver(db, schema, "tbl_player", "data", 1, scheduler=sched)
        s2 = DataSaver(db, schema, "tbl_player", "data", 2, scheduler=sched)
        await s1.load()
        await s2.load()
        s1.set_data({"a": 1})  # queued normally (outside any unit)
        s2.set_data({"b": 2})

        class _NullTx:
            async def execute(self, sql: str, args: tuple = ()) -> int:
                return 0

            async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
                return []

        async with bind_transaction(_NullTx()):  # type: ignore[arg-type]
            db.hook = lambda: s1.mark_dirty()  # deferral lands mid-upsert
            await sched.flush_batch()  # coalesced multi-row path
            assert s1.held_by_journal is None  # F-158: the hold dropped
            assert db.rows[("tbl_player", 1)]["data"] is not None

        # After the unit ends the saver must still be pickable: mark dirty
        # again and confirm the scheduler's due-selection finds it.
        s1.mark_dirty()
        assert sched._next_due(time.monotonic()) is s1


# --------------------------------------------------------------------------- #
# F-159: the inbound RPC task pool is bounded
# --------------------------------------------------------------------------- #


class _RecordingSender:
    def route(self, flag: str, payload: bytes, target: int, *, raise_on_drop: bool = False) -> None:
        self.sent: list[tuple[int, bytes]] = getattr(self, "sent", [])
        self.sent.append((target, payload))


class TestRpcTaskPoolBoundF159:
    async def test_call_flood_beyond_pool_gets_busy_without_spawning(self) -> None:
        """Every CALL used to spawn a task that parked up to ``inflight_wait``
        in _acquire_slot: live task count was bounded only by arrival rate.
        Beyond running+queued (= 2 x max_inflight here) a CALL is answered
        busy immediately, without a task."""
        rpc = RpcManager(
            ProtocolGateway(),
            _RecordingSender(),
            own_service_no=1,
            max_inflight=1,
            inflight_wait=0.05,
        )
        block = asyncio.Event()

        async def slow() -> int:
            await block.wait()
            return 42

        rpc.register("m.slow", slow)

        def call(call_id: int) -> bytes:
            return msgpack.packb([1, call_id, 3, "m.slow", []], use_bin_type=True)

        for call_id in range(1, 6):
            rpc.handle_message("@rpc", call(call_id), from_service=3)
        await asyncio.sleep(0.02)  # task 1 acquires and blocks; task 2 parks

        assert len(rpc._inbound) == 2  # the pool bound, not the flood size
        assert rpc.busy_rejects == 3
        busy_results = [
            msgpack.unpackb(payload)
            for _target, payload in rpc._sender.sent  # type: ignore[attr-defined]
            if (msg := msgpack.unpackb(payload))[0] == MSG_RESULT and msg[3] == BUSY_MESSAGE
        ]
        assert len(busy_results) == 3

        block.set()
        await asyncio.sleep(0.1)  # parked + executing tasks drain
        assert len(rpc._inbound) == 0


# --------------------------------------------------------------------------- #
# F-161: the Windows selector-loop fd budget is enforced at bind time
# --------------------------------------------------------------------------- #


class TestWindowsFdBudgetF161:
    def test_refuses_caps_over_the_ceiling(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from pyline.net.connection import (
            _WINDOWS_RESERVED_FDS,
            _WINDOWS_SELECT_FDS,
            check_windows_fd_budget,
        )

        monkeypatch.setattr(sys, "platform", "win32")
        ceiling = _WINDOWS_SELECT_FDS - _WINDOWS_RESERVED_FDS
        with pytest.raises(ConfigError, match="max_connections"):
            check_windows_fd_budget(4096)
        check_windows_fd_budget(ceiling)  # the boundary itself fits

    def test_noop_off_windows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from pyline.net.connection import check_windows_fd_budget

        monkeypatch.setattr(sys, "platform", "linux")
        check_windows_fd_budget(4096)  # POSIX has no select() fd ceiling


# --------------------------------------------------------------------------- #
# F-162: plain containers become tracked on set_data/load; awaitables awaited
# --------------------------------------------------------------------------- #


class TestTrackedWrapF162:
    async def test_set_data_wraps_containers_recursively(self) -> None:
        sched = SaveScheduler()
        saver = DataSaver(FakeDB(), make_schema(), "tbl_player", "data", 5, scheduler=sched)
        await saver.load()
        saver.set_data({"gold": 5, "bag": {"items": [1, 2]}})
        sched.forget(saver)  # dequeue the set_data mark
        assert sched.queue_depth() == 0

        from pyline.db.tracked import TrackedDict, TrackedList

        assert isinstance(saver.data, TrackedDict)
        assert isinstance(saver.data["bag"], TrackedDict)
        assert isinstance(saver.data["bag"]["items"], TrackedList)
        saver.data["bag"]["items"].append(3)  # no explicit touch() anywhere
        assert sched.queue_depth() == 1  # the mutation marked the saver dirty

    async def test_load_path_wraps_decoded_containers(self) -> None:
        db = FakeDB()
        db.rows[("tbl_player", 4)] = {"data": dumps({"x": [1]})}
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 4, scheduler=SaveScheduler())
        data = await saver.load()
        from pyline.db.tracked import TrackedDict, TrackedList

        assert isinstance(data, TrackedDict)
        assert isinstance(data["x"], TrackedList)

    async def test_event_handler_returning_future_is_awaited(self) -> None:
        """iscoroutine -> isawaitable: a handler handing back a Task/Future
        used to have its result (and exception) silently dropped."""
        bus = EventBus()
        probe: list[str] = []

        def first(event: NewHourEvent) -> Any:
            async def body() -> None:
                await asyncio.sleep(0)
                probe.append("future-done")

            return asyncio.ensure_future(body())

        def second(event: NewHourEvent) -> None:
            probe.append("second-ran")

        bus.subscribe(NewHourEvent, first, layer=LAYER_FRAMEWORK)
        bus.subscribe(NewHourEvent, second, layer=LAYER_BUSINESS)
        await bus.emit(NewHourEvent(hour=1))
        # the future finished BEFORE the later-layer handler ran
        assert probe == ["future-done", "second-ran"]


# --------------------------------------------------------------------------- #
# F-163: production requires an explicit inter_token
# --------------------------------------------------------------------------- #


class TestInterTokenProductionF163:
    def _ctx(self, config_dir: Path, *, srv_type: str, inter: str | None) -> Context:
        settings = load_project_settings(config_dir)
        socket = settings.socket
        if inter is not None:
            socket = socket.model_copy(update={"inter_token": SecretStr(inter)})
        settings = settings.model_copy(update={"srv_type": srv_type, "socket": socket})
        registry = load_server_registry(config_dir)
        return Context(
            settings=settings,
            registry=registry,
            tables=load_table_defs(config_dir),
            entry=registry.entry(10001),
            process_type=PROCESS_MAIN,
            process_index=0,
            main_pid=0,
        )

    def test_production_without_inter_token_refuses(self, config_dir: Path) -> None:
        """The client-token fallback IS the vulnerability in production:
        every game client would hold a credential that impersonates servers
        on the inter-server plane (bus, proxy, full SQL passthrough)."""
        ctx = self._ctx(config_dir, srv_type="production", inter=None)
        with pytest.raises(ConfigError, match="inter_token is required"):
            inter_token(ctx)

    def test_production_with_inter_token_boots(self, config_dir: Path) -> None:
        ctx = self._ctx(config_dir, srv_type="production", inter="s3cret-inter")
        assert inter_token(ctx) == "s3cret-inter"

    def test_develop_keeps_the_fallback(self, config_dir: Path) -> None:
        ctx = self._ctx(config_dir, srv_type="develop", inter=None)
        assert inter_token(ctx) == "unit-test-token"


# --------------------------------------------------------------------------- #
# F-164: calendar events dispatch strictly in order
# --------------------------------------------------------------------------- #


class TestCalendarOrderF164:
    async def test_midnight_chain_emits_in_order(self, tmp_path: Path) -> None:
        """One spawn per event started tasks in creation order but
        interleaved them at the first await -- an async NewHour handler could
        run after the NewDay/NewWeek handlers behind it. The chain is awaited
        now: NewHour -> NewDay -> NewMonth -> NewYear -> NewWeek."""
        from pyline.core.clock import GameClock
        from pyline.core.events import (
            NewDayEvent,
            NewMonthEvent,
            NewWeekEvent,
            NewYearEvent,
        )
        from pyline.core.scheduler import Scheduler

        clock = GameClock(epoch=dt.datetime(2024, 1, 1), tz="UTC")
        bus = EventBus()
        order: list[str] = []

        async def handler(event: object) -> None:
            await asyncio.sleep(0)  # force a suspension point
            order.append(type(event).__name__)

        for et in (NewHourEvent, NewDayEvent, NewMonthEvent, NewYearEvent, NewWeekEvent):
            bus.subscribe(et, handler)
        emitter = ClockEventEmitter(
            clock,
            Scheduler(),
            bus,
            log_dir=tmp_path,
            spawn=lambda coro: asyncio.get_running_loop().create_task(coro),
        )
        # compute the next Jan-1 00:00 boundary that is also a Monday (the
        # full hour/day/month/year/week chain only fires there); wall-space
        # arithmetic via the clock's own tz keeps ts <-> local consistent.
        target = clock.local(clock.now()).replace(hour=0, minute=0, second=0, microsecond=0)
        target += dt.timedelta(days=1)
        for _ in range(1500):  # at most ~4 years; a Monday Jan-1 always inside
            if target.month == 1 and target.day == 1 and target.weekday() == 0:
                break
            target += dt.timedelta(days=1)
        else:  # pragma: no cover - fixture guard
            raise AssertionError("no Monday Jan-1 boundary within 4 years")
        ts = target.timestamp()

        await emitter._fire_boundary_events(ts)
        assert order == [
            "NewHourEvent",
            "NewDayEvent",
            "NewMonthEvent",
            "NewYearEvent",
            "NewWeekEvent",
        ]
