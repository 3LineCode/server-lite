"""ORM + auto-save: DataSaver lifecycle and SaveScheduler batching."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from typing import Any

import pytest

from pyline.config.models import TableDef, TableFieldDef
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import DataSaver, SaveState, TrackableModel, dataclass_codec
from pyline.db.schema import SchemaManager
from pyline.db.serialization import dumps, loads
from pyline.db.tracked import TrackedDict, TrackedList
from pyline.db.transaction import bind_transaction


def dumps_versioned(data: object) -> bytes:
    return dumps(data, schema_version=1)


class FakeDB:
    """In-memory QueryExecutor: one table, one blob column."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, Any], dict[str, bytes | None]] = {}
        self.executed: list[tuple[str, tuple]] = []

    async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
        self.executed.append((sql, args))
        if sql.startswith("SELECT"):
            key = args[0]
            row = self.rows.get(("tbl_player", key))
            if row is None:
                return []
            return [(row.get("data"),)]
        return []

    async def execute(self, sql: str, args: tuple = ()) -> int:
        self.executed.append((sql, args))
        if sql.startswith("INSERT INTO"):
            # single-row and coalesced multi-row upserts (F-42) both arrive
            # as flattened (key, blob) pairs
            for i in range(0, len(args), 2):
                self.rows.setdefault(("tbl_player", args[i]), {})["data"] = args[i + 1]
            return len(args) // 2
        return 0


def make_schema() -> SchemaManager:
    tables = {
        "tbl_player": TableDef(
            fields={
                "id": TableFieldDef(type="BIGINT", primary=True),
                "data": TableFieldDef(type="MEDIUMBLOB"),
            }
        )
    }
    return SchemaManager(FakeDB(), tables, "test_db")  # type: ignore[arg-type]


@dataclass
class PlayerData(TrackableModel):
    name: str = ""
    level: int = 1


class TestDataSaver:
    async def test_load_missing_then_set_and_flush(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 7)
        assert await saver.load() is None
        assert saver.state == SaveState.MISSING

        saver.set_data({"gold": 5})
        await saver.flush()
        stored = db.rows[("tbl_player", 7)]["data"]
        assert stored is not None and stored.startswith(b"PLD1")

        # reload from storage
        saver2 = DataSaver(db, make_schema(), "tbl_player", "data", 7)
        assert await saver2.load() == {"gold": 5}

    async def test_dataclass_codec(self) -> None:
        db = FakeDB()
        codec = dataclass_codec(PlayerData)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", "abc", codec=codec)
        saver.set_data(PlayerData(name="alice", level=3))
        await saver.flush()
        saver2 = DataSaver(db, make_schema(), "tbl_player", "data", "abc", codec=codec)
        loaded = await saver2.load()
        assert isinstance(loaded, PlayerData)
        assert loaded.name == "alice" and loaded.level == 3

    async def test_trackable_model_marks_dirty(self) -> None:
        db = FakeDB()
        codec = dataclass_codec(PlayerData)
        scheduler = SaveScheduler()
        saver = DataSaver(
            db, make_schema(), "tbl_player", "data", 1, codec=codec, scheduler=scheduler
        )
        model = PlayerData()
        model.bind_saver(saver)
        await saver.load()

        model.level = 9  # attribute write -> dirty
        assert scheduler.queue_depth() == 1

        model.touch()  # explicit after in-place container mutation
        assert scheduler.queue_depth() == 1  # deduplicated

    async def test_dirty_before_load_rejected(self) -> None:
        saver = DataSaver(FakeDB(), make_schema(), "tbl_player", "data", 1)
        with pytest.raises(IOError, match="before load"):
            saver.mark_dirty()

    async def test_load_hooks(self) -> None:
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 2)
        called: list[str] = []
        saver.add_load_hook(lambda s: called.append("hook"))
        await saver.load()
        assert called == ["hook"]

    async def test_orm_concurrent_load_singleflight(self) -> None:
        """F-03: 50 concurrent loads issue one query and agree on the result."""
        db = FakeDB()
        db.rows[("tbl_player", 3)] = {"data": b""}  # placeholder, set below

        class CountingDB(FakeDB):
            queries = 0

            async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
                self.queries += 1
                await asyncio.sleep(0.02)  # widen the join window
                return await super().query(sql, args)

        cdb = CountingDB()
        cdb.rows[("tbl_player", 3)] = {"data": dumps_versioned({"gold": 42})}
        saver = DataSaver(cdb, make_schema(), "tbl_player", "data", 3)
        results = await asyncio.gather(*(saver.load() for _ in range(50)))
        assert cdb.queries == 1
        assert all(r == {"gold": 42} for r in results)

    async def test_set_data_during_load_not_clobbered(self) -> None:
        """F-03: a set_data() that lands mid-query must not be overwritten."""

        class SlowDB(FakeDB):
            async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
                await asyncio.sleep(0.05)
                return await super().query(sql, args)

        db = SlowDB()
        db.rows[("tbl_player", 8)] = {"data": dumps_versioned({"stale": True})}
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 8)
        task = asyncio.get_running_loop().create_task(saver.load())
        await asyncio.sleep(0.01)  # query now in flight
        saver.set_data({"fresh": True})
        await task
        assert saver.data == {"fresh": True}

    async def test_orm_delete_failure_keeps_state(self) -> None:
        """F-04: a failed DELETE leaves the saver loaded and retryable."""

        class FlakyDB(FakeDB):
            fail = True

            async def execute(self, sql: str, args: tuple = ()) -> int:
                if self.fail and sql.startswith("DELETE"):
                    raise ConnectionError("db down")
                return await super().execute(sql, args)

        db = FlakyDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 6)
        saver.set_data({"x": 1})
        with pytest.raises(ConnectionError):
            await saver.delete()
        assert saver.state == SaveState.LOADED  # unchanged, retryable
        saver.mark_dirty()  # still usable
        db.fail = False
        await saver.delete()
        assert saver.state == SaveState.DELETED

    async def test_orm_delete_idempotent(self) -> None:
        """F-04: concurrent deletes join one DELETE; a second call is a no-op."""
        db = FakeDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 6)
        saver.set_data({"x": 1})
        await asyncio.gather(saver.delete(), saver.delete())
        deletes = [sql for sql, _ in db.executed if sql.startswith("DELETE")]
        assert len(deletes) == 1
        await saver.delete()  # already deleted: no extra SQL
        deletes = [sql for sql, _ in db.executed if sql.startswith("DELETE")]
        assert len(deletes) == 1

    async def test_flush_before_load_rejected(self) -> None:
        """F-102: flush() on a never-loaded saver used to encode ``None`` into
        a perfectly valid blob and overwrite the real row with it; it must
        refuse loudly instead."""
        db = FakeDB()
        db.rows[("tbl_player", 5)] = {"data": dumps_versioned({"real": True})}
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 5)
        with pytest.raises(OSError, match="before load"):
            await saver.flush()
        assert db.rows[("tbl_player", 5)]["data"] == dumps_versioned({"real": True})
        # the batch path (begin_flush_row) gets the same guard
        with pytest.raises(OSError, match="before load"):
            await saver.begin_flush_row()

        await saver.load()  # afterwards a flush is fine
        saver.set_data({"real": False})
        await saver.flush()
        assert loads(db.rows[("tbl_player", 5)]["data"]) == {"real": False}


class TestSaveScheduler:
    async def test_batch_flush_and_stats(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=0.05, batch_size=10)
        scheduler.start()
        for i in range(5):
            saver = DataSaver(db, make_schema(), "tbl_player", "data", i, scheduler=scheduler)
            saver.set_data({"i": i})
        assert scheduler.queue_depth() == 5
        await asyncio.sleep(0.3)
        assert scheduler.queue_depth() == 0
        assert scheduler.saved_total == 5
        assert len(db.rows) == 5

    async def test_autosave_retry_keeps_dirty(self) -> None:
        """F-01: a failing saver is never dropped; retries back off exponentially."""

        class FlakyDB(FakeDB):
            fail = True
            attempts = 0

            async def execute(self, sql: str, args: tuple = ()) -> int:
                self.attempts += 1
                if self.fail:
                    raise ConnectionError("db down")
                return await super().execute(sql, args)

        db = FlakyDB()
        alarms: list[tuple[str, dict]] = []
        scheduler = SaveScheduler(
            interval=60.0,  # drive flush_batch() manually
            retry_cooldown=0.05,
            retry_cap=1.0,
            alarm_threshold=2,
            on_alarm=lambda kind, payload: alarms.append((kind, payload)),
        )
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 9, scheduler=scheduler)
        saver.set_data({"x": 1})

        await scheduler.flush_batch()  # attempt 1 fails -> deferred 0.05s
        assert scheduler.queue_depth() == 1
        assert db.attempts == 1

        await scheduler.flush_batch()  # not due yet: backoff gates the retry
        assert db.attempts == 1
        assert scheduler.queue_depth() == 1

        await asyncio.sleep(0.15)  # backoff expires (generous margin)
        await scheduler.flush_batch()  # attempt 2 fails -> deferred 0.1s
        assert db.attempts == 2
        assert scheduler.queue_depth() == 1  # still queued, never dropped
        assert alarms and alarms[0][0] == "save_retry"
        assert scheduler.saved_total == 0

        # database recovers: the next due retry succeeds and drains the queue
        db.fail = False
        await asyncio.sleep(0.25)  # second-stage backoff (0.1s) expires
        await scheduler.flush_batch()
        assert scheduler.queue_depth() == 0
        assert scheduler.saved_total == 1
        assert ("tbl_player", 9) in db.rows

    async def test_autosave_shutdown_flush_retries(self) -> None:
        """F-02: shutdown draining retries until the database recovers."""

        class FlakyDB(FakeDB):
            fail_remaining = 2

            async def execute(self, sql: str, args: tuple = ()) -> int:
                if self.fail_remaining > 0:
                    self.fail_remaining -= 1
                    raise ConnectionError("db down")
                return await super().execute(sql, args)

        db = FlakyDB()
        scheduler = SaveScheduler(interval=60.0, shutdown_flush_timeout=2.0)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 4, scheduler=scheduler)
        saver.set_data({"final": True})
        assert await scheduler.stop() is True
        assert ("tbl_player", 4) in db.rows
        assert scheduler.saved_total == 1

    async def test_autosave_shutdown_deadline_reports_loss(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """F-02: deadline exceeded -> per-saver CRITICAL report and False."""

        class DeadDB(FakeDB):
            async def execute(self, sql: str, args: tuple = ()) -> int:
                raise ConnectionError("db down")

        scheduler = SaveScheduler(interval=60.0, shutdown_flush_timeout=0.05)
        saver = DataSaver(DeadDB(), make_schema(), "tbl_player", "data", 5, scheduler=scheduler)
        saver.set_data({"lost": True})
        with caplog.at_level("CRITICAL", logger="pyline.db.autosave"):
            assert await scheduler.stop() is False
        assert any("UNFLUSHED DATA AT SHUTDOWN" in r.message for r in caplog.records)
        assert any(repr(saver) in r.message for r in caplog.records)

    async def test_autosave_stop_drains_inflight(self) -> None:
        """F-02: cancellation mid-flush requeues the saver; stop() still flushes it."""

        class SlowDB(FakeDB):
            async def execute(self, sql: str, args: tuple = ()) -> int:
                await asyncio.sleep(0.2)
                return await super().execute(sql, args)

        db = SlowDB()
        scheduler = SaveScheduler(interval=0.01, batch_size=10)
        scheduler.start()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 7, scheduler=scheduler)
        saver.set_data({"x": 1})
        for _ in range(200):  # wait until the loop task is mid-flush
            if scheduler._inflight:
                break
            await asyncio.sleep(0.005)
        assert scheduler._inflight  # cancellation will land inside saver.flush()
        assert await scheduler.stop() is True
        assert ("tbl_player", 7) in db.rows

    async def test_flush_all_on_stop(self) -> None:
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0)  # loop never fires
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 5, scheduler=scheduler)
        saver.set_data({"final": True})
        await scheduler.stop()  # shutdown path flushes everything
        assert scheduler.saved_total == 1
        assert ("tbl_player", 5) in db.rows

    async def test_flush_all_does_not_starve_behind_failing_saver(self) -> None:
        """F-33: a persistently failing saver used to keep its queue-head
        position on every retry; the healthy saver behind it must still get
        its shutdown flush inside the deadline."""
        failing, healthy_key = "broken", "ok"
        db = _SelectiveFailureDB(fail_for_key=failing)
        scheduler = SaveScheduler(interval=60.0, shutdown_flush_timeout=0.5)
        broken = DataSaver(db, make_schema(), "tbl_player", "data", failing, scheduler=scheduler)
        good = DataSaver(db, make_schema(), "tbl_player", "data", healthy_key, scheduler=scheduler)
        broken.set_data({"n": 1})  # enqueued first: owns the queue head
        good.set_data({"n": 2})
        assert await scheduler.stop() is False  # broken never flushes in time
        assert ("tbl_player", healthy_key) in db.rows  # but good was flushed
        assert ("tbl_player", failing) not in db.rows

    async def test_flush_delete_race_no_resurrection(self) -> None:
        """F-34: an upsert in flight when delete() runs used to land AFTER
        the DELETE and resurrect the row; the flush lock serializes them."""
        db = _SlowUpsertDB()
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 9)
        saver.set_data({"gold": 5})
        flush_task = asyncio.get_running_loop().create_task(saver.flush())
        await asyncio.sleep(0.02)  # flush SQL is now in flight under the lock
        await saver.delete()
        await flush_task
        key_stmts = [sql for sql, args in db.executed if args and args[0] == 9]
        assert key_stmts[0].startswith("INSERT INTO")
        assert key_stmts[-1].startswith("DELETE")  # no upsert after the DELETE
        assert ("tbl_player", 9) not in db.rows  # row not resurrected


class _SelectiveFailureDB(FakeDB):
    """Fails every write for one key, succeeds for the rest."""

    def __init__(self, fail_for_key: object) -> None:
        super().__init__()
        self._fail_key = fail_for_key

    async def execute(self, sql: str, args: tuple = ()) -> int:
        if args and args[0] == self._fail_key:
            raise RuntimeError("write path broken for this row")
        return await super().execute(sql, args)


class _SlowUpsertDB(FakeDB):
    """Upserts pause mid-flight; DELETEs actually remove the row."""

    async def execute(self, sql: str, args: tuple = ()) -> int:
        if sql.startswith("INSERT INTO"):
            await asyncio.sleep(0.05)
            return await super().execute(sql, args)
        if sql.startswith("DELETE"):
            self.executed.append((sql, args))
            self.rows.pop(("tbl_player", args[0]), None)
            return 1
        return await super().execute(sql, args)


class TestCoalescedFlushF42:
    async def test_same_table_flushes_as_one_statement(self) -> None:
        """F-42: rows sharing (table, column) coalesce into one multi-row
        upsert -- one round-trip per batch, not one per saver."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=50)
        for i in range(10):
            saver = DataSaver(db, make_schema(), "tbl_player", "data", i, scheduler=scheduler)
            saver.set_data({"i": i})
        await scheduler.flush_batch()
        inserts = [sql for sql, _ in db.executed if sql.startswith("INSERT INTO")]
        assert len(inserts) == 1
        assert inserts[0].count("(%s, %s)") == 10
        assert scheduler.saved_total == 10
        assert len(db.rows) == 10

    async def test_group_failure_falls_back_to_per_saver(self) -> None:
        """F-42: one poisoned row fails the multi-row statement; the fallback
        isolates rows individually, preserving F-33 retry semantics."""

        class NoMultiRowDB(FakeDB):
            rejected_multi = 0

            async def execute(self, sql: str, args: tuple = ()) -> int:
                if sql.count("(%s, %s)") > 1:
                    self.rejected_multi += 1
                    raise ConnectionError("packet too large")
                return await super().execute(sql, args)

        db = NoMultiRowDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=50)
        for i in range(3):
            saver = DataSaver(db, make_schema(), "tbl_player", "data", 100 + i, scheduler=scheduler)
            saver.set_data({"i": i})
        await scheduler.flush_batch()
        assert scheduler.queue_depth() == 0
        assert scheduler.saved_total == 3
        assert db.rejected_multi == 1  # the coalesced statement was attempted once
        singles = [sql for sql, _ in db.executed if sql.count("(%s, %s)") == 1]
        assert len(singles) == 3
        assert len(db.rows) == 3

    async def test_row_cap_chunks_groups(self) -> None:
        """F-42: the 32-row chunk cap keeps one statement bounded."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=100)
        for i in range(70):
            saver = DataSaver(db, make_schema(), "tbl_player", "data", i, scheduler=scheduler)
            saver.set_data({"i": i})
        await scheduler.flush_batch()
        inserts = [sql for sql, _ in db.executed if sql.startswith("INSERT INTO")]
        assert len(inserts) == 3  # 32 + 32 + 6
        assert scheduler.saved_total == 70
        assert len(db.rows) == 70

    async def test_deleted_mid_batch_counts_as_saved(self) -> None:
        """A saver deleted while its batch is being encoded is skipped, not
        resurrected (F-34 semantics inside the coalesced path)."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        keep = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        keep.set_data({"k": 1})
        gone = DataSaver(db, make_schema(), "tbl_player", "data", 2, scheduler=scheduler)
        gone.set_data({"g": 2})
        await gone.delete()  # wins the lock before flush_row runs
        await scheduler.flush_batch()
        assert scheduler.saved_total == 2  # keep flushed + gone no-op'd
        assert ("tbl_player", 2) not in db.rows

    async def test_queue_depth_alarm_edge_triggered(self) -> None:
        """F-42: crossing the threshold alarms once (not per mark); recovering
        below it re-arms the alarm."""
        alarms: list[tuple[str, dict]] = []
        scheduler = SaveScheduler(
            interval=60.0,
            queue_alarm_threshold=5,
            on_alarm=lambda kind, payload: alarms.append((kind, payload)),
        )
        db = FakeDB()
        for i in range(6):
            saver = DataSaver(db, make_schema(), "tbl_player", "data", i, scheduler=scheduler)
            saver.set_data({"i": i})
        depth_alarms = [a for a in alarms if a[0] == "save_queue_depth"]
        assert len(depth_alarms) == 1  # fired when crossing 5, silent at 6
        assert depth_alarms[0][1]["depth"] == 5

        await scheduler.flush_batch()
        assert scheduler.queue_depth() == 0
        for i in range(6, 9):  # below threshold again: stays silent
            saver = DataSaver(db, make_schema(), "tbl_player", "data", i, scheduler=scheduler)
            saver.set_data({"i": i})
        assert len([a for a in alarms if a[0] == "save_queue_depth"]) == 1


async def _raise_runtime_error() -> None:
    raise RuntimeError("boom")


class TestLoopGuardF46:
    async def test_poison_blob_is_quarantined_not_fatal(self) -> None:
        """F-46: a blob that cannot be encoded used to escape flush_batch as
        an uncaught error, killing the loop task -- auto-save then silently
        stopped for every later mark. Now the row is quarantined like a
        failing flush and the loop stays alive."""
        db = FakeDB()
        alarms: list[tuple[str, dict]] = []
        scheduler = SaveScheduler(
            interval=0.02,
            retry_cooldown=0.05,
            alarm_threshold=1,
            shutdown_flush_timeout=0.05,
            on_alarm=lambda kind, payload: alarms.append((kind, payload)),
        )
        scheduler.start()
        poison = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        poison.set_data(object())  # not msgpack-serializable
        healthy = DataSaver(db, make_schema(), "tbl_player", "data", 2, scheduler=scheduler)
        healthy.set_data({"ok": True})

        await asyncio.sleep(0.2)
        assert scheduler._task is not None and not scheduler._task.done()  # loop survived
        assert any(kind == "save_retry" for kind, _ in alarms)
        assert scheduler.queue_depth() == 1  # poison requeued; healthy flushed
        assert ("tbl_player", 2) in db.rows
        assert poison in scheduler._deferred  # backoff prevents a hot retry loop
        assert await scheduler.stop() is False  # poison can never flush: reported, not lost
        assert ("tbl_player", 1) not in db.rows

    async def test_poison_row_does_not_starve_the_batch(self) -> None:
        """F-46: the quarantine is per-row -- a saver queued behind an
        unencodable one must still flush in the same round."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        poison = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        poison.set_data(object())
        healthy = DataSaver(db, make_schema(), "tbl_player", "data", 2, scheduler=scheduler)
        healthy.set_data({"ok": True})
        await scheduler.flush_batch()
        assert ("tbl_player", 2) in db.rows  # flushed despite poison ahead of it
        assert scheduler.queue_depth() == 1  # only poison remains

    async def test_unexpected_flush_error_requeues_and_keeps_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """F-46 outer net: a bug escaping the per-row quarantine requeues the
        already-popped savers (never-drop), alarms, and the loop keeps running."""
        db = FakeDB()
        alarms: list[tuple[str, dict]] = []
        scheduler = SaveScheduler(
            interval=0.05,
            retry_cooldown=0.05,
            on_alarm=lambda kind, payload: alarms.append((kind, payload)),
        )
        first = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        first.set_data({"x": 1})
        second = DataSaver(db, make_schema(), "tbl_player", "data", 2, scheduler=scheduler)
        second.set_data({"x": 2})

        async def broken_flush_group(
            members: list[tuple[DataSaver, int, Any, bytes]],
            held: set[DataSaver],
        ) -> set[DataSaver]:
            raise RuntimeError("bug in flush path")

        monkeypatch.setattr(scheduler, "_flush_group", broken_flush_group)
        with pytest.raises(RuntimeError):
            await scheduler.flush_batch()
        assert scheduler.queue_depth() == 2  # both requeued, never dropped
        assert first in scheduler._deferred and second in scheduler._deferred
        assert not scheduler._inflight

        scheduler.start()  # loop-level guard: the bug no longer kills the task
        await asyncio.sleep(0.15)
        assert scheduler._task is not None and not scheduler._task.done()
        assert any(kind == "save_loop_error" for kind, _ in alarms)
        await scheduler.stop()  # flush_all path does not use _flush_group: drains

    async def test_loop_death_is_alarmed(self) -> None:
        """F-46 defense-in-depth: if the loop task exits with an exception
        anyway, the done callback alarms; a normal stop() must not."""
        alarms: list[tuple[str, dict]] = []
        scheduler = SaveScheduler(on_alarm=lambda kind, payload: alarms.append((kind, payload)))
        scheduler.start()
        assert scheduler._task is not None
        boom = asyncio.get_running_loop().create_task(_raise_runtime_error())
        boom.add_done_callback(scheduler._on_loop_done)
        await asyncio.sleep(0.01)
        assert any(kind == "save_loop_died" for kind, _ in alarms)
        await scheduler.stop()  # cancellation path: no extra death alarm
        assert len([1 for kind, _ in alarms if kind == "save_loop_died"]) == 1


class _SignalSlowUpsertDB(FakeDB):
    """Coalesced upserts signal their start, then pause mid-flight."""

    def __init__(self) -> None:
        super().__init__()
        self.sql_started = asyncio.Event()

    async def execute(self, sql: str, args: tuple = ()) -> int:
        if sql.startswith("INSERT INTO"):
            self.sql_started.set()
            await asyncio.sleep(0.05)
            return await super().execute(sql, args)
        if sql.startswith("DELETE"):
            self.executed.append((sql, args))
            self.rows.pop(("tbl_player", args[0]), None)
            return 1
        return await super().execute(sql, args)


class TestBatchDeleteRaceF59:
    async def test_delete_during_batch_sql_does_not_resurrect(self) -> None:
        """F-59: flush_batch used to release each saver's flush lock after
        encoding it; a delete() landing in the unlocked gap before the
        coalesced upsert executed was then resurrected by that upsert -- the
        row came back while memory said DELETED, and the saver had already
        been counted as saved.  The locks are now held until the SQL lands,
        so the DELETE waits and strictly follows the upsert."""
        db = _SignalSlowUpsertDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        keep = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        keep.set_data({"k": 1})
        gone = DataSaver(db, make_schema(), "tbl_player", "data", 2, scheduler=scheduler)
        gone.set_data({"g": 2})

        batch_task = asyncio.get_running_loop().create_task(scheduler.flush_batch())
        await asyncio.wait_for(db.sql_started.wait(), timeout=5.0)
        delete_task = asyncio.get_running_loop().create_task(gone.delete())
        await asyncio.sleep(0.01)  # the delete is now blocked on the held lock
        assert gone.state != SaveState.DELETED  # it has NOT slipped in early
        assert not delete_task.done()
        assert ("tbl_player", 2) not in db.rows  # upsert still in flight, no row yet

        await asyncio.wait_for(asyncio.gather(batch_task, delete_task), timeout=5.0)
        assert gone.state == SaveState.DELETED
        assert ("tbl_player", 2) not in db.rows  # no resurrection
        assert ("tbl_player", 1) in db.rows  # the survivor is unaffected
        key_stmts = [sql for sql, args in db.executed if args and 2 in args]
        assert key_stmts[0].startswith("INSERT INTO")  # upsert first...
        assert key_stmts[-1].startswith("DELETE")  # ...DELETE strictly last
        assert scheduler.queue_depth() == 0

    async def test_batch_holds_locks_through_encode_of_later_rows(self) -> None:
        """F-59 (encode-phase window): a delete() issued while flush_batch is
        still encoding later rows must not land before the earlier-encoded
        row's SQL either -- the lock is taken *before* the encode now."""
        db = _SignalSlowUpsertDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        first = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        first.set_data({"f": 1})
        second = DataSaver(db, make_schema(), "tbl_player", "data", 2, scheduler=scheduler)
        second.set_data({"s": 2})

        # delete() racing the batch must observe either outcome-consistent
        # order; here we delete `first` after the batch's SQL started.
        batch_task = asyncio.get_running_loop().create_task(scheduler.flush_batch())
        await asyncio.wait_for(db.sql_started.wait(), timeout=5.0)
        await first.delete()
        await batch_task
        assert ("tbl_player", 1) not in db.rows
        assert ("tbl_player", 2) in db.rows


class _NullTxExecutor:
    """TransactionExecutor stand-in: bind_transaction only needs the shape."""

    async def execute(self, sql: str, args: tuple = ()) -> int:
        return 0

    async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
        return []


class TestAutosaveTransactionAtomicityF63:
    async def test_mark_inside_transaction_defers_background_flush(self) -> None:
        """F-63 (window a): a saver marked dirty inside an open transaction
        used to enter the background queue immediately; the 5 s round then
        autocommitted it *outside* the unit, surviving a later rollback."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        async with bind_transaction(_NullTxExecutor()):
            saver.set_data({"x": 1})  # mark inside the unit -> deferred
            assert saver.held_by_journal is not None
            assert scheduler.queue_depth() == 0  # not queued yet
            await scheduler.flush_batch()  # a background round fires mid-unit
            assert scheduler.saved_total == 0
            assert ("tbl_player", 1) not in db.rows  # nothing leaked out
        assert saver.held_by_journal is None  # unit committed: hold released
        assert scheduler.queue_depth() == 1  # re-marked at unit end
        await scheduler.flush_batch()
        assert ("tbl_player", 1) in db.rows  # persisted outside the unit

    async def test_deferred_saver_remarked_after_rollback(self) -> None:
        """F-63 (rollback window): a rollback re-marks the deferred saver --
        its in-memory data never reached the DB and must be retried."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        with pytest.raises(RuntimeError, match="rollback me"):
            async with bind_transaction(_NullTxExecutor()):
                saver.set_data({"x": 1})
                await scheduler.flush_batch()  # background round mid-unit
                assert ("tbl_player", 1) not in db.rows
                raise RuntimeError("rollback me")
        assert saver.held_by_journal is None
        assert scheduler.queue_depth() == 1  # re-marked dirty, not dropped
        await scheduler.flush_batch()
        assert ("tbl_player", 1) in db.rows

    async def test_pre_marked_saver_held_while_modified_in_transaction(self) -> None:
        """F-63 (window b): a saver queued *before* the unit started is held
        once the unit modifies it, so the background round cannot leak a
        mid-unit snapshot outside the unit."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        saver.set_data({"v": 0})  # marked BEFORE the unit
        assert scheduler.queue_depth() == 1
        async with bind_transaction(_NullTxExecutor()):
            saver.set_data({"v": 1})  # modified inside: hold set on the queued saver
            assert saver.held_by_journal is not None
            await scheduler.flush_batch()  # background round: skipped, not dropped
            assert scheduler.saved_total == 0
            assert ("tbl_player", 1) not in db.rows
            assert scheduler.queue_depth() == 1
        await scheduler.flush_batch()
        stored = db.rows[("tbl_player", 1)]["data"]
        assert stored is not None and loads(stored) == {"v": 1}  # post-unit data

    async def test_explicit_flush_inside_unit_supersedes_deferral(self) -> None:
        """F-63: an explicit flush() joins the unit (F-50) and cancels the
        deferral -- on rollback the F-50 remark path owns the retry."""
        db = FakeDB()
        scheduler = SaveScheduler(interval=60.0, batch_size=10)
        saver = DataSaver(db, make_schema(), "tbl_player", "data", 1, scheduler=scheduler)
        with pytest.raises(RuntimeError, match="rollback me"):
            async with bind_transaction(_NullTxExecutor()):
                saver.set_data({"x": 1})  # deferred
                await saver.flush()  # explicit: supersedes the deferral
                assert saver.held_by_journal is None
                raise RuntimeError("rollback me")
        assert scheduler.queue_depth() == 1  # F-50 remark re-queued it


class TestTrackedContainersF64:
    async def test_asdict_over_tracked_containers(self) -> None:
        """F-64: dataclasses.asdict rebuilds container fields via
        ``type(obj)(...)`` without keyword args; a required ``touch`` made
        the documented dataclass_codec + TrackedDict combination raise
        TypeError on encode."""

        @dataclass
        class Bag(TrackableModel):
            counts: TrackedDict[str, int] = field(default_factory=dict)
            tags: TrackedList[str] = field(default_factory=list)

        bag = Bag(counts=TrackedDict({"hp": 1}), tags=TrackedList(["new"]))
        snapshot = asdict(bag)  # used to raise TypeError (missing touch)
        assert snapshot == {"counts": {"hp": 1}, "tags": ["new"]}

        codec = dataclass_codec(Bag)
        blob = codec.encode(bag)  # the documented combination, end to end
        loaded = codec.decode(blob)
        assert isinstance(loaded, Bag)
        assert dict(loaded.counts) == {"hp": 1}
        assert list(loaded.tags) == ["new"]

    def test_touch_callback_still_fires_when_given(self) -> None:
        """F-64: optionality must not silence a real owner -- with a callback
        the mutation reporting works exactly as before."""
        touches: list[int] = []
        tracked = TrackedDict(touch=lambda: touches.append(1))
        tracked["a"] = 1
        tracked.update({"b": 2})
        tracked |= {"c": 3}
        lst = TrackedList([1], touch=lambda: touches.append(1))
        lst.append(2)
        assert len(touches) == 4  # three dict mutations + one list mutation
        assert dict(tracked) == {"a": 1, "b": 2, "c": 3}
        assert list(lst) == [1, 2]
