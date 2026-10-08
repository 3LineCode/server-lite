"""ORM + auto-save: DataSaver lifecycle and SaveScheduler batching."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from pyline.config.models import TableDef, TableFieldDef
from pyline.db.autosave import SaveScheduler
from pyline.db.orm import DataSaver, SaveState, TrackableModel, dataclass_codec
from pyline.db.schema import SchemaManager
from pyline.db.serialization import dumps


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
