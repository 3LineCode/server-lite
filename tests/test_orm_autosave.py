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
            self.rows.setdefault(("tbl_player", args[0]), {})["data"] = args[1]
            return 1
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

        await asyncio.sleep(0.06)  # backoff expires
        await scheduler.flush_batch()  # attempt 2 fails -> deferred 0.1s
        assert db.attempts == 2
        assert scheduler.queue_depth() == 1  # still queued, never dropped
        assert alarms and alarms[0][0] == "save_retry"
        assert scheduler.saved_total == 0

        # database recovers: the next due retry succeeds and drains the queue
        db.fail = False
        await asyncio.sleep(0.11)
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
