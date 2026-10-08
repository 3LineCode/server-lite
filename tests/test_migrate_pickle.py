"""F-08: the pickle -> msgpack migration tool."""

from __future__ import annotations

import pickle
import re

from pyline.config.models import TableDef, TableFieldDef
from pyline.db.serialization import dumps
from pyline.tools.migrate_pickle import migrate_blobs, restricted_loads


class _Secret:
    """Guinea pig for the forbidden-global check."""


class FakeMigratePool:
    """Returns canned rows per (table, column); records UPDATEs."""

    def __init__(
        self, rows: dict[tuple[str, str], list[tuple[object, bytes | None]]]
    ) -> None:
        self.rows = rows
        self.updates: list[tuple[str, tuple]] = []

    async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
        match = re.search(r"SELECT `(\w+)`, `(\w+)` FROM `(\w+)`", sql)
        assert match is not None, sql
        return list(self.rows.get((match.group(3), match.group(2)), []))

    async def execute(self, sql: str, args: tuple = ()) -> int:
        self.updates.append((sql, args))
        return 1


def tables_def() -> dict[str, TableDef]:
    return {
        "tbl_player": TableDef(
            fields={
                "id": TableFieldDef(type="BIGINT", primary=True),
                "data": TableFieldDef(type="MEDIUMBLOB"),
            }
        )
    }


class TestMigrateBlobs:
    async def test_dry_run_reports_without_writing(self) -> None:
        rows = {
            ("tbl_player", "data"): [
                (1, pickle.dumps({"gold": 5, "tags": (1, 2)})),
                (2, dumps({"already": "pyline"})),
                (3, pickle.dumps(_Secret())),
            ]
        }
        pool = FakeMigratePool(rows)
        report = await migrate_blobs(pool, tables_def(), execute=False)
        col = report.columns[0]
        assert col.scanned == 3
        assert col.converted == 1  # the legacy pickle row
        assert col.skipped_modern == 1
        assert col.failed == 1  # forbidden global reference
        assert pool.updates == []  # dry run writes nothing
        assert col.failures and "_Secret" in " ".join(col.failures)

    async def test_execute_writes_verified_msgpack(self) -> None:
        legacy = {"gold": 5, "tags": (1, 2)}
        pool = FakeMigratePool({("tbl_player", "data"): [(7, pickle.dumps(legacy))]})
        report = await migrate_blobs(pool, tables_def(), execute=True)
        assert report.total_converted == 1
        assert len(pool.updates) == 1
        sql, args = pool.updates[0]
        assert sql.startswith("UPDATE `tbl_player` SET `data` = %s WHERE `id` = %s")
        blob = args[0]
        assert blob[:4] == b"PLD1"
        from pyline.db.serialization import loads

        assert loads(blob) == {"gold": 5, "tags": [1, 2]}  # tuple -> list normalization

    async def test_restricted_loads_rejects_globals(self) -> None:
        try:
            restricted_loads(pickle.dumps(_Secret()))
        except pickle.UnpicklingError as exc:
            assert "forbidden" in str(exc)
        else:
            raise AssertionError("expected UnpicklingError for global reference")

    async def test_restricted_loads_accepts_plain_data(self) -> None:
        data = {"list": [1, 2.5, "x", True, None], "nested": {"k": (3, 4)}}
        assert restricted_loads(pickle.dumps(data)) == data
