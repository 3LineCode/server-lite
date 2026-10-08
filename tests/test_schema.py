"""Schema: table specs, DDL generation, identifier safety, versioned migration."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from pyline.config.models import TableDef, TableFieldDef
from pyline.db.schema import (
    SchemaError,
    SchemaManager,
    TableSpec,
    check_identifier,
)


def make_def() -> TableDef:
    return TableDef(
        comment="players",
        fields={
            "id": TableFieldDef(type="BIGINT", primary=True, comment="pk"),
            "data": TableFieldDef(type="MEDIUMBLOB"),
        },
    )


class SchemaFakePool:
    """Minimal MySQLPool stand-in exercising SchemaManager's SQL contract."""

    def __init__(
        self,
        *,
        tables: set[str] | None = None,
        columns: dict[str, list[tuple[str, str, str]]] | None = None,
        version: int = 0,
    ) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.tables = set(tables or ())
        # columns: table -> list of (name, column_type, is_nullable)
        self.columns: dict[str, list[tuple[str, str, str]]] = dict(columns or {})
        self.version = version

    async def execute(self, sql: str, args: tuple = ()) -> int:
        self.statements.append((sql, args))
        create = re.match(r"CREATE TABLE (?:IF NOT EXISTS )?`(\w+)`", sql)
        if create:
            self.tables.add(create.group(1))
            self.columns.setdefault(create.group(1), [])
        elif sql.startswith(("INSERT INTO `pyline_schema`", "UPDATE `pyline_schema`")):
            self.version = args[0]
        alter = re.search(r"ALTER TABLE `(\w+)` ADD COLUMN `(\w+)` (`\w+` \w+)", sql)
        if alter:
            self.columns.setdefault(alter.group(1), []).append(
                (alter.group(2), alter.group(3).strip("` "), "YES")
            )
        return 1

    async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
        self.statements.append((sql, args))
        if sql == "SHOW TABLES":
            return [(t,) for t in self.tables]
        if sql.startswith("SELECT `version` FROM `pyline_schema`"):
            return [(self.version,)]
        if "information_schema.COLUMNS" in sql:
            table = args[1]
            if "IS_NULLABLE" in sql:
                return list(self.columns.get(table, []))
            return [(name,) for name, _, _ in self.columns.get(table, [])]
        return []


class TestIdentifiers:
    def test_valid(self) -> None:
        assert check_identifier("tbl_player") == "tbl_player"

    @pytest.mark.parametrize("bad", ["", "1abc", "has space", "drop;--", "a" * 100])
    def test_invalid(self, bad: str) -> None:
        with pytest.raises(SchemaError):
            check_identifier(bad)


class TestTableSpec:
    def test_from_def(self) -> None:
        spec = TableSpec.from_def("tbl_player", make_def())
        assert spec.primary_column().name == "id"

    def test_no_primary_rejected(self) -> None:
        bad = TableDef(fields={"x": TableFieldDef(type="INT")})
        with pytest.raises(SchemaError, match="primary"):
            TableSpec.from_def("t", bad)

    def test_create_sql(self) -> None:
        spec = TableSpec.from_def("tbl_player", make_def())
        sql = spec.create_sql()
        assert sql.startswith("CREATE TABLE `tbl_player`")
        assert "`id` BIGINT PRIMARY KEY" in sql
        assert "`data` MEDIUMBLOB" in sql

    def test_query_and_upsert_use_parameters(self) -> None:
        spec = TableSpec.from_def("tbl_player", make_def())
        assert spec.query_sql("data").count("%s") == 1
        upsert = spec.upsert_sql("data")
        assert "%s" in upsert and "ON DUPLICATE KEY UPDATE" in upsert

    def test_column_type_width_less(self) -> None:
        spec = TableSpec.from_def(
            "t", TableDef(fields={"id": TableFieldDef(type="INT(4)", primary=True)})
        )
        assert spec.columns["id"].column_type() == "int"  # display width stripped


class TestVersionedMigration:
    async def test_schema_version_table(self) -> None:
        pool = SchemaFakePool()
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        await manager.ensure_all()
        assert "pyline_schema" in pool.tables
        assert "tbl_player" in pool.tables
        assert await manager.current_version() == 0

    async def test_schema_migration_linear_apply(self, tmp_path: Path) -> None:
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_add_score.sql").write_text(
            "-- add a score column\nALTER TABLE `tbl_player` ADD COLUMN `score` BIGINT NULL;\n",
            encoding="utf-8",
        )
        (migrations / "002_add_rank.sql").write_text(
            "ALTER TABLE `tbl_player` ADD COLUMN `rank` BIGINT NULL;\n",
            encoding="utf-8",
        )
        pool = SchemaFakePool(version=1)  # 001 already applied on a previous boot
        manager = SchemaManager(
            pool, {"tbl_player": make_def()}, "test_db", migrations_dir=migrations
        )
        await manager.ensure_all()
        assert await manager.current_version() == 2
        applied = [sql for sql, _ in pool.statements if "ADD COLUMN `rank`" in sql]
        assert len(applied) == 1
        skipped = [sql for sql, _ in pool.statements if "ADD COLUMN `score`" in sql]
        assert not skipped  # 001 was below the recorded version

    async def test_schema_migration_rejects_bad_names(self, tmp_path: Path) -> None:
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "no_number.sql").write_text("SELECT 1;", encoding="utf-8")
        manager = SchemaManager(
            SchemaFakePool(), {"tbl_player": make_def()}, "db", migrations_dir=migrations
        )
        with pytest.raises(SchemaError, match=r"NNN_name\.sql"):
            await manager.ensure_all()

    async def test_schema_migration_rejects_duplicates(self, tmp_path: Path) -> None:
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_a.sql").write_text("SELECT 1;", encoding="utf-8")
        (migrations / "001_b.sql").write_text("SELECT 1;", encoding="utf-8")
        manager = SchemaManager(
            SchemaFakePool(), {"tbl_player": make_def()}, "db", migrations_dir=migrations
        )
        with pytest.raises(SchemaError, match="duplicate migration numbers"):
            await manager.ensure_all()

    async def test_schema_drift_detected(self) -> None:
        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={"tbl_player": [("id", "varchar(127)", "NO"), ("data", "mediumblob", "YES")]},
        )
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        with pytest.raises(SchemaError, match="type drift"):
            await manager.ensure_all()

    async def test_add_column_pins_instant_algorithm(self) -> None:
        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={"tbl_player": [("id", "bigint", "NO")]},  # data column missing
        )
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        await manager.ensure_all()
        alters = [sql for sql, _ in pool.statements if sql.startswith("ALTER TABLE")]
        assert alters and all(
            sql.endswith("ALGORITHM=INSTANT, LOCK=NONE") for sql in alters
        )
