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


class FakeSchemaLockSession:
    """Stands in for the dedicated MySQLSession that holds GET_LOCK (F-67)."""

    def __init__(self, pool: SchemaFakePool) -> None:
        self._pool = pool
        self.closed = False
        self.released = False

    async def query(self, sql: str, args: tuple = ()) -> list[tuple]:
        self._pool.lock_statements.append((sql, args))
        if sql.startswith("SELECT GET_LOCK"):
            return [(self._pool.get_lock_result,)]
        if sql.startswith("SELECT RELEASE_LOCK"):
            self.released = True
            return [(1,)]
        return []

    async def close(self) -> None:
        self.closed = True


class SchemaFakePool:
    """Minimal MySQLPool stand-in exercising SchemaManager's SQL contract."""

    def __init__(
        self,
        *,
        tables: set[str] | None = None,
        columns: dict[str, list[tuple[str, ...]]] | None = None,
        version: int = 0,
    ) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.tables = set(tables or ())
        # columns: table -> list of (name, column_type, is_nullable[, column_key])
        self.columns: dict[str, list[tuple[str, str, str]]] = dict(columns or {})
        self.version = version
        # F-51: migration -> statements applied by a (possibly failed) attempt
        self.migration_progress: dict[int, int] = {}
        # F-67: the dedicated GET_LOCK session
        self.get_lock_result = 1
        self.lock_statements: list[tuple[str, tuple]] = []
        self.sessions: list[FakeSchemaLockSession] = []
        # server version reported to SELECT VERSION() (ODKU syntax probe)
        self.server_version = "5.7.44"

    async def open_session(self) -> FakeSchemaLockSession:
        session = FakeSchemaLockSession(self)
        self.sessions.append(session)
        return session

    async def execute(self, sql: str, args: tuple = ()) -> int:
        self.statements.append((sql, args))
        create = re.match(r"CREATE TABLE (?:IF NOT EXISTS )?`(\w+)`", sql)
        if create:
            self.tables.add(create.group(1))
            self.columns.setdefault(create.group(1), [])
        elif sql.startswith(("INSERT INTO `pyline_schema`", "UPDATE `pyline_schema`")):
            self.version = args[0]
        elif sql.startswith("INSERT INTO `pyline_schema_progress`"):
            self.migration_progress[args[0]] = args[1]
        elif sql.startswith("DELETE FROM `pyline_schema_progress`"):
            self.migration_progress.pop(args[0], None)
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
        if sql == "SELECT VERSION()":
            return [(self.server_version,)]
        if sql.startswith("SELECT `statements` FROM `pyline_schema_progress`"):
            statements = self.migration_progress.get(args[0])
            return [(statements,)] if statements is not None else []
        if "information_schema.COLUMNS" in sql:
            table = args[1]
            # Fixtures may carry 3-field rows (name, type, nullable); the
            # drift check also reads COLUMN_KEY (F-207) -- synthesize it for
            # legacy rows: every fixture spells its primary "id".
            rows: list[tuple[str, ...]] = []
            for row in self.columns.get(table, []):
                rows.append(row if len(row) >= 4 else (*row, "PRI" if row[0] == "id" else ""))
            if "IS_NULLABLE" in sql:
                return rows
            return [(name,) for name, *_ in rows]
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


class TestDDLHardening:
    def test_create_sql_escapes_table_comment(self) -> None:
        spec = TableSpec.from_def("tbl_player", make_def())
        spec.comment = "player's data"
        sql = spec.create_sql()
        assert "COMMENT 'player''s data'" in sql

    def test_create_sql_rejects_table_comment_backslash(self) -> None:
        """The table-level path used to double quotes without rejecting
        backslashes (the column-level check_comment does); a trailing
        backslash escaped the closing quote and shifted the rest of the
        CREATE TABLE DDL."""
        spec = TableSpec.from_def("tbl_player", make_def())
        spec.comment = "trailing backslash\\"
        with pytest.raises(SchemaError, match="backslashes are not allowed"):
            spec.create_sql()

    def test_default_literal_whitelist(self) -> None:
        from pyline.db.schema import check_default_literal

        assert check_default_literal("42") == "42"
        assert check_default_literal("-1.5") == "-1.5"
        assert check_default_literal("null") == "NULL"
        assert check_default_literal("'it''s'") == "'it''s'"  # already-escaped passes
        for bad in ["1; DROP TABLE x", "now()", "x'y", "'a\\b'", "", "'unbalanced"]:
            with pytest.raises(SchemaError, match="DEFAULT literal"):
                check_default_literal(bad)

    def test_field_flags_wired_into_ddl(self) -> None:
        spec = TableSpec.from_def(
            "t",
            TableDef(
                fields={
                    "id": TableFieldDef(type="BIGINT", primary=True),
                    "name": TableFieldDef(
                        type="VARCHAR(64)",
                        not_null=True,
                        unique=True,
                        default="'anon'",
                    ),
                }
            ),
        )
        ddl = spec.columns["name"].ddl()
        assert "UNIQUE KEY" in ddl
        assert "NOT NULL" in ddl
        assert "DEFAULT 'anon'" in ddl
        assert spec.columns["name"].nullable is False

    def test_blob_columns_stay_nullable(self) -> None:
        # prototype semantics: TEXT/BLOB never get NOT NULL even if asked
        spec = TableSpec.from_def(
            "t",
            TableDef(
                fields={
                    "id": TableFieldDef(type="BIGINT", primary=True),
                    "data": TableFieldDef(type="MEDIUMBLOB", not_null=True),
                }
            ),
        )
        assert "NOT NULL" not in spec.columns["data"].ddl()


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

    async def test_failed_migration_resumes_from_failed_statement(self, tmp_path: Path) -> None:
        """F-51: a statement failing mid-file used to leave no progress; the
        next boot re-ran the WHOLE file, trusting script idempotency. Now it
        resumes from the failed statement only."""

        class FailBoomOnce(SchemaFakePool):
            boom_seen = False

            async def execute(self, sql: str, args: tuple = ()) -> int:
                if "CREATE TABLE `boom`" in sql and not self.boom_seen:
                    self.boom_seen = True
                    raise ConnectionError("migration statement failed")
                return await super().execute(sql, args)

        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_init.sql").write_text(
            "CREATE TABLE `tbl_a` (id INT);\n"
            "CREATE TABLE `boom` (id INT);\n"
            "CREATE TABLE `tbl_c` (id INT);\n",
            encoding="utf-8",
        )
        pool = FailBoomOnce()
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "db", migrations_dir=migrations)
        with pytest.raises(ConnectionError, match="migration statement failed"):
            await manager.ensure_all()
        assert pool.migration_progress == {1: 1}  # first statement landed, second failed

        # next boot: resumes at statement 2, does not re-run statement 1
        pool2 = SchemaFakePool(tables={"pyline_schema", "pyline_schema_progress", "tbl_player"})
        pool2.migration_progress = {1: 1}
        manager2 = SchemaManager(pool2, {"tbl_player": make_def()}, "db", migrations_dir=migrations)
        await manager2.ensure_all()
        assert pool2.migration_progress == {}  # completed: progress row cleared
        assert pool2.version == 1
        firsts = [sql for sql, _ in pool2.statements if "tbl_a" in sql]
        assert not firsts  # statement 1 was NOT re-executed
        booms = [sql for sql, _ in pool2.statements if "`boom`" in sql and "CREATE" in sql]
        assert len(booms) == 1
        tbl_c = [sql for sql, _ in pool2.statements if "`tbl_c`" in sql and "CREATE" in sql]
        assert len(tbl_c) == 1

    async def test_completed_statements_only_crashed_before_version(self, tmp_path: Path) -> None:
        """F-51: all statements landed but the process died before the version
        update -- the next boot completes the version bump without re-running
        any statement."""
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_init.sql").write_text(
            "CREATE TABLE `tbl_a` (id INT);\nCREATE TABLE `tbl_b` (id INT);\n",
            encoding="utf-8",
        )
        pool = SchemaFakePool(tables={"pyline_schema", "pyline_schema_progress", "tbl_player"})
        pool.migration_progress = {1: 2}  # everything applied, version not bumped
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "db", migrations_dir=migrations)
        await manager.ensure_all()
        assert pool.version == 1
        creates = [sql for sql, _ in pool.statements if "CREATE TABLE `tbl_" in sql]
        assert not creates  # nothing re-executed

    async def test_shrunk_migration_file_refused(self, tmp_path: Path) -> None:
        """F-51: progress beyond the file's statement count means the file was
        edited after a failed attempt -- refuse instead of mis-resuming."""
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_init.sql").write_text(
            "CREATE TABLE `tbl_a` (id INT);\n", encoding="utf-8"
        )
        pool = SchemaFakePool(tables={"pyline_schema", "pyline_schema_progress", "tbl_player"})
        pool.migration_progress = {1: 5}
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "db", migrations_dir=migrations)
        with pytest.raises(SchemaError, match="the file changed in between"):
            await manager.ensure_all()

    async def test_schema_drift_detected(self) -> None:
        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={"tbl_player": [("id", "varchar(127)", "NO"), ("data", "mediumblob", "YES")]},
        )
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        with pytest.raises(SchemaError, match="type drift"):
            await manager.ensure_all()

    async def test_not_null_blob_no_self_drift(self) -> None:
        # F-38: ddl() creates TEXT/BLOB nullable (F-36), so a `not_null: true`
        # blob column whose live row is nullable is exactly what this framework
        # itself created -- the drift check must expect that, not the raw flag.
        blob_def = TableDef(
            fields={
                "id": TableFieldDef(type="BIGINT", primary=True),
                "data": TableFieldDef(type="MEDIUMBLOB", not_null=True),
            }
        )
        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={"tbl_player": [("id", "bigint", "NO"), ("data", "mediumblob", "YES")]},
        )
        manager = SchemaManager(pool, {"tbl_player": blob_def}, "test_db")
        await manager.ensure_all()  # must not raise nullability drift

    async def test_genuinely_wrong_nullability_still_detected(self) -> None:
        # the F-38 fix must not silence real drift: a non-blob column declared
        # NOT NULL but nullable live still fails startup
        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={"tbl_player": [("id", "bigint", "NO"), ("name", "varchar(64)", "YES")]},
        )
        strict_def = TableDef(
            fields={
                "id": TableFieldDef(type="BIGINT", primary=True),
                "name": TableFieldDef(type="VARCHAR(64)", not_null=True),
            }
        )
        manager = SchemaManager(pool, {"tbl_player": strict_def}, "test_db")
        with pytest.raises(SchemaError, match="nullability drift"):
            await manager.ensure_all()

    async def test_add_column_pins_instant_algorithm(self) -> None:
        pool = SchemaFakePool(
            tables={"tbl_player"},
            columns={"tbl_player": [("id", "bigint", "NO")]},  # data column missing
        )
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        await manager.ensure_all()
        alters = [sql for sql, _ in pool.statements if sql.startswith("ALTER TABLE")]
        assert alters and all(sql.endswith("ALGORITHM=INSTANT, LOCK=NONE") for sql in alters)

    async def test_migration_file_with_semicolon_comments(self, tmp_path: Path) -> None:
        """F-66 end to end: a semicolon inside a ``--`` comment used to split
        the comment open and glue its tail onto the next statement."""
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_c.sql").write_text(
            "-- create the foo; bar tables\n"
            "CREATE TABLE `tbl_a` (id INT); -- trailing note; with semicolon\n"
            "\n-- a whole comment; line\n"
            "CREATE TABLE `tbl_b` (id INT)\n;",
            encoding="utf-8",
        )
        pool = SchemaFakePool()
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "db", migrations_dir=migrations)
        await manager.ensure_all()
        creates = [
            sql
            for sql, _ in pool.statements
            if sql.startswith(("CREATE TABLE `tbl_a`", "CREATE TABLE `tbl_b`"))
        ]
        assert creates == ["CREATE TABLE `tbl_a` (id INT)", "CREATE TABLE `tbl_b` (id INT)"]


class TestSplitStatementsF66:
    def test_semicolon_inside_comment_does_not_split(self) -> None:
        from pyline.db.schema import _split_statements

        sql = (
            "-- create the foo; bar tables\n"
            "CREATE TABLE `a` (id INT); -- trailing note; with semicolon\n"
            "\n-- a whole comment; line\n"
            "CREATE TABLE `b` (id INT)\n;"
        )
        assert _split_statements(sql) == ["CREATE TABLE `a` (id INT)", "CREATE TABLE `b` (id INT)"]

    def test_plain_statements_unchanged(self) -> None:
        from pyline.db.schema import _split_statements

        sql = "SELECT 1;\nSELECT 2;\n"
        assert _split_statements(sql) == ["SELECT 1", "SELECT 2"]


class TestMigrationLockF67:
    async def test_ensure_all_takes_and_releases_named_lock(self) -> None:
        """F-67: the whole ensure+migrate cycle runs while holding a MySQL
        named lock scoped to this database, and the lock is released on the
        dedicated session that took it."""
        pool = SchemaFakePool()
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        await manager.ensure_all()
        assert len(pool.sessions) == 1
        get_locks = [sql for sql, _ in pool.lock_statements if sql.startswith("SELECT GET_LOCK")]
        assert get_locks == ["SELECT GET_LOCK(%s, %s)"]
        _sql, args = pool.lock_statements[0]
        assert args == ("pyline_schema.test_db", 60)
        releases = [sql for sql, _ in pool.lock_statements if sql.startswith("SELECT RELEASE_LOCK")]
        assert releases == ["SELECT RELEASE_LOCK(%s)"]
        assert pool.sessions[0].released and pool.sessions[0].closed

    async def test_lock_unavailable_fails_loudly(self) -> None:
        """F-67: a peer holding the lock past the bounded wait is an error --
        never a silent skip that would migrate unsynchronized."""
        pool = SchemaFakePool()
        pool.get_lock_result = 0
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        with pytest.raises(SchemaError, match="migration lock"):
            await manager.ensure_all()
        assert pool.statements == []  # nothing was ensured or migrated
        assert pool.sessions[0].closed  # the dedicated session is cleaned up

    async def test_server_error_on_get_lock_fails_loudly(self) -> None:
        pool = SchemaFakePool()
        pool.get_lock_result = None  # NULL: server-side error
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        with pytest.raises(SchemaError, match="migration lock"):
            await manager.ensure_all()

    async def test_lock_released_when_migration_fails(self) -> None:
        """F-67: a migration statement exploding mid-file must still release
        the lock and close the session (finally, not luck)."""

        class BoomPool(SchemaFakePool):
            async def execute(self, sql: str, args: tuple = ()) -> int:
                raise ConnectionError("db gone mid-migration")

        pool = BoomPool()
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        with pytest.raises(ConnectionError, match="mid-migration"):
            await manager.ensure_all()
        assert pool.sessions[0].released and pool.sessions[0].closed


class TestOdkuSyntaxF132:
    """ON DUPLICATE KEY UPDATE: the deprecated VALUES(col) form switches to
    the 8.0.19+ row-alias form when the server supports it."""

    def test_alias_form_when_enabled(self) -> None:
        spec = TableSpec.from_def("tbl_player", make_def())
        assert "VALUES(`data`)" in spec.upsert_sql("data")
        spec.odku_alias = True
        single = spec.upsert_sql("data")
        many = spec.upsert_many_sql("data", 3)
        assert "AS `_new`" in single and "`_new`.`data`" in single
        assert "VALUES(" not in single.replace("VALUES %s", "")
        assert "AS `_new`" in many and many.count("(%s, %s)") == 3

    async def test_version_probe_selects_syntax(self) -> None:
        pool = SchemaFakePool(tables={"tbl_player"}, columns={"tbl_player": []}, version=0)
        pool.server_version = "8.0.34"
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        await manager.ensure_all()
        assert manager.table("tbl_player").odku_alias is True

    async def test_old_server_keeps_legacy_values(self) -> None:
        pool = SchemaFakePool(tables={"tbl_player"}, columns={"tbl_player": []}, version=0)
        pool.server_version = "8.0.18"  # alias form lands in 8.0.19
        manager = SchemaManager(pool, {"tbl_player": make_def()}, "test_db")
        await manager.ensure_all()
        assert manager.table("tbl_player").odku_alias is False


class TestSplitStatementsStringsF133:
    """The migration splitter honours string literals and block comments, not
    just ``--`` lines."""

    def test_semicolon_inside_string_literal(self) -> None:
        from pyline.db.schema import _split_statements

        stmts = _split_statements(
            "INSERT INTO t VALUES ('a;b');\n-- comment; with semicolon\n"
            "UPDATE t SET c = 'it''s;fine';"
        )
        assert len(stmts) == 2
        assert "'a;b'" in stmts[0]
        assert "it''s;fine" in stmts[1]

    def test_block_comments_stripped(self) -> None:
        from pyline.db.schema import _split_statements

        stmts = _split_statements("/* header; v2 */ ALTER TABLE t ADD c INT;\n-- tail; note\n")
        assert stmts == ["ALTER TABLE t ADD c INT"]

    def test_dashes_glued_to_identifier_are_not_a_comment(self) -> None:
        from pyline.db.schema import _split_statements

        stmts = _split_statements("SELECT a--b FROM t;")
        assert stmts == ["SELECT a--b FROM t"]
