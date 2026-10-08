"""Table schema definitions and safe migration.

Identifiers (table/column names) come from the tables config and are validated
against a strict pattern before ever reaching DDL; all data values travel as
bound parameters.

Migration model (migration plan F-06, Alembic-style):

* a single-row ``pyline_schema`` version table records the highest applied
  migration number;
* scripts in ``migrations/NNN_name.sql`` apply in order on every startup,
  skipping already-applied numbers; scripts must be idempotent (DDL does not
  roll back in MySQL) and additive-only (expand-contract: dropping/retyping
  happens in a deliberate maintenance window, never at boot);
* declared-vs-live drift (type/nullability mismatch on existing columns)
  fails startup with a diff instead of silently auto-ALTERing;
* generated ``ADD COLUMN`` statements pin ``ALGORITHM=INSTANT, LOCK=NONE``
  so a non-instant change fails fast rather than rebuilding a big table.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyline.config.models import TableDef
from pyline.db.mysql import MySQLPool

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

VERSION_TABLE = "pyline_schema"

# Integer display widths (INT(4)) are deprecated in MySQL 8: information_schema
# reports them without the width, so the drift check compares the bare name.
_WIDTH_LESS_TYPES = frozenset(
    {"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR"}
)


class SchemaError(ValueError):
    pass


def check_identifier(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise SchemaError(f"invalid SQL identifier: {name!r}")
    return name


@dataclass(slots=True)
class ColumnSpec:
    name: str
    data_type: str  # e.g. VARCHAR, BIGINT, MEDIUMBLOB
    length: int | None = None
    primary: bool = False
    unique: bool = False
    nullable: bool = True
    default: str | None = None
    comment: str = ""

    def ddl(self) -> str:
        check_identifier(self.name)
        if self.length is not None:
            part = f"`{self.name}` {self.data_type}({self.length})"
        else:
            part = f"`{self.name}` {self.data_type}"
        if self.primary:
            part += " PRIMARY KEY"
        if self.unique:
            part += " UNIQUE KEY"
        if self.default is not None:
            part += f" DEFAULT {self.default}"
        if not self.nullable and self.data_type not in ("MEDIUMTEXT", "MEDIUMBLOB"):
            part += " NOT NULL"
        if self.comment:
            part += f" COMMENT '{self.comment.replace(chr(39), chr(39) * 2)}'"
        return part

    def column_type(self) -> str:
        """Canonical ``COLUMN_TYPE`` string as information_schema reports it."""
        name = self.data_type.lower()
        if self.length is not None and self.data_type.upper() not in _WIDTH_LESS_TYPES:
            return f"{name}({self.length})"
        return name


@dataclass(slots=True)
class TableSpec:
    name: str
    comment: str = ""
    columns: dict[str, ColumnSpec] = field(default_factory=dict)

    @classmethod
    def from_def(cls, name: str, table_def: TableDef) -> TableSpec:
        check_identifier(name)
        spec = cls(name=name, comment=table_def.comment)
        for col_name, col_def in table_def.fields.items():
            check_identifier(col_name)
            data_type, length = _parse_type(col_def.type)
            spec.columns[col_name] = ColumnSpec(
                name=col_name,
                data_type=data_type,
                length=length,
                primary=col_def.primary,
                unique=col_def.primary,
                nullable=not col_def.primary,
                comment=col_def.comment,
            )
        if not any(col.primary for col in spec.columns.values()):
            raise SchemaError(f"table {name!r} has no primary key column")
        return spec

    def primary_column(self) -> ColumnSpec:
        return next(col for col in self.columns.values() if col.primary)

    def create_sql(self) -> str:
        check_identifier(self.name)
        parts = ",\n  ".join(col.ddl() for col in self.columns.values())
        return f"CREATE TABLE `{self.name}` (\n  {parts}\n) COMMENT '{self.comment}'"

    def query_sql(self, column: str) -> str:
        check_identifier(self.name)
        check_identifier(column)
        pk = self.primary_column().name
        return f"SELECT `{column}` FROM `{self.name}` WHERE `{pk}` = %s"

    def upsert_sql(self, column: str) -> str:
        check_identifier(self.name)
        check_identifier(column)
        pk = self.primary_column().name
        return (
            f"INSERT INTO `{self.name}` (`{pk}`, `{column}`) VALUES (%s, %s) "
            f"ON DUPLICATE KEY UPDATE `{column}` = VALUES(`{column}`)"
        )

    def insert_row_sql(self) -> str:
        check_identifier(self.name)
        pk = self.primary_column().name
        return f"INSERT IGNORE INTO `{self.name}` (`{pk}`) VALUES (%s)"

    def delete_sql(self) -> str:
        check_identifier(self.name)
        pk = self.primary_column().name
        return f"DELETE FROM `{self.name}` WHERE `{pk}` = %s"


def _parse_type(type_str: str) -> tuple[str, int | None]:
    match = re.fullmatch(r"([A-Za-z]+)(?:\((\d+)\))?", type_str.strip())
    if not match:
        raise SchemaError(f"unparseable column type: {type_str!r}")
    name = match.group(1).upper()
    length = int(match.group(2)) if match.group(2) else None
    return name, length


def _split_statements(sql_text: str) -> list[str]:
    """Split a migration file into statements on ``;``.

    ``--`` comment lines are dropped.  String literals containing semicolons
    are not supported -- migrations are DDL, keep values out of them.
    """
    statements: list[str] = []
    for chunk in sql_text.split(";"):
        lines = [
            line
            for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


class SchemaManager:
    """Creates the database and evolves tables additively at startup."""

    def __init__(
        self,
        pool: MySQLPool,
        tables: dict[str, TableDef],
        db_name: str,
        *,
        migrations_dir: Path | None = None,
    ) -> None:
        self._pool = pool
        self._tables = {name: TableSpec.from_def(name, tdef) for name, tdef in tables.items()}
        check_identifier(db_name)
        self._db_name = db_name
        self._migrations_dir = migrations_dir
        self.alter_statements: list[str] = []

    async def ensure_all(self) -> None:
        await self._ensure_database()
        await self._ensure_version_table()
        await self._ensure_tables()
        await self._apply_migrations()

    async def _ensure_database(self) -> None:
        # The database name was identifier-validated in __init__.
        await self._pool.execute(
            f"CREATE DATABASE IF NOT EXISTS `{self._db_name}` DEFAULT CHARACTER SET utf8mb4"
        )

    async def _ensure_version_table(self) -> None:
        await self._pool.execute(
            f"CREATE TABLE IF NOT EXISTS `{VERSION_TABLE}` (\n"
            "  `version` INT NOT NULL,\n"
            "  `applied_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            ") COMMENT 'pyline schema version (single row)'"
        )
        rows = await self._pool.query(f"SELECT `version` FROM `{VERSION_TABLE}`")
        if not rows:
            await self._pool.execute(f"INSERT INTO `{VERSION_TABLE}` (`version`) VALUES (0)")

    async def current_version(self) -> int:
        rows = await self._pool.query(f"SELECT `version` FROM `{VERSION_TABLE}`")
        return int(rows[0][0]) if rows else 0

    async def _ensure_tables(self) -> None:
        existing = {row[0] for row in await self._pool.query("SHOW TABLES")}
        for name, spec in self._tables.items():
            if name not in existing:
                logger.info("creating table %s", name)
                await self._pool.execute(spec.create_sql())
                continue
            await self._add_missing_columns(name, spec)
            await self._check_drift(name, spec)

    async def _add_missing_columns(self, name: str, spec: TableSpec) -> None:
        rows = await self._pool.query(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (self._db_name, name),
        )
        present = {row[0] for row in rows}
        for col in spec.columns.values():
            if col.name in present:
                continue
            statement = (
                f"ALTER TABLE `{name}` ADD COLUMN {col.ddl()}, "
                "ALGORITHM=INSTANT, LOCK=NONE"
            )
            logger.info("adding column %s.%s", name, col.name)
            await self._pool.execute(statement)
            self.alter_statements.append(statement)

    async def _check_drift(self, name: str, spec: TableSpec) -> None:
        """Declared config vs live columns: mismatch fails startup (F-06)."""
        rows = await self._pool.query(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (self._db_name, name),
        )
        actual = {row[0]: (row[1], row[2]) for row in rows}
        problems: list[str] = []
        for col in spec.columns.values():
            if col.name not in actual:
                continue  # missing columns are handled additively above
            live_type, live_nullable = actual[col.name]
            if live_type.lower() != col.column_type():
                problems.append(
                    f"{name}.{col.name}: type drift, declared {col.column_type()}, "
                    f"live {live_type}"
                )
            expected_nullable = "YES" if col.nullable else "NO"
            if live_nullable != expected_nullable:
                problems.append(
                    f"{name}.{col.name}: nullability drift, declared "
                    f"{expected_nullable}, live {live_nullable}"
                )
        if problems:
            raise SchemaError(
                "schema drift detected (tables config vs live database):\n  "
                + "\n  ".join(problems)
                + "\nrefusing to auto-ALTER; migrate explicitly via migrations/"
            )

    # ------------------------------ migrations ------------------------------ #

    def _load_migrations(self) -> list[tuple[int, Path]]:
        if self._migrations_dir is None or not self._migrations_dir.is_dir():
            return []
        found: list[tuple[int, Path]] = []
        for path in sorted(self._migrations_dir.glob("*.sql")):
            match = re.fullmatch(r"(\d{3,})_.+\.sql", path.name)
            if not match:
                raise SchemaError(f"migration file name must be NNN_name.sql: {path.name!r}")
            found.append((int(match.group(1)), path))
        numbers = [n for n, _ in found]
        if len(set(numbers)) != len(numbers):
            duplicates = sorted({n for n in numbers if numbers.count(n) > 1})
            raise SchemaError(f"duplicate migration numbers: {duplicates}")
        return sorted(found)

    async def _apply_migrations(self) -> None:
        applied = await self.current_version()
        for number, path in self._load_migrations():
            if number <= applied:
                continue
            for statement in _split_statements(path.read_text(encoding="utf-8")):
                logger.info("applying migration %s: %s", path.name, statement.splitlines()[0])
                await self._pool.execute(statement)
            await self._pool.execute(
                f"UPDATE `{VERSION_TABLE}` SET `version` = %s WHERE `version` < %s",
                (number, number),
            )
            logger.info("applied migration %s (version %d)", path.name, number)

    def table(self, name: str) -> TableSpec:
        try:
            return self._tables[name]
        except KeyError:
            raise SchemaError(f"table {name!r} not defined in tables config") from None

    async def row_exists(self, table: str, key: Any) -> bool:
        spec = self.table(table)
        pk = spec.primary_column().name
        rows = await self._pool.query(f"SELECT 1 FROM `{table}` WHERE `{pk}` = %s LIMIT 1", (key,))
        return bool(rows)
