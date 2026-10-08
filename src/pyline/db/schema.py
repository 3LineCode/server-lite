"""Table schema definitions and safe migration.

Identifiers (table/column names) come from the tables config and are validated
against a strict pattern before ever reaching DDL; all data values travel as
bound parameters. Migration is additive: missing tables are created, missing
columns are ALTERed in; nothing is ever dropped or retyped automatically
(prototype issue #22 -- "create if absent" upgraded to versioned evolution).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from pyline.config.models import TableDef
from pyline.db.mysql import MySQLPool

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


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


class SchemaManager:
    """Creates the database and evolves tables additively at startup."""

    def __init__(self, pool: MySQLPool, tables: dict[str, TableDef], db_name: str) -> None:
        self._pool = pool
        self._tables = {name: TableSpec.from_def(name, tdef) for name, tdef in tables.items()}
        check_identifier(db_name)
        self._db_name = db_name
        self.alter_statements: list[str] = []

    async def ensure_all(self) -> None:
        await self._ensure_database()
        await self._ensure_tables()

    async def _ensure_database(self) -> None:
        # The database name was identifier-validated in __init__.
        await self._pool.execute(
            f"CREATE DATABASE IF NOT EXISTS `{self._db_name}` DEFAULT CHARACTER SET utf8mb4"
        )

    async def _ensure_tables(self) -> None:
        existing = {row[0] for row in await self._pool.query("SHOW TABLES")}
        for name, spec in self._tables.items():
            if name not in existing:
                logger.info("creating table %s", name)
                await self._pool.execute(spec.create_sql())
                continue
            await self._add_missing_columns(name, spec)

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
            statement = f"ALTER TABLE `{name}` ADD COLUMN {col.ddl()}"
            logger.info("adding column %s.%s", name, col.name)
            await self._pool.execute(statement)
            self.alter_statements.append(statement)

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
