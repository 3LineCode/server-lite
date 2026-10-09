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

import contextlib
import logging
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from pyline.config.models import TableDef
from pyline.db.mysql import MySQLPool
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

VERSION_TABLE = "pyline_schema"
# F-51: per-migration resume progress (statements applied by a failed attempt).
PROGRESS_TABLE = "pyline_schema_progress"

# F-67: cross-process migration mutex.  GET_LOCK is advisory and scoped to a
# single connection, so the lock runs on one dedicated out-of-pool session for
# the whole ensure+migrate cycle.  60 s is generous for a peer that is already
# finishing its migrations; past it we fail loudly rather than run unsynchronized.
_MIGRATION_LOCK_WAIT_SECONDS = 60
_MIGRATION_LOCK_PREFIX = "pyline_schema."

# Integer display widths (INT(4)) are deprecated in MySQL 8: information_schema
# reports them without the width, so the drift check compares the bare name.
_WIDTH_LESS_TYPES = frozenset(
    {"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR"}
)

# TEXT/BLOB columns are always created nullable (see ColumnSpec.ddl()): MySQL
# grants them no DEFAULT either, so the prototype semantics keep them loose.
# Drift detection must expect exactly what ddl() emits, or every boot re-flags
# tables this very code created (F-36).
_BLOB_TYPES = frozenset({"MEDIUMTEXT", "MEDIUMBLOB"})


class SchemaError(ValueError):
    pass


def check_identifier(name: str) -> str:
    if not _IDENT_RE.fullmatch(name):
        raise SchemaError(f"invalid SQL identifier: {name!r}")
    return name


_NUM_LITERAL_RE = re.compile(r"^-?\d+(\.\d+)?$")
# An already-valid SQL string literal: quotes inside must be doubled, no backslashes.
_STR_LITERAL_RE = re.compile(r"^'(?:[^'\\]|'')*'$")
_KEYWORD_LITERALS = frozenset({"NULL", "CURRENT_TIMESTAMP"})


def check_default_literal(default: str) -> str:
    """Validate a DEFAULT literal; returns the safe SQL text (F-09).

    Accepts numbers, single-quoted strings (inner quotes doubled, no
    backslashes), NULL and CURRENT_TIMESTAMP.  Anything else -- expressions,
    function calls, injection attempts -- is rejected before it can reach DDL.
    """
    text = default.strip()
    upper = text.upper()
    if upper in _KEYWORD_LITERALS:
        return upper
    if _NUM_LITERAL_RE.fullmatch(text):
        return text
    if _STR_LITERAL_RE.fullmatch(text):
        return text  # already a well-formed literal; pass through unchanged
    raise SchemaError(
        f"invalid DEFAULT literal: {default!r} (allowed: number, "
        "'quoted string', NULL, CURRENT_TIMESTAMP)"
    )


def check_comment(comment: str) -> str:
    """Validate a COMMENT payload; returns the escaped SQL text (F-37).

    Single quotes are doubled on emit, but a backslash cannot be neutralized
    the same way -- a trailing ``\\`` escapes the closing quote and shifts
    the rest of the DDL -- so backslashes are rejected outright, mirroring
    the backslash ban in :func:`check_default_literal`.
    """
    if "\\" in comment:
        raise SchemaError(f"invalid COMMENT text: {comment!r} (backslashes are not allowed)")
    return comment.replace(chr(39), chr(39) * 2)


@dataclass(slots=True)
class ColumnSpec:
    name: str
    data_type: str  # e.g. VARCHAR, BIGINT, MEDIUMBLOB
    length: str | None = None
    primary: bool = False
    unique: bool = False
    nullable: bool = True
    default: str | None = None
    comment: str = ""

    def expects_not_null(self) -> bool:
        """Whether generated DDL emits NOT NULL for this column.

        Drift detection compares against what ddl() actually creates, not the
        raw config flag: TEXT/BLOB columns stay nullable regardless of
        ``not_null`` (F-36).
        """
        return not self.nullable and self.data_type not in _BLOB_TYPES

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
            part += f" DEFAULT {check_default_literal(self.default)}"
        if self.expects_not_null():
            part += " NOT NULL"
        if self.comment:
            part += f" COMMENT '{check_comment(self.comment)}'"
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
    # ODKU syntax selector, set by SchemaManager after probing the server
    # version: MySQL >= 8.0.19 deprecates ``VALUES(col)`` in ON DUPLICATE KEY
    # UPDATE (noisy deprecation warnings on every save); the row-alias form
    # ``AS _new ... _new.col`` is the replacement. False (legacy VALUES) keeps
    # 5.7 / <8.0.19 servers working.
    odku_alias: bool = False

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
                unique=col_def.primary or col_def.unique,
                nullable=not (col_def.primary or col_def.not_null),
                default=col_def.default,
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
        # Through check_comment like the column-level path (F-37): the old
        # table-level escaping doubled quotes but did not reject backslashes,
        # so a trailing ``\`` shifted the rest of the CREATE TABLE DDL.
        comment = check_comment(self.comment) if self.comment else ""
        return f"CREATE TABLE `{self.name}` (\n  {parts}\n) COMMENT '{comment}'"

    def query_sql(self, column: str) -> str:
        check_identifier(self.name)
        check_identifier(column)
        pk = self.primary_column().name
        return f"SELECT `{column}` FROM `{self.name}` WHERE `{pk}` = %s"

    def _odku_update(self, column: str) -> str:
        if self.odku_alias:
            return f"ON DUPLICATE KEY UPDATE `{column}` = `_new`.`{column}`"
        return f"ON DUPLICATE KEY UPDATE `{column}` = VALUES(`{column}`)"

    def _row_alias(self) -> str:
        # MySQL 8.0.19+: the row alias must ride the VALUES clause for the
        # ODKU assignment to reference it.
        return "AS `_new` " if self.odku_alias else ""

    def upsert_sql(self, column: str) -> str:
        check_identifier(self.name)
        check_identifier(column)
        return (
            f"INSERT INTO `{self.name}` (`{self.primary_column().name}`, `{column}`) "
            f"VALUES (%s, %s) {self._row_alias()}{self._odku_update(column)}"
        )

    def upsert_many_sql(self, column: str, rows: int) -> str:
        """Multi-row form of :meth:`upsert_sql` (F-42): one round-trip for a
        whole dirty batch instead of one per saver."""
        check_identifier(self.name)
        check_identifier(column)
        if rows < 1:
            raise ValueError("rows must be >= 1")
        values = ", ".join(["(%s, %s)"] * rows)
        return (
            f"INSERT INTO `{self.name}` (`{self.primary_column().name}`, `{column}`) "
            f"VALUES {values} {self._row_alias()}{self._odku_update(column)}"
        )

    def insert_row_sql(self) -> str:
        check_identifier(self.name)
        pk = self.primary_column().name
        return f"INSERT IGNORE INTO `{self.name}` (`{pk}`) VALUES (%s)"

    def delete_sql(self) -> str:
        check_identifier(self.name)
        pk = self.primary_column().name
        return f"DELETE FROM `{self.name}` WHERE `{pk}` = %s"


def _parse_type(type_str: str) -> tuple[str, str | None]:
    """F-207: parse a tables-config column type into ``(name, length)``.

    Accepts the forms MySQL itself spells: ``INT UNSIGNED`` (attribute
    words after the base name), ``DECIMAL(10,2)`` (multi-argument widths),
    and ENUM/SET value lists ``ENUM('a','b')``. The parenthesised payload is
    strictly validated -- digits and commas, or single-quoted values -- so
    no arbitrary text can ride a "length" into generated DDL."""
    match = re.fullmatch(r"([A-Za-z]+(?:\s+[A-Za-z]+)*)\s*(?:\(([^)]*)\))?", type_str.strip())
    if not match:
        raise SchemaError(f"unparseable column type: {type_str!r}")
    name = match.group(1).upper()
    raw_len = match.group(2)
    length: str | None = None
    if raw_len:
        if re.fullmatch(r"\d+(?:\s*,\s*\d+)*", raw_len):
            length = re.sub(r"\s+", "", raw_len)  # "10, 2" -> "10,2"
        elif re.fullmatch(r"'[^']*'(?:\s*,\s*'[^']*')*", raw_len):
            length = raw_len  # ENUM/SET values: keep verbatim
        else:
            raise SchemaError(
                f"column type {type_str!r}: the parenthesised part must be "
                "numeric widths or single-quoted values"
            )
    return name, length


def _split_statements(sql_text: str) -> list[str]:
    """Split a migration file into statements on ``;``.

    F-66 kept ``--`` comment halves from leaking into the next statement; this
    state-machine pass additionally honours ``'...'`` string literals (with the
    ``''`` doubling and backslash escape of MySQL's default sql_mode) and
    ``/* ... */`` block comments, so a semicolon inside a DEFAULT or a seeded
    value no longer cuts the statement in two. Values in migrations remain
    discouraged -- this just stops them from silently corrupting the file.
    """
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    i = 0
    n = len(sql_text)
    while i < n:
        ch = sql_text[i]
        if in_string:
            current.append(ch)
            if ch == "\\" and i + 1 < n:  # default sql_mode: escaped char
                current.append(sql_text[i + 1])
                i += 2
                continue
            if ch == "'":
                if sql_text.startswith("''", i):  # doubled quote stays literal
                    current.append("'")
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            current.append(ch)
            i += 1
            continue
        # MySQL only opens a ``--`` comment when whitespace (or end-of-line)
        # follows the dashes; that exact shape is skipped (F-66).
        if (
            ch == "-"
            and sql_text.startswith("--", i)
            and (i + 2 >= n or sql_text[i + 2] in " \t\r\n")
        ):
            newline = sql_text.find("\n", i)
            i = n if newline == -1 else newline
            continue
        if ch == "#":
            # MySQL's other line-comment opener -- unlike ``--`` it needs no
            # following whitespace, and a ``;`` inside it must not split the
            # statement.
            newline = sql_text.find("\n", i)
            i = n if newline == -1 else newline
            continue
        if ch == "/" and sql_text.startswith("/*", i):
            end = sql_text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        if ch == ";":
            statement = _tidy_statement("".join(current))
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    tail = _tidy_statement("".join(current))
    if tail:
        statements.append(tail)
    return statements


def _tidy_statement(statement: str) -> str:
    return "\n".join(line for line in statement.splitlines() if line.strip()).strip()


def _parse_server_version(version: str) -> tuple[int, int, int]:
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
    if match is None:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


class TableProvider(Protocol):
    """What :class:`~pyline.db.orm.DataSaver` needs from a schema source:
    spec lookup by table name. ``SchemaManager`` satisfies this structurally;
    so does the pool-less :class:`TableCatalog`."""

    def table(self, name: str) -> TableSpec: ...


class TableCatalog:
    """Config-only :class:`TableSpec` registry (F-156).

    A business process in a ``sub_process`` topology owns no MySQL pool, so
    ``SchemaManager`` (whose ``ensure_all`` runs DDL) was never constructed
    there and ``api.orm.make_saver`` raised ``ApiServiceUnavailableError`` in
    exactly the processes where business code runs. The catalog parses the
    same ``tables.json5`` the DB process manages DDL against, which is all a
    saver needs to validate ``(table, column)`` and generate SQL; every
    statement still executes through ``DatabaseAccess`` -> RPC -> the DB
    process.

    F-211: ``odku_alias`` mirrors ``mysql.odku_row_alias`` from the shared
    config so remote upserts can use the same row-alias ODKU syntax the DB
    process selected by version probe -- set it true on MySQL >= 8.0.19 and
    the deprecation-warning noise (and eventual VALUES() removal risk) goes
    away on BOTH write paths. Default False keeps the legacy form working
    everywhere.
    """

    def __init__(self, tables: dict[str, TableDef], *, odku_alias: bool = False) -> None:
        self._tables = {name: TableSpec.from_def(name, tdef) for name, tdef in tables.items()}
        if odku_alias:
            for spec in self._tables.values():
                spec.odku_alias = True

    def table(self, name: str) -> TableSpec:
        try:
            return self._tables[name]
        except KeyError:
            raise SchemaError(f"table {name!r} not defined in tables config") from None


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
        """Create/evolve the schema under a cross-process lock (F-67).

        Two owns_db processes booting at the same time used to double-run the
        migration loop and race the version table's SELECT-then-INSERT into
        two rows; the whole cycle now runs while holding a MySQL named lock,
        and a peer that cannot take it within the bounded wait fails startup
        loudly instead of migrating unsynchronized.
        """
        async with self._cross_process_lock():
            await self._ensure_database()
            await self._select_odku_syntax()
            await self._ensure_version_table()
            await self._ensure_tables()
            await self._apply_migrations()

    async def _select_odku_syntax(self) -> None:
        """Pick the ODKU syntax per server version (see TableSpec.odku_alias).

        A probe failure keeps the legacy ``VALUES()`` form: it still executes
        on every supported server (deprecated, not removed), so an
        information-gathering hiccup must not block boot.
        """
        try:
            rows = await self._pool.query("SELECT VERSION()")
            version = str(rows[0][0]) if rows else ""
        except Exception:
            logger.warning(
                "could not probe mysql version; upserts keep the legacy VALUES() ODKU syntax",
                exc_info=True,
            )
            return
        use_alias = _parse_server_version(version) >= (8, 0, 19)
        if use_alias:
            for spec in self._tables.values():
                spec.odku_alias = True
        logger.info(
            "upsert ODKU syntax: %s (server version %s)",
            "row alias" if use_alias else "legacy VALUES()",
            version,
        )

    @contextlib.asynccontextmanager
    async def _cross_process_lock(self) -> AsyncIterator[None]:
        # The lock name is capped at MySQL's 64-char limit; db_name itself is
        # identifier-validated in __init__ so the prefix cannot be injected.
        lock_name = (_MIGRATION_LOCK_PREFIX + self._db_name)[:64]
        session = await self._pool.open_session()
        try:
            rows = await session.query(
                "SELECT GET_LOCK(%s, %s)", (lock_name, _MIGRATION_LOCK_WAIT_SECONDS)
            )
            raw = rows[0][0] if rows else 0
            got = int(raw) if raw is not None else 0  # NULL: server-side error
            if got != 1:
                # 0 = still held by a peer after the wait; NULL = server error
                raise SchemaError(
                    f"could not acquire the schema migration lock {lock_name!r} "
                    f"within {_MIGRATION_LOCK_WAIT_SECONDS}s "
                    "(another process is migrating, or the server errored)"
                )
            try:
                yield
            finally:
                # Best-effort: if RELEASE_LOCK itself fails, the lock dies with
                # the closing connection anyway -- but say so in the log.
                with contextlib.suppress(Exception):
                    rows = await session.query("SELECT RELEASE_LOCK(%s)", (lock_name,))
                if not rows or rows[0][0] != 1:
                    logger.warning("RELEASE_LOCK(%r) did not confirm release", lock_name)
        finally:
            await session.close()

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
            statement = f"ALTER TABLE `{name}` ADD COLUMN {col.ddl()}, ALGORITHM=INSTANT, LOCK=NONE"
            logger.info("adding column %s.%s", name, col.name)
            await self._pool.execute(statement)
            self.alter_statements.append(statement)

    async def _check_drift(self, name: str, spec: TableSpec) -> None:
        """Declared config vs live columns: mismatch fails startup (F-06).

        F-207: the PRIMARY KEY set is compared too -- a table recreated (or
        hand-migrated) with a different key silently changed upsert/delete
        semantics while type and nullability still matched."""
        rows = await self._pool.query(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (self._db_name, name),
        )
        actual = {row[0]: (row[1], row[2], row[3]) for row in rows}
        problems: list[str] = []
        for col in spec.columns.values():
            if col.name not in actual:
                continue  # missing columns are handled additively above
            live_type, live_nullable, live_key = actual[col.name]
            if live_type.lower() != col.column_type():
                problems.append(
                    f"{name}.{col.name}: type drift, declared {col.column_type()}, live {live_type}"
                )
            expected_nullable = "NO" if col.expects_not_null() else "YES"
            if live_nullable != expected_nullable:
                problems.append(
                    f"{name}.{col.name}: nullability drift, declared "
                    f"{expected_nullable}, live {live_nullable}"
                )
            if col.primary and live_key != "PRI":
                problems.append(
                    f"{name}.{col.name}: declared PRIMARY KEY but the live column "
                    f"has COLUMN_KEY={live_key!r} (upsert/delete semantics changed)"
                )
        live_primaries = {col_name for col_name, (_, _, key) in actual.items() if key == "PRI"}
        declared_primaries = {col.name for col in spec.columns.values() if col.primary}
        unexpected_primaries = (
            live_primaries - declared_primaries - (set(actual) - set(spec.columns))
        )
        if unexpected_primaries:
            problems.append(
                f"{name}: live PRIMARY KEY columns {sorted(unexpected_primaries)} are not "
                "declared primary in the config"
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
        """Apply pending migrations, resuming a partially-failed one (F-51).

        MySQL DDL implicitly commits, so a multi-statement migration cannot be
        atomic. Progress is recorded per statement: a restart resumes from the
        failed statement instead of re-running the whole file (which used to
        rely entirely on the "every script is idempotent" convention). The
        failing statement itself is still re-executed -- keep statements
        idempotent; the blast radius is now one statement, not one file.
        """
        applied = await self.current_version()
        pending = [(n, p) for n, p in self._load_migrations() if n > applied]
        if not pending:
            return
        await self._pool.execute(
            f"CREATE TABLE IF NOT EXISTS `{PROGRESS_TABLE}` (\n"
            "  `migration` INT NOT NULL PRIMARY KEY,\n"
            "  `statements` INT NOT NULL\n"
            ") COMMENT 'pyline migration resume progress (F-51)'"
        )
        for number, path in pending:
            statements = _split_statements(path.read_text(encoding="utf-8"))
            done = await self._migration_progress(number)
            if done > len(statements):
                raise SchemaError(
                    f"migration {path.name} has {len(statements)} statements but "
                    f"{done} were recorded as applied on a previous attempt; the "
                    "file changed in between -- resolve manually"
                )
            if done:
                logger.warning(
                    "resuming migration %s at statement %d/%d (an earlier attempt failed)",
                    path.name,
                    done + 1,
                    len(statements),
                )
            try:
                for index in range(done, len(statements)):
                    statement = statements[index]
                    logger.info("applying migration %s: %s", path.name, statement.splitlines()[0])
                    await self._pool.execute(statement)
                    await self._pool.execute(
                        f"INSERT INTO `{PROGRESS_TABLE}` (`migration`, `statements`) "
                        f"VALUES (%s, %s) ON DUPLICATE KEY UPDATE `statements` = %s",
                        (number, index + 1, index + 1),
                    )
            except Exception:
                get_metrics().schema_migrations.labels(result="failed").inc()
                raise
            await self._pool.execute(
                f"UPDATE `{VERSION_TABLE}` SET `version` = %s WHERE `version` < %s",
                (number, number),
            )
            await self._pool.execute(
                f"DELETE FROM `{PROGRESS_TABLE}` WHERE `migration` = %s", (number,)
            )
            get_metrics().schema_migrations.labels(result="applied").inc()
            logger.info("applied migration %s (version %d)", path.name, number)

    async def _migration_progress(self, number: int) -> int:
        rows = await self._pool.query(
            f"SELECT `statements` FROM `{PROGRESS_TABLE}` WHERE `migration` = %s", (number,)
        )
        return int(rows[0][0]) if rows else 0

    def table(self, name: str) -> TableSpec:
        try:
            return self._tables[name]
        except KeyError:
            raise SchemaError(f"table {name!r} not defined in tables config") from None

    async def row_exists(self, table: str, key: Any) -> bool:
        spec = self.table(table)  # validates: only registered names reach SQL
        pk = spec.primary_column().name
        rows = await self._pool.query(
            f"SELECT 1 FROM `{spec.name}` WHERE `{pk}` = %s LIMIT 1", (key,)
        )
        return bool(rows)
