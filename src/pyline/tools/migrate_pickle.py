"""One-shot migration: legacy pickle blobs -> pyline msgpack blobs (F-08).

The prototype stored ORM blob columns as raw ``pickle``; pyline stores
``PLD1 | version | msgpack``.  This tool walks every blob column declared in
``tables.json5``, finds rows that do not start with the pyline magic, and
re-encodes them.

Safety model:

* **dry-run by default** -- pass ``--execute`` to write;
* unpickling is restricted to plain data types (dict/list/tuple/set/
  frozenset/str/bytes/int/float/bool/None): any pickle referencing a global
  (class, function) is reported as failed and left untouched, per the
  official pickle security guidance -- pickle must never cross a trust
  boundary, and a migration scan counts as one;
* every converted value is verified by a round-trip before the UPDATE;
* tuples/sets normalize to lists (msgpack has neither) -- this is the
  documented normalization pyline decoding applies everywhere.

Usage::

    python -m pyline.tools.migrate_pickle --config aioconfig           # dry-run
    python -m pyline.tools.migrate_pickle --config aioconfig --execute
"""

from __future__ import annotations

import argparse
import io
import logging
import pickle
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyline.config.loader import load_project_settings, load_table_defs
from pyline.db.mysql import MySQLPool
from pyline.db.schema import check_identifier
from pyline.db.serialization import BLOB_MAGIC, dumps, loads

logger = logging.getLogger(__name__)


class _RestrictedUnpickler(pickle.Unpickler):
    """Reject every global reference: only plain data may be loaded."""

    def find_class(self, module: str, name: str) -> Any:
        raise pickle.UnpicklingError(f"global {module}.{name} is forbidden")


def restricted_loads(data: bytes) -> Any:
    return _RestrictedUnpickler(io.BytesIO(data)).load()


def _normalize(value: Any) -> Any:
    """Canonical form comparable across pickle and msgpack round-trips."""
    if isinstance(value, (tuple, set, frozenset)):
        return [_normalize(v) for v in value]
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, dict):
        return {str(_normalize(k)): _normalize(v) for k, v in value.items()}
    return value


@dataclass
class ColumnReport:
    table: str
    column: str
    scanned: int = 0
    skipped_modern: int = 0
    converted: int = 0
    failed: int = 0
    failures: list[str] = field(default_factory=list)

    def line(self) -> str:
        return (
            f"  {self.table}.{self.column}: scanned={self.scanned} "
            f"converted={self.converted} skipped(already pyline)={self.skipped_modern} "
            f"failed={self.failed}"
        )


@dataclass
class MigrationReport:
    execute: bool
    columns: list[ColumnReport] = field(default_factory=list)

    @property
    def total_converted(self) -> int:
        return sum(c.converted for c in self.columns)

    @property
    def total_failed(self) -> int:
        return sum(c.failed for c in self.columns)

    def summary(self) -> str:
        mode = "EXECUTE" if self.execute else "DRY-RUN (nothing written; pass --execute)"
        lines = [f"pickle -> msgpack migration ({mode}):"]
        lines.extend(c.line() for c in self.columns)
        lines.append(f"total: converted={self.total_converted} failed={self.total_failed}")
        for col in self.columns:
            for failure in col.failures:
                lines.append(f"  FAILED {col.table}.{col.column} key={failure}")
        return "\n".join(lines)


def _blob_columns(tables: dict[str, Any]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for table_name, table_def in tables.items():
        for col_name, col_def in table_def.fields.items():
            if "BLOB" in col_def.type.upper():
                out.append((table_name, col_name))
    return out


async def migrate_blobs(
    pool: MySQLPool,
    tables: dict[str, Any],
    *,
    execute: bool = False,
    batch_size: int = 1000,
) -> MigrationReport:
    """Walk every blob column and convert legacy pickle rows.

    Rows stream in ``batch_size`` keyset pages (``WHERE pk > last ORDER BY pk``)
    -- the original unbounded ``SELECT`` materialized the whole table in
    memory at once, fine for prototype-scale tables but not for millions of
    rows.
    """
    report = MigrationReport(execute=execute)
    for table, column in _blob_columns(tables):
        # F-103: these names travel into f-string SQL below -- validate them
        # the same way the schema layer does instead of trusting the config.
        check_identifier(table)
        check_identifier(column)
        col_report = ColumnReport(table=table, column=column)
        report.columns.append(col_report)
        spec_pk = _primary_key(tables, table)
        last_key: Any = None
        while True:
            if last_key is None:
                rows = await pool.query(
                    f"SELECT `{spec_pk}`, `{column}` FROM `{table}` "
                    f"WHERE `{column}` IS NOT NULL ORDER BY `{spec_pk}` LIMIT %s",
                    (batch_size,),
                )
            else:
                rows = await pool.query(
                    f"SELECT `{spec_pk}`, `{column}` FROM `{table}` "
                    f"WHERE `{column}` IS NOT NULL AND `{spec_pk}` > %s "
                    f"ORDER BY `{spec_pk}` LIMIT %s",
                    (last_key, batch_size),
                )
            if not rows:
                break
            for key, blob in rows:
                col_report.scanned += 1
                if not isinstance(blob, (bytes, bytearray)):
                    col_report.failed += 1
                    col_report.failures.append(f"{key!r} (not binary: {type(blob).__name__})")
                    continue
                if bytes(blob).startswith(BLOB_MAGIC):
                    col_report.skipped_modern += 1
                    continue
                try:
                    data = restricted_loads(bytes(blob))
                    # No explicit schema_version: the tool writes whatever the
                    # CURRENT codec default is, so a future v2 codec migrates
                    # old rows straight to the version the game reads.
                    converted = dumps(data)
                    # F-65: normalize BOTH sides.  msgpack preserves int/bytes
                    # dict keys exactly like pickle does; comparing the raw loads
                    # result against the str()-keyed normalization reported every
                    # non-string-keyed row as a round-trip mismatch, so it was
                    # left as pickle and unreadable (bad magic) at runtime.
                    if _normalize(loads(converted)) != _normalize(data):
                        raise ValueError("round-trip mismatch")
                except Exception as exc:
                    col_report.failed += 1
                    col_report.failures.append(f"{key!r} ({exc})")
                    continue
                col_report.converted += 1
                if execute:
                    await pool.execute(
                        f"UPDATE `{table}` SET `{column}` = %s WHERE `{spec_pk}` = %s",
                        (converted, key),
                    )
            last_key = rows[-1][0]
            if len(rows) < batch_size:
                break
    return report


def _primary_key(tables: dict[str, Any], table: str) -> str:
    for col_name, col_def in tables[table].fields.items():
        if col_def.primary:
            return check_identifier(str(col_name))
    raise SystemExit(f"table {table!r} has no primary key column in tables config")


def main(argv: Sequence[str] | None = None) -> int:
    import asyncio

    parser = argparse.ArgumentParser(
        prog="pyline.tools.migrate_pickle",
        description="Convert legacy pickle blob columns to pyline msgpack blobs",
    )
    parser.add_argument("--config", default="aioconfig", help="config directory")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="write conversions (default is a dry run that changes nothing)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="rows fetched per page (keyset pagination; the scan never "
        "materializes the whole table)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config_dir = Path(args.config)
    settings = load_project_settings(config_dir)
    tables = load_table_defs(config_dir)

    async def run() -> int:
        pool = MySQLPool(settings.mysql)
        try:
            await pool.connect()
            report = await migrate_blobs(
                pool, tables, execute=args.execute, batch_size=args.batch_size
            )
        finally:
            await pool.close()
        print(report.summary())
        return 1 if report.total_failed else 0

    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
