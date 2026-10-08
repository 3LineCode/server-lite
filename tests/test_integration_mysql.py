"""Integration tests against a live MySQL server (CI service or local dev).

Env overrides: PYLINE_TEST_MYSQL_HOST / _PORT / _USER / _PASSWORD
(defaults match the CI mysql:8.4 service: 127.0.0.1:3306 root/test).

Skipped automatically when no server is reachable, so the default local
`uv run pytest` stays green without a database.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import asyncmy
import pytest

from pyline.config.models import MySQLSettings, TableDef, TableFieldDef
from pyline.db.mysql import MySQLPool
from pyline.db.schema import SchemaError, SchemaManager

pytestmark = pytest.mark.mysql


def _mysql_host_port() -> tuple[str, int]:
    return (
        os.environ.get("PYLINE_TEST_MYSQL_HOST", "127.0.0.1"),
        int(os.environ.get("PYLINE_TEST_MYSQL_PORT", "3306")),
    )


def _mysql_settings() -> MySQLSettings:
    host, port = _mysql_host_port()
    return MySQLSettings(
        host=host,
        port=port,
        user=os.environ.get("PYLINE_TEST_MYSQL_USER", "root"),
        password=os.environ.get("PYLINE_TEST_MYSQL_PASSWORD", "test"),
        db_name=f"pyline_it_{uuid.uuid4().hex[:8]}",
    )


def _mysql_reachable() -> bool:
    host, port = _mysql_host_port()
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


requires_mysql = pytest.mark.skipif(not _mysql_reachable(), reason="no reachable MySQL")


@pytest.fixture()
async def mysql_settings() -> MySQLSettings:
    """Skip unless the server actually accepts the test credentials."""
    host, port = _mysql_host_port()
    try:
        conn = await asyncmy.connect(
            host=host,
            port=port,
            user=os.environ.get("PYLINE_TEST_MYSQL_USER", "root"),
            password=os.environ.get("PYLINE_TEST_MYSQL_PASSWORD", "test"),
            connect_timeout=3,
        )
    except Exception as exc:
        pytest.skip(f"MySQL reachable but not usable: {exc}")
    await conn.ensure_closed()
    return _mysql_settings()


async def _drop_database(settings: MySQLSettings) -> None:
    conn = await asyncmy.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        autocommit=True,
    )
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP DATABASE IF EXISTS `{settings.db_name}`")
    finally:
        await conn.ensure_closed()


def _player_tables() -> dict[str, TableDef]:
    return {
        "tbl_player": TableDef(
            comment="players",
            fields={
                "id": TableFieldDef(type="BIGINT", primary=True),
                "data": TableFieldDef(type="MEDIUMBLOB"),
            },
        )
    }


@requires_mysql
class TestFreshBoot:
    async def test_schema_creates_database_first(self, mysql_settings: MySQLSettings) -> None:
        """F-05: a brand-new server boots: bootstrap DB, pool, tables, version row."""
        settings = mysql_settings
        pool = MySQLPool(settings)
        try:
            await pool.connect()  # bootstrap creates the (random-named) database
            manager = SchemaManager(pool, _player_tables(), settings.db_name)
            await manager.ensure_all()
            tables = {row[0] for row in await pool.query("SHOW TABLES")}
            assert {"tbl_player", "pyline_schema"} <= tables
            assert await manager.current_version() == 0
        finally:
            await pool.close()
            await _drop_database(settings)

    async def test_schema_migration_linear_apply(
        self, tmp_path: Path, mysql_settings: MySQLSettings
    ) -> None:
        """F-06: pending scripts apply in order once; the version row advances."""
        settings = mysql_settings
        pool = MySQLPool(settings)
        migrations = tmp_path / "migrations"
        migrations.mkdir()
        (migrations / "001_add_score.sql").write_text(
            "ALTER TABLE `tbl_player` ADD COLUMN `score` BIGINT NULL;\n", encoding="utf-8"
        )
        (migrations / "002_add_rank.sql").write_text(
            "ALTER TABLE `tbl_player` ADD COLUMN `rank` BIGINT NULL;\n", encoding="utf-8"
        )
        try:
            await pool.connect()
            manager = SchemaManager(
                pool, _player_tables(), settings.db_name, migrations_dir=migrations
            )
            await manager.ensure_all()
            assert await manager.current_version() == 2
            await manager.ensure_all()  # second boot: everything skipped
            assert await manager.current_version() == 2
            cols = {
                row[0]
                for row in await pool.query(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'tbl_player'",
                    (settings.db_name,),
                )
            }
            assert {"score", "rank"} <= cols
        finally:
            await pool.close()
            await _drop_database(settings)

    async def test_schema_drift_detected(self, mysql_settings: MySQLSettings) -> None:
        """F-06: a live column whose type contradicts the config fails startup."""
        settings = mysql_settings
        pool = MySQLPool(settings)
        try:
            await pool.connect()
            drifted = {
                "tbl_player": TableDef(
                    fields={
                        "id": TableFieldDef(type="BIGINT", primary=True),
                        # config says VARCHAR(50); the created table has MEDIUMBLOB
                        "data": TableFieldDef(type="VARCHAR(50)"),
                    }
                )
            }
            await SchemaManager(pool, _player_tables(), settings.db_name).ensure_all()
            with pytest.raises(SchemaError, match="type drift"):
                await SchemaManager(pool, drifted, settings.db_name).ensure_all()
        finally:
            await pool.close()
            await _drop_database(settings)


@requires_mysql
class TestTransactionsF43:
    async def test_pool_transaction_commits_and_rolls_back(
        self, mysql_settings: MySQLSettings
    ) -> None:
        """F-43: a dedicated pooled connection runs BEGIN..COMMIT / ROLLBACK;
        a second connection sees nothing until the unit commits."""
        settings = mysql_settings
        pool = MySQLPool(settings)
        try:
            await pool.connect()
            manager = SchemaManager(pool, _player_tables(), settings.db_name)
            await manager.ensure_all()
            async with pool.transaction() as tx:
                await tx.execute("INSERT INTO `tbl_player` (`id`) VALUES (1)")
                # uncommitted write invisible from the autocommit pool path
                rows = await pool.query("SELECT `id` FROM `tbl_player` WHERE `id` = 1")
                assert rows == []
                rows_in_tx = await tx.query("SELECT `id` FROM `tbl_player` WHERE `id` = 1")
                assert rows_in_tx == [(1,)]
            rows = await pool.query("SELECT `id` FROM `tbl_player` WHERE `id` = 1")
            assert rows == [(1,)]

            class Boom(Exception):
                pass

            with pytest.raises(Boom):
                async with pool.transaction() as tx:
                    await tx.execute("DELETE FROM `tbl_player` WHERE `id` = 1")
                    raise Boom()
            rows = await pool.query("SELECT `id` FROM `tbl_player` WHERE `id` = 1")
            assert rows == [(1,)]  # rolled back
        finally:
            await pool.close()
            await _drop_database(settings)

    async def test_open_session_round_trip(self, mysql_settings: MySQLSettings) -> None:
        """F-43: the RPC-session primitive (dedicated out-of-pool connection)
        begins, executes, commits and closes."""
        settings = mysql_settings
        pool = MySQLPool(settings)
        try:
            await pool.connect()
            manager = SchemaManager(pool, _player_tables(), settings.db_name)
            await manager.ensure_all()
            session = await pool.open_session()
            await session.begin()
            await session.execute("INSERT INTO `tbl_player` (`id`) VALUES (2)")
            await session.commit()
            await session.close()
            rows = await pool.query("SELECT `id` FROM `tbl_player` WHERE `id` = 2")
            assert rows == [(2,)]
        finally:
            await pool.close()
            await _drop_database(settings)
