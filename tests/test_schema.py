"""Schema: table specs, DDL generation, identifier safety."""

from __future__ import annotations

import pytest

from pyline.config.models import TableDef, TableFieldDef
from pyline.db.schema import SchemaError, TableSpec, check_identifier


def make_def() -> TableDef:
    return TableDef(
        comment="players",
        fields={
            "id": TableFieldDef(type="BIGINT", primary=True, comment="pk"),
            "data": TableFieldDef(type="MEDIUMBLOB"),
        },
    )


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
