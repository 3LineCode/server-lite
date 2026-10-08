"""Blob serialization: round-trip, versioning, migration chain."""

from __future__ import annotations

import pytest

from pyline.db.serialization import (
    BlobFormatError,
    dumps,
    loads,
    loads_migrated,
    peek_version,
)


def test_round_trip() -> None:
    data = {"name": "alice", "level": 3, "tags": ["a", "b"], "ok": True}
    blob = dumps(data)
    assert blob[:4] == b"PLD1"
    assert loads(blob) == data


def test_version_header() -> None:
    assert peek_version(dumps({}, schema_version=7)) == 7
    with pytest.raises(ValueError):
        dumps({}, schema_version=70_000)


def test_none_passthrough() -> None:
    assert loads(None) is None


def test_corrupt_blob() -> None:
    with pytest.raises(BlobFormatError):
        loads(b"XXXXgarbage")
    with pytest.raises(BlobFormatError):
        peek_version(b"PLD1")


def test_migration_chain() -> None:
    # v1 payload -> v2 (add field) -> v3 (rename)
    migrations = {
        1: lambda d: {**d, "level": 1},
        2: lambda d: {"name": d["name"], "level": d["level"]},
    }
    blob = dumps({"name": "bob"}, schema_version=1)
    result = loads_migrated(blob, migrations, latest_version=3)
    assert result == {"name": "bob", "level": 1}


def test_missing_migration_rejected() -> None:
    blob = dumps({"x": 1}, schema_version=1)
    with pytest.raises(BlobFormatError, match="no migration"):
        loads_migrated(blob, {}, latest_version=2)
