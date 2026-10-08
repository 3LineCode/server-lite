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


def test_truncated_blob_raises_format_error() -> None:
    """F-35: a truncated payload used to escape as a raw msgpack OutOfData
    instead of the uniform BlobFormatError every caller expects."""
    blob = dumps({"padding": "x" * 64})
    with pytest.raises(BlobFormatError):
        loads(blob[:-7])
    with pytest.raises(BlobFormatError):
        loads(blob[:6])  # cut at the header/payload boundary


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


# ------------------------- F-07: codec wiring ------------------------- #


def test_decode_rejects_newer_version() -> None:
    """Code older than data must fail fast, not silently misread."""
    from pyline.db.orm import MsgpackCodec

    blob = dumps({"x": 1}, schema_version=2)
    codec = MsgpackCodec(schema_version=1)
    with pytest.raises(Exception, match="newer") as excinfo:
        codec.decode(blob)
    from pyline.db.serialization import BlobVersionError

    assert isinstance(excinfo.value, BlobVersionError)


def test_codec_applies_migration_chain() -> None:
    from pyline.db.orm import MsgpackCodec

    codec = MsgpackCodec(
        schema_version=2,
        migrations={1: lambda d: {"name": d["name"], "level": d.get("lvl", 1)}},
    )
    old_blob = dumps({"name": "bob", "lvl": 9}, schema_version=1)
    assert codec.decode(old_blob) == {"name": "bob", "level": 9}
    fresh_blob = dumps({"name": "amy", "level": 2}, schema_version=2)
    assert codec.decode(fresh_blob) == {"name": "amy", "level": 2}


def test_codec_old_blob_without_chain_rejected() -> None:
    from pyline.db.orm import MsgpackCodec

    codec = MsgpackCodec(schema_version=2)
    with pytest.raises(BlobFormatError, match="no migration chain"):
        codec.decode(dumps({"x": 1}, schema_version=1))


def test_dataclass_codec_migration_on_dict_form() -> None:
    from dataclasses import dataclass

    from pyline.db.orm import dataclass_codec

    @dataclass
    class Model:
        name: str = ""
        level: int = 1

    codec = dataclass_codec(
        Model,
        schema_version=2,
        migrations={1: lambda d: {"name": d["name"], "level": d.get("lvl", 1)}},
    )
    blob = dumps({"name": "zoe", "lvl": 7}, schema_version=1)
    loaded = codec.decode(blob)
    assert isinstance(loaded, Model)
    assert loaded.name == "zoe" and loaded.level == 7
