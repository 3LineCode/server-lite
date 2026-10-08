"""Persisted-blob serialization: msgpack with a schema-version header.

Replaces pickle for everything stored in MySQL blobs (prototype issue #10 and
#22). Layout::

    b"PLD1" | schema_version (2 bytes, big-endian) | msgpack payload

Data must be plain msgpack-compatible types (dict/list/str/int/float/bool/
bytes/None); business models define explicit to_dict/from_dict. A migration
chain upgrades old payloads on load.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import msgpack

BLOB_MAGIC = b"PLD1"
HEADER_SIZE = len(BLOB_MAGIC) + 2


class BlobFormatError(ValueError):
    pass


def dumps(data: Any, *, schema_version: int = 1) -> bytes:
    if not 0 < schema_version < 0x10000:
        raise ValueError(f"schema_version {schema_version} out of range")
    try:
        body = cast(bytes, msgpack.packb(data, use_bin_type=True))
    except (TypeError, ValueError) as exc:
        raise BlobFormatError(f"data is not msgpack-serializable: {exc}") from exc
    return BLOB_MAGIC + schema_version.to_bytes(2, "big") + body


def peek_version(blob: bytes) -> int:
    _check_header(blob)
    return int.from_bytes(blob[4:6], "big")


def loads(blob: bytes | None) -> Any:
    """Load without migration; returns ``None`` for NULL columns."""
    if blob is None:
        return None
    _check_header(blob)
    try:
        return msgpack.unpackb(blob[HEADER_SIZE:], raw=False, strict_map_key=False)
    except (ValueError, msgpack.exceptions.ExtraData) as exc:
        raise BlobFormatError(f"corrupt blob: {exc}") from exc


Migration = Callable[[Any], Any]


def loads_migrated(
    blob: bytes | None,
    migrations: dict[int, Migration],
    *,
    latest_version: int,
) -> Any:
    """Load and run the migration chain up to ``latest_version``.

    ``migrations[v]`` upgrades a v-payload to v+1.
    """
    if blob is None:
        return None
    data = loads(blob)
    if data is None:
        return None
    version = peek_version(blob)
    while version < latest_version:
        step = migrations.get(version)
        if step is None:
            raise BlobFormatError(
                f"no migration from schema version {version}; payload cannot be upgraded"
            )
        data = step(data)
        version += 1
    return data


def _check_header(blob: bytes) -> None:
    if len(blob) < HEADER_SIZE or blob[:4] != BLOB_MAGIC:
        raise BlobFormatError("blob is not pyline-format data (bad magic)")
