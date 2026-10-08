"""ORM facade: saver factory + tracked containers (old ColumnSave/RowSave
surface, explicit-declaration edition)."""

from __future__ import annotations

from collections.abc import Callable

from pyline import api
from pyline.db.orm import DataSaver, TrackableModel
from pyline.db.tracked import TrackedDict, TrackedList

__all__ = ["DataSaver", "TrackableModel", "TrackedDict", "TrackedList", "make_saver"]


def make_saver(
    table: str, column: str, key: object, codec: object = None, **kwargs: object
) -> DataSaver:
    """Create a saver bound to this process's schema and save scheduler."""
    factory: Callable[..., DataSaver] = api.service("make_saver")  # type: ignore[assignment]
    return factory(table, column, key, codec, **kwargs)
