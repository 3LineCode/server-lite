"""ORM: lazy key->blob savers with dirty tracking and explicit codecs.

The prototype's bytecode-magic attribute observation (co_names scraping,
ObsDict/ObsList wrappers) is replaced by a small explicit protocol:

* :class:`TrackableModel` -- dataclass mixin; plain attribute assignment
  marks the bound saver dirty automatically;
* mutating a *container* attribute in place requires an explicit
  ``model.touch()`` (one call, spelled out, instead of invisible magic);
* :class:`DataSaver` owns one ``(table, column, key)`` blob: lazy load,
  dirty flag, upsert-flush, delete, load hooks.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict, fields
from enum import Enum
from typing import Any, Protocol

from pyline.db.schema import SchemaManager
from pyline.db.serialization import dumps, loads

logger = logging.getLogger(__name__)


class SaveState(Enum):
    NEW = "new"
    LOADING = "loading"
    LOADED = "loaded"
    MISSING = "missing"  # loaded, but no row existed yet
    DELETED = "deleted"


class QueryExecutor(Protocol):
    async def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]: ...

    async def execute(self, sql: str, args: tuple[Any, ...] = ()) -> int: ...


class Codec(Protocol):
    def encode(self, data: Any) -> bytes: ...

    def decode(self, blob: bytes | None) -> Any: ...


class MsgpackCodec:
    """Default codec: versioned msgpack blobs."""

    def __init__(self, *, schema_version: int = 1) -> None:
        self.schema_version = schema_version

    def encode(self, data: Any) -> bytes:
        return dumps(data, schema_version=self.schema_version)

    def decode(self, blob: bytes | None) -> Any:
        return loads(blob)


class DataclassCodec:
    """Codec for dataclass models via ``to_dict``/``from_dict`` adapters."""

    def __init__(
        self,
        to_dict: Callable[[Any], dict[str, Any]],
        from_dict: Callable[[dict[str, Any]], Any],
        *,
        schema_version: int = 1,
    ) -> None:
        self._to_dict = to_dict
        self._from_dict = from_dict
        self._inner = MsgpackCodec(schema_version=schema_version)

    def encode(self, data: Any) -> bytes:
        return self._inner.encode(self._to_dict(data))

    def decode(self, blob: bytes | None) -> Any:
        raw = self._inner.decode(blob)
        return None if raw is None else self._from_dict(raw)


def dataclass_codec(model_cls: type, *, schema_version: int = 1) -> DataclassCodec:
    """Build a codec for a dataclass model using asdict/kwargs reconstruction."""

    def to_dict(instance: Any) -> dict[str, Any]:
        return asdict(instance)

    def from_dict(data: dict[str, Any]) -> Any:
        known = {f.name for f in fields(model_cls)}
        return model_cls(**{k: v for k, v in data.items() if k in known})

    return DataclassCodec(to_dict, from_dict, schema_version=schema_version)


class DataSaver:
    """One persisted blob at ``(table[column], key)``."""

    def __init__(
        self,
        db: QueryExecutor,
        schema: SchemaManager,
        table: str,
        column: str,
        key: Any,
        codec: Codec | None = None,
        *,
        auto_save: bool = True,
        scheduler: SaveSchedulerLike | None = None,
    ) -> None:
        self._db = db
        self._spec = schema.table(table)
        if column not in self._spec.columns:
            raise KeyError(f"table {table!r} has no column {column!r}")
        self._column = column
        self.key = key
        self._codec = codec or MsgpackCodec()
        self._auto_save = auto_save
        self.state = SaveState.NEW
        self._data: Any = None
        self._load_hooks: list[Callable[[DataSaver], None]] = []
        self._scheduler = scheduler

    # ------------------------------ data ------------------------------- #

    @property
    def data(self) -> Any:
        return self._data

    def set_data(self, data: Any) -> None:
        self._data = data
        self.state = SaveState.LOADED
        self.mark_dirty()

    async def load(self, *, force: bool = False) -> Any:
        if self.state == SaveState.DELETED:
            return None
        if self.state in (SaveState.LOADED, SaveState.MISSING) and not force:
            return self._data
        if self.state == SaveState.LOADING:
            return self._data
        self.state = SaveState.LOADING
        rows = await self._db.query(self._spec.query_sql(self._column), (self.key,))
        if rows and rows[0][0] is not None:
            self._data = self._codec.decode(rows[0][0])
            self.state = SaveState.LOADED
        else:
            self._data = None
            self.state = SaveState.MISSING
        for hook in self._load_hooks:
            try:
                hook(self)
            except Exception:
                logger.exception("load hook failed for %s[%s]", self._column, self.key)
        self._load_hooks.clear()
        return self._data

    def add_load_hook(self, hook: Callable[[DataSaver], None]) -> None:
        if self.state in (SaveState.LOADED, SaveState.MISSING):
            hook(self)
            return
        self._load_hooks.append(hook)

    # ------------------------------ dirty ------------------------------ #

    def bind_scheduler(self, scheduler: SaveSchedulerLike) -> None:
        self._scheduler = scheduler

    def mark_dirty(self) -> None:
        if self.state == SaveState.DELETED:
            raise OSError(f"saver {self._column}[{self.key!r}] marked dirty after delete")
        if self.state not in (SaveState.LOADED, SaveState.MISSING) and self._data is None:
            raise OSError(
                f"saver {self._column}[{self.key!r}] marked dirty before load; "
                "call load() or set_data() first"
            )
        self.state = SaveState.LOADED
        if self._auto_save and self._scheduler is not None:
            self._scheduler.mark(self)

    async def flush(self) -> None:
        """Encode and upsert immediately."""
        if self.state == SaveState.DELETED:
            return
        blob = self._codec.encode(self._data)
        await self._db.execute(self._spec.upsert_sql(self._column), (self.key, blob))

    async def delete(self) -> None:
        self.state = SaveState.DELETED
        await self._db.execute(self._spec.delete_sql(), (self.key,))

    def __repr__(self) -> str:
        return f"DataSaver({self._spec.name}.{self._column}[{self.key!r}], {self.state.value})"


class SaveSchedulerLike(Protocol):
    def mark(self, saver: DataSaver) -> None: ...


class TrackableModel:
    """Mixin for dataclass models: attribute writes dirty the bound saver.

    In-place container mutation is not observable without wrapping magic --
    call ``self.touch()`` after mutating lists/dicts.
    """

    _INTERNAL_ATTRS = frozenset({"_saver", "_dirty", "_INTERNAL_ATTRS", "__dict__", "__weakref__"})

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    _saver: DataSaver | None = None
    _dirty: bool = False

    @property
    def saver(self) -> DataSaver | None:
        return self._saver

    def bind_saver(self, saver: DataSaver) -> None:
        object.__setattr__(self, "_saver", saver)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name in self._INTERNAL_ATTRS or name.startswith("__"):
            return
        self.touch()

    def touch(self) -> None:
        """Mark dirty explicitly (required after in-place container mutation)."""
        object.__setattr__(self, "_dirty", True)
        saver = getattr(self, "_saver", None)
        if saver is not None:
            saver.mark_dirty()
