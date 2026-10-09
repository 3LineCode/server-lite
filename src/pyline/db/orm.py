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

import asyncio
import logging
from collections.abc import Callable
from dataclasses import asdict, fields
from enum import Enum
from typing import Any, Protocol

from pyline.db.schema import SchemaManager
from pyline.db.serialization import (
    BlobFormatError,
    BlobVersionError,
    Migration,
    dumps,
    loads,
    loads_migrated,
    peek_version,
)
from pyline.db.transaction import TransactionJournal, current_flush_journal

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
    """Default codec: versioned msgpack blobs with a migration chain (F-07).

    Decoding peeks the stored version: equal -> direct load; older -> the
    migration chain upgrades it; newer -> ``BlobVersionError`` (fail fast:
    code older than data must not silently misread it).
    """

    def __init__(
        self,
        *,
        schema_version: int = 1,
        migrations: dict[int, Migration] | None = None,
    ) -> None:
        self.schema_version = schema_version
        self._migrations = migrations

    def encode(self, data: Any) -> bytes:
        return dumps(data, schema_version=self.schema_version)

    def decode(self, blob: bytes | None) -> Any:
        if blob is None:
            return None
        version = peek_version(blob)
        if version > self.schema_version:
            raise BlobVersionError(
                f"blob schema version {version} is newer than this code's "
                f"{self.schema_version}; refusing to load (deploy the newer "
                "code first, or migrate the data down explicitly)"
            )
        if version == self.schema_version:
            return loads(blob)
        if self._migrations is None:
            raise BlobFormatError(
                f"blob schema version {version} is older than {self.schema_version} "
                "and no migration chain is configured"
            )
        return loads_migrated(blob, self._migrations, latest_version=self.schema_version)


class DataclassCodec:
    """Codec for dataclass models via ``to_dict``/``from_dict`` adapters.

    Migrations (if given) run on the dict form before ``from_dict``.
    """

    def __init__(
        self,
        to_dict: Callable[[Any], dict[str, Any]],
        from_dict: Callable[[dict[str, Any]], Any],
        *,
        schema_version: int = 1,
        migrations: dict[int, Migration] | None = None,
    ) -> None:
        self._to_dict = to_dict
        self._from_dict = from_dict
        self._inner = MsgpackCodec(schema_version=schema_version, migrations=migrations)

    def encode(self, data: Any) -> bytes:
        return self._inner.encode(self._to_dict(data))

    def decode(self, blob: bytes | None) -> Any:
        raw = self._inner.decode(blob)
        return None if raw is None else self._from_dict(raw)


def dataclass_codec(
    model_cls: type,
    *,
    schema_version: int = 1,
    migrations: dict[int, Migration] | None = None,
) -> DataclassCodec:
    """Build a codec for a dataclass model using asdict/kwargs reconstruction."""

    def to_dict(instance: Any) -> dict[str, Any]:
        return asdict(instance)

    def from_dict(data: dict[str, Any]) -> Any:
        known = {f.name for f in fields(model_cls)}
        return model_cls(**{k: v for k, v in data.items() if k in known})

    return DataclassCodec(to_dict, from_dict, schema_version=schema_version, migrations=migrations)


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
        # F-03/F-04: in-flight futures make concurrent load()/delete() calls
        # join the running operation instead of racing it.
        self._load_future: asyncio.Future[Any] | None = None
        self._delete_future: asyncio.Future[None] | None = None
        # F-63: the open transaction that owns this saver's next auto-save
        # (set while its dirty marking is deferred into the journal).
        self._pending_journal: TransactionJournal | None = None
        # F-34: flush() and delete() serialize on this lock so an upsert that
        # is already in flight can never land after a DELETE and resurrect
        # the row.  asyncio.Lock no longer binds to a loop at creation
        # (Python 3.10+), so constructing it here is safe even when the
        # saver is built outside any running loop.
        self._flush_lock = asyncio.Lock()

    # ------------------------------ data ------------------------------- #

    @property
    def table(self) -> str:
        return self._spec.name

    @property
    def column(self) -> str:
        return self._column

    @property
    def executor(self) -> QueryExecutor:
        return self._db

    @property
    def data(self) -> Any:
        return self._data

    def set_data(self, data: Any) -> None:
        self._data = data
        self.state = SaveState.LOADED
        self.mark_dirty()

    async def load(self, *, force: bool = False) -> Any:
        """Load the blob, single-flight: concurrent callers join one query.

        Callers that arrive while a load is running await the same future
        (they previously observed a transient ``None`` and could mistake it
        for "no data" and overwrite real data via ``set_data``).
        """
        if self.state == SaveState.DELETED:
            return None
        if self.state in (SaveState.LOADED, SaveState.MISSING) and not force:
            return self._data
        if self._load_future is not None:
            return await asyncio.shield(self._load_future)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._load_future = future
        try:
            await self._load_once(future)
        finally:
            self._load_future = None
        return self._data

    async def _load_once(self, future: asyncio.Future[Any]) -> None:
        self.state = SaveState.LOADING
        try:
            rows = await self._db.query(self._spec.query_sql(self._column), (self.key,))
        except asyncio.CancelledError:
            self.state = SaveState.NEW
            if not future.done():
                # The loading task was cancelled; joiners must not inherit a
                # CancelledError they did not ask for.
                future.set_exception(RuntimeError("load interrupted by cancellation"))
            raise
        except BaseException as exc:
            self.state = SaveState.NEW
            if not future.done():
                future.set_exception(exc)
            raise
        if self.state != SaveState.LOADING:
            # set_data()/delete() landed while the query was in flight: the
            # in-memory state is newer than the row -- keep it.
            if not future.done():
                future.set_result(self._data)
            return
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
        if not future.done():
            future.set_result(self._data)

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
        if not self._auto_save or self._scheduler is None:
            return
        journal = current_flush_journal()
        if journal is not None:
            # F-63: marking dirty inside an open transaction must NOT reach
            # the background queue yet -- the auto-save round runs without
            # this journal in its context and would autocommit the row
            # outside the unit (surviving a later rollback, punching through
            # the unit's atomicity).  The journal re-queues the saver when
            # the unit ends.
            self._pending_journal = journal
            journal.note_deferred(self)
            return
        self._scheduler.mark(self)

    @property
    def held_by_journal(self) -> TransactionJournal | None:
        """F-63: the open transaction that owns this saver's next auto-save,
        if its dirty marking is currently deferred into one."""
        return self._pending_journal

    def requeue_deferred(self) -> None:
        """F-63: the transaction that deferred this saver has ended (committed
        or rolled back); the in-memory data is what must be persisted next,
        outside any unit.  The hold is cleared first so this cannot be
        re-deferred into the dying journal's context."""
        self._pending_journal = None
        if self.state == SaveState.DELETED:
            return
        if self._auto_save and self._scheduler is not None:
            self._scheduler.mark(self)

    def _require_loaded(self, op: str) -> None:
        """F-102: flushing a never-loaded saver would encode ``None`` into a
        perfectly valid blob and overwrite the real row with it; refuse."""
        if self.state not in (SaveState.LOADED, SaveState.MISSING):
            raise OSError(
                f"saver {self._column}[{self.key!r}] {op} before load; "
                "call load() or set_data() first"
            )

    async def flush(self) -> None:
        """Encode and upsert immediately.

        Serialized against delete() by the flush lock (F-34): the state is
        re-checked *after* acquiring it, so a delete that won the lock makes
        the in-waiting flush a no-op instead of resurrecting the row.
        """
        async with self._flush_lock:
            if self.state == SaveState.DELETED:
                return
            self._require_loaded("flushed")
            blob = self._codec.encode(self._data)
            await self._db.execute(self._spec.upsert_sql(self._column), (self.key, blob))
            # F-50: if this upsert joined an ambient transaction, register it
            # so a rollback re-marks the saver dirty (the DB keeps the old
            # row while memory holds the new data).
            journal = current_flush_journal()
            if journal is not None:
                journal.note_flush(self)
                self._pending_journal = None  # F-63: the explicit flush supersedes a deferral

    def remark_dirty_after_rollback(self) -> None:
        """F-50: this saver's upsert joined a transaction that rolled back;
        re-enter the dirty queue so the data is retried outside the unit."""
        if self.state == SaveState.DELETED:
            return
        if not self._auto_save or self._scheduler is None:
            logger.warning(
                "saver %r flushed inside a rolled-back transaction has no auto-save "
                "scheduler; it will not be retried automatically",
                self,
            )
            return
        self.mark_dirty()

    async def begin_flush_row(self) -> tuple[Any, bytes] | None:
        """Acquire the flush lock and encode this saver's row (F-59).

        Paired with :meth:`end_flush_row`.  The old ``flush_row()`` released
        the lock when it returned ("the caller owns the SQL"), which let a
        concurrent ``delete()`` land its DELETE in the unlocked gap between
        encode and the coalesced upsert -- the upsert then resurrected the
        row (an F-34 regression).  Here the lock stays held until the caller
        releases it after the SQL carrying this row has executed.  Returns
        ``(key, blob)`` with the lock held, or None (lock already released)
        when the row was deleted while waiting.
        """
        await self._flush_lock.acquire()
        if self.state == SaveState.DELETED:
            self._flush_lock.release()
            return None
        try:
            self._require_loaded("batch-flushed")
            return (self.key, self._codec.encode(self._data))
        except BaseException:
            self._flush_lock.release()
            raise

    def end_flush_row(self) -> None:
        """Release the flush lock taken by :meth:`begin_flush_row` (F-59)."""
        self._flush_lock.release()

    def upsert_many_sql(self, rows: int) -> str:
        """Multi-row upsert for this saver's ``(table, column)`` (F-42)."""
        return self._spec.upsert_many_sql(self._column, rows)

    async def delete(self) -> None:
        """Delete the row first, then flip to DELETED (F-04).

        The SQL runs before the state change so a failed delete leaves the
        saver usable and retryable; concurrent deletes join one DELETE.
        The whole operation holds the flush lock (F-34) so no upsert is in
        flight while the DELETE runs.
        """
        if self.state == SaveState.DELETED:
            return
        async with self._flush_lock:
            # A concurrent delete may have won the lock while we waited;
            # mypy cannot see that ``state`` mutates across the await.
            if self.state == SaveState.DELETED:  # type: ignore[comparison-overlap]
                return  # a concurrent delete won the lock and already finished
            if self._delete_future is not None:
                await asyncio.shield(self._delete_future)
                return
            future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._delete_future = future
            try:
                await self._db.execute(self._spec.delete_sql(), (self.key,))
            except asyncio.CancelledError:
                if not future.done():
                    future.set_exception(RuntimeError("delete interrupted by cancellation"))
                raise
            except BaseException as exc:
                if not future.done():
                    future.set_exception(exc)
                raise
            finally:
                self._delete_future = None
            self.state = SaveState.DELETED
            if not future.done():
                future.set_result(None)

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
