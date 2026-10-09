"""Tracked containers: dict/list subclasses that mark their owner dirty on
mutation (the ergonomic core of the prototype's ObsDict/ObsList, without the
co_names bytecode magic -- the touch callback is declared explicitly, and
optional so dataclasses.asdict can rebuild copies, F-64)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any, SupportsIndex


class TrackedDict[K, V](dict[K, V]):
    """A dict that calls ``touch()`` on every mutation.

    ``touch`` is optional (F-64): ``dataclasses.asdict`` rebuilds container
    fields via ``type(obj)(...)`` without extra keywords, which made ``asdict``
    on a model holding a TrackedDict raise TypeError -- even though
    ``dataclass_codec`` (asdict-based) plus tracked containers is exactly the
    documented combination.  A copy without a callback simply stops reporting
    mutations; serialized copies are read-only, so nothing is lost.
    """

    def __init__(
        self,
        *args: Mapping[K, V] | Iterable[tuple[K, V]],
        touch: Callable[[], None] | None = None,
        **kwargs: V,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._touch = touch

    def _notify(self) -> None:
        if self._touch is not None:
            self._touch()

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        self._notify()

    def __delitem__(self, key: K) -> None:
        super().__delitem__(key)
        self._notify()

    def pop(self, key: K, *default: V) -> V:  # type: ignore[override]
        result = super().pop(key, *default)
        self._notify()
        return result

    def popitem(self) -> tuple[K, V]:
        result = super().popitem()
        self._notify()
        return result

    def clear(self) -> None:
        super().clear()
        self._notify()

    def update(  # type: ignore[override]
        self, other: Mapping[K, V] | Iterable[tuple[K, V]] = (), **kwargs: V
    ) -> None:
        super().update(other, **kwargs)
        self._notify()

    def setdefault(self, key: K, default: V | None = None) -> V:
        result = super().setdefault(key, default)  # type: ignore[arg-type]
        self._notify()
        return result

    def __ior__(  # type: ignore[misc, override]
        self, other: Mapping[K, V] | Iterable[tuple[K, V]]
    ) -> TrackedDict[K, V]:
        # dict.__ior__ bypasses update(); without this override ``d |= {...}``
        # mutates without ever marking the owner dirty (silent data loss).
        super().__ior__(other)
        self._notify()
        return self


class TrackedList[V](list[V]):
    """A list that calls ``touch()`` on every mutation.

    ``touch`` is optional for the same ``asdict`` reason as TrackedDict (F-64).
    """

    def __init__(self, *args: Iterable[V], touch: Callable[[], None] | None = None) -> None:
        super().__init__(*args)
        self._touch = touch

    def _changed(self) -> None:
        if self._touch is not None:
            self._touch()

    def append(self, item: V) -> None:
        super().append(item)
        self._changed()

    def extend(self, items: Iterable[V]) -> None:
        super().extend(items)
        self._changed()

    def insert(self, index: SupportsIndex, item: V) -> None:
        super().insert(index, item)
        self._changed()

    def remove(self, item: V) -> None:
        super().remove(item)
        self._changed()

    def pop(self, index: SupportsIndex = -1) -> V:
        result = super().pop(index)
        self._changed()
        return result

    def clear(self) -> None:
        super().clear()
        self._changed()

    def sort(self, **kwargs: object) -> None:
        super().sort(**kwargs)  # type: ignore[call-overload]
        self._changed()

    def reverse(self) -> None:
        super().reverse()
        self._changed()

    def __setitem__(self, index: SupportsIndex | slice, value: Any) -> None:
        super().__setitem__(index, value)
        self._changed()

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        super().__delitem__(index)
        self._changed()

    def __iadd__(self, items: Iterable[V]) -> Any:  # type: ignore[misc,override]
        super().__iadd__(items)
        self._changed()
        return self

    def __imul__(self, n: SupportsIndex) -> Any:
        # list.__imul__ bypasses the tracked mutators; same class of bug as __ior__.
        super().__imul__(n)
        self._changed()
        return self
