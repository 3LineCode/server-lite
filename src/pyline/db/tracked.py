"""Tracked containers: dict/list subclasses that mark their owner dirty on
mutation (the ergonomic core of the prototype's ObsDict/ObsList, without the
co_names bytecode magic -- the touch callback is declared explicitly, and
optional so dataclasses.asdict can rebuild copies, F-64).

F-165: values *inserted into* a tracked container are themselves wrapped when
they are plain dicts/lists -- ``td["a"] = {"x": 1}`` followed by
``td["a"]["x"] = 2`` marks the owner dirty. Before F-165 only the initial
``set_data``/load path wrapped (orm._bind_tracking), so every later insert of
a plain container re-opened the "in-place mutation is invisible" hole the
tracked containers exist to close. Wrapping binds the child to the SAME touch
callback as its parent, so nested mutations dirty the same saver.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any, SupportsIndex, cast

logger = logging.getLogger(__name__)

# F-216: warn once per subclass type -- bind_tracking rebuilds dict/list
# SUBCLASSES as plain TrackedDict/TrackedList (a defaultdict loses its
# default_factory, an OrderedDict its ordering guarantees); the warning must
# not fire per instance or the load path would spam.
_SUBCLASS_WARNED: set[type] = set()


def _warn_subclass(value: dict[Any, Any] | list[Any]) -> None:
    vt = type(value)
    if vt in _SUBCLASS_WARNED:
        return
    _SUBCLASS_WARNED.add(vt)
    logger.warning(
        "tracked container field holds %s; it will be rebuilt as a plain tracked "
        "%s (subclass-specific behaviour is lost) -- container fields should be "
        "plain dict/list",
        vt.__qualname__,
        "dict" if isinstance(value, dict) else "list",
    )


def bind_tracking(value: Any, touch: Callable[[], None] | None) -> Any:
    """Recursively wrap plain dicts/lists in tracked containers bound to
    ``touch`` (F-162/F-165).

    Already-tracked containers pass through untouched (they keep their own
    callback -- no double wrap); every other object type (models included)
    passes through unchanged: model attribute tracking is the model's own
    ``__setattr__``, not the container's business.
    """
    if isinstance(value, TrackedDict):
        return value
    if isinstance(value, TrackedList):
        return value
    if isinstance(value, dict):
        if type(value) is not dict:
            _warn_subclass(value)  # F-216
        wrapped = {k: bind_tracking(v, touch) for k, v in value.items()}
        return TrackedDict(wrapped, touch=touch)
    if isinstance(value, list):
        if type(value) is not list:
            _warn_subclass(value)  # F-216
        wrapped_items = [bind_tracking(v, touch) for v in value]
        return TrackedList(wrapped_items, touch=touch)
    return value


def _wrap_nested(value: Any, touch: Callable[[], None] | None) -> Any:
    """Wrap ``value`` for insertion into a container with ``touch``.

    No-op when the container has no callback (asdict-built copies) or the
    value is not a plain dict/list -- wrapping anything else would change
    observable types for no tracking benefit.
    """
    if touch is None:
        return value
    if isinstance(value, (dict, list)) and not isinstance(value, (TrackedDict, TrackedList)):
        return bind_tracking(value, touch)
    return value


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
        if touch is not None:
            # F-165: initial plain-container values are wrapped too -- the
            # constructor is just another insertion path.
            for key in list(self.keys()):
                super().__setitem__(key, _wrap_nested(self[key], touch))

    def _notify(self) -> None:
        if self._touch is not None:
            self._touch()

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, _wrap_nested(value, self._touch))
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
        if isinstance(other, Mapping):
            for key, value in other.items():
                super().__setitem__(key, _wrap_nested(value, self._touch))
        else:
            for key, value in other:
                super().__setitem__(key, _wrap_nested(value, self._touch))
        for key, value in kwargs.items():
            super().__setitem__(cast("K", key), _wrap_nested(value, self._touch))
        self._notify()

    def setdefault(self, key: K, default: V | None = None) -> V:
        if key not in self:
            super().__setitem__(key, _wrap_nested(default, self._touch))
            self._notify()
            return self[key]
        return self[key]

    def __ior__(  # type: ignore[misc, override]
        self, other: Mapping[K, V] | Iterable[tuple[K, V]]
    ) -> TrackedDict[K, V]:
        # dict.__ior__ bypasses update(); without this override ``d |= {...}``
        # mutates without ever marking the owner dirty (silent data loss).
        self.update(other)
        return self


class TrackedList[V](list[V]):
    """A list that calls ``touch()`` on every mutation.

    ``touch`` is optional for the same ``asdict`` reason as TrackedDict (F-64).
    """

    def __init__(self, *args: Iterable[V], touch: Callable[[], None] | None = None) -> None:
        if args:
            # list() accepts at most one iterable; wrap its items (F-165).
            super().__init__(_wrap_nested(v, touch) for v in args[0])
        self._touch = touch

    def _changed(self) -> None:
        if self._touch is not None:
            self._touch()

    def append(self, item: V) -> None:
        super().append(_wrap_nested(item, self._touch))
        self._changed()

    def extend(self, items: Iterable[V]) -> None:
        for item in items:
            super().append(_wrap_nested(item, self._touch))
        self._changed()

    def insert(self, index: SupportsIndex, item: V) -> None:
        super().insert(index, _wrap_nested(item, self._touch))
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
        if isinstance(index, slice):
            value = [_wrap_nested(v, self._touch) for v in value]
        else:
            value = _wrap_nested(value, self._touch)
        super().__setitem__(index, value)
        self._changed()

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        super().__delitem__(index)
        self._changed()

    def __iadd__(self, items: Iterable[V]) -> Any:  # type: ignore[misc,override]
        self.extend(items)
        return self

    def __imul__(self, n: SupportsIndex) -> Any:
        # list.__imul__ bypasses the tracked mutators; same class of bug as __ior__.
        super().__imul__(n)
        self._changed()
        return self
