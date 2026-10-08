"""In-place reload implementation (see package docstring for the contract)."""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import logging
import sys
import types
from pathlib import Path

logger = logging.getLogger(__name__)

from pyline.obs.metrics import get_metrics  # noqa: E402

_METRICS = get_metrics()

_FUNC_ATTRS = (
    "__code__",
    "__defaults__",
    "__kwdefaults__",
    "__annotations__",
    "__doc__",
    "__dict__",
    "__closure__",
)

_RELOADING: set[str] = set()


class ReloadError(Exception):
    pass


class ReloadRejected(ReloadError):
    """Forbidden structural change detected in the sandbox pass."""


class ReloadedClass(dict[type, type]):
    """Map of old-class -> new-class produced by a reload."""


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def reload_module(module_name: str) -> types.ModuleType:
    """Reload ``module_name`` in place, preserving object identity."""
    module = sys.modules.get(module_name)
    if module is None:
        module = importlib.import_module(module_name)
    if module_name in _RELOADING:
        logger.debug("nested reload of %s ignored", module_name)
        return module
    if getattr(module, "__file__", None) is None:
        raise ReloadError(f"module {module_name!r} has no source file; cannot reload")

    sandbox = _sandbox_import(module)
    _validate_structure(module, sandbox)

    _RELOADING.add(module_name)
    cache = ModCache(module)  # pre-reload snapshot: the OLD objects
    try:
        before_digest = _source_digest(module)
        importlib.reload(module)  # module dict now holds NEW objects
        class_map = _update_module(module, cache)
        after_digest = _source_digest(module)
        logger.info(
            "reloaded %s (classes=%d, checksum %s -> %s)",
            module_name,
            len(class_map),
            before_digest[:10],
            after_digest[:10],
        )
        _run_module_hook(module, "__reload__")
        _METRICS.reload_total.labels(result="ok").inc()
    except Exception:
        cache.recover()
        logger.exception("reload of %s failed; module state restored", module_name)
        _METRICS.reload_total.labels(result="failed").inc()
        raise
    finally:
        _RELOADING.discard(module_name)
    return module


def _run_module_hook(module: types.ModuleType, hook_name: str) -> None:
    hook = module.__dict__.get(hook_name)
    if isinstance(hook, types.FunctionType) and hook.__module__ == module.__name__:
        try:
            hook()
        except Exception:
            logger.exception("%s hook of %s failed", hook_name, module.__name__)


# --------------------------------------------------------------------------- #
# Guard 1: sandbox prevalidation
# --------------------------------------------------------------------------- #


def _sandbox_import(module: types.ModuleType) -> types.ModuleType:
    """Execute the module's current source into a scratch namespace."""
    assert module.__file__ is not None
    source_path = Path(module.__file__)
    source = source_path.read_bytes()
    code = compile(source, str(source_path), "exec")
    scratch = types.ModuleType(f"__sandbox__.{module.__name__}")
    scratch.__file__ = module.__file__
    scratch.__dict__["__name__"] = scratch.__name__
    try:
        exec(code, scratch.__dict__)
    except Exception as exc:
        raise ReloadRejected(f"sandbox exec of {module.__name__} failed: {exc}") from exc
    return scratch


def _validate_structure(old: types.ModuleType, new: types.ModuleType) -> None:
    problems: list[str] = []
    old_dict = {
        k: v
        for k, v in old.__dict__.items()
        if getattr(v, "__module__", None) == old.__name__ and not k.startswith("__")
    }
    for name, old_obj in old_dict.items():
        new_obj = new.__dict__.get(name)
        if new_obj is None:
            continue  # deletion is allowed
        if type(old_obj) is not type(new_obj):
            problems.append(
                f"{name}: kind changed {type(old_obj).__name__} -> {type(new_obj).__name__}"
            )
            continue
        if isinstance(old_obj, type):
            _check_class(name, old_obj, new_obj, problems)
        elif isinstance(old_obj, types.FunctionType):
            _check_function(name, old_obj, new_obj, problems)
    if problems:
        raise ReloadRejected(
            "forbidden structural changes (restart required):\n  - " + "\n  - ".join(problems)
        )


def _check_class(name: str, old: type, new: type, problems: list[str]) -> None:
    if old.__bases__ != new.__bases__:
        problems.append(
            f"{name}: inheritance changed "
            f"{[b.__name__ for b in old.__bases__]} -> {[b.__name__ for b in new.__bases__]}"
        )
    for attr_name, old_attr in old.__dict__.items():
        if attr_name.startswith("__") and attr_name not in ("__init__",):
            continue
        new_attr = new.__dict__.get(attr_name)
        if isinstance(old_attr, types.FunctionType) and isinstance(new_attr, types.FunctionType):
            _check_function(f"{name}.{attr_name}", old_attr, new_attr, problems)


def _check_function(
    name: str, old: types.FunctionType, new: types.FunctionType, problems: list[str]
) -> None:
    if old.__code__.co_freevars != new.__code__.co_freevars:
        problems.append(
            f"{name}: closure layout changed "
            f"{old.__code__.co_freevars} -> {new.__code__.co_freevars}"
        )


# --------------------------------------------------------------------------- #
# Guard 2: in-place update with rollback support
# --------------------------------------------------------------------------- #


def _update_module(module: types.ModuleType, cache: ModCache) -> ReloadedClass:
    """Fold freshly reloaded objects into the OLD identities.

    ``cache`` holds the pre-reload dict; ``module.__dict__`` holds the new
    objects. For every module-owned name present in both, the old object is
    updated in place (code swap / class-dict diff) and restored into the
    module dict so external ``from module import x`` references stay valid.
    """
    class_map = ReloadedClass()
    for name, old_obj in cache.snapshot().items():
        if name.startswith("__"):
            continue  # module dunders (__name__, __loader__...) belong to importlib
        new_obj = module.__dict__.get(name)
        if new_obj is None or new_obj is old_obj:
            continue  # deleted in new version, or untouched by the reload
        if isinstance(new_obj, (types.FunctionType, type)):
            if not cache.owned(old_obj):
                # Was plain state, is now a definition -- accept the new one.
                continue
            _update_generic(old_obj, new_obj, class_map)
            setattr(module, name, old_obj)
        elif isinstance(old_obj, (types.FunctionType, type)):
            continue  # definition became plain state: sandbox rejects this earlier
        else:
            # Plain module-level values are runtime state: a reload never
            # clobbers live state. (The prototype needed a manual
            # "if not g_X in globals()" guard for this; here it is default.)
            setattr(module, name, old_obj)
    return class_map


def _update_generic(old_obj: object, new_obj: object, class_map: ReloadedClass) -> object:
    if isinstance(old_obj, type) and isinstance(new_obj, type):
        _update_class(old_obj, new_obj, class_map)
        return old_obj
    if isinstance(old_obj, types.FunctionType) and isinstance(new_obj, types.FunctionType):
        _update_function(old_obj, new_obj)
        return old_obj
    if isinstance(old_obj, property) and isinstance(new_obj, property):
        for attr in ("fdel", "fget", "fset"):
            old_part = getattr(old_obj, attr)
            new_part = getattr(new_obj, attr)
            if isinstance(old_part, types.FunctionType) and isinstance(
                new_part, types.FunctionType
            ):
                _update_function(old_part, new_part)
        return old_obj
    if isinstance(old_obj, staticmethod) and isinstance(new_obj, staticmethod):
        _update_descriptor(old_obj, new_obj)
        return old_obj
    if isinstance(old_obj, classmethod) and isinstance(new_obj, classmethod):
        _update_descriptor(old_obj, new_obj)
        return old_obj
    return new_obj


def _update_descriptor(old_desc: object, new_desc: object) -> None:
    """Swap the underlying function of static/class method descriptors."""
    old_func = getattr(old_desc, "__func__", None)
    new_func = getattr(new_desc, "__func__", None)
    if isinstance(old_func, types.FunctionType) and isinstance(new_func, types.FunctionType):
        _update_function(old_func, new_func)


def _update_function(old_func: types.FunctionType, new_func: types.FunctionType) -> None:
    for attr in _FUNC_ATTRS:
        with contextlib.suppress(AttributeError, TypeError):
            setattr(old_func, attr, getattr(new_func, attr))


def _update_class(old_cls: type, new_cls: type, class_map: ReloadedClass) -> None:
    class_map[old_cls] = new_cls
    reload_keep = set(getattr(old_cls, "__reloadkeep__", ()))
    old_dict = dict(old_cls.__dict__)
    new_dict = dict(new_cls.__dict__)
    for key in old_dict:
        if key in new_dict or key in reload_keep or key == "__reloadkeep__":
            continue
        if getattr(old_dict[key], "__reloadkeep__", False):
            continue
        with contextlib.suppress(AttributeError, TypeError):
            delattr(old_cls, key)
    for key, new_val in new_dict.items():
        if key == "__dict__" or key == "__weakref__":
            continue
        if key in reload_keep:
            continue  # kept attributes are neither deleted nor overwritten
        old_val = old_dict.get(key)
        if old_val is None:
            setattr(old_cls, key, new_val)
            continue
        updated = _update_generic(old_val, new_val, class_map)
        if updated is old_val:
            # Keep the identity of in-place-updated members.
            setattr(old_cls, key, old_val)
        else:
            setattr(old_cls, key, new_val)
    _run_class_hook(old_cls)


def _run_class_hook(cls: type) -> None:
    hook = cls.__dict__.get("__reload__")
    if hook is not None and callable(hook):
        try:
            hook()
        except Exception:
            logger.exception("class __reload__ hook of %s failed", cls.__qualname__)


def _source_digest(module: types.ModuleType) -> str:
    try:
        assert module.__file__ is not None
        data = Path(module.__file__).read_bytes()
    except OSError:
        return "unknown"
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------- #
# Guard 2 support: module snapshot / rollback
# --------------------------------------------------------------------------- #


class ModCache:
    """Snapshot of a module dict for rollback and ownership checks."""

    def __init__(self, module: types.ModuleType) -> None:
        self._module = module
        self._snapshot = dict(module.__dict__)

    def snapshot(self) -> dict[str, object]:
        return self._snapshot

    def owned(self, obj: object) -> bool:
        module_attr = getattr(obj, "__module__", None)
        return module_attr == self._module.__name__

    def recover(self) -> None:
        self._module.__dict__.clear()
        self._module.__dict__.update(self._snapshot)
