"""In-place reload implementation (see package docstring for the contract).

Guard architecture (F-29):

1. **Static AST validation, no execution** -- the new source is parsed (never
   exec'd into a scratch namespace). The prototype-style sandbox executed the
   module top level once, double-running import side effects; the check is
   now purely structural: kinds, class bases, ``__slots__``/identity dunders,
   and *call-compatibility* of every function/method signature (a new
   required parameter used to be accepted and then TypeError every caller).
2. **Single source read** -- validate and apply the SAME bytes/AST-derived
   code object; ``importlib.reload`` re-read the file, so an edit landing
   between the two reads was checked as A and executed as B.
3. **True rollback** -- the pre-reload snapshot covers module dict, every
   module-owned class dict and every function's swappable attributes, so a
   failure mid-update is fully undone (the old code restored only the module
   dict while half-updated classes kept their new methods).
4. **Runtime invariants** -- closure-layout equality is enforced at swap
   time; with real rollback a violation is now safely fatal to the reload,
   not to the process.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import inspect
import logging
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

logger = logging.getLogger(__name__)

from pyline.obs.metrics import get_metrics  # noqa: E402

# Swappable function state. ``__closure__`` is deliberately absent: it is a
# read-only attribute (assignment silently fails), and closure-layout equality
# is enforced separately at swap time, so old cells stay self-consistent with
# the swapped code. Closure *captured values* are therefore preserved across
# reloads, never updated (see docs/hot-reload.md).
_FUNC_ATTRS = (
    "__code__",
    "__defaults__",
    "__kwdefaults__",
    "__annotations__",
    "__doc__",
    "__dict__",
)

_RELOADING: set[str] = set()

# Changing these on a live class breaks instances already inside dicts/sets.
_IDENTITY_DUNDERS = frozenset(
    {"__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__", "__hash__"}
)


class ReloadError(Exception):
    pass


class ReloadRejected(ReloadError):
    """Forbidden structural change detected by static validation."""


class ReloadedClass(dict[type, type]):
    """Map of old-class -> new-class produced by a reload."""


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def reload_module(module_name: str) -> types.ModuleType:
    """Reload ``module_name`` in place, preserving object identity."""
    module = sys.modules.get(module_name)
    if module is None:
        module = importlib_import(module_name)
    if module_name in _RELOADING:
        logger.debug("nested reload of %s ignored", module_name)
        return module
    if getattr(module, "__file__", None) is None:
        raise ReloadError(f"module {module_name!r} has no source file; cannot reload")

    source, tree = _read_and_parse(module)  # single read (F-29: no TOCTOU)
    _validate_structure(module, tree)

    assert module.__file__ is not None
    code = compile(source, module.__file__, "exec")  # the SAME validated bytes
    _RELOADING.add(module_name)
    cache = ModCache(module)  # pre-reload deep snapshot: the OLD objects
    try:
        before_digest = hashlib.sha256(source).hexdigest()
        exec(code, module.__dict__)  # module dict now holds NEW objects
        class_map = _update_module(module, cache)
        logger.info(
            "reloaded %s (classes=%d, checksum %s)",
            module_name,
            len(class_map),
            before_digest[:10],
        )
        _run_module_hook(module, "__reload__")
        get_metrics().reload_total.labels(result="ok").inc()
    except Exception:
        cache.recover()
        logger.exception("reload of %s failed; module state restored", module_name)
        get_metrics().reload_total.labels(result="failed").inc()
        raise
    finally:
        _RELOADING.discard(module_name)
    return module


def importlib_import(module_name: str) -> types.ModuleType:
    import importlib

    return importlib.import_module(module_name)


def _read_and_parse(module: types.ModuleType) -> tuple[bytes, ast.Module]:
    """Read the source exactly once; return raw bytes and parsed tree."""
    assert module.__file__ is not None
    try:
        source = Path(module.__file__).read_bytes()
    except OSError as exc:
        raise ReloadError(f"cannot read source of {module.__name__}: {exc}") from exc
    try:
        tree = ast.parse(source, filename=module.__file__)
    except SyntaxError as exc:
        raise ReloadRejected(f"syntax error in {module.__name__}: {exc}") from exc
    return source, tree


def _run_module_hook(module: types.ModuleType, hook_name: str) -> None:
    hook = module.__dict__.get(hook_name)
    if isinstance(hook, types.FunctionType) and hook.__module__ == module.__name__:
        try:
            hook()
        except Exception:
            logger.exception("%s hook of %s failed", hook_name, module.__name__)


# --------------------------------------------------------------------------- #
# Guard 1: static (AST) structural validation -- executes nothing
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class _FnSpec:
    positional: list[str] = field(default_factory=list)
    pos_default_count: int = 0
    kwonly: list[str] = field(default_factory=list)
    kwonly_defaults: set[str] = field(default_factory=set)
    star_args: bool = False
    star_kwargs: bool = False


@dataclass(slots=True)
class _ClassInfo:
    bases: list[str] = field(default_factory=list)
    functions: dict[str, _FnSpec] = field(default_factory=dict)
    has_slots: bool = False
    slot_names: frozenset[str] = frozenset()
    slots_unresolved: bool = False
    identity_dunders: frozenset[str] = frozenset()


def _base_name(expr: ast.expr) -> str:
    """Comparable name for a base-class expression: ``list[int]`` and
    ``module.Base`` must compare equal to their runtime ``__qualname__``
    (``list`` / ``Base``), or every subscripted/dotted base is a false reject."""
    if isinstance(expr, ast.Subscript):
        expr = expr.value
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return ast.unparse(expr)


def _slots_names(value: ast.expr) -> tuple[frozenset[str], bool]:
    """Statically resolve a ``__slots__`` assignment value.

    Returns ``(names, unresolved)``; ``unresolved`` is True for anything but a
    string or a tuple/list of strings (the caller then rejects conservatively).
    """
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return frozenset({value.value}), False
    if isinstance(value, (ast.Tuple, ast.List)):
        names: set[str] = set()
        for item in value.elts:
            if not (isinstance(item, ast.Constant) and isinstance(item.value, str)):
                return frozenset(), True
            names.add(item.value)
        return frozenset(names), False
    return frozenset(), True


def _spec_from_arguments(args: ast.arguments) -> _FnSpec:
    positional = [a.arg for a in args.posonlyargs] + [a.arg for a in args.args]
    # ast defaults apply to the trailing N positional parameters
    pos_default_count = len(args.defaults)
    kwonly = [a.arg for a in args.kwonlyargs]
    kwonly_defaults = {
        a.arg for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=False) if d is not None
    }
    return _FnSpec(
        positional=positional,
        pos_default_count=pos_default_count,
        kwonly=kwonly,
        kwonly_defaults=kwonly_defaults,
        star_args=args.vararg is not None,
        star_kwargs=args.kwarg is not None,
    )


def _spec_from_function(fn: types.FunctionType) -> _FnSpec:
    spec = _FnSpec()
    for param in inspect.signature(fn).parameters.values():
        kind = param.kind
        if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            spec.positional.append(param.name)
            if param.default is not inspect.Parameter.empty:
                spec.pos_default_count += 1
        elif kind is inspect.Parameter.KEYWORD_ONLY:
            spec.kwonly.append(param.name)
            if param.default is not inspect.Parameter.empty:
                spec.kwonly_defaults.add(param.name)
        elif kind is inspect.Parameter.VAR_POSITIONAL:
            spec.star_args = True
        elif kind is inspect.Parameter.VAR_KEYWORD:
            spec.star_kwargs = True
    return spec


def _fn_node_spec(node: ast.FunctionDef | ast.AsyncFunctionDef) -> _FnSpec:
    return _spec_from_arguments(node.args)


def _class_info(node: ast.ClassDef) -> _ClassInfo:
    # ``class X:`` has no explicit bases but __bases__ == (object,)
    bases = [_base_name(b) for b in node.bases] or ["object"]
    info = _ClassInfo(bases=bases)
    for stmt in node.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            info.functions[stmt.name] = _fn_node_spec(stmt)
            if stmt.name in _IDENTITY_DUNDERS:
                info.identity_dunders = info.identity_dunders | {stmt.name}
        elif isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "__slots__":
                    info.has_slots = True
                    info.slot_names, info.slots_unresolved = _slots_names(stmt.value)
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            if stmt.target.id == "__slots__" and stmt.value is not None:
                info.has_slots = True
                info.slot_names, info.slots_unresolved = _slots_names(stmt.value)
    return info


def _ast_summary(tree: ast.Module) -> dict[str, tuple[str, object]]:
    """name -> ("function", _FnSpec) | ("class", _ClassInfo) | ("value", None).

    Assignments are reported as plain values: their runtime kind is unknown
    statically (e.g. ``scaled = factory()`` yields a closure function).
    """
    summary: dict[str, tuple[str, object]] = {}
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            summary[stmt.name] = ("function", _fn_node_spec(stmt))
        elif isinstance(stmt, ast.ClassDef):
            summary[stmt.name] = ("class", _class_info(stmt))
        elif isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    summary.setdefault(target.id, ("value", None))
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            summary.setdefault(stmt.target.id, ("value", None))
    return summary


def _validate_structure(old: types.ModuleType, tree: ast.Module) -> None:
    problems: list[str] = []
    summary = _ast_summary(tree)
    for name, old_obj in old.__dict__.items():
        if name.startswith("__"):
            continue
        if getattr(old_obj, "__module__", None) != old.__name__:
            continue
        entry = summary.get(name)
        if entry is None:
            continue  # deleted in the new source: allowed
        kind, info = entry
        if isinstance(old_obj, types.FunctionType):
            if kind == "function" and isinstance(info, _FnSpec):
                _check_signature_compatible(name, _spec_from_function(old_obj), info, problems)
            # function -> assignment/class handled below (class is a problem)
            if kind == "class":
                problems.append(f"{name}: kind changed function -> class")
        elif isinstance(old_obj, type):
            if kind != "class":
                if kind != "value":  # class -> assignment may be a closure trick; reject clearly
                    problems.append(f"{name}: kind changed class -> {kind}")
                else:
                    problems.append(f"{name}: class became a plain assignment")
                continue
            assert isinstance(info, _ClassInfo)
            _check_class(name, old_obj, info, problems)
    if problems:
        raise ReloadRejected(
            "forbidden structural changes (restart required):\n  - " + "\n  - ".join(problems)
        )


def _unwrap_method_function(attr: object) -> object:
    """Descriptor-wrapped methods carry a swappable inner function; signature
    compatibility must be validated on that inner function (the swap would
    replace it, so the wrapper being opaque is no excuse to skip the check)."""
    if isinstance(attr, (staticmethod, classmethod)):
        return attr.__func__
    if isinstance(attr, property):
        return attr.fget
    return attr


def _runtime_slot_names(cls: type) -> frozenset[str]:
    decl = cls.__dict__.get("__slots__", ())
    if isinstance(decl, str):
        return frozenset({decl})
    return frozenset(s for s in decl if isinstance(s, str))


def _check_class(name: str, old: type, info: _ClassInfo, problems: list[str]) -> None:
    old_bases = [b.__qualname__ for b in old.__bases__]
    if old_bases != info.bases:
        problems.append(f"{name}: inheritance changed {old_bases} -> {info.bases}")
    # Own ``__slots__`` only: hasattr() would also see inherited slots and
    # falsely reject every slot-less subclass of a slotted parent.
    old_has_slots = "__slots__" in old.__dict__
    if old_has_slots != info.has_slots:
        problems.append(f"{name}: __slots__ presence changed (hash/layout contract)")
    elif old_has_slots:
        if info.slots_unresolved:
            problems.append(f"{name}: __slots__ cannot be statically resolved (restart required)")
        else:
            old_names = _runtime_slot_names(old)
            if old_names != info.slot_names:
                # Renaming a slot orphans the data still sitting in existing
                # instances' old slot layout; presence alone never caught it.
                problems.append(
                    f"{name}: __slots__ layout changed {sorted(old_names)} -> "
                    f"{sorted(info.slot_names)} (existing instances keep the old layout)"
                )
    old_identity = {d for d in _IDENTITY_DUNDERS if d in old.__dict__}
    if old_identity != set(info.identity_dunders):
        problems.append(
            f"{name}: identity dunder change {sorted(old_identity)} -> "
            f"{sorted(info.identity_dunders)} (instances may sit in dicts/sets)"
        )
    for attr_name, old_attr in old.__dict__.items():
        if attr_name.startswith("__") and attr_name not in ("__init__",):
            continue
        old_fn = _unwrap_method_function(old_attr)
        if not isinstance(old_fn, types.FunctionType):
            continue
        new_spec = info.functions.get(attr_name)
        if new_spec is None:
            continue  # method deleted: allowed (same as before)
        _check_signature_compatible(
            f"{name}.{attr_name}", _spec_from_function(old_fn), new_spec, problems
        )


def _check_signature_compatible(name: str, old: _FnSpec, new: _FnSpec, problems: list[str]) -> None:
    """Old callers must keep working: same positional prefix; extra positionals
    only with defaults; no removed keyword-only; no keyword-only that gains
    "required" status (added without default, or its default removed); same
    *args/**kwargs shape."""
    n = len(old.positional)
    if new.positional[:n] != old.positional:
        problems.append(
            f"{name}: positional parameters changed {old.positional} -> {new.positional}"
        )
        return
    extras = new.positional[n:]
    if extras and new.pos_default_count < len(extras):
        problems.append(
            f"{name}: new parameters {extras} must have defaults "
            "(existing callers pass fewer arguments)"
        )
    if new.pos_default_count < old.pos_default_count:
        problems.append(f"{name}: a positional default was removed")
    old_kw = set(old.kwonly)
    new_kw = set(new.kwonly)
    if old_kw - new_kw:
        problems.append(f"{name}: keyword-only parameters removed {sorted(old_kw - new_kw)}")
    new_required = new_kw - new.kwonly_defaults
    old_required = old_kw - old.kwonly_defaults
    if new_required - old_required:
        problems.append(
            f"{name}: keyword-only parameters {sorted(new_required - old_required)} "
            "must have defaults (existing callers pass fewer arguments)"
        )
    if old.star_args != new.star_args or old.star_kwargs != new.star_kwargs:
        problems.append(f"{name}: *args/**kwargs shape changed")


# --------------------------------------------------------------------------- #
# Guard 2: in-place update with deep rollback support
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
            continue  # definition became plain state: validation rejects this earlier
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
    if old_func.__code__.co_freevars != new_func.__code__.co_freevars:
        # Runtime invariant (rollback makes this fatal to the reload only):
        # swapping a closure with a different free-variable layout breaks
        # every already-created cell consumer.
        raise ReloadError(
            f"closure layout of {old_func.__qualname__} changed "
            f"{old_func.__code__.co_freevars} -> {new_func.__code__.co_freevars}"
        )
    for attr in _FUNC_ATTRS:
        with contextlib.suppress(AttributeError, TypeError):
            setattr(old_func, attr, getattr(new_func, attr))


def _update_class(old_cls: type, new_cls: type, class_map: ReloadedClass) -> None:
    class_map[old_cls] = new_cls
    raw_keep = getattr(old_cls, "__reloadkeep__", ())
    # tuple/list form lists kept attribute names; a bare True (value-level
    # marker) is also legal on classes that happen to be reload targets.
    reload_keep: set[str] = (
        set(raw_keep) if isinstance(raw_keep, (tuple, list, set, frozenset)) else set()
    )
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
        if old_val is not None and getattr(old_val, "__reloadkeep__", False):
            continue  # value-level keep: the carried flag also blocks overwrite
        if old_val is None:
            # New class attributes are visible to existing instances through
            # normal class-level lookup -- no instance migration needed.
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


# --------------------------------------------------------------------------- #
# Guard 2 support: deep module snapshot / rollback
# --------------------------------------------------------------------------- #


def _func_state(fn: types.FunctionType) -> tuple[object, ...]:
    # ``__closure__`` is intentionally not captured: it is read-only on
    # functions, and closure layout equality at swap time guarantees the old
    # cells remain valid for the restored code.
    return (
        fn.__code__,
        fn.__defaults__,
        fn.__kwdefaults__,
        dict(fn.__dict__),
        dict(fn.__annotations__) if fn.__annotations__ else {},
        fn.__doc__,
    )


def _restore_func(fn: types.FunctionType, state: tuple[object, ...]) -> None:
    code, defaults, kwdefaults, fdict, annotations, doc = state
    with contextlib.suppress(AttributeError, TypeError):
        fn.__code__ = code  # type: ignore[assignment]
    with contextlib.suppress(AttributeError, TypeError):
        fn.__defaults__ = defaults  # type: ignore[assignment]
    with contextlib.suppress(AttributeError, TypeError):
        fn.__kwdefaults__ = kwdefaults  # type: ignore[assignment]
    with contextlib.suppress(AttributeError, TypeError):
        fn.__dict__.clear()
        cast("dict[str, object]", fdict)
        fn.__dict__.update(cast("dict[str, object]", fdict))
    with contextlib.suppress(AttributeError, TypeError):
        fn.__annotations__ = annotations  # type: ignore[assignment]
    with contextlib.suppress(AttributeError, TypeError):
        fn.__doc__ = doc  # type: ignore[assignment]


class ModCache:
    """Deep snapshot of a module for rollback and ownership checks.

    Covers the module dict, every module-owned class dict and every module-
    owned function's swappable state (including methods) -- a failure mid-
    update is fully undone (F-29; the old restore only reset the module dict
    and left half-updated classes carrying their new methods).
    """

    def __init__(self, module: types.ModuleType) -> None:
        self._module = module
        self._snapshot = dict(module.__dict__)
        self._class_snapshots: dict[type, dict[str, object]] = {}
        self._func_snapshots: dict[int, tuple[types.FunctionType, tuple[object, ...]]] = {}
        for obj in self._snapshot.values():
            self._capture(obj)
            if isinstance(obj, type) and self._owned(obj):
                for member in obj.__dict__.values():
                    self._capture(member)

    def _capture(self, obj: object) -> None:
        if isinstance(obj, types.FunctionType) and self._owned(obj):
            self._func_snapshots[id(obj)] = (obj, _func_state(obj))
        elif isinstance(obj, type) and self._owned(obj):
            self._class_snapshots[obj] = dict(obj.__dict__)
        elif isinstance(obj, (staticmethod, classmethod)):
            # The swap replaces the descriptor's inner function; rollback must
            # cover it or a failed reload leaves half-new static/class methods.
            func = getattr(obj, "__func__", None)
            if isinstance(func, types.FunctionType):
                self._capture(func)
        elif isinstance(obj, property):
            for part in (obj.fget, obj.fset, obj.fdel):
                if isinstance(part, types.FunctionType):
                    self._capture(part)

    def _owned(self, obj: object) -> bool:
        module_attr = getattr(obj, "__module__", None)
        return module_attr == self._module.__name__

    def snapshot(self) -> dict[str, object]:
        return self._snapshot

    def owned(self, obj: object) -> bool:
        return self._owned(obj)

    def recover(self) -> None:
        for fn, state in self._func_snapshots.values():
            _restore_func(fn, state)
        for cls, saved in self._class_snapshots.items():
            current = dict(cls.__dict__)
            for key in current:
                if key not in saved and key not in ("__dict__", "__weakref__"):
                    with contextlib.suppress(AttributeError, TypeError):
                        delattr(cls, key)
            for key, value in saved.items():
                if key in ("__dict__", "__weakref__"):
                    continue
                setattr(cls, key, value)
        self._module.__dict__.clear()
        self._module.__dict__.update(self._snapshot)
