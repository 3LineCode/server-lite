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
# ``__dict__`` is also absent: it is merged instead of swapped (see
# _update_function, F-100) so runtime-attached function attributes survive.
_FUNC_ATTRS = (
    "__code__",
    "__defaults__",
    "__kwdefaults__",
    "__annotations__",
    "__doc__",
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


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def reload_module(module_name: str) -> types.ModuleType:
    """Reload ``module_name`` in place, preserving object identity."""
    module = sys.modules.get(module_name)
    if module is None:
        # F-96: the watcher used to auto-import unknown modules, so saving
        # any stray .py file (a test, a build script) EXECUTED its import
        # side effects inside the live server. A reload is only meaningful
        # for code the server already runs; everything else must be imported
        # explicitly first.
        raise ReloadRejected(f"module {module_name!r} is not loaded; import it explicitly first")
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
        updated_classes = _update_module(module, cache)
        logger.info(
            "reloaded %s (classes=%d, checksum %s)",
            module_name,
            updated_classes,
            before_digest[:10],
        )
        _run_module_hook(module, "__reload__")
        get_metrics().reload_total.labels(result="ok").inc()
    except BaseException:
        # F-41: the re-executed module top level can raise SystemExit /
        # KeyboardInterrupt / GeneratorExit (BaseException, not Exception).
        # Skipping the rollback there used to leave the module half-updated.
        cache.recover()
        logger.exception("reload of %s failed; module state restored", module_name)
        get_metrics().reload_total.labels(result="failed").inc()
        raise
    finally:
        _RELOADING.discard(module_name)
    return module


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
    # Names among ``positional`` that are POSITIONAL_ONLY (``def f(a, /)``).
    # The merged ``positional`` list alone cannot see ``def f(a, b)`` ->
    # ``def f(a, /, b)`` -- same merged list, but every ``f(b=...)`` keyword
    # caller now TypeErrors. Kind may only loosen (posonly -> pos-or-kw),
    # never tighten.
    posonly: frozenset[str] = frozenset()
    # WHICH positional parameters have defaults, by name (F-155). A mere
    # count cannot see the breakage class ``def f(a, b=1)`` ->
    # ``def f(a, b, c=1)``: the count matches (1 == 1) and the shared
    # prefix [a, b] matches, yet every old ``f(1)`` caller now TypeErrors
    # because ``b`` -- defaulted before -- lost its default. Positions are
    # what callers depend on.
    pos_defaults: frozenset[str] = frozenset()
    kwonly: list[str] = field(default_factory=list)
    kwonly_defaults: set[str] = field(default_factory=set)
    star_args: bool = False
    star_kwargs: bool = False


@dataclass(slots=True)
class _ClassInfo:
    bases: list[str] = field(default_factory=list)
    # F-100: module-qualified base keys parallel to ``bases``; ``None`` marks
    # a base that cannot be resolved statically (the caller then falls back
    # to the legacy bare-name comparison for that base only).
    base_keys: list[str | None] = field(default_factory=list)
    functions: dict[str, _FnSpec] = field(default_factory=dict)
    # F-149: methods declared ``async def`` in the new source. Swapping a
    # sync function's code for a coroutine's (or vice versa) passes every
    # signature check yet TypeErrors every existing ``await f()`` (or bare
    # ``f()``) caller -- the exact caller-breakage class this validator
    # exists to prevent.
    async_functions: set[str] = field(default_factory=set)
    has_slots: bool = False
    slot_names: frozenset[str] = frozenset()
    slots_unresolved: bool = False
    identity_dunders: frozenset[str] = frozenset()


def _import_bindings(tree: ast.Module, module_name: str) -> dict[str, str]:
    """Static ``bound name -> dotted object path`` map for top-level imports.

    ``from m import n as k`` -> ``k: "m.n"``; ``import a.b as c`` ->
    ``c: "a.b"``; ``import a.b`` -> ``a: "a"`` (the top package binds).
    Relative imports resolve against ``module_name``'s package."""
    package = module_name.rsplit(".", 1)[0] if "." in module_name else ""
    bindings: dict[str, str] = {}

    def _resolve_relative(level: int, tail: str) -> str:
        # level 1 = the containing package, each extra level steps up.
        parts = package.split(".") if package else []
        if level > 1:
            steps = level - 1
            parts = parts[: len(parts) - steps] if steps <= len(parts) else []
        prefix = ".".join(parts)
        if tail and prefix:
            return f"{prefix}.{tail}"
        return tail or prefix

    for stmt in tree.body:
        if isinstance(stmt, ast.ImportFrom):
            src = stmt.module or ""
            if stmt.level:
                src = _resolve_relative(stmt.level, src)
            for alias in stmt.names:
                bound = alias.asname or alias.name
                bindings[bound] = f"{src}.{alias.name}" if src else alias.name
        elif isinstance(stmt, ast.Import):
            for alias in stmt.names:
                if alias.asname:
                    bindings[alias.asname] = alias.name
                else:
                    top = alias.name.split(".")[0]
                    bindings[top] = top
    return bindings


def _base_name(expr: ast.expr) -> str:
    """Display name for a base-class expression (``list[int]`` -> ``list``,
    ``module.Base`` -> ``Base``) -- used in rejection messages and as the
    legacy fallback comparison key."""
    if isinstance(expr, ast.Subscript):
        expr = expr.value
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return ast.unparse(expr)


def _base_key(expr: ast.expr, imports: dict[str, str], module: types.ModuleType) -> str | None:
    """Module-qualified comparison key for one base expression, or ``None``
    when the name cannot be resolved statically.

    F-100: the legacy bare-name comparison accepted swapping a base for a
    same-named class from a DIFFERENT module (``from other1 import Base`` ->
    ``from other2 import Base``): validation passed, but _update_class never
    re-points ``__bases__``, so the live class silently kept the OLD base
    while the source advertised the new one. Resolvable names now compare by
    ``module.qualname``; unresolvable ones keep the old bare-name semantics
    so typing tricks (``Generic[T]``) are not false-rejected."""
    if isinstance(expr, ast.Subscript):
        expr = expr.value
    if isinstance(expr, ast.Attribute):
        parts = ast.unparse(expr).split(".")
        if parts[0] in imports:
            parts[0] = imports[parts[0]]
        return ".".join(parts)
    if isinstance(expr, ast.Name):
        name = expr.id
        if name in imports:
            return imports[name]
        existing = module.__dict__.get(name)
        if isinstance(existing, type) and existing.__module__ == module.__name__:
            return f"{module.__name__}.{name}"
        import builtins

        if hasattr(builtins, name):
            return name
        return None
    return None  # calls / complex expressions: legacy fallback


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
    # ast defaults apply to the trailing N positional parameters -- so the
    # defaulted names are the last N of ``positional`` (F-155: by name, not
    # by count).
    defaulted = positional[len(positional) - len(args.defaults) :] if args.defaults else []
    kwonly = [a.arg for a in args.kwonlyargs]
    kwonly_defaults = {
        a.arg for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=False) if d is not None
    }
    return _FnSpec(
        positional=positional,
        posonly=frozenset(a.arg for a in args.posonlyargs),
        pos_defaults=frozenset(defaulted),
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
            if kind is inspect.Parameter.POSITIONAL_ONLY:
                spec.posonly = spec.posonly | {param.name}
            if param.default is not inspect.Parameter.empty:
                spec.pos_defaults = spec.pos_defaults | {param.name}
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


def _class_info(
    node: ast.ClassDef, imports: dict[str, str], module: types.ModuleType
) -> _ClassInfo:
    # ``class X:`` has no explicit bases but __bases__ == (object,)
    bases = [_base_name(b) for b in node.bases] or ["object"]
    base_exprs: list[ast.expr] = list(node.bases) or [ast.Name(id="object", ctx=ast.Load())]
    info = _ClassInfo(
        bases=bases,
        base_keys=[_base_key(b, imports, module) for b in base_exprs],
    )
    for stmt in node.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            info.functions[stmt.name] = _fn_node_spec(stmt)
            if isinstance(stmt, ast.AsyncFunctionDef):
                info.async_functions.add(stmt.name)  # F-149
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


def _is_coroutine_func(fn: types.FunctionType) -> bool:
    """Whether ``fn`` was declared ``async def`` (F-149)."""
    return bool(fn.__code__.co_flags & inspect.CO_COROUTINE)


def _ast_summary(
    tree: ast.Module, imports: dict[str, str], module: types.ModuleType
) -> dict[str, tuple[str, object]]:
    """name -> ("function"|"asyncfunction", _FnSpec) | ("class", _ClassInfo)
    | ("value", None).

    Assignments are reported as plain values: their runtime kind is unknown
    statically (e.g. ``scaled = factory()`` yields a closure function).
    F-149: coroutine functions get their own kind so a sync<->async swap is
    rejectable -- the signature compatibility alone cannot see it.
    """
    summary: dict[str, tuple[str, object]] = {}
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            kind = "asyncfunction" if isinstance(stmt, ast.AsyncFunctionDef) else "function"
            summary[stmt.name] = (kind, _fn_node_spec(stmt))
        elif isinstance(stmt, ast.ClassDef):
            summary[stmt.name] = ("class", _class_info(stmt, imports, module))
        elif isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    summary.setdefault(target.id, ("value", None))
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            summary.setdefault(stmt.target.id, ("value", None))
    return summary


def _validate_structure(old: types.ModuleType, tree: ast.Module) -> None:
    problems: list[str] = []
    imports = _import_bindings(tree, old.__name__)
    summary = _ast_summary(tree, imports, old)
    for name, old_obj in old.__dict__.items():
        # No dunder skip here (F-41): non-owned machinery (__builtins__,
        # __loader__, ...) is filtered by the __module__ ownership check below,
        # while a module-level ``__getattr__`` defined in this module is a
        # real protocol surface whose signature must stay call-compatible.
        if getattr(old_obj, "__module__", None) != old.__name__:
            continue
        entry = summary.get(name)
        if entry is None:
            continue  # deleted in the new source: allowed
        kind, info = entry
        if isinstance(old_obj, types.FunctionType):
            # F-149: an async<->sync swap passes every signature check but
            # breaks every caller's await (or bare call) -- reject it with
            # the other kind changes.
            if kind == "asyncfunction" and not _is_coroutine_func(old_obj):
                problems.append(f"{name}: kind changed function -> coroutine function")
                continue
            if kind == "function" and _is_coroutine_func(old_obj):
                problems.append(f"{name}: kind changed coroutine function -> function")
                continue
            if kind in ("function", "asyncfunction") and isinstance(info, _FnSpec):
                _check_signature_compatible(name, _spec_from_function(old_obj), info, problems)
            # function -> class handled below (class is a problem)
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


def _runtime_base_keys(cls: type) -> list[tuple[str, str]]:
    """``(bare qualname, module-qualified)`` pair per runtime base class."""
    return [
        (
            b.__qualname__,
            b.__qualname__ if b.__module__ == "builtins" else f"{b.__module__}.{b.__qualname__}",
        )
        for b in cls.__bases__
    ]


def _check_class(name: str, old: type, info: _ClassInfo, problems: list[str]) -> None:
    old_pairs = _runtime_base_keys(old)
    if len(old_pairs) != len(info.bases):
        problems.append(f"{name}: inheritance changed {info.bases} (base count differs)")
    else:
        for (bare, qualified), new_key, new_display in zip(
            old_pairs, info.base_keys, info.bases, strict=True
        ):
            # F-100: resolvable AST bases compare module-qualified (catches
            # the same-name different-module swap); unresolvable ones fall
            # back to the legacy bare-name comparison.
            if new_key is None:
                if new_display != bare:
                    problems.append(
                        f"{name}: inheritance changed {[b for b, _ in old_pairs]} -> {info.bases}"
                    )
                    break
            elif new_key != qualified:
                problems.append(
                    f"{name}: inheritance changed {[q for _, q in old_pairs]} -> "
                    f"{[k if k is not None else d for k, d in zip(info.base_keys, info.bases, strict=True)]}"
                )
                break
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
        # F-41: only name-mangled privates (``__foo``) skip -- they live under
        # ``_Cls__foo`` in the class dict, so AST and runtime names cannot be
        # matched. Real dunders DO get validated: ``__exit__``/``__call__``/
        # ``__aiter__``/... are invoked by the language with fixed arity, and a
        # signature change that slipped validation used to break every
        # protocol caller the moment the swap landed.
        if attr_name.startswith("__") and not attr_name.endswith("__"):
            continue
        old_fn = _unwrap_method_function(old_attr)
        if not isinstance(old_fn, types.FunctionType):
            continue
        new_spec = info.functions.get(attr_name)
        if new_spec is None:
            continue  # method deleted: allowed (same as before)
        # F-149: method-level twin of the module-level async<->sync check.
        if _is_coroutine_func(old_fn) != (attr_name in info.async_functions):
            problems.append(
                f"{name}.{attr_name}: kind changed "
                f"{'coroutine function -> function' if _is_coroutine_func(old_fn) else 'function -> coroutine function'}"
            )
            continue
        _check_signature_compatible(
            f"{name}.{attr_name}", _spec_from_function(old_fn), new_spec, problems
        )


def _check_signature_compatible(name: str, old: _FnSpec, new: _FnSpec, problems: list[str]) -> None:
    """Old callers must keep working: same positional prefix; extra positionals
    only with defaults; no shared positional parameter may LOSE its default
    (F-155: the old count-only comparison missed ``def f(a, b=1)`` ->
    ``def f(a, b, c=1)`` -- counts match, prefix matches, yet every ``f(1)``
    caller breaks); no removed keyword-only; no keyword-only that gains
    "required" status (added without default, or its default removed); same
    *args/**kwargs shape; and no parameter kind tightening -- a
    positional-or-keyword parameter becoming positional-only keeps the merged
    positional prefix identical but breaks every ``f(x=...)`` keyword caller."""
    n = len(old.positional)
    if new.positional[:n] != old.positional:
        problems.append(
            f"{name}: positional parameters changed {old.positional} -> {new.positional}"
        )
        return
    tightened = sorted(
        param
        for param in old.positional
        if param not in old.posonly  # keyword callers exist for these
        and param in new.posonly
    )
    if tightened:
        problems.append(
            f"{name}: positional-or-keyword parameters {tightened} became "
            "positional-only (keyword callers would break)"
        )
    extras_missing = [p for p in new.positional[n:] if p not in new.pos_defaults]
    if extras_missing:
        problems.append(
            f"{name}: new parameters {extras_missing} must have defaults "
            "(existing callers pass fewer arguments)"
        )
    lost_defaults = sorted(old.pos_defaults - new.pos_defaults)
    if lost_defaults:
        problems.append(
            f"{name}: parameters {lost_defaults} lost their defaults "
            "(existing callers pass fewer arguments)"
        )
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


def _update_module(module: types.ModuleType, cache: ModCache) -> int:
    """Fold freshly reloaded objects into the OLD identities.

    ``cache`` holds the pre-reload dict; ``module.__dict__`` holds the new
    objects. For every module-owned name present in both, the old object is
    updated in place (code swap / class-dict diff) and restored into the
    module dict so external ``from module import x`` references stay valid.

    Returns the number of classes updated (log line only -- the old
    old->new class map this used to build was never read by anything).
    """
    updated_classes = 0
    for name, old_obj in cache.snapshot().items():
        if name.startswith("__") and name.endswith("__") and len(name) > 4:
            # Real dunders (__name__, __loader__, __all__, __getattr__...)
            # belong to importlib or are source metadata -- the fresh exec
            # result stays. F-100: a leading-``__`` private WITHOUT the
            # trailing dunder (``__flag``) is ordinary module state and now
            # flows through the same runtime-state preservation below
            # (it used to be excluded wholesale and silently reset on every
            # reload, unlike any other plain value).
            continue
        new_obj = module.__dict__.get(name)
        if new_obj is None or new_obj is old_obj:
            continue  # deleted in new version, or untouched by the reload
        if isinstance(new_obj, (types.FunctionType, type)):
            if not cache.owned(old_obj):
                # Was plain state, is now a definition -- accept the new one.
                continue
            if isinstance(old_obj, type):
                updated_classes += 1
            _update_generic(old_obj, new_obj)
            setattr(module, name, old_obj)
        elif isinstance(old_obj, (types.FunctionType, type)):
            # Definition became plain state. class -> value was rejected by
            # validation above; function -> value is deliberately allowed
            # (the ``x = factory()`` closure pattern reports as a value), so
            # the NEW value wins the module attribute while pre-existing
            # ``from module import x`` holders keep the old function object.
            continue
        else:
            # Plain module-level values are runtime state: a reload never
            # clobbers live state. (The prototype needed a manual
            # "if not g_X in globals()" guard for this; here it is default.)
            setattr(module, name, old_obj)
    return updated_classes


def _update_generic(old_obj: object, new_obj: object) -> object:
    if isinstance(old_obj, type) and isinstance(new_obj, type):
        _update_class(old_obj, new_obj)
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
    if _is_coroutine_func(old_func) != _is_coroutine_func(new_func):
        # F-149 runtime net (AST validation catches the direct case): a
        # code object swap between coroutine and plain functions keeps every
        # signature identical while flipping how every caller must invoke it.
        raise ReloadRejected(
            f"{old_func.__qualname__}: async/sync kind changed "
            f"({'coroutine -> plain' if _is_coroutine_func(old_func) else 'plain -> coroutine'}); "
            "restart required"
        )
    for attr in _FUNC_ATTRS:
        with contextlib.suppress(AttributeError, TypeError):
            setattr(old_func, attr, getattr(new_func, attr))
    # F-100: ``__dict__`` holds runtime-attached state (caches, memo flags,
    # registration marks) that the module-level policy never clobbers for
    # plain values -- a wholesale swap dropped it on every reload, silently
    # un-caching/de-registering the function. Merge instead: def-time
    # attributes of the NEW function only fill in keys the old one never
    # had; anything attached at runtime always wins.
    for key, value in new_func.__dict__.items():
        old_func.__dict__.setdefault(key, value)


def _update_class(old_cls: type, new_cls: type) -> None:
    if type(old_cls) is not type(new_cls):
        # F-100: metaclass changes were silently ignored -- the class-dict
        # diff below never touches ``__class__``, so the live class kept its
        # old metaclass while the source advertised the new one (every
        # enum/ABC trick built on it then misbehaved). In-place swapping a
        # metaclass is not possible; reject explicitly (rolled back like any
        # other reload failure).
        raise ReloadRejected(
            f"{old_cls.__qualname__}: metaclass changed "
            f"{type(old_cls).__module__}.{type(old_cls).__qualname__} -> "
            f"{type(new_cls).__module__}.{type(new_cls).__qualname__} (restart required)"
        )
    # F-155: only the class's OWN marker applies. getattr() walked the MRO,
    # so a base's ``__reloadkeep__`` silently pinned attribute names in
    # every subclass diff too -- surprising inheritance semantics for what
    # is documented as a per-class contract.
    raw_keep = old_cls.__dict__.get("__reloadkeep__", ())
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
        updated = _update_generic(old_val, new_val)
        if updated is old_val:
            # Keep the identity of in-place-updated members.
            setattr(old_cls, key, old_val)
        else:
            setattr(old_cls, key, new_val)
    _run_class_hook(old_cls)


def _run_class_hook(cls: type) -> None:
    hook = cls.__dict__.get("__reload__")
    if hook is None:
        return
    # Bind through the descriptor protocol: a raw ``classmethod`` object is
    # not callable at all on 3.12 (the old ``callable(hook)`` guard silently
    # skipped the documented classmethod form), and calling descriptor
    # objects bare would drop the cls binding. ``__get__(None, cls)`` binds
    # classmethods to their class, unwraps staticmethods, and passes plain
    # functions through -- so a plain ``def __reload__(self)`` (no instance
    # exists at reload time) still fails loudly in the except below.
    with contextlib.suppress(AttributeError):
        hook = hook.__get__(None, cls)
    if not callable(hook):
        return
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

    def _capture(self, obj: object) -> None:
        if isinstance(obj, types.FunctionType) and self._owned(obj):
            self._func_snapshots.setdefault(id(obj), (obj, _func_state(obj)))
        elif isinstance(obj, type) and self._owned(obj):
            if obj in self._class_snapshots:
                return  # already captured; also breaks pathological cycles
            self._class_snapshots[obj] = dict(obj.__dict__)
            # F-100: recurse into the members. The old one-level iteration
            # (module value -> class -> class members) never reached members
            # of NESTED classes, so a rolled-back reload could leave a nested
            # class half-updated -- exactly the F-29 bug class, one level
            # deeper.
            for member in obj.__dict__.values():
                self._capture(member)
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
