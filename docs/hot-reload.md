# Hot reload: contract and forbidden changes

`pyline.reload.reload_module(name)` swaps code in place: object identities
survive, external references and live instances pick up new code
automatically. Validation is **static (AST)** -- the new source is never
executed for checking purposes, so import-time side effects run exactly once
(the old prototype sandbox executed them twice).

## Guarantees

| Property | Mechanism |
|---|---|
| Identity preservation | old objects receive new code; `from mod import f` keeps working |
| Live instances follow new methods | class dict diff on the OLD class object |
| Single source read | validation and application compile the SAME bytes (no TOCTOU) |
| True rollback | deep snapshot (module dict + class dicts + function state) restored on any failure |
| Runtime invariants | closure-layout mismatch aborts the reload before any swap |

## Forbidden (rejected with `ReloadRejected` -- restart required)

* **Inheritance change** -- `__bases__` of a live class cannot be re-pointed
  safely (the prototype allowed a narrow special case; pyline rejects all).
  Subscripted/dotted bases (`list[int]`, `module.Base`) compare by their bare
  name and are not false-rejected.
* **`__slots__` presence or layout change** -- the instance layout contract
  changes. Renaming a slot is rejected even though presence is unchanged:
  existing instances keep the old layout and their data would be orphaned.
  Statically unresolvable `__slots__` values (calls, name references) are
  rejected conservatively -- restart required.
* **Identity dunder change** (`__eq__`, `__ne__`, `__lt__`, `__le__`,
  `__gt__`, `__ge__`, `__hash__`) -- instances already sitting in dicts/sets
  would be looked up under the wrong hash/equality.
* **Call-incompatible signature change** -- removed/renamed positional
  parameters, new parameters without defaults, removed keyword-only
  parameters, keyword-only parameters that become required (added without a
  default, or their default removed), `*args`/`**kwargs` shape change. The
  check covers plain methods **and** the inner functions of
  `@staticmethod`/`@classmethod`/`@property`. Adding trailing optional
  parameters and new defaulted keyword-only parameters is allowed.
* **Kind change** -- a module-level function becoming a class (or vice
  versa).
* **Closure layout change** (`ReloadError` at swap time, rolled back) --
  e.g. a nested function gaining a new free variable. Note: closure
  *captured values* are preserved across reloads -- changing `factor = 2` to
  `factor = 3` inside a factory does **not** re-bind cells created before the
  reload (the closure attribute is read-only on functions; layout equality
  keeps old cells self-consistent with the swapped code).

## Allowed but sharp

* **Deleting** a function/class: other modules holding `from mod import f`
  references keep the old (now zombie) object. Prefer deprecation shims.
* **Adding** class attributes: visible to existing instances through normal
  class-level lookup; no instance migration needed. Instance-*dict* slots
  still need a `__reload__` hook if your logic depends on them.
* **New module-level plain values**: only visible to code that re-reads the
  module attribute (existing `from mod import x` bindings are not updated --
  same as CPython semantics).

## Hooks

* module-level `__reload__()` and classmethod-style `__reload__()` run after
  a successful reload (only when defined in the reloaded module itself);
* `__reloadkeep__ = ("attr", ...)` on a class lists attributes that survive
  reloads untouched; an attribute carrying `__reloadkeep__ = True` is kept
  individually;
* module-level plain values are runtime state by default: a reload never
  clobbers them (the prototype needed manual `if not "g_X" in globals()`
  guards for this).

## Not supported

C extension modules, metaclass changes, decorated functions whose closure
structure changes. Python version bumps must pass the reload regression
suite (`tests/test_reload.py`) in CI before rollout.
