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
| True rollback | deep snapshot (module dict + class dicts + function state) restored on any failure, including `BaseException`s like `SystemExit` raised by the re-executed top level (F-41) |
| Runtime invariants | closure-layout mismatch is detected at swap time and the deep rollback undoes every swap the reload had already made (the check compares the *executed* new code object's free variables, so it cannot run before the swap starts) |

## Forbidden (rejected with `ReloadRejected` -- restart required)

* **Inheritance change** -- `__bases__` of a live class cannot be re-pointed
  safely (the prototype allowed a narrow special case; pyline rejects all).
  Resolvable bases (including dotted `module.Base` and subscripted
  `list[int]`) compare by their module-qualified key (F-100), so swapping
  `from other1 import Base` for `from other2 import Base` is rejected;
  a base that cannot be resolved statically falls back to bare-name
  comparison for that base only.
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
  default, or their default removed), a positional-or-keyword parameter
  becoming positional-only (`def f(a, b)` -> `def f(a, /, b)`: the merged
  positional prefix is unchanged but every `f(b=...)` keyword caller
  breaks; the reverse loosening is allowed), `*args`/`**kwargs` shape
  change. The
  check covers plain methods **and** the inner functions of
  `@staticmethod`/`@classmethod`/`@property`, **and** protocol dunders
  (`__exit__`, `__call__`, `__aiter__`, `__enter__`, ... -- F-41: they are
  invoked by the language with fixed arity, so a silent signature change
  broke every caller at swap time) as well as module-level `__getattr__`.
  Name-mangled privates (`__helper`) are exempt: their live names
  (`_Cls__helper`) cannot be matched against the AST statically. Adding
  trailing optional parameters and new defaulted keyword-only parameters is
  allowed.
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

* module-level `__reload__()` and class `__reload__()` hooks run after
  a successful reload (only when defined in the reloaded module itself);
  the class form binds through the descriptor protocol, so
  `@classmethod` (receives `cls`), `@staticmethod`, and a zero-argument
  plain `def __reload__()` all work; an instance-method form
  (`def __reload__(self)` -- no instance exists at reload time) is logged
  as a failed hook, never silently skipped;
* `__reloadkeep__ = ("attr", ...)` on a class lists attributes that survive
  reloads untouched; an attribute carrying `__reloadkeep__ = True` is kept
  individually;
* module-level plain values are runtime state by default: a reload never
  clobbers them (the prototype needed manual `if not "g_X" in globals()`
  guards for this).

## Reload events (PreReloadEvent / OnReloadEvent)

Every framework-driven reload (console `update`, file watcher) goes through
one chain:

1. `PreReloadEvent(module)` handlers run to completion **before** the module
   swap. The runtime *awaits* them; a handler may quiesce traffic, serialize
   state or unsubscribe stale handlers, and it is guaranteed that none of
   the new code is live yet when they run.
2. `reload_module` swaps the code in place (this document).
3. `OnReloadEvent(module)` is emitted after the swap and the network-handler
   rebind, as a fire-and-forget notification (restart tasks, refresh
   caches).

Calling `pyline.reload.reload_module` directly (no events) remains legal --
the event chain is what the runtime's `reload_hook` adds around it. Handler
failures are isolated by the bus and never abort the reload itself.

## Known limits (accepted -- plan restarts around them)

Static validation makes reloads safe against *layout* breakage; it cannot
make the re-executed top level side-effect free, and it only sees what the
AST can see:

* **Top-level side effects are real and are NOT rolled back.** The new
  source's module top level genuinely executes once during the swap. Network
  calls, file writes, registration into other modules' globals, or spawned
  tasks that escape before a failed reload leaves their traces behind. The
  snapshot restores the *reloaded module's* namespace only -- state written
  into other modules stays written. Keep top levels pure; register in hooks.
* **Decorator-wrapped functions are checked by their wrapper's outer
  signature only.** A decorator that adapts `(*args, **kwargs)` hides the
  inner function's real signature from the validator; a call-incompatible
  change inside the wrapper passes validation and fails at runtime. Validate
  such changes by hand (or restart).
* **Closure factories created by assignment are treated as values** and skip
  signature validation entirely (`handler = make_handler(...)`).
* **Only the reloaded module is validated.** Other modules' call sites keep
  their old compiled bytecode: a change that is compatible per the table
  above can still break an old caller in another module that passes
  arguments positionally in a way the new signature no longer accepts at
  *that* call's shape. Restart-sensitive changes deserve a restart.
* **The watcher execs synchronously on the event loop.** Reloading a module
  whose top level is slow blocks the loop for that duration (visible as a
  `loop_latency` alert). Prefer small modules or console-triggered reloads
  during quiet windows.

## Not supported

C extension modules, metaclass changes, decorated functions whose closure
structure changes. Python version bumps must pass the reload regression
suite (`tests/test_reload.py`) in CI before rollout.
