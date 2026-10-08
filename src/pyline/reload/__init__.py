"""In-place hot reload with static (AST) prevalidation.

Ported from the prototype's battle-tested algorithm (keep object identity,
swap ``__code__``, diff class dicts, ``__reloadkeep__`` state preservation,
rollback on failure) and hardened with the F-29 three-layer defence:

1. **Static validation** -- the new source is parsed (never executed into a
   sandbox); forbidden structural changes (class bases, ``__slots__``
   presence/layout, identity dunders, object-kind swaps, call-incompatible
   signatures) are rejected *before* any live object is touched.
2. **Deep rollback** -- a snapshot covering the module dict, every owned class
   dict and every owned function's swappable state (including the inner
   functions of static/class methods and properties) restores the previous
   state if the in-place update raises at any point.
3. **Auditability** -- structured logging with a checksum of the applied
   source; success/failure counters in Prometheus.

Documented, deliberate restrictions (inherited from the prototype's own
field notes):

* Do not change a class's inheritance -- restart the process instead.
* Do not change a function's closure/free-variable layout. Closure *captured
  values* are preserved across reloads (never re-bound to new values).
* Instances keep their class; deep bound references (stored methods, cached
  partials) are not re-pointed automatically.
* ``__reloadkeep__`` on a class attribute keeps that attribute across reload.
"""

from pyline.reload.inplace import (
    ModCache,
    ReloadedClass,
    ReloadError,
    ReloadRejected,
    reload_module,
)

__all__ = [
    "ModCache",
    "ReloadError",
    "ReloadRejected",
    "ReloadedClass",
    "reload_module",
]
