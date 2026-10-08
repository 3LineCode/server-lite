"""In-place hot reload with sandbox prevalidation.

Ported from the prototype's battle-tested algorithm (keep object identity,
swap ``__code__``/``__closure__``, diff class dicts, ``__reloadkeep__``
state preservation, rollback on failure) and hardened with the approved
three-layer defence:

1. **Sandbox prevalidation** -- the new source is compiled and executed into
   an isolated namespace first; forbidden structural changes (class bases,
   closure/free-var layouts, object-kind swaps) are rejected *before* any
   live object is touched.
2. **Rollback** -- a module-dict snapshot restores the previous state if the
   in-place update raises at any point.
3. **Auditability** -- structured logging with a source checksum before and
   after every reload.

Documented, deliberate restrictions (inherited from the prototype's own
field notes):

* Do not change a class's inheritance -- restart the process instead.
* Do not change a function's closure/free-variable layout.
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
