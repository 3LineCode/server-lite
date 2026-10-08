"""Business-facing API facade (the ``aioapi`` equivalent).

Usage: the entry point binds the process Context once, then business code
imports the thin submodules::

    from pyline import api
    api.bind(ctx)                     # once, from the entry point
    ...
    from pyline.api import rpc, timer, env
    await rpc.call(210001, "game.shop.buy", uid=1)
    timer.call("daily", 30.0, refresh_shop)

The bound holder is the ONE deliberate module-level convenience (explicit
``bind``, no import side effects, loud error when unbound) -- the bridge
between the old repo's ``from aio_core import CreateTask`` ergonomics and
the new repo's no-hidden-globals rule. Framework code keeps using Context
directly.
"""

from __future__ import annotations

from pyline.core.context import Context

_BOUND: Context | None = None


class ApiUnboundError(RuntimeError):
    def __init__(self) -> None:
        super().__init__(
            "pyline.api is not bound; call api.bind(ctx) once from the entry "
            "point before using the facade"
        )


def bind(ctx: Context) -> None:
    """Bind the process context (idempotent; rebinding the same ctx is fine)."""
    global _BOUND
    _BOUND = ctx


def unbind() -> None:
    global _BOUND
    _BOUND = None


def ctx() -> Context:
    if _BOUND is None:
        raise ApiUnboundError()
    return _BOUND


def service(name: str) -> object:
    """Fetch a runtime service handle registered by ServerRuntime."""
    value = ctx().services.get(name)
    if value is None:
        raise RuntimeError(
            f"service {name!r} is not available in this process (or not yet "
            "at this boot stage)"
        )
    return value


# Convenience: make ``pyline.api.env`` etc. resolve without extra imports.
# Imported at the END to keep bind()/ctx() defined before circular imports.
from pyline.api import (  # noqa: E402,F401
    clock,
    db,
    debug,
    env,
    log,
    orm,
    registry,
    rpc,
    task,
    timer,
)
