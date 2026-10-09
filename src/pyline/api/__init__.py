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

from types import ModuleType
from typing import TYPE_CHECKING

from pyline.core.context import Context

if TYPE_CHECKING:
    # Static knowledge only: mypy keeps ``api.db.query`` style attribute
    # access fully typed while the runtime import stays lazy (F-88).
    from pyline.api import (  # noqa: F401
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

_BOUND: Context | None = None


class ApiUnboundError(RuntimeError):
    def __init__(self) -> None:
        super().__init__(
            "pyline.api is not bound; call api.bind(ctx) once from the entry "
            "point before using the facade"
        )


class ApiServiceUnavailableError(RuntimeError):
    """A service name is not registered in the bound context (wrong process,
    or called before the boot step that registers it)."""


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
    """Fetch a runtime service handle registered by ServerRuntime.

    Untyped on purpose: some registrations are plain factory callables that
    have no class to check against. Typed consumers use
    ``ctx().service(name, cls)`` instead (F-87)."""
    value = ctx().services.get(name)
    if value is None:
        raise ApiServiceUnavailableError(
            f"service {name!r} is not available in this process (or not yet at this boot stage)"
        )
    return value


# F-88: importing ``pyline.api`` used to pull every facade submodule, and
# with them the heavy dependency chain (asyncmy via api.db, zmq via
# api.rpc) -- even for tools that only wanted bind()/ctx(). PEP 562 lazy
# attribute resolution keeps the public surface (``from pyline import api``
# then ``api.timer.call(...)``) while importing a submodule only on first
# attribute access.
_SUBMODULES = (
    "clock",
    "db",
    "debug",
    "env",
    "log",
    "orm",
    "registry",
    "rpc",
    "task",
    "timer",
)


def __getattr__(name: str) -> ModuleType:
    if name in _SUBMODULES:
        import importlib

        module = importlib.import_module(f"pyline.api.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals().keys(), *_SUBMODULES})
