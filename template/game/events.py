"""Business event hooks (game layer).

Subscribe to any pyline core event; handlers may be sync or async. This
skeleton demonstrates the common lifecycle points.

Hot-reload note: this module's ``register(bus)`` runs once, when the module
is first imported. After ``update game.events`` the framework swaps the code
of every function in place but does NOT re-run ``register`` -- handler
additions/removals in a reloaded events module are yours to manage (call
``register`` from a module-level ``__reload__()`` hook, or unsubscribe stale
handlers in ``PreReloadEvent``). Existing handler code itself picks up the
new bodies automatically; see docs/hot-reload.md.
"""

from pyline.core.events import (
    BaseInitEvent,
    FuncInitEvent,
    FuncQuitEvent,
    NewDayEvent,
    NewHourEvent,
)


def register(bus) -> None:
    bus.subscribe(BaseInitEvent, on_base_init)
    bus.subscribe(FuncInitEvent, on_func_init)
    bus.subscribe(NewHourEvent, on_new_hour)
    bus.subscribe(NewDayEvent, on_new_day)
    bus.subscribe(FuncQuitEvent, on_func_quit)


async def on_base_init(event: BaseInitEvent) -> None:
    print("base systems init")


async def on_func_init(event: FuncInitEvent) -> None:
    print("business systems init")


async def on_new_hour(event: NewHourEvent) -> None:
    print(f"hour tick: {event.hour}")


async def on_new_day(event: NewDayEvent) -> None:
    print(f"new day: {event.day}")


async def on_func_quit(event: FuncQuitEvent) -> None:
    print("business teardown")
