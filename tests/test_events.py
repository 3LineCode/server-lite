"""Event bus: layer ordering, reverse dispatch, handler isolation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from pyline.core.events import (
    LAYER_BUSINESS,
    LAYER_FRAMEWORK,
    LAYER_PUBLIC,
    EventBus,
)


@dataclass
class Ping:
    n: int


async def test_layer_order() -> None:
    bus = EventBus()
    order: list[str] = []
    bus.subscribe(Ping, lambda e: order.append("fw"), layer=LAYER_FRAMEWORK)
    bus.subscribe(Ping, lambda e: order.append("pub"), layer=LAYER_PUBLIC)
    bus.subscribe(Ping, lambda e: order.append("biz"), layer=LAYER_BUSINESS)
    await bus.emit(Ping(n=1))
    assert order == ["fw", "pub", "biz"]


async def test_reverse_order() -> None:
    bus = EventBus()
    order: list[str] = []
    bus.subscribe(Ping, lambda e: order.append("fw"), layer=LAYER_FRAMEWORK)
    bus.subscribe(Ping, lambda e: order.append("biz"), layer=LAYER_BUSINESS)
    await bus.emit(Ping(n=1), reverse=True)
    assert order == ["biz", "fw"]


async def test_async_handlers() -> None:
    bus = EventBus()
    seen: list[int] = []

    async def handler(event: Ping) -> None:
        await asyncio.sleep(0.01)
        seen.append(event.n)

    bus.subscribe(Ping, handler)
    await bus.emit(Ping(n=7))
    assert seen == [7]


async def test_failing_handler_does_not_block_others() -> None:
    bus = EventBus()

    def bad(_: Ping) -> None:
        raise RuntimeError("nope")

    got: list[int] = []
    bus.subscribe(Ping, bad)
    bus.subscribe(Ping, lambda e: got.append(e.n))
    await bus.emit(Ping(n=3))
    assert got == [3]
    assert bus.handler_failures()


async def test_unsubscribe() -> None:
    bus = EventBus()
    seen: list[int] = []
    unsub = bus.subscribe(Ping, lambda e: seen.append(e.n))
    unsub()
    await bus.emit(Ping(n=1))
    assert seen == []


def test_invalid_layer_rejected() -> None:
    bus = EventBus()
    with pytest.raises(ValueError):
        bus.subscribe(Ping, lambda e: None, layer=99)
