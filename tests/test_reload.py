"""Hot reload: identity-preserving updates, keep-attrs, rollback, sandbox guard.

This is the regression suite the CI matrix reruns per Python version.
"""

from __future__ import annotations

import itertools
import os
import sys
import textwrap
import time
from pathlib import Path

import pytest

from pyline.reload import ReloadRejected, reload_module

V1 = textwrap.dedent(
    """
    COUNTER = {"n": 0}

    def value() -> int:
        return 1

    def _make_scaled():
        factor = 2

        def scaled(x: int) -> int:
            return x * factor

        return scaled

    scaled = _make_scaled()

    class Greeter:
        keep_me = "preserved"

        def greet(self) -> str:
            return "v1"

    class Holder:
        __reloadkeep__ = ("state", )
        state = {"open": True}
    """
)

V2_FUNCTION_ONLY = V1.replace("return 1", "return 2")


# Monotonically increasing future mtimes: same-second, same-size rewrites
# would otherwise hit Python's stale-bytecode cache during reload.
_mtime_seq = itertools.count(int(time.time()) + 10)


def write_module(tmp_path: Path, source: str) -> None:
    path = tmp_path / "hotmod.py"
    path.write_text(source, encoding="utf-8")
    stamp = next(_mtime_seq)
    os.utime(path, (stamp, stamp))


@pytest.fixture()
def hotmod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Drop any cached import from a previous test's tmp_path.
    monkeypatch.delitem(sys.modules, "hotmod", raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    write_module(tmp_path, V1)
    import hotmod

    return hotmod


def test_function_body_updates_in_place(hotmod, tmp_path: Path) -> None:
    old_func = hotmod.value
    instance = hotmod.Greeter()
    old_greet = instance.greet

    write_module(tmp_path, V2_FUNCTION_ONLY)
    reload_module("hotmod")

    assert hotmod.value is old_func  # identity preserved
    assert hotmod.value() == 2  # new body
    assert hotmod.Greeter().greet() == "v1"  # untouched class still works
    assert instance.greet() == "v1"
    assert instance.greet.__func__ is old_greet.__func__


def test_class_method_updates_and_instances_follow(hotmod, tmp_path: Path) -> None:
    v2 = V1.replace('return "v1"', 'return "v2"')
    instance = hotmod.Greeter()
    write_module(tmp_path, v2)
    reload_module("hotmod")
    # Same class object; existing instances see the new method.
    assert isinstance(instance, hotmod.Greeter)
    assert instance.greet() == "v2"


def test_reloadkeep_preserves_state(hotmod, tmp_path: Path) -> None:
    hotmod.Holder.state["open"] = False  # mutate module-level class attr
    v2 = V1.replace('state = {"open": True}', 'state = {"open": "replaced"}')
    write_module(tmp_path, v2)
    reload_module("hotmod")
    assert hotmod.Holder.state["open"] is False  # kept, not reinitialized


def test_module_globals_survive(hotmod, tmp_path: Path) -> None:
    hotmod.COUNTER["n"] = 42
    reload_module("hotmod")  # unchanged source reload
    assert hotmod.COUNTER["n"] == 42


def test_sandbox_rejects_inheritance_change(hotmod, tmp_path: Path) -> None:
    v2 = V1.replace("class Greeter:", "class Parent:\n    pass\n\n\nclass Greeter(Parent):")
    write_module(tmp_path, v2)
    with pytest.raises(ReloadRejected, match="inheritance"):
        reload_module("hotmod")
    assert hotmod.value() == 1  # nothing was touched


def test_sandbox_rejects_closure_change(hotmod, tmp_path: Path) -> None:
    v2 = V1.replace(
        "def scaled(x: int) -> int:\n        return x * factor",
        "def scaled(x: int) -> int:\n        return x * factor + offset",
    ).replace("    factor = 2\n", "    factor = 2\n    offset = 1\n")
    write_module(tmp_path, v2)
    with pytest.raises(ReloadRejected, match="closure"):
        reload_module("hotmod")


def test_sandbox_rejects_kind_change(hotmod, tmp_path: Path) -> None:
    v2 = V1.replace("def value() -> int:\n    return 1", "class value:\n    pass")
    write_module(tmp_path, v2)
    with pytest.raises(ReloadRejected, match="kind changed"):
        reload_module("hotmod")


def test_external_reference_stays_valid(hotmod, tmp_path: Path) -> None:
    from hotmod import value as imported_value

    write_module(tmp_path, V2_FUNCTION_ONLY)
    reload_module("hotmod")
    assert imported_value() == 2  # the *imported* binding picked up new code


def test_syntax_error_leaves_module_untouched(hotmod, tmp_path: Path) -> None:
    write_module(tmp_path, "def broken(:\n")
    with pytest.raises(SyntaxError):
        reload_module("hotmod")
    assert hotmod.value() == 1
    assert hotmod.Greeter().greet() == "v1"
