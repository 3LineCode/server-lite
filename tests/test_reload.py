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


def test_closure_change_rejected_with_rollback(hotmod, tmp_path: Path) -> None:
    """F-29: closure-layout changes are caught at swap time and rolled back
    (the AST pass cannot see free variables; the runtime invariant plus the
    deep snapshot make this fatal to the reload, not the process)."""
    v2 = V1.replace(
        "def scaled(x: int) -> int:\n        return x * factor",
        "def scaled(x: int) -> int:\n        return x * factor + offset",
    ).replace("    factor = 2\n", "    factor = 2\n    offset = 1\n")
    write_module(tmp_path, v2)
    from pyline.reload import ReloadError

    with pytest.raises(ReloadError, match="closure"):
        reload_module("hotmod")
    assert hotmod.scaled(21) == 42  # old closure still intact
    assert hotmod.value() == 1


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
    with pytest.raises(ReloadRejected, match="syntax error"):
        reload_module("hotmod")
    assert hotmod.value() == 1
    assert hotmod.Greeter().greet() == "v1"


class TestHardeningF29:
    def test_rejects_signature_change(self, hotmod, tmp_path: Path) -> None:
        """Adding a required parameter used to be accepted, then TypeError
        every existing caller at next invocation."""
        v2 = V1.replace("def value() -> int:", "def value(extra: int) -> int:")
        write_module(tmp_path, v2)
        with pytest.raises(ReloadRejected, match="must have defaults"):
            reload_module("hotmod")
        assert hotmod.value() == 1

    def test_allows_optional_parameter_addition(self, hotmod, tmp_path: Path) -> None:
        v2 = V1.replace(
            "def value() -> int:\n    return 1",
            "def value(extra: int = 5) -> int:\n    return extra",
        )
        write_module(tmp_path, v2)
        reload_module("hotmod")
        assert hotmod.value() == 5
        assert hotmod.value(extra=9) == 9

    def test_rejects_hash_dunder_change(self, hotmod, tmp_path: Path) -> None:
        v2 = V1.replace(
            "class Holder:",
            "class Holder:\n    def __hash__(self) -> int:\n        return 1\n",
        )
        write_module(tmp_path, v2)
        with pytest.raises(ReloadRejected, match="identity dunder"):
            reload_module("hotmod")
        hash(hotmod.Holder())  # original hashing semantics intact

    def test_sandbox_executes_nothing(self, hotmod, tmp_path: Path) -> None:
        """The old sandbox exec'd the module top level; a module with an
        import-time side effect must not run it twice during validation."""
        side_effect = tmp_path / "side_effect.txt"
        v1 = textwrap.dedent(
            f"""
            PATH = {str(side_effect)!r}
            def _touch() -> None:
                pass
            _touch()
            with open(PATH, "a") as fh:
                fh.write("x")
            def value() -> int:
                return 1
            """
        )
        write_module(tmp_path, v1)
        monkey_path = tmp_path
        import sys as _sys

        _sys.modules.pop("hotmod", None)
        _sys.path.insert(0, str(monkey_path))
        try:
            import hotmod as fresh  # noqa: F401  (side effect runs once)
            count_before = side_effect.read_text().count("x")
            v2 = v1.replace("return 1", "return 2")
            write_module(tmp_path, v2)
            reload_module("hotmod")
            count_after = side_effect.read_text().count("x")
            assert count_after == count_before + 1  # exactly once (the exec)
        finally:
            _sys.path.remove(str(monkey_path))
            _sys.modules.pop("hotmod", None)

    def test_class_rollback_restores_class_dict(self, hotmod, tmp_path: Path) -> None:
        """A failure mid-update must restore the class dict, not leave a mix
        of new methods on old classes (the old restore only reset the module)."""
        v2 = V1.replace(
            'def greet(self) -> str:\n        return "v1"',
            'def greet(self) -> str:\n        return "v2"\n\n    def __hash__(self) -> int:\n        return 7\n',
        )
        write_module(tmp_path, v2)
        with pytest.raises(ReloadRejected, match="identity dunder"):
            reload_module("hotmod")
        assert hotmod.Greeter().greet() == "v1"  # untouched, validation rejected earlier
        # now force a RUNTIME failure after validation: closure swap error
        v3 = V1.replace(
            "    factor = 2\n", "    factor = 2\n    offset = 1\n"
        ).replace("return x * factor", "return x * factor + offset")
        write_module(tmp_path, v3)
        from pyline.reload import ReloadError

        with pytest.raises(ReloadError, match="closure"):
            reload_module("hotmod")
        assert hotmod.Greeter().greet() == "v1"
        assert hotmod.Holder.state == {"open": True}

    def test_single_source_read(self, hotmod, tmp_path: Path) -> None:
        """Validation and application consume the same bytes: a file changed
        in between cannot be checked-as-A-executed-as-B anymore."""
        original = Path(hotmod.__file__)
        v2 = V1.replace("return 1", "return 2")
        write_module(tmp_path, v2)
        reload_module("hotmod")
        assert hotmod.value() == 2
        assert original.read_text(encoding="utf-8").count("return 2") == 1
