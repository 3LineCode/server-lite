"""Debug facade: exception/stack formatting with locals (old TraceMsg /
RaiseError's local-variable dump, implemented WITHOUT global hooks)."""

from __future__ import annotations

_MAX_VALUE_LEN = 120


def _fmt_value(value: object) -> str:
    text = repr(value)
    if len(text) > _MAX_VALUE_LEN:
        return f"{text[:_MAX_VALUE_LEN]}...[{len(text)} chars]"
    return text


def format_exception(exc: BaseException) -> str:
    """Full traceback with per-frame locals and instance attributes."""
    lines: list[str] = [f"{type(exc).__name__}: {exc}"]
    tb = exc.__traceback__
    frames = []
    while tb is not None:
        frames.append(tb.tb_frame)
        tb = tb.tb_next
    for frame in frames:
        lines.append(
            f"  at {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_qualname}"
        )
        for name, value in list(frame.f_locals.items())[:20]:
            if name.startswith("__"):
                continue
            lines.append(f"    {name} = {_fmt_value(value)}")
        self_obj = frame.f_locals.get("self")
        if self_obj is not None and hasattr(self_obj, "__dict__"):
            for name, value in list(vars(self_obj).items())[:15]:
                lines.append(f"    self.{name} = {_fmt_value(value)}")
    cause = exc.__cause__
    if cause is not None:
        lines.append("caused by:")
        lines.append(format_exception(cause))
    return "\n".join(lines)


def trace(message: str = "") -> list[str]:
    """Caller stack with locals (old TraceMsg); returns the rendered lines."""
    import inspect

    lines = [f"trace: {message}" if message else "trace"]
    stack = inspect.stack()[1:]
    for frame_info in stack[:8]:
        frame = frame_info.frame
        lines.append(
            f"  at {frame.f_code.co_filename}:{frame_info.lineno} in {frame.f_code.co_qualname}"
        )
        for name, value in list(frame.f_locals.items())[:12]:
            if name.startswith("__"):
                continue
            lines.append(f"    {name} = {_fmt_value(value)}")
    return lines
