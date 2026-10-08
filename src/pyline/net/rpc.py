"""Async RPC: ``result = await rpc.call(target, "module.func", args...)``.

Improvements over the prototype:

* Native async/await with exceptions (``RpcTimeoutError``, ``RpcRemoteError``)
  instead of status dictionaries and callbacks.
* The pending-call table is the fix for prototype bug #1: every entry is
  removed exactly once -- on result, on timeout, or on caller cancellation --
  so the table can never grow without bound.
* Cancellation propagates to the remote side (best-effort CANCEL message).
* Every call carries a call id that links request/response in the logs.
"""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import msgpack

from pyline.net.network import Network

logger = logging.getLogger(__name__)

RPC_FLAG = "@rpc"

MSG_CALL = 1
MSG_RESULT = 2
MSG_CANCEL = 3

# Service number of the caller of the RPC currently executing (prototype
# RpcFromServer); 0 outside an inbound RPC.
_current_caller: contextvars.ContextVar[int] = contextvars.ContextVar(
    "pyline_rpc_caller", default=0
)


def current_caller() -> int:
    """Service number of the caller of the RPC being executed here, else 0."""
    return _current_caller.get()


class RpcError(Exception):
    """Base class for RPC failures."""


class RpcTimeoutError(RpcError):
    pass


class RpcRemoteError(RpcError):
    """The remote function raised; ``remote_repr`` carries its description."""

    def __init__(self, func: str, remote_repr: str) -> None:
        super().__init__(f"remote call {func!r} failed: {remote_repr}")
        self.remote_repr = remote_repr


class RpcUnknownFunctionError(RpcRemoteError):
    pass


class SenderProtocol(Protocol):
    def route(self, flag: str, payload: bytes, target_service_no: int) -> None: ...


@dataclass(slots=True)
class _PendingCall:
    call_id: int
    target: int
    func: str
    future: asyncio.Future[Any]
    timer: asyncio.TimerHandle


class RpcManager(Network):
    """Inbound side is a Network on the gateway (flag ``@rpc``)."""

    flag = RPC_FLAG

    def __init__(self, gateway: Any, sender: SenderProtocol, *, own_service_no: int) -> None:
        super().__init__(gateway)
        self._sender = sender
        self._own_service_no = own_service_no
        self._functions: dict[str, Callable[..., Any]] = {}
        self._pending: dict[int, _PendingCall] = {}
        self._running: dict[int, asyncio.Task[None]] = {}
        self._call_seq = itertools.count(1)
        self.timed_out_calls = 0

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #

    def expose(
        self, func: Callable[..., Any] | None = None, *, path: str | None = None
    ) -> Callable[..., Any]:
        """Decorator: make a module-level function callable via RPC."""

        def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
            key = path or f"{fn.__module__}.{fn.__qualname__}"
            if key in self._functions:
                raise ValueError(f"rpc function {key!r} already registered")
            self._functions[key] = fn
            return fn

        return wrap(func) if func is not None else wrap

    def register(self, path: str, func: Callable[..., Any]) -> None:
        self.expose(func, path=path)

    def resolve(self, path: str) -> Callable[..., Any]:
        func = self._functions.get(path)
        if func is not None:
            return func
        raise RpcUnknownFunctionError(path, "function not registered")

    # ------------------------------------------------------------------ #
    # Outbound
    # ------------------------------------------------------------------ #

    async def call(
        self,
        target_service_no: int,
        func_path: str | Callable[..., Any],
        *args: Any,
        timeout: float = 10.0,
    ) -> Any:
        """Invoke ``func_path`` on the target service and await the result.

        ``func_path`` may be the target function object itself (its
        ``module.qualname`` is used) or the path string.
        """
        func_path = _path_of(func_path)
        call_id = next(self._call_seq)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        # F-14: fail here (before any pending entry exists) when the arguments
        # cannot be packed -- previously this surfaced as a fake timeout.
        try:
            call_body = msgpack.packb(
                [MSG_CALL, call_id, self._own_service_no, func_path, list(args)],
                use_bin_type=True,
            )
        except (TypeError, ValueError) as exc:
            raise TypeError(f"rpc arguments for {func_path!r} not serializable: {exc}") from exc
        timer = loop.call_later(timeout, self._timeout_call, call_id)
        self._pending[call_id] = _PendingCall(
            call_id=call_id,
            target=target_service_no,
            func=func_path,
            future=future,
            timer=timer,
        )
        try:
            self._sender.route(RPC_FLAG, call_body, target_service_no)
        except Exception:
            # F-14: a failed send must not strand the pending entry (and its
            # timer) until timeout -- the call failed right here.
            pending = self._pending.pop(call_id, None)
            if pending is not None:
                pending.timer.cancel()
            raise
        try:
            return await future
        except asyncio.CancelledError:
            pending = self._pending.pop(call_id, None)
            if pending is not None:
                pending.timer.cancel()
                self._send(target_service_no, [MSG_CANCEL, call_id, self._own_service_no])
            raise
        finally:
            # Belt and braces: never leave an entry behind (prototype bug #1).
            pending = self._pending.pop(call_id, None)
            if pending is not None:
                pending.timer.cancel()

    def notify(
        self, target_service_no: int, func_path: str | Callable[..., Any], *args: Any
    ) -> None:
        """One-way call: no result, no timeout, fire and forget."""
        self._send(
            target_service_no,
            [MSG_CALL, 0, self._own_service_no, _path_of(func_path), list(args)],
        )

    def _timeout_call(self, call_id: int) -> None:
        pending = self._pending.pop(call_id, None)
        if pending is None or pending.future.done():
            return
        self.timed_out_calls += 1
        pending.future.set_exception(
            RpcTimeoutError(f"rpc call {pending.func!r} -> service {pending.target} timed out")
        )
        # F-18: best-effort -- tell the target to stop executing instead of
        # burning CPU on a result nobody will ever read.
        try:
            self._send(pending.target, [MSG_CANCEL, call_id, self._own_service_no])
        except Exception:
            logger.debug("post-timeout CANCEL undeliverable for call %d", call_id, exc_info=True)

    # ------------------------------------------------------------------ #
    # Inbound (Network entry point)
    # ------------------------------------------------------------------ #

    def handle_message(self, flag: str, payload: bytes) -> None:
        try:
            message = msgpack.unpackb(payload, raw=False, strict_map_key=False)
        except (ValueError, msgpack.exceptions.ExtraData):
            logger.exception("malformed rpc payload")
            return
        if not isinstance(message, list) or not message:
            logger.warning("malformed rpc message shape")
            return
        kind = message[0]
        if kind == MSG_CALL:
            self._on_call(message)
        elif kind == MSG_RESULT:
            self._on_result(message)
        elif kind == MSG_CANCEL:
            self._on_cancel(message)
        else:
            logger.warning("unknown rpc message kind %r", kind)

    def _on_call(self, message: list[Any]) -> None:
        # F-13: a malformed CALL must be dropped here, never unpacked -- a
        # ValueError from the tuple below used to tear the whole connection.
        if len(message) != 5:
            logger.warning("malformed rpc CALL arity %d (dropped)", len(message))
            return
        _, call_id, from_service, func_path, args = message
        if (
            not isinstance(call_id, int)
            or not isinstance(from_service, int)
            or not isinstance(func_path, str)
            or not isinstance(args, list)
        ):
            logger.warning("malformed rpc CALL field types (dropped)")
            return
        task = asyncio.get_running_loop().create_task(
            self._execute(call_id, from_service, func_path, args)
        )
        if call_id:
            self._running[call_id] = task

            def drop_running(_task: asyncio.Task[None], cid: int = call_id) -> None:
                self._running.pop(cid, None)

            task.add_done_callback(drop_running)

    async def _execute(
        self, call_id: int, from_service: int, func_path: str, args: list[Any]
    ) -> None:
        result_kind = 0  # 0=error, 1=success, 2=unknown-function
        caller_token = _current_caller.set(from_service)
        try:
            try:
                func = self._functions.get(func_path)
                if func is None:
                    result_kind = 2
                    raise RpcUnknownFunctionError(func_path, "function not registered")
                result = func(*args)
                if asyncio.iscoroutine(result):
                    result = await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if call_id:
                    self._send(
                        from_service,
                        [MSG_RESULT, call_id, result_kind, f"{type(exc).__name__}: {exc}"],
                    )
                logger.warning("rpc %r raised on execution", func_path, exc_info=True)
                return
            if call_id:
                # F-14: an unserializable result must reach the caller as an
                # error, not as a silent swallow followed by a fake timeout.
                try:
                    msgpack.packb(result, use_bin_type=True)
                except (TypeError, ValueError) as exc:
                    logger.error("rpc %r result not serializable: %s", func_path, exc)
                    self._send(
                        from_service,
                        [MSG_RESULT, call_id, 0, f"result not serializable: {exc}"],
                    )
                    return
                self._send(from_service, [MSG_RESULT, call_id, 1, result])
        finally:
            _current_caller.reset(caller_token)

    def _on_result(self, message: list[Any]) -> None:
        if len(message) != 4:
            logger.warning("malformed rpc RESULT arity %d (dropped)", len(message))
            return
        _, call_id, ok, value = message
        pending = self._pending.get(call_id)
        if pending is None:
            logger.debug("rpc result for unknown/expired call_id=%d", call_id)
            return
        pending.timer.cancel()
        self._pending.pop(call_id, None)
        if pending.future.done():
            return
        if ok == 1:
            pending.future.set_result(value)
        elif ok == 2:
            pending.future.set_exception(RpcUnknownFunctionError(pending.func, str(value)))
        else:
            pending.future.set_exception(RpcRemoteError(pending.func, str(value)))

    def _on_cancel(self, message: list[Any]) -> None:
        if len(message) != 3:
            logger.warning("malformed rpc CANCEL arity %d (dropped)", len(message))
            return
        _, call_id, _from = message
        task = self._running.get(call_id)
        if task is not None and not task.done():
            task.cancel()

    # ------------------------------------------------------------------ #

    def _send(self, target: int, message: list[Any]) -> None:
        try:
            payload = msgpack.packb(message, use_bin_type=True)
        except (TypeError, ValueError) as exc:
            logger.error("rpc message not serializable: %s", exc)
            return
        self._sender.route(RPC_FLAG, payload, target)

    def pending_count(self) -> int:
        return len(self._pending)


def _path_of(func: str | Callable[..., Any]) -> str:
    """Accept a path string or a function object; return the path string."""
    if isinstance(func, str):
        return func
    return f"{func.__module__}.{func.__qualname__}"
