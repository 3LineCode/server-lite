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
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import msgpack

from pyline.net.network import Network
from pyline.obs.metrics import get_metrics

logger = logging.getLogger(__name__)

RPC_FLAG = "@rpc"

MSG_CALL = 1
MSG_RESULT = 2
MSG_CANCEL = 3

# Sent back when an inbound CALL cannot acquire an execution slot within
# ``inflight_wait``: the function is NOT executed and the caller sees this as
# an RpcRemoteError instead of piling up tasks on the server.
BUSY_MESSAGE = "server busy: rpc inflight limit reached, retry later"

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
    started: float = 0.0


class RpcManager(Network):
    """Inbound side is a Network on the gateway (flag ``@rpc``)."""

    flag = RPC_FLAG

    def __init__(
        self,
        gateway: Any,
        sender: SenderProtocol,
        *,
        own_service_no: int,
        max_inflight: int = 128,
        inflight_wait: float = 5.0,
    ) -> None:
        super().__init__(gateway)
        self._sender = sender
        self._own_service_no = own_service_no
        self._functions: dict[str, Callable[..., Any]] = {}
        self._pending: dict[int, _PendingCall] = {}
        # (caller service no, call_id) -> (running task, caller) -- the caller
        # is who a MSG_CANCEL must come from (F-40). F-69: keyed by the
        # composite ``(from_service, call_id)`` -- every process numbers its
        # calls from 1, so two callers' first CALLs used to collide on bare
        # ``call_id``: the second overwrote the first's entry and the first
        # task's done-callback then popped the second's, leaving a CANCEL
        # with no table entry and a remote task running forever. The caller
        # half of the key comes from the transport-validated envelope origin
        # (F-39), so it cannot be forged.
        self._running: dict[tuple[int, int], tuple[asyncio.Task[None], int]] = {}
        self._call_seq = itertools.count(1)
        self._metrics = get_metrics()
        self.timed_out_calls = 0
        # F-78: msgpack can also raise RecursionError (deeply nested payload)
        # -- counted, not just logged.
        self.malformed_messages = 0
        # F-19: inbound concurrency cap. A bare task per CALL let any
        # authenticated peer stack unbounded work with a CALL storm; execution
        # now queues on this semaphore, and callers that wait longer than
        # ``inflight_wait`` get a busy error instead of executing.
        self._max_inflight = max_inflight
        self._inflight_wait = inflight_wait
        self._inflight = asyncio.Semaphore(max_inflight)
        self.busy_rejects = 0
        # F-77: notify-style calls (call_id 0) rejected while busy used to
        # vanish without a trace; counted now.
        self.busy_notify_drops = 0

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
            started=time.monotonic(),
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
        self._metrics.rpc_timeouts.inc()
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

    def handle_message(self, flag: str, payload: bytes, from_service: int = 0) -> None:
        try:
            message = msgpack.unpackb(payload, raw=False, strict_map_key=False)
        except (ValueError, msgpack.exceptions.ExtraData, RecursionError):
            # F-78: RecursionError covers adversarially deep msgpack nesting
            # (msgpack builds containers recursively while unpacking); only
            # ValueError/ExtraData used to be caught, so one such payload
            # used to tear the connection down instead of dropping the frame.
            self.malformed_messages += 1
            logger.warning("malformed rpc payload (total=%d)", self.malformed_messages)
            return
        if not isinstance(message, list) or not message:
            logger.warning("malformed rpc message shape")
            return
        kind = message[0]
        if kind == MSG_CALL:
            self._on_call(message, from_service)
        elif kind == MSG_RESULT:
            self._on_result(message, from_service)
        elif kind == MSG_CANCEL:
            self._on_cancel(message, from_service)
        else:
            logger.warning("unknown rpc message kind %r", kind)

    def _reject_origin(self, reason: str, kind: str, from_service: int) -> None:
        """F-40: every inbound rpc message must carry a validated origin.

        The ZMQ ROUTER only forwards messages whose claimed ``from`` matched
        the sender's socket identity (F-39), and the proxy stamps the original
        sender into the ``@fwd`` envelope; ``from_service == 0`` therefore
        means the message bypassed both (or a legacy peer) and is untrusted.
        A spoofed RESULT used to resolve any pending future for a guessed
        call_id with an attacker-chosen value.
        """
        self._metrics.rpc_origin_rejects.labels(reason=reason).inc()
        logger.warning(
            "rpc %s dropped: origin validation failed (%s, from_service=%d)",
            kind,
            reason,
            from_service,
        )

    def _on_call(self, message: list[Any], from_service: int) -> None:
        # F-13: a malformed CALL must be dropped here, never unpacked -- a
        # ValueError from the tuple below used to tear the whole connection.
        if len(message) != 5:
            logger.warning("malformed rpc CALL arity %d (dropped)", len(message))
            return
        _, call_id, from_service_claim, func_path, args = message
        if (
            not isinstance(call_id, int)
            or not isinstance(from_service_claim, int)
            or not isinstance(func_path, str)
            or not isinstance(args, list)
        ):
            logger.warning("malformed rpc CALL field types (dropped)")
            return
        if from_service == 0:
            self._reject_origin("unknown", "CALL", from_service_claim)
            return
        if from_service_claim != from_service:
            # The transport-validated origin wins over the body's claim;
            # a mismatch means someone is lying about who they are.
            logger.warning(
                "rpc CALL claims from=%d but validated origin is %d; using the "
                "validated origin for replies",
                from_service_claim,
                from_service,
            )
            from_service_claim = from_service
        task = asyncio.get_running_loop().create_task(
            self._execute(call_id, from_service_claim, func_path, args)
        )
        # F-20: keep a strong reference + observe the outcome for EVERY task
        # (call_id 0 used to leave the task unreferenced -- GC could kill it
        # mid-run and exceptions were never retrieved).
        self._inbound.add(task)
        task.add_done_callback(self._inbound_done)
        if call_id:
            # F-69: composite key -- see _running's declaration comment.
            key = (from_service_claim, call_id)
            self._running[key] = (task, from_service_claim)

            def drop_running(_task: asyncio.Task[None], k: tuple[int, int] = key) -> None:
                self._running.pop(k, None)

            task.add_done_callback(drop_running)

    async def _acquire_slot(self) -> bool:
        """Bounded wait for an execution slot; ``False`` means busy (F-19).

        F-77: a plain ``wait_for(semaphore.acquire())`` leaks permits through
        a cancellation window. When the grant lands in the same loop
        iteration in which the caller is cancelled (or the wait times out),
        the permit is consumed but the caller never resumes from the
        acquire -- its ``finally: release()`` below never runs, and after a
        few unlucky cancellations the server reports busy forever. Here the
        acquire runs as an explicit task wrapped in ``shield``; a flag set
        atomically by that task (no await between grant and flag) records
        whether the permit was consumed, and every non-taking exit path
        hands a raced-in grant straight back.
        """
        granted = False

        async def _acquire() -> None:
            nonlocal granted
            await self._inflight.acquire()
            granted = True

        acq = asyncio.ensure_future(_acquire())
        took = False
        try:
            await asyncio.wait_for(asyncio.shield(acq), timeout=self._inflight_wait)
            took = True
            return True
        except TimeoutError:
            # Busy: the wait expired. The finally block cancels the acquire
            # and reclaims any permit that raced in.
            return False
        finally:
            if not took:
                # Timeout or caller cancellation (MSG_CANCEL). If the grant
                # raced in before the cancellation was delivered the acquire
                # task still ran to its flag assignment, so the permit was
                # consumed and must be returned here (F-77). When it did not,
                # CPython's Semaphore itself gives an assigned-but-unconsumed
                # permit back and ``granted`` stays False.
                acq.cancel()
                if granted:
                    self._inflight.release()

    async def _execute(
        self, call_id: int, from_service: int, func_path: str, args: list[Any]
    ) -> None:
        result_kind = 0  # 0=error, 1=success, 2=unknown-function
        caller_token = _current_caller.set(from_service)
        try:
            # F-19: wait bounded for an execution slot. On timeout the function
            # is never touched -- the caller learns the server is busy instead
            # of the task waiting forever (or the table growing unbounded).
            # Cancellation (MSG_CANCEL) propagates untouched and, with the
            # F-77 helper, never strands a permit.
            if not await self._acquire_slot():
                self.busy_rejects += 1
                logger.warning(
                    "rpc inflight limit (%d) reached; rejecting call %d to %r "
                    "(waited %.1fs, rejects=%d)",
                    self._max_inflight,
                    call_id,
                    func_path,
                    self._inflight_wait,
                    self.busy_rejects,
                )
                if call_id:  # notify-style calls (call_id 0) expect no reply
                    self._send(from_service, [MSG_RESULT, call_id, 0, BUSY_MESSAGE])
                else:
                    # F-77: nothing is replied to a notify, so the drop used to
                    # be invisible -- count it and leave a debug trace.
                    self.busy_notify_drops += 1
                    logger.debug(
                        "notify-style call %r dropped: rpc inflight limit reached (total=%d)",
                        func_path,
                        self.busy_notify_drops,
                    )
                return
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
                self._inflight.release()
        finally:
            _current_caller.reset(caller_token)

    def _on_result(self, message: list[Any], from_service: int) -> None:
        if len(message) != 4:
            logger.warning("malformed rpc RESULT arity %d (dropped)", len(message))
            return
        _, call_id, ok, value = message
        pending = self._pending.get(call_id)
        if pending is None:
            logger.debug("rpc result for unknown/expired call_id=%d", call_id)
            return
        # F-40: only the service the caller actually invoked may resolve its
        # future. A result arriving via any other (or unknown) origin is an
        # injection attempt -- drop it and let the caller's own timeout fire.
        if from_service == 0:
            self._reject_origin("unknown", "RESULT", from_service)
            return
        if from_service != pending.target:
            self._reject_origin("mismatch", "RESULT", from_service)
            return
        pending.timer.cancel()
        self._pending.pop(call_id, None)
        if pending.future.done():
            return
        self._metrics.rpc_latency.observe(time.monotonic() - pending.started)
        if ok == 1:
            pending.future.set_result(value)
        elif ok == 2:
            pending.future.set_exception(RpcUnknownFunctionError(pending.func, str(value)))
        else:
            pending.future.set_exception(RpcRemoteError(pending.func, str(value)))

    def _on_cancel(self, message: list[Any], from_service: int) -> None:
        if len(message) != 3:
            logger.warning("malformed rpc CANCEL arity %d (dropped)", len(message))
            return
        _, call_id, from_claim = message
        # F-40: only the service that issued the CALL may cancel it -- a
        # spoofed CANCEL is a cheap remote DoS against arbitrary running calls.
        if from_service == 0 or from_claim != from_service:
            self._reject_origin(
                "unknown" if from_service == 0 else "mismatch", "CANCEL", from_service
            )
            return
        # F-69: the composite key scopes the lookup to THIS caller's call --
        # another service calling with the same call_id must not be found (and
        # cannot be cancelled) here.
        entry = self._running.get((from_service, call_id))
        if entry is None:
            return
        task, caller = entry
        if caller != from_service:
            self._reject_origin("mismatch", "CANCEL", from_service)
            return
        if not task.done():
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
