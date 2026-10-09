"""RPC facade: await-based calls (old RpcCall/RCB/RpcFunc/RpcFromServer)."""

from __future__ import annotations

from typing import Any

from pyline import api
from pyline.net.rpc import RpcManager
from pyline.net.rpc import current_caller as _current_caller

__all__ = ["call", "current_caller", "notify"]


def _rpc() -> RpcManager:
    # F-87: typed bag access instead of an unchecked cast.
    return api.ctx().service("rpc", RpcManager)


async def call(
    target_service_no: int,
    func_path: str,
    *args: Any,
    timeout: float = 10.0,
) -> Any:
    """Invoke an exposed function on another service and await the result.

    Failures arrive as exceptions (old RpcFunctor's ovtfunc/errfunc):
    RpcTimeoutError on timeout, RpcRemoteError when the callee raised,
    RpcUnknownFunctionError for missing paths.
    """
    return await _rpc().call(target_service_no, func_path, *args, timeout=timeout)


def notify(target_service_no: int, func_path: str, *args: Any) -> None:
    """One-way call (old needBack=0): no result, no timeout."""
    _rpc().notify(target_service_no, func_path, *args)


def current_caller() -> int:
    """Service number of the caller inside an executing handler (0 outside)."""
    return _current_caller()
