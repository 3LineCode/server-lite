"""Networking: frame protocol, connections, gateway, RPC, ZeroMQ bus, proxy."""

from pyline.net.connection import (
    Connection,
    ConnectionClosedError,
    close_server,
    open_connection,
    serve,
)
from pyline.net.gateway import ProtocolGateway
from pyline.net.ipc import ZmqBus, main_service_no, service_no_bytes
from pyline.net.network import Network, pack_call, unpack_call
from pyline.net.protocol import (
    Frame,
    FrameDecoder,
    ProtocolError,
    encode_message,
)
from pyline.net.proxy import ProxyClient, ProxyServer
from pyline.net.router import CrossServerError, MessageRouter, NoProxyAvailableError
from pyline.net.rpc import (
    RpcError,
    RpcManager,
    RpcRemoteError,
    RpcTimeoutError,
    RpcUnknownFunctionError,
)

__all__ = [
    "Connection",
    "ConnectionClosedError",
    "CrossServerError",
    "Frame",
    "FrameDecoder",
    "MessageRouter",
    "Network",
    "NoProxyAvailableError",
    "ProtocolError",
    "ProtocolGateway",
    "ProxyClient",
    "ProxyServer",
    "RpcError",
    "RpcManager",
    "RpcRemoteError",
    "RpcTimeoutError",
    "RpcUnknownFunctionError",
    "ZmqBus",
    "close_server",
    "encode_message",
    "main_service_no",
    "open_connection",
    "pack_call",
    "serve",
    "service_no_bytes",
    "unpack_call",
]
