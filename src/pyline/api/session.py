"""Client session facade (F-225): the connection<->player toolkit.

Business code sees three primitives:

* ``session.current()`` -- the client Connection this handler's frame
  arrived on (None on the RPC/bus faces and outside dispatch);
* ``session.send(conn_id, flag, payload)`` / ``session.kick(conn_id)`` --
  push a frame to, or close, a specific client from anywhere (timers, RPC
  handlers, other processes' relayed requests);
* ``ClientConnectedEvent``/``ClientDisconnectedEvent`` on the event bus --
  the hooks a connection<->player map rides on.

Login binding stays deliberately in business hands: the registry maps
conn_id -> Connection only. A "which player is this" mapping is game data
(player id -> conn_id after a login payload), not transport state.
"""

from __future__ import annotations

import asyncio
import logging

from pyline import api
from pyline.net.connection import Connection, ConnectionClosedError
from pyline.net.session import ClientSessionRegistry, current_connection

logger = logging.getLogger(__name__)


def _registry() -> ClientSessionRegistry:
    return api.ctx().service("client_sessions", ClientSessionRegistry)


def current() -> Connection | None:
    """The client connection the currently running handler was dispatched
    for, else None (bus/RPC face, timer, outside dispatch)."""
    return current_connection()


def get(conn_id: int) -> Connection | None:
    """Look up a registered connection by id (None once disconnected)."""
    return _registry().get(conn_id)


def count() -> int:
    """Number of live registered client connections."""
    return _registry().count()


def all_ids() -> list[int]:
    """Ids of every live registered client connection (broadcast source)."""
    return [conn.conn_id for conn in _registry().all()]


def send(conn_id: int, flag: str, payload: bytes) -> bool:
    """Queue one frame to a client; False if the connection is gone.

    Raises ConnectionClosedError when the connection exists but its send
    path is closed/overflowing (the same contract as Connection.send_message
    -- a slow consumer closes the link, that fact should not be swallowed).
    """
    conn = _registry().get(conn_id)
    if conn is None:
        return False
    conn.send_message(flag, payload)
    return True


def kick(conn_id: int, reason: str = "kicked") -> bool:
    """Close a client connection (logout, ban, operator kick). False if it
    is already gone."""
    conn = _registry().get(conn_id)
    if conn is None:
        return False
    asyncio.get_running_loop().create_task(conn.close(reason))
    return True


def inflight(conn_id: int) -> int:
    """Handler tasks currently running for this connection (F-224 gauge)."""
    conn = _registry().get(conn_id)
    return conn.inflight if conn is not None else 0


__all__ = [
    "Connection",
    "ConnectionClosedError",
    "all_ids",
    "count",
    "current",
    "get",
    "inflight",
    "kick",
    "send",
]
