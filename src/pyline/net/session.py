"""Client session layer: connection identity, registry and dispatch context.

The client listener used to be connection-blind in three ways (F-225):

* inbound frames carried no link to the Connection they arrived on, so a
  handler could not tell WHICH client sent a frame -- before login the only
  identifier was a client-supplied id inside the payload, i.e. forgeable;
* there was a ``ClientConnectedEvent`` but no disconnect counterpart, so
  connection<->player maps leaked entries forever and "kick on logout" had
  no hook to ride;
* the only send path was a reply in the handler's own stack -- no way to
  push a frame to connection 7 from a timer, an RPC handler, or anywhere
  else.

This module owns the per-process registry (``conn_id -> Connection``) and
the ``current_connection`` contextvar. The Connection read loop sets the
contextvar around every application dispatch, and asyncio task creation
copies the context, so plain-network handler tasks (sync or async) see the
connection they were dispatched for -- no handler signature changes. The
RPC/bus faces never set it (their frames arrive through the gateway from
the bus, not a client listener), so ``current()`` is None there by
construction.
"""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyline.net.connection import Connection

logger = logging.getLogger(__name__)

_current_connection: contextvars.ContextVar[Connection | None] = contextvars.ContextVar(
    "pyline_current_connection", default=None
)


def current_connection() -> Connection | None:
    """The client Connection this handler's frame arrived on, else None.

    Valid inside plain-network handlers (and anything they await on the same
    task); None on the RPC/bus faces and outside dispatch."""
    return _current_connection.get()


def set_current_connection(conn: Connection | None) -> contextvars.Token[Connection | None]:
    """Bind/unbind the dispatch context (used by the Connection read loop)."""
    return _current_connection.set(conn)


class ClientSessionRegistry:
    """Per-process map of live client connections (F-225).

    ``register`` assigns the monotonic connection id; the runtime's close
    hook calls ``unregister`` exactly once per connection (Connection.close
    runs hooks once), so the map cannot leak dead entries. Ids are not
    reused -- a reconnecting client gets a fresh id, so a stale id in
    business state can never alias a NEW connection (the classic
    kick-the-wrong-player race).
    """

    def __init__(self) -> None:
        self._connections: dict[int, Connection] = {}
        self._seq = itertools.count(1)

    def register(self, conn: Connection) -> int:
        conn_id = next(self._seq)
        self._connections[conn_id] = conn
        conn.conn_id = conn_id
        return conn_id

    def unregister(self, conn: Connection) -> None:
        self._connections.pop(conn.conn_id, None)

    def get(self, conn_id: int) -> Connection | None:
        return self._connections.get(conn_id)

    def all(self) -> list[Connection]:
        return list(self._connections.values())

    def count(self) -> int:
        return len(self._connections)

    async def drain(self, timeout: float = 2.0) -> None:
        """Close every registered connection (teardown path).

        Each close bounds itself (Connection.close's own flush timeout), so
        the whole drain is bounded by ``timeout`` per wave; unbounded waits
        are exactly what teardown steps exist to prevent."""
        conns = list(self._connections.values())
        if not conns:
            return
        await asyncio.wait_for(
            asyncio.gather(
                *(conn.close("server shutdown") for conn in conns), return_exceptions=True
            ),
            timeout=timeout,
        )


__all__ = ["ClientSessionRegistry", "current_connection", "set_current_connection"]
