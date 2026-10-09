"""Message router: decides how a message reaches a target service number.

Routing rules (explicit, replacing the prototype's scattered logic):

* target == own service number          -> local gateway dispatch;
* target on the same physical server    -> ZeroMQ bus (via the main ROUTER),
  preserving the validated original sender when relaying (F-70);
* target on another physical server:
  - main process  -> proxy connection (direct node table first, else the
    configured proxy servers);
  - sub-process    -> ``@relay`` envelope via the local bus to the main
    process, which forwards it (F-70). The prototype silently relied on this
    relay without implementing it; the interim hard failure (CrossServerError)
    made every cross-server RPC from a sub-process impossible.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import msgpack

from pyline.net.ipc import main_service_no
from pyline.net.network import Network

if TYPE_CHECKING:
    from pyline.core.context import Context
    from pyline.net.gateway import ProtocolGateway
    from pyline.net.ipc import ZmqBus
    from pyline.net.proxy import ProxyClient, ProxyServer

logger = logging.getLogger(__name__)

#: Flag of the envelope a sub-process hands to its local main process when
#: the destination lives on another physical server (F-70). The payload is
#: the same 5-field ``@fwd`` envelope (``[target, from_service, flag,
#: payload, hops]``) so the proxy paths can forward it unchanged.
RELAY_FLAG = "@relay"


class CrossServerError(RuntimeError):
    """Historic: a sub-process tried to send cross-server directly.

    Retained for API compatibility (docs/migration-plan.md references it and
    ``pyline.net`` re-exports it). F-70 replaces the hard failure with the
    ``@relay`` path, so :meth:`MessageRouter.route` no longer raises it.
    """


class NoProxyAvailableError(ConnectionError):
    pass


class _RelayNetwork(Network):
    """Gateway endpoint for ``@relay`` envelopes (F-70, main process only).

    The payload is not a ``[sub, *args]`` network call but an ``@fwd``-style
    envelope, so ``handle_message`` is overridden instead of subscribing.
    """

    flag = RELAY_FLAG

    def __init__(self, gateway: ProtocolGateway, on_relay: Callable[[bytes, int], None]) -> None:
        super().__init__(gateway)
        self._on_relay = on_relay

    def handle_message(self, flag: str, payload: bytes, from_service: int = 0) -> None:
        # ``flag`` is RELAY_FLAG by construction; ``from_service`` is the
        # sending sub-process, validated against its ZMQ socket identity by
        # the bus ROUTER (F-39).
        self._on_relay(payload, from_service)


class MessageRouter:
    def __init__(
        self,
        ctx: Context,
        gateway: ProtocolGateway,
        bus: ZmqBus,
    ) -> None:
        self._ctx = ctx
        self._gateway = gateway
        self._bus = bus
        self._proxy_client: ProxyClient | None = None
        self._proxy_server: ProxyServer | None = None
        # F-70 observability: envelopes dropped for origin spoofing / passed on.
        self.relay_spoofed = 0
        self.relayed_messages = 0
        if ctx.is_main_process:
            # Only the main process can relay for its sub-processes (it alone
            # holds the cross-server proxy links). Registered here so every
            # composition (runtime or tests) gets the handler for free.
            _RelayNetwork(gateway, self._on_relay)

    def attach_proxy_client(self, client: ProxyClient) -> None:
        self._proxy_client = client

    def attach_proxy_server(self, server: ProxyServer) -> None:
        self._proxy_server = server

    def route(
        self,
        flag: str,
        payload: bytes,
        target_service_no: int,
        from_service: int | None = None,
        hops: int = 0,
        raise_on_drop: bool = False,
    ) -> None:
        """Route one message toward ``target_service_no``.

        ``from_service`` is the validated ORIGINAL sender when relaying a
        message that arrived from elsewhere (proxy ``@fwd`` delivery);
        ``None`` means this process is the sender (F-39).

        ``hops`` is the number of proxy hops already traversed when relaying
        an ``@fwd``; every onward proxy send stamps ``hops + 1`` so MAX_HOPS
        bounds loops that re-enter the router (a registry disagreement used
        to reset the counter to 0 at every ProxyClient hop).

        ``raise_on_drop`` propagates through every leg: a refused bus enqueue
        surfaces as ``BusOverflowError``, and a proxy leg with no reachable
        proxy surfaces as ``NoProxyAvailableError`` (F-145) -- callers
        awaiting a result fail fast instead of timing out. Proxy send
        failures themselves always raise (ConnectionClosedError)."""
        if target_service_no == self._ctx.service_no:
            origin = self._ctx.service_no if from_service is None else from_service
            self._gateway.dispatch(flag, payload, origin)
            return
        if main_service_no(target_service_no) == self._ctx.main_service_no:
            # F-70: forward the original sender -- the bus used to stamp this
            # process's own number unconditionally, so an RPC RESULT for a
            # cross-machine caller of a local sub-process left with the wrong
            # origin and the caller's F-40 target check rejected it (then the
            # 10s timeout fired).
            self._bus.send(
                target_service_no,
                flag,
                payload,
                from_service=from_service,
                raise_on_drop=raise_on_drop,
            )
            return
        if self._ctx.is_sub_process:
            # F-70: sub-processes have no proxy links; hand the message to the
            # local main process inside an @relay envelope (see below).
            self._relay_via_main(
                target_service_no, from_service, flag, payload, hops, raise_on_drop
            )
            return
        # On a proxy machine, deliver directly from the local node table first.
        origin = self._ctx.service_no if from_service is None else from_service
        if self._proxy_server is not None and self._proxy_server.forward(
            target_service_no, origin, flag, payload, hops
        ):
            return
        if self._proxy_client is None:
            logger.error("cross-server send to %d dropped: no proxy client", target_service_no)
            if raise_on_drop:
                raise NoProxyAvailableError(
                    f"no proxy client configured for cross-server send to {target_service_no}"
                )
            return
        try:
            self._proxy_client.send_to_service(
                target_service_no, flag, payload, from_service=origin, hops=hops
            )
        except NoProxyAvailableError:
            logger.error("cross-server send to %d dropped: no proxy connected", target_service_no)
            # F-145: a caller awaiting a result (raise_on_drop, an RPC CALL)
            # must fail fast here -- swallowing the error used to burn the
            # caller's full rpc timeout on a message that never left, exactly
            # the failure mode F-14 removed for the bus legs.
            if raise_on_drop:
                raise

    def _relay_via_main(
        self,
        target_service_no: int,
        from_service: int | None,
        flag: str,
        payload: bytes,
        hops: int = 0,
        raise_on_drop: bool = False,
    ) -> None:
        """F-70: send a cross-server message through the local main process.

        The envelope keeps the original sender as its origin; the bus frames
        themselves carry the sub-process's own identity (F-39 requires
        identity == from on the wire), which is exactly what the main-side
        handler needs to authenticate the relay request.  The hop count
        rides along unchanged -- relaying via the local main is not a proxy
        hop, and the main's onward send keeps accumulating it.
        """
        origin = self._ctx.service_no if from_service is None else from_service
        envelope = msgpack.packb(
            [target_service_no, origin, flag, payload, hops], use_bin_type=True
        )
        self._bus.send(self._ctx.main_service_no, RELAY_FLAG, envelope, raise_on_drop=raise_on_drop)

    def _on_relay(self, payload: bytes, from_service: int) -> None:
        """Handle an ``@relay`` envelope from a local sub-process (F-70).

        ``from_service`` is the sending sub-process, validated against its
        ZMQ identity by the bus (F-39). The envelope's inner origin is bound
        the same way the proxy binds ``@fwd`` origins at the first hop
        (F-48): a sub-process may only emit cross-server traffic whose
        origin belongs to THIS machine's mesh -- its own identity, or an
        origin this main itself validated and routed to it. A claim of some
        other machine's process is spoofing and is dropped, so the first-hop
        origin-binding semantics survive the extra indirection.
        """
        # Lazy import: proxy.py imports this module (router) at top level.
        from pyline.net.proxy import parse_forward

        try:
            target, origin, inner_flag, inner_payload, _hops = parse_forward(payload)
        except (ValueError, msgpack.exceptions.ExtraData, RecursionError):
            self.relay_spoofed += 1
            logger.warning("malformed @relay envelope from service %d; dropped", from_service)
            return
        if from_service == 0 or main_service_no(from_service) != self._ctx.main_service_no:
            # Not reachable through the local bus (F-39 validates the sender
            # identity) -- defensive only.
            self.relay_spoofed += 1
            logger.error("@relay from untrusted sender %d; dropped", from_service)
            return
        if origin != 0 and main_service_no(origin) != self._ctx.main_service_no:
            self.relay_spoofed += 1
            logger.error(
                "@relay from sub-process %d claims origin %d of another machine; "
                "dropped (spoofed=%d)",
                from_service,
                origin,
                self.relay_spoofed,
            )
            return
        self.relayed_messages += 1
        self.route(inner_flag, inner_payload, target, from_service=origin, hops=_hops)
