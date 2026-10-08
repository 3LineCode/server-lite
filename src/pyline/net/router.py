"""Message router: decides how a message reaches a target service number.

Routing rules (explicit, replacing the prototype's scattered logic):

* target == own service number          -> local gateway dispatch;
* target on the same physical server    -> ZeroMQ bus (via the main ROUTER);
* target on another physical server     -> proxy connection, **main process
  only** -- a sub-process attempting a cross-server send fails loudly
  (the prototype silently relied on this rule; now it is enforced here).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyline.net.ipc import main_service_no

if TYPE_CHECKING:
    from pyline.core.context import Context
    from pyline.net.gateway import ProtocolGateway
    from pyline.net.ipc import ZmqBus
    from pyline.net.proxy import ProxyClient, ProxyServer

logger = logging.getLogger(__name__)


class CrossServerError(RuntimeError):
    """A sub-process tried to send cross-server directly."""


class NoProxyAvailableError(ConnectionError):
    pass


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
    ) -> None:
        """Route one message toward ``target_service_no``.

        ``from_service`` is the validated ORIGINAL sender when relaying a
        message that arrived from elsewhere (proxy ``@fwd`` delivery);
        ``None`` means this process is the sender (F-39).
        """
        if target_service_no == self._ctx.service_no:
            origin = self._ctx.service_no if from_service is None else from_service
            self._gateway.dispatch(flag, payload, origin)
            return
        if main_service_no(target_service_no) == self._ctx.main_service_no:
            self._bus.send(target_service_no, flag, payload)
            return
        if self._ctx.is_sub_process:
            raise CrossServerError(
                f"sub-process {self._ctx.service_no} cannot send cross-server to "
                f"{target_service_no}; relay through the main process instead"
            )
        # On a proxy machine, deliver directly from the local node table first.
        origin = self._ctx.service_no if from_service is None else from_service
        if self._proxy_server is not None and self._proxy_server.forward(
            target_service_no, origin, flag, payload
        ):
            return
        if self._proxy_client is None:
            logger.error("cross-server send to %d dropped: no proxy client", target_service_no)
            return
        try:
            self._proxy_client.send_to_service(target_service_no, flag, payload)
        except NoProxyAvailableError:
            logger.error("cross-server send to %d dropped: no proxy connected", target_service_no)
