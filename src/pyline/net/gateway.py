"""Protocol gateway: routes inbound messages to registered networks by flag."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyline.net.network import Network

logger = logging.getLogger(__name__)


class ProtocolGateway:
    def __init__(self) -> None:
        self._networks: dict[str, Network] = {}
        self.unknown_dispatches = 0

    def register(self, network: Network) -> None:
        """Register a network under its ``flag``; duplicates are a config error."""
        flag = type(network).flag
        if not flag:
            raise ValueError(f"network {type(network).__name__} has empty flag")
        existing = self._networks.get(flag)
        if existing is not None:
            raise ValueError(
                f"duplicate protocol flag {flag!r}: {type(existing).__module__}."
                f"{type(existing).__qualname__} vs "
                f"{type(network).__module__}.{type(network).__qualname__}"
            )
        self._networks[flag] = network

    def get(self, flag: str) -> Network | None:
        return self._networks.get(flag)

    def rebind_module(self, module_name: str) -> int:
        """Ask every network defined in ``module_name`` to re-register its
        handlers after that module was hot-reloaded (F-30); returns the
        count of networks consulted."""
        count = 0
        for network in self._networks.values():
            if type(network).__module__ == module_name:
                network.rebind_handlers()
                count += 1
        if count:
            logger.info("rebound handlers for %d network(s) in %s", count, module_name)
        return count

    def dispatch(self, flag: str, payload: bytes, from_service: int = 0) -> None:
        """Dispatch one inbound message.

        ``from_service`` is the validated origin service number (F-39): the
        ZMQ ROUTER only passes through values that matched the sender's
        socket identity, and the proxy stamps the original sender into the
        ``@fwd`` envelope. ``0`` means "origin unknown" -- transports that
        cannot establish an origin (e.g. the untrusted client listener)
        dispatch with 0 and trust each network to decide what it accepts.

        Unknown protocols are counted and dropped, never raised: a single bad
        flag must not be able to kill the dispatch path.

        For *registered* networks this may raise out of the handler; the
        transport edge (TCP Connection / ZMQ bus) isolates handler exceptions
        so one bad frame drops the frame, not the connection (F-13).
        """
        network = self._networks.get(flag)
        if network is None:
            self.unknown_dispatches += 1
            logger.warning(
                "no network registered for flag %r (dropped, total=%d)",
                flag,
                self.unknown_dispatches,
            )
            return
        network.handle_message(flag, payload, from_service)
