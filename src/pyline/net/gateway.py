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

    def dispatch(self, flag: str, payload: bytes) -> None:
        """Dispatch one inbound message.

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
        network.handle_message(flag, payload)
