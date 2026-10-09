"""Shared server-to-server secret resolution and HMAC helpers.

Lives below every consumer (ipc, proxy) so both planes -- the ZMQ bus and the
TCP proxy mesh -- derive the SAME inter-server token without import cycles,
and emit the fallback warning exactly once per process (F-73 lineage).
"""

from __future__ import annotations

import hashlib
import hmac
import logging

from pyline.core.context import Context

logger = logging.getLogger(__name__)

#: Length of the per-connection challenge nonces (bytes).
NONCE_LEN = 16

_inter_token_warned = False


def inter_token(ctx: Context) -> str:
    """Server-to-server token; falls back to the client token (F-16).

    The fallback keeps single-token deployments working but widens the blast
    radius of a client-token leak to the inter-server plane -- warn so ops
    can see it in the log instead of discovering it during an incident.

    F-73: the warning is emitted once per process. ``inter_token`` is called
    on every reconnect (and every server-side accept), so a flapping proxy
    link used to re-log the same configuration fact on every attempt --
    once is informative, every second is log spam that buries real events.
    """
    s = ctx.settings.socket
    global _inter_token_warned
    if s.inter_token is None:
        if not _inter_token_warned:
            _inter_token_warned = True
            logger.warning(
                "socket.inter_token not set; server-to-server links reuse the CLIENT "
                "token -- configure a separate $env: reference for production"
            )
        return s.token.get_secret_value()
    return s.inter_token.get_secret_value()


def hmac_digest(token: str, *chunks: bytes) -> bytes:
    """HMAC-SHA256 over the concatenation of ``chunks`` keyed by ``token``.

    Both the TCP handshake and the bus handshake hash per-connection random
    nonces with this, so the token itself never crosses any wire and a
    captured digest is bound to one connection's nonce (fresh every time).
    """
    mac = hmac.new(token.encode("utf-8"), digestmod=hashlib.sha256)
    for chunk in chunks:
        mac.update(chunk)
    return mac.digest()


def digest_matches(expected: bytes, claimed: bytes) -> bool:
    """Constant-time digest comparison (the F-22 discipline, on digests)."""
    return hmac.compare_digest(expected, claimed)
