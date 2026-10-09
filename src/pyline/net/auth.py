"""Shared server-to-server secret resolution and HMAC helpers.

Lives below every consumer (ipc, proxy) so both planes -- the ZMQ bus and the
TCP proxy mesh -- derive the SAME inter-server token without import cycles,
and emit the fallback warning exactly once per process (F-73 lineage).
"""

from __future__ import annotations

import hashlib
import hmac
import logging

from pyline.config.errors import ConfigError
from pyline.core.context import Context

logger = logging.getLogger(__name__)

#: Length of the per-connection challenge nonces (bytes).
NONCE_LEN = 16

_inter_token_warned = False


def inter_token(ctx: Context) -> str:
    """Server-to-server token; falls back to the client token (F-16).

    The fallback keeps single-token DEVELOPMENT deployments working but
    widens the blast radius of a client-token leak to the inter-server
    plane -- in production that fallback is the vulnerability, not the
    convenience: every game client would hold a credential that fully
    impersonates servers (bus + proxy + the full-SQL RPC surface), so
    ``srv_type=production`` refuses to boot without an explicit
    ``socket.inter_token`` (F-163). Develop mode keeps the once-per-process
    warning (F-73).

    F-73 lineage: the warning is emitted once per process. ``inter_token``
    is called on every reconnect (and every server-side accept), so a
    flapping proxy link used to re-log the same configuration fact on every
    attempt -- once is informative, every second is log spam that buries
    real events.
    """
    s = ctx.settings.socket
    global _inter_token_warned
    if s.inter_token is None:
        if ctx.settings.srv_type == "production":
            raise ConfigError(
                "socket.inter_token is required when srv_type=production: "
                "without it every game client holds a token that fully "
                "impersonates servers on the inter-server plane "
                "(bus, proxy, full SQL passthrough)"
            )
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
