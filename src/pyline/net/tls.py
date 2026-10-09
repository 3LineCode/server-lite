"""TLS context construction for the TCP planes (F-187/F-188).

The deployment trust model (docs/deployment.md) draws a hard line: the HMAC
handshakes AUTHENTICATE (the token never crosses the wire, digests cannot be
replayed) but nothing ENCRYPTS -- payloads on the client listener and the
proxy mesh are readable by anything on the path.  TLS closes that gap:
server certificates on the client listener, mutual TLS (each machine's cert
is its identity) on the proxy plane.

Design notes:

* Fail-fast at boot, not per-connection: an unreadable cert/key or a client
  context without a CA raises ConfigError naming the file -- a server that
  silently fell back to plaintext would be worse than one that refuses to
  start.
* Verification is never disabled.  A client context REQUIRES ``ca_file``:
  "encrypted but unverified" is security theater and is not offered.
* The same :class:`~pyline.config.models.TlsSettings` serves both roles:
  ``cert_file``/``key_file`` are this machine's identity (server cert on its
  listener, client cert when dialing), ``ca_file`` verifies the peer.
"""

from __future__ import annotations

import logging
import os
import ssl
from pathlib import Path

from pyline.config.errors import ConfigError
from pyline.config.models import TlsSettings

logger = logging.getLogger(__name__)


def _resolve(path: str, what: str) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        # Loader resolves project-relative paths against the project root
        # (the config dir's parent). Contexts built outside the loader
        # (tests, embedding) fall back to cwd and then to the project root
        # implied by $PYLINE_CONFIG_DIR (F-220: main() exports it, so a
        # server started from another working directory resolves its
        # certificate the same way the loader would have).
        candidates = [Path.cwd() / resolved]
        config_dir = os.environ.get("PYLINE_CONFIG_DIR")
        if config_dir:
            candidates.append((Path(config_dir).parent / resolved).resolve())
        for candidate in candidates:
            if candidate.is_file():
                return candidate.resolve()
        resolved = candidates[0].resolve()
    if not resolved.is_file():
        raise ConfigError(f"tls.{what}: certificate file {resolved} does not exist")
    return resolved


def _apply_floor(context: ssl.SSLContext) -> None:
    """F-220: pin the minimum protocol version instead of trusting the

    interpreter's OpenSSL defaults -- "secure defaults today" is a property
    of the build, not of this code."""
    context.minimum_version = ssl.TLSVersion.TLSv1_2


def build_server_context(settings: TlsSettings) -> ssl.SSLContext:
    """Server-side context for a listener (client listener, proxy server)."""
    if settings.require_client_cert and settings.ca_file is None:
        raise ConfigError(
            "tls.require_client_cert needs tls.ca_file (the CA that signs the "
            "certificates you want to accept)"
        )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    _apply_floor(context)
    try:
        context.load_cert_chain(
            str(_resolve(settings.cert_file, "cert_file")),
            str(_resolve(settings.key_file, "key_file")),
        )
    except ssl.SSLError as exc:
        raise ConfigError(f"tls.cert_file/key_file could not be loaded: {exc}") from exc
    if settings.require_client_cert:
        assert settings.ca_file is not None  # guarded above
        try:
            context.load_verify_locations(str(_resolve(settings.ca_file, "ca_file")))
        except ssl.SSLError as exc:
            raise ConfigError(f"tls.ca_file could not be loaded: {exc}") from exc
        context.verify_mode = ssl.CERT_REQUIRED
        # The client cert is a machine identity, not a hostnames proof: the
        # proxy plane dials advertise_ips that may not match CN/SAN.
        context.check_hostname = False
    else:
        # Anonymous clients (game clients): the challenge-response handshake
        # inside the tunnel still authenticates them; TLS here is
        # confidentiality + server identity.
        context.verify_mode = ssl.CERT_NONE
    logger.info(
        "tls server context ready (cert=%s, client_cert=%s)",
        settings.cert_file,
        "required" if settings.require_client_cert else "not required",
    )
    return context


def build_client_context(settings: TlsSettings) -> ssl.SSLContext:
    """Client-side context for dialing a TLS peer (proxy links).

     The server certificate is ALWAYS verified against ``ca_file`` -- without
     it the client would accept any certificate, and an active attacker could
    relay
     the (authenticated but plaintext-to-them) traffic. ``cert_file``/
     ``key_file`` become the client certificate for mutual-TLS servers.
    """
    if settings.ca_file is None:
        raise ConfigError(
            "tls.ca_file is required to dial a TLS peer: without a CA to verify "
            "the server certificate against, TLS would not authenticate the "
            "server (encrypt-only is not offered)"
        )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    _apply_floor(context)
    try:
        context.load_verify_locations(str(_resolve(settings.ca_file, "ca_file")))
    except ssl.SSLError as exc:
        raise ConfigError(f"tls.ca_file could not be loaded: {exc}") from exc
    context.verify_mode = ssl.CERT_REQUIRED
    if settings.verify_hostname:
        # F-220: with per-host certificates (SANs matching the dial target)
        # the cert proves WHICH machine it is, not just "one of ours". The
        # dial target (host/IP) is supplied by asyncio/openssl as
        # server_hostname automatically.
        context.check_hostname = True
    else:
        # Proxy machines dial each other's advertise_ip; the classic
        # one-cert-per-machine model does not bind those into SANs, so
        # name-checking would reject legitimate links. Deployment doc states
        # the pinning story and the verify_hostname upgrade path.
        context.check_hostname = False
    try:
        context.load_cert_chain(
            str(_resolve(settings.cert_file, "cert_file")),
            str(_resolve(settings.key_file, "key_file")),
        )
    except ssl.SSLError as exc:
        raise ConfigError(f"tls.cert_file/key_file could not be loaded: {exc}") from exc
    return context


__all__ = ["build_client_context", "build_server_context"]
