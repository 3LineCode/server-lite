"""Secret resolution.

Secret-bearing config fields must be references, never inline plaintext:

- ``$env:VAR_NAME``    -- read from the environment variable ``VAR_NAME``.
- ``$file:key``        -- read ``key`` from the local secrets file
                          (``secrets.json`` next to the config dir, git-ignored).
- ``$plain:value``     -- explicit inline value. Allowed only for local dev;
                          a warning is logged so plaintext never sneaks into a
                          reviewed config unnoticed.

Anything else in a secret field is a :class:`ConfigError` at load time.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from pyline.config.errors import ConfigError

logger = logging.getLogger(__name__)

_SECRETS_FILE = "secrets.json"


def resolve_secret(value: str, *, config_dir: Path | None = None) -> str:
    """Resolve a secret reference into its concrete value."""
    if not value.startswith("$"):
        raise ConfigError(
            f"secret field must be a reference ($env:/$file:/$plain:), got inline "
            f"value of {len(value)} chars; use $env:VAR or $file:key instead"
        )
    kind, _, ref = value.partition(":")
    if kind == "$plain":
        # Empty $plain: means "explicitly no secret" (e.g. no-auth Redis),
        # matching the RedisSettings docs; all other kinds require a ref.
        # Warn here -- the match below is unreachable for $plain and the
        # docstring promises the warning is never skipped.
        logger.warning("inline plaintext secret in use ($plain:) -- dev only!")
        return ref
    if not ref:
        raise ConfigError(f"malformed secret reference: {value!r}")
    match kind:
        case "$env":
            resolved = os.environ.get(ref)
            if resolved is None:
                raise ConfigError(
                    f"environment variable {ref!r} (secret) is not set; "
                    "export it before starting the server"
                )
            return resolved
        case "$file":
            secrets_path = (config_dir or Path(".")).parent / _SECRETS_FILE
            if not secrets_path.exists():
                raise ConfigError(f"secrets file not found: {secrets_path} (expected key {ref!r})")
            try:
                data = json.loads(secrets_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ConfigError(f"cannot read secrets file {secrets_path}: {exc}") from exc
            if ref not in data:
                raise ConfigError(f"secrets file {secrets_path} has no key {ref!r}")
            return str(data[ref])
        case _:
            raise ConfigError(f"unknown secret reference kind {kind!r} in {value!r}")
