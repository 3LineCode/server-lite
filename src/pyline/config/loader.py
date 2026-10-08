"""Config file loading: JSON5 parsing, secret resolution, server inheritance."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import json5
from pydantic import ValidationError

from pyline.config.errors import ConfigError
from pyline.config.models import (
    ProjectSettings,
    ServerEntry,
    ServerRegistry,
    TableDef,
)
from pyline.config.secrets import resolve_secret

# Paths (section, key) inside project.json5 that hold secret references.
_SECRET_PATHS: tuple[tuple[str, str], ...] = (
    ("socket", "token"),
    ("socket", "inter_token"),
    ("mysql", "password"),
    ("redis", "password"),
)


def load_json5(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc
    try:
        data = json5.loads(text)
    except ValueError as exc:
        raise ConfigError(f"syntax error in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be an object")
    return data


def _resolve_secrets(data: dict[str, Any], config_dir: Path) -> None:
    for section, key in _SECRET_PATHS:
        value = data.get(section, {}).get(key)
        if isinstance(value, str):
            data[section][key] = resolve_secret(value, config_dir=config_dir)


def load_project_settings(config_dir: Path) -> ProjectSettings:
    raw = load_json5(config_dir / "project.json5")
    _resolve_secrets(raw, config_dir)
    try:
        return ProjectSettings.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"{config_dir / 'project.json5'}: invalid: {exc}") from exc


def load_server_registry(config_dir: Path) -> ServerRegistry:
    raw = load_json5(config_dir / "servers.json5")
    bases: dict[str, dict[str, Any]] = {}
    servers: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            raise ConfigError(f"servers.json5: entry {key!r} must be an object")
        if key.isdigit():
            servers[key] = dict(value)
        else:
            bases[key] = dict(value)

    entries: dict[int, ServerEntry] = {}
    for key, server in servers.items():
        server_no = int(key)
        if server_no in entries:
            raise ConfigError(
                f"servers.json5: duplicate server number {server_no} "
                f"(keys {key!r} collide after int conversion)"
            )
        merged: dict[str, Any] = {}
        base_name = server.pop("base", None)
        if base_name is not None:
            base = bases.get(base_name)
            if base is None:
                raise ConfigError(
                    f"servers.json5: server {key} references unknown base {base_name!r}"
                )
            if "base" in base:
                raise ConfigError(
                    f"servers.json5: base {base_name!r} must not itself have a 'base'; "
                    "inheritance is single-level by design"
                )
            merged.update(base)
        merged.update(server)
        merged["server_no"] = server_no
        try:
            entries[server_no] = ServerEntry.model_validate(merged)
        except ValidationError as exc:
            raise ConfigError(f"servers.json5: invalid server {key}: {exc}") from exc
    if not entries:
        raise ConfigError("servers.json5: no server entries (numeric keys) defined")
    return ServerRegistry(entries)


def load_table_defs(config_dir: Path) -> dict[str, TableDef]:
    raw = load_json5(config_dir / "tables.json5")
    defs: dict[str, TableDef] = {}
    for name, body in raw.items():
        try:
            defs[name] = TableDef.model_validate(body)
        except ValidationError as exc:
            raise ConfigError(f"tables.json5: invalid table {name}: {exc}") from exc
    return defs
