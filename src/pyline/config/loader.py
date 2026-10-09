"""Config file loading: JSON5 parsing, secret resolution, server inheritance."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, get_args

import json5
from pydantic import BaseModel, SecretStr, ValidationError

from pyline.config.errors import ConfigError
from pyline.config.models import (
    ProjectSettings,
    ServerEntry,
    ServerRegistry,
    TableDef,
)
from pyline.config.secrets import resolve_secret


def load_json5(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc
    try:
        data = json5.loads(text)
    except ValueError as exc:
        raise ConfigError(f"syntax error in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be an object")
    return data


def _secret_paths(
    model: type[BaseModel], prefix: tuple[str, ...] = ()
) -> Iterator[tuple[str, ...]]:
    """Paths of SecretStr-annotated fields, derived from the models (F-53).

    The old hand-maintained list could silently miss a newly added secret
    field -- inline plaintext would then pass validation unnoticed. Walking
    the model annotations makes that impossible: every SecretStr field is a
    secret, by construction.
    """
    for name, info in model.model_fields.items():
        path = (*prefix, name)
        annotations = [info.annotation, *get_args(info.annotation)]
        if any(a is SecretStr for a in annotations):
            yield path
            continue
        for annotation in annotations:
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                yield from _secret_paths(annotation, path)
                break


def _resolve_secrets(data: dict[str, Any], config_dir: Path) -> None:
    for path in _secret_paths(ProjectSettings):
        section: Any = data
        for key in path[:-1]:
            section = section.get(key) if isinstance(section, dict) else None
        if isinstance(section, dict):
            value = section.get(path[-1])
            if isinstance(value, str):
                section[path[-1]] = resolve_secret(value, config_dir=config_dir)


def load_project_settings(config_dir: Path) -> ProjectSettings:
    raw = load_json5(config_dir / "project.json5")
    _resolve_secrets(raw, config_dir)
    try:
        settings = ProjectSettings.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"{config_dir / 'project.json5'}: invalid: {exc}") from exc
    # mysql.migrations_dir is written relative to the PROJECT root (the
    # config dir's parent); resolve once here so every process interprets it
    # identically regardless of its working directory.
    migrations = settings.mysql.migrations_dir
    if migrations is not None and not Path(migrations).is_absolute():
        settings = settings.model_copy(
            update={
                "mysql": settings.mysql.model_copy(
                    update={"migrations_dir": str((config_dir.parent / migrations).resolve())}
                )
            }
        )
    return settings


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
