"""Config loading: JSON5, secrets, inheritance, fail-fast validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from pyline.config import (
    ConfigError,
    load_project_settings,
    load_server_registry,
    load_table_defs,
)
from pyline.config.secrets import resolve_secret


class TestSecrets:
    def test_env_reference(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_SECRET", "s3cret")
        assert resolve_secret("$env:MY_SECRET") == "s3cret"

    def test_env_missing_fails_fast(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NOPE", raising=False)
        with pytest.raises(ConfigError, match="not set"):
            resolve_secret("$env:NOPE")

    def test_inline_plaintext_rejected(self) -> None:
        with pytest.raises(ConfigError, match="reference"):
            resolve_secret("hunter2")

    def test_plain_prefix_allowed(self) -> None:
        assert resolve_secret("$plain:dev-token") == "dev-token"

    def test_file_reference(self, tmp_path: Path) -> None:
        (tmp_path / "secrets.json").write_text('{"k": "v"}', encoding="utf-8")
        assert resolve_secret("$file:k", config_dir=tmp_path / "cfg") == "v"

    def test_file_missing_key(self, tmp_path: Path) -> None:
        (tmp_path / "secrets.json").write_text('{"k": "v"}', encoding="utf-8")
        with pytest.raises(ConfigError):
            resolve_secret("$file:absent", config_dir=tmp_path / "cfg")


class TestProjectSettings:
    def test_load_valid(self, config_dir: Path) -> None:
        settings = load_project_settings(config_dir)
        assert settings.project == "test-game"
        assert settings.socket.token == "unit-test-token"
        assert settings.mysql.password == "test"

    def test_unknown_key_fails(self, config_dir: Path) -> None:
        text = (config_dir / "project.json5").read_text(encoding="utf-8")
        (config_dir / "project.json5").write_text(
            text.replace('"project"', '"projekt"'), encoding="utf-8"
        )
        with pytest.raises(ConfigError, match=r"projekt|invalid"):
            load_project_settings(config_dir)

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="not found"):
            load_project_settings(tmp_path)

    def test_syntax_error_names_file(self, config_dir: Path) -> None:
        (config_dir / "project.json5").write_text("{ broken", encoding="utf-8")
        with pytest.raises(ConfigError, match="syntax error"):
            load_project_settings(config_dir)


class TestServerRegistry:
    def test_base_inheritance(self, config_dir: Path) -> None:
        registry = load_server_registry(config_dir)
        entry = registry.entry(10001)
        assert entry.name == "dev"
        assert entry.use_mysql is True  # inherited from base
        assert entry.sub_process == ()

    def test_proxy_list(self, config_dir: Path) -> None:
        registry = load_server_registry(config_dir)
        assert registry.proxy_list() == [10009]

    def test_advertise_ip_index(self, config_dir: Path) -> None:
        registry = load_server_registry(config_dir)
        assert registry.by_advertise_ip("127.0.0.1") == 10001

    def test_duplicate_ip_rejected(self, config_dir: Path) -> None:
        (config_dir / "servers.json5").write_text(
            """
{
    "1": {"name": "a", "advertise_ip": "10.0.0.1"},
    "2": {"name": "b", "advertise_ip": "10.0.0.1"},
}
""",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="duplicate advertise_ip"):
            load_server_registry(config_dir)

    def test_unknown_base_rejected(self, config_dir: Path) -> None:
        (config_dir / "servers.json5").write_text(
            '{ "5": {"base": "ghost", "name": "x", "advertise_ip": "10.0.0.5"} }',
            encoding="utf-8",
        )
        with pytest.raises(ConfigError, match="unknown base"):
            load_server_registry(config_dir)

    def test_missing_advertise_ip_rejected(self, config_dir: Path) -> None:
        (config_dir / "servers.json5").write_text('{ "5": {"name": "x"} }', encoding="utf-8")
        with pytest.raises(ConfigError):
            load_server_registry(config_dir)


class TestTables:
    def test_load(self, config_dir: Path) -> None:
        tables = load_table_defs(config_dir)
        assert "tbl_player" in tables
        assert tables["tbl_player"].fields["id"].primary is True
