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
        assert settings.socket.token.get_secret_value() == "unit-test-token"
        assert settings.mysql.password.get_secret_value() == "test"

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
        with pytest.raises(ConfigError, match="duplicate advertise_ip"):
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


class TestMySQLSettingsHardening:
    def test_isolation_level_whitelist(self) -> None:
        from pyline.config.models import MySQLSettings

        good = MySQLSettings(
            user="root",
            password="$plain:x",
            db_name="d",
            isolation_level="READ UNCOMMITTED",
        )
        assert good.isolation_level == "READ UNCOMMITTED"
        with pytest.raises(Exception, match="isolation_level"):
            MySQLSettings(
                user="root",
                password="$plain:x",
                db_name="d",
                isolation_level="SERIALIZABLE; DROP TABLE x",
            )

    def test_table_field_default_literal(self) -> None:
        from pyline.config.models import TableFieldDef

        assert TableFieldDef(type="BIGINT", default="7").default == "7"
        field = TableFieldDef(type="VARCHAR(8)", default="'a'")
        assert field.default == "'a'"


class TestLoaderHardeningF26:
    def test_duplicate_server_number_rejected(self, config_dir) -> None:
        (config_dir / "servers.json5").write_text(
            '{"1": {"name": "a", "advertise_ip": "10.0.0.1"}, '
            '"0001": {"name": "b", "advertise_ip": "10.0.0.2"}}',
            encoding="utf-8",
        )
        with pytest.raises(Exception, match="duplicate server number 1"):
            load_server_registry(config_dir)

    def test_plain_empty_allowed_for_noauth(self) -> None:
        assert resolve_secret("$plain:") == ""

    def test_inter_token_resolved_from_secrets(self, config_dir) -> None:
        (config_dir / "project.json5").write_text(
            (config_dir / "project.json5")
            .read_text(encoding="utf-8")
            .replace(
                '"token": "$plain:unit-test-token",',
                '"token": "$plain:unit-test-token",\n        "inter_token": "$plain:inner-token",',
            ),
            encoding="utf-8",
        )
        settings = load_project_settings(config_dir)
        assert settings.socket.inter_token.get_secret_value() == "inner-token"


class TestProcessPortF84:
    def test_missing_server_port_raises_with_guidance(self) -> None:
        """F-84: ``server_port or 0`` handed callers port 0 -- listeners
        bound an OS-assigned port nobody could reach, peers dialed 0."""
        from pyline.config.models import ServerEntry

        entry = ServerEntry(server_no=10009, name="proxy", advertise_ip="10.0.0.9", is_proxy=True)
        with pytest.raises(ConfigError, match="server_port"):
            entry.process_port(0)

    def test_configured_ports_keep_offset_scheme(self) -> None:
        from pyline.config.models import ServerEntry

        low = ServerEntry(server_no=1, name="a", advertise_ip="10.0.0.1", server_port=2520)
        assert low.process_port(0) == 2520
        assert low.process_port(2) == 4520  # +1000 per index at low bases
        high = ServerEntry(server_no=2, name="b", advertise_ip="10.0.0.2", server_port=25200)
        assert high.process_port(1) == 35200  # +10000 per index above 10000


class TestClientListenPortF84Twin:
    def test_missing_client_port_raises_with_guidance(self) -> None:
        """F-84 twin: ``client_port or 0`` used to return 0/1000/... for
        entries without a client_port; the main process bound a port no
        client could dial (the listener is bound unconditionally)."""
        from pyline.config.models import ServerEntry

        entry = ServerEntry(server_no=1, name="game", advertise_ip="10.0.0.1", server_port=2520)
        with pytest.raises(ConfigError, match="client_port"):
            entry.client_listen_port(0)

    def test_configured_client_ports_keep_offset_scheme(self) -> None:
        from pyline.config.models import ServerEntry

        low = ServerEntry(
            server_no=1, name="a", advertise_ip="10.0.0.1", server_port=2520, client_port=1520
        )
        assert low.client_listen_port(0) == 1520
        assert low.client_listen_port(2) == 3520  # +1000 per index at low bases
        high = ServerEntry(
            server_no=2, name="b", advertise_ip="10.0.0.2", server_port=25200, client_port=15200
        )
        assert high.client_listen_port(1) == 25200  # +10000 per index above 10000


class TestMySQLPoolBoundsF90d:
    def test_min_conn_above_max_conn_rejected(self) -> None:
        from pyline.config.models import MySQLSettings

        with pytest.raises(ConfigError, match=r"min_conn.*max_conn"):
            MySQLSettings(user="root", password="$plain:x", db_name="d", min_conn=5, max_conn=4)

    def test_equal_bounds_allowed(self) -> None:
        from pyline.config.models import MySQLSettings

        ok = MySQLSettings(user="root", password="$plain:x", db_name="d", min_conn=4, max_conn=4)
        assert ok.min_conn == ok.max_conn


class TestSecretMaskingF53:
    def test_repr_never_leaks_resolved_secrets(self, config_dir: Path) -> None:
        """F-53: printing settings (logs, error reports, debug consoles) used
        to print the resolved token/password in clear text."""
        settings = load_project_settings(config_dir)
        rendered = repr(settings)
        assert "unit-test-token" not in rendered
        for line in rendered.splitlines():
            if "token" in line or "password" in line:
                assert "unit-test-token" not in line
        # the value itself is still reachable where it is needed
        assert settings.socket.token.get_secret_value() == "unit-test-token"

    def test_secret_paths_derive_from_model(self) -> None:
        """F-53: the loader's secret-field list walks the model annotations,
        so a newly added SecretStr field is covered without touching the
        loader -- inline plaintext in it fails at load time."""
        from pydantic import BaseModel, SecretStr

        from pyline.config.loader import _secret_paths

        class _Nested(BaseModel):
            key: SecretStr
            plain: str

        class _Outer(BaseModel):
            nested: _Nested
            optional: SecretStr | None = None
            count: int = 0

        paths = sorted(_secret_paths(_Outer))
        assert paths == [("nested", "key"), ("optional",)]

    def test_new_secret_field_rejects_inline_plaintext(self, config_dir: Path) -> None:
        """End-to-end proof of the derived paths: an inline plaintext value in
        a SecretStr field (here redis.password) is rejected at load."""
        text = (config_dir / "project.json5").read_text(encoding="utf-8")
        assert '"$plain:test"' in text
        (config_dir / "project.json5").write_text(
            text.replace('"$plain:test"', '"n0t-a-reference"', 1), encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="reference"):
            load_project_settings(config_dir)
