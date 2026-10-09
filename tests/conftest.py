"""Shared fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from pyline.net.loop_policy import install_loop_policy

install_loop_policy()  # zmq needs a selector loop on Windows


@pytest.fixture()
def config_dir(tmp_path: Path) -> Path:
    """A minimal valid config directory."""
    (tmp_path / "project.json5").write_text(
        """
{
    "project": "test-game",
    "srv_type": "develop",
    "socket": {
        "token": "$plain:unit-test-token",
        "client_port": 11520,
        "server_port": 12520,
        // F-161: the Windows selector-loop fd budget is enforced at bind
        // time; the 4096 default would (correctly) fail boot there.
        "max_connections": 32,
    },
    "mysql": {
        "user": "root",
        "password": "$plain:test",
        "db_name": "pyline_test",
    },
    "redis": {
        "password": "$plain:test",
    },
}
""",
        encoding="utf-8",
    )
    (tmp_path / "servers.json5").write_text(
        """
{
    "normal": {
        "sub_process": [],
        "use_mysql": true,
        "use_redis": true,
    },
    "10001": {
        "base": "normal",
        "name": "dev",
        "advertise_ip": "127.0.0.1",
        "client_port": 1520,
        "server_port": 2520,
    },
    "10009": {
        "base": "normal",
        "name": "proxy",
        "advertise_ip": "10.0.0.9",
        "is_proxy": true,
    },
}
""",
        encoding="utf-8",
    )
    (tmp_path / "tables.json5").write_text(
        """
{
    "tbl_player": {
        "comment": "players",
        "fields": {
            "id": {"type": "BIGINT", "primary": true},
            "data": {"type": "MEDIUMBLOB"},
        },
    },
}
""",
        encoding="utf-8",
    )
    return tmp_path
