"""Pydantic settings models.

Every model uses ``extra="forbid"``: a misspelled or unknown key fails startup
with the exact offending field named -- the fail-fast replacement for the
prototype's attribute-access config that silently returned ``None``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LogSettings(_StrictModel):
    log_dir: str = ".aiolog"
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    retention_days: int = Field(default=7, ge=1)
    rotation_mb: int = Field(default=64, ge=1)


class SocketSettings(_StrictModel):
    # Secret reference ($env:/$file:/$plain:), resolved at load time.
    token: str
    # Separate token for server-to-server/proxy links (F-16); falls back to
    # ``token`` so existing single-token deployments keep working.
    inter_token: str | None = None
    bind_host: str = "0.0.0.0"
    client_port: int = Field(ge=1, le=65535)
    server_port: int = Field(ge=1, le=65535)
    max_frame_size: int = Field(default=16 * 1024 * 1024, ge=1024)
    send_queue_limit: int = Field(default=1024, ge=1)
    handshake_timeout: float = Field(default=5.0, gt=0)
    idle_timeout: float = Field(default=60.0, gt=0)


class ZeroMQSettings(_StrictModel):
    bind_host: str = "tcp://127.0.0.1:2918"
    bind_file: str = "/tmp/pyline.ipc"
    hwm: int = Field(default=10_000, ge=1)
    reconnect_min_ms: int = Field(default=500, ge=100)
    reconnect_max_ms: int = Field(default=30_000, ge=1000)
    # Per-destination outbound queue bound (F-15): bounds memory when a peer
    # is slow; overflow drops and counts, mirroring ZMQ's own HWM semantics.
    queue_bound: int = Field(default=1000, ge=1)


class MySQLSettings(_StrictModel):
    # Whitelist, not free text: the value is interpolated into
    # "SET SESSION TRANSACTION ISOLATION LEVEL {...}" (F-09).
    isolation_level: Literal[
        "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"
    ] = "READ COMMITTED"
    host: str = "127.0.0.1"
    port: int = Field(default=3306, ge=1, le=65535)
    user: str
    # Secret reference, resolved at load time.
    password: str
    db_name: str
    charset: str = "utf8mb4"
    max_conn: int = Field(default=4, ge=1)
    min_conn: int = Field(default=1, ge=0)
    keepalive_interval: float = Field(default=5.0, gt=0)
    keepalive_miss_limit: int = Field(default=3, ge=1)


class RedisSettings(_StrictModel):
    host: str = "127.0.0.1"
    port: int = Field(default=6379, ge=1, le=65535)
    # Secret reference (or $plain: with empty value for no-auth setups).
    password: str | None = None
    db_index: int = Field(default=0, ge=0)
    conn_cnt: int = Field(default=2, ge=1)
    # redis-py official options (F-11): without a socket timeout a dead
    # server parks every awaiting caller forever.
    socket_timeout: float = Field(default=5.0, gt=0)
    health_check_interval: int = Field(default=30, ge=0)


class ProjectSettings(_StrictModel):
    project: str
    srv_type: Literal["develop", "production"]
    log: LogSettings = LogSettings()
    socket: SocketSettings
    zeromq: ZeroMQSettings = ZeroMQSettings()
    mysql: MySQLSettings
    redis: RedisSettings
    # Prometheus export port on the MAIN process (F-28); null disables.
    metrics_port: int | None = Field(default=9100, ge=1, le=65535)


class TableFieldDef(_StrictModel):
    """One column declaration in ``tables.json5``.

    ``default`` is a SQL literal (number, ``'quoted string'``, ``NULL``,
    ``CURRENT_TIMESTAMP``) validated against a whitelist before it may
    reach DDL -- never a placeholder or expression (F-09).
    """

    type: str
    primary: bool = False
    comment: str = ""
    unique: bool = False
    not_null: bool = False
    default: str | None = None


class TableDef(_StrictModel):
    comment: str = ""
    fields: dict[str, TableFieldDef]


class ServerEntry(_StrictModel):
    """One row of the server registry (``servers.json5``).

    ``advertise_ip`` is mandatory and explicit: the framework never guesses its
    own identity from NIC probing (prototype bug #5). ``bind_ip`` defaults to
    ``advertise_ip``.
    """

    server_no: int = Field(ge=1, le=99_999)
    name: str
    advertise_ip: str
    bind_ip: str | None = None
    client_port: int | None = Field(default=None, ge=1, le=65535)
    server_port: int | None = Field(default=None, ge=1, le=65535)
    sub_process: tuple[str, ...] = ()
    coverage_dir: tuple[str, ...] = ()
    use_mysql: bool = True
    use_redis: bool = True
    is_proxy: bool = False

    def bind_host(self) -> str:
        return self.bind_ip or self.advertise_ip

    def process_port(self, process_index: int) -> int:
        """Inter-process listening port for the process with this index.

        Same offsetting scheme as the prototype: +10000 per index when the
        configured port is >10000, else +1000 per index.
        """
        base = self.server_port or 0
        if base > 10_000:
            return base + process_index * 10_000
        return base + process_index * 1_000

    def client_listen_port(self, process_index: int) -> int:
        base = self.client_port or 0
        if base > 10_000:
            return base + process_index * 10_000
        return base + process_index * 1_000


class ServerRegistry:
    """Resolved registry of all servers, keyed by server number."""

    def __init__(self, entries: dict[int, ServerEntry]) -> None:
        self._entries = entries
        self._by_ip: dict[str, int] = {}
        for no, entry in entries.items():
            if entry.advertise_ip in self._by_ip:
                raise ValueError(
                    f"duplicate advertise_ip {entry.advertise_ip} in servers config "
                    f"(servers {self._by_ip[entry.advertise_ip]} and {no})"
                )
            self._by_ip[entry.advertise_ip] = no

    def __len__(self) -> int:
        return len(self._entries)

    def entry(self, server_no: int) -> ServerEntry:
        try:
            return self._entries[server_no]
        except KeyError:
            raise KeyError(f"unknown server number {server_no}") from None

    def by_advertise_ip(self, ip: str) -> int | None:
        return self._by_ip.get(ip)

    def proxy_list(self) -> list[int]:
        return [no for no, e in self._entries.items() if e.is_proxy]

    def entries(self) -> list[ServerEntry]:
        return list(self._entries.values())

    def all_servers(self, *, exclude: list[int] | None = None) -> list[int]:
        skip = set(exclude or ())
        return [no for no in self._entries if no not in skip]
