"""Pydantic settings models.

Every model uses ``extra="forbid"``: a misspelled or unknown key fails startup
with the exact offending field named -- the fail-fast replacement for the
prototype's attribute-access config that silently returned ``None``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from pyline.config.errors import ConfigError


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LogSettings(_StrictModel):
    log_dir: str = ".aiolog"
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    retention_days: int = Field(default=7, ge=1)
    rotation_mb: int = Field(default=64, ge=1)


class SocketSettings(_StrictModel):
    # Secret reference ($env:/$file:/$plain:), resolved at load time into a
    # SecretStr (F-53): the resolved value never shows in repr/logs.
    token: SecretStr
    # Separate token for server-to-server/proxy links (F-16); falls back to
    # ``token`` so existing single-token deployments keep working.
    inter_token: SecretStr | None = None
    bind_host: str = "0.0.0.0"
    client_port: int = Field(ge=1, le=65535)
    server_port: int = Field(ge=1, le=65535)
    max_frame_size: int = Field(default=16 * 1024 * 1024, ge=1024)
    send_queue_limit: int = Field(default=1024, ge=1)
    # Byte-based companion to send_queue_limit: a count-only bound lets
    # ``send_queue_limit * max_frame_size`` bytes accumulate before the
    # overflow guard fires. Default 64 MiB.
    send_queue_bytes: int = Field(default=64 * 1024 * 1024, ge=1024)
    # In-flight cap for inbound RPC CALLs (per RPC service): a token-holding
    # peer must not be able to pile up unbounded tasks.
    rpc_max_inflight: int = Field(default=128, ge=1)
    rpc_inflight_wait: float = Field(default=5.0, gt=0)
    handshake_timeout: float = Field(default=5.0, gt=0)
    idle_timeout: float = Field(default=60.0, gt=0)
    # Global and per-peer-IP connection caps: each pending handshake costs
    # tasks plus up to max_frame_size of decode buffer; unbounded accepts are
    # a cheap FD/task exhaustion attack.
    max_connections: int = Field(default=4096, ge=1)
    max_connections_per_ip: int = Field(default=256, ge=1)


class ZeroMQSettings(_StrictModel):
    bind_host: str = "tcp://127.0.0.1:2918"
    # F-105: the scheme is load-bearing -- zmq.bind("/tmp/x.ipc") is an
    # invalid address, so a bare-path default broke the whole bus on POSIX
    # with default config (Windows uses bind_host, which hid it).
    bind_file: str = "ipc:///tmp/pyline.ipc"
    hwm: int = Field(default=10_000, ge=1)
    reconnect_min_ms: int = Field(default=500, ge=100)
    reconnect_max_ms: int = Field(default=30_000, ge=1000)
    # Per-destination outbound queue bound (F-15): bounds memory when a peer
    # is slow; overflow drops and counts, mirroring ZMQ's own HWM semantics.
    queue_bound: int = Field(default=1000, ge=1)
    # Upper bound on simultaneously tracked destinations: the ROUTER must not
    # grow an unbounded queue+writer per arbitrary target value.
    max_destinations: int = Field(default=256, ge=1)


class MySQLSettings(_StrictModel):
    # Whitelist, not free text: the value is interpolated into
    # "SET SESSION TRANSACTION ISOLATION LEVEL {...}" (F-09).
    isolation_level: Literal[
        "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"
    ] = "READ COMMITTED"
    host: str = "127.0.0.1"
    port: int = Field(default=3306, ge=1, le=65535)
    user: str
    # Secret reference, resolved at load time; masked in repr (F-53).
    password: SecretStr
    db_name: str
    charset: str = "utf8mb4"
    max_conn: int = Field(default=4, ge=1)
    min_conn: int = Field(default=1, ge=0)
    keepalive_interval: float = Field(default=5.0, gt=0)
    keepalive_miss_limit: int = Field(default=3, ge=1)
    # Symmetric with the redis socket_timeout fix (F-11): without a read
    # timeout a half-dead server parks every awaiting query forever.
    # (asyncmy has no write_timeout parameter -- read side only.)
    read_timeout: float = Field(default=30.0, gt=0)
    # Bound on waiting for a free connection from the pool: without it the
    # caller parks forever once the pool is exhausted (a half-dead DB holds
    # every connection up to read_timeout before that, but a leak holds it
    # forever).
    acquire_timeout: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _check_pool_bounds(self) -> MySQLSettings:
        # F-90d: min_conn > max_conn is unsatisfiable -- the pool either
        # never reaches its minimum or overgrows its maximum depending on
        # the implementation. Both bounds are individually valid, so only a
        # cross-field validator can catch it; ConfigError (not a
        # pydantic-internal wrap) keeps the loader's fail-fast message
        # contract.
        if self.min_conn > self.max_conn:
            raise ConfigError(
                f"mysql.min_conn ({self.min_conn}) must not exceed mysql.max_conn ({self.max_conn})"
            )
        return self


class RedisSettings(_StrictModel):
    host: str = "127.0.0.1"
    port: int = Field(default=6379, ge=1, le=65535)
    # Secret reference (or $plain: with empty value for no-auth setups);
    # masked in repr (F-53).
    password: SecretStr | None = None
    db_index: int = Field(default=0, ge=0)
    conn_cnt: int = Field(default=2, ge=1)
    # redis-py official options (F-11): without a socket timeout a dead
    # server parks every awaiting caller forever.
    socket_timeout: float = Field(default=5.0, gt=0)
    health_check_interval: int = Field(default=30, ge=0)


class ClockSettings(_StrictModel):
    """Game-calendar timezone.

    Empty string = the host's local timezone (current behaviour; the frozen
    numbering anchors in ``core.clock`` were defined against local time, so
    this must only be set for NEW projects whose calendar is meant to follow
    a specific zone).
    """

    tz: str = ""


class ProjectSettings(_StrictModel):
    project: str
    srv_type: Literal["develop", "production"]
    log: LogSettings = LogSettings()
    socket: SocketSettings
    zeromq: ZeroMQSettings = ZeroMQSettings()
    mysql: MySQLSettings
    redis: RedisSettings
    clock: ClockSettings = ClockSettings()
    # Prometheus export port on the MAIN process (F-28); null disables.
    metrics_port: int | None = Field(default=9100, ge=1, le=65535)
    # F-54: bind address for the metrics endpoint. Loopback by default --
    # metrics carry operational detail; expose them beyond the host only on a
    # network you already trust, and set a token.
    metrics_bind: str = "127.0.0.1"
    # Optional bearer token (secret reference) required to scrape (F-54).
    metrics_token: SecretStr | None = None
    # Hard bound per boot-step action: a hung connect aborts the boot
    # (the startup watchdog only observes stalls between steps).
    # None disables. Default is generous enough for schema migrations.
    boot_step_timeout: float | None = Field(default=300.0, gt=0)


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

        F-84: ``server_port or 0`` used to hand callers port 0 when the
        entry configured no ``server_port`` -- listeners bound an
        OS-assigned port nobody could connect to, and peers dialed 0.
        ``None`` means "this entry does not interconnect", which is a
        configuration error the moment something needs the port, so raise
        loudly instead of returning a bogus value.
        """
        base = self.server_port
        if base is None:
            raise ConfigError(
                f"server {self.server_no} ({self.name!r}) has no server_port; "
                "entries that interconnect (server-to-server links, proxies) must "
                "declare server_port in servers.json5"
            )
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
                raise ConfigError(
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
