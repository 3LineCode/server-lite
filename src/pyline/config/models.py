"""Pydantic settings models.

Every model uses ``extra="forbid"``: a misspelled or unknown key fails startup
with the exact offending field named -- the fail-fast replacement for the
prototype's attribute-access config that silently returned ``None``.
"""

from __future__ import annotations

import sys
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
    # Decode-buffer cap until the handshake verifies the peer: the handshake
    # payloads are tiny, so an unauthenticated connection gets to buffer
    # kilobytes, not ``max_frame_size`` (16 MiB), before it proves anything --
    # the accept caps count connections, not bytes.
    preauth_max_frame: int = Field(default=64 * 1024, ge=256)
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
    # F-175: bound on the TCP dial itself (proxy links). Without it a
    # dropped SYN parks the connect on the OS timeout (~21 s Windows,
    # ~2 min Linux) and the proxy maintainer's backoff ladder never runs.
    connect_timeout: float = Field(default=10.0, gt=0)
    # Global and per-peer-IP connection caps: each pending handshake costs
    # tasks plus up to max_frame_size of decode buffer; unbounded accepts are
    # a cheap FD/task exhaustion attack.
    # F-219: the default must boot on the platform it ships on -- 4096 blew
    # the Windows selector-loop fd budget at bind time (F-161), and even 448
    # left nothing for a proxy listener in the same process. 384 + the
    # proxy-plane cap (64) fits the ~448-fd Windows ceiling exactly.
    max_connections: int = Field(
        default_factory=lambda: 384 if sys.platform == "win32" else 4096, ge=1
    )
    max_connections_per_ip: int = Field(default=256, ge=1)
    # F-224: per-connection cap on concurrently RUNNING handler tasks. The
    # Network-level cap is process-global: one authenticated connection could
    # occupy every slot with a frame storm and starve all other clients.
    # 0 disables the per-connection cap.
    max_inflight_per_connection: int = Field(default=16, ge=0)
    # TLS on the CLIENT listener (F-187): server certificate for game
    # clients. None = plaintext (the pre-F-187 default; HMAC auth still
    # runs, but payloads are sniffable on the path).
    tls: TlsSettings | None = None
    # TLS on the PROXY plane (F-188): server-to-server links. None =
    # plaintext. Set ``require_client_cert`` for the mutual mode the
    # deployment doc prescribes for untrusted networks (each machine's cert
    # is its identity; only CA-signed machines connect).
    proxy_tls: TlsSettings | None = None


class TlsSettings(_StrictModel):
    """TLS for one TCP plane (F-187/F-188).

    One certificate per machine is the intended model: the same cert/key is
    the server certificate on the machine's listener and the client
    certificate when it dials peers (classic mutual-TLS mesh). ``ca_file``
    verifies the other side -- for the proxy plane set
    ``require_client_cert`` so only machines holding a CA-signed certificate
    connect at all. Paths are resolved against the config dir's parent (the
    project root) at load time, exactly like ``mysql.migrations_dir``.
    """

    cert_file: str
    key_file: str
    # CA bundle verifying the peer. Required for client contexts and for
    # ``require_client_cert``; without it the client context cannot be built
    # (verification is the entire point -- an unverified TLS client is
    # theater).
    ca_file: str | None = None
    require_client_cert: bool = False
    # F-220: verify the server certificate's hostname/IP against the dial
    # target. Off by default because the proxy mesh dials ``advertise_ip``
    # values that the classic one-cert-per-machine model does not bind into
    # SANs; with per-host certificates (IP SANs or DNS names that match the
    # dial target) turn it on -- it upgrades the cert from "signed by our
    # CA" to "is the machine we named".
    verify_hostname: bool = False


class CurveSettings(_StrictModel):
    """CURVE transport security for the ZMQ bus (F-189).

    All processes of a server read the same config, so only the two SECRETS
    are needed: the ROUTER's long-term secret and the DEALER's. Public keys
    are derived at boot (``zmq.curve_public``); an explicit
    ``server_public``/``client_public`` may pin them (validated to match the
    derived value). Values are z85 strings (40 chars) -- generate with
    ``python -c "import zmq; print(zmq.curve_keypair())"``. Secrets are
    secret references ($env:/$file:), never committed plaintext.

    ``extra_client_keys``: additional DEALER public keys (z85) the ROUTER's
    ZAP handler accepts -- for deployments giving every process its own
    keypair instead of sharing one client secret. The allowlist always
    includes the public key derived from ``client_secret``.
    """

    server_secret: SecretStr
    client_secret: SecretStr
    server_public: str | None = None
    client_public: str | None = None
    extra_client_keys: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _check_key_shapes(self) -> CurveSettings:
        # z85 of a 32-byte key is exactly 40 characters; catching a typo'd
        # or truncated key here fails boot with a fix-it message instead of
        # a libzmc "invalid key" deep inside the bus start.
        for name, value in (
            ("server_secret", self.server_secret.get_secret_value()),
            ("client_secret", self.client_secret.get_secret_value()),
        ):
            if len(value) != 40:
                raise ConfigError(
                    f"zeromq.curve.{name} must be a 40-char z85 key "
                    f"(got {len(value)} chars; generate with "
                    '"python -c \\"import zmq; print(zmq.curve_keypair())\'")'
                )
        optional_keys: list[tuple[str, str | None]] = [
            ("server_public", self.server_public),
            ("client_public", self.client_public),
        ]
        for key_name, key_value in optional_keys:
            if key_value is not None and len(key_value) != 40:
                raise ConfigError(f"zeromq.curve.{key_name} must be a 40-char z85 key")
        for i, key in enumerate(self.extra_client_keys):
            if len(key) != 40:
                raise ConfigError(f"zeromq.curve.extra_client_keys[{i}] must be a 40-char z85 key")
        return self


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
    # Byte-based companion to queue_bound (the TCP side's F-21 analogue): a
    # count-only bound lets ``queue_bound * max_frame_size`` bytes (~16 GiB)
    # accumulate in one destination queue before the count guard fires --
    # frames on the bus may be up to ``max_frame_size`` large. Default 64 MiB.
    queue_bytes: int = Field(default=64 * 1024 * 1024, ge=1024)
    # Deadline for the bus HMAC handshake (see net/ipc.py): a peer that never
    # completes authentication fails its boot instead of silently dropping
    # every message it sends. Generous because the DEALER queues its handshake
    # until the ROUTER binds -- a child may boot before the parent's socket.
    auth_timeout: float = Field(default=30.0, gt=0)
    # Upper bound on simultaneously tracked destinations: the ROUTER must not
    # grow an unbounded queue+writer per arbitrary target value.
    max_destinations: int = Field(default=256, ge=1)
    # CURVE transport security (F-189): encryption + per-key authentication
    # for the bus. None = the HMAC-handshake-only plane (authenticated but
    # sniffable by anything on the endpoint's network path).
    curve: CurveSettings | None = None


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
    # Seconds before a pooled connection is discarded and re-established on
    # acquire, so a connection killed by the server's ``wait_timeout`` (which
    # the dedicated keepalive socket cannot see) fails at most once. Set below
    # the MySQL ``wait_timeout``. 0 disables age-based recycling.
    pool_recycle: int = Field(default=3600, ge=0)
    # Versioned ``.sql`` migration directory, resolved relative to the project
    # root at load time; ``None`` disables the versioned-migration engine
    # (table creation / additive columns / drift detection still run).
    migrations_dir: str | None = None
    # F-211: use the row-alias ``ON DUPLICATE KEY UPDATE`` syntax (MySQL >=
    # 8.0.19) in REMOTE processes' upserts too. The pool-owning process picks
    # the form by version probe; business processes cannot probe (no pool),
    # so this setting aligns their TableCatalog with the probed choice.
    odku_row_alias: bool = False

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
    # F-213: dedicated liveness probe cadence (mirror of mysql's keepalive).
    # redis-py reconnects on next use by itself, but WITHOUT a probe the
    # outage is invisible until a business call fails -- no alarm, no metric,
    # no recovery signal.
    keepalive_interval: float = Field(default=5.0, gt=0)
    keepalive_miss_limit: int = Field(default=3, ge=1)


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
    # Per-process metrics export (see docs/deployment.md): when true, every
    # sub-process ALSO exports its own registry on ``metrics_port + index``
    # (the db child of a two-process split binds 9101, etc.). Without it only
    # the main process exports, and sub-process autosave/RPC/loop metrics are
    # invisible to Prometheus.
    metrics_all_processes: bool = False
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
        """Client listener port for the process with this index.

        F-84 twin: ``self.client_port or 0`` used to return 0/1000/... for
        entries that configured no ``client_port`` -- the main process bound
        an OS-assigned (or plainly wrong) port no client could dial.  The
        client listener is bound unconditionally for every main process, so
        a missing ``client_port`` is a configuration error the moment the
        port is needed: raise loudly instead of returning a bogus value.
        """
        base = self.client_port
        if base is None:
            raise ConfigError(
                f"server {self.server_no} ({self.name!r}) has no client_port; "
                "entries whose main process accepts client connections must "
                "declare client_port in servers.json5"
            )
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
