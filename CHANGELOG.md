# Changelog

## v1.0.0-rc.1 (2026-10-08)

Migration-finish release per `docs/migration-plan.md`: all 30 review defects
(F-01..F-30) fixed, old-repo business surface preserved, engineering gates
green (ruff + mypy --strict + pytest).

### Data safety (M1)
- Auto-save never drops dirty data: exponential-backoff retry, bounded
  deadline shutdown flush, per-saver CRITICAL loss report, non-zero exit (3)
- Single-flight ORM loads, SQL-first delete, in-flight-safe shutdown
- First boot creates the database before pooling; Alembic-style
  `pyline_schema` version table + `migrations/NNN_name.sql`; drift detection
  fails startup; ADD COLUMN pins `ALGORITHM=INSTANT, LOCK=NONE`
- Blob migration chain wired into decoding (old→new upgrades, newer→
  `BlobVersionError`); `python -m pyline.tools.migrate_pickle` converts
  legacy pickle blobs (restricted unpickling, dry-run default)
- isolation-level whitelist, DEFAULT-literal whitelist, COMMENT escaping,
  dedicated keepalive connection with observable loss, redis socket timeouts

### Network robustness (M2)
- Chunk reassembly applies to every flag (silent >1MB @rpc corruption fixed);
  `max_frame_size` wired everywhere
- Handler exceptions drop frames, not connections; malformed-RPC arity
  validation; error-storm disconnects
- RPC: pre-packed arguments, send-failure pending cleanup, unserializable
  results reported as remote errors, timeouts cancel remote execution,
  `current_caller()`
- ZMQ bus: per-destination bounded queues + single writers (no ROUTER
  head-of-line blocking, per-destination FIFO, drop counters)
- Proxy: guarded reconnect, IDENT machine/IP validation + duplicate-claim
  rejection, separate `inter_token`, client network blocked from `@`
  control flags
- Connections: two-phase draining close, bounded `wait_closed` (new
  `close_server` helper), server-side idle probes, `@welcome` handshake

### Kernel (M3)
- Failed startup tasks abort boot; shutdown-during-boot exits cleanly;
  guarded bounded teardown chain; main-process signal handlers with
  double-signal force exit
- Supervisor: non-blocking joins, tracked tasks, first test suite
- Log channels keyed by identity; scheduler teardown/typing fixes; event
  bus supports base-class subscriptions, `EnvReadyEvent`; clock boundary
  catch-up, tz pinning, week_no fix (numbering bases frozen for persisted
  data); config hardening (duplicate servers, `$plain:` no-auth, unknown
  process types); console on main process only, `￥` prefix
- Metrics fully wired + Prometheus endpoint (`metrics_port`) + AlarmHub

### Hot reload (M4)
- Static AST validation (no sandbox execution -- import side effects run
  once), call-compatibility signature checks, dunder/`__slots__` guards,
  single-source read (no TOCTOU), deep-snapshot true rollback,
  closure-layout runtime invariant; `docs/hot-reload.md` contract;
  gateway `rebind_module` hook

### Business surface (M5)
- `pyline.api` facade (env/registry/task/timer/rpc/db/clock/orm/debug/log),
  bound via `api.bind(ctx)`; business module auto-registration
  (`game.events`, overridable via `$PYLINE_EVENTS`)
- `TrackedDict`/`TrackedList` containers; template `game/com_time.py`
  (old-repo function set, frozen numbering) and `game/containers.py`
  (TimeData/DayData/WeekData/DataOP, TimeUpset bug fixed)

### Engineering
- CI integration job runs mysql/redis/integration marked tests against
  service containers; coverage gate 65 -> 75; live-MySQL/Redis integration
  suites (skip when unreachable); cryptography + tzdata(win32) dependencies
