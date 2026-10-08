# Changelog

## Unreleased: full code-review fix pass (F-31..F-37 + kernel/net hardening)

Second review pass over the whole tree; every fix carries a regression test
(212 passed / 5 skipped vs 184 before; ruff format+check and mypy --strict
clean -- note the rc.1 "gates green" claim predates a ruff-format drift that
this pass also cleared).

### Data safety (round 2)
- F-31: `MySQLSettings.read_timeout` wired into the pool and the keepalive
  connection (asyncmy has no write_timeout) -- a half-dead server no longer
  parks every pooled query forever
- F-32: keepalive loss now arms an exponential-backoff recovery loop that
  rebuilds the pool in place; `on_lost` fires once per incident (wired to the
  `mysql_lost` alarm in the runtime)
- F-33: shutdown `flush_all` rotates a failing saver to the queue tail -- one
  broken row used to monopolize the whole deadline and starve every other
  dirty saver
- F-34: per-saver flush lock serializes `flush()`/`delete()`; an in-flight
  upsert can no longer land after the DELETE and resurrect a deleted row
- F-35: truncated msgpack blobs (`OutOfData`) surface as `BlobFormatError`
- F-36: schema drift detection expects exactly what `ddl()` emits (TEXT/BLOB
  columns are created nullable); self-created tables no longer self-drift
- F-37: COMMENT text rejects backslashes (mirrors the DEFAULT-literal rule)
- `TrackedDict.__ior__` / `TrackedList.__imul__` overrides: `d |= {...}` and
  `l *= n` used to mutate without marking the owner dirty (silent data loss)
- Shutdown teardown timeout before the flush step completes now forces the
  non-zero exit code instead of silently exiting 0 with possibly-dirty data

### Network hardening (round 2)
- Inbound RPC concurrency cap (`socket.rpc_max_inflight`/`rpc_inflight_wait`):
  a CALL storm gets busy errors instead of unbounded task growth; inbound
  tasks are strongly referenced and their failures observed
- Byte-based send-queue budget (`socket.send_queue_bytes`, default 64 MiB) in
  addition to the message-count limit; bytes stay accounted while `drain()`
  is blocked on a slow peer
- `@auth` token comparison via `hmac.compare_digest` (constant time)
- ZMQ bus destination-table cap (`zeromq.max_destinations`, default 256):
  arbitrary `target` values can no longer grow unbounded queue+writer pairs
  (`ipc_dest_overflow_total` counter added)
- Proxy: `close()` now closes established proxy connections; deregistration
  hooks are identity-checked (a replaced connection's hook cannot evict its
  live replacement); `inter_token` fallback to the client token logs a warning
- Dead `@bye` flag removed (never had a sender)

### Kernel
- Multi-process main entry now calls `api.bind(ctx)` (business calls from the
  main process used to raise `ApiUnboundError`); children are terminated even
  when main-process boot fails; child-watch starts only after the runtime
  exists (early child death can no longer be silently ignored)
- Boot steps are bounded by `boot_step_timeout` (default 300s): a hung
  connect aborts boot instead of parking the process (the watchdog only
  observes stalls between steps)
- Business module import failure now fails boot (a typo'd `PYLINE_EVENTS`
  used to boot a business-less server that looked healthy)
- Scheduler: `call_repeating` re-arms on the original deadline grid (no drift
  accumulation); `close()` cancels running coroutine callbacks; `TimerHandle
  .left()` gives real remaining time and `api.timer.left()` uses it
- Clock boundary events derive wall-clock fields through `GameClock.local()`
  (a pinned tz can no longer disagree with the boundary math);
  `NewDayEvent.day` is now the game `day_no`, not the day-of-month
- Event bus dedups repeated subscriptions; `PreReloadEvent`/`OnReloadEvent`
  are actually emitted around hot reloads; dead code removed
  (`describe_event_file`, `_frame_locals`)
- `api.task.spawn` observes task failures; `api.service()` raises a dedicated
  `ApiServiceUnavailableError`; timer facade cache invalidates on rebind;
  `$plain:` secret warning is no longer dead code; duplicate `advertise_ip`
  raises `ConfigError`
- Supervisor pid probe uses `OpenProcess` on Windows (`os.kill(pid, 0)`
  terminates the target on win32); console emit tasks keep strong references;
  file watcher maps changed files by longest sys.path prefix and stops via
  its `stop_event`

### Hot reload (round 2)
- Keyword-only signature check fixed: adding a required kw-only param (or
  stripping a default) is now rejected -- it used to pass validation and
  TypeError every old call site
- `__slots__` guard compares the slot NAME SET (a rename used to orphan data
  in existing instances); own-vs-inherited slots no longer false-rejects
  subclasses; statically unresolvable `__slots__` rejects conservatively
- Descriptor-wrapped methods (`@staticmethod`/`@classmethod`/`@property`)
  are signature-checked (they used to bypass validation while the swap really
  replaced their inner functions) and their inner functions are covered by
  the rollback snapshot
- Subscripted/dotted bases (`list[int]`, `module.Base`) compare by bare name
  (no more false "inheritance changed" rejects); value-level
  `__reloadkeep__` also blocks overwrites; `__reloadkeep__ = True` on a
  reload-target class no longer crashes the update
- `__closure__` removed from the swappable set (assignment is a silent no-op
  on functions); docs now state that closure captured VALUES are preserved
  across reloads, never re-bound

### Engineering
- CI test matrix adds `windows-latest` (the repo carries win32-specific code
  paths that were previously never exercised in CI)
- `ruff format` drift (19 files) cleared; format is part of the lint job

### Known gaps (deliberate, documented)
- Sub-process Prometheus metrics are not scrapeable (only the main process
  exports :9100); multiprocess mode or per-process ports needs a deployment
  decision
- Polling loops (watchdog 20Hz, park 2Hz, ...) are intentionally kept --
  converting them to event-driven is a refactor with regression risk, not a
  defect fix
- `log._file_channels` remains a module-level cache keyed by run_dir (F-22
  leftover); moving it into Context requires touching every file_logger caller

## v1.0.0-rc.1 (2026-10-08)

Migration-finish release per `docs/migration-plan.md`: all 30 review defects
(F-01..F-30) fixed, old-repo business surface preserved, engineering gates
green at commit time (ruff check + mypy --strict + pytest; the ruff-format
drift fixed in the pass above postdates this tag).

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
