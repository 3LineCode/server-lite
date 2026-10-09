# Changelog

## Unreleased: fifth review pass (F-58..F-105)

Full-assessment fix pass (one P0, every P1/P2/P3 finding); every fix carries
a regression test; ruff format+check and mypy --strict clean.

### Shutdown & lifecycle (the P0)
- F-58: a signal/console/child-death shutdown runs teardown fire-and-forget
  while the parking loop only polls `in_quit()` -- which `request_shutdown`
  sets *before* running its hooks. The main coroutine could return while
  the save-flush was still in flight, `asyncio.run` then cancelled it
  midway, and `save_flush_ok` still read True: exit 0 with dirty data lost,
  defeating F-01. `request_shutdown` now resolves a completion future only
  after hooks + quit-task drain (finally-guarded), and both parking loops
  join it via `_settle_shutdown` before returning. `_spawn` also observes
  background-task exceptions now, and a shutdown landing before/between
  boot steps aborts the boot cleanly instead of a transition-table KeyError

### Data safety
- F-59: the F-42 coalesced upsert released each saver's flush lock after
  encoding (`flush_row` returned "the caller owns the SQL") -- a concurrent
  `delete()` could land its DELETE before the multi-row upsert resurrected
  the row, and the saver was counted saved and never retried. Batch flush
  now holds the locks until the group SQL has executed
  (`begin_flush_row`/`end_flush_row`; single-row `flush()` unchanged)
- F-60: a failed COMMIT no longer bypasses the F-50 journal -- the local
  and remote transaction paths wrap the whole session stack, so commit
  failure re-marks every saver in the unit (idempotent, safe on unknown
  outcome)
- F-61: a remote COMMIT that times out is reconciled once against the DB
  process's recorded outcome (`RPC_TX_STATUS`: committed/rolled_back/
  unknown) before being treated as failed -- a slow-but-successful commit
  is no longer retried as a double-write
- F-63: mutations inside `db.transaction()` no longer leak through the
  background auto-save (autocommit mid-transaction): dirty marks are
  deferred to the end of the unit and the flush loop skips savers held by
  an active journal; shutdown `flush_all` deliberately still drains them
  (never-drop wins over atomicity when the process is dying)
- F-68: COMMIT+ROLLBACK double failure discards the connection instead of
  returning an open-transaction socket to the pool
- F-102: `DataSaver.flush()` before load no longer encodes `None` over an
  existing row
- F-101: `DatabaseService.close()` rolls back and closes active sessions,
  awaits disposers, and refuses new begins

### Network & mesh
- F-69: the RPC `_running` table is keyed by `(from_service, call_id)` --
  every process's counter starts at 1, so two concurrent callers of the
  same service collided on call_id 1, corrupting CANCEL routing
- F-70: cross-machine RPC to a *sub-process* used to dead-end (the bus
  rewrote the origin to the local main; replies left the target's machine
  with `CrossServerError`). The bus preserves the true origin, and
  sub-processes relay cross-machine sends through their main (`@relay`,
  envelope-isomorphic with `@fwd`, origin bound to the sender's local mesh
  -- F-39/F-40/F-48 checks all preserved, two-machine e2e test included)
- F-71: `send_message` rejects payloads over the frame cap up front
  instead of shipping 16 MB of legal chunks that the peer discards
- F-72: the client listener caps concurrent connections globally and per
  peer IP (`socket.max_connections`, `max_connections_per_ip`), wired
  through the runtime; refusals are counted, not allocated
- F-74: idle destination-slot eviction can no longer cancel a writer
  mid-multipart (which poisoned the shared socket); F-76 keeps writers
  alive through non-ZMQ errors; F-75 chmods POSIX `ipc://` endpoints to
  0600; F-73: the inter_token fallback warns once, not per reconnect
- F-77: the RPC inflight semaphore cannot leak permits through the
  grant-vs-cancel window; F-78: strong references for close tasks, wider
  msgpack exception classification, offset-cursor frame decoding (no
  per-frame O(n) buffer shift), unknown flag bits rejected at decode

### Runtime & hot-reload
- F-91: `PreReloadEvent` is now awaited *before* the code swap (it used to
  be spawned fire-and-forget and ran after `reload_module` in practice,
  breaking the documented quiesce contract); console/watcher hooks accept
  async reload hooks
- F-92: a `use_mysql=false` + `use_redis=false` server (pure gateway)
  boots instead of crashing on `db_service_no()` -- DB-less
  `DatabaseAccess` fails loudly on first use instead (F-104)
- F-93: teardown closes every ingress (client listener, proxy links,
  watcher, console) *before* the save-flush -- frames arriving after the
  flush snapshot used to mutate data that would never be persisted;
  zmq-bus closes after the flush (flush rides RPC through it)
- F-94: `clock`/`alarms` register at runtime construction and `db`/
  `make_saver` right after CONN_DB -- business BaseInit/FuncInit hooks can
  actually use them
- F-95: TeardownPlan has a per-step budget (`min(step cap, remaining
  total)`) -- one hung step no longer starves mysql.close out of the
  shutdown timeout; the save-flush is exempt from the per-step cap
- F-96: the dev watcher roots at the business package (not the whole cwd),
  ignores tests/build/venv/caches, and `reload_module` refuses modules
  that were never imported instead of importing them as a side effect
- F-97: a reload that raises SystemExit no longer silently kills the
  watcher/console tasks (guarded, handed to the shutdown hook)
- F-98: game-clock setback (SetTime) no longer replays calendar
  boundaries when time is restored; F-99: the metrics HTTP server is
  tracked and closed in teardown
- F-100: `clock.tz` setting wires the GameClock timezone (fail-fast on
  unknown zones; com_time derives through the clock); reload refinements:
  function runtime attributes survive reloads (merge, not replace), nested
  classes enter the deep rollback snapshot, module `__private` state is
  preserved, base classes compare by module+qualname, metaclass changes
  are explicitly rejected; template: ghost `SaverDataOP` docstring replaced
  by the real DataOP/TrackableModel/make_saver recipe, dev bind_host is
  127.0.0.1

### Core & config
- F-79/F-80: re-targeting a log channel to a new run_dir no longer stacks
  handlers (double write); per-process log files are `os-{process_tag}.log`
  with pid and logger name in the format -- note the main process file
  moves from `os.log` to `os-main.log`
- F-81/F-82: duplicate `sub_process` entries are rejected before spawn;
  the child watch cannot die silently (observed exceptions, fail-fast
  semantics when the callback itself fails)
- F-83: cancelling a repeating timer clears its pending wheel slot
  immediately and `left()` reports the next occurrence
- F-84: `process_port()` with no configured `server_port` raises a
  ConfigError with guidance instead of returning 0; F-90d validates
  `min_conn <= max_conn`
- F-85/F-86: `on_quit` keeps strong task references when no lifecycle is
  bound; the shutdown fallback is an honest `os._exit` (the old
  `os.kill(pid, 15)` was a Windows TerminateProcess in disguise)
- F-87: `Context.service(name, cls)` gives typed service lookup (facades
  no longer `cast`); F-88: `from pyline import api` no longer imports the
  db/net dependency chain (lazy submodule attributes)
- F-89: half-hour boundaries derive through calendar-aware arithmetic --
  DST transition days no longer skip or duplicate a beat
- F-90: AlarmHub `register_all` returns an unsubscribe and dedups;
  exception-chain formatting guards cycles; serial event dispatch is now
  documented as the contract (ordering over parallelism)

### Schema & tools
- F-65: the pickle migration tool no longer false-fails every dict with
  non-string keys (both sides normalized before comparison) -- such rows
  used to be "protected" as permanent pickle
- F-66: migration statement splitting strips `--` comments *before*
  splitting on `;`; F-67: migrations hold a MySQL `GET_LOCK` so two
  `owns_db` processes cannot race the schema/version table
- F-64: `TrackedDict`/`TrackedList` work with `dataclasses.asdict`
  (optional `touch`); F-103: the migration tool validates identifiers
- F-105: `zeromq.bind_file` defaults to a proper `ipc://` URL (a bare path
  is not a valid zmq address -- the POSIX bus failed to bind on default
  config; bare paths are normalized for old configs)

## Unreleased: fourth review pass (F-46..F-57)

Assessment-driven fix pass over every finding of the project review; every
fix carries a regression test; ruff format+check and mypy --strict clean;
300 unit tests passing, coverage 78% (the 75% CI gate had been drifting:
72% before this pass, and the F-38..F-45 note below overstated its count).

### Data safety
- F-46: the auto-save loop can no longer be killed by a poison row -- a blob
  that cannot be encoded used to escape `flush_batch` uncaught, silently
  stopping every later save until shutdown. Encode failures are now
  quarantined per-row (F-33 semantics: retry with backoff, no batch
  starvation); any other unexpected flush error requeues the already-popped
  savers (never-drop holds for bugs too) and the loop-level guard alarms
  (`save_loop_error`) instead of dying; a dead loop task itself alarms
  (`save_loop_died`) via a done-callback
- F-47: a failed remote COMMIT no longer leaks its dedicated connection
  (`rpc_tx_commit` closes the session in a finally; the record was already
  popped, so the caller's rollback remedy only ever saw
  `TransactionGoneError`); the 60 s session TTL now bounds *idle* time --
  execute/query renew it, so a legitimately long transaction is not rolled
  back for being long -- and the expiry sweep runs on execute/query too, not
  only on the next begin
- F-50: a transaction rollback re-marks every saver whose flush joined the
  unit (`TransactionJournal`): the DB keeps the old row while memory held
  the new data, and -- in the coalesced path -- the savers were already
  popped off the dirty queue, so nothing would ever retry them
- F-51: migrations are resumable per statement (`pyline_schema_progress`
  table): MySQL DDL implicitly commits, so a failed multi-statement file
  used to rely entirely on script idempotency and re-ran the whole file on
  the next boot; it now resumes from the failed statement (a file that
  shrank since the failed attempt is refused)

### Trust model (mesh hardening, round 2)
- F-48: the `@fwd` proxy validates the envelope's claimed `from_service`
  against the sending connection's IDENT-registered machine at the first
  hop (`proxy_spoofed_total`) -- any connected machine could previously
  stamp a victim's service number on its envelope and sail past the
  F-39/F-40 origin checks, which compare against the *claim*; relay hops
  (validated third-party origins) and the legacy origin-unknown form are
  unaffected
- F-53: every secret-bearing setting is a `SecretStr` -- printing settings
  (logs, error reports, consoles) no longer leaks resolved tokens/passwords.
  The loader's secret-field list is now derived from the model annotations,
  so a newly added secret field cannot be forgotten and let inline plaintext
  pass silently
- F-54: the Prometheus endpoint binds 127.0.0.1 by default and supports an
  optional bearer token (`metrics_bind`, `metrics_token`; constant-time
  compare, 401 otherwise)

### Operations
- F-49: ZMQ destination-table slots of peers silent past an idle TTL (empty
  queue) are reclaimed when the table is full -- vanished peers (and bogus
  targets a rogue DEALER named once) used to occupy their slot forever;
  after 256 the bus refused every new destination permanently
- F-52: exceeding the clock catch-up cap (>2 days of downtime) alarms
  (`clock_boundaries_skipped`) and logs instead of silently skipping the
  surplus boundaries -- daily resets that missed their `NewDayEvent` need
  an operator, not a shrug

### Tooling / engineering
- F-57: the file watcher's module mapping picked the SHALLOWEST matching
  sys.path entry -- the exact mapping its own comment forbids; the deepest
  (package-root) entry now wins, matching the documented intent
- F-55: version realigned (`pyproject` 0.1.0 -> 1.0.0rc1, matching the
  README's v1.0.0-rc.1); local `__pycache__` clutter under `template/`
  removed (never tracked)
- F-56: coverage/tests restored above the CI gate -- console commands,
  watcher mapping, debug formatting, log channels, task/env facades gained
  first-time unit tests (runtime.py's multi-process paths remain the big
  uncovered block, now the only one)
- `docs/hot-reload.md` gains a "Known limits" section (top-level side
  effects are real and not rolled back; decorated wrappers are checked by
  outer signature only; the watcher execs on the event loop)
- `docs/deployment.md` updated for the @fwd origin binding and the metrics
  endpoint auth options

## Unreleased: third review pass (F-38..F-45)

Every fix carries a regression test; ruff format+check and mypy --strict
clean; 244 unit tests passing plus 7 live-service integration tests
(vs 212 before; an earlier revision of this note overstated the count).

### Data safety / correctness
- F-38: schema drift detection now expects exactly what `ddl()` emits for
  nullability -- a `not_null: true` TEXT/BLOB column (created nullable by
  F-36's own rule) no longer permanently fails startup on the tables this
  framework itself created (completes the half-done F-36)
- F-42: auto-save coalesces dirty rows sharing (table, column) into one
  multi-row upsert per round-trip (32-row / 4 MiB chunk caps; single-row
  groups keep the direct path; a failing group falls back to per-saver
  flushes so F-33's poison-row isolation survives); edge-triggered
  `save_queue_depth` alarm when the never-drop backlog crosses 1000
- F-43: cross-saver transaction API -- `async with db.transaction():` binds
  a context-local session so statements *and* DataSaver flushes join one
  atomic unit; locally a dedicated pooled connection, remotely a
  TTL-bounded (60 s, lazy-reaped) dedicated session in the DB process with
  a 32-session cap; nesting rejected
- F-44: game calendar `day_no`/`week_no` derive from the local calendar
  date instead of a fixed 86,400-second grid -- a pinned DST timezone no
  longer splits one local day into two day numbers or disagrees with
  `NewDayEvent`; anchors stay frozen (F-25)

### Trust model (mesh hardening, round 1)
- F-39: the ZMQ ROUTER drops any message whose claimed `from` service
  number differs from the sender's socket identity (`ipc_spoofed_total`);
  the validated origin now flows through gateway dispatch
- F-40: RPC results/cancels are only accepted from the service the caller
  actually invoked (`rpc_origin_rejects_total{reason}`) -- a guessed
  call_id can no longer resolve a pending future with attacker data, and a
  forged CANCEL can no longer kill arbitrary running calls; the `@fwd`
  proxy envelope carries the original sender (legacy 4-field envelopes
  parse as origin-unknown = untrusted)

### Hot reload
- F-41: rollback now catches `BaseException` (a `SystemExit` from the
  re-executed top level used to leave the module half-updated); protocol
  dunder signatures (`__exit__`/`__call__`/`__aiter__`/...) and module-level
  `__getattr__` are validated like any other callable (name-mangled privates
  stay exempt -- their live names cannot be matched statically)

### Maintainability / docs
- F-45: ServerRuntime split -- clock-event derivation, the DB topology
  layer, the operator surface (monitor/metrics/console/watcher) and the
  shutdown sequence moved to `runtime_wiring.py` as explicit collaborators;
  the composition root is a ~500-line orchestrator instead of a ~650-line
  god class
- `docs/deployment.md`: the trust-model decision (safe on single-operator
  trusted networks; CURVE/ZAP + TLS required for anything hostile), the
  Windows selector-loop ~512 fd ceiling and its mitigation ladder, and the
  sub-process metrics gap

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
