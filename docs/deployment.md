# Deployment decisions

The framework's current trust model and platform limits, and what a
deployment must decide before crossing them. Written after the F-38..F-45
review pass, updated after F-46..F-57, the fifth review pass (F-58..F-105)
and the eighth pass (F-142..F-154); revisit when the answers change.

## Trust model: which network am I safe on?

The inter-process/inter-server mesh is *authenticated by identity binding,
not by encryption*:

- The TCP handshake is a **mutual** HMAC challenge-response (F-123, F-147):
  the server sends a random 16-byte nonce, the client answers
  `HMAC-SHA256(token, nonce)` plus its own fresh nonce, and the server's
  `@welcome` carries `HMAC-SHA256(token, client_nonce)` -- each side proves
  it holds the token, so a machine-in-the-middle can no longer impersonate
  the server and relay cleartext application frames. The token itself never
  crosses the wire, and a captured digest is useless on any other connection
  (fresh nonces each time). Traffic is still unencrypted -- payload
  confidentiality needs TLS (below). Pre-authentication the decoder accepts
  only `preauth_max_frame` (64 KiB default, F-124) instead of the full
  16 MiB frame budget, and every inbound msgpack decode is element/size-
  capped per container AND by a running total-element budget (F-125, F-146)
  -- a dense frame of tiny values used to pass each per-container cap while
  expanding ~30x the wire size in Python objects. The client listener caps
  concurrent connections, globally and per peer IP (F-72,
  `socket.max_connections` / `socket.max_connections_per_ip`).
- The ZeroMQ bus requires an HMAC handshake before any data flows (F-126):
  a DEALER must prove it holds the inter-server token against a per-
  connection nonce, and the ROUTER drops every frame from identities that
  never completed it (mutually authenticated -- AUTH1 also proves the ROUTER
  to the DEALER). The handshake re-runs on every transport-level reconnect
  (F-143): a restarted main-process ROUTER starts with an empty
  authentication table, and its DEALERs re-authenticate via the ZMQ monitor
  event (plus a 30 s keepalive re-offer as the belt-and-suspenders path) --
  previously every frame from every running sub-process was silently
  dropped until each child restarted. Junk identities from local AUTH0 spam
  can no longer occupy the bounded destination table (F-144: pre-auth
  slots are evicted first under pressure). On top of that, F-39 drops any
  message whose claimed `from` service number does not match the sender's
  socket identity, and RPC (F-40) only accepts results/cancels from the
  service the caller actually invoked. Per-destination queues are bounded
  by bytes as well as message count (F-127: `zeromq.queue_bytes`, 64 MiB
  default), so a slow peer cannot pile ~16 GiB of 16 MiB frames into one
  queue; RPC CALLs refused by a full queue fail fast at the caller instead
  of timing out 10 s later (F-128) -- and since F-145 the same fail-fast
  applies when no proxy link is available for a cross-server call. On POSIX
  the `ipc://` endpoint file is additionally chmod 0600 right after bind
  (F-75). A bare-path `bind_file` (pre-F-105 configs) is normalized to
  `ipc://` automatically.
- The `@fwd` proxy plane validates peers by token + IP allowlist + IDENT
  machine claim (F-16), and binds each envelope's claimed origin to the
  sending connection's registered machine at the first hop (F-48) -- a
  connected machine can no longer sign its frames as another machine.
  Sub-processes reach the cross-machine plane through their own main
  process via `@relay` (F-70); the relay re-validates that a claimed origin
  belongs to the sending machine's local mesh, so a sub-process cannot
  launder a third machine's identity.
- The DB proxy's RPC face is **full SQL passthrough** by design: any
  process that can speak on the mesh can execute arbitrary SQL with the DB
  process's credentials. The mesh identity checks (F-39/F-40/F-48/F-70)
  bound *who* can get there (your own processes), not *what* they can do.
  **Token separation is therefore load-bearing**: when
  `socket.inter_token` is unset it falls back to the CLIENT token (F-16) --
  in that single-token configuration every game client holds the secret
  that authenticates server-to-server links and the DB SQL plane, and the
  blast radius of any client-token leak is the entire database. Since
  F-160 the fallback is **enforced**: `srv_type=production` refuses to boot
  without an explicit `$env:`-referenced `inter_token` (develop mode keeps
  the once-per-process warning). Do not bridge the mesh onto a network you
  do not fully control.

**Safe today**: a single-operator cluster on trusted hosts (dedicated LAN,
cloud VPC, containers with a private network). This is the design target.

**Not safe today**: multi-tenant hosts, untrusted LANs, or anything
touching the public internet. Before crossing those lines the mesh needs
transport security:

- ZMQ plane: CURVE + ZAP (`zap_domain`, per-peer public keys) -- libzmq
  ships both, pyzmq exposes them (the F-126 HMAC handshake already removes
  the plaintext-token and identity-forgery exposure; CURVE adds
  confidentiality and per-peer keys instead of one shared secret);
- TCP planes (client listener + proxy): TLS (server certs at minimum,
  mutual for the proxy plane). The F-123 handshake already keeps the token
  off the wire and blocks digest replay; TLS adds confidentiality and
  server certificates.

These are deliberate non-goals of the current milestone: they change the
config surface (key distribution) and belong to a deployment-driven pass,
not a code-quality pass.

## Data-safety semantics (RPO and transactions)

What a deployment can expect to lose, and where the transaction guarantees
actually hold:

- **RPO on crash**: all un-flushed in-memory state. Dirty rows are flushed
  on the auto-save interval; a `kill -9` / power loss loses the current
  interval plus whatever was queued. Graceful shutdown drains the queue
  under a deadline and exits with code 3 (F-01) naming every row it could
  not save -- but those rows exist only in the log after the process dies;
  there is no emergency spool. If that window is too large, lower the
  auto-save interval; if it must be zero, that is a product decision the
  framework currently does not make for you.
- **Transaction atomicity** holds for mutations made through
  `db.transaction()` blocks (F-43/F-63): marks are deferred until the unit
  ends, the background auto-save skips savers held by an active journal,
  and both COMMIT failure (F-60) and rollback (F-50) re-mark every affected
  saver so memory reconverges on the next flush. A remote COMMIT that times
  out is reconciled once against the DB process's recorded outcome before
  being treated as failed (F-61) -- a slow-but-successful commit is no
  longer retried as a double-write.
- **Ambient binding caveat**: the transaction contextvar propagates to
  tasks *created inside* the `async with db.transaction():` block. A
  long-lived worker task created *before* the block silently bypasses it
  (its `db.execute` runs on the ambient pool, not the transaction session).
  Create workers inside the block, or pass the session explicitly.

## Windows: the selector-loop fd ceiling

pyzmq rejects the IOCP proactor loop, so on Windows the runtime installs
`WindowsSelectorEventLoopPolicy` (`net/loop_policy.py`). The selector loop
uses `select()`, which on Windows is hard-capped around **512 file
descriptors** (sockets) per process.

Practical ceilings per process, on Windows:

- one fd per client TCP connection, plus the ZMQ sockets, the MySQL/Redis
  connections, the metrics HTTP server and stdin/stdout;

so a few hundred concurrent clients per process is the realistic budget
before `select()` raises. Since F-159 the budget is **enforced at bind
time**: a listener whose `socket.max_connections` exceeds the ceiling
(512 minus a 64-fd reserve for the bus/proxy links/stdio, i.e. 448) fails
boot with an actionable ConfigError instead of crashing the loop under
load. Note the check is per listener (client and proxy each); a Windows
machine running BOTH listeners should budget the caps together. Options
when a deployment outgrows it:

1. **Scale out, not up** (recommended): split clients across more processes
   / servers -- the topology already supports it (ZMQ bus + proxy plane).
2. Move the client listener into a separate gateway tier on Linux/uvloop
   and relay internally.
3. Revisit loop support: run the ZMQ sockets on a dedicated thread's
   selector loop and the client listener on proactor -- a kernel change,
   not a config change; needs its own review.

## Windows: the ZMQ bus endpoint is loopback TCP

There is no `ipc://` transport on Windows, so the bus defaults to
`tcp://127.0.0.1:<port>` (`zeromq.bind_host`, `config/models.py`). Unlike
POSIX -- where the `ipc://` socket file is chmod 0600'd right after bind
(F-75) -- a loopback TCP endpoint has no OS-enforced access control. The bus
now carries its own authentication (F-126): a local process without the
inter-server token cannot place frames on the bus regardless of which
identity it claims, so the previous "any same-uid process can impersonate
any service" exposure is closed. What remains true: the token is a shared
secret readable by every process of the deploying user, traffic on the
loopback socket is unencrypted, and a same-uid process that reads the token
out of config can speak as anyone. The assumption a deployment actually
relies on is still **single-user host**; multi-user Windows hosts need the
CURVE+ZAP transport-security pass from the trust-model section above before
the bus endpoint is safe.

## Sub-process metrics

The main process exports Prometheus on `metrics_port` (F-28/F-54). With
`metrics_all_processes: true` (F-137) every sub-process ALSO exports its own
registry on `metrics_port + process_index` -- the "per-process ports"
option this document used to leave open. The endpoint binds `127.0.0.1` by
default; set `metrics_bind` and a `metrics_token` (bearer, secret reference)
before exposing it on a network you trust -- the token gates scraping, it is
not TLS.

Instrumented across processes (F-136): event-loop latency, connections,
RPC latency/timeouts and origin rejections, dispatch errors, IPC
safety counters, auto-save queue/flushes/failures, child liveness and exit
reasons (main process), boot phase durations, scheduler timer backlog,
MySQL pool saturation and acquire timeouts, schema migration outcomes, and
hot-reload results.
