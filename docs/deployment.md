# Deployment decisions

The framework's current trust model and platform limits, and what a
deployment must decide before crossing them. Written after the F-38..F-45
review pass, updated after F-46..F-57 and the fifth review pass (F-58..F-105);
revisit when the answers change.

## Trust model: which network am I safe on?

The inter-process/inter-server mesh is *authenticated by identity binding,
not by encryption*:

- The TCP handshake authenticates the **client** via a shared token
  (`hmac.compare_digest`); the server never proves itself to the client.
  The token is static (no nonce/challenge), so it is replayable by anyone
  who captures it on the wire -- another reason the plane must stay on a
  trusted network. The client listener caps concurrent connections, globally
  and per peer IP (F-72, `socket.max_connections` /
  `socket.max_connections_per_ip`), so the handshake cost can no longer be
  used for FD/task exhaustion.
- The ZeroMQ bus (F-39) drops any message whose claimed `from` service
  number does not match the sender's socket identity, and RPC (F-40) only
  accepts results/cancels from the service the caller actually invoked --
  but anyone who can *reach* the bus endpoint can still connect and speak
  as themselves. On POSIX the `ipc://` endpoint file is chmod 0600 right
  after bind (F-75), so other local users can neither snoop nor inject;
  the residual same-host exposure is root / same-uid processes. A bare-path
  `bind_file` (pre-F-105 configs) is normalized to `ipc://` automatically.
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
  Do not bridge the mesh onto a network you do not fully control.

**Safe today**: a single-operator cluster on trusted hosts (dedicated LAN,
cloud VPC, containers with a private network). This is the design target.

**Not safe today**: multi-tenant hosts, untrusted LANs, or anything
touching the public internet. Before crossing those lines the mesh needs
transport security:

- ZMQ plane: CURVE + ZAP (`zap_domain`, per-peer public keys) -- libzmq
  ships both, pyzmq exposes them;
- TCP planes (client listener + proxy): TLS (server certs at minimum,
  mutual for the proxy plane).

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
before `select()` raises. Options when a deployment outgrows it:

1. **Scale out, not up** (recommended): split clients across more processes
   / servers -- the topology already supports it (ZMQ bus + proxy plane).
2. Move the client listener into a separate gateway tier on Linux/uvloop
   and relay internally.
3. Revisit loop support: run the ZMQ sockets on a dedicated thread's
   selector loop and the client listener on proactor -- a kernel change,
   not a config change; needs its own review.

## Sub-process metrics are not scrapeable

Only the main process exports Prometheus. The endpoint binds `127.0.0.1` by
default (F-54); set `metrics_bind` and a `metrics_token` (bearer, secret
reference) before exposing it on a network you trust -- the token gates
scraping, it is not TLS. Sub-process metrics exist in-process but have no
exporter. Options: per-process ports (port budget needed), or prometheus
multiprocess mode (requires a shared dir + `PROMETHEUS_MULTIPROC_DIR`
lifecycle handling). Decide before relying on sub-process metrics in
alerting.
