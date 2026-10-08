# Deployment decisions

The framework's current trust model and platform limits, and what a
deployment must decide before crossing them. Written after the F-38..F-45
review pass; revisit when the answers change.

## Trust model: which network am I safe on?

The inter-process/inter-server mesh is *authenticated by identity binding,
not by encryption*:

- The TCP handshake authenticates the **client** via a shared token
  (`hmac.compare_digest`); the server never proves itself to the client.
- The ZeroMQ bus (F-39) drops any message whose claimed `from` service
  number does not match the sender's socket identity, and RPC (F-40) only
  accepts results/cancels from the service the caller actually invoked --
  but anyone who can *reach* the bus endpoint or the proxy port can still
  connect and speak as themselves.
- The `@fwd` proxy plane validates peers by token + IP allowlist + IDENT
  machine claim (F-16).

**Safe today**: a single-operator cluster on trusted hosts (dedicated LAN,
cloud VPC, containers with a private network). This is the design target.

**Not safe today**: multi-tenant hosts (the POSIX ZMQ endpoint is a
filesystem path in `/tmp`), untrusted LANs, or anything touching the public
internet. Before crossing those lines the mesh needs transport security:

- ZMQ plane: CURVE + ZAP (`zap_domain`, per-peer public keys) -- libzmq
  ships both, pyzmq exposes them;
- TCP planes (client listener + proxy): TLS (server certs at minimum,
  mutual for the proxy plane).

These are deliberate non-goals of the current milestone: they change the
config surface (key distribution) and belong to a deployment-driven pass,
not a code-quality pass.

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

Only the main process exports Prometheus (`metrics_port`). Sub-process
metrics exist in-process but have no exporter. Options: per-process ports
(port budget needed), or prometheus multiprocess mode (requires a shared
dir + `PROMETHEUS_MULTIPROC_DIR` lifecycle handling). Decide before relying
on sub-process metrics in alerting.
