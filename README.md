# pyline

Production-grade asyncio game-server framework. Complete ground-up rebuild of the
`ServerLite`/`frameaio` prototype: same proven architecture (boot state machine,
in-place hot reload, dirty-flag auto-save ORM, multi-process split with a DB
proxy process), rebuilt to production standards.

## Non-negotiable engineering rules

- Python 3.12 baseline (CI also runs 3.13).
- PEP 8 enforced by `ruff format` + `ruff check` (line length 100).
- 100% type annotations, `mypy --strict` clean.
- No bare `except:` anywhere. Recoverable errors are caught narrowly and logged;
  everything else fails fast.
- No module-level singleton globals and no import side effects: runtime state
  lives in an explicit `Context` object.
- No monkeypatching of `builtins.print`, `sys.settrace`, `warnings`, or the
  global logging manager.
- Secrets (DB passwords, inter-server tokens) come from environment variables
  or a git-ignored secrets file, never from committed config.
- Network payloads are `msgpack`; `pickle` never crosses a process boundary.

## Layout

```
src/pyline/
    config/     JSON5 + pydantic settings, server registry, secrets
    log/        structlog setup (no global hijacking)
    core/       event bus, boot state machine, supervisor, scheduler, game clock
    net/        frame codec, connections, gateway, RPC, ZeroMQ bus, cross-server proxy
    db/         MySQL pool, Redis, schema migration, ORM, auto-save, serialization
    reload/     in-place hot reload with sandbox prevalidation
    obs/        Prometheus metrics, event-loop latency monitor
    devtools/   safe-by-default console, file watcher
template/      business-project template (events skeleton, configs, entry point)
tests/         unit + integration suites
```

## Development

```bash
uv sync --extra dev
uv run pytest
uv run ruff check src tests
uv run mypy src tests
```

## Status

v1.0.0-rc.2: P0-P5 core plus the full migration-finish pass (F-01..F-30 in
`docs/migration-plan.md` -- data safety, network robustness, kernel,
hot-reload guards, business facade) and nine review passes on top
(F-31..F-164 -- see `CHANGELOG.md`). The seventh pass closed every finding
of the second external assessment; the eighth (F-142..F-154) closed the
third; the ninth (F-155..F-164) closed every finding of a full-repo
assessment: the ORM now works in the multi-process topology (pool-less
TableCatalog backs savers in business processes), a decode failure during
load resolves joiners instead of hanging them, the coalesced flush drops
the journal hold, plain containers become tracked automatically on
set_data/load, the inbound RPC task pool is bounded, a sub-process death
escalates to a main-runtime shutdown even when the callback fails, the
Windows fd ceiling and the production inter_token separation are enforced
at boot, and the hot-reload validator compares default POSITIONS, not
counts. The tenth pass (F-165..F-189) closed every remaining finding of
the repo evaluation and added the transport-security layer the trust model
was missing: TLS on the client listener and the proxy plane (mutual mode
for server-to-server links) and CURVE+ZAP on the ZMQ bus, plus
inserted-value tracking in the ORM containers, closure-aware hot reload
for decorated functions, ROUTER-confirmed bus authentication, bounded
dials, rate-limited hostile-traffic logging, and the data-layer
race/performance fixes. See `docs/plan.md` (master plan),
`docs/hot-reload.md` (reload contract and known limits) and
`docs/deployment.md` (trust model and platform limits before going
public).
