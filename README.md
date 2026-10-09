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

v1.0.0-rc.3: P0-P5 core, the full migration-finish pass (F-01..F-30), and
ten review passes on top (F-31..F-189 -- see `CHANGELOG.md`). The eleventh
pass (F-190..F-228) closed every finding of a full horizontal evaluation:
the bus-auth identity-inheritance hole (F-190) and the silently-dropped
forwarding leg (F-191) are fixed with regression tests, the client face
grew a real session layer (connection registry, disconnect events,
`api.session`, per-connection fairness), explicit flush/delete can no
longer punch through an open transaction, flush rounds are deadline-bounded
under DB outages, Redis got the mysql-style liveness alarm, the Windows fd
budget is enforced per process, and the docs/CI debts (.pyi stub promise,
api.redis facade row, partition coverage floors, nightly soak) are
settled. See `docs/plan.md` (master plan), `docs/hot-reload.md` (reload
contract and known limits) and `docs/deployment.md` (trust model and
platform limits before going public).
