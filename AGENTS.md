# Prisma Streams Agent Guide

## Scope

These instructions apply to the whole repository.

## Source Of Truth

- Start with [docs/index.md](docs/index.md) and [docs/overview.md](docs/overview.md).
- Treat documentation as authoritative. If code changes behavior, update the relevant docs in the same change.
- For HTTP or protocol semantics, [docs/durable-streams-spec.md](docs/durable-streams-spec.md) wins.
- If behavior is ambiguous, ask the user or record the decision in [docs/assumptions.md](docs/assumptions.md) and cover it with a test.

## Read Before You Change

- Core architecture and storage: [docs/architecture.md](docs/architecture.md), [docs/sqlite-schema.md](docs/sqlite-schema.md), [docs/tiered-index.md](docs/tiered-index.md)
- Live / touch: [docs/live.md](docs/live.md), [docs/prisma-dev-pglite-live.md](docs/prisma-dev-pglite-live.md)
- Local mode and package surfaces: [docs/local-dev.md](docs/local-dev.md), [docs/overview.md](docs/overview.md), [docs/releasing.md](docs/releasing.md)
- Schemas and lenses: [docs/schemas.md](docs/schemas.md), [docs/durable-lens-v1-schema.md](docs/durable-lens-v1-schema.md)
- Ops, recovery, and performance: [docs/operational-notes.md](docs/operational-notes.md), [docs/recovery-integrity-runbook.md](docs/recovery-integrity-runbook.md), [docs/metrics.md](docs/metrics.md), [docs/live-load-tests.md](docs/live-load-tests.md), [docs/segment-performance.md](docs/segment-performance.md), [docs/routing-key-performance.md](docs/routing-key-performance.md)
- Security posture: [docs/auth.md](docs/auth.md), [docs/security.md](docs/security.md)
- Workflow and validation: [docs/contributing.md](docs/contributing.md), [docs/conformance.md](docs/conformance.md), [docs/better-result-adoption.md](docs/better-result-adoption.md)

## Implementation Rules

- Do not preserve legacy fallback behavior, compatibility shims, dual paths, deprecated config aliases, or old endpoints unless the user explicitly asks for them.
- Prefer one supported code path, one API shape, and one documented behavior.
- Keep request handlers small: validate input, do indexed lookups, enqueue work, and wait. Slow work belongs in background loops.
- Never move segment building, uploads, index compaction, or large WAL scans into the request path.
- `uploaded_through` must not advance before manifest publication; manifest upload is the remote visibility commit point.
- Respect the full-vs-local split. Local mode is single-SQLite, loopback-oriented, and has no segmenting or object store.
- Do not assume built-in auth exists or add public-internet deployment assumptions.
- For Live, keep the current model: State Protocol input, invalidation only, in-memory touch journal, no sqlite touch mode, no database-specific WAL decoding inside core.
- For schema evolution, preserve the documented rules: first schema install requires an empty stream, later upgrades require `v -> v+1` lenses, and routing-key derivation rules must stay consistent.
- In `src/`, use `better-result` for expected runtime failures. Do not add throw-based control flow for normal error handling.

## Testing And Verification

- For behavior changes, add or update tests in the same change.
- For bug fixes, reproduce with a test first when practical.
- Default verification baseline: `bun run typecheck`, `bun run check:result-policy`, `bun test`
- If HTTP or protocol behavior changed, also run `bun test test/conformance.test.ts` and the relevant suites from [docs/conformance.md](docs/conformance.md).
- If package surfaces or build outputs changed, run `bun run test:node-local-package` and/or `bun run test:bun-server-package`, and update [docs/releasing.md](docs/releasing.md) if the contract changed.
- If Live behavior or throughput changed materially, run a focused demo or load test from [docs/live-load-tests.md](docs/live-load-tests.md) and report what you verified.

## Documentation Rules

- Repository documentation lives under `docs/` and uses lowercase kebab-case filenames.
- New product or engineering docs belong in `docs/`; keep the repo root for policy files like `AGENTS.md` and licensing or notice files.
- Update the specific docs affected by the change, not just the docs hub.
- If you add or rename a doc, update links and [docs/index.md](docs/index.md).
- Keep docs focused on the supported system only; remove obsolete behavior instead of documenting legacy alternatives.
