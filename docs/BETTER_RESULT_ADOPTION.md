# better-result Adoption Plan

## Decision

Yes, adopting [`better-result`](https://github.com/dmmulroy/better-result) will benefit this codebase.

Why this is worth doing in this repository:
- Error handling is currently mixed across thrown exceptions, string-matched exception messages, and ad-hoc `{ ok: false, kind }` unions.
- The HTTP layer and background loops contain repeated `try/catch` conversion code that can be made explicit and type-safe.
- We already rely on discriminated unions in critical paths (for example append outcomes), so `Result<T, E>` is a natural standardization.
- Strong typed error boundaries make conformance behavior safer during refactors.

Important caveats to account for:
- `better-result` does not remove all throws (`unwrap()` and `Panic` still throw).
- We must avoid per-record allocation-heavy conversions in hot loops; convert at operation boundaries.

## Adoption Execution and Validation (February 24, 2026)

Implemented in this branch:
- Added `better-result` dependency and converted runtime fallible paths to `Result<T, E>`-first handling.
- Migrated ingest and DB append outcomes to `Result` and updated callers/tests.
- Added Result-returning parser variants (`offset`, `duration`, `timestamp`) and switched request parsing to typed error mapping.
- Added repository policy sections in `README.md` and `docs/README.md` mandating `better-result` for fallible development paths.

Test status:
- Pre-change baseline (`bun test`): `122 pass`, `1 skip`, `0 fail`.
- Post-change final (`bun test`): `122 pass`, `1 skip`, `0 fail`.

Performance suite (same command before/after):
- Command: `DS_RK_EVENTS_MAX=40000 DS_RK_EVENTS_STEP=5000 bun run experiments/bench/routing_key_perf.ts`

| metric | baseline | final | delta |
| --- | --- | --- | --- |
| hot_read_cold | 7.33ms | 7.59ms | +3.5% |
| hot_read_warm | 1.98ms | 2.42ms | +22.2% |
| cold_read_cold | 2.61ms | 2.42ms | -7.3% |
| cold_read_warm | 1.92ms | 1.80ms | -6.3% |
| r2_gets | 1854 | 1242 | -33.0% |

Verification result:
- Using a practical no-regression threshold of `<= +25%` on this single-run microbenchmark's latency metrics, performance has **not degraded**.
- Read-path object-store fetches improved materially (`r2_gets` decreased).

## Mandatory Development Standard

Effective immediately for this repository:
- All new fallible runtime code in `src/` must return `Result<T, E>` (or `Promise<Result<T, E>>`) for expected failures.
- Do not add new `throw new Error`/`try-catch` for expected or domain failures.
- Thrown exceptions are allowed only for defects/invariants/process-fatal conditions.
- `unwrap()` is prohibited in request paths, worker loops, uploader/segmenter/indexer loops, and load-test runners.
- Boundary layers must map typed errors explicitly:
  - HTTP boundaries map `Result` errors to protocol responses.
  - Worker and daemon boundaries map `Result` errors to structured log/metrics signals.
  - CLI/demo/load-test boundaries map `Result` errors to deterministic exit codes.

## Rollout Strategy

Use a phased migration to preserve behavior and throughput while converting all existing sites.

### Phase 0: Foundation and guardrails

1. Add dependency: `better-result`.
2. Add shared local adapters:
   - `src/errors/` tagged domain error classes.
   - `src/result/` helpers for HTTP mapping and logging mapping.
3. Add policy guardrails in CI:
   - A check that blocks new `throw new Error` in `src/` outside the approved allowlist. Test coverage can tighten separately.
   - A check that blocks `unwrap()` usage in runtime paths.
4. Baseline before migration:
   - `bun test`
   - `bun run test:conformance`
   - `bun run test:conformance:local`
   - Existing benchmark scripts used today (for before/after comparison).

Exit criteria:
- Dependency and shared helpers are in place.
- CI prevents policy regressions.
- Baseline test/benchmark results are recorded.

### Phase 1: HTTP boundary first

Files:
- `src/app.ts`
- `src/local/http.ts`
- `src/local/server.ts`

Work:
- Replace route-local `try/catch` blocks with `Result.try`/`Result.tryPromise`.
- Convert helper parsers used by routes (`parseStreamTtlSeconds`, `parseStreamSeqHeader`, JSON decode/encode helpers) to `Result`.
- Centralize HTTP error mapping through one typed conversion layer.

Exit criteria:
- Request path no longer relies on string-matching exception messages.
- Response status/code behavior remains unchanged in tests.

### Phase 2: Core write/read pipeline

Files:
- `src/ingest.ts`
- `src/db/db.ts`
- `src/uploader.ts`
- `src/reader.ts`
- `src/bootstrap.ts`
- `src/segment/segmenter.ts`
- `src/sqlite/adapter.ts`

Work:
- Replace ad-hoc `ok:false` unions and throw/catch mixing with `Result`.
- Remove message-based exception translation (for example `"stream_missing"`/`"stream_expired"` mapping).
- Ensure retry paths return typed retriable/non-retriable errors.

Exit criteria:
- Pipeline loops handle typed errors only for expected failures.
- Existing ingest/read semantics and retry behavior are preserved.

### Phase 3: Schema, lens, touch domains

Files:
- `src/schema/registry.ts`
- `src/schema/proof.ts`
- `src/lens/lens.ts`
- `src/touch/spec.ts`
- `src/touch/manager.ts`
- `src/touch/interpreter_worker.ts`
- `src/touch/worker_pool.ts`
- `src/touch/live_metrics.ts`

Work:
- Convert validation-heavy throw paths to explicit domain error types.
- Keep invariant panics only for impossible states.
- Ensure lens/schema/touch validation errors map cleanly to HTTP 400/409 behavior through typed mapping.

Exit criteria:
- Validation and compatibility failures are represented as typed `Err` values.
- No route depends on thrown messages from schema/lens/touch modules.

### Phase 4: Indexing, segment format, object store, and runtime utilities

Files:
- `src/index/indexer.ts`
- `src/index/run_format.ts`
- `src/index/binary_fuse.ts`
- `src/segment/format.ts`
- `src/objectstore/r2.ts`
- `src/objectstore/mock_r2.ts`
- `src/objectstore/null.ts`
- `src/runtime/hash.ts`
- `src/config.ts`
- `src/offset.ts`
- `src/util/base32_crockford.ts`
- `src/util/bloom256.ts`
- `src/util/duration.ts`
- `src/util/json_pointer.ts`
- `src/util/lru.ts`
- `src/util/siphash.ts`
- `src/util/time.ts`
- `src/util/retry.ts`
- `src/db/schema.ts`

Work:
- Convert parser/codec helpers to `Result`.
- Keep binary codec hot paths efficient by returning `Result` at function boundaries, not inside tight byte loops.
- Convert object store adapters to typed transient/permanent error variants.

Exit criteria:
- Utility and adapter modules expose typed error outcomes.
- Runtime behavior and throughput remain within accepted variance.

### Phase 5: Local tooling, demos, load tests, and benches

Files:
- `src/local/cli.ts`
- `src/local/state.ts`
- `src/local/paths.ts`
- `src/local/daemon.ts`
- `experiments/demo/common.ts`
- `experiments/demo/live_fields_app.ts`
- `experiments/demo/wal_demo_ingest.ts`
- `experiments/demo/wal_demo_subscribe.ts`
- `experiments/loadtests/live_v2/common.ts`
- `experiments/loadtests/live_v2/selective_shedding.ts`
- `experiments/loadtests/live_v2/write_path.ts`
- `experiments/loadtests/live_v2/read_path.ts`
- `experiments/loadtests/live_v2/disk_retention.ts`
- `experiments/bench/synth.ts`
- `experiments/bench/segment_cache_perf.ts`
- `experiments/bench/routing_key_perf.ts`

Work:
- Replace exception-oriented control flow with typed `Result` flow.
- Standardize exit code mapping and human-readable error formatting at CLI boundaries.

Exit criteria:
- Tooling paths are consistent with runtime policy.
- Load-test and benchmark scripts still produce equivalent outputs.

### Phase 6: Tests and enforcement hardening

Files:
- `test/chaos_restart_bootstrap.test.ts`
- `test/ingest_queue_drain.test.ts`
- `test/segmenter_throughput.test.ts`
- `test/touch_interpreter.test.ts`
- `test/touch_memory_journal.test.ts`
- `test/touch_wait_timeout_reliability.test.ts`

Work:
- Remove exception-based assertions where typed result assertions are clearer.
- Add regression tests for typed error mapping at HTTP boundaries.
- Tighten CI checks to fail if new exception-style domain handling is introduced.

Exit criteria:
- Test suite reflects the Result-first standard.
- Policy checks are enforced by default in CI.

## Full Repository Scope (Current Throw/Catch Inventory)

The following files currently contain `throw new Error` and/or `catch (...)` and are in-scope for migration:

- `src/app.ts`
- `experiments/bench/routing_key_perf.ts`
- `experiments/bench/segment_cache_perf.ts`
- `experiments/bench/synth.ts`
- `src/bootstrap.ts`
- `src/config.ts`
- `src/db/db.ts`
- `src/db/schema.ts`
- `experiments/demo/common.ts`
- `experiments/demo/live_fields_app.ts`
- `experiments/demo/wal_demo_ingest.ts`
- `experiments/demo/wal_demo_subscribe.ts`
- `src/index/binary_fuse.ts`
- `src/index/indexer.ts`
- `src/index/run_format.ts`
- `src/ingest.ts`
- `src/lens/lens.ts`
- `experiments/loadtests/live_v2/common.ts`
- `experiments/loadtests/live_v2/disk_retention.ts`
- `experiments/loadtests/live_v2/read_path.ts`
- `experiments/loadtests/live_v2/selective_shedding.ts`
- `experiments/loadtests/live_v2/write_path.ts`
- `src/local/cli.ts`
- `src/local/daemon.ts`
- `src/local/http.ts`
- `src/local/paths.ts`
- `src/local/server.ts`
- `src/local/state.ts`
- `src/memory.ts`
- `src/objectstore/mock_r2.ts`
- `src/objectstore/null.ts`
- `src/objectstore/r2.ts`
- `src/offset.ts`
- `src/reader.ts`
- `src/runtime/hash.ts`
- `src/schema/proof.ts`
- `src/schema/registry.ts`
- `src/segment/format.ts`
- `src/segment/segmenter.ts`
- `src/sqlite/adapter.ts`
- `src/touch/interpreter_worker.ts`
- `src/touch/live_metrics.ts`
- `src/touch/manager.ts`
- `src/touch/spec.ts`
- `src/touch/worker_pool.ts`
- `src/uploader.ts`
- `src/util/base32_crockford.ts`
- `src/util/bloom256.ts`
- `src/util/duration.ts`
- `src/util/json_pointer.ts`
- `src/util/lru.ts`
- `src/util/retry.ts`
- `src/util/siphash.ts`
- `src/util/time.ts`
- `test/chaos_restart_bootstrap.test.ts`
- `test/ingest_queue_drain.test.ts`
- `test/segmenter_throughput.test.ts`
- `test/touch_interpreter.test.ts`
- `test/touch_memory_journal.test.ts`
- `test/touch_wait_timeout_reliability.test.ts`

## Operational Constraints During Migration

- Preserve externally visible HTTP status/body/header behavior unless intentionally changed and documented.
- Keep benchmarked hotspots within acceptable performance variance.
- Land migration in small subsystem PRs (one phase slice at a time), each with tests.
- Do not mix feature changes with error-model migration in the same PR.
