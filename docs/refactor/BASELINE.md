# Refactor Baseline (WP-00)

Executable safety perimeter for the restructuring work package in
`codereview1.md`. Everything here is measured from the snapshot below;
`scripts/architecture-report.py` regenerates the structural numbers.

## Snapshot

- **Commit:** `685ea0354f123864154b46e585f9c22664929763` (`slate` branch)
- **Package:** `streams-slate` `0.2.0-rc.4`, Rust edition 2024
- **SlateDB pin:** crates-io patch to upstream rev
  `0717cc1e4e9bad10a4773760f66bac4264ecf05e` (Cargo.toml `[patch.crates-io]`)
- **Toolchain:** CI uses `dtolnay/rust-toolchain@stable` (resolves at run
  time); baseline measured locally with `rustc 1.93.0 (254b59607 2026-01-19)`,
  `cargo 1.93.0`. `Cargo.lock` is committed.

## Baseline gate result (this commit, this machine)

`bash scripts/gate.sh` — **GATEDONE**:

- actionlint: not installed locally (CI enforces via workflow-lint.yml)
- RC evidence verifier self-test: 15/15 mutations refused
- full release suite (`cargo test --release --bin streams-slate -- --skip
  post_split_throughput_scales`): **600 passed, 0 failed** (87.98 s)
- capacity mechanism gate (`post_split_throughput_scales`, isolated):
  **1 passed** (19.96 s)
- clippy `--release --all-targets`: no new warning fingerprints
- multitenancy audit: MT_AUDIT_OK (baseline residuals: internal-target 10,
  stream-hash 53, tenant-fallback 6)

## Structural metrics (scripts/architecture-report.py)

Machine-readable snapshot: `docs/refactor/architecture-baseline.json`.

| Measure | Baseline |
|---|---:|
| Rust files | 53 |
| Rust lines | 100,890 |
| files over 1,000 lines | 20 (18 production + dst.rs/dst_tests.rs) |
| functions over 200 lines | 46 |
| `crate::http` references outside `src/http.rs` | 296 (67 of them in dst tests) |
| direct `std::env::var`/`var_os` reads | 71 (52 outside main.rs/bins) |
| Axum type refs outside transport adapters | 363 |
| raw key-tag construction sites | 27 |
| mutable process statics (Atomic/Mutex/RwLock/OnceLock/LazyLock) | 138 |

Largest functions (review-cited spans reproduced exactly):

| Function | Span | Lines |
|---|---|---:|
| `ShardEngine::commit_group` | src/shard.rs:2422-4527 | 2,106 |
| `append_core` | src/http.rs:4942-6049 | 1,108 |
| `create_stream` | src/http.rs:3235-4279 | 1,045 |
| `async_main` | src/main.rs:1720-2515 | 796 |
| `fleet::start` | src/fleet.rs:376-1137 | 762 |
| `sse::session::serve` | src/sse/session.rs:159-884 | 726 |
| `read_v3_lineage_inner` | src/http.rs:7711-8276 | 566 |
| `read_inner` | src/http.rs:6684-7133 | 450 |
| `Absorber::start` | src/history.rs:894-1298 | 405 |
| `product_consumer_delete` | src/product.rs:5848-6251 | 404 |

## CI and gate inventory

GitHub Actions (`.github/workflows/ci.yml`, pushes to main/slate + PRs):

- **rust** — `cargo check --all-targets`; legacy-symbol resurrection check;
  multitenancy audit; multitenancy identity lint; full release suite
  (product conformance + dual-surface + DST + cost budgets, skipping
  `post_split_throughput_scales`); capacity mechanism gate isolated.
- **livefeed** — `sse::` unit legs + `livefeed_` HTTP legs.
- **noisy-campaign** (nightly 03:17 UTC) — `scripts/mt-noisy-campaign.mjs`,
  locked thresholds.
- **platform-e2e** — `scripts/platform-e2e.mjs` (TS control-plane emulator).
- **livefeed-fleet-cert** — `bench/fleet/livefeed-cert.sh` (three-process
  fleet certification).
- **mt-cert-1000** — `shared_cell_certification_smoke` at 1,000 projects.
- **sdk-package** — SDK tarball build/install/smoke on Node 18 + current,
  Bun, Deno.
- **durable-streams-server-conformance** — pinned upstream Durable Streams
  suite, unmodified, fresh namespace (`conformance/`).
- **product-field-gate** — `scripts/field-gate.mjs` (20 checks, real scaler
  split in CI).
- **workflow-lint** (`.github/workflows/workflow-lint.yml`) — actionlint
  pinned by version + sha256, checkout pinned by SHA.

Local/release scripts (`scripts/`):

- `gate.sh` — the commit gate (fmt, suite, capacity, clippy fingerprints,
  MT audit, RC-evidence verifier self-test, actionlint when installed).
- `release-gate.sh` — local half of the release gate (fmt, clippy
  fingerprint gate, workflow lint, binary suite, supply-chain checks).
- `rc-certify.sh`, `promote-rc.sh`, `release-provenance.sh` — RC promotion
  and provenance verification.
- `verify-rc-evidence.py --self-test` — RC evidence verifier (runs in
  gate.sh every commit).
- `multitenancy-audit.sh` + `mt-audit-baseline.txt` — fingerprint-inventory
  audit; `src/mt_lint.rs` — syn-based identity lint (zero unmarked
  residuals).
- `clippy-fingerprints.py` + `clippy-baseline-fingerprints.txt` — warning
  fingerprint baseline.

## Performance/cost comparison baseline

Selected as the reference for refactor PRs (WP-11 performance gates):

- **Functional/cost in-suite:** the release suite above includes the
  request-cost budget tests and dual-surface corpus; `post_split_throughput_scales`
  is the isolated capacity gate (threshold 1.8x; in-suite measurements run
  1.73-1.80, so it must stay isolated).
- **Latency/throughput:** `BENCHMARKS.md` (append/read tables through
  s3lite, 25 ms injected store latency; driver `src/bin/bench.rs`).
- **Cost campaigns:** `docs/COST-CAMPAIGN-1.md`, `docs/COST-CAMPAIGN-2.md`,
  `docs/COST-AB1.md`, `docs/COST-WIDE1.md`, `docs/COST-WIDE2.md`,
  `docs/COST-METHODOLOGY.md`; idle-cost pin test
  `idle_engine_store_traffic_is_bounded_by_the_poll_cadence`.
- **Capacity reports:** `docs/CAPACITY-R26.md`, `docs/CAPACITY-R27.md`.
- **Soak/field:** `docs/SOAK-*.md`, `bench/results-2026-07-22/`,
  `bench/fra-ab-baseline.md`.

## Companion artifacts (WP-00 deliverables)

- `scripts/architecture-report.py` — warning-only structural report
  (this baseline's generator); runs in CI without failing until WP-17.
  PR 3.1 added `--self-test` and `--baseline-diff` (NEW/RESOLVED vs the
  snapshot below), both run in CI on every push.
- `docs/refactor/architecture-baseline.json` — machine-readable snapshot.
- `docs/refactor/WIRE-MATRIX.md` — wire characterization matrix for both
  HTTP surfaces. NOTE (PR 3.1): this is an *inventory* of current
  behavior, not a pin — routes gain executable characterization tests as
  they move (PR rule 4), pinned against this matrix.
- `docs/refactor/test-scenario-map.json` — scenario-ID ↔ test inventory
  (authoritative). `docs/refactor/SCENARIO-MAP.md` is GENERATED from it
  by `scripts/scenario-map-report.py` (validation: same IDs as the
  catalogue, unique, mapped-iff-tests, coverage values, test-symbol
  existence). Current counts: 189 inventoried, 138 mapped (111 full,
  25 partial, 2 external), 51 unmapped.
- Storage layout-4 golden tests (37, `src/golden_tests.rs`) and the
  `TraceStore` object-store trace adapter (`src/dst.rs`) live in the
  test tree. PR 3.1 removed one ambient-time cursor test from the
  golden suite (now an exact pin on `http::interval_cursor_at`) and
  rewrote `TraceStore::delete_stream` to delegate exactly once (its
  first version fanned calls out per item and could fabricate a
  success); `reset()` refuses while operations are in flight.
- `docs/refactor/COMMIT-ORDER.md` — the five commit-order properties
  mapped to their pinning tests.
