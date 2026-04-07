# Routing And Lexicon Index Acceleration Architecture

Status: partially shipped acceleration architecture

This document records the routing-key and routing-key lexicon acceleration plan
and distinguishes between what is already shipped and what remains for later
phases.

The current shipped model is documented in:

- [architecture.md](./architecture.md)
- [indexing-architecture.md](./indexing-architecture.md)
- [tiered-index.md](./tiered-index.md)

## Objective

Primary objective:

- achieve up to **100x faster backlog drain** for routing and lexicon indexing
  on large streams

The objective is measured as sustained indexing throughput, not one small
single-stream microbenchmark:

- `indexed_segments_per_second`
- `indexed_payload_bytes_per_second`
- `time_to_catch_up` from a known uploaded backlog

## Scope

In scope:

- routing-key index L0 and compaction
- routing-key lexicon L0 and compaction
- scheduler, worker, and publish path changes needed to raise throughput

Out of scope:

- changing append durability semantics
- making index artifacts the source of truth
- changing read correctness guarantees for unindexed tails

## Shipped Changes

The current runtime already ships these changes from the plan:

- one `GlobalIndexManager` that drains backlog continuously under load instead
  of advancing only once per timer interval
- a shared generic worker pool sized by `DS_INDEX_BUILDERS`
- `DS_ASYNC_INDEX_CONCURRENCY` defaulting to `DS_INDEX_BUILDERS` when unset, so
  the async gate is aligned with worker capacity by default
- one combined routing+lexicon L0 worker pass that reads each leased
  16-segment window once and emits both `.idx` and `.lex`
- a routing-key-only block scan in that combined worker, so L0 build no longer
  materializes append timestamps or payload slices that routing/lexicon do not
  use
- per-window distinct-key fingerprint caching in that combined worker, so
  repeated routing keys are hashed once and reused across repeated records and
  across the paired lexicon build
- local `async_index_actions` observability rows for all async index actions

Those changes remove the largest timer/gating inefficiencies and the duplicate
segment rescans that routing and lexicon previously paid independently.

## Remaining Bottlenecks

1. CPU-heavy hot loops still exist for high-cardinality windows.
   The worker no longer hashes every repeated record, but it still uses JS
   `BigInt` SipHash and map-heavy accumulation for each distinct key that
   reaches the routing path.

2. Repeated lexicon term decode work.
   Lexicon merge/list paths repeatedly decode restart-string terms in
   random-access patterns.

3. Manifest and metadata chatter.
   Frequent small publish/update cycles add overhead relative to useful index
   progress.

4. Segment payload rereads still exist at the L0 boundary.
   Routing+lexicon now share one scan, but they still reconstruct index state
   from full segment payloads instead of segment-time sidecars.

## Target Architecture

### 1. Global Round-robin Work Scheduler

The shipped runtime now uses one `GlobalIndexManager` with one shared worker
pool and one shared async-index gate:

- routing build
- routing-key lexicon build
- exact build
- bundled-companion build
- routing compaction
- routing-key lexicon compaction
- exact compaction

The scheduler round-robins those work kinds. It does not reserve a dedicated
priority lane for routing or lexicon work, and it does not keep a separate
catch-up-vs-compaction priority system.

Shipped implementation detail:

- under backlog, the global manager now keeps draining until no family makes
  progress, yielding cooperatively between rounds and retaining
  `DS_INDEX_CHECK_MS` only as a safety wake-up interval

Design rule:

- under backlog, progress should be bounded by worker capacity and the shared
  async-index gate, not by per-family timer skew or a special-priority lane

### 2. Shared Background Budget

All indexing families now compete for the same bounded background budget:

- one global `DS_INDEX_BUILDERS` worker-pool size
- one shared async-index concurrency gate that defaults to
  `DS_INDEX_BUILDERS` when unset
- one shared segment-locality lease manager

That preserves fairness between unrelated families without moving heavy
segment-backed work back onto the main thread.

### 3. Unified Window Build For Routing + Lexicon

For each uploaded window:

- read/decompress/iterate segment records once in a worker task
- emit both outputs:
  - routing L0 payload (`.idx`)
  - lexicon L0 payload (`.lex`)

This removes duplicate segment scan/decode costs.

Shipped implementation detail:

- the combined worker job is `routing_lexicon_l0_build`
- the main thread still persists routing and lexicon results independently, so
  manifest visibility and family-specific state machines remain unchanged

### 4. Hot-Loop Compute Rewrite

Move the most expensive per-record operations to lower-overhead execution:

- replace JS `BigInt` hash hot path with native or WASM keyed hash
- use typed-array pipelines for fingerprint accumulation and sort/reduce
- introduce streaming lexicon term cursors for merges to avoid repeated
  random-access decode work

### 5. Segment-Time Sidecar Extraction (Required For True 100x)

During segment build, routing keys already pass through memory.
Capture compact sidecars at this point:

- per-segment routing fingerprint sidecar
- per-segment sorted unique key sidecar

Then L0 builders merge sidecars instead of rereading/decompressing full
segment payloads.

This is the key architectural step that enables sustained 100x-class backlog
drain improvement on large histories.

### 6. Batched Publish And Metadata Commit

Reduce overhead from tiny publish units:

- batch run insertion and manifest publication for multiple completed windows
- keep visibility/correctness invariants intact

## Data Model Additions

The sidecar phase introduces new immutable per-segment objects, for example:

- `streams/<hash>/index-segment/<segment-index>.rks` (routing fingerprints)
- `streams/<hash>/index-segment/<segment-index>.lxs` (lexicon keys)

SQLite tracks sidecar availability and generation alignment so backlog jobs can
use sidecars when present and fall back to full segment scans when absent.

## Rollout Plan

### Phase 0: Instrumentation And Baseline

Deliverables:

- per-window timing breakdown (`segment_load`, `decode`, `hash`, `dedupe`,
  `encode`, `put`, `publish`)
- repeatable backlog-drain benchmark suite
- stable KPI dashboard for segments/s and bytes/s

Status:

- partially shipped

Expected gain:

- measurement only

### Phase 1: Scheduler And Gate Throughput

Status:

- shipped, except there is intentionally no family-specific priority lane

Deliverables:

- event-driven drain loop under backlog
- round-robin family scheduling with cooperative yielding
- shared bounded async-index gate aligned with builder count by default

Expected gain:

- **10x-50x** for backlog scenarios dominated by idle tick gaps and gate
  contention

### Phase 2: Single-Pass Combined Worker Build

Status:

- shipped for routing-key and routing-key lexicon L0 builds

Deliverables:

- one worker pass that emits routing and lexicon L0 outputs together
- removal of duplicate segment decode loops
- routing-key-only segment scans for the shared L0 path
- per-window distinct-key fingerprint reuse across routing and lexicon outputs

Expected gain:

- **2x-4x** compute reduction for routing+lexicon L0 work

### Phase 3: Hot-Loop Compute Optimizations

Status:

- partially shipped

Deliverables:

- native/WASM hash path
- typed-array aggregation path
- streaming lexicon merge cursors
- shipped subset:
  - distinct-key fingerprint caching for the combined routing+lexicon L0 build
  - removal of per-record append/payload materialization in that same path

Expected gain:

- **3x-10x** for CPU-heavy windows

### Phase 4: Segment Sidecar Architecture

Status:

- not shipped

Deliverables:

- segment-time sidecar generation
- sidecar-based routing/lexicon L0 builders
- mixed-mode fallback (sidecar absent -> current scan path)

Expected gain:

- architecture-level step needed for sustained **~100x** on large backlogs

### Phase 5: Batched Publish Path

Status:

- not shipped

Deliverables:

- batched metadata and manifest updates
- bounded flush rules and crash-safe replay behavior

Expected gain:

- additional throughput and lower publish overhead variance

## Acceptance Criteria

1. Throughput:
   - demonstrate targeted multiplier improvements on controlled 1 GiB and
     10 GiB backlog workloads

2. Correctness:
   - no false negatives in key-filtered reads
   - no false negatives in `_routing_keys` beyond documented partial-coverage
     semantics

3. Stability:
   - foreground read/search latency remains within existing SLO envelopes under
     concurrent indexing

4. Recovery:
   - crash/restart during each phase preserves rebuildability from manifest +
     immutable objects

## Risks And Mitigations

1. Worker complexity growth.
   Mitigation: strict job protocol versioning and deterministic fixture tests.

2. Sidecar schema lock-in.
   Mitigation: explicit sidecar version bytes and dual-read migration windows.

3. Foreground latency regressions.
   Mitigation: preserve cooperative yielding and foreground-aware backoff;
   gate each phase with load-test checks.

4. Publish semantics drift.
   Mitigation: keep manifest publication as the only remote visibility commit
   point and preserve existing invariants in tests.
