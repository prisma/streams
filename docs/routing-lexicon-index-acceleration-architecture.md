# Routing And Lexicon Index Acceleration Architecture

Status: proposed architecture

This document proposes a concrete architecture and rollout plan to deliver a
major throughput increase for routing-key and routing-key lexicon indexing.

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

## Current Bottlenecks

1. Timer-bound progress.
   Index managers run on periodic ticks and usually advance one build window
   per family iteration.

2. Shared async-index gate contention.
   Routing, lexicon, exact, and bundled companions compete for one top-level
   async-index budget.

3. Duplicate segment rescans.
   Routing and lexicon both read, decompress, and iterate the same segment
   windows independently.

4. CPU-heavy per-record hot loops.
   Routing build uses JS `BigInt` hashing and map-heavy accumulation for every
   keyed record.

5. Repeated lexicon term decode work.
   Lexicon merge/list paths repeatedly decode restart-string terms in
   random-access patterns.

6. Manifest and metadata chatter.
   Frequent small publish/update cycles add overhead relative to useful index
   progress.

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

Design rule:

- under backlog, progress should be bounded by worker capacity and the shared
  async-index gate, not by per-family timer skew or a special-priority lane

### 2. Shared Background Budget

All indexing families now compete for the same bounded background budget:

- one global `DS_INDEX_BUILDERS` worker-pool size
- one shared async-index concurrency gate
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

Expected gain:

- none (measurement only)

### Phase 1: Scheduler And Gate Throughput

Deliverables:

- event-driven drain loop under backlog
- explicit L0-first scheduling policy
- family-isolated background budgeting

Expected gain:

- **10x-50x** for backlog scenarios dominated by idle tick gaps and gate
  contention

### Phase 2: Single-Pass Combined Worker Build

Deliverables:

- one worker pass that emits routing and lexicon L0 outputs together
- removal of duplicate segment decode loops

Expected gain:

- **2x-4x** compute reduction for routing+lexicon L0 work

### Phase 3: Hot-Loop Compute Optimizations

Deliverables:

- native/WASM hash path
- typed-array aggregation path
- streaming lexicon merge cursors

Expected gain:

- **3x-10x** for CPU-heavy windows

### Phase 4: Segment Sidecar Architecture

Deliverables:

- segment-time sidecar generation
- sidecar-based routing/lexicon L0 builders
- mixed-mode fallback (sidecar absent -> current scan path)

Expected gain:

- architecture-level step needed for sustained **~100x** on large backlogs

### Phase 5: Batched Publish Path

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
