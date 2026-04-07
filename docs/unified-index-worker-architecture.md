# Unified Index Worker Architecture

Status: implemented architecture

This document describes the current indexing runtime that removes heavy
segment-backed index build work from the main server thread and consolidates
background build execution behind one global worker pool.

For the higher-level system view, see:

- [architecture.md](./architecture.md)
- [indexing-architecture.md](./indexing-architecture.md)

## Problem Statement

The current system uses one global indexing coordinator, one shared generic
worker pool, and one shared segment-locality manager. This document explains
that design in more detail than the higher-level architecture docs.

That causes three recurring problems:

1. Main-thread starvation is still possible.
   Some indexing families can still do meaningful local file reads, record
   scans, or binary build work on the main Bun thread.

2. Segment-locality rules are duplicated implicitly.
   The system already knows how to mark cached segment files as
   `required for indexing`, but that mechanism is not yet the single universal
   gate for all segment-backed index builds.

3. Backpressure must be globally coordinated.
   When indexing falls behind, the node needs one central place to decide:
   - how many build workers may run
   - which segment files are leased
   - when appends must be rejected to preserve local segment-cache guarantees

The shipped design is a single global indexing runtime with:

- one main-thread coordinator
- one configurable pool of generic indexing workers
- one segment-locality / lease manager
- one shared overload policy

## Goals

1. No heavy index build work on the main request thread.
2. One global concurrency setting for index builders.
3. One worker pool that can execute any index-family build task.
4. One segment-locality system for all segment-backed index builds.
5. A hard local segment-cache cap that still guarantees indexing progress.
6. Control-plane and read-path latency must stay bounded during indexing.

## Non-goals

1. Changing the manifest commit model.
   `uploaded_through` and manifest publication remain the remote visibility
   commit point.

2. Making index artifacts the source of truth.
   WAL + segments remain canonical. Indexes stay rebuildable accelerators.

3. Moving SQLite ownership into workers.
   SQLite state transitions, stream metadata, and manifest publication stay on
   the main thread.

4. Bypassing the local segment-cache cap.
   Required-for-indexing segment files remain subject to the configured local
   disk-cache budget.

## Design Rules

1. The main thread may schedule, persist, publish, and reject, but it must not
   perform long segment-backed index scans.
2. Workers may do expensive local file reads, decode segment records, and build
   immutable artifact payloads.
3. Workers must not mutate SQLite directly.
4. Any index build that depends on segment files must acquire a segment lease
   before the worker starts.
5. Required-for-indexing segment files are non-evictable while leased.
6. If a required lease cannot fit inside the segment-cache cap, the node may
   reject new appends with `429` / `error.code = "index_building_behind"`.
7. Even while append admission is rejecting, index building must continue so
   the node can recover automatically.

## High-level Architecture

### Main-thread components

#### Global Index Manager

The main-thread global index manager is the single coordinator for background
indexing work.

It owns:

- the only indexing timer
- shared worker-pool lifetime
- stream enqueue fan-out across family adapters
- worker dispatch for build jobs
- SQLite state updates
- object-store publication of finished artifacts
- manifest publication when required
- overload signaling and recovery

The global index manager does **not** build artifacts itself.

#### Segment Locality Manager

The segment locality manager is the only component allowed to mark cached
segments as `required for indexing`.

It owns:

- locating the local file for one segment object
- ensuring the required window is resident locally
- refusing leases that would exceed the configured cache cap
- reference-counted required leases
- releasing leases when a build finishes or fails

All segment-backed families use the same lease manager.

#### Family adapters

Each index family still defines its own semantics, but family-specific managers
become thin adapters rather than independent schedulers.

Examples:

- routing-key family
- routing-key lexicon family
- exact secondary index families
- bundled companion families (`.col`, `.fts`, `.agg`, `.mblk`)

Each adapter tells the global manager:

- how to discover eligible work
- what lease shape it needs
- what worker payload format it needs
- how to persist successful output

### Worker-thread components

#### Generic Index Worker Pool

The worker pool is controlled by one global setting:

- `DS_INDEX_BUILDERS`

That setting defines:

- how many generic indexing worker threads are started
- how many build jobs may run concurrently

Each worker thread can execute any supported index job type. Workers are not
dedicated to one family.

### Auto-tune ownership

`DS_INDEX_BUILDERS` should be owned by the memory auto-tune preset, not tuned as
an unrelated standalone knob.

The reason is straightforward:

- each indexing worker can hold meaningful transient build state
- several workers may also require leased local segment windows at the same time
- the safe worker count therefore depends on the node's memory budget and cache
  budget, not just CPU count

Recommended rule:

- memory auto-tune selects `DS_INDEX_BUILDERS`
- operators may still override it explicitly for controlled experiments, but
  the supported default behavior is auto-tuned

This keeps index-build concurrency aligned with the same memory preset that
already controls other bounded runtime budgets.

#### Worker job handlers

Inside the worker process, build logic is dispatched by job type.

Current job kinds:

- `routing_l0_build`
- `routing_compaction_build`
- `routing_lexicon_l0_build`
- `lexicon_compaction_build`
- `exact_l0_build`
- `secondary_compaction_build`
- `companion_col_build`
- `companion_fts_build`
- `companion_agg_build`
- `companion_mblk_build`
The protocol is intentionally generic so all heavy segment-backed and run-merge
compute can execute off the main thread.

## Work-item Model

Every background build is represented as one explicit work item.

Suggested shape:

```ts
type GlobalIndexWorkItem = {
  workId: string;
  stream: string;
  family: string;
  kind: string;
  createdAtMs: number;
  lease: null | {
    type: "segments";
    requiredSegments: Array<{
      segmentIndex: number;
      objectKey: string;
      localPath: string;
      sizeBytes: number;
    }>;
  };
  payload: unknown;
};
```

Important properties:

- `family` is the logical index family
- `kind` is the exact worker operation
- `lease` is explicit and already resolved before dispatch
- `payload` is immutable worker input

The worker never decides which segments to open. The main thread decides that
before dispatch and hands the worker only leased local file paths.

## Global Scheduling

The global manager replaces independent family timers with one scheduling loop.

### Scheduling inputs

The shipped scheduler considers:

- uploaded segment count
- current indexed watermark per family
- backlog in segments and bytes
- active leases
- worker availability
- overload state

### Scheduling policy

Current behavior is intentionally simple:

1. The global manager owns the only timer and worker-pool lifecycle.
2. It forwards enqueued streams into each family adapter.
3. Each family adapter discovers eligible work for that stream and exposes one
   runnable work step per work kind:
   - routing build
   - lexicon build
   - exact build
   - bundled-companion build
   - routing compaction
   - lexicon compaction
   - exact compaction
4. The global manager round-robins those work kinds. It does not reserve a
   dedicated priority lane for one family or task type.
5. Family adapters still own family-specific backlog discovery and correctness
   rules, but not independent timers, dedicated workers, or scheduler
   priority.

## Segment Leasing Model

This design expands the existing `required for indexing` mechanism into a
global invariant.

### Lease rules

1. Any worker job that depends on sealed segment files must acquire a lease
   before dispatch.
2. A lease marks the required segment-cache entries
   `required for indexing`.
3. Required-for-indexing entries are not evictable.
4. Lease ownership is reference-counted so multiple jobs may temporarily depend
   on the same file if that is ever allowed.
5. Lease release is guaranteed in `finally` on success, failure, timeout, or
   worker crash.

### Lease sizes

The current routing-style window remains the default segment-backed build unit:

- `DS_INDEX_L0_SPAN` uploaded segments
- default `16`

Families may request smaller windows, but they should not request larger ones
without an explicit design reason because that directly increases the
non-evictable cache footprint.

### Lease persistence

Leases are runtime-only. They are not durable state.

After restart:

- no leases exist
- workers are empty
- the global manager re-derives backlog from SQLite state
- it reacquires leases only for newly dispatched work

## Local Cache Cap And Overload Behavior

The local segment cache remains hard-capped by `DS_SEGMENT_CACHE_MAX_BYTES`.

Required-for-indexing files do not bypass that cap.

### Admission rule

If the next required indexing lease cannot fit locally, append admission may be
rejected with:

- HTTP `429`
- `error.code = "index_building_behind"`

The error should explicitly say:

- which family is behind
- how many required segment files are needed
- how many bytes are currently used
- the configured cache budget

### Progress guarantee

Rejecting appends is acceptable only if index progress continues.

That means:

1. Existing uploader/indexer work must keep running.
2. The global manager must continue dispatching build jobs.
3. Lease acquisition for the oldest eligible work must be retried promptly.
4. The node must automatically leave overload mode when backlog drains enough
   to make room again.

In other words, overload is a recovery mode, not a steady state.

## Main-thread Responsibilities

The main thread remains authoritative for all state-machine work:

- inspecting SQLite metadata
- choosing eligible work
- acquiring and releasing leases
- creating worker requests
- receiving worker results
- uploading built artifacts
- inserting run rows / companion rows
- advancing indexed watermarks
- publishing manifests when required
- updating details-version / notifier state
- exposing overload status and metrics

This keeps crash consistency and protocol semantics in one place.

## Worker Responsibilities

Workers are pure build executors.

They may:

- open leased local segment files
- scan records and blocks
- hash routing keys
- tokenize text
- build postings / restart tables / companion payload sections
- encode immutable artifact bytes

They may not:

- update SQLite
- upload manifests
- change watermarks
- make visibility decisions

The worker result should be self-contained:

```ts
type IndexWorkerResult = {
  workId: string;
  family: string;
  kind: string;
  output: {
    artifacts: Array<{
      objectKey: string;
      bytes: Uint8Array;
      meta: Record<string, unknown>;
    }>;
    stats: Record<string, number>;
  };
};
```

## Family-specific Notes

### Routing-key family

Routing-key L0 already fits this model well:

- it needs one uploaded 16-segment local window
- it scans routing keys
- it emits one immutable run payload

This family is the baseline template for the unified design.

### Routing-key lexicon family

Routing-key lexicon L0 uses the same local-window and worker model as the
routing-key family:

- same lease shape
- different payload builder
- same main-thread persistence and manifest rules

### Exact secondary index families

Exact L0 builds also use the same shared worker pool:

- they scan sealed segments
- they produce immutable run objects
- they do not need request-path participation
- they use the same `DS_INDEX_L0_SPAN` lease shape as routing and lexicon

Exact-family compactions now use the same shared worker pool:

- the main thread selects immutable input runs
- the worker performs decode / merge / encode
- the main thread persists the new run, retires old runs, and republishes
  manifest state if needed

### Bundled companion families

Bundled companions are also worker jobs:

- they already consume immutable segment inputs
- they produce immutable per-segment artifacts
- they reuse the same dispatch, timeout, and upload handling
- they acquire one uploaded-segment lease at a time rather than a 16-segment
  window

The main difference is that companions are per-segment outputs rather than
multi-segment tiered runs.

## Upload And Publication

This design does not change the publication rule:

1. Worker builds immutable artifacts.
2. Main thread uploads them with abortable object-store requests.
3. Main thread persists metadata rows.
4. Main thread republishes the manifest if the family requires manifest-visible
   state.

The worker pool is about compute isolation, not about changing commit order.

## Failure Handling

### Worker failure

If a worker crashes or times out:

- the main thread marks the work item failed
- all held leases are released
- the work is re-queued with backoff unless the error is classified permanent

### Upload failure

If artifact upload fails:

- leases are released
- SQLite state is unchanged
- the work is retried later

### Restart

After process restart:

- all worker state is gone
- all leases are gone
- the global manager rebuilds the queue from SQLite and manifest-visible state

This preserves the existing rebuildable-accelerator model.

## Foreground Isolation

The global manager must remain subordinate to foreground latency.

Supported rules:

1. Worker jobs may continue running while the main thread is serving requests.
2. Main-thread scheduling and result handling must stay small and bounded.
3. Result persistence should be one small transaction per finished job.
4. Control-plane endpoints must never rebuild or rescan index inputs in the
   request path.

The target is:

- control-plane endpoints in tens of milliseconds
- read/search latency independent of one whole index build finishing

## Observability

The global runtime needs first-class observability so overload is diagnosable.

Recommended counters:

- configured worker count
- active workers
- queued work items
- active work items by family/kind
- required-for-indexing segment files
- required-for-indexing bytes
- lease acquisition failures
- append rejections due to `index_building_behind`
- build latency by family/kind
- upload latency by family/kind
- build retry count
- worker crash count

Shipped local action log:

- every async build or compaction action now writes one row to local SQLite
  `async_index_actions`
- rows include sequence number, kind, begin/end timestamps, duration, input
  counts/bytes, output counts/bytes, and family-specific `detail_json`
- this log is local observability only; it is not published or restored from R2
- the intended target is that each logged async action completes in under
  `1000 ms`

Recommended per-stream surfaces:

- backlog by family in uploaded segments
- currently leased segment window
- oldest unindexed uploaded segment
- overload source and message

## Expected Benefits

1. Main-thread responsiveness becomes much more predictable.
2. One concurrency knob replaces several family-specific build loopholes.
3. Segment retention for indexing becomes explicit and auditable.
4. Overload handling becomes coherent and recoverable.
5. New index families can plug into the same execution model instead of
   inventing their own background loop.

## Remaining Main-thread Work

Heavy index compute is now off the main thread, but some bounded coordination
still stays in-process:

1. SQLite metadata reads to discover backlog and choose the next runnable job.
2. Segment-locality lease acquisition and release.
3. Small SQLite transactions to insert new run rows, companion rows, and
   updated watermarks.
4. Retiring superseded runs and companion generations.
5. Artifact upload initiation and manifest publication.
6. Append admission checks for `429 index_building_behind`.

These operations should stay bounded and must not scan segment contents or
decode large index/run payloads on the main thread.

## Current Direction

The shipped design is:

- one main-thread global index manager
- one global `DS_INDEX_BUILDERS` worker-pool limit
- one universal segment-locality lease manager
- one overload policy tied to required-for-indexing cache pressure

That is the simplest shipped design that eliminates main-thread
segment-backed index-build stalls without changing the durable WAL / segment /
manifest architecture.
