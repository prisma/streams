# Async Search Index Acceleration Architecture

Status: staged acceleration architecture, updated to match the shipped system

This document describes the current shipped acceleration baseline plus the next
optimization steps for the remaining slow async-index paths on `evlog`-style
streams:

- exact secondary L0 build
- bundled companion build
- exact secondary compaction, especially `requestId`

It is based on the current code paths in:

- `src/index/secondary_l0_build.ts`
- `src/search/companion_build.ts`
- `src/index/secondary_compaction_build.ts`
- `src/index/secondary_indexer.ts`
- `src/search/companion_manager.ts`
- `src/index/index_build_worker_pool.ts`
- `src/index/global_index_manager.ts`

It also uses the latest production evidence from `evlog-1`:

- unified `search_segment_build` is shipped and standalone companion builds no
  longer race exact on fresh `evlog` segments
- exact secondary L0 is now the main steady-state indexing cost on the live
  box, with the companion-owning owner batch still around `7-9s` while the
  other exact batches now mostly land in the `2-5s` range
- bundled companion build is no longer the fresh-segment bottleneck because its
  work is piggybacked from exact on fresh `evlog` segments
- exact compaction for high-cardinality fields such as `requestId` is still the
  main compaction outlier, but the worker-side merge CPU is no longer the
  dominant phase
- the latest exact-compaction telemetry shows:
  - `source_prepare_ms` in the low single-digit seconds
  - `worker_build_ms` under `1s` for fresh `requestId` level-0->1 compaction
  - `finalize_ms` still around `10s`, which is now the main compaction
    bottleneck
- the server has already been OOM-killed more than once with anonymous RSS in
  the `3+ GiB` range, so RSS shape still matters as much as raw wall time
- the latest bundled `.fts` memory-shape fix brought current RSS back down into
  the `300 MiB` range with restart-window spikes staying under `1 GiB`

## Summary

Several important changes are already shipped:

1. async index families run on the shared global worker pool and are scheduled
   by the round-robin global index manager
2. `evlog` exact and companion per-segment work now share one unified
   `search_segment_build` worker pass over each leased uploaded segment
3. exact single-field builds already use a raw-byte fast path for simple
   top-level scalar fields instead of always doing full `JSON.parse`
4. `evlog` companion extraction now shares that same raw-byte scan with exact
   secondary output generation, so the same segment is no longer scanned twice
   just to build the two search accelerators
5. bundled companion worker outputs are already file-backed and the main thread
   now uploads with `putFile()` and moves the same file into the local
   companion cache
6. exact secondary L0 and exact secondary compaction worker outputs are now
   file-backed and the main thread uploads them with `putFile()` and moves the
   same files into the local exact-run disk cache
7. exact secondary compaction source preparation now spills uncached inputs to
   local temp files when they cannot be admitted into the exact-run disk cache,
   so compaction no longer falls back to holding large downloaded run payloads
   in anonymous RSS just to merge them once
8. exact secondary compaction telemetry now records separate
   `source_prepare_ms`, `worker_build_ms`, `artifact_persist_ms`, and
   `finalize_ms` timings so we can distinguish cold-source loading from actual
   merge CPU cost on the live node
9. exact secondary compaction still deletes retired run objects synchronously,
   but those deletes are now executed with bounded parallelism instead of
   one-by-one serial waits
10. lagging exact batches now skip companion rebuild entirely when the current
    companion generation is already present for that segment, so slow exact
    catch-up work no longer redoes `.col` / `.fts` payloads that are already
    current
11. `evlog` exact L0 no longer treats every same-frontier exact field as one
    monolithic batch; the high-cardinality fields are now split into smaller
    deterministic sub-batches and only one owner batch is allowed to build the
    fresh companion payload for a segment
12. keyword `.fts` postings now use a compact singleton representation until a
    term actually appears in more than one document, instead of allocating a
    one-element `doc_ids` array for every unique `requestId` / `traceId` /
    `spanId` / `path` term
13. text `.fts` accumulation now aggregates token positions once per document
    before touching the shared postings map, so `message` / `why` / `fix` /
    `error.message` no longer pay one global postings mutation per token

Those changes removed the worst duplicate segment scan and took standalone
companion off the fresh-segment critical path, but they did not solve the whole
search backlog problem.

The next big wins are now more targeted:

1. **Exact secondary L0 must get materially faster.** It is now the main
   steady-state indexing cost and the reason exact coverage is not catching up.
2. **High-cardinality exact fields should adopt shard mode.** `requestId`,
   `traceId`, and `spanId` still create oversized runs and compaction variance
   even after the streaming merge rewrite.
3. **Exact compaction finalization should get faster without changing the
   semantics.** The worker merge is now cheap enough that main-thread
   finalization, especially publish-side work for the new run group, dominates
   compaction wall time even after synchronous retired-run cleanup was sped up.
4. **Unified search-build handoff should stay file-backed and ownership-aware.**
   The coordinator must not retain large completed payloads in anonymous RSS,
   and each consumer must get explicit ownership of any temp files it uses.

So the recommended architecture is now staged:

- keep the current global worker pool and round-robin scheduler
- keep the shipped raw-byte path and the new unified per-segment `evlog`
  search build
- keep unified search-build outputs file-backed and scoped to the exact
  frontier batch or exact-ready segment set that actually needs them
- keep standalone companion builds as stale-hole repair only below the exact
  frontier
- keep the shipped streaming exact compaction path
- then optimize exact L0 throughput
- then add shard mode for the worst high-cardinality exact fields
- then continue reducing exact compaction finalization time without deferring
  correctness-critical cleanup out of band

## Objective

Primary objective:

- dramatically increase sustained async indexing throughput for exact and
  companion work on `evlog`
- eliminate the current OOM pattern during backlog drain and compaction

The system should optimize for:

- `segments_built_per_second`
- `indexed_payload_bytes_per_second`
- `time_to_catch_up`
- peak RSS during backlog drain
- query freshness after segment upload

## Scope

In scope:

- exact L0 build
- bundled companion build (`.col`, `.fts`, `.agg`, `.mblk`)
- exact compaction
- worker handoff, upload, publish, and scheduling changes required to speed the
  above
- memory safety changes required to stop the current OOM pattern

Out of scope:

- changing WAL durability semantics
- changing manifest publication as the remote visibility commit point
- changing the source-of-truth rule (raw WAL + segments stay canonical)
- changing the read fallback rule for missing or stale acceleration artifacts

## Current Diagnosis

### 1. Unified per-segment search build is shipped, and fresh companion duplication is no longer the main issue

`secondary_l0_build.ts` and `companion_build.ts` no longer need to scan the
same uploaded `evlog` segment twice. The shipped `search_segment_build`
worker already computes:

- raw-byte exact-secondary outputs for the full configured exact set
- bundled companion `.col` / `.fts` output from the same segment walk

The remaining duplication is now mostly a stale-hole repair concern, not a
fresh-segment scan concern:

- exact can already piggyback companion persistence when it reaches a segment
- companion can also piggyback ready exact L0 runs when it reaches a segment
- but if standalone companion keeps running on the newest stale segments, it
  can still consume the whole shared build for segments that exact will cover
  shortly anyway

That change is now shipped for fresh `evlog` segments: standalone companion
build only repairs stale holes below the minimum exact frontier. So this is no
longer the next major optimization target.

The newest shipped change tightens the unified exact side further:

- exact L0 no longer asks the shared `search_segment_build` worker to build the
  full configured exact set when only a smaller frontier batch is actually
  being advanced
- lagging exact batches now also skip companion generation when the current
  companion row for that segment is already present, which is the common live
  `evlog` catch-up case once companion and the faster exact group are ahead of
  the slowest exact fields
- same-frontier `evlog` exact work is now split into deterministic sub-batches:
  `level,service,environment`, then `timestamp`, then one-field batches for
  `requestId`, `traceId`, `spanId`, and `path`, plus the existing
  `method,status,duration` batch
- companion piggyback only asks for exact outputs that are actually exact-ready
  for the current segment
- unified worker outputs are file-backed, and the shared coordinator only
  deduplicates inflight work instead of caching completed payloads in memory

That removes one important anonymous-RSS spike source and cuts wasted exact
work on `evlog` significantly.

### 2. Exact secondary L0 is now the main steady-state cost

The current exact L0 batch remains expensive even after the raw-byte extractor
work:

- fresh live jobs are still about `11-13s`
- exact coverage is far behind the uploaded head
- the slowest exact fields can remain stalled while faster fields inch forward

That makes exact L0 throughput, not companion extraction, the main steady-state
reason indexing is not catching up.

### 3. RequestId compaction is now streaming, but it is still too expensive

`secondary_compaction_build.ts` now:

- advances each input run lazily instead of decoding all postings into JS arrays
- keeps only the current posting entry per input cursor live at once
- writes file-backed output incrementally with a bounded write buffer

That removes the worst previous OOM shape, where `requestId` compaction could
accumulate a very large decoded input plus merged output in anonymous RSS.

The latest shipped changes also remove the remaining large-input fallback on
the source side and make synchronous cleanup cheaper:

- if a compaction input run is already resident in the exact-run disk cache,
  compaction reads it in place
- if it is fetched from object storage and the cache can admit it, compaction
  promotes it into the cache and then reads it in place
- if the cache cannot admit it, compaction now spills it to a temp local file
  and merges from that file instead of passing a large `Uint8Array` into the
  worker
- retired exact-run object deletion stays synchronous, but now runs with
  bounded parallelism instead of serial waits

So the current exact-compaction OOM risk is no longer "N large input runs plus
one large merged output all live in JS memory at once." The remaining risks are
now dominated by worker-side merge state for the worst high-cardinality fields,
especially `requestId`.

The newest phase timing makes the remaining problem even clearer:

- `worker_build_ms` is now sub-second for a fresh `requestId` compaction sample
- `source_prepare_ms` is meaningful but not dominant
- `finalize_ms` is still around `10s`

So the next step for exact compaction is no longer "fix the merge algorithm."
It is:

1. make the remaining publish/finalize path cheaper
2. then add shard mode for the worst fields

### 4. File-backed outputs helped, but exact L0 and finalization still need work

Bundled companion outputs are now file-backed:

- the worker writes the `.cix` payload to a temp file
- the main thread uploads it with `putFile()`
- the same file is moved into the companion disk cache

That change reduced one class of transient memory copies, and it is worth
keeping for RSS safety.

The same file-backed handoff now also applies to unified `search_segment_build`
outputs, and the coordinator no longer retains completed shared results in a
TTL cache. That matters because the exact L0 path previously overbuilt exact
outputs for fields that were not even part of the current frontier batch, and
those completed results could linger in anonymous RSS after the worker finished.

Production telemetry now shows two remaining issues instead:

- exact L0 is still too slow when it builds even the correct frontier batch
- exact compaction worker time is acceptable while finalization on the main
  thread is still too slow

### 5. The scheduler is no longer the primary bottleneck

The round-robin global scheduler is a good fairness mechanism.
It is not the first thing to change now.

The main remaining cost is inside the work items:

- exact L0 still takes too long per batch
- exact compaction finalization still spends too long on main-thread post-build
  work
- high-cardinality exact fields still create too much compaction variance

It is still true that `evlog` would benefit from one unified per-segment search
build in the long term, but the scheduler does not need another redesign before
we fix those two hot paths.

## Design Goals

1. Parse and extract each segment at most once for search acceleration.
2. Keep exact and companion state machines rebuildable and manifest-driven.
3. Eliminate large in-memory artifact copies between worker and main thread.
4. Make `requestId` compaction scale with streaming merge, not map growth.
5. Keep the current correctness model:
   - missing or stale acceleration still falls back to raw segment / WAL scan.
6. Preserve the current request-path rule:
   - heavy work remains off the request path.
7. Bound anonymous RSS under backlog and compaction.

## Proposed Architecture

## 1. Shipped: true raw-byte companion build for `evlog`

The current `evlog` companion path should not pay `JSON.parse` at all when the
requested fields are scalar and live on stable object paths.

### Required runtime shape

For `evlog` companion jobs:

1. compile scalar accessors once per schema version
2. build a trie over object-path segments once per schema version
3. walk raw JSON payload bytes directly
4. push canonicalized values straight into `.col` and `.fts` sinks
5. only fall back to parsed-object extraction for fields that are unsupported by
   the scalar byte scanner

### Result

Current telemetry still shows `companion_build` as the dominant steady-state
outlier. The fastest way to cut that down is to stop paying the largest per-row
CPU cost entirely.

This does not change any manifest, fallback, or scheduler semantics. It is the
lowest-risk high-yield optimization left in the worker hot path.

## 2. Shipped: file-backed companion and exact artifact handoff

Bundled companion workers plus exact-secondary L0 and compaction workers now
emit temp-file outputs for the main production path. The main thread:

1. uploads the artifact with `putFile()` when the object store supports it
2. moves the same file into the local exact-run or companion cache
3. avoids rebuilding a large in-memory payload copy just to upload and cache it

This remains a worthwhile RSS reduction and it already reduced the exact
compaction worker handoff cost materially in local benchmarking, but it is not
enough by itself to make the remaining multi-second `companion_build` outliers
disappear on `evlog`.

## 3. Shipped: unified `search_segment_build` worker job

`evlog` now ships one new worker job kind:

- `search_segment_build`

It replaces the duplicate per-segment scan inside the two current family
adapters:

- `secondary_l0_build` for one segment
- `companion_build` for one segment

### Input

```ts
type SearchSegmentBuildInput = {
  stream: string;
  registry: SchemaRegistry;
  exactIndexes: Array<{
    index: SecondaryIndexField;
    secret: Uint8Array;
  }>;
  plan: SearchCompanionPlan | null;
  planGeneration: number | null;
  segment: {
    segmentIndex: number;
    startOffset: bigint;
    localPath: string;
  };
};
```

### Output

```ts
type SearchSegmentBuildOutput = {
  exactRuns: Array<{
    indexName: string;
    storage: "bytes";
    payload: Uint8Array;
    meta: {
      runId: string;
      level: number;
      startSegment: number;
      endSegment: number;
      objectKey: string;
      filterLen: number;
      recordCount: number;
    };
  }>;
  companion: null | {
    storage: "bytes";
    payload: Uint8Array;
    sectionKinds: CompanionSectionKind[];
    sectionSizes: Record<string, number>;
    primaryTimestampMinMs: bigint | null;
    primaryTimestampMaxMs: bigint | null;
  };
};
```

The first shipped form keeps these outputs in memory because the exact and
companion managers share the same worker result through a short-lived
main-thread coordinator cache. That eliminates the duplicate scan now without
forcing fragile temp-file ownership across two independent managers. File-backed
handoff can be reintroduced later on top of the unified build once the shared
artifact ownership model is explicit.

The shipped `evlog` path also now treats the unified build as the canonical
per-segment exact build:

- the worker computes the full configured `evlog` exact field set once per
  segment
- the worker also computes the bundled `.col` + `.fts` companion output in that
  same pass
- `SecondaryIndexManager` persists only the subset of exact runs that are ready
  for the current exact batch
- `SearchCompanionManager` persists the companion artifact, and when companion
  reaches a segment first it also piggybacks the ready exact L0 runs from the
  same unified worker result so a later duplicate exact scan is not needed

That is important for cache reuse. The coordinator cache key must stay stable
between the exact and companion consumers, so they both request the same
segment-wide exact field set and then consume subsets of the shared result.

### Runtime shape

For one leased uploaded segment:

1. read the segment file once
2. iterate records once
3. run raw-byte scalar extraction for the built-in `evlog` fast path
4. normalize exact / `.col` / `.fts` values once per record
5. feed exact sinks and companion sinks in the same pass
6. encode exact artifacts and `.cix` artifact once
7. keep the encoded outputs in memory for short-lived sharing between the exact
   and companion managers

### Why this is the correct next step after raw-byte companion extraction

The repository already made this same architectural move once for routing and
routing-key lexicon:

- two separate segment scans were collapsed into one combined worker pass

The exact + companion pair is now the analogous duplicate work. After raw-byte
companion extraction lands, that duplicate scan cost becomes even more obvious,
because exact and companion will each be efficiently reading the same bytes in
separate jobs.

For `evlog`, this is an especially good fit because the exact field set is
almost a subset view of the same normalized values the companion builders are
already producing.

## 2. Generated, sink-oriented field extraction

The current extraction contract should be replaced in the hot build path.

Today the worker extracts into a generic structure:

- `Map<string, unknown[]>`

The proposed contract is sink-oriented:

```ts
type SearchFieldSink = {
  onKeyword(fieldOrdinal: number, value: string): void;
  onText(fieldOrdinal: number, value: string): void;
  onInteger(fieldOrdinal: number, value: bigint): void;
  onFloat(fieldOrdinal: number, value: number): void;
  onDate(fieldOrdinal: number, value: bigint): void;
  onBool(fieldOrdinal: number, value: boolean): void;
  onRecordEnd(docId: number): void;
};
```

The extractor should become a compiled plan that walks the record once and
pushes normalized values directly into the sinks.

### Required properties

- field ordinals instead of repeated field-name strings in the hot loop
- direct normalization while visiting the field
- no per-record `Map<string, unknown[]>`
- no generic “extract everything, then re-loop” temporary structure

### Profile-specific fast path

For built-in hot profiles, especially `evlog`, add a generated fast path:

- direct property loads for top-level canonical fields
- pre-bound lowercasing / date parsing / numeric canonicalization
- generic JSON-pointer fallback remains for arbitrary user schemas

This gives the system two modes:

- **generic** correctness-preserving extractor
- **profile-specialized** fast extractor for the known hot profiles

That is a large and low-risk CPU win because `evlog` has a stable canonical
shape.

## 3. Reuse companion accumulators to emit exact artifacts

This is the most important design detail.

The unified builder should not build exact and companion state independently.
It should reuse the companion-side accumulators wherever possible.

### 3.1 Keyword exact fields

For a keyword field with `exact=true` and `prefix=true`:

- the `.fts` keyword builder is already accumulating the unique normalized
  terms and doc postings for that field
- the exact builder only needs per-segment term presence for L0 emission

So the exact artifact should be emitted by iterating the final keyword term set
that the `.fts` builder already owns.

That removes duplicate work for the hottest `evlog` exact fields:

- `level`
- `service`
- `environment`
- `requestId`
- `traceId`
- `spanId`
- `path`
- `method`

### 3.2 Typed exact fields

For a field with `exact=true` and `column=true`:

- the `.col` builder already sees the canonical typed value stream
- the exact builder should reuse that same normalized value path

The unified builder may still keep a small per-field unique-value set for exact
emission, but it must do that off the same normalized value stream the column
builder already consumes.

This covers:

- `timestamp`
- `status`
- `duration`

### 3.3 Exact-only residual fields

If a schema has an exact field that is neither keyword-prefix nor column:

- keep a small dedicated exact-only sink for that field

That preserves generality without penalizing `evlog`.

## 4. File-backed worker outputs and zero-copy upload path

### Current problem

The worker currently returns `Uint8Array` payloads to the main thread.
That creates unnecessary memory pressure.

### New design

The worker writes artifacts to temp files under a bounded build directory, for
example:

- `${DS_ROOT}/tmp/index-builds/<workId>/...`

The main thread then uploads with:

- `os.putFile()` when available
- `os.put()` only as a fallback

### Benefits

- no large structured-clone copy for `.cix` payloads
- no large structured-clone copy for exact run payloads
- uploads can stream from disk-backed files
- easier cleanup on failure/restart
- much lower anonymous RSS during heavy backlog drain

### Design rule

Small metadata may cross the worker boundary.
Large artifact bytes should not.

## 5. Streaming exact compaction

The current exact compaction algorithm should be replaced completely.

### New algorithm

Use a streaming k-way merge over already-sorted input runs.

For each input run:

- open a cursor
- read the next fingerprint entry only when needed
- merge the smallest fingerprint across cursors
- write the merged posting list immediately to the output writer
- do not accumulate the entire merged key space in memory first

### Required properties

- no `Map<bigint, number[]>` of the full merged output
- no “decode every run completely, then sort again” step
- output file written incrementally
- binary-fuse build, if retained, should happen from the streamed fingerprint
  sequence or as a second bounded pass

### Why this matters

This directly targets the current `requestId` outlier.

`requestId` is not slow because the exact family is conceptually wrong.
It is slow because the current compaction implementation uses the wrong
algorithmic shape for a high-cardinality field.

## 6. High-cardinality shard mode for exact fields

Streaming merge is necessary.
Sharding is strongly recommended for the worst fields.

### Problem

Even with streaming merge, a monolithic exact run for `requestId` still creates:

- large output files
- long single-job critical sections
- compaction variance that scales with total distinct IDs in the whole run

### Proposed design

Allow exact fields to opt into shard mode, for example:

- shard by leading fingerprint bits
- `16`, `32`, or `64` shards per field

Then:

- L0 emission writes shard-local artifacts
- compaction merges one shard at a time
- query lookup computes the fingerprint and touches only the relevant shard

### Recommended default for evlog

Enable shard mode by default for:

- `requestId`
- `traceId`
- `spanId`

Optional for:

- `path` on high-cardinality deployments

Leave low-cardinality fields unsharded:

- `level`
- `service`
- `environment`
- `method`

### Why sharding helps

It converts one very long compaction into many smaller, parallelizable,
independently retryable jobs.

That is the cleanest way to keep `requestId` from dominating the exact family.

## 7. One persistence and publish cycle per segment build

A unified `search_segment_build` should persist exact and companion results
through one small main-thread commit path.

### Persist flow

1. upload all exact artifacts for the segment
2. upload the companion `.cix`
3. in one SQLite transaction:
   - insert exact runs
   - advance exact indexed-through for the aligned fields
   - upsert the segment companion row
4. publish manifest once

### Why this is better

Today, exact and companion builds each pay their own:

- upload orchestration
- metadata mutation
- manifest publication

That is extra control-plane overhead with no product benefit.

## 8. Scheduler policy under backlog

The current round-robin global scheduler can remain the outer policy.
The work kinds can remain separate for the next phase.

### Immediate recommendation

- keep:
  - exact L0 build
  - companion build
  - exact compaction

### Later recommendation

- replace exact L0 build + companion build with unified `search_segment_build`
  only if the targeted companion/compaction optimizations still leave backlog
  drain too slow

### Work-sharing recommendation

While uploaded-segment backlog exists for a stream:

1. exact L0 build
2. companion build
3. exact compaction
4. other background compactions

### Important detail for evlog

The user report states that `evlog` does not configure routing-key indexing.
That means this stream does not need routing or lexicon work at all.

For these streams, the scheduler naturally collapses to:

1. exact L0 build
2. companion build
3. exact compaction

That is sufficient for now because the worker pool is already shared and the
outer scheduler already round-robins across work kinds.

## 9. Memory guardrails

The current OOM evidence means memory discipline must be first-class in this
redesign.

### Required guardrails

1. **Artifact outputs are file-backed.**
   Do not keep final `.cix` and exact run payloads as long-lived in-memory
   arrays once encoded.

2. **Builder-local budgets per family.**
   The unified worker should have explicit soft ceilings for:
   - keyword term table bytes
   - postings bytes
   - exact unique-term bytes
   - aggregate working bytes

3. **Compaction-local budgets.**
   Streaming compaction should stop using data structures whose memory grows
   with the entire merged key space.

4. **Backlog-aware compaction suppression.**
   If search-segment build lag is above a configured threshold, suppress exact
   compaction temporarily. Fresh segment coverage is more important than deeper
   exact compaction while backlog is growing.

5. **Inflight upload byte budget.**
   Limit total in-flight upload bytes, not just task count.

6. **Fast failure cleanup.**
   On worker failure, timeout, or restart, delete temp artifact directories
   eagerly.

### Operational result we want

Under backlog, RSS should rise predictably with:

- bounded builder state
- bounded segment leases
- bounded mmap caches

It must not rise with:

- duplicated payload copies across worker/main/upload
- compaction merge-set cardinality

## 10. Optional Phase 2: durable search-source sidecar

The unified segment build is the highest-leverage immediate change.

If backlog drain is still not where it needs to be after that, the next
architecture step is to introduce a durable per-segment search-source sidecar.

### Purpose

Store the result of search extraction once, so rebuilds and historical plan
changes do not need to reparse raw segment payloads.

### Example

- `streams/<hash>/segments/<segment>-<plan>.ssx`

This would be a compact, plan-relative extraction artifact consumed by:

- unified search-segment build re-encode jobs
- historical rebuilds after search-plan change
- optional future read-path fast fallbacks

### Status recommendation

Do not make this the first step.

It is powerful, but it is a larger migration than the unified segment build and
streaming compaction changes. Use it if the first phase still leaves too much
CPU on rebuild-heavy workloads.

## Data Model Changes

The minimum viable redesign can keep most existing tables.

### Reuse existing tables

- `secondary_index_state`
- `secondary_index_runs`
- `search_companion_plans`
- `search_segment_companions`
- `async_index_actions`

### Recommended additions

#### Exact shard support

Add nullable shard metadata to exact runs:

- `shard INTEGER NULL`
- included in object key and uniqueness rules

#### Unified action visibility

Add a new action kind:

- `search_segment_build`

Its `detail_json` should include:

- exact field count
- companion section kinds
- parse/extract/encode timings
- temp/output file sizes
- peak builder-memory estimates if available

### Object layout

Recommended exact object key shape with shard support:

- `streams/<hash>/secondary-index/<field>/l<level>/shard-<n>/<run>.sidx`

Recommended companion object key shape can remain unchanged.

## Concrete implementation map

### Worker and job protocol

Change:

- `src/index/index_build_job.ts`
- `src/index/index_build_worker.ts`
- `src/index/index_build_worker_pool.ts`

Add:

- `search_segment_build`
- file-path based result objects
- optional transfer-list support for tiny outputs only

### Unified build logic

Add new module(s), for example:

- `src/search/search_segment_build.ts`
- `src/search/search_field_extractor.ts`
- `src/search/search_field_extractor_evlog.ts`

Retire the separate segment-scan role of:

- `src/index/secondary_l0_build.ts`
- `src/search/companion_build.ts`

Those modules can survive temporarily as wrappers during migration, but the
long-term supported path should be one unified builder.

### Manager integration

Change:

- `src/index/global_index_manager.ts`
- `src/index/secondary_indexer.ts`
- `src/search/companion_manager.ts`

Introduce:

- one coordinator for `search_segment_build`
- one shared persistence path for exact + companion outputs

The existing read-side interfaces can stay stable.

### Compaction rewrite

Replace the algorithm inside:

- `src/index/secondary_compaction_build.ts`

Add:

- streaming run iterators
- shard-aware compaction inputs if shard mode is enabled
- file-backed compaction output

## Rollout Plan

### Phase 0: measurement and failure-proofing

Deliverables:

- add timing breakdowns for current exact and companion paths:
  - `segment_read`
  - `json_parse`
  - `field_extract`
  - `exact_accumulate`
  - `fts_accumulate`
  - `col_accumulate`
  - `encode`
  - `upload`
- add compaction memory / key-count telemetry per field
- add temp-file cleanup on startup

Expected gain:

- measurement only

### Phase 1: shipped baseline

Delivered:

- shared global worker pool
- round-robin global scheduler
- one-field / one-segment exact L0 on `evlog`
- raw-byte exact L0 fast path for simple scalar fields

Observed result:

- moved heavy async-index work off the main request path
- enabled global round-robin worker scheduling
- but did not make exact L0 fast enough to catch up on `evlog`

### Phase 2: targeted companion and unified-search acceleration

Deliverables:

- raw-byte scalar extraction for companion build
- parsed-object fallback only for residual unsupported fields
- no mandatory `JSON.parse` for the common `evlog` field set
- unified `search_segment_build`
- fresh-segment companion piggyback from exact

Observed result:

- standalone companion no longer races exact on fresh `evlog` segments
- fresh-segment exact+companion work collapsed into one segment scan
- companion is no longer the main fresh-segment bottleneck

### Phase 3: file-backed artifact handoff (shipped)

Delivered for both bundled companions and exact-secondary artifacts:

- companion workers write `.cix` outputs to temp files
- exact-secondary L0 and compaction workers write `.irn` outputs to temp files
- main thread uploads with `putFile()`
- the same file is moved into the corresponding local disk cache
- unified `search_segment_build` now also writes exact and companion outputs to
  temp files when a caller provides an output directory
- the shared `search_segment_build` coordinator now only deduplicates inflight
  builds and no longer keeps a completed-result TTL cache in anonymous memory
- unified exact work is now scoped to the exact frontier batch or exact-ready
  segment set instead of always building the full configured exact set

Expected gain:

- modest latency gain
- **large RSS reduction**
- much lower OOM risk

### Phase 4: streaming exact compaction and source staging (partially shipped)

Deliverables:

- k-way merge
- low-allocation output encoding
- source staging to local files when uncached compaction inputs cannot be
  admitted into the exact-run disk cache
- shard-aware exact runs for `requestId`, `traceId`, and `spanId`
- no full merged `Map<bigint, number[]>`

Observed result so far:

- fresh `requestId` compaction improved from about `48.7s` to about `17.1s`
- compaction worker merge time is now sub-second on a fresh `requestId`
  sample
- the remaining compaction bottleneck is `finalize_ms`, not `worker_build_ms`
- peak RSS after restart stayed below `1 GiB` instead of reproducing the prior
  `3+ GiB` OOM shape

Remaining deliverables:

- shard-aware exact runs for `requestId`, `traceId`, and `spanId`
- compaction finalization that does not block on non-critical cleanup

### Phase 5: exact compaction finalization and shard mode

Deliverables:

- delete retired exact-run objects with bounded parallelism instead of serial
  waits
- keep manifest publication and other visibility-critical updates on the
  critical path
- shard-aware exact runs for `requestId`, `traceId`, and `spanId`

Observed result so far:

- fresh `requestId` compaction improved again from about `17.1s` to about
  `3.5-4.0s`
- `finalize_ms` on the fresh `requestId` sample dropped from about `10.7s` to
  about `0.9-1.0s`
- remaining compaction work is now mostly source preparation plus any residual
  publish-side work

Remaining deliverables:

- shard-aware exact runs for `requestId`, `traceId`, and `spanId`

### Phase 6: exact L0 throughput rewrite

Deliverables:

- faster exact L0 accumulation and encoding on `evlog`
- avoid building exact outputs for fields that are not actually in the current
  frontier batch
- skip companion generation on lagging exact batches when companion for the
  current segment is already at the current plan generation
- split same-frontier high-cardinality `evlog` exact fields into smaller
  deterministic batches so peak per-job wall time and peak RSS both come down
- explicit phase telemetry inside exact L0 so scan, accumulate, encode, and
  persist costs are separable

Observed result so far:

- unified exact work now only builds the actual frontier batch instead of the
  full configured exact set
- the local focused benchmark for the `method,status,duration` frontier batch
  is more than `25%` faster than building the full exact set for the same
  segment
- the local focused benchmark for a lagging `method,status,duration` batch is
  also more than `25%` faster when companion rebuild is skipped for a segment
  that already has the current companion generation
- the local focused benchmark for the fresh high-cardinality `evlog` exact
  group is also more than `25%` faster on peak per-job time when that work is
  split into deterministic sub-batches with a single companion-owning batch
- the local focused benchmark for `evlog` fast-byte companion build is more
  than `25%` faster than the legacy fast-byte path that still allocates a
  singleton `doc_ids` array for every unique keyword term
- on the live node, that singleton-postings change cut current RSS from about
  `2.42 GiB` to about `337 MiB` and reduced post-restart RSS high-water from
  about `2.42 GiB` to about `670 MiB` while `evlog-1` indexing continued

Remaining goals:

- exact coverage should stop falling behind the uploaded head
- exact L0 median should move toward the sub-`5s` target

### Phase 7: optional durable search-source sidecar

Deliverables:

- durable extraction sidecar
- rebuilds consume sidecar instead of raw segment parse

Expected gain:

- another large reduction for rebuild-heavy and plan-change workloads
- not required for the first major improvement

## Acceptance Criteria

Recommended acceptance targets for the current `evlog` workload:

1. exact + companion segment coverage
   - uploaded segments should receive both exact and companion coverage fast
     enough that lag does not grow without bound under steady ingest

2. exact L0 / companion freshness
   - newly uploaded segments should receive both exact and companion coverage in
     the same publication cycle

3. latency targets
   - exact L0 median under `5s`, with a long-term goal under `1s`
   - fresh-segment companion work piggybacked from exact on `evlog`
   - high-cardinality exact compaction median under `5s`, with
     `finalize_ms` no longer dominant

4. memory targets
   - no OOM on the current host class during sustained backlog drain
   - peak RSS materially below the previous `~3.36 GiB` kill point
   - anonymous RSS no longer grows in proportion to merged exact key space

5. correctness
   - no false negatives for exact read filters or `_search` exact clauses
   - stale or missing artifacts still fall back to raw segment/WAL scan exactly
     as today

## Risks And Mitigations

### 1. Unified builder complexity grows

Mitigation:

- keep one extractor API and one worker job protocol
- add deterministic fixture tests that compare the unified outputs against the
  current exact + companion outputs

### 2. Profile-specific extractors could diverge from generic behavior

Mitigation:

- keep a generic extractor as the reference path
- run the same corpus through both paths in tests and compare outputs

### 3. Shard mode complicates exact lookup and manifest state

Mitigation:

- enable shard mode only for a small allowlist of high-cardinality fields first
- keep low-cardinality fields on the unsharded path

### 4. File-backed artifacts add temp-file lifecycle work

Mitigation:

- dedicated build temp root
- delete-on-success
- startup sweep for abandoned temp directories

### 5. One unified build failure could delay both exact and companion coverage

Mitigation:

- exact and companion remain rebuildable accelerators
- allow the worker to return partial-family failure metadata if needed, but keep
  the first implementation simpler: fail the whole segment build and retry
- raw segment fallback preserves correctness

## Bottom Line

The current bottleneck is structural:

- the system still scans and extracts the same `evlog` segment twice
- it still compacts `requestId` with a full materialization algorithm

The best next architecture is therefore:

- **one unified search-segment build**
- **direct sink-oriented extraction**
- **streaming, optionally sharded exact compaction**

That is the shortest path to meaningfully faster exact indexing, meaningfully
faster companion indexing, a much smaller `requestId` compaction tail, and a
much lower chance of another OOM kill.
