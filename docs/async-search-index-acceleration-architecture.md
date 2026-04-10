# Async Search Index Acceleration Architecture

Status: current shipped design and regression plan as of 2026-04-09

This document describes the shipped async indexing architecture for `evlog`-style
streams, the memory findings that drove the current design, and the regression
strategy we now use to keep async job memory bounded.

It is intentionally current-state focused. Older optimization hypotheses and
intermediate designs have been removed where they no longer match the code.

## Objectives

Primary goals:

- keep the long-lived server process out of OOM territory during backlog drain
- keep every async indexing job shape below `100 MiB` peak contributed RSS in
  workload tests
- keep parent-process contributed RSS below `100 MiB` while async jobs run
- keep exact L0 and exact compaction jobs under `5s` on the live `evlog` stream
- improve total exact frontier catch-up without reintroducing memory regressions

Non-goals:

- changing manifest publication semantics
- moving heavy indexing work into request handlers
- preserving old worker-thread-based async execution

## Current Shipped Architecture

### Async execution boundary

Heavy async indexing jobs no longer run inside shared `worker_threads`.

The generic async build executor now runs each job in its own short-lived Bun
subprocess:

- `/Users/sorenschmidt/code/streams/src/index/index_build_worker_pool.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_executor.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_subprocess.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_wire.ts`

This is the supported isolation model for async indexing. The parent process
coordinates, uploads, updates SQLite state, and moves local cache files, but the
heavy segment/run decoding and artifact building happen in a separate process.

### Async job kinds

The current shipped async indexing job kinds are:

- `routing_l0_build`
- `routing_compaction_build`
- `lexicon_l0_build`
- `lexicon_compaction_build`
- `secondary_l0_build`
- `secondary_compaction_build`
- `companion_build`
- `companion_merge_build`

These are defined in:

- `/Users/sorenschmidt/code/streams/src/index/async_index_actions.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_job.ts`

### Per-job memory telemetry

Each async build subprocess now samples its own RSS during execution and writes
that into the async action detail JSON in SQLite.

Current fields:

- `job_worker_pid`
- `job_rss_baseline_bytes`
- `job_rss_peak_bytes`
- `job_rss_peak_contributed_bytes`

This telemetry comes from:

- `/Users/sorenschmidt/code/streams/src/index/index_build_telemetry.ts`
- `/Users/sorenschmidt/code/streams/src/index/async_index_actions.ts`

The baseline is sampled at job start, peak is sampled during the job, and
contributed RSS is `max(0, peak - baseline)`.

### Exact indexing shape on `evlog`

The shipped `evlog` exact path is now exact-only on the runtime critical path.

`search_segment_build` still exists, but exact owner jobs call it with
`includeCompanion: false`:

- `/Users/sorenschmidt/code/streams/src/search/search_segment_build.ts`
- `/Users/sorenschmidt/code/streams/src/index/secondary_indexer.ts`

That means:

- exact L0 no longer bundles companion work into the same runtime job
- the exact owner batch only pays for exact work
- companion work is handled independently by the companion manager

### Companion indexing shape on `evlog`

Fresh `evlog` companion work is no longer “piggybacked from exact” in the old
sense. Instead, companion work is split into smaller file-backed partial jobs
plus a cheap merge job:

- `/Users/sorenschmidt/code/streams/src/search/companion_build.ts`
- `/Users/sorenschmidt/code/streams/src/search/companion_merge.ts`
- `/Users/sorenschmidt/code/streams/src/search/companion_manager.ts`

Current partial plan families:

- `col`
- `keyword-core`
- `keyword-requestId`
- `keyword-traceId` split into `2` shards
- `keyword-spanId`
- `keyword-path`
- `text-message`
- `text-context`

The manager runs those smaller `companion_build` jobs, then a
`companion_merge_build` job combines the partial outputs into the final `.cix`
artifact.

This is the main memory reduction change for bundled companion work.

### File-backed artifact flow

Heavy async jobs now write outputs to files rather than returning large
in-memory payloads through the parent process.

That applies to:

- exact L0 outputs
- exact compaction outputs
- companion partial outputs
- companion merged outputs

The parent process uploads from file and moves the same file into the local
cache when appropriate.

## Why The Architecture Changed

The original async acceleration work optimized time first, but the live server
later showed repeated anonymous RSS climbs into the multi-gigabyte range.

Two separate problems were identified:

1. the long-lived parent process retained anonymous RSS when large build results
   crossed process boundaries inefficiently
2. some individual build jobs were intrinsically too large and needed to be
   split into smaller workload shapes

The current design addresses both:

- subprocess-per-job for parent-process containment
- smaller `evlog` companion workloads for job-local containment

## Memory Findings

### Old failure mode

The live server had previously been OOM-killed with anonymous RSS above `3 GiB`
while exact/search backlog drain was active.

That was enough to rule out “normal JS heap growth” as the whole explanation.

### Workload-scoped RSS harness

We now measure each async indexing workload directly with a dedicated local RSS
harness:

- `/Users/sorenschmidt/code/streams/test/index_job_rss.test.ts`
- `/Users/sorenschmidt/code/streams/test/helpers/index_job_rss_scenarios.ts`
- `/Users/sorenschmidt/code/streams/test/helpers/index_job_rss_runner.ts`
- `/Users/sorenschmidt/code/streams/test/helpers/process_tree_rss_sampler.ts`

Each scenario runs in a fresh subprocess and records:

- baseline RSS
- peak RSS
- settled RSS
- peak contributed RSS
- settled contributed RSS
- baseline/peak/settled `heapUsed`
- baseline/peak/settled `external`
- baseline/peak/settled `arrayBuffers`

### Parent-process RSS regression harness

We also measure what the long-lived parent process retains while dispatching
async jobs:

- `/Users/sorenschmidt/code/streams/test/index_build_worker_pool_rss.test.ts`

This suite verifies:

- parent-process peak contributed RSS stays below `100 MiB`
- parent-process settled contributed RSS stays below `100 MiB`
- repeated subprocess jobs do not ratchet parent RSS upward over time

### Important measurement correction

The RSS sampler was initially overcounting by including the sampler child itself
in the measured process tree. That has now been corrected in:

- `/Users/sorenschmidt/code/streams/test/helpers/process_tree_rss_sampler.ts`

After that correction, the per-job numbers became usable as a real regression
gate.

## Current Local RSS Results

Latest workload-harness measurements after the split-companion changes:

- `routing_lexicon_l0_build`: about `9.0 MiB`
- `routing_compaction_build`: about `0.9 MiB`
- `lexicon_compaction_build`: about `0.8 MiB`
- `secondary_l0_build`: about `41.5 MiB`
- `secondary_compaction_build`: about `1.5 MiB`
- `companion_build_col`: about `63.6 MiB`
- `companion_build_keyword_core`: about `89.7 MiB`
- `companion_build_keyword_request_id`: about `92.2 MiB`
- `companion_build_keyword_trace_id_shard_1`: about `70.4 MiB`
- `companion_build_keyword_trace_id_shard_2`: about `71.0 MiB`
- `companion_build_keyword_span_id`: about `91.3 MiB`
- `companion_build_keyword_path`: about `82.0 MiB`
- `companion_build_text_message`: about `83.3 MiB`
- `companion_build_text_context`: about `78.0 MiB`
- `companion_merge_build`: about `10.0 MiB`
- `search_segment_build_exact_only`: about `42.6 MiB`

This means every shipped async indexing workload shape currently stays below the
`100 MiB` job-local target in the local harness.

### What that means

The strictest requirement is now satisfied in the regression suite:

- every measured async indexing job kind is under `100 MiB`
- the long-lived parent process also stays under `100 MiB` contributed RSS
  while coordinating those jobs

That is better than the older state where large exact/search jobs could measure
hundreds of MiB in-process.

## Current Live Expectations

The live server should now behave like this during backlog drain:

- the server process remains in the few-hundred-MiB range instead of climbing
  into multi-gigabyte anonymous RSS
- individual async action rows now record a per-job peak RSS watermark
- companion work on `evlog` appears as multiple `companion_build` rows plus one
  `companion_merge_build` row instead of one large monolithic companion job

The exact live values depend on the backlog mix, but the intended shape is:

- steady-state server RSS in the `300-600 MiB` band
- spike ceiling below `1 GiB`
- `job_rss_peak_contributed_bytes` below `100 MiB` for each async job kind

## Current Performance State

The biggest performance improvements already shipped are:

- exact L0 is no longer monolithic across the entire `evlog` exact field set
- high-cardinality exact compaction now streams and stages sources to files
- fresh companion work is decomposed into smaller slice jobs

The remaining live performance concern is not the old memory blow-up. It is
throughput and catch-up:

- exact compaction is mostly in bounds
- most exact L0 jobs are already in bounds
- the remaining long-pole work is total exact frontier advancement versus
  uploaded-head growth

That should now be addressed without sacrificing the new memory guarantees.

## Latest Live Result

The current shipped `2 GiB` live shape now keeps both async job memory and
steady-state exact latency inside the target band:

- recent `async_index_actions` rows show `0` jobs above `100 MiB`
  `job_rss_peak_contributed_bytes`
- recent post-restart exact / compaction rows show `0` jobs above `5s` in a
  `91`-row live sample
- the live server stayed healthy with:
  - current RSS about `414 MiB`
  - restart-window RSS high-water about `493 MiB`

The two changes that closed the remaining live tail were:

1. exact run uploads now keep local artifacts on the file-backed `putFileNoEtag`
   path when that API is available, because the live R2 benchmark for the
   `~780 KiB` exact artifacts was faster and lower-tail than re-reading the file
   into `putNoEtag`
2. high-cardinality exact L0 jobs (`requestId`, `traceId`, `spanId`, `path`)
   now stay on single-field jobs but use a bounded `span=2` window, and their
   dedicated multi-segment fast path scans top-level scalar JSON tokens and
   hashes raw bytes where possible, without paying either the generic
   full-JSON parse loop or the generic scalar-extraction path on the hot path

That leaves the supported direction unchanged: keep the memory-safe split job
shapes, use bounded multi-segment windows only where the live throughput gain is
real, and continue pushing catch-up via narrower hot-path optimizations rather
than wide exact spans.

The later soak experiments narrowed that supported direction further:

- keep the shared async scheduler at the default `2/2` setting on the current
  `evlog` load; raising it to `3/3` increased recent `secondary_l0_build`
  latency enough to make catch-up worse rather than better
- keep high-cardinality singleton exact jobs (`requestId`, `traceId`, `spanId`,
  `path`) at bounded `span=2`; widening them further stayed inside the widened
  `150 MiB` memory budget, but regressed per-segment throughput
- do not add the same token-scanning multi-segment fast path to the grouped
  low-cardinality batches until it shows a measured win; the attempted grouped
  variant regressed the local `span=4` workload and was not kept

The current supported remote-read policy is also now narrower:

- segment cache fills prefer `getFile(...)` when the object store supports it,
  so remote cache misses download to disk and then mmap/read locally instead of
  materializing the whole segment via `arrayBuffer()`
- secondary exact-run loads prefer `getFile(...)` into the run disk cache when
  that cache is enabled
- only tiny exact compaction sources still inline-fetch into memory, and that
  path remains bounded by the small inline-fetch byte cap
- the R2 object store now uses direct SigV4-signed `fetch(...)` calls for
  object GET/PUT/HEAD/DELETE/LIST and no longer routes runtime data paths
  through `Bun.S3Client` / `Bun.S3File.arrayBuffer()`
- `getFile(...)` on R2 now streams the fetch body to disk instead of using
  `Bun.write(path, s3File)`

## Regression Strategy

We are not relying on runtime kill switches or soft-limit enforcement for the
`100 MiB` target. The supported confidence model is test-driven:

1. every async indexing job kind must have a workload RSS scenario
2. each scenario must run in a fresh subprocess
3. each scenario must assert peak contributed RSS `<= 100 MiB`
4. the parent-process executor regression suite must also stay below `100 MiB`
5. any architectural change that merges or widens workload shapes must update
   these tests in the same change

This keeps the boundary explicit and avoids a design where production depends on
runtime self-throttling to stay alive.

## Current Plan

The next optimization work should follow this order:

1. keep the current per-job RSS regression suite green for every async job kind
2. verify the live server is emitting sane `job_rss_peak_contributed_bytes`
   values in SQLite for all async job kinds
3. continue optimizing exact frontier catch-up, starting with the slowest exact
   L0 batches, but do not merge workload shapes in ways that invalidate the
   `100 MiB` local job bound
4. keep file-backed handoff as the default for heavy async artifacts
5. prefer split / streaming / sharded workloads over wider monolithic jobs

## Validation

Minimum validation for async-index memory work:

- `bun run typecheck`
- `bun run check:result-policy`
- `bun test test/index_job_rss.test.ts --timeout 120000`
- `bun test test/index_build_worker_pool_rss.test.ts --timeout 120000`
- `bun test test/async_index_actions.test.ts --timeout 120000`

When the async execution boundary changes materially, also verify on the live
server that:

- the server remains healthy during backlog drain
- per-job async action rows include the memory watermark fields
- server RSS no longer ratchets upward over a real ingest/index soak

## Source Files

Relevant shipped implementation:

- `/Users/sorenschmidt/code/streams/src/index/index_build_worker_pool.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_executor.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_subprocess.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_wire.ts`
- `/Users/sorenschmidt/code/streams/src/index/index_build_telemetry.ts`
- `/Users/sorenschmidt/code/streams/src/index/async_index_actions.ts`
- `/Users/sorenschmidt/code/streams/src/index/secondary_indexer.ts`
- `/Users/sorenschmidt/code/streams/src/index/run_payload_upload.ts`
- `/Users/sorenschmidt/code/streams/src/index/run_format.ts`
- `/Users/sorenschmidt/code/streams/src/index/secondary_l0_build.ts`
- `/Users/sorenschmidt/code/streams/src/search/search_segment_build.ts`
- `/Users/sorenschmidt/code/streams/src/search/companion_build.ts`
- `/Users/sorenschmidt/code/streams/src/search/companion_merge.ts`
- `/Users/sorenschmidt/code/streams/src/search/companion_manager.ts`

Relevant regression tests:

- `/Users/sorenschmidt/code/streams/test/index_job_rss.test.ts`
- `/Users/sorenschmidt/code/streams/test/index_build_worker_pool_rss.test.ts`
- `/Users/sorenschmidt/code/streams/test/async_index_actions.test.ts`
- `/Users/sorenschmidt/code/streams/test/helpers/index_job_rss_scenarios.ts`
- `/Users/sorenschmidt/code/streams/test/helpers/index_job_rss_runner.ts`
- `/Users/sorenschmidt/code/streams/test/helpers/process_tree_rss_sampler.ts`
