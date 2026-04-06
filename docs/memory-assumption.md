# Memory Attribution Assumptions

This document ranks the current memory observability groups from most likely to
least likely to explain the server RSS growth we are seeing on the current
`golden-stream-4` baseline.

This is a working hypothesis document, not a proof. The ranking is based on the
current implementation, the observability surfaces documented under
[`docs/memory-observability/`](./memory-observability), and the current live
baseline where:

- the stream has no routing key
- no search or aggregate families are configured
- async indexing is idle
- RSS still rises toward `~1 GiB` while ingest continues

That combination is important because it rules out most index-related memory
growth and leaves the core ingest/runtime path as the main suspect.

## Ranking

### 1. `process_breakdown.unattributed_anon_bytes` / `process_breakdown.unattributed_rss_bytes`

Most likely current contributor.

Why this ranks first:

- it is the remaining process RSS after subtracting:
  - JS-managed bytes
  - tracked mapped-file bytes
  - tracked SQLite runtime bytes
- on the current no-index baseline, file-backed memory is small and indexing is
  off, so a large unattributed anonymous bucket is the cleanest signal that the
  unexplained growth is inside the core runtime path
- this bucket is where native allocator growth, fragmentation, untracked SQLite
  memory, Bun runtime buffers, and other uninstrumented anonymous allocations
  will show up first

Interpretation:

- if this bucket is large and keeps climbing while other named buckets stay
  modest, the observability model is telling us that the main owner is still
  missing
- this is the first field to check when RSS rises unexpectedly

### 2. `sqlite` / `runtime_bytes.sqlite_runtime`

Most likely named subsystem after unattributed anonymous RSS.

Why this ranks second:

- even the bare `golden-stream-4` baseline still does heavy SQLite work for WAL,
  segment metadata, manifest state, and stream bookkeeping
- the no-index baseline removes routing, lexicon, exact, companion, and search
  backfill from the equation, which increases the relative likelihood that
  SQLite is a major contributor
- SQLite memory is anonymous/private memory, so it is exactly the kind of thing
  that can hide under `unattributed_anon_bytes` when the runtime stats are
  incomplete

Interpretation:

- rising `memory_used_bytes`, `pagecache_overflow_bytes`, or `malloc_count`
  alongside rising RSS should move SQLite from "likely" to "confirmed"
- if SQLite stays small while RSS rises, the culprit is elsewhere in the core
  runtime

### 3. `process.heap_used_bytes`, `process.external_bytes`, `process.array_buffers_bytes`, `process_breakdown.js_managed_bytes`

High-probability direct contributor.

Why this ranks third:

- these are the largest explicit JS/runtime-owned process allocations we expose
- the ingest path necessarily allocates JSON payloads, row batches, segment
  metadata, and transient buffers in JS and in Bun-managed native memory
- `external_bytes` and `array_buffers_bytes` are especially important because
  they capture process memory that is not visible from `heap_used_bytes` alone

Interpretation:

- if RSS rises roughly in step with `js_managed_bytes`, the issue is probably
  not hidden native memory but retained JS/runtime state
- if RSS rises much faster than `js_managed_bytes`, move attention back to
  SQLite or the unattributed anonymous bucket

### 4. `runtime_bytes.pipeline_buffers`

Plausible direct contributor during ingest, but should be bursty rather than
sticky.

Why this ranks fourth:

- this is the one instrumentation group that directly targets the active
  ingest/segment/upload pipeline
- the current baseline is ingest-only, so if RSS spikes around segment cuts or
  uploads, this is the most relevant named runtime bucket

Why it is not higher:

- these values are designed to rise and fall with work
- sustained RSS growth with small or flat pipeline buffers means the memory is
  being retained elsewhere

Interpretation:

- large live values here are expected during bursty ingest
- large RSS with consistently small pipeline buffers means the leak or retained
  working set is not in the active pipeline

### 5. `top_streams.pending_wal_bytes`

Useful stream-local suspect for ingest-only workloads.

Why this ranks fifth:

- retained WAL bytes are directly relevant to a stream that is actively
  ingesting and not indexing
- unlike segment disk cache bytes, retained WAL bytes correspond to live local
  state that can plausibly contribute to active memory pressure indirectly

Why it is not higher:

- retained WAL is usually much smaller than total RSS
- it is still not a direct process-memory measurement; it is a stream-local
  retained-state indicator

Interpretation:

- if one stream dominates pending WAL and RSS rises with it, that stream is the
  right place to inspect first
- if pending WAL is small while RSS rises, WAL retention is not the main cause

### 6. `runtime_bytes.heap_estimates`

Moderately useful, but incomplete by design.

What it currently includes:

- `ingest_queue_payload_bytes`
- `routing_run_cache_bytes`
- `exact_run_cache_bytes`
- `mock_r2_in_memory_bytes`

Why this ranks sixth:

- these are real byte owners when present
- but on the current no-index baseline they should be small or zero except for
  the ingest queue during bursts
- they intentionally do not represent the entire JS heap or native allocations

Interpretation:

- treat this group as confirmation for a narrow set of owners, not as a full
  heap attribution system

### 7. `runtime_bytes.mapped_files`

Low-probability contributor on the current baseline, but important on indexed
or search-heavy nodes.

Why this ranks seventh:

- `golden-stream-4` has no routing, lexicon, exact, or companion work
- the current mmap footprint is therefore expected to be small
- Linux RSS attribution distinguishes file-backed memory, and recent live checks
  showed file-backed RSS was much smaller than anonymous RSS

Interpretation:

- if file-backed RSS grows with mapped-file bytes, mmap-backed caches become a
  serious suspect
- on the current baseline this group should remain low and is unlikely to
  explain most of the RSS growth

### 8. `top_streams.local_storage_bytes`

Useful context, but easy to misread as RAM.

Why this ranks eighth:

- this surface is very good for identifying which stream owns retained local
  bytes
- but those bytes combine:
  - retained WAL
  - segment cache bytes
  - index cache bytes
- most of that is local disk occupancy, not direct process RSS

Interpretation:

- use this to find ownership by stream
- do not treat it as a direct memory measurement

### 9. `runtime_bytes.disk_caches` / `runtime_totals.disk_cache_bytes`

Low-probability direct contributor to RSS.

Why this ranks ninth:

- these values measure local cache occupancy on disk
- they are intentionally broader than mapped-file RSS
- a large segment or index disk cache does not imply a large process working set

Interpretation:

- use this for storage footprint and cache-pressure discussions
- do not use it as the primary explanation for resident memory growth

### 10. `runtime_counts` and `counters`

Indirect evidence, not memory ownership.

Why this ranks tenth:

- counts are cardinalities, slots, pinned entry counts, waiter counts, and leak
  candidate indicators
- they are excellent for spotting suspicious growth patterns
- but they are not byte owners by themselves

Interpretation:

- use these to explain *why* a byte bucket may be growing
- not to explain RSS directly

### 11. `gc` and `high_water`

Essential diagnostics, but not contributors.

Why this ranks near the bottom:

- these fields describe behavior over time, not ownership
- they answer questions like:
  - is forced GC happening?
  - is forced GC reclaiming anything?
  - what was the peak value and when?

Interpretation:

- a rising `forced_gc_count` with low reclaimed bytes strengthens the case for
  retained native or non-GC-managed memory
- high-water marks are context, not ownership

### 12. `runtime_bytes.configured_budgets`

Least likely contributor.

Why this ranks last:

- these are configured ceilings, not live allocations
- they are useful for policy and capacity planning only

Interpretation:

- use them to judge whether a runtime group is approaching its intended limit
- never use them as evidence that the process is currently using that memory

## Current Working Conclusion

For the current `golden-stream-4` no-index baseline, the ranking above implies
the following operator workflow:

1. Start with `process.rss_bytes`.
2. Check `process_breakdown.unattributed_anon_bytes` and
   `process_breakdown.unattributed_rss_bytes` first.
3. Compare SQLite runtime usage next.
4. Then compare JS-managed bytes.
5. Only after that inspect pipeline buffers, WAL retention, and stream-local
   ownership views.

The key assumption is:

- if RSS keeps rising on a stream with no indexing enabled,
- and `mapped_files`, `disk_caches`, and `pipeline_buffers` remain modest,
- then the most likely remaining owners are SQLite runtime, Bun runtime/native
  allocations, or some other currently unattributed anonymous memory source.

That is the current best explanation of the baseline behavior. When the live
instrumentation proves otherwise, this document should be updated rather than
left as stale lore.
