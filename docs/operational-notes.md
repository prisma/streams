# Prisma Streams Operational Notes

This document describes operational knobs, SQLite settings, and how to diagnose stalls.

## Configuration knobs

Most commonly tuned environment variables are listed below. For the broader
runtime overview and command surface, see `overview.md`.

- `DS_ROOT`: data directory (default `./ds-data`)
- `DS_DB_PATH`: SQLite file path (default `${DS_ROOT}/wal.sqlite`)
- `DS_SEGMENT_MAX_BYTES`: segment seal threshold (default 16 MiB; auto-tune preserves 16 MiB on every preset)
- `DS_BLOCK_MAX_BYTES`: max uncompressed bytes per DSB3 block (default 1 MiB; auto-tune preserves 1 MiB on every preset)
- `DS_SEGMENT_TARGET_ROWS`: segment seal threshold by row count (default 100k; auto-tune preserves 100k rows on every preset)
- `DS_SEGMENT_MAX_INTERVAL_MS`: max time between segment cuts (default 0; 0 disables time-based sealing)
- `DS_SEGMENT_CHECK_MS`: segmenter tick interval (default 250ms)
- `DS_SEGMENTER_WORKERS`: background segmenter worker threads (default 0; auto-tune uses `1` on 1 GiB and `2` on 2–8 GiB presets)
- `DS_UPLOAD_CHECK_MS`: uploader tick interval (default 250ms)
- `DS_UPLOAD_CONCURRENCY`: max concurrent uploads (default 4; auto-tune uses `2` on 1 GiB, `4` on 2–4 GiB, and `8` on 8 GiB)
- `DS_BASE_WAL_GC_CHUNK_OFFSETS`: max base-WAL rows deleted per GC sweep/manifest commit transaction (default 1,000,000)
- `DS_BASE_WAL_GC_INTERVAL_MS`: minimum delay between touch-manager base-WAL GC sweeps per stream (default 1000ms)
- `DS_SEGMENT_CACHE_MAX_BYTES`: on-disk segment cache cap (default 256 MiB)
- `DS_INDEX_L0_SPAN`: segments per L0 index run (default 16)
- `DS_INDEX_BUILDERS`: global worker-thread count for generic index-build jobs (default 1; auto-tune uses `1` on 256–2048 MiB presets, `2` on 4096 MiB, and `4` on 8192 MiB)
- `DS_INDEX_CHECK_MS`: in-process tick interval for the routing-key, exact secondary, `.col`, `.fts`, and `.agg` index managers (default 1000ms)
- `DS_SEARCH_COMPANION_BATCH_SEGMENTS`: uploaded stale segments rebuilt per bundled-companion pass before the manager yields and republishes the manifest (default 4; auto-tune uses `1` on 1–2 GiB presets)
- `DS_SEARCH_COMPANION_YIELD_BLOCKS`: decoded segment blocks processed by one bundled-companion build before it yields back to the event loop (default 4; auto-tune uses `1` on 1–2 GiB presets)
- `DS_SEARCH_COMPANION_FILE_CACHE_MAX_BYTES`: on-disk bundled-companion cache cap for local immutable `.cix` files under `${DS_ROOT}/cache/companions` (default 512 MiB, scaled up on larger backlog settings and capped at 4 GiB)
- `DS_SEARCH_COMPANION_FILE_CACHE_MAX_AGE_MS`: maximum age for cached `.cix` files before startup/admission pruning retires them (default 24h)
- `DS_SEARCH_COMPANION_MMAP_CACHE_ENTRIES`: hot mmap-backed companion bundles retained by the process (default 64)
- `DS_SEARCH_COMPANION_TOC_CACHE_BYTES`: in-memory TOC cache for bundled companions (default 1 MiB unless auto-tune raises it)
- `DS_SEARCH_COMPANION_SECTION_CACHE_BYTES`: in-memory raw section-byte cache for bundled companions (default 16 MiB unless auto-tune raises it)
- `DS_INDEX_RUN_CACHE_MAX_BYTES`: on-disk index-run cache cap (default 256 MiB)
- `DS_INDEX_RUN_MEM_CACHE_BYTES`: in-memory index-run cache cap (default 64 MiB, auto-tuned when memory limit is set)
- `DS_LEXICON_INDEX_CACHE_MAX_BYTES`: on-disk lexicon-run cache cap for local immutable `.lex` files under `${DS_ROOT}/cache/lexicon` (default derived from memory limit; auto-tune uses 8–64 MiB on 256–2048 MiB presets)
- `DS_LEXICON_MMAP_CACHE_ENTRIES`: hot mmap-backed lexicon files retained by the process (default 64)
- `DS_INDEX_COMPACTION_FANOUT`: compaction fanout (default 16)
- `DS_READ_MAX_BYTES`: read response byte cap (default 1 MiB)
- `DS_READ_MAX_RECORDS`: read response record cap (default 1000)
- `DS_APPEND_MAX_BODY_BYTES`: max append body size (default 10 MiB)
- `DS_INGEST_FLUSH_MS`: group-commit flush interval (default 10ms)
- `DS_INGEST_MAX_BATCH_REQS`: max requests per batch (default 200)
- `DS_INGEST_MAX_BATCH_BYTES`: max payload bytes per batch (default 8 MiB; auto-tune uses smaller values on 1–2 GiB presets)
- `DS_INGEST_MAX_QUEUE_REQS`: max queued append requests (default 50k)
- `DS_INGEST_MAX_QUEUE_BYTES`: max queued append bytes (default 64 MiB; auto-tune uses smaller values on 1–2 GiB presets)
- `DS_INGEST_CONCURRENCY`: max concurrent append/create request bodies admitted to the ingest path (default 2)
- `DS_LOCAL_BACKLOG_MAX_BYTES`: backpressure when unuploaded backlog exceeds this (default 10 GiB; 0 disables).
- `DS_SQLITE_CACHE_BYTES` / `DS_SQLITE_CACHE_MB`: SQLite page cache budget (defaults to 25% of `DS_MEMORY_LIMIT_*` when set)
- `DS_WORKER_SQLITE_CACHE_BYTES` / `DS_WORKER_SQLITE_CACHE_MB`: SQLite page cache budget for worker threads like segmenters and touch processors (defaults to a much smaller fraction of the main cache, capped at 32 MiB)
- `DS_READ_CONCURRENCY`: max concurrent `GET /v1/stream/...` read operations admitted at once (default 4)
- `DS_SEARCH_CONCURRENCY`: max concurrent `_search` and `_aggregate` request executions admitted at once (default 2)
- `DS_ASYNC_INDEX_CONCURRENCY`: shared permit pool for routing, routing-key lexicon, exact, and bundled-companion background work (defaults to `DS_INDEX_BUILDERS` when unset)
- `DS_MEMORY_LIMIT_MB` / `DS_MEMORY_LIMIT_BYTES`: memory-pressure threshold used to reduce search / async-index concurrency and trigger best-effort GC / heap snapshots (default disabled)
- `DS_HEAP_SNAPSHOT_PATH`: optional heap snapshot path to write when the memory-pressure threshold is exceeded; unset by default

Concurrency/load-shedding note:
- Streams no longer rejects HTTP requests just because the process is under memory pressure.
- Instead, it uses bounded concurrency everywhere:
  - ingest/create requests are admitted through `DS_INGEST_CONCURRENCY`
  - read requests are admitted through `DS_READ_CONCURRENCY`
  - `_search` and `_aggregate` requests are admitted through `DS_SEARCH_CONCURRENCY`
  - routing-key indexing, routing-key lexicon indexing, exact indexing, and bundled-companion builds share one `DS_ASYNC_INDEX_CONCURRENCY` gate
  - the global index manager schedules routing, lexicon, exact, and bundled-companion build/compaction work kinds in round-robin order onto the shared `DS_INDEX_BUILDERS` pool
- The memory sampler is now only an adaptive signal:
  - on macOS it confirms high RSS with physical memory from `top -stats pid,mem`
  - on Linux it uses `MemAvailable` from `/proc/meminfo`, not raw `free` memory
  - when over the configured threshold, it reduces `DS_SEARCH_CONCURRENCY` and `DS_ASYNC_INDEX_CONCURRENCY` to `max(1, ceil(base/2))`
  - it never reduces ingest or read concurrency
- While over the threshold, the sampler also rate-limits best-effort `Bun.gc()` calls and optional heap snapshots.
- `GET /v1/server/_details` exposes the configured cache / concurrency budgets,
  selected auto-tune preset, and the node's current effective runtime state.
- When `DS_MEMORY_SAMPLER_PATH` is enabled, each sampler record now also
  includes `memory_subsystems`, which mirrors the grouped runtime memory
  breakdown exposed by `GET /v1/server/_details`.

Companion-cache note:
- Bundled companion reads now fetch the full remote `.cix` object once, store it locally, and mmap the local cached file.
- Because Bun does not currently expose an explicit unmap primitive, a companion file that has been mmapped by the running process is treated as pinned until process restart.
- Startup pruning and new cache admissions retire stale or oldest unmmapped companion files first; if the hot mapped set alone exceeds the disk budget, the process may temporarily sit above the configured cache cap until restart.
- The auto-tuned 1–2 GiB presets also force companion rebuilding into one-segment / one-yield-block passes so aggregate-heavy `.cix` generation does not overlap too aggressively with append, segment cut, and upload work.
- The auto-tuned 1–2 GiB presets keep the same 16 MiB / 100k-row segment geometry as larger hosts. Only concurrency and cache budgets shrink on those presets.
- Routing-key lexicon reads now use the same local immutable-file pattern:
  - freshly built `.lex` runs are seeded into `${DS_ROOT}/cache/lexicon`
  - first read of an uncached `.lex` downloads the full object once, stores it locally, and then serves it from `Bun.mmap()`
  - mmapped `.lex` files are pinned until restart; if the pinned set alone exceeds the lexicon cache budget, the process may sit above the configured cap until restart
- Routing and exact run caches are still decoded in-memory structures, but the
  memory cache now admits runs by their actual encoded object size. On small
  presets this allows the full active routing run set to stay hot for point
  lookups instead of repeatedly decoding large postings runs from disk.
- Segment reads and segment-backed background jobs now follow the same full-object cache policy:
  - the first remote read of a segment downloads the whole `.bin` object and seeds `${DS_ROOT}/cache/`
  - hot cached segment files are served from `Bun.mmap()` views
  - keyed reads walk cached segment bytes in one forward pass, so they avoid both remote range reads and repeated local `open/read/close` calls for tiny slices
  - footer walks, block scans, keyed reads, stream-size reconciliation, exact-index builds, lexicon builds, and bundled-companion builds all read from that local segment file once it exists
  - there are no remote segment range reads in the supported read path anymore
  - segment-backed indexing jobs reserve required cached segment files through the shared segment-locality manager
  - routing-key, routing-key lexicon, and exact-secondary L0 builds each reserve the next `DS_INDEX_L0_SPAN` uploaded segment files as **required for indexing**
  - bundled companion builds reserve one uploaded segment file at a time through the same lease mechanism
  - required-for-indexing segment files are never evicted while their lease is active
  - once the relevant job finishes and its indexed watermark or companion catalog advances, those leases are released immediately
  - unlike the lexicon / companion / run caches, the segment cache may evict the on-disk file for an already-mmapped segment while keeping the live mapping resident in-process; this keeps the on-disk segment cache within `DS_SEGMENT_CACHE_MAX_BYTES`
  - if the next required indexing window cannot be retained locally without exceeding the segment-cache budget, append admission may return `429` with `error.code = "index_building_behind"` until indexing catches up
- `DS_OBJECTSTORE_TIMEOUT_MS`: object store request timeout (default 30s). Timed-out object-store writes now abort the underlying upload instead of letting the PUT continue in the background.
- `DS_OBJECTSTORE_RETRIES`: object store retry count (default 3)
- `DS_OBJECTSTORE_RETRY_BASE_MS`: base backoff for retries (default 50ms)
- `DS_OBJECTSTORE_RETRY_MAX_MS`: max backoff for retries (default 2s)
- `DS_EXPIRY_SWEEP_MS`: expired stream sweep interval (default 60s; 0 disables)
- `DS_EXPIRY_SWEEP_LIMIT`: expired streams per sweep tick (default 100)
- `DS_METRICS_FLUSH_MS`: metrics flush interval (default 10s; 0 disables)
- `DS_STATS_INTERVAL_MS`: stats log interval when using `--stats` (default 60s)
- `DS_BACKPRESSURE_BUDGET_MS`: per-request queue-wait budget used by stats (default `DS_INGEST_FLUSH_MS + 1`)
- `PORT`: HTTP listen port (default 8080)

MockR2 env vars (only when using `--object-store local`):
- `DS_MOCK_R2_MAX_INMEM_BYTES` / `DS_MOCK_R2_MAX_INMEM_MB`
- `DS_MOCK_R2_SPILL_DIR`

Indexing note:
- Full mode runs indexing in the server process. There is no separate indexing
  daemon or external indexing service.
- The main process now owns one `GlobalIndexManager`, one shared
  `IndexBuildWorkerPool`, and one shared `IndexSegmentLocalityManager`.
- Heavy segment-backed build compute is dispatched to generic worker jobs:
  - combined routing-key + routing-key lexicon L0 build
  - exact-secondary L0 build
  - bundled companion per-segment build
- Routing, lexicon, exact, and companion family adapters still own
  family-specific backlog discovery and compaction rules, but they no longer
  own separate timers or dedicated worker pools.
- All four families share one async-index gate, so they compete for the same
  bounded top-level permit pool instead of each expanding independently.
- Under backlog, the global index manager keeps draining runnable work kinds
  until no family makes progress, yielding cooperatively between rounds and
  using `DS_INDEX_CHECK_MS` only as a safety wake-up interval.
- Routing-key and routing-key lexicon L0 builds now share one worker pass over
  the same leased 16-segment window and emit both immutable outputs together.
- Background routing, exact, lexicon, and companion builders also yield inside
  segment scans. When a foreground read or search request is active, those
  background loops deliberately back off harder so the server can service the
  request promptly even on 1–2 GiB presets.
- The global index manager does not keep a dedicated scheduler priority lane
  for one async family over another. It round-robins work kinds across routing
  build/compaction, lexicon build/compaction, exact build/compaction, and
  bundled-companion build, and each family simply yields its turn when nothing
  is runnable.
- `GET /v1/server/_mem` runtime counts now expose:
  - `segment_required_for_indexing_files`
  - `runtime_bytes.disk_caches.segment_required_for_indexing_bytes`
- Every async index action now records one local SQLite row in
  `async_index_actions` with:
  - `action_kind`
  - `seq`
  - `begin_time_ms`
  - `end_time_ms`
  - `duration_ms`
  - input and output counts/bytes
  - family-specific `detail_json`
- This log is informational only:
  - it is not mirrored to object storage
  - it is not restored by `--bootstrap-from-r2`
  - stream delete clears the rows for that stream
- The intended target is that each logged async action completes in under
  `1000 ms`. A useful inspection query is:

```sql
SELECT seq, stream, action_kind, status, duration_ms, input_count, input_size_bytes, output_size_bytes, detail_json
FROM async_index_actions
ORDER BY seq DESC
LIMIT 50;
```
- Every segment build now also records one local SQLite row in
  `segment_build_actions` with:
  - `action_kind`
  - `seq`
  - `begin_time_ms`
  - `end_time_ms`
  - `duration_ms`
  - input row count / payload bytes
  - output segment bytes
  - start/end offsets and segment index
  - segment-build-specific `detail_json`
- This log is also informational only:
  - it is not mirrored to object storage
  - it is not restored by `--bootstrap-from-r2`
  - stream delete clears the rows for that stream
- Segment-build telemetry on worker-thread presets now reflects the actual
  worker hot path:
  - DSB3 block encoding uses one preallocated uncompressed buffer and one final
    output buffer per block
  - the worker-thread segmenter path does not inject timer-based cooperative
    yields inside a single segment build
  - the main-thread segmenter path still retains cooperative yields so request
    handling is not starved when `DS_SEGMENTER_WORKERS=0`
  - `detail_json` now splits the hot path into WAL fetch, row materialization,
    full WAL loop, block encode, write, fsync, rename, commit, and busy-retry
    wait so the next optimization pass can target the dominant stage
- A useful inspection query is:

```sql
SELECT seq, stream, action_kind, status, duration_ms, input_count, input_size_bytes, output_size_bytes,
       segment_index, start_offset, end_offset, detail_json
FROM segment_build_actions
ORDER BY seq DESC
LIMIT 50;
```
- If `429 index_building_behind` is active, background indexing still keeps
  running; the reject path is there to let indexing drain and release required
  local segment leases, not to pause indexing.

Deleted-stream note:
- `DELETE /v1/stream/{name}` is a tombstone plus local acceleration scrub, not a synchronous remote object purge.
- The delete transaction removes routing, exact, lexicon, and bundled-companion state for that stream immediately, and also clears per-stream object-store request-accounting counters.
- The same delete scrub also clears local `async_index_actions` rows for that
  stream.
- The same delete scrub also clears local `segment_build_actions` rows for that
  stream.
- The delete path clears the same request counters again after the tombstone manifest publish, so recreating the same stream name starts from zeroed request-accounting state.
- Startup also scans tombstoned streams and repeats the same scrub before async-index loops start, so stale deleted-stream counters or async-index rows do not survive a restart.
- If a deleted stream ever contributes async-index work after restart, treat that as a bug.

## SQLite PRAGMAs

Applied on open (see `src/db/schema.ts`):
- `journal_mode=WAL`
- `synchronous=FULL` (default; use `NORMAL` for benchmarks only)
- `foreign_keys=ON`
- `busy_timeout=5000`
- `temp_store=MEMORY`

If you need to cap memory, set SQLite `cache_size` manually at startup.

## Recommended profiles

### Low memory (0.5–1 GB RAM)
- `DS_SEGMENT_MAX_BYTES=8MiB`
- `DS_BLOCK_MAX_BYTES=1MiB`
- `DS_UPLOAD_CONCURRENCY=2`
- `DS_INGEST_MAX_BATCH_BYTES=4MiB`
- `DS_READ_MAX_BYTES=512KiB`
- SQLite `cache_size` around 32–64 MiB

### Medium (1–4 GB RAM)
- `DS_SEGMENT_MAX_BYTES=16MiB`
- `DS_BLOCK_MAX_BYTES=1MiB`
- `DS_UPLOAD_CONCURRENCY=2–8`
- `DS_INGEST_MAX_BATCH_BYTES=4–8MiB`
- `DS_READ_MAX_BYTES=1–4MiB`
- SQLite `cache_size` around 128–256 MiB
- Worker SQLite caches around 16–32 MiB each
- On hosts near the low end of this range, prefer `DS_SEGMENTER_WORKERS=1`,
  `DS_UPLOAD_CONCURRENCY=2`, and `DS_SEARCH_COMPANION_BATCH_SEGMENTS=1` while
  keeping the default 16 MiB / 100k-row segment geometry.

Segment geometry across presets:
- all auto-tune presets seal at `16 MiB` or `100,000` rows, whichever is reached first
- smaller presets reduce overlap and memory pressure by lowering queue sizes,
  worker counts, and background concurrency instead of emitting many more
  smaller segment objects
- segmenting also uses a cheap trailing compression heuristic over the latest
  `8` sealed segments for the same stream:
  - it sums each segment's stored `payload_bytes` and compressed `size_bytes`
  - if recent compression is stronger than `2:1`, it raises the logical byte
    seal target so the next segment aims for at least `50%` of
    `DS_SEGMENT_MAX_BYTES` after compression
  - this never lowers the target below `DS_SEGMENT_MAX_BYTES`
  - cut eligibility uses the same raised byte target, so the segmenter waits
    for enough logical backlog instead of starting immediately at the base
    byte threshold
  - this is best-effort only and still respects `DS_SEGMENT_TARGET_ROWS`

## Auto-tune presets

`--auto-tune` now chooses both cache sizes and concurrency caps. Current
presets:

- `256 MiB`:
  - geometry: segment `16 MiB`, block `1 MiB`, segment rows `100k`
  - caches: SQLite `16 MiB`, worker SQLite `8 MiB`, index-run memory `4 MiB`, lexicon cache `8 MiB`, companion TOC `1 MiB`, companion section `8 MiB`
  - concurrency: ingest `1`, read `2`, search `1`, async index `1`, index builders `1`, uploads `1`, segmenter workers `1`
- `512 MiB`:
  - geometry: segment `16 MiB`, block `1 MiB`, segment rows `100k`
  - caches: SQLite `32 MiB`, worker SQLite `8 MiB`, index-run memory `8 MiB`, lexicon cache `16 MiB`, companion TOC `1 MiB`, companion section `8 MiB`
  - concurrency: ingest `1`, read `2`, search `1`, async index `1`, index builders `1`, uploads `1`, segmenter workers `1`
- `1024 MiB`:
  - geometry: segment `16 MiB`, block `1 MiB`, segment rows `100k`
  - caches: SQLite `64 MiB`, worker SQLite `8 MiB`, index-run memory `16 MiB`, lexicon cache `32 MiB`, companion TOC `1 MiB`, companion section `16 MiB`
  - concurrency: ingest `2`, read `4`, search `2`, async index `1`, index builders `1`, uploads `2`, segmenter workers `1`
- `2048 MiB`:
  - geometry: segment `16 MiB`, block `1 MiB`, segment rows `100k`
  - caches: SQLite `128 MiB`, worker SQLite `16 MiB`, index-run memory `32 MiB`, lexicon cache `64 MiB`, companion TOC `1 MiB`, companion section `32 MiB`
  - concurrency: ingest `2`, read `4`, search `2`, async index `1`, index builders `1`, uploads `4`, segmenter workers `2`
- `4096 MiB`:
  - geometry: segment `16 MiB`, block `1 MiB`, segment rows `100k`
  - caches: SQLite `256 MiB`, worker SQLite `32 MiB`, index-run memory `64 MiB`, lexicon cache `128 MiB`, companion TOC `2 MiB`, companion section `64 MiB`
  - concurrency: ingest `4`, read `8`, search `4`, async index `2`, index builders `2`, uploads `4`, segmenter workers `2`
- `8192 MiB`:
  - geometry: segment `16 MiB`, block `1 MiB`, segment rows `100k`
  - caches: SQLite `512 MiB`, worker SQLite `32 MiB`, index-run memory `128 MiB`, lexicon cache `256 MiB`, companion TOC `4 MiB`, companion section `128 MiB`
  - concurrency: ingest `8`, read `16`, search `8`, async index `4`, index builders `4`, uploads `8`, segmenter workers `4`

## Diagnosing stalls

When throughput drops, check in this order:

1) Ingest queue backlog (append latency spikes)
- Reduce `DS_INGEST_MAX_BATCH_REQS` or increase `DS_INGEST_FLUSH_MS`.
- Inspect `GET /v1/server/_details` for current ingest queue fill and gate state.

2) Segmenter backlog (pending_bytes high)
- Reduce `DS_SEGMENT_MAX_BYTES` or decrease `DS_SEGMENT_CHECK_MS`.
- On small hosts, prefer lowering concurrency before changing segment geometry.
  Only reduce `DS_SEGMENT_TARGET_ROWS` intentionally if you want more frequent
  row-driven sealing.

3) Upload backlog (segments stuck locally)
- Increase `DS_UPLOAD_CONCURRENCY` if network allows.
- Check object store latency and error rates.
- Inspect `GET /v1/server/_details` and `tieredstore.upload.pending_segments`.
- Remember the uploader preserves a contiguous uploaded prefix per stream:
  - it always starts from the earliest missing segment for that stream
  - it may upload the earliest few pending segments in parallel, up to the
    configured upload concurrency
  - later segments from the same stream are not allowed to bypass that gap
  - so one old upload timeout can keep `uploaded_through` and WAL GC pinned
    until that exact missing segment succeeds
- `GET /v1/server/_details` now exposes `runtime.uploads.path`, which includes:
  - the last segment-selection duration and selected-window size
  - segment PUT / mark-uploaded timing totals and last values
  - manifest build / PUT / commit timing totals and last values
  - attempt / success / failure / timeout counters for segment and manifest work

4) Bundled companion lag (search coverage behind uploads)
- Check `/_details` or `/_index_status` for bundled companion coverage.
- Watch `tieredstore.companion.lag.segments` and
  `tieredstore.companion.build.latency` in `__stream_metrics__`.
- Reduce `DS_SEARCH_COMPANION_BATCH_SEGMENTS` or
  `DS_SEARCH_COMPANION_YIELD_BLOCKS` if backfill is making the server feel
  sluggish under large `.fts` fields.
- The internal `__stream_metrics__` system stream no longer builds routing,
  lexicon, exact, `.col`, `.fts`, `.agg`, or `.mblk` families. If you still
  see heavy bundled companion work after restart, look for user streams rather
  than self-indexing on the internal metrics stream.

5) SQLite write stalls
- Ensure the DB is on fast local SSD.
- Keep `synchronous=FULL` for correctness; `NORMAL` only for benchmarks.

6) RSS keeps climbing above the configured memory threshold
- Check worker-thread fanout and keep `DS_WORKER_SQLITE_CACHE_MB` much smaller than the main SQLite cache.
- Confirm the process is actually making forward progress and that the fixed-span exact indexes are not just waiting for the next full span.
- Lower `DS_SEARCH_CONCURRENCY` or `DS_ASYNC_INDEX_CONCURRENCY` if query and background indexing overlap too aggressively.
- Leave `DS_HEAP_SNAPSHOT_PATH` unset unless you are actively debugging memory, since heap snapshots increase peak RSS while they are being written.
- Watch `process.memory.pressure`, `process.rss.current.bytes`,
  `process.rss.max_interval.bytes`, `process.heap.used.bytes`,
  `tieredstore.memory.subsystem.bytes`, and `tieredstore.concurrency.*` in
  `__stream_metrics__`, plus:
  - `GET /v1/server/_details`
  - `GET /v1/server/_mem`
  to determine whether the growth is coming from:
  - retained JS-side state
  - file-backed mmap caches
  - live ingest pipeline buffers
  - SQLite allocator growth
  - or unattributed native/anonymous RSS
- Use `/_mem.process_breakdown.unattributed_rss_bytes` and
  `/_mem.process_breakdown.unattributed_anon_bytes` as the primary "what is
  still unexplained?" signals.
- Use `/_mem.sqlite` to distinguish SQLite allocator growth from JS heap growth.
- Use `/_mem.runtime_counts.sqlite_prepared_statements` together with
  `/_mem.sqlite.open_connections` to confirm whether prepared statement warmup
  stabilizes or keeps growing unexpectedly. With the current adapter design,
  repeated literal SQL should plateau at one prepared statement per connection
  and SQL text, not increase linearly with request count.
- Treat SQLite statement lifetime as a first-class memory discipline:
  - all repeated SQLite work should go through prepared statements, not dynamic
    `exec()`-style SQL assembly in hot paths
  - any fresh Bun [`Statement`](https://bun.com/reference/bun/sqlite/Statement)
    that is not intentionally retained in the adapter cache should be
    finalized immediately after use, ideally with `finalize()` in `finally` or
    with Bun's disposal semantics
  - the healthy steady-state shape for an app is about a dozen live prepared
    statements or fewer; materially higher counts should be treated as
    suspicious unless one explicit bounded cache explains them
- Use `/_mem.gc` to verify whether forced GC is actually reclaiming meaningful
  memory.
- Use `/_mem.top_streams` when the issue may be isolated to one stream's WAL,
  local segment cache, touch journal, or notifier waiters.

## Recovery tips

- On restart, the server resumes pending segments and uploads from SQLite state.
- Temporary segment files (`*.tmp`) are cleaned up by the segmenter on next run.

## Debug scripts

There are no bundled debug tools yet. Use the SQLite DB (`wal.sqlite`) and
stats logs to inspect state.
