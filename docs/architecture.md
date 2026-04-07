# Prisma Streams Architecture

This document describes the architecture of the Prisma Streams Bun + TypeScript
implementation using:
- SQLite (bun:sqlite) as the durable WAL and metadata store
- TieredStore-style segments and manifests
- An R2-compatible object store (MockR2 for tests)

The design prioritizes correctness, bounded memory, and crash safety.

## Stream Model

Every persisted object in the system is still a **stream**: an append-only log
stored in SQLite WAL, materialized into segments, and read back through Durable
Streams semantics.

Streams also carry two pieces of control-plane metadata:

- **profile**: stream semantics
- **schema**: payload structure

Current rule:

- a stream always has a profile
- if no profile was declared when the stream was created, it is treated as
  `generic`
- storage may omit an explicit `generic` declaration and keep only the declared
  profile metadata when present

Implemented built-ins today:

- `evlog`
- `generic`
- `metrics`
- `state-protocol`

`generic` adds no canonical payload envelope and leaves schema management to the
user. `evlog` owns canonical wide-event normalization, redaction, and its
default schema/search/rollup registry on JSON append. `metrics` owns canonical
metrics interval normalization, its default schema/search/rollup registry, and
the metrics-block companion family. `state-protocol` owns the live `/touch/*`
surface and its touch configuration.

See [stream-profiles.md](./stream-profiles.md) for the normative model.

## High-level components

1) HTTP layer (Bun server)
- Parses requests, enforces protocol semantics, and enqueues work.
- Performs only small indexed SQLite reads in the request path.
- Implements long-poll reads without busy loops.
- Resolves the stream profile definition before handling profile-owned
  metadata or routes.
- Admits ingest, read, and search work through bounded in-process concurrency
  gates instead of a direct memory-based reject path.

2) WAL writer (single-writer loop)
- Batches append requests from a bounded queue (group commit).
- Uses a single SQLite transaction per flush to reserve offsets and insert WAL rows.
- Acknowledges appends only after the transaction commits.

3) Segmenter (materializer)
- Periodically selects candidate streams from indexed SQLite metadata.
- Streams WAL rows out of SQLite using iterators to avoid large allocations.
- Builds segment files on disk (temp -> atomic rename) and records segment metadata.

4) Uploader
- Selects pending segments from SQLite and uploads with bounded concurrency.
- For each stream, it selects the earliest contiguous non-uploaded prefix,
  capped by the current upload concurrency.
- Later segments from the same stream do not bypass an older missing gap, but
  the earliest few pending segments may upload in parallel.
- Uploads segments first, then publishes a new manifest generation.
- Advances uploaded_through only after manifest upload succeeds, then GC WAL rows.
- `GET /v1/server/_details` exposes uploader path telemetry for:
  - segment selection
  - segment PUT timing
  - mark-uploaded timing
  - manifest build / PUT / commit timing

5) Global index manager
- The full server now starts one `GlobalIndexManager` in the main process.
- It owns:
  - the only indexing timer (`DS_INDEX_CHECK_MS`)
  - one shared `IndexBuildWorkerPool`, sized by `DS_INDEX_BUILDERS`
  - one shared `IndexSegmentLocalityManager`
  - four family adapters:
    - routing-key
    - routing-key lexicon
    - exact secondary
    - bundled companions (`.col`, `.fts`, `.agg`, `.mblk`) via `SearchCompanionManager`
- The main process still owns scheduling, SQLite state changes, and manifest
  publication.
- All heavy index-build computation now runs on the shared worker pool:
  - routing-key L0 run build
  - routing-key run compaction
  - routing-key lexicon L0 run build
  - routing-key lexicon run compaction
  - exact secondary L0 run build
  - exact secondary run compaction
  - bundled companion per-segment build
- Workers only read leased local files and return immutable artifact bytes or
  section payloads. They do not mutate SQLite or publish manifests.
- The family adapters still own family-specific backlog discovery, queueing,
  and compaction rules, but they no longer own independent timers or dedicated
  worker pools.
- The global manager schedules distinct background work kinds in round-robin
  order:
  - routing build
  - routing-key lexicon build
  - exact secondary build
  - bundled companion build
  - routing compaction
  - routing-key lexicon compaction
  - exact secondary compaction
- There is no separate scheduler priority lane for one async work kind over
  another. If a family is not currently runnable, it simply yields its turn and
  the scheduler advances to the next work kind.
- All indexing families still share one top-level async-index concurrency gate,
  so routing, routing-key lexicon, exact, and bundled-companion work compete
  for the same bounded budget.
- Background index work yields cooperatively at bounded per-record / per-block
  intervals, and it backs off further while foreground read and search
  requests are active. Foreground latency should not depend on one whole index
  build segment finishing first.
- `DS_INDEX_BUILDERS` controls how many generic index-build worker threads are
  started.

Remaining main-thread indexing work:

- selecting the next runnable background work item
- acquiring and releasing segment-cache leases
- cheap SQLite metadata reads to discover backlog
- persisting finished run / companion rows
- retiring superseded runs
- manifest publication
- append admission / `429 index_building_behind` decisions
- object-store GET/PUT initiation for cache misses and artifact publication

The main thread may still fetch an uncached immutable run object into the local
run cache before dispatching a compaction worker, but it no longer decodes or
merges that run on the main event loop.

6) Reader
- Merges historical data from segments (local cache or R2) with tail data in SQLite.
- Supports key-filtered reads and long-poll semantics.
- On a remote segment cache miss, it fetches the whole segment object directly
  from R2 and treats a missing-object GET as `null`, rather than probing object
  existence before the fetch.

7) Object store
- ObjectStore interface with put/get/head/list plus streaming uploads.
- MockR2 implements the interface with deterministic fault injection.

## Profile Runtime

Built-in profiles are implemented under `src/profiles/`.

Each profile definition owns:

- profile validation and normalization
- stored profile parsing and caching
- persistence side effects on update
- optional capability hooks for profile-owned runtime behavior
- optional JSON-ingest normalization hooks for profile-owned write shaping

The registry in `src/profiles/index.ts` is the single place where built-in
profiles are wired into the core engine.

The core engine does not branch on specific profile kinds for supported
profile-owned behavior. It resolves the profile definition and dispatches
through its hooks.

Profile-specific logic must live behind a dedicated profile entry module under
`src/profiles/` and may use a profile-owned subdirectory for its internal
helpers. The core engine should not grow direct `if (profile.kind === "...")`
checks for supported stream semantics.

Today, `state-protocol` uses this model to own:

- touch state seeding
- canonical change derivation for the touch processor
- the `/touch/*` HTTP surface

Today, `evlog` uses the same model to own:

- canonical wide-event normalization on JSON append
- pre-append redaction of sensitive context fields
- routing-key defaults from `requestId` or `traceId`
- default schema-owned `search` and `search.rollups` installation

Today, `metrics` uses the same model to own:

- canonical metrics interval normalization
- default schema-owned `search` and `search.rollups` installation
- the `.mblk` metrics-block companion family
- bundled per-segment `PSCIX2` `.cix` search companions for metrics-serving
  state

## Control-Plane Metadata

Per stream, SQLite stores:

- stream lifecycle and offsets
- logical payload-byte size for management lookups such as `/_details`
- profile metadata
- schema registry
- desired bundled companion plan state and current per-segment companion object
  catalog
- plan-relative bundled companion ordinals resolved through the current desired
  plan generation
- local `async_index_actions` history for routing, lexicon, exact-secondary,
  and bundled-companion build/compaction work
- profile-owned processing progress and other rebuildable helper state

In full mode, manifest objects, segment objects, and schema objects in object
storage are the recovery source for published stream history and metadata.
SQLite also holds transient local state, including the
unuploaded WAL tail and runtime helper state, which is not fully mirrored to
object storage. Published logical stream size is restored from the manifest,
and if it is missing a background reconciliation pass can rebuild it from
published segments plus retained WAL. Profiles and schemas only shape how a
stream is interpreted. The `async_index_actions` table is part of this local
runtime-only state: it is informational, not published, and not restored by
`--bootstrap-from-r2`.

## Stream Deletion Enforcement

`DELETE /v1/stream/{name}` is enforced as a tombstone plus local acceleration
scrub:

- the stream row stays in SQLite with the deleted flag set
- the same local delete transaction removes all stream-owned acceleration state:
  - routing index state and runs
  - exact secondary index state and runs
  - routing-key lexicon state and runs
  - bundled search companion plans and per-segment companion rows
  - per-stream object-store request-accounting rows
  - local `async_index_actions` rows
- after the delete publishes its tombstone manifest, the server clears the
  per-stream request-accounting rows again so recreating the same stream name
  does not inherit historical PUT/GET counters
- the request path does not synchronously delete already-published remote
  segment, manifest, schema, or index objects

Startup re-enforces the same invariant before background loops start. On boot,
the server scans tombstoned streams and re-runs the acceleration scrub so older
builds, crashes, or manual SQLite edits cannot leave orphaned async-index state
or per-stream request-accounting rows behind for deleted streams.

## Data flow

### Append
1. HTTP handler validates request and enqueues into the append queue.
2. Writer loop drains a batch and starts a SQLite transaction.
3. For each stream in the batch:
   - ensure stream row exists
   - reserve offsets (advance next_offset)
   - insert WAL rows (payload, routing key, timestamps)
   - update pending_bytes/pending_rows
4. Commit transaction and resolve promises with assigned offsets.

### Segment build
1. Segmenter queries streams where pending_bytes/rows exceed thresholds or where
   last_segment_cut_ms exceeds the max interval.
2. For each candidate stream:
   - mark segment_in_progress
   - iterate WAL rows to determine [start_offset, end_offset)
   - stream rows again to write a sealed segment file
   - write footer/index; compute checksums
   - insert a row into segments and append segment metadata arrays
   - update sealed_through / pending_* counters
3. Clear segment_in_progress.

### Upload
1. Uploader selects the earliest `uploaded_at_ms IS NULL` segment for each
   stream.
   - upload order may still interleave across different streams
   - but one stream's published prefix is preserved: later segments do not jump
     ahead of an earlier missing segment
2. Upload segment bytes to object store using the TieredStore key layout.
   - timed-out object-store writes abort the underlying PUT attempt; the uploader does not intentionally leave a timed-out upload running in the background and then retry the same object key
3. Generate and upload a new manifest generation for that stream:
   - use the append‑only segment meta arrays
   - include **only the contiguous uploaded prefix**
4. Mark segment uploaded, advance uploaded_through, and delete WAL rows with
   offset <= uploaded_through in one transaction.

### Indexing locality and leases

All segment-backed indexing work now uses one shared segment-locality manager.

Current lease rules:

- any worker job that depends on segment files acquires a lease before dispatch
- leased segment-cache entries are marked **required for indexing**
- required-for-indexing entries are never evicted while leased
- leases are released in `finally` on success, failure, timeout, or worker
  exit

Current lease sizes:

- routing-key L0: one `DS_INDEX_L0_SPAN` uploaded-segment window (`16` by default)
- routing-key lexicon L0: one `DS_INDEX_L0_SPAN` uploaded-segment window
- exact secondary L0: one `DS_INDEX_L0_SPAN` uploaded-segment window
- bundled companion build: one uploaded segment at a time

This keeps segment-backed indexing off the main event loop without changing the
manifest/state machine:

- workers depend on local files, not remote range reads
- the main process still owns indexed watermarks, run or companion catalog
  inserts, and manifest publication
- higher-level compaction still works from immutable run objects, not source
  segments

The on-disk segment cache budget remains authoritative. Required-for-indexing
leases do not allow the cache to grow past `DS_SEGMENT_CACHE_MAX_BYTES`. If the
next required indexing window cannot be retained locally, append admission may
return `429` with `error.code = "index_building_behind"` until indexing catches
up enough to release older required windows. Background indexing continues in
that state so the node can recover automatically.

### Read
- For offsets < uploaded_through: read from segments via a full-object local cache.
  - on first touch of a remote segment, the server downloads the entire segment object, stores it under `DS_ROOT/cache/`, and serves the read from that local file
  - later reads for the same segment are served from the local cached file, and hot cached segment files are read through `Bun.mmap()`; if mmap is unavailable, the reader falls back to a single full-file byte buffer, not repeated slice-by-slice file opens
  - keyed reads do a single forward pass over cached block headers and matching blocks; they do not issue remote range reads or repeatedly reopen local cached files for tiny slices
  - unkeyed offset reads use the segment footer's block index to jump directly to the first relevant block instead of decoding forward from block 0
  - when the routing index has a candidate set, keyed reads plan the sealed segment scan up front and visit only candidate indexed segments plus the uncovered uploaded tail
  - `since + key` cursor seeking uses the same routing-candidate plan, so it does not walk the full indexed sealed prefix segment-by-segment
- For offsets >= uploaded_through: read from SQLite WAL tail.
- Merge results in order, honor limit, key filter, and format.
- For unversioned JSON streams, `format=json` responses reuse stored payload
  bytes directly and concatenate them into the response array body. The handler
  does not decode and re-encode each record on the steady-state path.
- Supports catch‑up reads, long‑poll, and SSE.

## SQLite usage and invariants

SQLite is the immediate source of truth for local operation:
- WAL rows (append-only)
- Stream progress (next_offset, sealed_through, uploaded_through)
- Repeated literal SQL is prepared once per connection and then reused through
  the sqlite adapter's statement cache. This is the default path for
  `get`/`all`/`run` calls in the server.
- Iterator-style WAL scans and WAL GC `DELETE ... RETURNING` sweeps still
  prepare a fresh statement per call and finalize it immediately after use.
  That is intentional: Bun's sqlite iterator path is safe with fresh
  statements but not with a shared cached iterator statement.
- SQLite runtime policy is strict:
  - runtime reads and writes use prepared statements only
  - `db.exec(...)` is reserved for one-shot schema/bootstrap work, not request
    handlers, background loops, or repeated DML/SELECT paths
  - any statement that is not intentionally retained in the adapter's bounded
    per-connection cache must be finalized as soon as the caller is done with
    it
  - Bun's [`Statement`](https://bun.com/reference/bun/sqlite/Statement)
    reference matters here: fresh statements own native `sqlite3_stmt`
    resources until `finalize()` or Bun's disposal path runs
- Prepared-statement count is an operational guardrail, not a vanity metric:
  - a well-behaved app or helper process should usually stay at about a dozen
    live prepared statements or fewer
  - materially higher counts must be deliberate, bounded, and justified by one
    documented cache rather than accidental dynamic-SQL churn or unfinalized
    iterator statements
- Segment metadata (local files and upload state)
- Manifest generation state

In full mode, bootstrap from object storage reconstructs the published durable
state from:
- manifest objects
- segment objects
- schema objects
- published routing-key and secondary-index run objects

SQLite state that is intentionally local-only or transient includes:
- WAL rows above `uploaded_through`
- producer dedupe/gap-detection state
- runtime live/template state
- rebuildable helper state that is reseeded on restart

Key invariants:
- uploaded_through <= sealed_through <= next_offset
- WAL offsets are unique and strictly increasing per stream
- uploaded_through advances only after manifest upload succeeds
- WAL GC is only performed for offsets < uploaded_through

## Object layout and keys

- Stream hash: first 16 bytes of SHA-256, hex-encoded (32 chars)
- Segment object key: streams/<hash>/segments/<segment_index>.bin (16‑digit zero‑padded)
- Bundled companion object key: streams/<hash>/segments/<segment_index>-<id>.cix
- Manifest object key: streams/<hash>/manifest.json

Local disk layout (default):
- `DS_ROOT/wal.sqlite` (SQLite WAL + metadata)
- `DS_ROOT/local/streams/<hash>/segments/<segment_index>.bin` (sealed segments)
- `DS_ROOT/cache/` (downloaded segment cache, bounded by size)

## Crash safety and recovery

- Appends are durable after SQLite commit.
- Segment builds are atomic: temp files are renamed only after footer/index is
  fully written. Temp files are cleaned on startup.
- Upload is idempotent: segment bytes can be uploaded multiple times, but data
  becomes visible only after manifest upload succeeds.
- After restart:
  - resume pending segment uploads
  - resume segmenter from streams with pending_bytes
  - never scan all streams; use indexed queries

## Future Durability Modes

Not implemented today:

- object-store-acked durability: batch writes and acknowledge only after they
  are durably persisted to object storage
- cluster quorum durability: acknowledge writes only after a durability quorum
  in a cluster has accepted them

The current full-mode server does neither. Its ACK point is local SQLite
commit, and its object-store durability point is manifest publication.

## Bounded memory and backpressure

All work queues are bounded:
- append queue
- segment build queue
- upload queue
- inflight uploads semaphore

Request-path work is also bounded:
- ingest/create requests use a dedicated concurrency gate
- read requests use a dedicated concurrency gate
- search / aggregate requests use a dedicated concurrency gate

Background indexing is bounded by a shared async-index gate across routing,
exact, and bundled-companion work.

Memory pressure is no longer a direct reject path. Instead, it is sampled and
can reduce search and async-index concurrency, never below `1`.

Overload behavior is still explicit (429/503) rather than unbounded buffering,
but `429` now reflects queue/backlog pressure or index-locality pressure, not a
separate memory gate.

Caches (segment data cache, schema/lens caches, companion caches) are
size-limited and only cover active streams.

## Observability

- Interval metrics are appended to the `__stream_metrics__` stream using the
  built-in `metrics` profile.
- The internal `__stream_metrics__` stream intentionally installs only the
  canonical schema, not the full metrics search/rollup registry, so the node
  does not create `.agg`/`.mblk`/`.fts`/`.col` self-indexing work while
  emitting operational telemetry.
- Optional `--stats` log line provides ingest/stored/uploaded throughput plus WAL/meta sizes and backpressure.
