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
- Uploads segments first, then publishes a new manifest generation.
- Advances uploaded_through only after manifest upload succeeds, then GC WAL rows.

5) Index managers
- The full server starts three in-process indexing managers:
  - routing-key
  - exact secondary
  - bundled companions (`.col`, `.fts`, `.agg`, `.mblk`) via `SearchCompanionManager`
- They run on a timer (`DS_INDEX_CHECK_MS`) inside the main server process.
- They are asynchronous background loops, not dedicated worker threads or
  separate processes.
- `DS_INDEX_BUILD_CONCURRENCY` controls parallel segment-processing tasks
  inside one exact-family run build.
- `DS_INDEX_COMPACT_CONCURRENCY` controls parallel run-loading tasks inside
  one exact-family compaction job.

6) Reader
- Merges historical data from segments (local cache or R2) with tail data in SQLite.
- Supports key-filtered reads and long-poll semantics.

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
- profile-owned processing progress and other rebuildable helper state

In full mode, manifest objects, segment objects, and schema objects in object
storage are the recovery source for published stream history and metadata.
SQLite also holds transient local state, including the
unuploaded WAL tail and runtime helper state, which is not fully mirrored to
object storage. Published logical stream size is restored from the manifest,
and if it is missing a background reconciliation pass can rebuild it from
published segments plus retained WAL. Profiles and schemas only shape how a
stream is interpreted.

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
1. Uploader selects segments with uploaded_at_ms IS NULL.
2. Upload segment bytes to object store using the TieredStore key layout.
3. Generate and upload a new manifest generation for that stream:
   - use the append‑only segment meta arrays
   - include **only the contiguous uploaded prefix**
4. Mark segment uploaded, advance uploaded_through, and delete WAL rows with
   offset <= uploaded_through in one transaction.

### Read
- For offsets < uploaded_through: read from segments (local cache or range reads).
- For offsets >= uploaded_through: read from SQLite WAL tail.
- Merge results in order, honor limit, key filter, and format.
- Supports catch‑up reads, long‑poll, and SSE.

## SQLite usage and invariants

SQLite is the immediate source of truth for local operation:
- WAL rows (append-only)
- Stream progress (next_offset, sealed_through, uploaded_through)
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

Overload behavior is explicit (429/503) rather than unbounded buffering.
Caches (segment data cache, schema/lens caches) are size-limited and only cover
active streams.

## Observability

- Interval metrics are appended to the `__stream_metrics__` stream using the
  built-in `metrics` profile.
- Optional `--stats` log line provides ingest/stored/uploaded throughput plus WAL/meta sizes and backpressure.
