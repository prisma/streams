# Durable Streams Bun+TypeScript Rewrite Architecture

This document describes the architecture of the Bun+TypeScript rewrite of Durable Streams using:
- SQLite (bun:sqlite) as the durable WAL and metadata store
- TieredStore-style segments and manifests
- An R2-compatible object store (MockR2 for tests)

The design prioritizes correctness, bounded memory, and crash safety.

## High-level components

1) HTTP layer (Bun server)
- Parses requests, enforces protocol semantics, and enqueues work.
- Performs only small indexed SQLite reads in the request path.
- Implements long-poll reads without busy loops.

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

5) Reader
- Merges historical data from segments (local cache or R2) with tail data in SQLite.
- Supports key-filtered reads and long-poll semantics.

6) Object store
- ObjectStore interface with put/get/head/list plus streaming uploads.
- MockR2 implements the interface with deterministic fault injection.

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

SQLite is the source of truth for:
- WAL rows (append-only)
- Stream progress (next_offset, sealed_through, uploaded_through)
- Segment metadata (local files and upload state)
- Manifest generation state

Key invariants:
- uploaded_through <= sealed_through <= next_offset
- WAL offsets are unique and strictly increasing per stream
- uploaded_through advances only after manifest upload succeeds
- WAL GC is only performed for offsets < uploaded_through

## Object layout and keys

- Stream hash: first 16 bytes of SHA-256, hex-encoded (32 chars)
- Segment object key: streams/<hash>/segments/<segment_index>.bin (16‑digit zero‑padded)
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

- Interval metrics are appended to the `__stream_metrics__` stream.
- Optional `--stats` log line provides ingest/stored/uploaded throughput plus WAL/meta sizes and backpressure.
