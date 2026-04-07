# Prisma Streams SQLite Schema

This implementation uses **SQLite as the durable WAL** and as the local
metadata store.
The goal is to:
- minimize custom file formats
- keep memory bounded under load
- simplify crash recovery (SQLite transactions)

This document specifies the intended schema and the invariants it must uphold.

---

## 1) PRAGMAs (recommended defaults)

Set these at startup (configurable):
- `PRAGMA journal_mode = WAL;`
- `PRAGMA synchronous = FULL;` (safe default; allow `NORMAL` for benchmarks)
- `PRAGMA foreign_keys = ON;`
- `PRAGMA temp_store = MEMORY;` (optional; benchmark)
- `PRAGMA busy_timeout = 5000;` (avoid immediate SQLITE_BUSY)

Bound memory usage:
- `PRAGMA cache_size = -NNNN;` (negative means KB; choose based on RAM budget)
- consider `PRAGMA mmap_size = ...;` only if you understand the memory tradeoff

---

## 2) Tables

### 2.1 `schema_version`
Tracks migrations.

Columns:
- `version INTEGER NOT NULL`

Invariant:
- exactly one row

---

### 2.2 `streams`
One row per stream (up to 1,000,000 streams).

Columns (suggested):
- `stream TEXT PRIMARY KEY`
- `created_at_ms INTEGER NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Offsets / progress:
- `next_offset INTEGER NOT NULL`  
  Next offset to assign on append. Must be monotonic.
- `sealed_through INTEGER NOT NULL`  
  Highest offset included in locally built segments (may not be uploaded yet).
- `uploaded_through INTEGER NOT NULL`  
  Highest offset made visible/durable in R2 via manifest upload.
- `uploaded_segment_count INTEGER NOT NULL`  
  Count of **contiguous uploaded segments** (prefix), used to build manifests without scanning all segments.

WAL backlog counters (to avoid expensive `SUM(length(payload))` scans):
- `pending_rows INTEGER NOT NULL`
- `pending_bytes INTEGER NOT NULL`

Segmenting hints:
- `last_segment_cut_ms INTEGER NOT NULL`
- `segment_in_progress INTEGER NOT NULL` (0/1)

Additional columns present in the current implementation:
- Stream protocol/config state:
  - `content_type`
  - `profile`
  - `stream_seq`
  - `closed`
  - `closed_producer_id`
  - `closed_producer_epoch`
  - `closed_producer_seq`
  - `ttl_seconds`
- Retention/flags:
  - `expires_at_ms`
  - `stream_flags`
- WAL accounting:
  - `logical_size_bytes`
  - `wal_rows`
  - `wal_bytes`

Indexes:
- `CREATE INDEX streams_pending_bytes_idx ON streams(pending_bytes);`
- `CREATE INDEX streams_last_cut_idx ON streams(last_segment_cut_ms);`
- Optional composite index for candidate selection:
  - `(segment_in_progress, pending_bytes, last_segment_cut_ms)`

Invariants:
- `0 <= uploaded_through <= sealed_through <= next_offset`
- `0 <= uploaded_segment_count <= segment_count` (see `stream_segment_meta`)
- `profile IS NULL` means the stream has no explicit declaration and is treated
  as a `generic` stream
- `pending_bytes` and `pending_rows` reflect WAL rows with `offset >= sealed_through` (or `>= uploaded_through`, depending on design); pick one and enforce consistently.
- `logical_size_bytes` is the logical payload-byte size exposed by `/_details`;
  it is updated on append, restored from manifests for published history, and
  can be repaired asynchronously after bootstrap if missing.
- `segment_in_progress` must be 0/1.

---

### 2.3 `wal`
The durable write-ahead log is a single table.

Columns (suggested):
- `id INTEGER PRIMARY KEY` (rowid; insertion order)
- `stream TEXT NOT NULL`
- `offset INTEGER NOT NULL`
- `ts_ms INTEGER NOT NULL` (ingest time)
- `payload BLOB NOT NULL`
- `payload_len INTEGER NOT NULL` (denormalization for fast sums)

Optional columns (only if needed by protocol/indexing):
- `routing_key BLOB NULL`
- `content_type TEXT NULL`
- `flags INTEGER NOT NULL DEFAULT 0`

Indexes:
- `CREATE UNIQUE INDEX wal_stream_offset_uniq ON wal(stream, offset);`
- Optional for time-based ops:
  - `CREATE INDEX wal_ts_idx ON wal(ts_ms);`

Invariants:
- for each stream, `offset` is unique and strictly increasing by protocol rules.
- rows exist for offsets in `[uploaded_through, next_offset)` unless GC has occurred.

Notes:
- Do not rely on SQLite `rowid` as the protocol offset. Store the protocol offset explicitly.

---

### 2.4 `segments`
Tracks locally built segments and upload state.

Columns:
- `segment_id TEXT PRIMARY KEY` (stable identifier; matches object key naming rules)
- `stream TEXT NOT NULL`
- `start_offset INTEGER NOT NULL`
- `end_offset INTEGER NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `local_path TEXT NOT NULL`
- `created_at_ms INTEGER NOT NULL`
- `uploaded_at_ms INTEGER NULL`
- `r2_etag TEXT NULL`

Indexes:
- `CREATE INDEX segments_stream_start_idx ON segments(stream, start_offset);`
- `CREATE INDEX segments_pending_upload_idx ON segments(uploaded_at_ms);`

Invariants:
- `start_offset < end_offset`
- segments for a stream must not overlap
- a segment may exist locally without being uploaded; visibility is governed by manifest.

---

### 2.5 `stream_segment_meta`
Compact, append‑only per‑segment arrays used to build manifests without scanning the
entire `segments` table. **Derived state**; can be rebuilt from `segments`.

Columns:
- `stream TEXT PRIMARY KEY`
- `segment_count INTEGER NOT NULL`
- `segment_offsets BLOB NOT NULL` (u64le end_offset+1 array; length = 8*segment_count)
- `segment_blocks BLOB NOT NULL` (u32le block_count array; length = 4*segment_count)
- `segment_last_ts BLOB NOT NULL` (u64le append_ns array; length = 8*segment_count)

Invariants:
- arrays are append‑only (no rewrites on seal)
- lengths match `segment_count`

---

### 2.6 `manifests`
Tracks current manifest generation per stream and upload state.

Columns:
- `stream TEXT PRIMARY KEY`
- `generation INTEGER NOT NULL`
- `uploaded_generation INTEGER NOT NULL`
- `last_uploaded_at_ms INTEGER NULL`
- `last_uploaded_etag TEXT NULL`
- `last_uploaded_size_bytes INTEGER NULL`

Invariants:
- `uploaded_generation <= generation`
- manifest upload is the “commit point” that advances `uploaded_through`

---

### 2.7 `index_state`
Local cache of per‑stream index state. **Rebuildable from manifest**.

Columns:
- `stream TEXT PRIMARY KEY`
- `index_secret BLOB NOT NULL` (16 bytes; SipHash key)
- `indexed_through INTEGER NOT NULL` (highest segment index **exclusive**)
- `updated_at_ms INTEGER NOT NULL`

Invariants:
- `indexed_through <= segment_count`

---

### 2.8 `index_runs`
Local catalog of active index runs. **Rebuildable from manifest**.

Columns:
- `run_id TEXT PRIMARY KEY`
- `stream TEXT NOT NULL`
- `level INTEGER NOT NULL`
- `start_segment INTEGER NOT NULL`
- `end_segment INTEGER NOT NULL`
- `object_key TEXT NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `filter_len INTEGER NOT NULL`
- `record_count INTEGER NOT NULL`
- `retired_gen INTEGER NULL`
- `retired_at_ms INTEGER NULL`

Indexes:
- `CREATE INDEX index_runs_stream_idx ON index_runs(stream, level, start_segment);`

---

### 2.9 `secondary_index_state`
Local cache of the internal exact-match secondary index family.
**Rebuildable from manifest**.

Columns:
- `stream TEXT NOT NULL`
- `index_name TEXT NOT NULL`
- `index_secret BLOB NOT NULL`
- `indexed_through INTEGER NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Primary key:
- `(stream, index_name)`

---

### 2.10 `secondary_index_runs`
Local catalog of the internal exact-match secondary index family runs.
**Rebuildable from manifest**.

Columns:
- `run_id TEXT PRIMARY KEY`
- `stream TEXT NOT NULL`
- `index_name TEXT NOT NULL`
- `level INTEGER NOT NULL`
- `start_segment INTEGER NOT NULL`
- `end_segment INTEGER NOT NULL`
- `object_key TEXT NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `filter_len INTEGER NOT NULL`
- `record_count INTEGER NOT NULL`
- `retired_gen INTEGER NULL`
- `retired_at_ms INTEGER NULL`

Indexes:
- `CREATE INDEX secondary_index_runs_stream_idx ON secondary_index_runs(stream, index_name, level, start_segment);`

---

### 2.11 `schemas`
Current implementation table (see `src/db/schema.ts`):

- `stream TEXT PRIMARY KEY`
- `schema_json TEXT NOT NULL`
- `updated_at_ms INTEGER NOT NULL`
- `uploaded_size_bytes INTEGER NOT NULL`

`schema_json` stores the serialized per-stream schema registry JSON (schema versions,
lenses, routingKey config, and schema-owned `search` declarations).

---

### 2.12 `search_companion_plans`
Per-stream desired bundled companion plan. **Rebuildable from manifest**.

Columns:
- `stream TEXT PRIMARY KEY`
- `generation INTEGER NOT NULL`
- `plan_hash TEXT NOT NULL`
- `plan_json TEXT NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Notes:
- this is the durable local record of which bundled companion generation the
  stream currently wants
- `plan_json` stores the enabled families plus plan-relative field, rollup,
  interval, and measure ordinals used by the current `PSCIX2` companion format
- schema or profile changes that affect bundled sections increment the desired
  generation

---

### 2.13 `search_segment_companions`
Local catalog of current uploaded bundled `.cix` companion objects.
**Rebuildable from manifest**.

Columns:
- `stream TEXT NOT NULL`
- `segment_index INTEGER NOT NULL`
- `object_key TEXT NOT NULL`
- `plan_generation INTEGER NOT NULL`
- `sections_json TEXT NOT NULL`
- `section_sizes_json TEXT NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `primary_timestamp_min_ms INTEGER NULL`
- `primary_timestamp_max_ms INTEGER NULL`
- `updated_at_ms INTEGER NOT NULL`

Primary key:
- `(stream, segment_index)`

Indexes:
- `CREATE INDEX search_segment_companions_stream_plan_idx ON search_segment_companions(stream, plan_generation, segment_index);`

Notes:
- this is a local object catalog, not a row-level search projection
- each row points at one immutable bundled `PSCIX2` companion object
- `sections_json` records which bundled sections are present, such as `col`,
  `fts`, `agg`, and `mblk`
- `section_sizes_json` records the byte size of each binary bundled section
  payload that is present
- `primary_timestamp_min_ms` / `primary_timestamp_max_ms` store the bundled
  companion's covered bounds for the stream's primary timestamp field when that
  field is available; aggregate queries use these local values to skip
  non-overlapping published segments without fetching any companion object
- companions are published under `streams/<hash>/segments/...cix`

---

### 2.14 `objectstore_request_counts`
Node-local per-stream object-store request accounting used by `/_details`.

Columns:
- `stream_hash TEXT NOT NULL`
- `artifact TEXT NOT NULL`
- `op TEXT NOT NULL`
- `count INTEGER NOT NULL`
- `bytes INTEGER NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Primary key:
- `(stream_hash, artifact, op)`

Indexes:
- `CREATE INDEX objectstore_request_counts_stream_idx ON objectstore_request_counts(stream_hash, updated_at_ms);`

Notes:
- this table is local operational accounting, not durable published stream
  state
- counters are node-local and reflect requests observed through the current
  object-store wrapper
- request counts are exposed through `GET /v1/stream/{name}/_details`

---

### 2.15 `async_index_actions`
Node-local log of async index actions.

Columns:
- `seq INTEGER PRIMARY KEY AUTOINCREMENT`
- `stream TEXT NOT NULL`
- `action_kind TEXT NOT NULL`
- `target_kind TEXT NULL`
- `target_name TEXT NULL`
- `input_kind TEXT NOT NULL`
- `input_count INTEGER NOT NULL`
- `input_size_bytes INTEGER NOT NULL`
- `output_count INTEGER NOT NULL`
- `output_size_bytes INTEGER NOT NULL`
- `start_segment INTEGER NULL`
- `end_segment INTEGER NULL`
- `begin_time_ms INTEGER NOT NULL`
- `end_time_ms INTEGER NULL`
- `duration_ms INTEGER NULL`
- `status TEXT NOT NULL`
- `error_message TEXT NULL`
- `detail_json TEXT NOT NULL`

Indexes:
- `CREATE INDEX async_index_actions_stream_seq_idx ON async_index_actions(stream, seq DESC);`
- `CREATE INDEX async_index_actions_kind_seq_idx ON async_index_actions(action_kind, seq DESC);`

Notes:
- every routing, lexicon, exact-secondary, and bundled-companion async build or
  compaction action records one row here
- this is local observability only; it is not published to object storage and is
  not restored by `--bootstrap-from-r2`
- stream delete clears these rows so recreating the same stream name starts with
  a fresh local history
- the intended operating target is that each logged async index action should
  complete in under `1000 ms`

---

### 2.16 `stream_profiles`
Stores non-generic profile configuration.

Columns:
- `stream TEXT PRIMARY KEY`
- `profile_json TEXT NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Notes:
- `streams.profile` stores the profile kind for cheap listing/filtering
- `stream_profiles.profile_json` stores the full JSON config for profiles such
  as `evlog` and `state-protocol`
- missing stored profile metadata means the stream is treated as `generic`

---

### 2.17 `stream_touch_state`
Rebuildable helper state for touch-enabled `state-protocol` streams.

Columns:
- `stream TEXT PRIMARY KEY`
- `processed_through INTEGER NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Notes:
- tracks how far the background state-protocol touch worker has processed the
  base stream
- rows are created only for touch-enabled `state-protocol` streams
- the table is rebuildable from stream metadata plus the stream contents
- it is not mirrored to object storage as exact state; bootstrap/restart reseeds
  it locally

---

### 2.18 `producer_state`
Local idempotence and gap-detection state for producer-aware appends.

Columns:
- `stream TEXT NOT NULL`
- `producer_id TEXT NOT NULL`
- `epoch INTEGER NOT NULL`
- `last_seq INTEGER NOT NULL`
- `updated_at_ms INTEGER NOT NULL`

Notes:
- used for `Producer-Id` / `Producer-Epoch` / `Producer-Seq` admission checks
- local-only SQLite state; not mirrored to object storage
- reset by `--bootstrap-from-r2`

---

### 2.19 `live_templates`
Runtime template registry for touch-enabled `state-protocol` streams.

Columns:
- `stream TEXT NOT NULL`
- `template_id TEXT NOT NULL`
- `entity TEXT NOT NULL`
- `fields_json TEXT NOT NULL`
- `encodings_json TEXT NOT NULL`
- `state TEXT NOT NULL`
- `created_at_ms INTEGER NOT NULL`
- `last_seen_at_ms INTEGER NOT NULL`
- `inactivity_ttl_ms INTEGER NOT NULL`
- `active_from_source_offset INTEGER NOT NULL`
- `retired_at_ms INTEGER NULL`
- `retired_reason TEXT NULL`

Notes:
- runtime helper state for fine-grained live invalidation
- not part of the stream's durable published history
- not mirrored to object storage
- rebuilt or relearned by runtime traffic after restart/bootstrap

---

## 3) Garbage collection and compaction

### WAL GC rule (safe baseline)
You may delete WAL rows for a stream with `offset < uploaded_through` **only after**:
1) the corresponding segments are uploaded, AND
2) the manifest generation that references them is uploaded successfully.

Implementation pattern:
- in one SQLite transaction:
  - mark segment uploaded
  - update manifest state (uploaded_generation, last etag)
  - advance `uploaded_through`
  - `DELETE FROM wal WHERE stream=? AND offset < ?;`

### Vacuum
- use `PRAGMA wal_checkpoint(TRUNCATE)` periodically (configurable)
- avoid aggressive `VACUUM` on large DBs in the hot path

---

## 4) Candidate selection for segmenting (no full scans)

Never do:
- `SELECT stream FROM wal GROUP BY stream HAVING SUM(payload_len) > ...;` (too expensive)

Do instead:
- maintain `streams.pending_bytes` / `pending_rows` counters at append time
- query `streams` table for candidates by indexed columns

---

## 5) Transactions

### Append transaction (group commit)
Within one transaction:
- create stream row if missing
- reserve offsets (advance next_offset)
- insert WAL rows
- update pending counters
- commit

### Segment finalize transaction
After building segment file:
- insert segment row
- advance sealed_through (or other marker)
- decrement pending counters
- clear segment_in_progress
- commit

### Upload finalize transaction
After upload success:
- mark segment uploaded
- update manifest state
- advance uploaded_through
- delete WAL rows below uploaded_through
- commit

---

## 6) Testing the schema invariants

Add a test module that:
- seeds random operations (append, segment, upload, crash simulation)
- checks invariants after each step
- ensures recovery logic restores invariant satisfaction
