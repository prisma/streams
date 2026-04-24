# Prisma Streams Durable Streams HTTP Protocol Reference

This document is a **normative** description of the HTTP behavior Prisma
Streams must implement.

It is written so an engineer can implement the server **without access to the
original Go source code**.

Primary inputs in this repository:
- `overview.md`
- `architecture.md`
- `sqlite-schema.md`
- `schemas.md`

If any of those documents disagree, this spec is the tie-breaker for this
implementation.

---

## 1. Terminology

- **Stream**: an append-only ordered log addressed by a URL path segment (the “stream name”).
- **Entry**: one appended item. Every entry has:
  - `offset` (opaque, monotonic)
  - `append_time` (server-defined, monotonic per stream)
  - optional `key` (routing key / primary key)
  - `data` (opaque bytes)
- **Offset**: an opaque string checkpoint used for resumable reads.
  - The only special offset is `-1` meaning “before the first entry”.
- **Routing key**: a per-entry key used for key-filtered reads.

---

## 2. HTTP resources

### 2.1 Stream resource

- `PUT  /v1/stream/{name}` create stream
- `POST /v1/stream/{name}` append
- `GET  /v1/stream/{name}` read
- `HEAD /v1/stream/{name}` metadata
- `DELETE /v1/stream/{name}` delete

### 2.2 Schema subresource

- `GET  /v1/stream/{name}/_schema` get schema registry
- `POST /v1/stream/{name}/_schema` update schema registry

### 2.3 Profile subresource

- `GET  /v1/stream/{name}/_profile` get stream profile metadata
- `POST /v1/stream/{name}/_profile` update stream profile

### 2.4 Search and inspection subresources

- `GET  /v1/stream/{name}/_search?q=...` search
- `POST /v1/stream/{name}/_search` search
- `POST /v1/stream/{name}/_aggregate` aggregate
- `GET  /v1/stream/{name}/_routing_keys` list routing keys alphabetically
- `GET  /v1/stream/{name}/_index_status` get per-stream index status
- `GET  /v1/stream/{name}/_details` get combined stream details

### 2.5 Streams collection

- `GET /v1/streams` list streams

### 2.6 Server inspection

- `GET /v1/server/_details` get server-scoped configured limits and live runtime state

System streams (reserved names):
- `__stream_metrics__` (metrics; see `metrics.md`)
  - uses the `metrics` profile for canonical normalization
  - intentionally installs a lean internal schema registry with no `routingKey`
    and no `search` config
  - therefore does not build routing, lexicon, exact, or bundled companion
    families for the internal system stream
- `__stream_stats__` (segment stats; see `internal/STREAM_STATS.md`) (proposal
  only; not implemented in the current Bun + TypeScript server)
- `__registry__` (stream lifecycle log; recommended to make listing cheap)

---

## 3. Headers

### 3.1 Append request headers

- `Stream-Key: <string>`
  - Optional routing key for the appended entry (byte mode) or for each entry (JSON mode when allowed).
  - If the stream has a configured schema routing key extraction (see `schemas.md`), then **JSON appends must NOT include `Stream-Key`**.

- `Stream-Timestamp: <rfc3339 | rfc3339nano | unix_nanos>`
  - Optional append-time hint.
  - The server clamps timestamps so append time is monotonic per stream.

- `Stream-Seq: <string>`
  - Optional write coordination.
  - If provided, the server enforces monotonic increase (lexicographic compare).

- `Content-Type: application/json`
  - If set, the body must be a JSON array and the server appends one entry per array element.
  - Otherwise, the body is treated as opaque bytes and appended as a single entry.

### 3.2 Create request headers

- `Stream-TTL: <duration>` (example: `24h`, `30m`, `15s`)
- `Stream-Expires-At: <rfc3339 | rfc3339nano>`

Rules:
- At most one of `Stream-TTL` and `Stream-Expires-At` may be provided.
- If provided, the stream becomes unavailable for reads/appends after expiry.

### 3.3 Read request headers

None required.

### 3.4 Response headers

All successful responses should include:

- `Stream-Next-Offset: <offset>`
  - The checkpoint the client should pass as `offset=` in the next read.
  - For reads that return no new data, it should equal the request’s `offset` (or the canonicalized equivalent).

Reads should also include:

- `Stream-End-Offset: <offset>`
  - A checkpoint representing the current end of stream (at response time). Useful for UIs and diagnostics.

Caching headers:

- For non-live reads with bounded responses, return
  - `ETag: W/"slice:<start>:<next>:key=<keyOrEmpty>:fmt=<fmt>:filter=<filterOrEmpty>"`
  - `Cache-Control: immutable, max-age=31536000`

- For live reads (`live=true` or `live=long-poll`), return
  - `Cache-Control: no-store`

If a filtered read hits the scan cap, it must also include:

- `Stream-Filter-Scan-Limit-Reached: true`
- `Stream-Filter-Scan-Limit-Bytes: 104857600`
- `Stream-Filter-Scanned-Bytes: <bytes examined>`

---

## 4. Query parameters

### 4.1 Read parameters

- `offset=<opaque>` (required for normal reads)
  - `-1` means start.

- `since=<timestamp>` (optional)
  - Seek by append time (RFC3339/RFC3339Nano or unix nanos).
  - If both `offset` and `since` are present, **offset wins**.

- `format=json` (optional)
  - If set, server returns a JSON array of messages.
  - If absent, server returns raw concatenated bytes (byte mode).

- `live=true` OR `live=long-poll` (optional)
  - If set and there is no data available after `offset`, the server waits until either:
    - new data becomes available, or
    - timeout expires.

- `timeout=<duration>` (optional)
  - Only meaningful with `live`.
  - Default: 30s.

- `key=<string>` (optional)
  - Routing-key filtered read.

- `filter=<expr>` (optional)
  - Predicate filter for JSON streams.
  - Only schema `search.fields` may appear in the filter.
  - Supported clause forms in the current implementation:
    - exact match: `field:value`
    - comparisons: `field:>=value`, `field:>value`, `field:<=value`, `field:<value`
    - exists: `has:field`
    - boolean composition with `AND`, `OR`, `NOT`, `-`, and `(...)`
  - Exact-equality clauses may use the internal exact family to prune sealed segments.
  - Typed equality/range clauses may use `.col` companions to prune segment-local docs.
  - Remaining verification happens against the source stream.

Path form (equivalent to `key=`):
- `GET /v1/stream/{name}/pk/{url-escaped-key}?offset=...`

### 4.2 List streams

- `GET /v1/streams`
  - Returns a JSON array of stream descriptors.
  - Must be efficient up to ~1,000,000 streams.
  - Each descriptor should expose the stream profile. The current
    implementation returns a single `profile` field in the list response.

### 4.3 Aggregate parameters

`POST /v1/stream/{name}/_aggregate` uses a JSON request body.

Required fields:

- `rollup`
- `from`
- `to`
- `interval`

Optional fields:

- `q`
- `group_by`
- `measures`

---

## 5. Offsets

### 5.1 Semantics

- Offsets are **opaque strings**.
- The only special offset is `-1` meaning “start of stream”.
- A read with `offset=X` returns entries with offsets strictly greater than X.
- The server returns `Stream-Next-Offset` which the client uses for the next read.

### 5.2 Canonical encoding used by this implementation

To be cache-friendly and sortable, offsets are encoded as Crockford base32 of a 128-bit tuple:

- `epoch` (u32)
- `hi` (u32)
- `lo` (u32)
- `in_block` (u32)

Canonical representation:
- 26 characters of Crockford base32 (case-insensitive on input; server outputs uppercase).
- Left-pad with zero bits so the string is always 26 chars.
- `-1` is accepted as input shorthand for start-of-stream; response headers are canonical 26-char offsets.

Interpretation used by this implementation:
- `epoch` fences offsets across resets/migrations.
- `hi|lo` together store the 64-bit **logical entry offset** within the epoch.
- `in_block` is reserved for future sub-entry slicing; this implementation sets
  it to `0` for all returned offsets.

This keeps the storage engine’s internal offsets simple (a u64 sequence per epoch) while matching the “128-bit opaque offset” protocol shape.

Implementation requirement:
- The server must accept both:
  - `-1`, and
  - 26-char base32 offsets.
- Decimal aliases like `0`, `1`, `2`, ... are rejected in this implementation.

---

## 6. Create stream (PUT)

### Server-side request timeouts

Current implementation rules:

- all HTTP resolvers use a cooperative server-side timeout target of `5000 ms`
- if that generic resolver timeout is reached first, the server returns
  `408 Request Timeout` with:

```json
{
  "error": {
    "code": "request_timeout",
    "message": "request timed out"
  }
}
```

- route-specific lower limits still apply inside that outer cap
- because timeout checks are cooperative rather than preemptive, observed wall
  time may overshoot the target slightly while an in-flight unit of work
  completes
- long-poll clients should keep requested waits at `<= 5s` and reconnect after
  a `408`

### Request

`PUT /v1/stream/{name}`

Headers:
- Optional TTL (`Stream-TTL`) or expiry (`Stream-Expires-At`).

### Response

- `201 Created` if created
- `200 OK` if already exists (idempotent)

Profile rule:

- If no profile has been declared for the stream, the server treats it as a
  `generic` stream.

---

## 7. Append (POST)

### 7.1 Byte mode append

`POST /v1/stream/{name}` with any content-type except `application/json`.

- Body is treated as opaque bytes.
- Exactly one entry is appended.
- Routing key is taken from `Stream-Key` if present.

### 7.2 JSON mode append

`POST /v1/stream/{name}` with `Content-Type: application/json`.

- Body must be a JSON array.
- Each element in the array is appended as one entry.
- If the stream has `routingKey` configured in its schema registry:
  - server extracts routing keys per entry using the JSON pointer
  - request must not include `Stream-Key`

### 7.3 Timestamps

- If `Stream-Timestamp` is provided, it is used as an append-time hint.
- The server clamps timestamps so they never go backwards per stream.
- If omitted, the server assigns append time.

### 7.4 `Stream-Seq`

If `Stream-Seq` is provided:
- The server treats it as an optimistic concurrency control value.
- The value is opaque and compared lexicographically.
- The server rejects the append if `Stream-Seq` is less than or equal to the stream's current value.
- Rejection should be `409 Conflict` with a helpful error body.

### 7.5 Response

- `200 OK`
- Must include `Stream-Next-Offset` (the offset of the last appended entry).
- `429 Too Many Requests` may be returned when the append queue or local
  backlog budget is full

Current implementation timeout behavior:

- append waits use a cooperative server-side timeout target of `3000 ms`
- this applies to `POST /v1/stream/{name}` and to `PUT /v1/stream/{name}` when
  it includes an initial append or close operation
- if the append wait reaches that budget first, the server returns
  `408 Request Timeout` with:

```json
{
  "error": {
    "code": "append_timeout",
    "message": "append timed out; append outcome is unknown, check Stream-Next-Offset before retrying"
  }
}
```

- append timeout is ambiguous from the client's perspective because the server
  may still complete the append after the timeout response is sent
- clients should read or `HEAD` the stream and inspect `Stream-Next-Offset`
  before retrying a timed-out append

## 7A. Profile resource

### Get profile

`GET /v1/stream/{name}/_profile`

Response:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": { "kind": "generic" }
}
```

Rules:

- `profile` is always present
- if no explicit profile was declared when the stream was created, the server
  returns `{ "kind": "generic" }`

### Update profile

`POST /v1/stream/{name}/_profile`

Request:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": { "kind": "generic" }
}
```

Rules:

- supported built-ins are `evlog`, `generic`, `metrics`, and `state-protocol`
- `evlog` requires an `application/json` stream content type
- `evlog` normalizes JSON appends into a canonical request-log envelope and
  derives a routing key from `requestId` or `traceId` when the schema does not
  own routing-key extraction
- installing `evlog` also installs the canonical evlog schema version `1` and
  default `search` registry for that stream
- `metrics` requires an `application/json` stream content type
- `metrics` normalizes JSON appends into the canonical metrics interval
  envelope and derives a routing key from `seriesKey` when the schema does not
  own routing-key extraction
- installing `metrics` also installs the canonical metrics schema version `1`,
  default `search` registry, and default rollups for that stream
- `state-protocol` requires an `application/json` stream content type
- `state-protocol.touch.enabled=true` enables the `/touch/*` routes
- set `profile` to `{ "kind": "generic" }` to use the baseline durable stream
  behavior

## 7B. Stream inspection resources

### Routing-key listing

`GET /v1/stream/{name}/_routing_keys`

Query parameters:

- `limit` optional positive integer, default `100`, maximum `500`
- `after` optional exclusive cursor; when present, only routing keys strictly
  greater than `after` are returned

Response fields:

- `stream`
- `source`
- `took_ms`
- `coverage`
- `timing`
- `keys`
- `next_after`

Rules:

- this endpoint is read-only
- it is supported only when the installed schema declares `routingKey`
- keys are returned in strict ascending lexicographic order
- keys are distinct
- `next_after` is `null` when the page is exhausted; otherwise the client uses
  the returned value as the next request’s `after`
- the server uses the routing-key lexicon run family for the indexed uploaded
  prefix
- once any lexicon coverage exists, uploaded sealed segments beyond the indexed
  prefix are not scanned in the request path
- the request path may scan at most one sealed segment from the uncovered local
  tail; the WAL tail is still scanned directly
- before the first `.lex` run exists, the request path may scan at most one
  uploaded sealed segment plus the local tail and WAL
- when uncovered uploaded history exists, the response is a best-effort
  alphabetical page over the indexed prefix plus the directly scanned local
  tail / WAL
- `coverage.complete=false` means uncovered uploaded history may still contain
  routing keys that sort before or between the returned keys
- `next_after` may still be non-null when `coverage.complete=false`; clients may
  continue paging, but they must treat cursors as best-effort while uploaded
  lexicon lag remains
- `coverage.indexed_segments` reports the uploaded prefix covered by lexicon
  runs
- `coverage.scanned_uploaded_segments`, `coverage.scanned_local_segments`, and
  `coverage.scanned_wal_rows` report the uncovered data that had to be scanned
  directly
- `coverage.possible_missing_uploaded_segments` and
  `coverage.possible_missing_local_segments` report uncovered sealed segments
  that were not scanned in-request
- `timing.lexicon_run_get_ms`, `timing.lexicon_decode_ms`, and
  `timing.lexicon_enumerate_ms` break down indexed-prefix serving time
- `timing.lexicon_merge_ms` breaks down the final indexed/fallback merge
- `timing.fallback_scan_ms`, `timing.fallback_segment_get_ms`, and
  `timing.fallback_wal_scan_ms` break down direct fallback work
- `timing.lexicon_runs_loaded` reports how many active `.lex` runs were loaded
  for this page
- the indexed side fetches only a page-sized candidate set from active runs; it
  does not expand the indexed candidate count based on fallback key volume

### Index status

`GET /v1/stream/{name}/_index_status`

Response fields:

- `stream`
- `profile`
- `segments`
- `manifest`
- `routing_key_index`
- `routing_key_lexicon`
- `exact_indexes`
- `bundled_companions`
- `search_families`

Rules:

- this endpoint is read-only
- it reports current per-stream segment and manifest state
- it reports async index/search-family progress for the current stream
- `routing_key_index` covers the routing-key tiered index
- `routing_key_lexicon` covers the alphabetical routing-key lexicon run family
- `exact_indexes` covers the internal exact-match secondary family derived from
  schema `search.fields`
- `exact_indexes[*].stale_configuration` is true when a configured exact field
  changed and the exact family has not rebuilt for that config yet
- `bundled_companions` reports current `.cix` coverage for the desired
  companion plan generation
- `search_families` covers bundled companion sections such as `col`, `fts`,
  `agg`, and `mblk`
- `manifest.last_uploaded_size_bytes` is the uploaded manifest object size as a
  string when known
- `routing_key_index`, `routing_key_lexicon`, each `exact_indexes[*]`,
  `bundled_companions`, and each
  `search_families[*]` report `bytes_at_rest`
- `routing_key_index`, `routing_key_lexicon`, each `exact_indexes[*]`, and each
  `search_families[*]` report `lag_segments` and `lag_ms`
- `search_families[*].contiguous_covered_segment_count` is the contiguous
  uploaded prefix covered by that bundled section

### Combined details

`GET /v1/stream/{name}/_details`

Response fields:

- `stream`
- `profile`
- `schema`
- `index_status`
- `storage`
- `object_store_requests`

Rules:

- this endpoint is read-only
- `stream` is the full stream summary object, not a reduced descriptor
- `profile` matches `GET /_profile`
- `schema` matches `GET /_schema`
- `index_status` matches `GET /_index_status`
- `stream` includes the head/lifecycle fields needed by an active stream page,
  including `created_at`, `expires_at`, `epoch`, `next_offset`,
  `sealed_through`, and `uploaded_through`
- `stream.total_size_bytes` is the logical payload-byte size of the stream on
  this node, returned as a string
- `storage.object_storage` reports current uploaded bytes and object counts for:
  segments, indexes, and manifest/schema metadata
- `storage.local_storage` reports current local retained bytes for:
  WAL, pending sealed segments, caches, and the shared SQLite footprint
  - `segment_cache_bytes`, `routing_index_cache_bytes`,
    `exact_index_cache_bytes`, `lexicon_index_cache_bytes`, and
    `companion_cache_bytes` are local on-disk cache occupancy, not process heap
  - segment and index caches are seeded on first read of a remote object; they
    may increase even when the request was initiated by a read-only UI flow
- `storage.companion_families` splits bundled companion bytes by section family
  (`col`, `fts`, `agg`, `mblk`)
- `object_store_requests` reports node-local per-stream object-store request
  counters, split into puts and reads, plus a per-artifact breakdown
- this is the supported combined descriptor endpoint for stream-management UIs

Conditional and long-poll behavior:

- responses include `ETag`
- `If-None-Match` may be used for a normal conditional `GET`
- if `If-None-Match` matches the current descriptor, return `304 Not Modified`
- `live=true` or `live=long-poll` enables long-poll mode
- in long-poll mode, if `If-None-Match` matches the current descriptor, wait
  until:
  - the stream head changes because new events are appended
  - descriptor-visible metadata changes, including schema/profile changes,
    segment/upload progress, or async index progress
  - the timeout expires
- `timeout=<duration>` or `timeout_ms=<ms>` controls the long-poll deadline;
  default `3000ms`
- on long-poll timeout with no visible change, return `304 Not Modified`
- the generic resolver timeout still caps the overall request at `5000 ms`, so
  callers should keep requested long-poll waits at `<= 5s` and reconnect on
  `408 Request Timeout`

The same conditional-long-poll contract also applies to
`GET /v1/stream/{name}/_index_status`.

### Server details

`GET /v1/server/_details`

Response fields:

- `auto_tune`
- `configured_limits`
- `runtime`

Rules:

- this endpoint is read-only
- it is server-scoped, not stream-scoped
- `auto_tune` reports whether `--auto-tune` was active for this process and, when present:
  - `requested_memory_mb`
  - `preset_mb`
  - `effective_memory_limit_mb`
- `configured_limits` reports the currently configured budgets and caps for:
  - caches
  - concurrency
  - ingest queue and backlog limits
  - segmenting and upload settings
  - request / object-store timeouts
  - memory-pressure threshold
- `runtime` reports current live state for:
  - memory pressure and RSS
  - current process memory usage:
    - `rss_bytes`
    - `heap_total_bytes`
    - `heap_used_bytes`
    - `external_bytes`
    - `array_buffers_bytes`
  - process-level attribution:
    - `process_breakdown`
  - SQLite runtime allocator state:
    - `sqlite`
  - forced-GC and heap-snapshot state:
    - `gc`
  - tracked runtime memory subsystems, grouped as:
    - `heap_estimates`
    - `mapped_files`
    - `disk_caches`
    - `configured_budgets`
    - `pipeline_buffers`
    - `sqlite_runtime`
    - `counts`
  - subtotal rollups for the tracked groups:
    - `heap_estimate_bytes`
    - `mapped_file_bytes`
    - `disk_cache_bytes`
    - `configured_budget_bytes`
    - `pipeline_buffer_bytes`
    - `sqlite_runtime_bytes`
  - high-water marks with timestamps:
    - `high_water`
  - ingest queue fill
  - local backlog pressure
  - pending uploads
  - the effective concurrency gate state for ingest, read, search, and async index
  - bounded top-N stream contributors for local storage, retained WAL, touch journals, and notifier waiters:
    - `top_streams`
- `runtime.memory.subsystems.heap_estimates` is the operator-facing view of
  retained in-process bytes that the server can currently attribute directly,
  such as queued ingest payload bytes and in-memory index-run caches.
- `runtime.memory.subsystems.mapped_files` reports file-backed mmap bytes such
  as cached segment files, routing-key lexicon runs, and bundled companion
  files. These contribute to RSS differently from JS heap.
- `runtime.memory.subsystems.disk_caches` and
  `runtime.memory.subsystems.configured_budgets` are included so diagnostics
  UIs can contrast retained heap-like bytes with on-disk cache occupancy and
  configured cache budgets.
- `runtime.memory.subsystems.pipeline_buffers` reports current in-flight
  segmenter/uploader bytes rather than retained caches.
- `runtime.memory.subsystems.sqlite_runtime` reports SQLite process-global
  allocator bytes from `sqlite3_status64()` when available.
- `runtime.memory.process_breakdown.unattributed_rss_bytes` is the remaining
  RSS after subtracting JS-managed bytes, tracked mapped-file bytes, and tracked
  SQLite runtime bytes. It is a conservative approximation, not an exact
  ownership map.
- this endpoint is intended for operators and diagnostics UIs that need one cheap summary of the node's configured and effective runtime limits

### Server memory snapshot

`GET /v1/server/_mem`

Response fields:

- `ts`
- `process`
- `process_breakdown`
- `sqlite`
- `gc`
- `high_water`
- `counters`
- `runtime_counts`
- `runtime_bytes`
- `runtime_totals`
- `top_streams`

Rules:

- this endpoint is read-only
- it is server-scoped, not stream-scoped
- it is the compact memory-triage view, whereas `/_details` is the broader node descriptor
- `runtime_bytes` mirrors the byte-bearing memory subsystem groups without the
  `counts` section
- `runtime_totals` mirrors the byte rollups for those groups
- `top_streams` is bounded current state only; it is not part of the metrics stream because
  emitting top-N stream names as metrics would create unbounded series cardinality

---

## 8. Read (GET)

### 8.1 Non-live reads

`GET /v1/stream/{name}?offset=<off>`

- Returns a bounded batch.
- Must include `Stream-Next-Offset`.
- If `filter=` is present, `Stream-Next-Offset` still advances past scanned
  non-matching records.

### 8.2 Live reads (long-poll)

`GET /v1/stream/{name}?offset=<off>&live=true&timeout=5s`

- If data exists after `off`, return immediately.
- Otherwise, wait for new data or timeout.
- On timeout, return an empty batch with `Stream-Next-Offset` unchanged.
- `filter=` is supported for long-poll reads.
- `live=sse` is not supported together with `filter=`.
- the generic resolver timeout still caps the overall request at `5000 ms`, so
  callers should keep requested long-poll waits at `<= 5s` and reconnect on
  `408 Request Timeout`

### 8.3 Formats

- Default (raw): response body is concatenated bytes of returned entries.
- `format=json`: response body is a JSON array of entry payloads (each element is the raw JSON value that was appended).

### 8.4 Key-filtered reads

- `key=<k>` or `/pk/<k>` selects only entries whose routing key equals `<k>`.
- If the routing index has a candidate segment set, the server may plan the
  sealed scan up front and read only candidate indexed segments plus the
  uncovered uploaded tail.
- `since + key` cursor seeking may use the same planned sealed scan.

Correctness requirement:
- Key-filtering is exact: false positives from bloom/index must still validate actual key matches.

### 8.5 Filtered reads

- `filter=` is only supported on `application/json` streams.
- The server may use schema-owned exact and `.col` search families to prune
  sealed segments and segment-local docs.
- The server must still scan the local WAL tail so unsealed data remains
  visible to filtered reads.
- The current implementation stops after examining 100 MB of payload bytes for
  one filtered response and returns the filter scan headers above.

### 8.6 Search

Current endpoints:

- `POST /v1/stream/{name}/_search`
- `GET /v1/stream/{name}/_search?q=...`

Current request fields:

- `q`
- `size`
- `search_after`
- `sort`
  - optional sort list
  - when omitted, non-scoring filter queries sort by `offset:desc`
  - when omitted, scoring text queries sort by `_score:desc`, then the
    primary timestamp field, then `offset:desc`
- `timeout_ms`
  - optional lower per-request budget
  - server-side effective timeout is always clamped to `<= 3000 ms`
  - the deadline is enforced cooperatively between work units, so wall time may
    overshoot slightly before the partial response is returned

Current response fields:

- `stream`
- `snapshot_end_offset`
- `took_ms`
- `timed_out`
- `timeout_ms`
- `coverage`
- `total`
- `hits`
- `next_search_after`

Current response status behavior:

- `200` when search completes within the effective timeout budget
- `408` when search reaches the effective timeout budget
  - the response body is still a valid search result
  - partial hits and coverage counters are included
  - `total.relation` is `gte`
  - observed wall time may be slightly above `timeout_ms`
- if the outer generic `5000 ms` resolver timeout fires first while an
  in-flight search work unit is still running, `/_search` may instead return
  the generic request-timeout error body described above

Current search response headers:

- `search-timed-out`
- `search-timeout-ms`
- `search-took-ms`
- `search-total-relation`
- `search-coverage-complete`
- `search-indexed-segments`
- `search-indexed-segment-time-ms`
- `search-fts-section-get-ms`
- `search-fts-decode-ms`
- `search-fts-clause-estimate-ms`
- `search-scanned-segments`
- `search-scanned-segment-time-ms`
- `search-scanned-tail-docs`
- `search-scanned-tail-time-ms`
- `search-exact-candidate-time-ms`
- `search-index-families-used`

Current search coverage fields:

- `mode`
- `complete`
- `stream_head_offset`
- `visible_through_offset`
- `visible_through_primary_timestamp_max`
- `oldest_omitted_append_at`
- `possible_missing_events_upper_bound`
- `possible_missing_uploaded_segments`
- `possible_missing_sealed_rows`
- `possible_missing_wal_rows`
- `indexed_segments`
- `indexed_segment_time_ms`
- `fts_section_get_ms`
- `fts_decode_ms`
- `fts_clause_estimate_ms`
- `scanned_segments`
- `scanned_segment_time_ms`
- `scanned_tail_docs`
- `scanned_tail_time_ms`
- `exact_candidate_time_ms`
- `index_families_used`

Current query support:

- fielded exact keyword queries
- fielded keyword prefix queries
- typed equality and range queries
- `has:field`
- bare terms over `search.defaultFields`
- fielded text queries
- quoted phrase queries on text fields with `positions=true`
- alias resolution from `search.aliases`

Current non-support:

- `contains:`
- snippets
- multi-stream search

Current request-path behavior under active ingest:

- `/_search` always reports against the current stream head via
  `snapshot_end_offset`
- while sealed segments are still unpublished or bundled companions are still
  catching up, `/_search` may intentionally omit the newest suffix instead of
  scanning it on the request path
- in that case `coverage.complete=false` and the `possible_missing_*` fields
  report an upper bound on omitted newest events
- once publish and bundled-companion work are caught up, `/_search` still omits
  a fresh WAL tail during active ingest
- `/_search` may search the current WAL tail locally only after the tail is
  quiet for the configured overlay period and still fits within the overlay
  budget
- `visible_through_primary_timestamp_max` and `oldest_omitted_append_at` let
  clients explain the freshness gap in time terms
- if the newest suffix is omitted, `total.relation` is `gte`
- if the returned page may not include every visible match, `total.relation` is
  `gte`
- `/_search` does not support request-time exact total-hit counting
- when exact clauses provide a candidate segment set, `/_search` may plan the
  sealed segment scan up front instead of iterating the full indexed sealed
  prefix one segment at a time
- if the request hits the effective timeout budget, `/_search` returns `408`
  with a valid partial search result body instead of keeping the request open
- timeout checks are cooperative rather than preemptive, so clients should treat
  `timeout_ms` as a bounded target rather than a strict wall-clock guarantee

### 8.7 Aggregate

Current endpoint:

- `POST /v1/stream/{name}/_aggregate`

Current request fields:

- `rollup`
- `from`
- `to`
- `interval`
- `q`
- `group_by`
- `measures`

Current response fields:

- `stream`
- `rollup`
- `from`
- `to`
- `interval`
- `coverage`
- `buckets`

Current aggregate coverage fields:

- `mode`
- `complete`
- `stream_head_offset`
- `visible_through_offset`
- `visible_through_primary_timestamp_max`
- `oldest_omitted_append_at`
- `possible_missing_events_upper_bound`
- `possible_missing_uploaded_segments`
- `possible_missing_sealed_rows`
- `possible_missing_wal_rows`
- `used_rollups`
- `indexed_segments`
- `scanned_segments`
- `scanned_tail_docs`
- `index_families_used`

Current behavior:

- rollups are schema-owned under `search.rollups`
- aligned middle windows may use `.agg` companions
- partial edge windows must still scan source segments
- while sealed segments are still unpublished or bundled companions are still
  catching up, `/_aggregate` may intentionally omit the newest suffix instead
  of scanning it on the request path
- in that case `coverage.complete=false` and the `possible_missing_*` fields
  report an upper bound on omitted newest events
- once publish and bundled-companion work are caught up, `/_aggregate` still
  omits a fresh WAL tail during active ingest
- `/_aggregate` may evaluate the current WAL tail locally only after the tail
  is quiet for the configured overlay period and still fits within the overlay
  budget

---

## 9. HEAD (metadata)

`HEAD /v1/stream/{name}`

Should return:
- `200 OK` if exists
- `404` if missing

Headers:
- `Stream-End-Offset`
- `Content-Type` for the stream if known
- `Stream-Expires-At` if the stream has TTL

---

## 10. DELETE

`DELETE /v1/stream/{name}`

- Deletes/tombstones the stream.
- Removes the stream's local acceleration state in the same local delete
  transaction:
  - routing-key index state and runs
  - exact secondary index state and runs
  - routing-key lexicon state and runs
  - bundled search companion plans and per-segment companion catalog rows
- Does not synchronously delete already-published segment, manifest, schema, or
  index objects from remote object storage.
- Must be idempotent.

---

## 11. Errors

Recommended status codes:

- `400 Bad Request`: invalid parameters, invalid JSON, invalid schema/lens
- `404 Not Found`: unknown stream
- `409 Conflict`: `Stream-Seq` mismatch
- `410 Gone`: expired stream (or `404` if you prefer hiding existence; choose one and keep it consistent)
- `413 Payload Too Large`: append body too large
- `429 Too Many Requests`: transient queue or backlog backpressure
- `503 Service Unavailable`: transient server unavailability, such as shutdown
- `500 Internal Server Error`: unexpected errors

Errors should be JSON:

```json
{"error": {"code": "...", "message": "..."}}
```

Transient `429` and `503` responses should include `Retry-After` so clients can
apply server-guided backoff.

---

## 12. Schema endpoints

See `schemas.md` for the full model.

Minimum behavior required:
- `GET /_schema` returns the schema registry JSON (or 404 if none and stream missing).
- `POST /_schema` installs first schema only on empty streams.
- Later updates require a lens `v -> v+1` and must record a boundary at the current end offset.
- Appends validate against current schema.
- Reads promote older events through the lens chain to the current schema.
- `POST /_schema` accepts only the supported update fields: `schema`, `lens`,
  `routingKey`, and `search` (plus optional `apiVersion`).
- `search` is the only supported public search/indexing model.
- `search`-only updates require an already-installed schema version.
- `POST /_schema` rejects registry-shaped compatibility writes, alias field
  names, legacy `indexes[]`, and profile-owned live/touch configuration.
