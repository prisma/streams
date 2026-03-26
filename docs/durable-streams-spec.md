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
- `GET  /v1/stream/{name}/_index_status` get per-stream index status
- `GET  /v1/stream/{name}/_details` get combined stream details

### 2.5 Streams collection

- `GET /v1/streams` list streams

System streams (reserved names):
- `__stream_metrics__` (metrics; see `metrics.md`)
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

- supported built-ins are `evlog`, `generic`, and `state-protocol`
- `evlog` requires an `application/json` stream content type
- `evlog` normalizes JSON appends into a canonical request-log envelope and
  derives a routing key from `requestId` or `traceId` when the schema does not
  own routing-key extraction
- installing `evlog` also installs the canonical evlog schema version `1` and
  default `search` registry for that stream
- `state-protocol` requires an `application/json` stream content type
- `state-protocol.touch.enabled=true` enables the `/touch/*` routes
- set `profile` to `{ "kind": "generic" }` to use the baseline durable stream
  behavior

## 7B. Stream inspection resources

### Index status

`GET /v1/stream/{name}/_index_status`

Response fields:

- `stream`
- `profile`
- `segments`
- `manifest`
- `routing_key_index`
- `exact_indexes`
- `search_families`

Rules:

- this endpoint is read-only
- it reports current per-stream segment and manifest state
- it reports async index/search-family progress for the current stream
- `routing_key_index` covers the routing-key tiered index
- `exact_indexes` covers the internal exact-match secondary family derived from
  schema `search.fields`
- `search_families` covers object-store-native search companions such as `.col`
  and `.fts`

### Combined details

`GET /v1/stream/{name}/_details`

Response fields:

- `stream`
- `profile`
- `schema`
- `index_status`

Rules:

- this endpoint is read-only
- `profile` matches `GET /_profile`
- `schema` matches `GET /_schema`
- `index_status` matches `GET /_index_status`
- this is the supported combined descriptor endpoint for stream-management UIs

---

## 8. Read (GET)

### 8.1 Non-live reads

`GET /v1/stream/{name}?offset=<off>`

- Returns a bounded batch.
- Must include `Stream-Next-Offset`.
- If `filter=` is present, `Stream-Next-Offset` still advances past scanned
  non-matching records.

### 8.2 Live reads (long-poll)

`GET /v1/stream/{name}?offset=<off>&live=true&timeout=30s`

- If data exists after `off`, return immediately.
- Otherwise, wait for new data or timeout.
- On timeout, return an empty batch with `Stream-Next-Offset` unchanged.
- `filter=` is supported for long-poll reads.
- `live=sse` is not supported together with `filter=`.

### 8.3 Formats

- Default (raw): response body is concatenated bytes of returned entries.
- `format=json`: response body is a JSON array of entry payloads (each element is the raw JSON value that was appended).

### 8.4 Key-filtered reads

- `key=<k>` or `/pk/<k>` selects only entries whose routing key equals `<k>`.

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
- `track_total_hits`
- `timeout_ms`

Current response fields:

- `stream`
- `snapshot_end_offset`
- `took_ms`
- `coverage`
- `total`
- `hits`
- `next_search_after`

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

Current behavior:

- rollups are schema-owned under `search.rollups`
- aligned middle windows may use `.agg` companions
- partial edge windows must still scan source segments
- uncovered or stale ranges must still scan source segments
- the WAL tail must always be evaluated directly

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
- Must be idempotent.

---

## 11. Errors

Recommended status codes:

- `400 Bad Request`: invalid parameters, invalid JSON, invalid schema/lens
- `404 Not Found`: unknown stream
- `409 Conflict`: `Stream-Seq` mismatch
- `410 Gone`: expired stream (or `404` if you prefer hiding existence; choose one and keep it consistent)
- `413 Payload Too Large`: append body too large
- `429 Too Many Requests` or `503 Service Unavailable`: backpressure / memory budget exhausted
- `500 Internal Server Error`: unexpected errors

Errors should be JSON:

```json
{"error": {"code": "...", "message": "..."}}
```

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
