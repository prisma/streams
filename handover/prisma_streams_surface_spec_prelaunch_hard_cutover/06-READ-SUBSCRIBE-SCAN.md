# Stage 6 — `read`, `subscribe`, and `scan`

**Goal:** replace read-mode flags with three operations whose semantics match the underlying ordering model.

---

## 0. Pre-launch clean-switch policy

This specification targets a **pre-launch** product. Implementation MUST be a destructive clean switch to the final design described here.

- Existing Prisma Streams development, test, staging, and campaign data is disposable. Existing descriptors, cursors, consumer state, watch state, segment maps, history/index layouts, and product API requests MUST NOT be migrated.
- The new implementation MUST be deployed against a fresh storage namespace, such as a new bucket or `PATH_PREFIX`. It MUST NOT open or mutate a namespace written by the current experimental implementation.
- Do not build compatibility translators, legacy decoders, cutover offsets, dual readers, dual writers, shadow writes, aliases, deprecation windows, sunset headers, or background migration tools.
- Delete obsolete fields, routes, codecs, and branches instead of retaining dormant compatibility code.
- Update server, SDK, documentation, conformance fixtures, deployment configuration, and test data together. Intermediate stage builds are engineering checkpoints, not supported product versions.
- Rollback means restoring the previous binary together with its previous isolated storage namespace. The old binary is not required to read data written by the new binary, and the new binary is not required to read old data.
- The only preserved external contract is conformance to the pinned **Durable Streams protocol** on `/v1/stream/{name}`. That route is a standards surface for Durable Streams clients, not a legacy Prisma Streams update path.

---

## 1. Public SDK

### 1.1 Ordered key read

```ts
const page = await stream.read({
  routingKey: "customer-42",
  from: "beginning",
  maxBytes: 1_048_576,
})
```

Omitting `routingKey` selects the default empty key.

### 1.2 Ordered key subscription

```ts
for await (const record of stream.subscribe({
  routingKey: "customer-42",
  from: page.cursor,
  signal,
})) {
  // ordered for this routing key
}
```

Omitting `routingKey` subscribes to the default key.

### 1.3 Cross-key scan

```ts
for await (const record of stream.scan()) {
  // deterministic snapshot traversal
  // no cross-key append-order guarantee
}
```

`scan` is finite and snapshot-bounded. It is not a live subscription and is not represented as a Durable Streams offset sequence.

---

## 2. Read semantics

### 2.1 Input

```ts
interface ReadOptions {
  routingKey?: string
  from?: "beginning" | "now" | KeyCursor
  maxBytes?: number
}
```

Defaults:

```text
routingKey = ""
from       = "beginning"
maxBytes   = server page default
```

### 2.2 Output

```ts
interface ReadPage<T> {
  records: T[]
  cursor: KeyCursor
  upToDate: boolean
  sealed: boolean
}
```

For byte streams, SDK output may be a byte chunk plus the same cursor/status metadata.

### 2.3 Cursor

A key cursor encodes opaquely:

```text
stream incarnation
routing-key hash discriminator
segment-lineage position
segment-local consumed offset
```

The server validates that the cursor belongs to the requested stream and exact routing key. A cursor for another key/stream returns `400 invalid_cursor`.

Cursors remain valid across ownership movement and segment split/merge because lineage is preserved.

### 2.4 `from: "now"`

Captures the current tail of the selected key and returns an empty, up-to-date page. For subscribe, it begins waiting for future records without first replaying history.

### 2.5 Page progress

The internal reader tracks:

```text
last returned record
consumed-through key position
```

A page may advance its cursor across a postings range with no matching records. It MUST never loop forever on an empty result.

---

## 3. Product read wire API

```text
GET /v1/streams/{name}/records
    ?routingKey={encoded}
    &cursor={key-cursor|beginning|now}
    &maxBytes={n}
```

Product response headers:

```text
Prisma-Next-Cursor
Prisma-Up-To-Date: true
Prisma-Sealed: true
```

For JSON streams, body is a JSON array of records. For byte streams, body is raw bytes.

These headers are deliberately not named `Stream-Next-Offset`; a key cursor is not the global offset of a Durable Stream URL.

Caching:

- immutable historical key pages may have ETag/cache headers;
- ETag includes stream incarnation, routing-key discriminator, start cursor, end cursor, and sealed state;
- confidential streams use private caching or cache keys that include authorization;
- `now` responses are `no-store`.

---

## 4. Subscribe semantics

### 4.1 Public contract

`subscribe()` is an async iterable that:

1. catches up from the requested key cursor;
2. switches to live waiting;
3. reconnects after transport closure;
4. resumes from the last committed cursor;
5. stops permanently when collection sealing is observed at the key tail.

The public API does not expose `long-poll`, `SSE`, timeout, heartbeat, or reconnect mode.

### 4.2 Transport selection

SDK default:

- JSON/text: SSE when available;
- binary: pipelined long-poll unless binary SSE is explicitly proven better;
- automatic fallback between supported transports on intermediary incompatibility.

Transport choice must not change record, cursor, EOF, or retry semantics.

### 4.3 Committed versus speculative cursor

The client maintains:

```text
committed cursor   = body fully received and decoded
speculative cursor = next request may already be in flight
```

If the prior body fails or is truncated, discard the speculative response and retry from the committed cursor. This prevents pipelining from skipping records.

### 4.4 Product subscribe routes

The server may expose transport-specific internal routes:

```text
GET /v1/streams/{name}/records:long-poll
GET /v1/streams/{name}/records:sse
```

They are SDK implementation details, not user-selected product modes.

Both use `routingKey` and key cursors.

### 4.5 Tail ring

The durable tail ring is the preferred hot path:

```text
durable state update
publish canonical batch to ring
notify key waiters
send producer acknowledgements
```

A subscriber covered by the ring avoids a SlateDB scan. Falling behind the ring transparently uses the canonical/history reader.

Ring memory is bounded globally and records are canonical encrypted frames or another explicitly accounted representation.

---

## 5. Cross-key scan

### 5.1 Semantics

A scan returns every record that existed at scan snapshot creation, exactly once, in deterministic traversal order.

It does **not** claim:

- cross-key append order;
- real-time live delivery;
- one global Durable Streams offset.

### 5.2 Snapshot creation

On the first scan request, capture:

```text
stream incarnation
segment-map version
ordered segment lineage set
per-segment terminal boundary for this snapshot
```

New appends and new successor segments after snapshot creation are excluded.

### 5.3 Cursor encoding without storage requests

The scan cursor is a signed, versioned, optionally compressed token containing:

```rust
struct ScanCursorV1 {
    stream_epoch: StreamEpoch,
    map_version: u64,
    segments: Vec<SegmentSnapshot>,
    current_segment_index: u32,
    current_offset: u64,
}

struct SegmentSnapshot {
    segment_id: u32,
    end_exclusive: u64,
}
```

The snapshot is embedded in the cursor so creating a scan does not add a control-plane PUT/database row.

Limits:

```text
max active/historical segments encoded = configured safety limit
max encoded scan cursor                = 16 KiB
```

If the topology exceeds the embedded-cursor limit, the server may create a persistent scan snapshot as an explicit paid/advanced operation, but the default implementation should keep segment counts within the embedded bound.

### 5.4 Traversal order

Segments are traversed by stable lineage order:

```text
creation generation, then segment ID
```

Within a segment, canonical offsets ascend.

The order is deterministic for the snapshot and supports exact resume. It is not described as chronological across keys.

### 5.5 Split/merge during scan

A scan continues reading the segments captured at snapshot creation, including sealed predecessors. GC must retain referenced history until the cursor's retention horizon or documented scan TTL expires.

The scan cursor includes an expiry. An expired cursor returns `410 scan_expired` rather than silently restarting.

### 5.6 Product route

```text
GET /v1/streams/{name}:scan
    ?cursor={scan-cursor}
    &maxBytes={n}
```

Response:

```text
Prisma-Next-Scan-Cursor
Prisma-Scan-Complete: true
```

No `Stream-*` offset headers are used.

---

## 6. Raw Durable Streams reads

The standards endpoint permanently retains:

```text
GET /v1/stream/{name}?offset=...
GET /v1/stream/{name}?offset=...&live=long-poll
GET /v1/stream/{name}?offset=...&live=sse
```

It preserves:

- protocol offset sentinels `-1` and `now`;
- `Stream-Next-Offset`;
- `Stream-Up-To-Date`;
- `Stream-Closed` EOF;
- `Stream-Cursor` collapsing;
- SSE control/data framing;
- binary base64 behavior;
- ETag/304 rules;
- byte-exact resumption.

The SDK may implement its default-key `read`/`subscribe` through this route when doing so is advantageous, but it presents the simplified product API.

---

## 7. Error model

Product key read:

| Condition | Status |
|---|---:|
| invalid cursor/key mismatch | `400` |
| stream not found | `404` |
| expired collection/cursor before retained data | `410` |
| wrong encryption key | `403` |
| rate limit | `429` |
| temporary owner movement | `503` |

Subscribe retries transient `429`, `503`, timeouts, resets, and normal SSE rotation with backoff/jitter while preserving the committed cursor.

Scan never automatically restarts from beginning after `410`; that would duplicate a large export silently.

---

## 8. Read cost constraints

Key read inherits the postings-cache/range-planner limits from Stage 3.

Subscribe target:

```text
hot-ring remote GETs per delivered page     ~0
postings cold load per active key window    <= 1 typical
rearm gap p50                               ~0
one remote GET per record pattern           forbidden
```

Scan uses large bulk read-ahead and a larger page budget than live key reads. `TAIL_MAX_BYTES` and scan/bulk limits are separate.

---

## 9. Clean API and cursor switch

Remove the old product read/tail methods and their options:

```text
offset
key
live
timeout-as-mode-selection
```

The only product operations are:

```ts
stream.read({ from, routingKey })
stream.subscribe({ from, routingKey })
stream.scan({ from })
```

Rules:

- No `offset` or `key` aliases are provided.
- No old unkeyed cross-segment read is translated into `scan()`.
- No old unkeyed live read receives a special migration error; the obsolete route/method is simply absent.
- Product cursors use only the final signed cursor codecs.
- Raw Durable Streams offsets, long-poll, and SSE remain on the singular standards route because they are protocol behavior.

## 10. Correctness invariants

1. Key read returns exact per-key order with no duplicates/gaps.
2. Subscribe resumes from committed, not speculative, cursor.
3. Tail-ring and DB paths are byte/message equivalent.
4. A key cursor never crosses into another key.
5. A scan snapshot includes each captured record exactly once.
6. New records after snapshot creation are excluded.
7. Scan traversal is deterministic but makes no global-order claim.
8. Raw protocol reads remain byte-exact and conformant.
9. EOF/seal is observable without waiting after the key/default stream is at tail.

---

## 11. Test plan

- cursor stream/key mismatch;
- empty-key default behavior;
- split/merge lineage reads;
- ring eviction and DB fallback;
- partial body reset with speculative long-poll;
- SSE reconnect and final sealed control;
- postings empty-range progress;
- scan with concurrent appends;
- scan with concurrent split/merge;
- scan cursor tamper/expiry;
- large segment-count cursor bound;
- raw catch-up/long-poll/SSE conformance unchanged.

---

## 12. Observability

```text
key_read_pages
key_read_records
key_read_cache_hits
key_read_remote_scans
subscribe_connections
subscribe_reconnects
subscribe_speculative_discards
subscribe_ring_hits
subscribe_db_fallbacks
scan_started
scan_completed
scan_expired
scan_bytes
scan_segments
```

---

## 13. Exit criteria

```text
new SDK live mode option                         removed
new SDK ambiguous unkeyed cross-key read         removed
keyed read/subscribe DST corpus                  pass
scan snapshot exactness corpus                   pass
speculative pipelining reset test                pass
raw Durable Streams read conformance             pass
product route returns Stream-Next-Offset cursor  0

```
