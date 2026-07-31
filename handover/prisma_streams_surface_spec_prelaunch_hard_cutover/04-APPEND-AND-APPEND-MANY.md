# Stage 4 — `append` and `appendMany`

**Goal:** make operation semantics explicit so a JSON payload's shape never silently changes whether the user appends one record or many.

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

### 1.1 Append one

```ts
await stream.append(value, {
  routingKey: "customer-42",
})
```

For a JSON stream, `value` is exactly one JSON message. A JSON array value is stored as one array-valued message.

For a bytes stream, `value` is `Uint8Array`, `ArrayBuffer`, `Blob`, or a supported readable byte stream.

### 1.2 Append many

```ts
await stream.appendMany(values, {
  routingKey: "customer-42",
})
```

`appendMany` is available only for JSON streams in the first implementation. It stores each array element as one message and is atomic as one append request.

Rules:

- `values.length` MUST be at least 1;
- all records in one call share one routing key;
- all records are routed to one segment;
- all records are accepted or rejected together;
- one producer sequence number identifies the whole batch;
- offsets inside the key sequence are contiguous.

No cross-key batch API is introduced. Cross-key batching would couple independent partitions and weaken scaling/failure isolation.

---

## 2. Product wire API

```text
POST /v1/streams/{name}/records
POST /v1/streams/{name}/records:batch
```

Routing key is supplied using the namespaced field defined in Stage 8.

### 2.1 Single JSON record

```http
POST /v1/streams/orders/records
Content-Type: application/json
Prisma-Routing-Key: customer-42

[1, 2, 3]
```

This stores one message whose value is the JSON array `[1,2,3]`.

### 2.2 JSON batch

```http
POST /v1/streams/orders/records:batch
Content-Type: application/json
Prisma-Routing-Key: customer-42

[{"id":1},{"id":2}]
```

This stores two messages.

### 2.3 Bytes append

```http
POST /v1/streams/output/records
Content-Type: application/octet-stream

<bytes>
```

`records:batch` returns `405 Method Not Allowed` for non-JSON formats until an explicit framed batch format is standardized.

---

## 3. Durable Streams standards behavior

The raw protocol endpoint retains exact JSON-mode behavior:

```text
POST /v1/stream/{name}
```

For `application/json`:

- an object/scalar body stores one message;
- a top-level array is flattened one level and stores each element as one message;
- an empty array is rejected on POST;
- GET returns a JSON array.

The product SDK does not expose that ambiguity:

- `append(value)` sends the product single-record route;
- `appendMany(values)` sends the product batch route.

A fallback SDK implementation against a raw-only Durable Streams server MAY implement `append(value)` by POSTing `[value]`, which relies on the protocol's one-level flattening and correctly stores an array-valued `value` as one nested element.

This stage MUST NOT change raw protocol semantics or conformance behavior.

---

## 4. Internal request model

Both routes compile to one internal type:

```rust
struct AppendCommand {
    collection: StreamIncarnation,
    routing_key: RoutingKey,
    entries: Vec<Bytes>,
    content_type: MediaType,
    producer: Option<ProducerRequest>,
    seal_after: bool,
    request_bytes: u64,
}
```

`entries.len()` is 1 for `append` and N for `appendMany`.

The committer assigns offsets, compresses/encrypts frames, stages producer state, updates the dirty-stream index, and writes one atomic SlateDB `WriteBatch`.

There is no different storage path for single and batch append.

---

## 5. Validation order

Product append validation order:

1. authenticate and authorize;
2. stream exists and is not deleted/expired;
3. collection is not sealed;
4. content type matches immutable config;
5. routing key syntax/size is valid;
6. producer tuple is valid and duplicate detection runs;
7. body size is within limit;
8. JSON syntax is valid;
9. batch is non-empty and within record-count limit;
10. service limits admit the request;
11. route to current segment;
12. commit.

Duplicate producer requests MUST be recognized before rejecting a retried body for a later validation condition that was irrelevant to the original committed request, consistent with the raw producer contract.

---

## 6. Limits

Initial defaults:

```text
max request body           = 32 MiB
max JSON messages/batch    = 10,000
max routing-key bytes      = 1,024
max uncompressed message   = 32 MiB
```

Limits are enforced before creating unbounded per-message allocations. JSON parsing SHOULD use a streaming/raw-value parser rather than a full generic DOM followed by reserialization.

For batch JSON:

- retain slices into the request body where safe;
- avoid serializing each parsed value again;
- account exact request bytes and estimated frame bytes before enqueue.

---

## 7. Response contract

Product response:

```json
{
  "cursor": "opaque-key-cursor",
  "count": 2,
  "duplicate": false,
  "sealed": false
}
```

Status:

| Situation | Status |
|---|---:|
| accepted append | `200 OK` |
| duplicate producer request | `200 OK` with `duplicate: true` |
| malformed/empty batch | `400` |
| content-type mismatch | `409` |
| producer gap | `409` |
| sealed collection | `409` |
| stale producer epoch | `403` |
| rate/admission limit | `429` |
| body too large | `413` |

The product cursor is for the routing-key sequence. It MUST NOT be called `Stream-Next-Offset` on the product route.

The raw protocol route continues returning protocol-defined status codes and headers.

---

## 8. Atomicity and durability

For one append request:

- all frames;
- routing-key tail state;
- producer state;
- stream tail/dirty metadata;
- optional seal state;
- usage counters required for durable billing/outbox integration

MUST be staged in one serialized commit and become acknowledged only after the durability contract is met.

`appendMany` is not partially successful. A failure before acknowledgment leaves either zero or all messages committed; an ambiguous timeout is resolved through producer idempotence.

---

## 9. Sealing

The SDK removes `close` from append options.

Atomic final append remains available through the explicit lifecycle method defined in Stage 8:

```ts
await stream.seal({
  final: value,
  routingKey: "customer-42",
})
```

Internally this is one `AppendCommand` with `seal_after = true`.

The raw Durable Streams endpoint continues to support `Stream-Closed: true` on POST exactly as required by the protocol.

---

## 10. Cost and performance constraints

The two product routes MUST NOT add object-store operations.

Acceptance:

```text
one append request              -> one committer command
one appendMany request          -> one committer command
WAL/manifest requests           <= raw equivalent
batch payload copy count        <= 1 avoidable copy before encryption
JSON DOM + reserialize path      removed
```

SDK auto-batching MAY combine consecutive `append` calls only when:

- they target the same collection, routing key, producer session, and content type;
- their producer ordering remains exact;
- the latency deadline is respected;
- each caller receives the correct resulting cursor range.

Auto-batching is transport optimization, not a separate public mode.

---

## 11. Clean API switch

The SDK and Prisma product route switch directly to the final operations:

```ts
producer.append(value, { routingKey })
producer.appendMany(values, { routingKey })
stream.seal({ final: value, routingKey })
```

Rules:

- Remove the old product `append(value, { key, seq, close })` signature.
- Do not provide `key`, `seq`, or `close` aliases.
- Do not retain a legacy product POST whose batch semantics depend on a top-level JSON array.
- Do not emit deprecation warnings or translation telemetry.
- Keep protocol-defined JSON flattening only on the singular Durable Streams standards route because it is part of that protocol, not because it is an update path.

## 12. Correctness invariants

1. `append([1,2])` stores one array message.
2. `appendMany([1,2])` stores two messages.
3. Empty `appendMany` never consumes an offset.
4. A batch is atomic and contiguous for its routing key.
5. Retry of the same producer request does not duplicate any element.
6. Split/ownership movement cannot divide one batch across segments.
7. Raw JSON mode remains protocol-conformant.
8. Single and batch operations share one storage implementation.

---

## 13. Test plan

- all JSON scalar/object/array nesting shapes;
- empty array rules on product and raw routes;
- max record count/body boundary ±1;
- invalid JSON without partial writes;
- ambiguous response and producer retry;
- appendMany during segment split;
- append-and-seal crash matrix;
- bytes streaming/chunked request;
- allocation/copy benchmark;
- official JSON-mode conformance unchanged.

---

## 14. Exit criteria

```text
SDK payload shape changes operation semantics          0
product append and batch routes share commit path      yes
raw Durable Streams JSON conformance                   pass
ambiguous batch retry duplicates                       0
extra object-store requests vs raw equivalent          0
old key/seq/close SDK aliases                         absent

```
