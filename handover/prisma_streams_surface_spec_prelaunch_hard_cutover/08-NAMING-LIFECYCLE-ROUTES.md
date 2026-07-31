# Stage 8 — Naming, Lifecycle, and Routes

**Goal:** make terminology unambiguous, lifecycle explicit, and HTTP resources predictable while retaining the raw Durable Streams URL.

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

## 1. Terminology

Use these terms consistently in SDKs, docs, HTTP fields, metrics, and code:

| Term | Meaning |
|---|---|
| `token` | account/service authorization credential |
| `encryptionKey` | 32-byte customer-held stream encryption key |
| `routingKey` | byte/string key that selects an independently ordered sequence |
| `cursor` | opaque product key-read position |
| `scanCursor` | opaque snapshot scan position |
| `offset` | opaque Durable Streams protocol position only |
| `stream` | Prisma stream collection in product docs |
| `default-key stream` | standards-conformant sequence at the singular route |
| `segment` | internal scaling unit, operator-only |
| `seal` | durable, monotonic end of writes |
| `delete` | remove resource/data subject to fork/reference rules |

Do not use the unqualified word `key` in public APIs.

---

## 2. Client and stream handles

```ts
const client = new StreamsClient({
  url,
  token,
})

const orders = client.stream("orders", {
  encryptionKey: ordersKey,
})
```

Authentication belongs to the client. Encryption belongs to the stream handle.

This supports one client accessing streams with different keys and discourages accidental key reuse.

`createStream` returns the same configured handle:

```ts
const orders = await client.createStream("orders", {
  encryptionKey: ordersKey,
  format: { kind: "json" },
})
```

The server never treats the authorization token as an encryption key or vice versa.

---

## 3. Product route hierarchy

### Collection management

```text
PUT    /v1/streams/{name}
GET    /v1/streams/{name}
DELETE /v1/streams/{name}
POST   /v1/streams/{name}:seal
```

### Records

```text
POST   /v1/streams/{name}/records
POST   /v1/streams/{name}/records:batch
GET    /v1/streams/{name}/records
GET    /v1/streams/{name}/records:long-poll   // SDK internal
GET    /v1/streams/{name}/records:sse         // SDK internal
GET    /v1/streams/{name}:scan
```

### Consumer groups

```text
PUT    /v1/streams/{name}/consumers/{consumer}
GET    /v1/streams/{name}/consumers/{consumer}
DELETE /v1/streams/{name}/consumers/{consumer}
POST   /v1/streams/{name}/consumers/{consumer}:pull
POST   /v1/streams/{name}/consumers/{consumer}:settle
```

### Watches

```text
GET /v1/streams/{name}/watches
GET /v1/streams/{name}/watches/{watch}
GET /v1/streams/{name}/watches/{watch}/keys/{watchKey}
```

### Durable Streams standards route

```text
PUT|POST|GET|HEAD|DELETE /v1/stream/{name}
```

### Protocol control namespace

```text
/v1/stream/__ds/...
```

or the exact root-relative shape required by the pinned Durable Streams protocol. Reserved `__ds` routes MUST be matched before wildcard stream names.

---

## 4. Stream naming

### 4.1 Product names

A name is a hierarchical UTF-8 path relative to the stream root.

Examples:

```text
orders
customers/acme/orders
agents/run-123/output
```

Rules:

- 1–512 UTF-8 bytes;
- path segments separated by `/`;
- no empty segments;
- no `.` or `..` segments;
- no control characters;
- percent-decoding occurs exactly once;
- canonical re-encoding is stable;
- first root-relative segment `__ds` is reserved;
- product subresource suffixes are not parsed from the wildcard name because explicit route matching occurs first.

Authorization and registry identity use the canonical decoded name.

### 4.2 Consumer/watch names

Consumer and watch names are one path-safe UTF-8 segment, 1–128 bytes, with no `/`, control characters, `.` or `..`.

### 4.3 Internal names

Segment IDs, history partitions, DLQ idempotency keys, adapter catalog IDs, and watch-key hashes are never customer stream names.

---

## 5. Wire field names

Product request headers:

```text
Prisma-Encryption-Key
Prisma-Routing-Key
```

Product response headers:

```text
Prisma-Next-Cursor
Prisma-Up-To-Date
Prisma-Sealed
Prisma-Next-Scan-Cursor
Prisma-Scan-Complete
```

Removed experimental product names:

```text
Stream-Encryption-Key
Stream-Key
?key=
```

Rules:

- the plural Prisma product route accepts only the final `Prisma-*` names and `routingKey` query field;
- removed names are rejected or treated as unknown input; they are never translated;
- raw Durable Streams protocol headers keep their official `Stream-*`/`Producer-*` names on the singular standards route;
- product fields do not squat on new unregistered `Stream-*` names.

---

## 6. Metadata operation

```text
GET /v1/streams/{name}
```

Response:

```json
{
  "name": "orders",
  "contentType": "application/json",
  "createdAt": "...",
  "sealed": false,
  "expiry": { "idle": "30d" },
  "watches": [
    { "name": "by-customer", "fields": ["/customerId"] }
  ]
}
```

It MUST NOT expose:

- segment IDs or count as control fields;
- storage prefixes;
- history layout/index format;
- encryption fingerprint;
- internal route hashes;
- raw customer routing keys.

An operator/debug endpoint may expose redacted topology separately.

The raw protocol `HEAD /v1/stream/{name}` remains the canonical default-key metadata operation and returns protocol headers with `Cache-Control: no-store`.

---

## 7. Seal lifecycle

### 7.1 Seal without final record

```ts
await stream.seal()
```

Wire:

```http
POST /v1/streams/orders:seal
Content-Type: application/json

{}
```

### 7.2 Atomic final append and seal

```ts
await stream.seal({
  final: { type: "completed" },
  routingKey: "customer-42",
  producer,
})
```

Wire uses one serialized append command with `seal_after = true`.

### 7.3 Semantics

- sealing is collection-wide;
- state is durable and monotonic;
- after acknowledgment, no routing key accepts appends;
- duplicate seal-only requests succeed idempotently;
- duplicate producer final-append requests return deduplicated success;
- a different append after sealing returns `409 sealed`;
- reads remain available;
- subscriptions terminate after reaching each key tail;
- consumer groups may continue draining existing records;
- watches may report final invalidations and then sealed state.

### 7.4 Raw protocol mapping

The default-key standards route retains:

```text
POST /v1/stream/{name}
Stream-Closed: true
```

A raw close seals the entire underlying collection. This is conservative and ensures the protocol URL's closed state remains truthful.

Product `seal` updates the raw default-key view so `HEAD`/GET report `Stream-Closed: true` at the appropriate tail.

---

## 8. Delete lifecycle

```ts
await stream.delete()
```

Wire:

```text
DELETE /v1/streams/{name}
```

Semantics:

- tombstone collection descriptor;
- reject future direct operations;
- fence active owners/tasks;
- invalidate consumers/watches;
- schedule data/index/consumer/watch cleanup;
- preserve data required by active protocol forks according to the pinned Durable Streams soft-delete rules;
- block unsafe immediate name reuse.

Raw `DELETE /v1/stream/{name}` maps to the same collection delete and returns protocol-defined status codes.

Recreation policy MUST conform to pinned protocol expectations and the product's incarnation-isolation requirements. Existing code may permit recreation after full deletion; if the pinned protocol says paths should remain blocked, the protocol baseline and product policy must be reconciled explicitly rather than accidentally diverging.

---

## 9. Expiry lifecycle

Product metadata uses:

```text
expiry.idle
expiry.at
```

Raw protocol uses:

```text
Stream-TTL
Stream-Expires-At
```

On expiry:

- direct operations return the documented missing/gone result;
- active live reads terminate;
- consumers/watches are tombstoned;
- forks/reference retention rules are honored;
- cleanup is asynchronous and idempotent;
- usage/billing emits one final lifecycle event through the durable outbox when implemented.

---

## 10. List operation

Product list:

```text
GET /v1/streams?cursor=...&limit=...
```

Response is paginated and does not scan one descriptor GET per stream. Use the registry's compact catalog/index.

```json
{
  "streams": [
    {
      "name": "orders",
      "contentType": "application/json",
      "sealed": false,
      "createdAt": "..."
    }
  ],
  "cursor": "..."
}
```

Listing is a product management API, not part of the core Durable Streams stream URL contract.

---

## 11. Error format

Product JSON errors use one stable shape:

```json
{
  "error": {
    "code": "producer_gap",
    "message": "producer sequence gap",
    "details": {
      "expected": 4,
      "received": 6
    },
    "retryable": false
  }
}
```

Rules:

- code is stable machine-readable snake_case;
- message is human-readable and non-authoritative;
- details contain structured values;
- retryable is explicit;
- `Retry-After` is included where appropriate;
- secrets, raw encryption keys, and raw routing keys are never logged/returned unnecessarily.

The raw Durable Streams standards endpoint retains protocol-defined statuses/headers. It may use the same error body only where the protocol allows clients not to depend on it.

---

## 12. SDK final surface

```ts
interface StreamsClientOptions {
  url: string
  token?: string
  fetch?: typeof fetch
}

interface StreamHandleOptions {
  encryptionKey: string
}

class StreamsClient {
  stream<T>(name: string, options: StreamHandleOptions): Stream<T>
  createStream<T>(name: string, options: CreateStreamOptions): Promise<Stream<T>>
  listStreams(options?: ListOptions): AsyncIterable<StreamMetadata>
}

class Stream<T> {
  append(value: T, options?: { routingKey?: string }): Promise<AppendResult>
  appendMany(values: readonly T[], options?: { routingKey?: string }): Promise<AppendResult>
  producer(id: string, options: ProducerOptions): Producer<T>
  read(options?: ReadOptions): Promise<ReadPage<T>>
  subscribe(options?: SubscribeOptions): AsyncIterable<T>
  scan(options?: ScanOptions): AsyncIterable<ScanRecord<T>>
  consumer(name: string, config?: ConsumerConfig): Promise<Consumer<T>>
  watch(name: string, values: Record<string, unknown>): Watch
  metadata(): Promise<StreamMetadata>
  seal(options?: SealOptions<T>): Promise<void>
  delete(): Promise<void>
}
```

No public fields use unqualified `key`, `offset` for product cursors, `profile`, `ordering`, `segments`, `scaling`, `live`, or `close` flags.

---

## 13. Clean route switch

Register only the final route hierarchy.

The following current experimental surfaces are removed with no aliases or deprecation window:

```text
/v1/stream/{name}/queue/...
/v1/stream/{name}/touch/...
/segments on customer routes
legacy plural-route product headers
old wildcard subresource parsing
```

The singular base route `/v1/stream/{name}` remains because it is the pinned Durable Streams standards endpoint, not because it preserves an old Prisma product contract.

The implementation MUST NOT include redirect handlers, alias handlers, warning headers, sunset dates, or telemetry whose purpose is to support an update path. Requests to removed product routes return the normal unknown-route response.

## 14. Security

- token authorizes account/product operations;
- encryption key proves decrypt/append access according to product policy;
- routing keys are sensitive application metadata and are redacted or hashed in shared logs;
- watch signed URLs grant observation only;
- consumer claim/lease tokens are scoped and expiring;
- webhook/callback routes follow the pinned protocol's signature and SSRF requirements;
- production protocol/product traffic uses TLS;
- browser responses retain `nosniff` and appropriate cross-origin policy.

---

## 15. Durable Streams compliance

The explicit product hierarchy does not replace the raw stream URL.

The raw route continues to support all pinned protocol methods, headers, offsets, JSON semantics, live modes, caching, closure, expiry, producer coordination, forks, and reserved subscription routes.

The server MUST route reserved protocol paths before product wildcard names, and product cursors MUST never be accepted as protocol offsets.

---

## 16. Correctness invariants

1. Product and raw routes resolve to one collection incarnation.
2. Raw default-key bytes/messages remain byte-exact.
3. Product seal and raw close agree on one monotonic state.
4. Product delete and raw delete cannot race into two incarnations.
5. Encryption and routing credentials are never confused.
6. Canonical naming is stable and traversal-safe.
7. Reserved `__ds` cannot be shadowed by a customer name.
8. Product cursors are never mislabeled as protocol offsets.

---

## 17. Test plan

- all name canonicalization/path traversal cases;
- reserved `__ds` routing;
- removed product headers and query names are rejected;
- multi-stream client with distinct encryption keys;
- seal-only and final-append seal retry;
- consumer drain after seal;
- delete with active readers/consumers/watches;
- expiry and fork soft-delete interactions;
- raw/product route race on create/seal/delete;
- product error schema;
- official lifecycle/security/conformance suite unchanged.

---

## 18. Exit criteria

```text
public unqualified key option                         0
client-wide encryption key requirement                removed
public close/live/order/segment/scaling mode flags    0
reserved __ds shadowing                               impossible
raw Durable Streams lifecycle conformance             pass
product/raw incarnation divergence                    0
removed route/header aliases                         0

```
