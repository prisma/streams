# Stage 2 — Consumer Groups and Watches

**Goal:** turn queue delivery and state invalidation into independent resources available on every stream collection.

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

## 1. End state

Any Prisma stream may have:

- zero or more consumer groups;
- zero or more watch definitions;
- both at the same time;
- neither.

Neither changes the canonical record layout. Enqueue is append. Watch derivation happens only after the append is durable.

---

## 2. Consumer groups

### 2.1 Public model

```ts
const consumer = await stream.consumer("fulfilment", {
  visibilityTimeout: "30s",
  maxAttempts: 5,
  deadLetterStream: "orders-dlq",
})

const batch = await consumer.pull({ max: 10, wait: "20s" })

for (const message of batch.messages) {
  try {
    await process(message.value)
    message.ack()
  } catch {
    message.retry({ delay: "1s" })
  }
}

await batch.settle()
```

A consumer group is identified by:

```text
stream incarnation + consumer name
```

Names are UTF-8, normalized exactly as stream names, and immutable.

### 2.2 Configuration

```rust
struct ConsumerConfig {
    visibility_timeout_ms: u32,
    max_attempts: u32,
    dead_letter_stream: Option<String>,
    max_batch_records: u16,
}
```

Defaults:

```text
visibility_timeout = 30 seconds
max_attempts       = 5
max_batch_records  = 10
```

Configuration is idempotent:

- create with identical normalized config → `200 OK`;
- create new → `201 Created`;
- same name with different config → `409 Conflict`.

`maxAttempts` belongs to the consumer, not the stream.

### 2.3 Delivery guarantee

Consumer groups provide **at-least-once delivery**.

For one consumer and one routing key:

- records are delivered in routing-key order;
- at most one record or contiguous batch for that key may be actively leased;
- later records for the key are blocked until the earlier lease is acknowledged, retried, dead-lettered, or expires.

Different routing keys may be processed concurrently.

This is the only ordering behavior; there is no queue ordering mode.

### 2.4 Message identity

Public message IDs are opaque tokens:

```text
MessageId = encode(stream epoch, routing-key hash, segment lineage, segment offset)
```

Clients MUST NOT receive or depend on internal numeric offsets or segment IDs.

Response shape:

```ts
interface ConsumerMessage<T> {
  id: string
  routingKey: string
  attempts: number
  value: T
  ack(): void
  retry(options?: { delay?: Duration }): void
  extend(options: { visibility: Duration }): void
}
```

### 2.5 Pull and settle routes

```text
PUT    /v1/streams/{name}/consumers/{consumer}
GET    /v1/streams/{name}/consumers/{consumer}
DELETE /v1/streams/{name}/consumers/{consumer}

POST   /v1/streams/{name}/consumers/{consumer}:pull
POST   /v1/streams/{name}/consumers/{consumer}:settle
```

Pull request:

```json
{
  "max": 10,
  "waitMs": 20000,
  "visibilityMs": 30000
}
```

Settle request:

```json
{
  "acks": [{ "leaseToken": "..." }],
  "retries": [{ "leaseToken": "...", "delayMs": 1000 }],
  "extends": [{ "leaseToken": "...", "visibilityMs": 30000 }]
}
```

A settle request is atomic as a consumer-state transition. Individual stale tokens are ignored and counted, not allowed to alter a newer lease generation.

### 2.6 Durable state

Consumer state is stored in the owning shard's serialized commit path:

```text
consumer config
contiguous acked cursor per segment lineage
out-of-order settled set above cursor
active leases keyed by message ID
retry visibility deadlines
attempt counts
DLQ completion marker
```

All state changes share the same shard WAL/group commit as stream operations. There is no per-consumer database, manifest, LIST loop, or object namespace.

### 2.7 Lease fencing

Every lease token contains or authenticates:

```text
consumer generation
message ID
lease generation
deadline
```

A settle/extend request MUST match the current generation and lease generation. A stale worker receives `409 FENCED` or a per-item stale result and cannot affect state.

The lease-generation model SHOULD share primitives with the Durable Streams reserved subscription API, whose current protocol also defines generation fencing, wake IDs, claims, acknowledgements, releases, and lease expiry. The public Prisma consumer API remains message-oriented; the raw `__ds` subscription API remains protocol-oriented.

### 2.8 Dead-letter behavior

When delivery would exceed `maxAttempts`:

1. Append one DLQ record to `deadLetterStream` using an idempotency key derived from the original message ID and consumer ID.
2. Wait for that append to become durable.
3. Mark the source message settled.
4. Advance the contiguous cursor when possible.

DLQ payload:

```json
{
  "sourceStream": "orders",
  "consumer": "fulfilment",
  "messageId": "...",
  "routingKey": "customer-42",
  "attempts": 5,
  "value": { "...": "..." }
}
```

A crash between DLQ append and source settlement MUST not create duplicate DLQ messages.

### 2.9 Segment scaling and ownership movement

Consumer state is logical-stream state but physically partitioned with segments.

Rules:

- a sealed predecessor remains readable and drainable;
- its consumer state remains valid until its backlog is fully settled;
- successors do not deliver a routing key until the predecessor's same-key backlog is drained;
- ownership movement fences old workers through lease generations;
- consumer APIs never expose segment topology.

### 2.10 Backlog summaries

Maintain bounded summaries per consumer and segment:

```text
available
leased
delayed
oldest_available_age
keys_blocked_by_active_lease
DLQ count
```

Summaries are observability and scheduling hints; durable cursors and leases are authoritative.

---

## 3. Watches

### 3.1 Public model

Watch definitions are immutable stream configuration:

```ts
const stream = await client.createStream("orders", {
  ...,
  watches: [
    {
      name: "by-customer",
      fields: ["/customerId"],
    },
    {
      name: "by-store-and-status",
      fields: ["/storeId", "/status"],
    },
  ],
})

const watch = stream.watch("by-customer", {
  customerId: "customer-42",
})

for await (const invalidation of watch.subscribe({ from: "now" })) {
  await refreshQuery()
}
```

Watches require JSON streams because field extraction is defined over JSON messages.

### 3.2 Watch definition

```rust
struct WatchDefinition {
    name: String,
    fields: Vec<JsonPointer>,
}
```

Rules:

- names are unique per stream;
- field order is significant and persisted;
- pointers are validated at creation;
- definitions are immutable for one stream incarnation;
- changing the definition set requires deleting and recreating the pre-launch stream under a fresh incarnation; no in-place migration operation is implemented.

### 3.3 Watch-key derivation

For a definition and one JSON message:

```text
watchDefinitionId = hash(stream epoch, normalized definition)
watchKey          = hash(watchDefinitionId, canonical extracted values)
```

Canonical value encoding distinguishes strings, numbers, booleans, null, arrays, and objects. Missing fields produce no fine-grained key for that definition; the coarse stream key may still be touched.

### 3.4 Commit ordering

Watch journal ingestion occurs only after:

1. canonical record durability;
2. history/tail read visibility;
3. durable-state publication to readers.

An invalidation MUST never become visible before a rerun can observe the triggering record.

### 3.5 Watch routes

Public management:

```text
GET /v1/streams/{name}/watches
GET /v1/streams/{name}/watches/{watchName}
```

Generated subscription URL:

```text
GET /v1/streams/{name}/watches/{watchName}/keys/{watchKey}
    ?cursor={cursor}
    &timeoutMs={timeout}
    &sig={signature}
```

The SDK constructs this URL. Users do not manipulate touch tokens, template IDs, journal epochs, or HMAC keys.

Response:

```json
{
  "invalidated": true,
  "reason": "changed",
  "cursor": "...",
  "streamCursor": "..."
}
```

Timeout:

```json
{
  "invalidated": false,
  "cursor": "...",
  "streamCursor": "..."
}
```

A stale cursor returns an explicit `resync` result, never silent false.

### 3.6 Edge collapsing

Identical watch URLs and cursors may be collapsed at the edge. A successful head wake may use a very short cache window; timeouts remain `no-store`.

The URL signature is an observation capability only. It MUST NOT grant record decryption, append, consumer, or stream-management privileges.

### 3.7 Segment behavior

A fine-grained watch key is routed with the same routing-key hash model as the records it observes where possible. A coarse whole-stream watch fans out internally across active segments but presents one cursor to the SDK through a bounded aggregate journal.

No user selects per-segment watch behavior.

---

## 4. Raw Durable Streams subscriptions

The server MUST reserve and route the pinned protocol's `__ds` control namespace before application stream paths.

If the pinned protocol baseline includes subscriptions, implement its webhook and pull-wake resources unchanged on the Durable Streams standards surface. Prisma consumer groups do not replace or reinterpret these routes.

Shared internal primitives MAY include:

- generation fencing;
- lease expiry;
- durable cursors;
- wake deduplication;
- signed callback tokens.

The two public contracts remain distinct:

- Durable Streams subscriptions wake workers about pending stream cursors.
- Prisma consumer groups lease and settle individual records with retry/DLQ semantics.

---

## 5. Clean switch from the current profile implementation

There is no migration from queue or state profiles.

- Consumer groups are created only through the final named-consumer resource.
- Watch definitions are created only through the final typed stream configuration.
- Existing queue cursors, leases, retries, DLQ state, touch cursors, journal epochs, and capability material are discarded with the old namespace.
- Legacy `/queue/*` and `/touch/*` routes are not registered.
- The final server contains no `desc.profile` checks and no alias path that translates old operations.

## 6. Durable Streams compliance

- Canonical stream reads/appends are unchanged.
- Consumer and watch routes are additive Prisma resources under the plural product root.
- Reserved `__ds` routes are never interpreted as application stream names.
- Consumer cursors and watch cursors MUST NOT be returned as `Stream-Next-Offset` values.
- A generic Durable Streams client can ignore consumers and watches completely.
- Official subscription APIs, when part of the pinned baseline, retain their exact protocol semantics.

---

## 7. Correctness invariants

1. Every acknowledged source record is deliverable to every consumer until settled or retention makes it unavailable according to a documented policy.
2. At most one active lease exists per `(consumer, routing key)`.
3. Stale leases cannot ack, retry, extend, or DLQ a newer delivery.
4. A DLQ transition is idempotent across crashes.
5. Consumer cursor advancement never skips an unsettled record.
6. An invalidation never precedes record visibility.
7. Watch cursor gaps produce `resync`, not false negatives.
8. Consumer/watch state does not create new object-store databases or periodic LISTs.

---

## 8. Observability

Consumer metrics:

```text
consumer_available
consumer_leased
consumer_delayed
consumer_oldest_age_ms
consumer_pull_waiters
consumer_claim_conflicts
consumer_stale_settles
consumer_retries
consumer_dlq
consumer_state_bytes
```

Watch metrics:

```text
watch_definitions
watch_keys_touched
watch_waiters
watch_collapsed_waiters
watch_wakes
watch_timeouts
watch_resyncs
watch_journal_bytes
```

All cardinality-bearing maps expose current, peak, eviction, and byte estimates.

---

## 9. Test plan

### Consumer tests

- per-key FIFO with interleaved keys;
- parallel delivery across keys;
- visibility expiry and redelivery;
- stale token fencing;
- out-of-order acks and contiguous cursor advancement;
- crash at every DLQ transition boundary;
- split/merge and owner movement while leased;
- retention interaction;
- 100k routing keys with bounded memory.

### Watch tests

- extracted key determinism;
- missing/null/type distinctions;
- invalidation after durability only;
- edge-collapse cohort wake;
- stale cursor resync;
- split/merge and owner movement;
- wrong signature and replay rejection.

### DST

Inject crashes after:

```text
lease publication
handler response before settle
DLQ append before source settlement
watch journal publication
segment seal before successor publication
owner fencing while waiters are blocked
```

### Protocol conformance

Run the official base suite and any pinned subscription suite unchanged.

---

## 10. Exit criteria

```text
queue profile branches                          0
state-protocol profile branches                 0
per-key consumer FIFO corpus                    pass
DLQ crash matrix                                pass
watch visibility-before-invalidation invariant pass
official Durable Streams conformance            pass
new object-store DBs/namespaces per consumer    0
new object-store DBs/namespaces per watch       0

```
