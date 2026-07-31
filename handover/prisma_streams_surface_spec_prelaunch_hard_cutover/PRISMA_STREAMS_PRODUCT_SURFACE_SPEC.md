# Prisma Streams Unified Product-Surface Specification

**Consolidated implementation edition**  
**Delivery model:** eight implementation workstreams, one destructive pre-launch hard cutover.  
**Preserved contract:** the pinned Durable Streams standard on `/v1/stream/{name}`.

This document intentionally provides **no update path** from the current experimental Prisma Streams implementation. There are no legacy decoders, aliases, dual layouts, migration jobs, rolling old/new versions, or data conversion. Implement the final server and SDK against a fresh object-store namespace, pass every conformance and product gate, then replace the old pre-launch environment.

The individual stage files are authoritative implementation units; this document is generated from them.

---

## Contents

1. Overview, standards boundary, and clean-switch policy
2. Stage 1 — Remove profiles
3. Stage 2 — Consumer groups and watches
4. Stage 3 — Unified routing, automatic scaling, and compact postings
5. Stage 4 — `append` and `appendMany`
6. Stage 5 — Producer sessions
7. Stage 6 — `read`, `subscribe`, and `scan`
8. Stage 7 — Typed creation document
9. Stage 8 — Naming, lifecycle, and routes
10. Durable Streams conformance matrix and pre-launch hard-cutover plan

---

<!-- BEGIN 00-OVERVIEW.md -->

# Prisma Streams Product-Surface Simplification

**Status:** implementation specification  
**Target:** `prisma/streams` on the SlateDB architecture  
**Scope:** eight implementation workstreams delivered together as one destructive pre-launch switch while preserving the Durable Streams protocol  
**Normative language:** `MUST`, `MUST NOT`, `SHOULD`, and `MAY` are used as in RFC 2119/8174.

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

## 1. Executive decision

Prisma Streams will expose one coherent product model:

> A Prisma Stream is an encrypted, append-only collection of records. Records are ordered by routing key, partitions scale automatically, and consumers resume with opaque cursors.

Users choose:

- a stream name;
- a content format;
- an optional routing key on each append/read;
- optional consumer groups and watch definitions;
- optional lifecycle settings.

Users do **not** choose:

- a profile;
- total versus per-key ordering;
- a segment count;
- whether scaling is enabled;
- an index format;
- a live transport;
- a separate queue storage mode.

The implementation retains one compact postings-index strategy, one automatic segmentation strategy, one producer-session abstraction, and one set of lifecycle operations.

---

## 2. Durable Streams compliance boundary

The Durable Streams protocol defines a URL-addressable append-only byte sequence with a strict offset order. Its extensions must be additive and must not break base behavior. A key-partitioned collection with no cross-key total order is therefore **not itself one Durable Stream URL**.

To remain honest and conformant, the server exposes two layers:

### 2.1 Durable Streams standards surface

```text
/v1/stream/{name}
```

This URL remains a fully conforming Durable Stream. It represents the Prisma stream's **default routing key** (`""`) and therefore has one strict byte/message order.

It MUST continue to support, according to the pinned Durable Streams protocol version:

- idempotent `PUT` creation;
- `POST` append;
- raw byte mode and JSON mode, including one-level JSON array flattening;
- opaque, lexicographically sortable offsets;
- catch-up reads;
- long-poll and SSE;
- `HEAD` metadata;
- close and atomic append-and-close;
- delete;
- `Stream-TTL` and `Stream-Expires-At`;
- `Stream-Seq` as required by the pinned Durable Streams baseline;
- idempotent producer headers;
- ETag and cache behavior;
- fork behavior when supported by the pinned protocol baseline;
- reserved `__ds` subscription routes when supported by the pinned baseline.

A generic Durable Streams client that knows nothing about Prisma extensions MUST continue to work against this URL.

### 2.2 Prisma product surface

```text
/v1/streams/{name}
```

This is a Prisma **stream collection**, not a claim that all routing keys form one globally ordered Durable Stream. It adds:

- routing-key appends and reads;
- automatic segmentation;
- collection scans;
- consumer groups;
- watches;
- typed creation configuration;
- product-oriented lifecycle operations.

The product SDK uses this surface. The singular route remains available solely as the pinned Durable Streams standards implementation and as a direct integration surface for Durable Streams clients.

### 2.3 Shared storage model

The two surfaces share one logical stream descriptor and one storage engine:

- appends through `/v1/stream/{name}` target routing key `""`;
- product appends without `routingKey` also target `""`;
- product appends with a routing key target that key's independently ordered sequence;
- sealing or deleting the collection affects every routing key and is reflected by the default-key Durable Stream view.

The distinct URLs prevent Prisma's routing extension from silently changing the byte sequence represented by a conforming Durable Stream URL.

---

## 3. Protocol-version discipline

The Durable Streams protocol is evolving. The repository MUST pin:

```text
DURABLE_STREAMS_PROTOCOL_COMMIT
DURABLE_STREAMS_CONFORMANCE_PACKAGE_VERSION
```

Every release note MUST state both values.

A protocol-upgrade PR MUST be separate from a Prisma product-surface PR. This prevents a conformance failure from being ambiguously attributed to both upstream evolution and local product changes.

Every stage in this specification has two mandatory CI jobs:

```text
1. Official Durable Streams server conformance suite, unchanged
2. Prisma extension conformance suite
```

The first runs against `/v1/stream/{name}`. The second runs against `/v1/streams/{name}` and the SDK.

---

## 4. Terms

### Prisma stream collection

The user-facing resource identified by a stream name. It owns configuration, encryption identity, routing-key sequences, segment lineage, consumers, watches, lifecycle, and usage accounting.

### Default routing key

The empty byte string. It is used when `routingKey` is omitted. The Durable Streams standards URL exposes exactly this sequence.

### Routing-key sequence

The ordered record sequence for one routing key. Ordering is guaranteed only inside this sequence.

### Segment

An internal range of routing-key hashes. Segments are implementation details and never customer-selected.

### Protocol offset

An opaque offset returned by the Durable Streams standards surface. It applies to the default-key sequence.

### Key cursor

An opaque Prisma cursor for one routing-key sequence. It may cross internal segment lineage while preserving per-key order.

### Scan cursor

An opaque Prisma cursor for a deterministic, snapshot-bounded cross-key scan. It is not a Durable Streams offset and MUST use a distinct response field/header.

### Consumer group

Durable delivery state layered on a stream collection. It does not change how records are stored.

### Watch

A durable invalidation definition that derives watch keys from committed JSON records. It does not change the stream's primary storage model.

---

## 5. Cross-stage invariants

Every stage MUST preserve these invariants.

### 5.1 Durability

Once an append is acknowledged, its canonical record, producer state, routing metadata, and any required index entries survive process loss and ownership transfer.

### 5.2 Canonical storage

Each record payload is stored once in the canonical history keyspace. Secondary indexes contain offsets or compact metadata, never a second payload copy.

### 5.3 Per-key order

For any stream incarnation and routing key, acknowledged records are returned exactly once and in append order.

### 5.4 No invented global order

The product API MUST NOT imply a total order across routing keys. Cross-key scans are explicitly deterministic traversals of a bounded snapshot, not append-order streams.

### 5.5 Automatic scaling

Every collection starts with one internal segment and automatically splits or merges. A single hot key is identified as unsplittable and throttled; it is not repeatedly split.

### 5.6 Opaque cursors

Clients MUST treat protocol offsets, key cursors, and scan cursors as opaque. The three token classes MUST be distinguishable and rejected on the wrong endpoint.

### 5.7 Standards preservation

The raw Durable Streams standards endpoint MUST implement protocol-defined behavior even when the product SDK offers a simpler abstraction. In particular, the product API must not remove raw support for JSON flattening, `Stream-Seq`, producer headers, live modes, close, TTL/expiry, or fork/subscription capabilities required by the pinned baseline.

### 5.8 Cost discipline

No simplification may add a per-routing-key database, manifest, object namespace, LIST loop, or background poller. New metadata MUST share existing shard/history batches and be bounded by bytes and cardinality.

### 5.9 Bounded resources

Every per-stream, per-key, per-consumer, and per-watch in-memory structure MUST have an explicit byte/cardinality bound, an eviction or persistence strategy, and observability.

### 5.10 Deterministic testing

Every new state transition MUST have:

- an invariant;
- a mechanism-coverage counter;
- a crash/fault scenario;
- a liveness recovery scenario;
- a physical-request budget where object storage is involved.

---

## 6. Stage map

| Stage | Specification | Primary outcome |
|---|---|---|
| 1 | Remove profiles | One stream primitive; adapters and capabilities replace mutually exclusive types |
| 2 | Consumer groups and watches | Queue and invalidation behavior become orthogonal resources |
| 3 | Unified routing, scaling, and postings | Per-key order, automatic scaling, one compact index implementation |
| 4 | `append` and `appendMany` | Payload shape no longer changes operation meaning in the SDK/product API |
| 5 | Producer sessions | One public writer-coordination abstraction; pinned raw protocol semantics retained |
| 6 | `read`, `subscribe`, and `scan` | Operations replace live/read mode flags; no fake global order |
| 7 | Typed creation document | Immutable product configuration becomes one idempotent JSON contract |
| 8 | Naming, lifecycle, and routes | Clear terminology and explicit resource operations |

Stages are ordered to keep implementation dependencies understandable. They are not separately supported releases and MUST NOT be hidden behind compatibility adapters. The final branch switches server, SDK, routes, descriptors, and storage layout together against a fresh namespace.

---

## 7. Final SDK target

```ts
const client = new StreamsClient({
  url,
  token,
})

const orders = await client.createStream("orders", {
  encryptionKey,
  format: { kind: "json" },
  expiry: { idle: "30d" },
  watches: [
    { name: "by-customer", fields: ["customerId"] },
  ],
})

const producer = orders.producer("checkout", {
  state: producerStateStore,
})

await producer.append(order, {
  routingKey: order.customerId,
})

await producer.appendMany(ordersForOneCustomer, {
  routingKey: customerId,
})

const page = await orders.read({
  routingKey: customerId,
  from: "beginning",
})

for await (const order of orders.subscribe({
  routingKey: customerId,
  from: page.cursor,
})) {
  // ordered for this customer
}

for await (const record of orders.scan()) {
  // deterministic snapshot traversal; no cross-key order promise
}

const workers = await orders.consumer("fulfilment", {
  visibilityTimeout: "30s",
  maxAttempts: 5,
  deadLetterStream: "orders-dlq",
})

for await (const message of workers) {
  await process(message.value)
  message.ack()
}

await orders.seal()
```

The pinned Durable Streams standards route remains available underneath this SDK; no current Prisma product API or data-layout compatibility is retained.

---

## 8. Required documentation outcome

After all stages, public documentation has three layers only:

1. **Streams:** append, read, subscribe, scan, lifecycle.
2. **Consumers and watches:** orthogonal processing/notification capabilities.
3. **Typed adapters:** logs, metrics, traces, state, CRDTs, and other higher-level packages.

Segments, profiles, index layouts, touch journals, queue storage keys, and live transports are implementation details.

<!-- END 00-OVERVIEW.md -->

---

<!-- BEGIN 01-REMOVE-PROFILES.md -->

# Stage 1 — Remove Profiles

**Goal:** replace mutually exclusive stream profiles with one storage primitive plus orthogonal resources and typed adapters.

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

## 1. Problem

The current descriptor contains a `profile` field and core request paths branch on values such as:

```text
generic
queue
state-protocol
evlog
metrics
otel-traces
```

These values represent different categories:

- `queue` is delivery state;
- `state-protocol` is an invalidation capability;
- logs, metrics, and traces are record schemas and derived views;
- `generic` is simply the absence of an adapter.

Mutual exclusion prevents valid combinations and causes core storage code to accumulate product-specific branches.

---

## 2. End state

A stream descriptor contains only properties that affect the durable stream itself:

```rust
struct StreamDescVNext {
    name: String,
    stream_epoch: StreamEpoch,
    key_fingerprint: KeyFingerprint,
    created_ms: i64,

    content_type: String,
    idle_ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    deleted: bool,
    sealed: bool,

    segment_map: SegmentMap,
    watch_definitions: Vec<WatchDefinition>,

    layout_version: u32,
}
```

It MUST NOT contain:

```text
profile
queue_max_deliveries
ordering
segment_count
scaling
adapter kind
```

Consumer groups are separate resources. Watch definitions are orthogonal immutable configuration. Logs, metrics, traces, Durable State, Automerge, and other domain products are SDK/adapter packages that append ordinary records and maintain optional derived views.

---

## 3. Public API

Remove from the product SDK:

```ts
profile: "generic" | "queue" | "state-protocol" | ...
```

Replace profile-specific creation with composition:

```ts
const stream = await client.createStream("orders", config)
const consumer = await stream.consumer("fulfilment", consumerConfig)
const watch = stream.watch("by-customer", params)
```

Typed products become packages or namespaces:

```ts
client.logs("application")
client.metrics("application")
client.traces("application")
client.state("application")
```

Those packages MAY provision streams, schemas, watches, consumers, and materialized views. They MUST NOT require a core storage profile.

---

## 4. Server architecture

### 4.1 Core hooks

The core may retain generic hook interfaces, but they MUST be capability-based rather than profile-dispatched:

```rust
trait CommitObserver {
    fn on_durable_batch(&self, batch: &DurableBatch);
}

trait DerivedView {
    fn ingest(&self, batch: &DurableBatch) -> Result<()>;
}
```

Registration is driven by explicit resources:

- a stream with watch definitions registers the watch journal observer;
- a consumer group registers consumer-state resources;
- an installed adapter may register a derived view by adapter ID outside the stream descriptor.

Core code MUST NOT contain logic equivalent to:

```rust
if desc.profile == "queue" { ... }
```

### 4.2 Adapter catalog

If Prisma needs to remember that a stream was created by a logs/metrics/traces adapter, store this in a separate non-authoritative catalog:

```text
adapters/<stream-incarnation>/<adapter-id>.json
```

The catalog is for tooling and lifecycle orchestration. Losing it MUST NOT make primary stream records unreadable. Adapter state MUST be rebuildable from durable stream records and immutable configuration.

### 4.3 Storage

Removing profiles MUST NOT change canonical record keys, WAL semantics, history-v2 layout, encryption, or absorbed boundaries.

---

## 5. Clean switch implementation

Implement the final descriptor and core directly:

1. Remove `profile`, `queue_max_deliveries`, touch-profile fields, and all profile-specific branches from the descriptor, HTTP layer, committer, registry, and SDK.
2. Define consumer groups and watch definitions only through the new resources in Stage 2.
3. Keep logs, metrics, and traces as adapters over ordinary streams; do not persist adapter names in the core descriptor.
4. Start with a fresh storage namespace containing only the final descriptor shape.
5. Reject any descriptor carrying the removed profile schema as `unsupported_storage_layout`; do not decode, translate, or rewrite it.
6. Delete legacy queue/touch routes and old profile test fixtures.

No record, queue state, touch journal, descriptor, or capability material from the current implementation is migrated.

## 6. Removed surface

The final product API does not accept `Stream-Profile` or any equivalent profile field.

- `Stream-Profile` on the Prisma product route returns `400 unknown_field` or `400 obsolete_field`.
- No `legacy-default` consumer is synthesized.
- No `/queue/*` or `/touch/*` alias is installed.
- No deprecation, sunset, translation, or legacy-usage telemetry is implemented.
- The singular Durable Streams standards route implements only the pinned Durable Streams protocol and no legacy Prisma product contract.

## 7. Durable Streams compliance

This stage MUST NOT alter the Durable Streams standards URL's behavior.

Specifically:

- `Content-Type` remains stream configuration.
- Idempotent `PUT` comparison continues to include protocol-defined configuration.
- JSON mode, offsets, live reads, closure, TTL/expiry, producer headers, and cache headers are unchanged.
- Extra adapter metadata is never required by a generic Durable Streams client.

The official conformance suite MUST pass unchanged on a fresh namespace with the final implementation.

---

## 8. Correctness invariants

1. The final implementation contains no descriptor or storage branch keyed by a profile name.
2. Consumer and watch state is created only in the final Stage 2 format.
3. A watch invalidation is never emitted before the corresponding record is durable and readable.
4. Derived products can rebuild their state from durable records and immutable configuration.
5. A stream may have consumers and watches simultaneously.
6. No core storage branch depends on an adapter/profile name.

---

## 9. Observability

Add:

```text
streams_with_consumers
streams_with_watches
adapter_catalog_entries
profile_branch_executions  // compile-time/test assertion; must be zero
obsolete_profile_requests  // optional request rejection counter, not a translator
```

A release gate requires `profile_branch_executions == 0` under the full product test suite.

## 10. Test plan

### Unit

- Final descriptor serialization contains no profile fields.
- Removed profile fields are rejected by the product configuration parser.
- Core modules expose no profile enum or profile-name dispatch.

### Integration

- A stream can have consumers and watches simultaneously.
- Queue semantics operate through a consumer group on an ordinary stream.
- Watches operate through immutable watch definitions on an ordinary stream.
- Logs, metrics, and traces adapters use ordinary streams.
- Removed `/queue/*`, `/touch/*`, and profile-creation inputs are absent or rejected.

### DST

Inject crashes around creation and use of final consumer/watch resources. Recovery MUST converge without duplicate state or profile-dependent behavior.

### Conformance

Run the pinned official suite unchanged against `/v1/stream/{name}` on a fresh namespace.

## 11. Clean cutover

1. Implement the final descriptor and Stage 2 resources.
2. Delete profile decoding, translation, aliases, and old fixtures in the same branch.
3. Update the SDK and documentation to emit only the final surface.
4. Deploy to a fresh bucket or `PATH_PREFIX`; do not attach the new binary to an old namespace.
5. Run Durable Streams conformance, Prisma product conformance, DST, and cost gates.
6. Delete the previous pre-launch environment after the new environment passes.

## 12. Exit criteria

Stage 1 is complete when:

```text
final descriptors containing profile fields        0
core branches on profile names                     0
profile translators or legacy decoders             0
legacy queue/touch route aliases                    0
official Durable Streams conformance                pass
consumer + watch composition test                   pass
fresh-namespace destructive cutover test            pass
```

<!-- END 01-REMOVE-PROFILES.md -->

---

<!-- BEGIN 02-CONSUMERS-AND-WATCHES.md -->

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

<!-- END 02-CONSUMERS-AND-WATCHES.md -->

---

<!-- BEGIN 03-UNIFIED-ROUTING-SCALING-POSTINGS.md -->

# Stage 3 — Unified Routing, Automatic Scaling, and Compact Postings

**Goal:** replace all ordering/scaling choices and the full-frame routing-key index with one implementation that is efficient for both sparse and dense keyed workloads.

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

## 1. Executive decision

Every Prisma stream collection is internally key-partitioned.

1. Every append has one routing key.
2. An omitted routing key means the empty/default key.
3. Ordering is guaranteed per routing key only.
4. Streams begin with one segment and split or merge automatically.
5. A single hot key is unsplittable and is throttled rather than repeatedly split.
6. The encrypted frame is stored exactly once.
7. Keyed history uses one immutable compact postings index.
8. A large, decoded, single-flight postings cache and bounded canonical-range planner prevent point-read amplification.
9. There are no index modes, ordering modes, static segment counts, or scaling flags.

---

## 2. Durable Streams standards boundary

A Durable Stream URL represents one strictly ordered byte/message sequence. The key-partitioned collection has no cross-key total order and MUST NOT be presented as one protocol stream.

Therefore:

```text
/v1/stream/{name}
```

is the standards-conformant **default-key sequence**.

```text
/v1/streams/{name}
```

is the Prisma collection resource.

Consequences:

- raw Durable Streams `POST`/`GET` without Prisma routing extensions operate on key `""`;
- product appends with a non-empty `routingKey` use the collection route;
- a product read for one routing key returns a Prisma key cursor;
- a cross-key scan returns a Prisma scan cursor;
- neither key cursors nor scan cursors are protocol offsets;
- official conformance runs only against the singular Durable Streams standards route.

This preserves the protocol's strict-order and byte-exact-resumption requirements without imposing a global sequencer on all keys.

---

## 3. Routing-key model

### 3.1 Types

```rust
struct RoutingKey(Bytes);
struct RoutingKeyHash([u8; 16]);
struct RoutePoint(u64);
struct RouteHash([u8; 16]);
struct SegmentHash([u8; 16]);
struct StreamEpoch([u8; 16]);
```

These types MUST NOT be interchangeable at module boundaries.

### 3.2 Normalization

Routing keys are byte strings.

Product SDK string keys are UTF-8 encoded exactly. No Unicode normalization, trimming, lowercasing, or case folding occurs.

Limits:

```text
maximum routing-key bytes = 1,024
empty key                = valid default key
```

Hash:

```text
RoutingKeyHash = SHA-256(key bytes)[0..16]
RoutePoint     = big-endian first 8 bytes of SHA-256(key bytes)
```

### 3.3 Ordering

For one stream incarnation and routing key:

> acknowledged records are returned exactly once in append order.

No order is promised between distinct routing keys.

If every append uses the default key, the collection has one logical sequence and behaves as a totally ordered stream.

### 3.4 Close semantics

Collection sealing is global:

- once sealed, no routing key accepts appends;
- all key reads eventually report sealed after reaching their key tail;
- the protocol default-key view reports `Stream-Closed: true` according to the Durable Streams protocol.

There is no per-key close mode.

---

## 4. Descriptor and segment map

### 4.1 Parent descriptor

```rust
struct StreamDescVNext {
    // identity and protocol config
    name: String,
    stream_epoch: StreamEpoch,
    key_fingerprint: KeyFingerprint,
    content_type: String,
    idle_ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    sealed: bool,
    deleted: bool,

    // unified routing
    segment_map: SegmentMap,

    // orthogonal configuration
    watch_definitions: Vec<WatchDefinition>,
    layout_version: u32,
}
```

Remove:

```text
ordering
segment_count
scaling
```

The initial map is embedded in the same descriptor PUT, so stream creation adds no segment-map object request.

### 4.2 Segment descriptor

```rust
struct SegmentDesc {
    id: u32,
    lo_inclusive: u64,
    hi_exclusive: Option<u64>, // None = 2^64

    route_hash: RouteHash,
    segment_hash: SegmentHash,

    created_ms: i64,
    sealed_ms: Option<i64>,
    sealed_next_offset: Option<u64>,

    predecessors: SmallVec<[u32; 2]>,
    successors: SmallVec<[u32; 2]>,
}
```

The map MUST be a complete, non-overlapping partition of the 64-bit routing space for live segments.

### 4.3 No child registry streams

Internal segments MUST NOT create registry descriptors such as `<parent>#segN`.

The parent descriptor owns configuration. Segment storage identity is deterministic from stream epoch and segment ID. Internal routing calls the shard engine directly; it never recursively invokes the HTTP append handler.

### 4.4 CAS updates

Split and merge map changes use descriptor ETag/CAS. A transition intent is persisted before any non-atomic external step and is idempotently resumable.

---

## 5. Automatic scaling

### 5.1 Signals

Maintain bounded per-segment EWMAs:

```rust
struct SegmentLoad {
    requests_per_sec: Ewma,
    records_per_sec: Ewma,
    bytes_per_sec: Ewma,
    key_distribution: KeyDistribution,
}

struct KeyDistribution {
    bins: [Ewma; 64],
    heavy_hitters: SpaceSaving<8>,
    distinct_estimate: Hll64,
}
```

No unbounded per-key load map is permitted.

### 5.2 Split eligibility

A segment may split only when all are true:

1. a service-limit dimension is hot for the configured sustained interval;
2. cooldown has elapsed;
3. at least two distinct keys have material load;
4. a split point predicts at least 15% of recent load in each child;
5. maximum segment count has not been reached;
6. memory and fleet placement can admit successors.

Choose the recent load-weighted median bin/split point, not the numeric midpoint.

### 5.3 Hot-key behavior

If one routing key dominates and no effective split exists:

- set `hot_key = true`;
- do not split;
- enforce the per-key request/record/byte limit;
- expose the key hash and load share in operator telemetry, never raw customer key bytes;
- clear the state after a sustained cool interval.

### 5.4 Seal-before-successor

Split sequence:

1. Persist split intent.
2. Serialize a seal through predecessor committer.
3. Persist predecessor terminal offset.
4. Create successor storage identities.
5. CAS-publish successor map.
6. Route new appends to successors.
7. Retain predecessor for reads, consumers, producer-state lookup, and GC safety.

A crash at every boundary is idempotently recoverable.

### 5.5 Merge

Adjacent cold segments may merge after cooldown:

1. persist merge intent;
2. seal both predecessors;
3. publish one successor covering the union;
4. route new appends to successor;
5. preserve predecessor history until no reader/consumer/fork reference requires it.

No historical data moves during split or merge.

### 5.6 Placement

`route_hash` determines physical shard ownership. New successors are assigned route hashes that distribute load while preserving the route-first history layout and supporting future shard-range splits.

---

## 6. Split-safe producer and sequence state

### 6.1 Product producer scope

For product appends, idempotent producer state is scoped to:

```text
stream incarnation + routing key + producer ID
```

Sequence numbers are per HTTP request/batch.

### 6.2 Predecessor lookup

After a split, a successor first checks local producer state. On miss, it resolves state along the routing key's predecessor lineage.

The resolved state is seeded atomically with the successor's first accepted append.

### 6.3 Ambiguous retry invariant

If an attempt committed on the sealed predecessor but the response was lost, retrying the same producer tuple against the successor MUST:

- return deduplicated success;
- return the original logical result/cursor where available;
- consume no new offset;
- write no duplicate record.

### 6.4 Raw `Stream-Seq`

The raw protocol endpoint retains protocol-defined `Stream-Seq` semantics for the default-key Durable Stream view. The product SDK does not expose it.

---

## 7. Canonical history layout

### 7.1 One payload copy

Canonical history row:

```text
<route16><segment16>'r'<offset_be_u64>
    -> encrypted/compressed frame
```

The frame is the only payload copy and source of truth.

### 7.2 Postings row

Every routing key, including the default key, has compact postings.

Fixed bucket:

```text
POSTINGS_BUCKET_OFFSETS = 65,536
bucket = offset / POSTINGS_BUCKET_OFFSETS
```

Key:

```text
<route16>
<segment16>
'p'
<routing_key_hash16>
<bucket_be_u64>
<page_first_offset_be_u64>
    -> PostingsPageV1
```

No user-selectable index mode exists.

### 7.3 Page codec

```rust
struct PostingsPageV1 {
    version: u8,
    codec: u8,

    first_offset: u64,
    last_offset_exclusive: u64,

    run_count: u32,
    matching_frame_bytes: u64,

    encoded_runs: Bytes,
}

struct PostingRun {
    gap_offsets: u64,
    record_count: u32,
    matching_frame_bytes: u64,
    gap_frame_bytes_before: u64,
}
```

Runs use unsigned varints. The page is deterministically compressed only when compression wins.

Format limits:

```text
max encoded page bytes  = 32 KiB
bucket offsets          = 65,536
builder decoded memory  = 8 MiB
```

### 7.4 Write path

During history-v2 absorption:

1. Copy each encrypted frame to its canonical row.
2. Decode only frame metadata.
3. Group consecutive matching offsets by key hash and bucket.
4. Build immutable postings pages.
5. Add canonical rows and postings pages to the same history `WriteBatch`.
6. Include exact/upper-bound key, value, and operation overhead in the existing aggregate gather budget.
7. Flush once.
8. Publish absorbed boundaries only after flush durability.

Postings MUST NOT introduce:

- another database;
- another manifest;
- another flush;
- another object namespace;
- another LIST or GC loop.

### 7.5 Idempotence

Page keys and bytes are deterministic for a given absorbed range. Replaying a pass before boundary publication overwrites equivalent values and cannot duplicate logical results.

### 7.6 Collision safety

`RoutingKeyHash` is a lookup accelerator only. Every fetched canonical frame is checked against the exact requested routing-key bytes. A collision can add candidates but cannot return another key's record.

---

## 8. Large-section postings cache

### 8.1 Ownership

Each shard engine participates in one process-wide weighted cache budget:

```rust
struct PostingsCache {
    entries: WeightedLru<PostingsSliceKey, Arc<PostingsSlice>>,
    in_flight: HashMap<PostingsSliceKey, SharedLoad>,
    max_decoded_bytes: usize,
}
```

Default initial budget:

```text
16 MiB per process, configurable by deployment class
idle eviction = 10 minutes
```

A per-shard fixed allocation is forbidden because shard count varies.

### 8.2 Slice key

```rust
struct PostingsSliceKey {
    stream_epoch: StreamEpoch,
    segment: SegmentHash,
    key_hash: RoutingKeyHash,
    first_bucket: u64,
}
```

### 8.3 Slice contents

```rust
struct PostingsSlice {
    first_bucket: u64,
    last_bucket_exclusive: u64,
    indexed_to_offset: u64,
    runs: Arc<[DecodedRun]>,
    encoded_bytes_read: usize,
    decoded_bytes: usize,
}
```

### 8.4 Cold-load window

A cold read intentionally loads a large forward section:

```text
up to 64 offset buckets
or 1 MiB encoded postings
or requested absorbed boundary
whichever comes first
```

This is expected to cover many subsequent pages for a key active during a five-minute window.

### 8.5 Single-flight and cancellation

Loads are single-flight and cache-owned. Cancellation of all request waiters does not cancel the storage scan or prevent the completed slice from entering the cache.

### 8.6 Incremental extension

Postings below the absorbed boundary are immutable. If a later read needs a greater boundary, load only the missing forward range and atomically extend/replace the slice. Never discard already covered buckets solely because absorption advanced.

### 8.7 Read-ahead

Cold index scan settings:

```text
read_ahead_bytes = 1–2 MiB
max_fetch_tasks  = 2
```

When a request consumes 75% of a cached slice and memory pressure is low, prefetch the next slice asynchronously. Prefetch never delays the response.

### 8.8 Negative cache

A slice records proven empty bucket ranges. Repeated reads of a key with no postings in that range are cache hits and do not rescan object storage until the absorbed boundary extends past the proof.

---

## 9. Canonical range planner

### 9.1 Goal

Resolve postings to canonical frames without one GET per offset.

### 9.2 Inputs

```rust
struct CanonicalSpan {
    from: u64,
    to: u64,
    estimated_scan_bytes: u64,
    expected_matching_bytes: u64,
}
```

### 9.3 Planning

1. Start with one span per contiguous run.
2. Compute gap byte costs from postings metadata.
3. Merge cheapest neighboring spans while within limits.
4. Prefer one envelope scan when cheaper than multiple remote scans.
5. Stop once enough expected matching bytes exist to fill the response.

Initial hard limits:

```text
max spans per response       = 8
max concurrent scans         = 4
target read amplification    = 2x
hard read amplification      = 4x
max coalesced gap            = 64 KiB
max canonical scan bytes     = 16 MiB
```

These are safety constants and tunables, not product modes.

### 9.4 No point-read explosion

Singleton offsets are coalesced, included among the bounded spans, or deferred to a later response. The implementation MUST NOT issue one object-store read per posting.

### 9.5 Execution and verification

Execute spans with bounded concurrency, preserve span order, decode frame headers, verify exact routing-key bytes, then decrypt and return matches in key order.

### 9.6 Progress

Internally track:

```rust
last_returned_offset: Option<u64>
consumed_to_offset: u64
```

A page may advance `consumed_to_offset` even when it returns no records because postings prove the range empty for that key.

When a request budget is exhausted, return a valid partial page and a key cursor at `consumed_to_offset`.

### 9.7 Corruption fallback

A missing/malformed postings page cannot produce `complete=true` over an unverified range.

The server may perform one bounded canonical envelope fallback for the affected bucket. If the fallback budget is exhausted, return a partial page and increment corruption metrics.

---

## 10. Key reads across segment lineage

For a key hash, derive every historical segment range that contained it.

A read:

1. starts at cursor segment/offset;
2. drains postings/canonical records through the segment's sealed offset;
3. follows the successor whose range contains the key;
4. begins successor offset zero;
5. repeats through the current live segment.

Sealed segment postings are immutable and cacheable indefinitely subject to memory eviction.

The key cursor encodes lineage position opaquely and remains valid across ownership movement.

---

## 11. Product API

Remove product configuration inputs:

```text
Stream-Ordering
Stream-Segments
Stream-Scaling
ordering
segments
scaling
```

Product append/read use `routingKey` in the typed API and a namespaced Prisma wire field defined in Stage 8.

The product metadata response may expose aggregate counts and health but not segment controls. Segment maps are operator/debug data only.

---

## 12. Clean storage-layout switch

Implement only the final unified layout:

- Every stream starts with one automatically managed segment.
- Every routing key, including the empty key, uses per-key ordering.
- The shared history database stores one canonical encrypted frame plus compact postings pages.
- The full-frame covering index is removed completely.
- Static per-key segment descriptors, dynamic child stream descriptors, legacy total-order routing metadata, and child registry objects are not decoded or imported.
- There is no `postings_from`, no mixed covering/postings read, no backfill tool, and no dual write.
- The new binary starts against a fresh namespace. Old history, segment maps, offsets, and descriptors are disposable.

The singular Durable Streams standards route begins with an empty default-key sequence in the fresh namespace and is conformant from its first write.

## 13. Correctness invariants

1. Every durable segment offset has one canonical frame.
2. An absorbed boundary advances only after canonical frames and required postings are durable in one history flush.
3. Postings offsets strictly increase and stay inside their bucket.
4. Every returned frame's exact routing key matches the request.
5. Key reads traverse predecessors before successors.
6. Retries across split commit at most once.
7. One response stays within span, byte, concurrency, and amplification budgets.
8. Cache memory is weighted and bounded.
9. Empty key pages can make cursor progress over proven-empty ranges.
10. Postings add no Class A path beyond the existing history flush.
11. The raw Durable Streams default-key sequence remains byte-exact and strictly ordered.

---

## 14. Observability

Routing/scaling:

```text
segment_count
segment_splits
segment_merges
split_intents_pending
ineffective_splits_avoided
hot_key_detected
hot_key_throttles
segment_map_cas_conflicts
```

Postings:

```text
postings_pages_written
postings_bytes_written
postings_runs_written
covering_bytes_avoided

postings_cache_hits
postings_cache_misses
postings_cache_coalesced_waiters
postings_cache_bytes
postings_cache_evictions
postings_prefetch_started
postings_prefetch_useful

postings_index_bytes_read
canonical_spans
canonical_scan_bytes
matching_frame_bytes
read_amplification
planner_exact
planner_coalesced
planner_envelope
planner_partial
postings_corrupt
postings_fallbacks
routing_hash_false_positives
```

Cost dashboard:

```text
history Class A / GiB absorbed
history Class B / 1,000 keyed reads
postings bytes / canonical bytes
canonical scan bytes / matching bytes
segment-map requests / million appends
```

---

## 15. Test and acceptance plan

### 15.1 Write/storage gates

Against current covering-index baseline:

```text
history Class A                       <= baseline + 1%
history flush/manifest count          unchanged
history stored bytes, 1 KiB records   <= 55% baseline
postings/canonical bytes              <= 8% at batch=1
                                      <= 2% at batch=10
LIST count                            unchanged
```

### 15.2 Read gates

```text
cold keyed p50                        <= 1.5x covering baseline
warm keyed p50                        <= 1.1x covering baseline
keyed p99                             <= 2x covering baseline
postings cache hit after first read   >= 90% within active 5-minute window
canonical spans                       <= 8/response
normal read amplification             <= 4x
per-offset object-read pattern        0
```

### 15.3 Scaling gates

- one hot key never triggers ineffective repeated splits;
- balanced multi-key load splits and improves capacity;
- per-key order survives recursive split/merge;
- ambiguous producer retry at every seal boundary commits once;
- crash after seal/before CAS self-heals;
- no child registry objects are created.

### 15.4 Economic gate

For one million routing keys with 100 randomly active per five-minute window:

```text
total storage + Class A + Class B + Compute COGS
<= 60% of current covering-index implementation
```

### 15.5 Protocol gate

The official Durable Streams suite passes unchanged against the singular default-key URL.

---

## 16. Explicitly rejected designs

- user-selectable total/per-key ordering;
- static user-selected segment counts;
- optional scaling;
- full-frame covering index;
- user-selectable index format;
- one object/database/manifest per key;
- one point GET per posting;
- one mutable bitmap row per key;
- child registry streams;
- recursive internal HTTP append;
- splitting one unsplittable hot key;
- presenting a cross-key collection scan as a Durable Streams offset sequence.

---

## 17. Exit criteria

```text
ordering/segments/scaling config accepted by new API       0
child registry streams created                             0
full-frame routing index writes                            0
postings cost/read gates                                   pass
split/merge/idempotence DST matrix                         pass
official Durable Streams conformance                       pass
cross-key API makes global-order claim                     0

```

<!-- END 03-UNIFIED-ROUTING-SCALING-POSTINGS.md -->

---

<!-- BEGIN 04-APPEND-AND-APPEND-MANY.md -->

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

<!-- END 04-APPEND-AND-APPEND-MANY.md -->

---

<!-- BEGIN 05-PRODUCER-SESSIONS.md -->

# Stage 5 — Producer Sessions

**Goal:** expose one safe public writer-coordination abstraction while implementing raw Durable Streams `Stream-Seq` and producer headers exactly as required by the pinned standard.

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

```ts
const producer = stream.producer("checkout-service", {
  state: producerStateStore,
})

await producer.append(order, {
  routingKey: order.customerId,
})

await producer.appendMany(batch, {
  routingKey: customerId,
})
```

The normal product API does not expose:

```text
Stream-Seq
Producer-Id
Producer-Epoch
Producer-Seq
manual retry tuple construction
```

Those remain wire-level protocol capabilities.

---

## 2. Producer identity and scope

### 2.1 Identity

`producerId` is a stable, application-chosen non-empty UTF-8 string.

Recommended examples:

```text
checkout-service
billing-importer-3
worker-installation UUID
```

Maximum encoded length: 256 bytes.

### 2.2 Product scope

Producer state is scoped to:

```text
stream incarnation + routing key + producer ID
```

This gives independent ordered pipelines per routing key and avoids one global producer lock across a key-partitioned stream.

### 2.3 Raw protocol scope

On `/v1/stream/{name}`, producer state follows the pinned Durable Streams protocol and is scoped to the default-key stream URL and producer ID.

The server documents that raw `Stream-Seq` is scoped per authenticated writer identity on the default-key protocol stream.

---

## 3. Client state store

A durable producer session needs client-side epoch/sequence persistence.

```ts
interface ProducerStateStore {
  load(scope: ProducerScope): Promise<ProducerState | undefined>
  save(scope: ProducerScope, state: ProducerState): Promise<void>
  compareAndSwap?(
    scope: ProducerScope,
    expected: ProducerState | undefined,
    next: ProducerState,
  ): Promise<boolean>
}

interface ProducerState {
  epoch: number
  nextSeq: number
}
```

`ProducerScope` includes stream identity, producer ID, and routing key.

SDK implementations provide adapters for:

- memory, for tests only;
- browser IndexedDB;
- Node/Bun file or SQLite storage;
- user-supplied database storage.

The default production SDK MUST warn or reject when an idempotent producer is created without a durable store, unless explicitly marked ephemeral.

---

## 4. Session lifecycle

### 4.1 First use

If no local state exists:

```text
epoch = 0
nextSeq = 0
```

### 4.2 Successful request

For request sequence `N`:

1. persist or reserve `(epoch, N)` locally;
2. send request;
3. on accepted or duplicate success, persist `nextSeq = N + 1`;
4. expose success to caller.

The SDK MUST retry network failures with the same epoch, sequence, routing key, operation kind, and body bytes.

### 4.3 Process restart

The SDK loads persisted state and continues.

Applications that deliberately establish a new writer incarnation call:

```ts
await producer.bumpEpoch()
```

which increments epoch and resets every routing-key sequence to zero.

### 4.4 Auto-claim

For explicitly configured ephemeral/serverless sessions:

1. try local `(epoch=0, seq=0)`;
2. if server returns stale epoch with current epoch `E`;
3. retry with `(epoch=E+1, seq=0)`;
4. persist the new state.

Auto-claim is an SDK policy, not a second producer mode in the server. It MUST be opt-in because it can fence another live producer using the same ID.

---

## 5. Concurrency

### 5.1 Same routing key

The SDK serializes sequence assignment per `(producer, routingKey)`.

It MAY pipeline multiple HTTP requests, but it MUST send and retry them in sequence order and handle gap responses without allocating a new sequence.

### 5.2 Different routing keys

Different routing keys use independent sequence counters and may proceed concurrently.

### 5.3 Multiple processes

Two processes sharing one producer ID and routing key require a state store with compare-and-swap/transactional sequence allocation. Otherwise, they MUST use distinct producer IDs.

The SDK MUST not imply that a local file/IndexedDB store coordinates several independent machines.

---

## 6. Wire mapping

Product requests send the official producer headers:

```text
Producer-Id
Producer-Epoch
Producer-Seq
```

They also send the routing-key selector on the product route.

One sequence identifies one HTTP append or appendMany batch, matching the protocol's per-request/batch semantics.

Server validation:

```text
epoch < current       -> 403 stale epoch
epoch > current,
  seq != 0            -> 400 new epoch must start at zero
epoch > current,
  seq == 0            -> accept and replace epoch
same epoch,
  seq <= last         -> duplicate success
same epoch,
  seq == last + 1     -> accept
same epoch,
  seq > last + 1      -> 409 gap
```

Validation and append are serialized and committed atomically per producer scope.

---

## 7. Stronger product duplicate record

For product producer state, persist:

```rust
struct ProducerCheckpoint {
    epoch: u64,
    last_seq: u64,
    last_request_hash: [u8; 16],
    last_result: AppendResultRef,
}
```

`last_request_hash` covers:

```text
operation kind
routing key
content type
canonical request body bytes
seal flag
```

On exact duplicate, return the original result/cursor.

If the same `(producerId, epoch, seq)` arrives with a different request hash, return:

```text
409 producer_sequence_reused
```

This protects callers from accidentally reusing a sequence for different data. The raw standards endpoint retains whatever behavior is required by the pinned protocol and conformance suite; the stricter check is additive on product routes.

---

## 8. Split and ownership movement

Producer state follows routing-key lineage.

After a split:

1. successor checks local state;
2. on miss, resolves checkpoint from predecessors that contained the key;
3. seeds the checkpoint in the same batch as the first successor append;
4. returns duplicate success for an attempt already committed on predecessor.

Ownership movement opens the same durable state and does not reset producer epoch/sequence.

The old owner is fenced and cannot acknowledge after the new owner takes possession.

---

## 9. SDK error behavior

```ts
class ProducerFencedError extends Error {
  currentEpoch: number
}

class ProducerGapError extends Error {
  expectedSeq: number
  receivedSeq: number
}

class ProducerSequenceReusedError extends Error {}
```

Default policy:

- transport timeout/reset → retry exact request;
- 429/503 → honor retry delay, retry exact request;
- duplicate success → return normal success with `duplicate: true` metadata;
- stale epoch → throw unless auto-claim is enabled;
- gap → reload local state or fail loudly; never skip forward automatically;
- sequence reused with different body → fail permanently.

---

## 10. Raw Durable Streams `Stream-Seq`

The product SDK removes `seq` options.

The raw Durable Streams endpoint continues to:

- accept `Stream-Seq`;
- compare values lexicographically;
- reject non-increasing values with `409`;
- preserve the documented scope;
- support using `Stream-Seq` together with producer headers as the protocol permits.

Internally, raw `Stream-Seq` uses a separate standards checkpoint from product producer sessions. It is not emulated by inventing a producer ID visible to users.

---

## 11. Clean producer-API switch

The final Prisma product SDK exposes only producer sessions. The implementation MUST delete, not adapt:

```text
product-level Stream-Seq options
manual Product Producer-* header construction
old producer-state codecs
producer-state import or rewrite jobs
old SDK retry helpers and aliases
```

No producer epoch, sequence, request hash, or local producer checkpoint from the current experimental implementation is preserved. Fresh product producers start at fresh durable state in the fresh namespace.

Raw `Stream-Seq` and raw `Producer-*` headers remain only on `/v1/stream/{name}` because they are part of the final pinned Durable Streams standards surface. They are not backward-compatibility shims.

---

## 12. Billing and usage

Duplicate requests:

- count as requests for operational telemetry;
- do not count as new records/bytes stored;
- do not emit a second billing storage event;
- expose duplicate counters by producer scope without raw producer IDs in shared operator views.

---

## 13. Durable Streams compliance

This stage uses the protocol's producer headers and validation behavior on the Durable Streams standards endpoint.

It MUST preserve:

- all-three-or-none header validation;
- integer bounds and grammar;
- stale epoch `403` with current epoch;
- duplicate `204` semantics on raw routes;
- gap `409` headers;
- atomic producer state and append;
- close interactions;
- raw `Stream-Seq` behavior.

The product SDK's `200` JSON response is on the separate product route and does not alter raw protocol responses.

---

## 14. Correctness invariants

1. One producer request commits zero or one times.
2. Producer state and records are atomic.
3. Retry uses identical bytes and tuple.
4. Same tuple with different bytes never silently succeeds on product routes.
5. Same-key requests serialize in sequence order.
6. Different keys can progress independently.
7. Split/merge/move does not reset producer state.
8. Closing append deduplicates correctly.
9. Raw protocol producer and `Stream-Seq` conformance remains intact.

---

## 15. Test plan

- first epoch/sequence;
- duplicate at every failure point;
- sequence gap and recovery;
- epoch bump and zombie fencing;
- concurrent same-key requests arriving out of order;
- concurrent different-key requests;
- process restart with persisted client state;
- ephemeral auto-claim fencing warning;
- same sequence/different body rejection;
- appendMany retry;
- append-and-seal retry;
- split predecessor/successor ambiguous retry;
- ownership handoff;
- producer state cleanup/retention policy;
- official producer and Stream-Seq conformance unchanged.

---

## 16. Observability

```text
producer_accepts
producer_duplicates
producer_gaps
producer_stale_epochs
producer_auto_claims
producer_sequence_reuse_conflicts
producer_predecessor_lookups
producer_predecessor_cache_hits
producer_state_bytes
producer_state_entries
```

---

## 17. Exit criteria

```text
public SDK Stream-Seq option                         removed
public manual Producer-* construction               removed
product duplicate corpus                            pass
split/move producer DST matrix                      pass
raw producer conformance                            pass
raw Stream-Seq conformance                          pass
producer duplicate storage/billing double count     0

```

<!-- END 05-PRODUCER-SESSIONS.md -->

---

<!-- BEGIN 06-READ-SUBSCRIBE-SCAN.md -->

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

<!-- END 06-READ-SUBSCRIBE-SCAN.md -->

---

<!-- BEGIN 07-TYPED-CREATION-DOCUMENT.md -->

# Stage 7 — Typed Creation Document

**Goal:** replace a growing set of product-specific creation headers with one idempotent, typed JSON configuration while preserving the raw Durable Streams `PUT` contract.

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

## 1. SDK contract

```ts
const stream = await client.createStream("orders", {
  encryptionKey,
  format: { kind: "json" },
  expiry: { idle: "30d" },
  watches: [
    { name: "by-customer", fields: ["/customerId"] },
  ],
})
```

Configuration type:

```ts
interface CreateStreamOptions {
  encryptionKey: string

  format:
    | { kind: "json" }
    | { kind: "bytes"; contentType?: string }

  expiry?:
    | { idle: Duration }
    | { at: string } // RFC 3339

  watches?: WatchDefinition[]
}
```

No fields exist for:

```text
profile
ordering
segments
scaling
queueMaxDeliveries
index mode
live mode
```

Consumer groups are created separately. Producer state is created by use. Segmentation/indexing are automatic.

---

## 2. Product management route

```http
PUT /v1/streams/{name}
Content-Type: application/json
Prisma-Encryption-Key: <base64url key>

{
  "format": { "kind": "json" },
  "expiry": { "idle": "30d" },
  "watches": [
    { "name": "by-customer", "fields": ["/customerId"] }
  ]
}
```

Response:

- `201 Created` for a new collection;
- `200 OK` for identical normalized configuration;
- `409 Conflict` for existing collection with different immutable configuration;
- `400 Bad Request` for invalid configuration;
- `403 Forbidden` for an encryption-key mismatch;
- `429 Too Many Requests` for management-plane limits.

Response body:

```json
{
  "name": "orders",
  "contentType": "application/json",
  "createdAt": "...",
  "sealed": false,
  "expiry": { "idle": "30d" }
}
```

---

## 3. Format mapping

### JSON

```json
{ "kind": "json" }
```

maps to:

```text
Content-Type: application/json
```

and enables standards-conformant JSON message semantics on the default-key standards view.

### Bytes/custom MIME

```json
{
  "kind": "bytes",
  "contentType": "application/x-protobuf"
}
```

maps to the specified MIME type. If omitted:

```text
application/octet-stream
```

The server operates at the byte level for all non-JSON content types.

A separate `text` mode is unnecessary; use `kind: "bytes"` with `text/plain`.

---

## 4. Expiry mapping

### Idle expiry

```json
{ "expiry": { "idle": "30d" } }
```

maps to Durable Streams `Stream-TTL` semantics:

- the window is sliding;
- origin reads and writes reset it;
- `HEAD` does not reset it;
- CDN-served reads do not reach the origin and do not reset it;
- live reads reset it when origin processing begins.

The product documentation must call this **idle expiry**, not retention or fixed time since creation.

### Absolute expiry

```json
{ "expiry": { "at": "2027-01-01T00:00:00Z" } }
```

maps to `Stream-Expires-At`.

The two forms are mutually exclusive.

### Record retention

Per-record retention is not part of this creation schema unless a separate retention specification is implemented. Do not overload collection expiry to mean record-age trimming.

---

## 5. Encryption-key handling

The SDK accepts `encryptionKey` in the creation options for ergonomics, but the JSON document MUST NOT contain the secret.

Wire:

```text
Prisma-Encryption-Key: <key>
```

The server stores only the stream-epoch-bound fingerprint and required derived observation capability material.

The same key is attached to the returned stream handle. Account authorization and record encryption remain separate credentials.

---

## 6. Watch configuration

`watches` is normalized and immutable per stream incarnation.

Normalization:

- watch names compared byte-for-byte after UTF-8 validation;
- names sorted for config hashing;
- field order inside one definition remains significant;
- JSON pointers use canonical escaping;
- duplicate names rejected;
- watches rejected for non-JSON formats.

The watch definitions are stored in the parent descriptor and do not create separate descriptor PUTs.

---

## 7. Idempotent normalization

The server computes:

```text
config_hash = hash(normalized protocol config + normalized watch config)
```

Normalized protocol config includes:

```text
content type
idle TTL or absolute expiry
initial sealed state (product create always open)
```

Equivalent duration spellings normalize to the same integer seconds. Media types normalize case and parameters according to existing protocol comparison rules.

A retry after a lost create response returns `200` when the normalized hash matches.

---

## 8. No initial records in product create

The product route creates configuration only. It does not treat its JSON body as stream content.

Users append explicitly after creation:

```ts
const stream = await client.createStream(...)
await stream.append(...)
```

This makes resource configuration unambiguous and keeps create retries independent from append retries.

The raw Durable Streams standards endpoint permanently retains optional initial content and create-and-close behavior as required by the protocol.

---

## 9. Raw Durable Streams `PUT`

```text
PUT /v1/stream/{name}
```

continues to use protocol headers and optional initial body:

```text
Content-Type
Stream-TTL
Stream-Expires-At
Stream-Closed
Stream-Forked-From
Stream-Fork-Offset
Stream-Fork-Sub-Offset
```

Mapping:

- creates the same parent collection descriptor;
- configures the default-key Durable Stream view;
- creates the initial one-segment automatic map;
- does not require product JSON configuration;
- preserves protocol idempotence and error behavior.

A collection created by raw `PUT` may be opened by the product SDK as the same final-format resource. It has no watches. If watches are required, create the stream through the product route from the start or delete and recreate it before launch; no management update or migration operation is provided.

---

## 10. Forks

The raw standards behavior route supports the pinned Durable Streams fork contract.

A fork created through raw protocol forks the default-key Durable Stream view. Whole-collection, all-routing-key fork semantics are not invented implicitly.

A future product method may be explicit:

```ts
client.forkDefaultStream(source, target, options)
```

or a separate collection-fork specification may define how every key and segment lineage is shared. Stage 7 does not pretend these are the same operation.

---

## 11. Immutable versus mutable resources

Immutable collection config:

```text
content format/type
stream encryption identity
watch definitions
expiry policy for the incarnation
automatic routing model
```

Separate mutable resources:

```text
consumer groups
adapter catalog entries
operator limits/policies outside customer config
```

Changing immutable config requires deleting and recreating the stream under a new incarnation. No generic `PATCH` or in-place migration operation is introduced.

---

## 12. Clean creation-contract switch

The plural Prisma product route accepts only the final typed JSON creation document plus the namespaced encryption-key header.

Remove, rather than translate, legacy product creation inputs:

```text
Content-Type as product configuration
Stream-TTL as product configuration
Stream-Expires-At as product configuration
Stream-Touch-Templates
Stream-Profile
Stream-Ordering
Stream-Segments
Stream-Scaling
```

Rules:

- Conflicting old/new input handling is unnecessary because old product headers are rejected.
- No header-to-JSON translator, descriptor upgrader, config-version bridge, or deprecation window is implemented.
- The singular Durable Streams standards route continues to use protocol-defined headers and `PUT` semantics independently.
- The new server starts with a fresh namespace containing only normalized final descriptors.

## 13. Configuration validation

- stream name follows Stage 8 rules;
- content type is syntactically valid;
- duration is positive and within service maximum;
- absolute expiry is future RFC 3339;
- key is valid 32-byte base64/base64url;
- watch count, names, and JSON pointers are bounded;
- unknown JSON fields are rejected in v1 to catch typos;
- `configVersion` may be added when future evolution requires it.

Initial bounds:

```text
max watch definitions      = 64
max fields/watch           = 16
max config body            = 256 KiB
```

---

## 14. Durable Streams compliance

The product management route is additive. The singular protocol route retains exact `PUT` semantics, including:

- optional content type/default;
- idempotent matching;
- TTL/expiry;
- initial bytes;
- initial closed state;
- fork headers;
- response headers and status codes.

The shared descriptor comparison must satisfy both contracts without making the raw client understand Prisma watches or routing internals.

A raw idempotent `PUT` compares only protocol-defined configuration and initial closure/fork identity. Product-only immutable config that cannot be expressed by raw protocol must not cause a generic raw retry of an already-existing stream to fail unexpectedly. Therefore:

- raw-created streams have an empty product capability config;
- product-created streams expose the same protocol config to raw clients;
- raw `PUT` against a product-created stream compares protocol fields and succeeds when those match, while leaving product watches unchanged.

---

## 15. Correctness invariants

1. Lost create response is safely retryable.
2. The encryption secret never enters descriptor JSON/object storage.
3. Raw and product creation resolve to one stream incarnation, not duplicates.
4. Config normalization is deterministic across languages.
5. Watches are durable before the first append can depend on them.
6. Product create never accidentally stores its config as a record.
7. Raw protocol initial-body/fork/close behavior remains conformant.

---

## 16. Test plan

- normalized config equivalence;
- all duration/media-type edge cases;
- conflicting expiry forms;
- unknown-field rejection;
- wrong-key idempotent retry;
- lost response and concurrent creators;
- product create then raw `PUT`;
- raw `PUT` then product open;
- JSON watch validation;
- create crash after descriptor PUT/before response;
- fork and soft-delete conformance corpus;
- official create/TTL/expiry/closure conformance unchanged.

---

## 17. Exit criteria

```text
new product create headers for profile/order/segments  0
product config accidentally appended as data           0
normalized idempotence corpus                          pass
raw create/fork/TTL/close conformance                   pass
secret keys persisted in descriptor                    0
extra Class A request for initial segment map           0

```

<!-- END 07-TYPED-CREATION-DOCUMENT.md -->

---

<!-- BEGIN 08-NAMING-LIFECYCLE-ROUTES.md -->

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

<!-- END 08-NAMING-LIFECYCLE-ROUTES.md -->

---

<!-- BEGIN 09-CONFORMANCE-MATRIX-AND-HARD-CUTOVER.md -->

# Appendix — Durable Streams Conformance Matrix and Pre-launch Hard-Cutover Plan

**Purpose:** make protocol preservation executable across all eight stages. This appendix is not a ninth product stage.

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

## 1. Normative baseline

The implementation MUST pin and record:

```text
DURABLE_STREAMS_PROTOCOL_COMMIT
DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION
DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION
```

The authoritative external references are:

- [Durable Streams core concepts](https://durablestreams.com/concepts)
- [Building a Durable Streams server](https://durablestreams.com/building-a-server)
- [Building a Durable Streams client](https://durablestreams.com/building-a-client)
- [Durable Streams JSON mode](https://durablestreams.com/json-mode)
- [Durable Streams fork semantics](https://durablestreams.com/fork)
- the protocol specification and conformance packages at the pinned upstream commit

The pre-launch implementation pins one protocol baseline. A change to that baseline is a separate pull request with a conformance report; support for mixed baselines or data migration between them is out of scope for this clean switch.

---

## 2. Non-negotiable standards boundary

The singular route is the protocol product:

```text
/v1/stream/{name}
```

It exposes one strictly ordered Durable Stream: the collection's default routing-key sequence.

The plural route is the Prisma product:

```text
/v1/streams/{name}
```

It exposes a key-partitioned collection. It is an additive API, not a reinterpretation of a Durable Stream offset sequence.

This is not a user-selectable mode. There is one Prisma implementation and one permanent standards-conformant view over its default routing key.

The following MUST be true:

1. A generic Durable Streams client can use the singular route without Prisma-specific knowledge.
2. A product append without `routingKey` and a raw append target the same canonical default-key sequence.
3. Product key cursors and scan cursors are rejected by the raw endpoint.
4. Raw offsets are rejected by product key/scan APIs unless an explicit conversion operation is specified.
5. Product cross-key scans never emit `Stream-Next-Offset` and never claim global append order.
6. Prisma extensions never occupy or shadow upstream-reserved `__ds` paths.

---

## 3. Operation matrix

| Capability | Raw Durable Streams route | Prisma product route | Shared implementation requirement |
|---|---|---|---|
| Create | `PUT /v1/stream/{name}` with protocol headers/body | `PUT /v1/streams/{name}` with typed JSON config | One descriptor/incarnation; protocol and product config normalized independently |
| Append one | Protocol `POST`; JSON mode preserves upstream flattening rules | `POST /records`; exactly one product record | One committer and durability path |
| Append many | Protocol JSON array flattening where applicable | `POST /records:batch`; explicit atomic batch | One atomic storage batch; no extra object-store request |
| Read history | `GET ?offset=` | `GET /records?routingKey=&cursor=` | Same canonical frames/history tier; distinct cursor codecs |
| Live read | Protocol long-poll/SSE and `Stream-Cursor` | SDK `subscribe()` with transport hidden | Same durable notifications/tail ring; protocol headers retained only on raw route |
| Metadata | `HEAD` with protocol headers and `no-store` | `GET /v1/streams/{name}` typed metadata | One descriptor and lifecycle state |
| Seal/close | Protocol `POST` with `Stream-Closed: true` | `POST /v1/streams/{name}:seal` | One monotonic sealed bit; raw default-key view observes closure |
| Delete | Protocol `DELETE` | Product `DELETE` | One deletion/tombstone/fork-reference implementation |
| TTL/expiry | `Stream-TTL` / `Stream-Expires-At` | Typed `expiry` object | One normalized expiry policy |
| Idempotent producer | Raw `Producer-*` headers | Producer session abstraction | One durable producer-state machine; product state scoped by routing key |
| Raw sequence | `Stream-Seq` retained | Not exposed | Standards route only |
| Fork | Pinned protocol fork semantics for default-key stream | No implicit collection fork | Shared references only where semantics are explicitly defined |
| Upstream subscriptions | Reserved `__ds` routes when in baseline | Product consumer groups on plural route | Shared low-level lease primitives MAY be reused; wire contracts remain separate |
| Cross-key scan | Not available as one Durable Stream read | Explicit snapshot `scan()` | Product-only signed scan cursor; no global-order claim |

---

## 4. Raw protocol behaviors that may not regress

The official server conformance suite is the source of truth. At minimum, each release MUST preserve:

```text
idempotent create and configuration conflict behavior
byte-exact resumption
strictly increasing opaque offsets
Content-Type preservation and append validation
JSON message boundaries and one-level array flattening
empty JSON-array rejection where required
catch-up reads and up-to-date signaling
long-poll behavior, including immediate EOF at a closed tail
SSE framing and control events
HEAD metadata and Cache-Control: no-store
monotonic durable closure
atomic append-and-close
DELETE and soft-delete/fork-reference behavior
TTL and absolute-expiry semantics
ETag/conditional-read behavior
Stream-Cursor behavior used for live request collapsing
Stream-Seq standards behavior
Producer-Id / Producer-Epoch / Producer-Seq validation and duplicate handling
fork semantics in the pinned baseline
reserved subscription routes in the pinned baseline
```

No product-stage simplification may remove these raw standards behaviors.

---

## 5. Header and token namespaces

### Raw protocol

All protocol-defined headers retain their exact spelling and semantics, including:

```text
Content-Type
Stream-Next-Offset
Stream-Up-To-Date
Stream-Closed
Stream-Cursor
Stream-TTL
Stream-Expires-At
Stream-Seq
Producer-Id
Producer-Epoch
Producer-Seq
Stream-Forked-From
Stream-Fork-Offset
Stream-Fork-Sub-Offset
```

### Prisma product

Product-only headers MUST be namespaced and MUST NOT reuse protocol headers for different token classes:

```text
Prisma-Encryption-Key
Prisma-Routing-Key
Prisma-Cursor
Prisma-Next-Cursor
Prisma-Scan-Cursor
Prisma-Next-Scan-Cursor
```

JSON response fields MAY be preferred over headers, but the token classes remain distinct.

All token codecs MUST include an explicit kind/version discriminator and authenticated integrity protection where a client could otherwise alter routing or snapshot state.

---

## 6. Stage-by-stage conformance gates

| Stage | Raw-suite focus | Prisma extension focus | Required standards evidence |
|---|---|---|---|
| 1 — Profiles | Full base suite | Final descriptor and capability composition | No profile fields, translators, or raw header changes |
| 2 — Consumers/watches | Base plus pinned upstream subscriptions | Per-key consumer and watch suites | `__ds` paths unshadowed; no consumer/watch DB per resource |
| 3 — Routing/scaling/postings | Full base suite against a fresh default key | Per-key order, split/merge, postings cost | Standards offsets remain valid inside the new namespace; no cross-key order claim |
| 4 — Append APIs | JSON and append/close suites | Explicit one/batch semantics | Product array-as-value does not alter raw JSON flattening |
| 5 — Producers | Producer and `Stream-Seq` suites | Producer-session retry/split corpus | Raw duplicate/gap/stale statuses and headers match the pinned standard |
| 6 — Read APIs | Catch-up, long-poll, SSE, cache suites | Key read/subscribe/scan | Product cursor never appears as `Stream-Next-Offset` |
| 7 — Creation | Create, TTL, expiry, fork, close suites | Typed config/idempotence | Product-created default key is raw-readable in the fresh namespace |
| 8 — Naming/routes | Full lifecycle/security suite | Product route/error/name corpus | Singular standards route remains permanent; removed product routes are absent |

A stage cannot be declared complete when only the Prisma suite passes.

## 7. Required CI topology

Every pull request touching protocol or storage behavior runs:

```text
job: durable-streams-server-conformance
  target: /v1/stream/{generated-name}
  upstream tests: unmodified, pinned version

job: prisma-product-conformance
  target: /v1/streams/{generated-name}
  tests: product API and SDK

job: dual-surface-equivalence
  tests: default-key operations through both routes produce identical canonical data

job: dst-focused
  tests: crash/fence/retry/segment/consumer/watch scenarios

job: request-cost-budgets
  tests: physical Class A/Class B budgets for changed paths
```

Nightly and release candidates add:

```text
full deterministic seed corpus
100k+ routing-key/cardinality workload
long-running split/merge/GC soak
real HTTP client reset/truncation tests
```

The conformance server MUST start from a fresh namespace per test case. Tests must also cover idempotent retry against an existing namespace.

---

## 8. Dual-surface equivalence corpus

For the default routing key, execute equivalent operations through raw and product APIs in both orders.

### Required cases

1. Product create → raw append → product read.
2. Raw create → product append without routing key → raw read.
3. Raw producer append → product read.
4. Product producer append → raw read.
5. Product seal → raw closed-tail read.
6. Raw close → product metadata and consumer drain.
7. Product delete → raw gone response.
8. Raw delete → product gone response.
9. Raw TTL create → product metadata/expiry.
10. Product idle expiry → raw `HEAD`/read semantics.
11. Product-created JSON stream → raw JSON array flattening.
12. Raw fork of default-key stream → product opening of target without inventing collection-wide fork semantics.

For every case, compare:

```text
canonical record bytes/messages
offsets/cursors at their respective surfaces
sealed/deleted/expiry state
producer state
usage/billing counters
object-store request count
```

---

## 9. Pre-launch hard-cutover order

The eight workstreams are implementation dependencies, not customer-visible versions or a migration sequence. The only deployment is one coordinated destructive pre-launch switch.

1. Implement the final internal representation, final readers/writers, final SDK, and final routes on one branch.
2. Delete old product descriptors, codecs, route handlers, headers, aliases, profile branches, covering-index readers, and legacy fixtures.
3. Build and test against a fresh object-storage namespace.
4. Run official Durable Streams server/client conformance, Prisma product conformance, dual-surface equivalence, DST, request-cost, and wide-cardinality gates.
5. Deploy server and SDK together into a new pre-launch environment with a new bucket or `PATH_PREFIX`.
6. Do not run old and new binaries against the same namespace.
7. After validation, delete the old pre-launch environment and its disposable data.

### Fresh-layout rule

Only the final descriptor, segment, postings, consumer, watch, and cursor formats exist in the new namespace. There are no layout-version bridges, cutover markers, mixed secondary indexes, or old-format readers.

### Rollback rule

Rollback is environment-level only: restore the previous binary and its previous isolated namespace. Never point the previous binary at the new namespace, and never point the new binary at the previous namespace. No N/N-1 rolling or storage compatibility is required before launch.

## 10. Protocol baseline upgrades

A baseline-upgrade pull request MUST contain:

```text
old and new protocol commit/version
upstream changelog summary
new/changed headers and routes
server conformance before/after report
client conformance before/after report
reserved-path collision audit
fresh-layout impact
conformance decision
```

If a new upstream feature conflicts with a Prisma product route, Prisma moves its product route. The raw protocol surface and upstream-reserved path win.

---

## 11. Security and privacy gates

- Encryption keys never enter object storage or logs.
- Routing-key hashes in postings are non-authoritative; canonical frames are verified against exact routing-key bytes.
- Product cursors are authenticated and cannot be edited to cross tenant, stream incarnation, routing key, or snapshot bounds.
- Consumer lease tokens are generation-fenced and unforgeable.
- Watch observation URLs contain only the minimum capability required to observe invalidation.
- Raw protocol authentication remains independent of record-encryption credentials.
- Error bodies never echo keys, tokens, plaintext records, or internal object paths.

---

## 12. Cost gates

No stage may shift simplicity into hidden object-storage cost.

For changed paths, report:

```text
Class A requests / million records
Class A requests / GiB ingested
Class B requests / GiB delivered
bytes stored / canonical payload byte
background Class A / shard-hour
background Class B / shard-hour
memory per active stream/key/consumer/watch
CPU per record read/written
```

Hard invariants:

```text
per-routing-key database/manifest/object namespace      prohibited
per-consumer database/manifest/object namespace         prohibited
per-watch database/manifest/object namespace            prohibited
one canonical payload copy                              required
point GET per posting/record pattern                     prohibited
unbounded in-memory cardinality                          prohibited
periodic LIST per key/consumer/watch                     prohibited
```

---

## 13. Final release gate

The full simplification is releasable when:

```text
official pinned Durable Streams server suite             pass
pinned Durable Streams client conformance                 pass
Prisma product conformance                                pass
dual-surface equivalence                                  pass
removed product routes/fields/codecs absent               pass
fresh-namespace destructive cutover                       pass
DST safety and liveness corpus                            pass
postings storage/read COGS gate                           pass
consumer/watch cost gates                                 pass
wide-cardinality memory gates                             pass
real Compute + Tigris field campaign                      pass
```

The release report lists every raw protocol capability, the pinned baseline, and the final product surface. “Durable Streams compliant” may be claimed only for the singular standards route tested by the official suite; the plural Prisma collection API is documented as a separate product API.

<!-- END 09-CONFORMANCE-MATRIX-AND-HARD-CUTOVER.md -->

---
