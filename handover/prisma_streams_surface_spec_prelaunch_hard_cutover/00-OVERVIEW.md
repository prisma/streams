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
