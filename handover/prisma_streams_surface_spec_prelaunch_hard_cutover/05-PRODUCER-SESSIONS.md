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
