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
