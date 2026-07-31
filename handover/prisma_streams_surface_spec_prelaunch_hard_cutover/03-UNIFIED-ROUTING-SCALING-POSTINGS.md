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
