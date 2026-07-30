# ROUTING-V3: one routing model, compact postings

Status: implementation companion to the full specification ("Unified
Routing-Key Streams and Compact Postings Index", received 2026-07-30),
which is authoritative. This file records the REPO-SIDE bindings and
the few deliberate deltas, marked **[bound here]**:

- Segment 0's engine identity is `storage_hash()` for NEW streams too
  (the spec's §4.2 formula applies from seg_id ≥ 1; keeping seg 0 ≡
  storage_hash makes fresh and migrated streams identical and keeps
  the implicit map at zero descriptor bytes — spec §12.1 semantics,
  applied universally).
- PostingsPageV1 header integers are fixed-width LE; runs are varint
  (spec §6.4 leaves header encoding open). codec=1
  (compress-if-smaller) is reserved but not yet emitted.
- Greenfield deployment: postings_from = 0 everywhere (spec §12.4's
  pre-GA arm); the covering-index reader was DELETED rather than
  retained — the only fallback is the §8.6 corruption envelope. The
  `k`-keyspace rows in old dev partitions are inert.
- PR ordering: repo PR1 = spec PR1 minus the legacy scaling fold
  (child-stream routing stays functional for pre-v3 descriptors until
  spec-PR5); repo PR2 = spec PR3 + the planner/consumed_to half of
  spec PR4. The slice cache, sketch scaler, split-safe producers and
  profile integrations follow in spec order.

## 0. What is being replaced

Today there are three overlapping routing concepts:

1. **Total order (default)** — one sequence; `Stream-Key` is metadata
   within it. `desc.storage_hash()` is the only engine identity.
2. **`Stream-Ordering: per-key` + `Stream-Segments: N`** — static
   power-of-two segmentation inside ONE descriptor;
   `segment_for(rk) = top bits of SHA(rk)`; engine identity per ordinal
   via `desc.segment_hash(ordinal)`; route hash stays the parent name
   hash (`src/registry.rs:94-115`, `src/http.rs` seg_ord resolution).
3. **`Stream-Scaling: auto`** — a CAS-versioned `SegmentMap` in the ops
   bucket (`streams/<hash>/segmap.json`, `src/segmap.rs`), segments as
   INTERNAL CHILD REGISTRY STREAMS `"<parent>#<seg_id>"` created
   lazily, appends routed by RECURSIVE HTTP self-calls with
   sealed-child retry (`src/http.rs` append() head, `src/scaler.rs`).

Path 3's costs are structural: an extra segment-map object per stream,
child descriptor PUTs, child descriptor GETs on every cold route, and
recursive internal HTTP requests on every append. Path 2 has no
scaling. Path 1 overloads `Stream-Key` into pure metadata with a
covering `k!` index that duplicates every keyed frame.

## 1. The model

**Every stream is key-partitioned internally. An absent routing key is
the empty/default key. Ordering is always per key. Streams begin with
one segment and split or merge automatically. A stream using only the
empty key has one routing key and therefore remains totally ordered.**

Removed (creation rejects them with 400 `unified_routing`):
`Stream-Ordering`, `Stream-Segments`, `Stream-Scaling` headers; the
`ordering`, `segment_count`, `scaling` descriptor fields (kept as
serde-tolerated legacy inputs for migration, never written for new
streams, never consulted by new code paths).

Append resolution:

```
routing key present  → that exact key
routing key absent   → "" (the empty/default key)
key_point = u64 fixed-point position of stream_hash(rk) in [0,1)
segment   = the live segment whose [lo,hi) contains key_point
```

The sole ordering guarantee is per-routing-key order.

## 2. Descriptor-resident segment map

`StreamDesc` gains `segments: Option<SegmentMap>` (the existing
`src/segmap.rs` type — version, next_seg_id, ranges, seals,
predecessors — unchanged shape).

- `None` (the common case, and every fresh stream) means the implicit
  single-segment map: segment 0 covers the whole keyspace, is live,
  and **its engine identity is `storage_hash()`** — so creating a
  stream costs zero extra requests and zero extra bytes, and every
  existing total-order stream is ALREADY in the new model with its
  whole history as segment 0. **[bound here]**
- The map is materialized into the descriptor by the FIRST split.
  Descriptor writes are If-Match CAS on the registry object (single
  writer per transition; losers reload and re-evaluate — the same
  discipline segmap.json used). Registry cache revalidation (ETag
  If-None-Match, 304-heavy) already propagates map changes at TTL
  cadence; the append path force-refreshes once when it routes to a
  sealed segment.

Segment engine identities **[bound here]**:

```
seg_id == 0             → desc.storage_hash()          (zero-move migration)
seg_id >= 1 (dynamic)   → stream_hash("{name}\0segid\0{seg_id}\0{epoch}")
legacy static ordinal k → desc.segment_hash(k)         (migration only)
```

Shard placement: `SegmentDesc.shard_prefix` ("" = the parent's default
route `stream_hash(name)`); dispatch goes DIRECTLY to that segment's
shard engine — no child descriptors, no recursive HTTP, no per-segment
registry objects, no separate segmap object.

Migration of the three legacy paths (no data movement):
- total-order: nothing to do (implicit map ≡ current behavior).
- static per-key: on first touch, materialize a map of N segments over
  even ranges with identities `segment_hash(ordinal)`.
- scaling=auto: on first touch, fold ops-bucket `segmap.json` into the
  descriptor; each legacy segment's identity is its CHILD STREAM's
  `storage_hash()` (children keep their data); child registry rows are
  tombstoned. **[bound here]**

## 3. Compact postings replace the covering index

Canonical row (unchanged): `<route16><seg16> 'r' <offset_be8>` →
encrypted frame, stored ONCE.

The `k!` covering index (v1) and `hist2_index_key` full-frame
duplicates (v2) are replaced by postings pages for EVERY routing key
including the empty key:

```
<route16><seg16> 'p' <rk_hash16> <bucket_be8> <page_first_offset_be8>
    → postings page (compressed offset runs)
```

- `rk_hash16 = stream_hash(rk)` (16 bytes). 128-bit collisions can add
  CANDIDATES, never wrong data: every retrieved frame is verified
  against the exact routing-key bytes before it is returned.
- `bucket = segment_local_offset / 65_536` — fixed size, so the bucket
  holding any cursor is directly calculable; no predecessor scan.
- A page covers offsets `[page_first_offset, ..)` within one bucket;
  the gather emits at most one page per (key, bucket) per flush, so
  page granularity tracks absorption batching.

Page value codec — PostingsPageV1 (spec §6.4; header widths
**[bound here]**):

```
u8  version = 1
u8  codec   = 0 raw (1 reserved: deterministic compress-if-smaller)
u64 first_offset (LE)            // duplicates the key's page_first
u64 last_offset_exclusive (LE)
u32 run_count (LE)
u64 matching_frame_bytes (LE)    // page total
run*: varint gap_offsets            // offsets skipped since prev run end
      varint record_count           // matching records in this run
      varint matching_frame_bytes   // stored bytes of the matching frames
      varint gap_frame_bytes_before // stored bytes of the skipped gap
```

Pages self-describe (header/runs disagreement or key/header first-
offset mismatch = corruption → the §8.6 envelope). Encoded pages cap
at POSTINGS_PAGE_MAX_ENCODED_BYTES = 32 KiB; the builder splits a
bucket into further pages (fresh page_first) at the cap. The byte
fields let the read planner choose between scanning exact runs,
combining nearby runs, or reading one envelope and filtering.

Postings pages enter the SAME history WriteBatch and flush as their
canonical rows. Therefore postings add **no** additional Class A
request, manifest update, database, object namespace, or LIST/GC
lifecycle — the acceptance gate `history flush/manifest count
unchanged` is structural, not aspirational.

## 4. PostingsSlice cache

Per shard engine, a weighted single-flight decoded cache:

```
PostingsSlice {
    first_bucket, last_bucket_exclusive,
    indexed_to_offset,          // how far the index provably covers
    runs: Arc<[DecodedRun]>,
}
```

Cold miss loads forward up to: 64 buckets, or 1 MiB encoded postings,
or the requested absorbed boundary — whichever first. Weighted by
decoded bytes, globally bounded (env `POSTINGS_CACHE_BYTES`, default
64 MiB **[bound here]**), idle-evicted after 10 minutes, single-flight,
cancellation-proof (worker task owns the load), extended FORWARD
incrementally rather than invalidated, async prefetch of the next
slice when 75% of the current slice is consumed.

Acceptance: for 100 randomly active keys per 5-minute window,
postings-cache hit rate after each key's first read ≥ 90%.

## 5. Bounded canonical indirection (read planner)

The second step never degenerates into one GET per posting:

```
maximum spans per response      8
maximum concurrent span reads   4
target read amplification       2x
hard read amplification         4x
maximum coalesced gap           64 KiB
maximum canonical scan bytes    16 MiB
```

Past budget → honest partial + resume cursor. The cursor tracks
`consumed_to_offset` separately from the last returned record, so a
key read advances over ranges the index PROVES contain no matches.

## 6. Automatic scaling on real distributions

Per-segment sketch, fed at append admission:

```
KeyDistribution {
    bins: [Ewma; 64],      // key_point-space load histogram
    top_keys: SpaceSaving8,
    distinct: Hll64,
}
```

- Split only when BOTH predicted children receive meaningful load
  (env floor, default: each child ≥ 20% of the split threshold
  **[bound here]**); split point = recent load-weighted median of the
  bins, not the numeric midpoint.
- A single dominant key (SpaceSaving majority above the hot line) is
  intrinsically unsplittable while preserving its order: expose it as
  `hot_key` in /v1/debug/usage and let the per-key limit apply —
  never mint segments that cannot help.
- Merge: adjacent cold siblings, existing cooldown discipline.

## 7. Split-safe key semantics

- `Stream-Seq` is scoped to the ROUTING KEY, not the segment.
- Producer state (id → epoch/seq/offset) is looked up through the
  predecessor chain after a split and seeded into the child's first
  append for that key.
- An ambiguous retry that committed on the parent is suppressed by the
  child (duplicate ack with the parent's committed identity), and the
  duplicate consumes NO new offset.

## 8. Read semantics

- `?key=<k>` — ordered historical+live read for one routing key.
- no key — deterministic segment-sequential replay of the complete
  stream (segment lineage order; no cross-key ordering claim).
- live read without a key — unsupported (400 `keyless_live`): one
  scalar cursor cannot represent concurrently progressing segments.
- `?key=` — the empty/default key, historical or live.

Total-order streams keep exactly their old behavior through the
`?key=` (or implicit empty-key) path: one key, one segment, total
order preserved; and keyless replay of a single-segment stream is
byte-identical to today's read.

## 9. Acceptance gates

Cost gate = storage byte-month + Class A + Class B + compute CPU, at
the million-routing-key workload:

```
total postings COGS                <= 60% of covering-index COGS
history Class A                    <= baseline + 1%
history flush/manifest count       unchanged
history stored bytes               <= 55% of covering layout
postings bytes / canonical bytes   <= 8% at batch 1, <= 2% at batch 10
cold keyed p50                     <= 1.5x covering index
warm keyed p50                     <= 1.1x covering index
canonical spans per response       <= 8
normal read amplification          <= 4x
per-offset GET pattern             zero
```

## 10. Staging

PR1 one routing model (fields/headers removed, descriptor-resident
map, direct dispatch, migration, single segment always) → PR2 postings
write+read (planner, verification, consumed_to) → PR3 slice cache →
PR4 sketch scaler (splits/merges live) → PR5 split-safe producers →
gates harness. Each PR lands suite-green; DST crash points ride the
PR that introduces the machinery they cover.
