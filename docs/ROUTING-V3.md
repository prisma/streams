# ROUTING-V3: one routing model

Status: implementation companion to the full specification ("Unified
Routing-Key Streams and Compact Postings Index", received 2026-07-30).
The routing model, the descriptor-resident segment map, automatic scaling
and split-safe keys below are live. The spec's compact postings index is
retired: since layout 6 a keyed read is served from the seal directory's
per-chunk routing-key summaries (§3; SPEC §3.5 and §3.6). This file records
the REPO-SIDE bindings and the few deliberate deltas, marked
**[bound here]**:

- Segment 0's engine identity is `storage_hash()` for NEW streams too
  (the spec's §4.2 formula applies from seg_id ≥ 1; keeping seg 0 ≡
  storage_hash makes fresh and migrated streams identical and keeps
  the implicit map at zero descriptor bytes — spec §12.1 semantics,
  applied universally).
- PR ordering: repo PR1 = spec PR1 minus the legacy scaling fold
  (child-stream routing stays functional for pre-v3 descriptors until
  spec-PR5). The sketch scaler, split-safe producers and profile
  integrations followed in spec order.

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

## 3. Keyed reads (layout 6)

A stream's records are stored once, as pages (one append request's
records each, at most 64 KiB and 4,096 records), behind a clear header
that names the page's routing key (SPEC §3.3). There is no per-key index
of any kind.

- **Unsealed pages** are held in memory by the stream's handle; a keyed
  read filters them by their clear headers.
- **Sealed pages** live in the shard's segment objects, one chunk per
  stream per seal (`src/spill/object.rs`). Each chunk's row in the seal
  directory (`src/spill/directory.rs`) carries a routing-key summary:
  the key fingerprints (the first 8 bytes of each key's hash, sorted)
  when the chunk holds 1 to 16 keys, else a bloom of at most 256 bytes.
  A keyed read skips every chunk whose summary excludes its key without
  a GET (its offsets still count as consumed), fetches the chunks the
  summary admits with one ranged GET each, and filters their pages by
  their headers. A bloom's false positive costs one GET that serves
  nothing.
- Every returned record is checked against the exact routing-key bytes,
  so a fingerprint or hash collision can only add a candidate, never
  return another key's data.

The read is bounded by the chunks it fetches (at most 8 GETs in flight, at
most 256 per read) and its byte budget; past either it ends with an
honest partial and a resume cursor that tracks how far the read consumed,
separately from the last returned record, so a key read advances over
chunks the summaries prove hold no match (edge change #207).

## 4. Automatic scaling on real distributions

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

## 5. Split-safe key semantics

- `Stream-Seq` is scoped to the ROUTING KEY, not the segment.
- Producer state (id → epoch/seq/offset) is looked up through the
  predecessor chain after a split and seeded into the child's first
  append for that key.
- An ambiguous retry that committed on the parent is suppressed by the
  child (duplicate ack with the parent's committed identity), and the
  duplicate consumes NO new offset.

## 6. Read semantics

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

## 7. History

The 2026-07-30 acceptance campaign measured the compact postings index
against the covering index it replaced (stored bytes, Class A parity,
keyed latency) and reviewed the split machinery at 30d0a4b. Both indexes
are retired, and the split findings are tracked in
[SCALING.md](SCALING.md); the campaign's numbers are in this file's git
history before layout 6.
