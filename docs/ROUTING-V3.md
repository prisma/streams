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
    covered_from,               // runs are COMPLETE from this offset
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

**Write-through warming.** A cold keyed read otherwise pays two
DEPENDENT store round trips — index page fetch, then canonical span
fetch — measuring ~2× the covering baseline's cold p50 (covering
needed one trip because its index WAS the data). Indirection cannot
beat that structurally unless the index half is already resident. It
can be: reads route to the shard owner, the shard owner runs the
absorber, and the absorber holds every chunk's decoded runs at the
instant it encodes them — so the gather installs each chunk's runs
into the slice cache right after the batch flush.

Coverage claims stay provable via `covered_from` plus per-segment warm
state (`SegWarm{from, to, clean}`):

- a fresh warm install claims coverage from 0 only while the segment's
  chunks have been contiguous from offset 0 within this process AND no
  entry of the segment was ever evicted (weight eviction and the idle
  sweep both poison `clean` — an evicted key must not re-appear
  claiming its pre-eviction history was empty);
- any other fresh install claims only its own chunk; extensions keep
  their existing `covered_from`; a read below `covered_from` bypasses
  to the normal store load;
- chunk seams carry GAP_UNKNOWN exactly like store-loaded page
  boundaries, so the planner treats warm and loaded runs identically.

Readers clip every claim to their own durable-absorbed snapshot, so an
install racing the boundary advance can never over-serve. Restarted or
non-absorbing instances find nothing warm and take the cold-load path.

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

Measurement bindings (what the harness actually computes — learned the
hard way in the acceptance campaign):

- **history stored bytes** comes from the engine's own counters
  (canonical + postings bytes + 65 B/page key), NOT whole-pipeline
  `put_bytes`: WAL SSTs, compaction rewrites and manifests are
  identical in both arms and dominate at rig scale, diluting the
  layout difference to noise.
- **total COGS** binds the at-scale asymptote — the stored-byte-month
  ratio — with measured op-parity guards (read Class B, CPU) alongside
  the Class A/flush/LIST parity gates. Raw local-run COGS is reported
  but not gated: rig flush Class A is cadence-driven (25 ms ticker
  over a trickle), so op counts do not extrapolate; byte ratios do.
- **the byte-ratio workload must be incompressible**: frames compress
  before storage, so compressible padding deflates the canonical
  denominator and inflates the ratio (a repeated-digest pad read as
  23.9% where the true ratio was ~3%). The harness pads with base64
  over chained distinct sha256 blocks — the JSON-safe entropy ceiling.
- **harness clients must read HTTP headers case-insensitively**: hyper
  lowercases header names on the wire; a `dict()`-based lookup of
  `Stream-Next-Offset` silently misses, which truncates any read that
  paginates — mid-segment pages and every lineage hop across a split.

## 10. Staging

PR1 one routing model (fields/headers removed, descriptor-resident
map, direct dispatch, migration, single segment always) → PR2 postings
write+read (planner, verification, consumed_to) → PR3 slice cache →
PR4 sketch scaler (splits/merges live) → PR5 split-safe producers →
gates harness. Each PR lands suite-green; DST crash points ride the
PR that introduces the machinery they cover.

## 11. Campaign results (2026-07-30)

**Precise status: compact postings and logical split-lineage gates
pass. Physical automatic scaling and several transition edge cases
remain open** (review of 30d0a4b; disposition below).

Local rig: s3lite at 25 ms latency, 1 KiB incompressible records
(base64 chained sha256), covering baseline = pre-postings build on the
identical driver. Suite 137 green. Batch-1 16/16, batch-10 16/16,
live split 7/7 — as those gates are defined here.

```
                                   batch 1 (20k keys x2)   batch 10 (10k keys)
history stored bytes               54.0%  (28.1/52.1 MB)   43.2%  (12.7/29.5 MB)
postings/canonical bytes           4.09%  (gate 8%)        0.41%  (gate 2%)
history Class A                    +0.9%  (1654/1640)      +0.2%  (492/491)
flush/manifest/compaction puts     unchanged               unchanged
whole-pipeline put bytes (info)    79.2%                   94.9%
cold keyed p50                     1.38x  (90.5/65.4 ms)   1.00x  (220.8/220.1 ms)
warm keyed p50                     1.00x  (36.8/36.8 ms)   1.00x  (218.2/218.6 ms)
keyed p99                          1.36x  (101/74 ms)      0.92x  (325/352 ms)
spans per response                 2                        1
read amplification                 1.00x                    1.00x
postings cache hits (1200 warm)    1801                     1807
sst GETs vs covering               3602 vs 3602             36,721 vs 36,813
COGS asymptote (byte-months)       54.0%                    43.2%
COGS measured @1mo / @3mo (info)   88.4% / 74.2%            97.8% / 94.2%
zero read errors                   both arms                both arms
```

Live split scenario (scaler knobs hot): a multi-key hot stream splits
under lockstep 8-key load with ZERO client-visible append errors
through the seal; per-key order and exact counts hold across the split
(also verified page-by-page: 511/511 every key, seg0 drain → child
token hop → up-to-date); a one-dominant-key stream never splits and
stays fully readable (hot_key exposed); resume/idempotence via the
descriptor's pending intent.

Keyless regression check on the same code: w100k wide soak — steady
Class A 101,060 (round-4 baseline 98,269, +2.8%, within the ±3% run
variance of absorb cadence), append p50 46.4 ms, honest footprint
gauge 778 MB vs 783 baseline, ps-RSS 908 vs 896 MB, zero errors.

Read-latency context: the honest-padding regime exposed that BOTH
arms' keyed reads pay ~1-2 store GETs per ~1 KiB record — warm equals
cold — i.e. range scans are not served from the shared block cache
across statements. This is the shared history read path (identical in
the covering baseline), tracked as a follow-up outside routing-v3;
the earlier "0.8 ms warm / 0 GETs" numbers from compressible-padding
runs were a write-through block-cache artifact, not a real read path.

COGS precision (review wording): the stored-byte and asymptotic
retention COGS gates pass (54.0% / 43.2%). Total workload COGS
reaches those ratios only when retained history dominates fixed WAL
and request costs — the measured 1-month totals were 88.4% / 97.8%.
The asymptote is a production projection, not a measured total-COGS
pass. The break-even model below states exactly when the total gate
holds.

### Production break-even model

Prices: Class A $4.50/M, Class B $0.36/M, storage $0.02/GiB-month,
CPU $0.03/vCPU-hour. Measured invariants from the campaign: history
stored ratio 0.51–0.54× of covering at batch 1 (0.43× at batch 10);
write Class A, read Class B and CPU at parity (gates above). Per
stored GiB-month with keyed drains at frequency `f` (fraction of the
stored data fully drained per month) and record size `z`:

```
storage_cov = 2.04 x $0.02        = $0.0408
storage_v3  = 1.04 x (1+r_p) x $0.02 ≈ $0.0208
reads(f,z)  ≈ f x (2^30/z) x $0.36e-6      (parity, both arms)
ratio(f,z)  = (0.0208 + reads) / (0.0408 + reads)
```

Fixed WAL Class A per ingested GiB (~64 flush PUTs at 16 MiB SSTs ≈
$0.0003) and CPU deltas (measured ≈ 0) are negligible against the
byte-month terms. Solving ratio ≤ 60% at z = 1 KiB:

```
f ≤ ~0.05   — total COGS gate holds when at most ~5% of stored
              data is keyed-drained per month (≈ 25k key-drains
              per stored GiB-month at 2 KiB keys)
```

Postures (z = 1 KiB): archive / event-log replay-rarely (f→0) →
**51%**; balanced (f = 0.05) → **60%**; the spec's hot activity model
(100 active keys per 5-minute window over 1M keys ≈ 0.86 drains per
key-month, f ≈ 0.86) → **~97%** — which independently reproduces the
measured 1-month totals (97.8% at batch 10), validating the model.
Larger records move the break-even up linearly (B-ops per GiB fall
with z): at z = 100 KiB, f ≤ ~2.4 full drains per stored GiB-month.

Bottom line: postings are strictly ≤ covering at every posture (reads
at parity, storage strictly less, write side structurally identical);
the ≤60% TOTAL claim is a storage-dominated-posture claim — retention
months ≥ 1 and monthly keyed-drain volume under ~5% of the store at
1 KiB records — and should be quoted with those conditions.

### Review disposition (30d0a4b)

Approved: postings storage format; same-flush write economics;
measured b1/b10 read behavior (for the tested shapes); large-section
cache mechanics; logical split lineage.

Open, release-blocking:

- **Splits are logical, not physical.** Children carry
  `shard_prefix: ""` / `route_hash: [0;16]` and resolve to the parent
  route — same engine, same committer, same admission bucket; a split
  adds no capacity. Children need persisted independent routes used
  consistently by append/read/seal/absorb/queue/consumer paths
  (seal_identity and read_v3_lineage currently hard-code the parent
  route), admission split into stream accounting vs segment capacity,
  and a two-owner campaign proving ≥1.8× post-split throughput on
  distinct ShardEngines.
- **Seal-to-publication reads can report permanent closure.** Read
  dispatch must be transition-aware (`segments.len()>1 ||
  pending.is_some()`), never emit Stream-Closed / final Up-To-Date
  while a pending transition's successor is unpublished, and resume
  the transition; deterministic failpoint between seal and successor
  CAS, exercised by GET/HEAD/long-poll/mid-gap/cancel-retry tests.
- **Oversized keyed records stall.** A run larger than the 8 MiB read
  budget plans zero spans; consumed_to must flow end-to-end and the
  first record must always progress (bounded sub-runs, budget-stop).

Open, incomplete: Stream-Seq must resolve through sealed predecessors
(a parent-accepted sequence is currently reusable on a child);
producer rows must include the routing-key hash; the hard 4× read
amplification bound is not enforced (a 64 KiB gap coalesce over tiny
records can reach ~31×); the postings cache is per-engine (nominal
512 MiB at 32 engines), its weight ignores entry overhead, its
POSTINGS_CACHE_BYTES env is unwired, and write-through admits every
absorbed key rather than read-interested ones; heavy-hitter and HLL
sketch state never decays and SKETCH_MAX=4096 silently stops sketching
new segments.

Resolved since the review:

- **Merge execution** — implemented: execute_merge + resume(kind=
  "merge") with the same two-phase seal/publish discipline, children
  route-assigned, crash-resumable, seal-gap read semantics applying to
  both parents automatically; a conservative auto-policy (all sketched
  segments cold at 5% of the hot line for 4x the split patience, both
  segments older than the cooldown) drives it from the eval loop.
- **SSE across lineage** — implemented for keyed subscribers: drain
  every predecessor's matches, then live-follow the key's live
  segment; a seal that is not a genuine close ends the connection
  WITHOUT streamClosed and the reconnect follows the successors.
  Keyless SSE on a segmented stream is 400 keyless_live (same scalar-
  cursor impossibility as keyless long-poll).

Still deferred, by explicit posture:

- **Queue and state-protocol profiles stay pinned single-segment**
  (scaler note_append pins them). Their cursor/journal semantics are
  per-stream scalars today; un-pinning requires the per-segment
  consumer-state design (spec §11), a product decision — not a gap in
  the scaling machinery.
- **Legacy surface removal** (static per-key layouts, scaling=auto
  parents, the old scaler paths): tracked as its own pre-launch
  cleanup — the surface spans seven modules with its own test matrix
  and deletes cleanly only with its creation-time validation and
  conformance accommodations.
