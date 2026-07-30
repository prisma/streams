# Cost campaign 2 — shared history, LIST-free steady state, and the recovery gate

Second consolidated report (continues docs/COST-CAMPAIGN-1.md, which
covered the request-cost round 1, the wide-cardinality characterization,
and the active-axis study). This campaign's scope: the round-2 reviewer
verdict — shared history v2 as the main project, the sparse interim
policy, the GC cost work (adaptive cadence, then the structural
LIST-free fix), the wedge liveness gate from FAIL to PASS, and the hash
newtypes. Everything verified on the local twin rig (bench/costab:
s3lite with 25 ms latency + Tigris-shaped billing ledger); field
validation is explicitly deferred until credentials are provisioned.

Commit chain (slate): 581f1e2 (sparse policy + gate + v2 design) →
57827c6 (history v2) → 64534ac (GC adaptive cadence) → 2c316bf
(LIST-free steady state + newtypes) → e5f7d98 (shed recovery fix).
Fork branch sorenbs/slatedb#gc-adaptive-backoff: f5e5380 (cadence) →
42e3249 (probe-cached latest reads + listing reuse) → 7cb8fb4
(empty-sweep refresh floor).

## 1. Headline economics (w100k: 100k one-record streams, 100 active, 15 min)

| arm | steady total Class A | history Class A (total) | Class A per sparse stream |
|---|---|---|---|
| A: v1 absorb-all | 685,647 | 559,470 | ≈ 43 (≈ $215/M at Tigris prices) |
| B: defer sparse (interim policy) | 272,791 | 147,872 | deferral, not absorption |
| C: shared history v2 | 133,808 | 9,490 | 0.095 |
| **C + LIST-free GC (final)** | **96,906** | **1,203** | **0.012 (≈ $0.06/M)** |
| acceptance gate | — | ≤ 5,000 | ≤ 0.05 |

All measured local history-v2 REQUEST-COST, append-performance,
cold-read, and integrity gates passed for this workload: full 100k
drain in-window (backlog 0, deferred 0), appends byte-flat across
every arm (p50 46.9 / p99 88.0 ms, 4.14 M ok, 0 errors), absorbed cold
scans 54.8 ms (1.96× the 28 ms unabsorbed control, within the ≤2×
gate), history LISTs 128 for the whole run. End-to-end: absorbing a
sparse stream's history now costs ~3,600× fewer Class A requests than
v1, and steady-state total request volume is 7.1× lower than where
the campaign started. NOT covered by that sentence at the time it was
first written — and called out by the round-3 static audit — were: the
1-GiB RSS gate (measured 820 MB footprint, not passed), product
accounting above 65,536 streams, aggregate gather memory safety,
passive restart discovery, mixed-layout migration, the full
crash-point matrix, and field validation. §7 records how round 3
closed the first four of those.

With the interim deferral policy active instead (Arm B posture on the
same binary), history Class A is 485 total and scan p50 halves to
27.6 ms — sparse reads stay on the shard-log fast path. Both postures
are now cheap; deferral remains the better read-latency choice for
overwhelmingly-sparse populations, absorb-all the better trim/storage
choice.

## 2. The LIST problem, solved in two layers

Measured on the capped 30-min soak (tiers 1,2,3,4 × 450 s), the
pre-round baseline spent 16,671 LISTs (12,493 shard + 4,178 hist).
Only ~5% originated in the GC scheduler loop that adaptive cadence
(round 28) already throttles — the rest were structural:

1. **Every manifest/compactions poll listed its directory.** SlateDB's
   sequenced object store discovers "latest version" by LIST. At 1 s
   manifest polls + 500 ms compactor polls across 8 DBs, that is ~6-7
   LISTs/s forever, independent of load.
2. **Every productive GC sweep re-listed its directory.** Busy
   directories never reach the adaptive backoff ceiling, so they paid
   a LIST per sweep indefinitely.

Fixes (fork, branch gc-adaptive-backoff):

- **Probe-cached latest reads.** Version ids are dense at creation
  (CAS create at predecessor+1), so the protocol instance caches the
  newest observed (id, etag, encoded bytes) — seeded for free by its
  own writes — and resolves "latest" by probing id+1 (a GET miss) and
  304-revalidating the anchor. LIST remains the cold-start,
  anchor-deleted, and >8-versions-behind fallback. No caller changes;
  every returned value is freshly fetched or 304-revalidated.
- **Reusable GC inventories** (`list_cache_ttl`, default 1 h via
  GC_LIST_TTL_SECS / HISTORY_GC_LIST_TTL_SECS). Sweeps keep their full
  cadence and re-read every deletion anchor fresh (latest manifest,
  checkpoint pins, replay_after, compaction watermarks — all now
  LIST-free via the probe cache), but the candidate inventory is one
  cached listing, pruned as objects are deleted. Objects created after
  the listing are invisible until refresh — deletion can only be
  *delayed*, never done early. An empty sweep arms an early refresh
  clocked from the exhaustion moment (2× base interval, ≥ 60 s).
- **Bounded-concurrency GC deletes** (WAL + compacted SSTs, fan-out 16).
  Surfaced by this round's verification: deletes ran one object per
  store round-trip — a 40/s ceiling at 25 ms RTT, *below* hot-shard WAL
  churn. The old list-per-sweep design survived only by running at
  exactly that ceiling continuously; any dead window made retention
  diverge.

**What the verification harness caught** (each rerun of the same
capped 30-min soak, each fixed before proceeding):

1. *No-refresh:* a 1 h TTL against the boot-time (near-empty) listing
   suppressed collection for the whole run — deletes 50,877 → 25.
   Fix: refresh when a sweep finds nothing in the view.
2. *Floor-from-listing + min_age inheritance:* drained views fed
   false-empty sweeps into the adaptive backoff and big-min_age dirs
   sat in multi-minute dead zones — end-of-run bucket residual 34,912
   objects vs 10,411 baseline. Fix: floor runs from the exhaustion
   moment and scales with the sweep interval, not min_age (early
   relisting never deletes early).
3. *Serial-delete divergence:* with dead windows in play, the 40/s
   delete ceiling could no longer hide — the live-object census
   (added to the s3lite ledger this round) showed 32,945 retained WAL
   objects while sweep drains stretched 940 → 1,865 → 6,397 candidates
   and starved the shared GC actor. Fix: concurrent deletes.

**Verification (capped 30-min soak, same workload, final binary):**

| metric | GC-cadence baseline | LIST-free | delta |
|---|---|---|---|
| total LISTs | 16,671 | **200** | **−98.8%** |
| total Class A | 77,937 | 61,121 | −21.6% |
| total Class B | 25,772 | 10,847 | −57.9% |
| GC deletes | 50,877 | 57,769 | +13.5% (clears baseline's own residual) |
| end-state live objects | ~10,411 residual | **3,075** (WAL 1,596 ≈ one min_age window) | healthier |
| append integrity / errors | exact / 0 | exact / 0 | unchanged |

Wide-shape confirmation (w100k, 100 active, 15 min, GC-cadence binary
vs LIST-free binary with the sparse-deferral interim policy active):
history-tier Class A 8,891 → **485 total** (LISTs 7,503 → 124), shard
LISTs 31,083 → 334, steady total Class A −21%, appends byte-identical
(p50 47.2 vs 47.7 ms, p99 ~86, 4.14 M ok, 0 errors), scan p50 halved
(27.6 vs 54.8 ms — deferral keeps sparse reads on the shard-log fast
path). The absorb-all Arm C scorecard run is in §5.

## 3. The wedge, root-caused: a shed that could not un-trip

Gate 0 (bench/costab/wedge-liveness.sh) drove a single stream past the
absorber envelope at field posture until goodput collapsed, removed all
load, and required fresh appends within 300 s. It FAILED: 429s forever,
"RSS" frozen at exactly 647,504 KB, with the final store window showing
**zero writes and zero absorber backlog** — a fully drained, idle
process that still rejected everything.

The shed line was comparing against metrics that structurally cannot
decrease:

1. macOS `rss_bytes()` returned `getrusage().ru_maxrss` — the lifetime
   **peak**. One spike over the line = permanent 429 on any dev rig.
2. Current resident_size is also wrong on Darwin: mimalloc surrenders
   freed pages with `MADV_FREE_REUSABLE`, and Darwin keeps them in
   resident_size. Micro-repro: after a 512 MB alloc/free cycle,
   resident = 120 MB while **phys_footprint = 2 MB** (reusable 117 MB).
   The memory was already given back; the gauge refused to see it.
3. mimalloc purges freed OS pages on allocation-path ticks — an idle
   post-overload process never allocates, so purges never run (the
   Linux-relevant component; the field soak7 sjc wedge has this shape).

Fix (e5f7d98): `rss_bytes()` reads task_vm_info.phys_footprint on macOS
(statm unchanged on Linux — MADV_DONTNEED purges genuinely shrink it),
and the 500 ms sampler forces `mi_collect(true)` (≤1/10 s) whenever the
reading is over the shed line, re-measuring immediately after. Retained
idle memory can no longer masquerade as live pressure.

**Gate rerun with the fix: PASS** (a105408), with two findings worth
more than the verdict:

1. **The wedge is unreproducible.** The same conc24 load that froze ok
   at 264k now sustains 334k accepted with footprint flat at ~285 MB
   (ps-RSS ~490 MB — the 130-200 MB gap is OS-reclaimable pages the
   old gauge counted). The 600 MB line never engages.
2. **Under a line it CAN reach (WEDGE_SHED_MB=280), the instance
   self-regulates instead of wedging** — a throttled equilibrium at
   the line (goodput −80%, footprint pinned) that never collapses. The
   gate's detector now counts sustained shedding as its overload
   precondition; the recovery run then shows probes 429 at fp=294 MB
   and five consecutive successes as the footprint drains through
   272 MB — **recovered 197 s before the deadline**.

(Two harness traps fixed en route: probe-stream creation must retry
inside the recovery loop — the shed 429s the create, and a swallowed
one-shot failure made every probe 404, a phantom FAIL; and shed lines
must be placed against the server's own footprint gauge, never ps-RSS.)

## 4. Hash newtypes at the measured confusion seams

`crypto::RouteHash` (stream_hash(name): routing, usage keys, v2 key
prefix) vs `crypto::SegmentHash` (engine hash: lag map, v2 incarnation
slot) — applied to the hist2 keyspace functions, `read_history2`, and
the usage-link join. These are the two seams where a bare-`[u8;16]`
swap already caused a real bug (the absorb-lag join read 0 for its
entire life, docs/COST-WIDE2.md §4) or would silently corrupt every v2
key. Engine internals keep bare arrays; conversion happens at the
boundary. Zero-cost (`repr(transparent)`).

## 5. Memory posture on 1 GiB (measured, honest gauge)

The footprint gauge (task_vm_info / statm, not ps-RSS) is now captured
in every wide-run snapshot. w100k with 100 active at the full perf
knobs (4×32 MiB rings, 64 MiB shared cache, ABSORB_CONCURRENCY 6):

- absorb-all peak footprint **820 MB** (ps-RSS reads 950);
- deferral-policy peak **800 MB** — near-identical, i.e. absorption is
  NOT the memory driver at this shape; rings + 100k stream/registry
  metadata + caches dominate;
- single-hot-stream overload (the gate run) plateaus at ~285 MB.

Consequence for 1 GiB field instances: a 100k-wide tenant on one
instance runs above the 600 MB shed line regardless of absorb policy,
so the posture is (a) trimmed knobs — TAIL_RING_BYTES=16 MiB (−64 MB),
SHARED_CACHE_BYTES=32 MiB (−32 MB), ABSORB_CONCURRENCY=2 — for an
estimated ~650-700 MB peak, (b) the now-honest shed as the guardrail
(it self-regulates and provably recovers instead of wedging), and
(c) fleet-level splitting of very wide tenants as the real fix. The
old belief that this shape "needs ~1 GB RSS" overstated live memory by
the reusable-page gap (~130-200 MB on this rig).

## 6. What remains open

- **Field validation** of everything since 581f1e2 (deferred by
  decision until credentials are provisioned): the shed fix on
  Linux/musl semantics (statm + MADV_DONTNEED), LIST-free behavior
  against real Tigris, v2 economics at field latency.
- **Fork upstreaming** — explicitly out of scope this round; the fork
  branch carries the yield points, adaptive cadence, listing reuse,
  probe-cached latest reads, and concurrent GC deletes, and should
  become upstream PRs.
- Compacted-SST GC still lists at refresh cadence rather than taking a
  compactor-fed exact candidate feed; with the other layers in place
  its residual cost (a handful of LISTs per hour) did not justify the
  plumbing this round.
- The registry still pays one Class A PUT per stream creation (100,001
  per w100k setup) — the dominant remaining per-stream cost, untouched
  by this campaign and priced into stream creation, not retention.


## 7. Round 3: static-audit findings addressed

An external static audit of a0c36fa found two release blockers and one
significant gap that the friendly w100k workload (tiny records, 65,536
accounting cap) could not surface. All were fixed and regression-tested
on this branch; the wide rerun in the table below revalidates them at
100k scale.

1. **Aggregate gather budget** (was: one WriteBatch could hold ~4 GiB;
   oversized frames worse). `ABSORB_GATHER_MAX_BYTES` (default 32 MiB,
   one history memtable) with key+frame+keyed-duplicate accounting;
   non-fitting streams gather on later ticks; an oversized chunk
   proceeds alone. A SOFT budget, not an absolute memory ceiling: the
   one-chunk exception admits a per-stream-cap chunk, and one oversized
   KEYED frame is stored twice — worst case ≈ 2× the largest admissible
   frame plus overhead. Deterministic packing tests.
2. **Usage/limits never fail open** (was: silently unlimited and
   unaccounted past 65,536 streams; the two hot-path counters() calls
   returned unrelated temporaries). Overflow admissions now share one
   conservative bucket and one aggregate counter set; idle tracked
   entries evict (600 s) to restore full tracking; /v1/debug/usage
   exposes tracked_streams + overflow gauges. Tested past the cap.
3. **Billing emitter** advances checkpoints only on append success
   (failed emits retry the accumulated delta) and handles counter
   resets from eviction. Posture stated in-code: best-effort telemetry
   until a durable outbox exists.
4. **Durable dirty-stream index**: `absorbed < next` markers written
   atomically with the tail, cleared on catch-up, scanned once at
   absorber start — the audit's append→crash→new-owner→zero-requests
   scenario now converges with the resident-handle sweep disabled.
5. **Memory**: idle StreamHandle eviction (HANDLE_IDLE_EVICT_SECS,
   default 600, safe via strong-count + the dirty index), registry
   cache bound, KeyCache expired-entry removal + bound, absorber
   `submitted` pruning, metrics collection gated on a flusher, and
   cardinality gauges in /v1/debug/load.
6. **Correctness edges**: gather boundary advances now ride ONE
   `AbsorbedBatch` committer message (deterministic single-batch);
   `history_partition` re-checks closed after init (close race);
   history2 paths derive from the same helper as the shard DB path
   (they previously landed BESIDE the shards/ tree — a pre-GA layout
   fix, breaking for any existing v2 data, of which there is none
   deployed); zero-route legacy streams stay on v1 so a future
   route-range split cannot misclassify their records.
7. **Reporting**: wide-report.py now proves drains from the uncapped
   aggregate backlog gauge and reports the honest footprint gauge; ps
   RSS remains as a labeled fallback column.

**Round-3 validation runs** (same harness, round-3 binary fe30317):

- Capped 30-min soak vs the pre-round-3 binary: statistically
  identical — LISTs 207 vs 208, total Class A 60,192 vs 60,233,
  Class B 10,584 vs 10,562, integrity exact, 0 errors. The fixes cost
  nothing on the hot path.
- w100k absorb-all: **drain proven from the uncapped aggregate gauge**
  (backlog 0 streams / 0 s, deferred 0); **the previously invisible
  population is now accounted** — tracked_streams 65,536 at cap plus
  overflow 34,464 admits / 34,464 records / 35.1 MB, exactly the
  100,000 − 65,536 streams the audit flagged, rate-governed through
  the shared bucket; history Class A 1,461 (gate ≤ 5,000; +258 vs
  pre-round-3, the dirty-marker bytes riding existing batches); total
  Class A +1.6%; appends flat (p50 47.5 / p99 87.3 ms, 4.14 M ok,
  0 errors); honest footprint max **777 MB** (was 820) with end-state
  cardinality bounded and evicting — resident handles 34,012,
  keycache 65,259 (capped), registry cache 14,917, metrics 0 (gated).

Explicitly deferred, with rationale: v1→v2 cutover offsets
(docs/HISTORY-V2.md rollout section) — no production v1 history data
exists; greenfield deployments start on v2, and the boolean flag is
sufficient until a deployment with real v1 data needs migrating.
Newtypes remain scoped to the two measured confusion seams
(RouteHash/SegmentHash); incarnation/partition/offset identities widen
opportunistically as APIs are touched.

## 8. Field validation on Prisma Compute (Frankfurt + US East, 2026-07-30)

First field run of the corrected binary (caada7a, x86_64-musl, fork
323bc1b9): fresh projects/buckets per region, co-located generators,
the standard 10-tier ramp (conc 1→64, 180 s/tier), field posture
(32 MiB ring, 600 MB shed on the honest statm gauge, group-commit +
post-ACK gather, RESOLV_OVERRIDE per the soak-3 DNS forensics).
Harvested tables in the soak workspace; headline:

| region | PoP | records | errors | append p50 (best/steady) | append p99 | notes |
|---|---|---|---|---|---|---|
| eu-central-1 | fra | 3,977,740 | **0** | 40 / 81 ms | 244-284 ms | throttles at conc≥48 are the per-stream limiter (~490 req/s ceiling), not the shed |
| us-east-1 | ewr | 799,010 | **0** | 228 / 463 ms | 1.0-2.2 s | the known ewr↔Tigris(iad) distance; store ops 5-15× fra's (put:manifest 225 vs 46 ms p50) — platform geography, not a regression, and better than the 456 ms historical record |

What the deferred field items each showed:

- **Integrity**: server-durable ≥ client-acked in both regions (fra
  +520 records, ewr +640 — ambiguous-timeout retries that committed;
  the safe direction). Zero errors across 4.78 M records.
- **LIST-free posture on real Tigris**: final 60 s windows show **3
  LISTs per region, `list:wal` = 0** — the probe cache + listing-reuse
  behavior transfers from the s3lite twin to the real store.
- **Linux shed semantics**: the statm footprint gauge reads sane
  (265 / 246 MB post-load), the shed never tripped (admit_shed 0), and
  the throttling observed was the per-stream limiter doing its job.
- **Memory/eviction in vivo**: end-of-run cardinality — 1 resident
  handle, 1 usage entry, metrics 0 — the eviction stack behaves on
  musl/mimalloc exactly as on the dev rig.
- **v2 absorption keyless against real Tigris**: absorb backlog 0 in
  both regions at end of ramp.

All infrastructure torn down and verified (services destroyed, buckets
and projects deleted, zero soak30 projects remaining). Remaining field
scope for the fleet phase: multi-instance fleet mode (FLEET_PREFIX),
wide-cardinality shapes in-region, and the ewr latency SLO question —
a placement/routing decision, not a Streams code question.

## 9. Round 4: the final static review, addressed

The round-3 review verdict was "genuinely and thoughtfully addressed,
but the loop is not complete": one new release-blocking memory risk and
several liveness/observability gaps. All six items in its release
sequence are now closed.

**P0 — AbsorbedBatch could expand into an unbounded trim batch.** One
gather covers up to 1,024 streams; each advancing `Absorbed` op used to
emit up to `max_trim_per_op` deletes inline, so a second absorption
wave across mature streams could build ONE shard WriteBatch of 1,024 ×
65,536 = 67M deletes at the fleet posture (~multi-GiB). First
absorptions owe no trims (`prev_absorbed == 0`), which is exactly why
w100k never saw it. Fix: boundary publication and physical trimming are
decoupled. The tail persists `trim_safe_to` (the previous absorbed
boundary — the same one-advance reader lag as before); an advance
records the target and trims only what a GLOBAL per-commit budget
(`TRIM_GLOBAL_BUDGET`, default 65,536 deletes) still allows; the
remainder becomes trim debt drained by a `TrimTick` maintenance op the
5 s flush ticker queues, round-robin across streams, same budget per
commit. Telemetry: `/v1/debug/load` `trim` {debt_streams,
deletes_last_batch, deletes_max_batch, deletes_total}. DST: mature
second wave over 24 streams with the per-stream cap maxed proves the
GLOBAL bound binds (max ≤ budget, mutation-verified), boundaries
advance immediately, debt drains via the production ticker, every owed
offset trims exactly once, markers clear. Bench: run-mature.sh (below).

**P1 — budget-deferred streams fell out of pending.** The gather
correctly skipped streams past the byte budget, but the pump then
removed every lane member from `pending` — deferred streams lost their
lag entry and waited for the ~60 s handle sweep. `absorb_gather_v2` now
returns a per-stream classification (`advanced` / `no_work` /
`deferred_budget`); the pump retires only the first two. Deferred
streams keep entry, age and lag, and gather next tick. Mutation-
verified against a deadline below the rescan cadence.

**P1 — restart rediscovery now survives production defaults.** The
dirty marker carried only (absorbed, next); startup estimated pending
bytes as records × 1 KiB, so one unabsorbed 32 MiB record read as 1 KiB
— below both default thresholds, never absorbed again without a
customer request. The tail now maintains exact `unabsorbed_bytes`
(appends add stored frame lengths; Absorbed ops subtract the bytes the
absorber actually copied, both v1 and v2), and the startup scan reads
marked streams' tails for the truth. A failed scan used to log-and-
forget — permanently stranding pre-restart streams; the scan now runs
inside the tick loop with exponential backoff until it succeeds, plus a
low-cadence rescan (~10 min) as protection against runtime handle
eviction and dropped signals. Three DSTs under TRUE defaults: a 5 MiB
pre-restart record absorbs with no request; two injected scan failures
then success converges; a 512 B sparse record stays deferred AND
reported (`deferred_sparse` ≥ 1) rather than absorbed or dropped.

**P1 — pending summaries survive shard movement.** The absorber exit
path cleared per-stream and per-shard lag but not its PENDING_SUMMARY
row, so after a move the frozen row double-counts against the new
owner's and the fleet rollup reports phantom backlog — and wide-report
uses that rollup as drain proof. `clear_absorb_pending_summary()` now
runs on absorber exit; DST covers publish → close → row gone.

**Usage accounting sharpened (still best-effort billing by posture).**
`admit_append` now returns the `Arc<Counters>` chosen atomically with
admission and the append path carries that one Arc through both count
sites — a concurrent eviction/promotion can no longer split one
request's accounting across the overflow aggregate and a fresh tracked
entry. Counters carry a generation id; the billing emitter treats a
generation change as a reset and bills the fresh cumulative (value-
regression detection missed evict → return → regrow-past-checkpoint and
under-billed it), and prunes checkpoints for streams no longer in the
snapshot after each successful emit (billing memory no longer grows
with every stream ever seen). Overflow-aggregate traffic remains
deliberately un-emitted (no per-stream attribution past the cap) and
process-restart re-bills current cumulatives — the durable-outbox
ledger stays the acceptance bar for invoice-grade billing.

**Memory: capacity cap joins time-based eviction.** `HANDLE_MAX_RESIDENT`
(default 65,536 per shard): past the cap the ticker evicts oldest-
touched unreferenced handles immediately instead of waiting out the
idle window, so a cardinality burst can no longer hold rate × 600 s of
handles. Referenced handles never evict. The 1-GiB posture in
docs/STAGING.md already pins ABSORB_CONCURRENCY=2 and now pins
TRIM_GLOBAL_BUDGET explicitly.

**Precision fixes from the review margins.** The keyed-index row now
costs 43 bytes + routing key in the gather budget (the index key is two
bytes longer than the record key; was under-counted by 2/row);
`gather_max_bytes` is documented as a SOFT budget (the one-oversized-
chunk exception admits ~2× the largest admissible keyed frame, not an
absolute ceiling); the dirty-sentinel comment no longer claims
"unreachable" (p = 2^-128 and the tag byte is the real separator); and
HISTORY-V2.md records the range-split constraint: the maintenance index
must move route-local (or to a tracker partition) before physical
splits land.

**Found and fixed BY this round's validation: the history layout was
never sealed (I1-class).** Looping the acked-records DST 120× on the
round-4 tree failed 10× (8.3%; baseline 0/120) — and the specimen
showed acked records permanently invisible: `completed=true` over
[0,80) with offsets {0, 13–17} missing. Root cause (traced classify →
pass → advance): the absorber's lane classification decides a
PERSISTENT layout from a racy snapshot. A pending entry created in the
commit-to-dispatch window (round 4's dirty-index seed/rescan reads
commit-visible DB state; on round-3 binaries only a restart-time scan
could do it) sees route==0 → the zero-route guard picks the v1 lane;
one tick later a stale absorbed==0 re-admits the v2 lane; the two
interleave, a v1 advance lands under the v2 flag, and its range exists
only in the per-stream v1 DB — which a flagged-v2 stream's reads never
consult. Three-part fix: (1) the COMMITTER seals the layout at the
first advance (v2 advances require a fresh boundary or the v2 flag; v1
advances require the flag unset; violators are dropped whole — the
range stays in the shard log below an unmoved boundary, so nothing is
lost, and `absorb_lane_dropped` in /v1/debug/load counts every drop);
(2) lane eligibility reads the APPLIED tail, which is updated
synchronously at commit and cannot race dispatch; (3) the absorber's
submitted floors are lane-scoped, so a dropped lane's mark can never
make the surviving lane skip a range that only exists in the dropped
tier. Deterministic seal DST both directions + the acked-records loop
clean after the fix. Round-3 field binaries carried the narrow
restart-window variant; nothing in the fra/ewr runs matches its
signature (single continuously-appended streams classify long after
dispatch), and the seal closes it everywhere.

**Validation round 4.** Full suite green (115 tests; the new coverage
is mutation-verified — each headline fix was reverted in isolation and
its test failed). One pre-existing test fragility fixed along the way:
`history_reads_reuse_a_cached_reader` raced the absorber's cadence
under full-suite CPU load (an absorb advance between drains converts a
cache hit into a stale reopen); it now waits for full absorption before
draining — same assertions, deterministic setup. Validation numbers
below; the capped-soak parity row is appended when that run completes.

**Mature second-absorption stress (bench/costab/run-mature.sh —
the review's required pre-promotion scenario).** 1,024 streams × 2,048
records absorbed to maturity (wave 1: zero trims owed, as designed —
why w100k never saw the bug), then a 384 KiB wave-2 append per stream
(crossing the sparse-deferral bar) re-absorbs every stream and makes
2,097,152 offsets trimmable at once, under the fleet posture the review
flagged (TRIM_PER_OP=65536). Depth is scaled (~2k/stream vs the
review's 65k — the budget bound is depth-independent; the DST twin
pins the per-stream-cap interplay with the cap maxed):

| gate | result |
|---|---|
| max trim deletes in ANY commit | **65,536 = budget, never exceeded** (old code: 2.1M in one batch here; 67M at fleet shape) |
| boundary/trim decoupling | boundaries advanced immediately; peak 896 streams of trim debt |
| convergence | debt → 0 via the production 5 s ticker; totals quiet at 153 s |
| exactness | deletes_total = 2,097,152 = owed, each offset once |
| memory | gauge sawtooths 786→1,127→~920 MB (compaction churn across 4 shard DBs), < 1,300 gate, < 1,400 shed; wave-2 ingest itself steady ~830 |
| integrity | 32 sampled streams read back 2,054/2,054 records end-to-end |

**Full field ladder (bonus): 1..64 × 180 s survived whole.** The ladder
that historically wedged the process at conc6+ (history flush stall →
RSS through the shed → 429-forever) now completes end-to-end:
2,333,024 records, ZERO errors, p50 flat at 47 ms through conc64,
throughput ceiling ~490 req/s = the per-stream limiter doing its job
under 14.6 M throttles, RSS plateau ~602 MB, LISTs 207 total
(LIST-free posture parity), zero layout-seal drops.

**w100k rerun (the handle cap actively engages at 100 k streams).**
Steady Class A 98,269 vs round 3's 96,906 (+1.4%, parity); honest
gauge 783 vs 777 MB; setup 1.21 Class A/stream unchanged; zero
errors/throttles; sparse policy intact (99,900 deferred). The new
machinery in vivo: resident handles END at 1,154 (pre-cap: ~100 k
resident), usage/keycache pinned at their 65,536 caps, trim budget
held silently through 1.59 M wide-run deletes (max batch 49,670 ≤
65,536), zero seal drops. hist-tier Class A fell 1,461 → 424: round
3's runs were quietly paying per-stream v1-DB costs for streams the
racy pre-seal classification misrouted — the layout fix is also a
cost fix.
