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

All gates pass on the final binary: full 100k drain in-window (backlog
0, deferred 0), appends byte-flat across every arm (p50 46.9 / p99
88.0 ms, 4.14 M ok, 0 errors), absorbed cold scans 54.8 ms (1.96× the
28 ms unabsorbed control, within the ≤2× gate), history LISTs 128 for
the whole run. End-to-end: absorbing a sparse stream's history now
costs ~3,600× fewer Class A requests than v1, and steady-state total
request volume is 7.1× lower than where the campaign started.

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
