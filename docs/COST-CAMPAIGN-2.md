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

| arm | steady total Class A | history Class A | $/1M sparse streams (hist, Tigris) |
|---|---|---|---|
| A: v1 absorb-all | 685,647 | 559,470 | ≈ $215/M (≈43/stream) |
| B: defer sparse (interim policy) | 272,791 | 147,872 | deferral, not absorption |
| C: shared history v2 | 133,808 | 9,490 | ≈ $0.47/M (0.095/stream) |
| C + LIST-free GC (this round) | _pending rerun_ | _pending rerun_ | target ≤ 5,000 hist A (gate) |

v2's residual history cost was 79% GC LISTs (7,489 of 9,490) — the
structural fix below targets exactly that. The v2 layout itself absorbs
100k one-record streams in-window, keyless, with the 321 ms cold-read
cliff gone (55 ms vs 28 ms unabsorbed) and appends flat.

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
  *delayed*, never done early. An empty sweep arms an early refresh at
  max(min_age, 2× base interval); the first soak proved why (a 1 h TTL
  against a boot-time empty listing suppressed collection entirely:
  deletes 50,877 → 25).

**Verification (capped 30-min soak, same workload, LIST-free binary):**

| metric | GC-cadence baseline | LIST-free | delta |
|---|---|---|---|
| total LISTs | 16,671 | _pending_ | — |
| total Class A | 77,937 | _pending_ | — |
| total Class B | 25,772 | _pending_ | — |
| GC deletes | 50,877 | _pending_ | must stay ≈ flat |
| append integrity | exact | _pending_ | must stay exact |

(The tainted first rerun — no refresh floor — measured LISTs 16,671 →
937 and Class A −21%, but with collection broken; the numbers above
will be from the corrected binary.)

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

**Gate rerun with the fix:** _pending_ (target VERDICT=PASS).

## 4. Hash newtypes at the measured confusion seams

`crypto::RouteHash` (stream_hash(name): routing, usage keys, v2 key
prefix) vs `crypto::SegmentHash` (engine hash: lag map, v2 incarnation
slot) — applied to the hist2 keyspace functions, `read_history2`, and
the usage-link join. These are the two seams where a bare-`[u8;16]`
swap already caused a real bug (the absorb-lag join read 0 for its
entire life, docs/COST-WIDE2.md §4) or would silently corrupt every v2
key. Engine internals keep bare arrays; conversion happens at the
boundary. Zero-cost (`repr(transparent)`).

## 5. What remains open

- **Field validation** of everything since 581f1e2 (deferred by
  decision until credentials are provisioned): the shed fix on
  Linux/musl semantics, LIST-free behavior against real Tigris, v2
  economics at field latency.
- **1 GiB posture profile** — _pending this campaign's final runs_.
- **Fork upstreaming** — explicitly out of scope this round; the fork
  carries three patches (yield points, adaptive cadence + listing
  reuse, probe-cached latest reads) that should become upstream PRs.
- Compacted-SST GC still lists at refresh cadence rather than taking a
  compactor-fed exact candidate feed; with the other layers in place
  its residual cost did not justify the plumbing this round.
