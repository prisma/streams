# LiveFeed performance characterization (round 12)

Controlled three-arm study answering the round-12 charter: the
economic and operational envelope of the LiveFeed engine after the
rewrite. Harness, raw manifests and 10-second series:
`bench/livefeed-perf/` (all runs reconcile deliveries EXACTLY per
subscriber; a non-PASS run is not evidence and none is cited here).

**Arms.** A = legacy direct/LiveHub, B = livefeed, both the SAME
dual-engine binary at `1834b726` (engine chosen by env); C = the
certified `v0.2.0-rc.2` artifact (`1eb36b10…`) at `3a8016e6`.
**Environment.** linux/amd64 (Rosetta), s3lite store at 2 ms, the
server alone in a 1-CPU / 1-GiB cgroup (nofile 32768), the pinned
compute-1g memory knobs, fresh bucket/process per run, serial runs in
`A B B A C C` order per shape. Host ran unrelated workloads; the
alternation + per-cell medians absorb that noise.

## 1. Legacy versus LiveFeed (density sweep, 1,000 subscribers)

| Shape | Verdict |
|---|---|
| `10x100` (product target) | **Parity** on delivery p99 (±0%) and idle RSS (−10% for livefeed). Append-p99 comparison **withdrawn** pending the recompare (below). |
| `1x1000` (max sharing) | Parity (delivery +6%). |
| `1000x1` (breadth) | Dual-commit livefeed dipped (+12% full-run CPU); the final RC recovered it (C ≈ A). Append-p99 leg of this claim **withdrawn** pending the recompare. |
| `500x2`, `100x10` (many shared feeds) | Delivery p99 3–8× worse on livefeed **under the default retention budgets** — see §2; with adequate retention: parity. |

**Measurement retractions (review 2026-08-29).** Two harness defects
invalidate specific numbers from this sweep, and they are withdrawn,
not restated: (1) background appends recorded ~0 ms latency samples
(an unassigned timer variable), so at `10x100` ~95% of mixed-phase
append-latency samples were bogus — every cross-arm **append p99**
comparison from this sweep is untrustworthy; (2) "CPU per delivered
record" divided WHOLE-RUN CPU (creation, idle, teardown included) by
four phases' deliveries — it was a full-run cost indicator, not
µs/delivery, and is relabeled as such. The harness now records
per-attempt timing with split subscribed/background histograms,
per-phase CPU windows against the proc series, an open-loop scheduler
with scheduled-intent denominators, and unique-vs-resume-overlap
delivery counts; the affected shapes are being re-measured.

- Memory per subscriber: **43–60 KB across every shape and arm**
  (matches the workload-cert 55–70 KB estimate). Livefeed ≤ legacy.
- Full-run CPU (rough indicator only, see retraction 2): livefeed ≤
  legacy at most shapes; per-phase CPU/delivery numbers will come
  from the re-measurement.
- Idle: 1,000 parked subscribers cost no measurable CPU on either
  engine; RSS flat over the 10-minute idle windows; teardown returns
  feeds/reserved bytes to zero every run.

**Reconciliation contract, stated exactly:** every ACKED record was
observed by every subscriber (zero missing gates the verdict);
delivery is exactly-once within a connection and at-least-once across
resume — resume-overlap volume is measured and reported separately,
and duplicate-free delivery across resumes is NOT asserted.

**B vs C regression gate (±5%):** met at the product shapes on the
surviving metrics; the only above-noise drift is `1x1000` delivery
p99 +23% (95→116 ms, two runs each, absolute values small) — noted,
not actioned.

## 2. The one real finding: shared-feed retention depth

At hundreds of SHARED feeds in one project, the per-project retention
backstop (`SSE_FEED_PROJECT_BYTES`, defaulted to global/4 = 4 MiB in
round 10) saturates; every over-cap publication clears that feed's
ring and floors parked followers → typed lag-disconnect + resume
churn. Zero loss (the resume contract holds exactly — one sweep ran
64,028 cut/resume cycles with zero missing records), but delivery p99
inflates 3–8× and reconnect work amplifies CPU. The legacy hub served
the same pressure uncached in place — this is livefeed's one
deliberate contract divergence made visible.

Causal chain proven by intervention at `500x2` (arm C):

| Config | Lag/resume cycles | Mixed delivery p99 |
|---|---|---|
| 16 MiB global / 4 MiB project (pinned) | 64,028 | 354 ms |
| 64 MiB global / 4 MiB project | 64,028 | 352 ms (project cap binds) |
| 64 MiB global / 16 MiB project | 16,742 | 333 ms |
| 64 MiB global / 64 MiB project | **1,556** | **137 ms ≈ legacy 134 ms** |
| `1250x2` deep retention | **0** | **35 ms** |

**Recommendation (E4 closure):** raise the 1-GiB profile to
`SSE_FEED_TOTAL_BYTES=67108864` and `SSE_FEED_PROJECT_BYTES=33554432`
(the backstop stays meaningful for isolation but stops binding at
realistic shared-feed counts). Retention is exactly accounted; the
memory headroom exists (10,000 parked subscribers idle at 307 MB).
Do NOT build a dedicated shared-feed reader: with adequate retention
the realistic shapes are at parity or better.

**Review caveat (2026-08-29):** the table above measured project caps
of 4/16/64 MiB — there is no measured point at the SHIPPED 64/32
combination (the canary proved correctness and survivability under
64/32, not that the 500x2 latency regression is gone at exactly
32 MiB). A measured 64/32 `500x2` point is being produced. Note also
that this local benchmark puts all 500 shared feeds in ONE project —
an intentionally hostile single-tenant geometry; the multi-project
field legs (§4 follow-ups) test whether 32 MiB is even the binding
knob at the product's tenant distribution.

## 3. The 1-GiB envelope (arm C — local container, provisional)

**Scope correction (review 2026-08-29):** these are
`S_local_container` numbers — linux/amd64 under Rosetta, s3lite at
2 ms, auth off, effectively one project. `S_server_instance` on real
Compute is pending (and currently blocked above ~1.4k by the edge
wall, §4). A second defect: the ladder's write rate was DERIVED from
the delivery target, so the all-solo rungs received ~6× the write
load of the fanout rungs — the breadth-vs-fanout attribution is
strongly suggested, not cleanly established. The ladder is being
re-run on two independent axes (fixed 300 w/s residency axis; fixed
delivery-rate throughput axis at the product geometry, whose 10k
deliveries/s leg is the product-throughput receipt).

Original ladder (write rates NOT equalized across geometries):

| Rung | Result | Idle RSS | Peak RSS |
|---|---|---|---|
| `1000x1` | PASS | 86 MB | 389 MB |
| `2500x1` | PASS | 135 MB | 417 MB |
| `5000x1` | FAIL (peak 498 MB, 440 admission sheds) | 206 MB | 498 MB |
| `25x100` (2,500 subs) | PASS | 111 MB | 284 MB |
| **`100x100` (10,000 subs)** | **PASS** | **307 MB** | **399 MB** (p99 54 ms, zero reconnects) |

- Memory model: **base ≈ 46 MB + ~26 KB/connection + ~8–10 KB/feed —
  provisional.** The density sweep holds subscribers constant at
  1,000, which makes base and per-subscriber cost mathematically
  inseparable from those points alone; the ladder points restore
  identifiability but the published fit did not document its point
  set. The analyzer now emits the exact fit set, residuals and R²;
  the re-fit publishes with the two-axis rerun.
- **`S_local_container`:** ≥ **10,000** at the product fanout
  geometry; **2,500** at the all-solo worst case (at the ladder's
  unequal write rates — see the scope correction). The `5000x1`
  failure at 6× the fanout rungs' write load strongly suggests, but
  does not cleanly establish, that feed breadth is the binding axis.
- **`SSE_MAX_CONNECTIONS`: the 1200 profile pin STANDS.** The earlier
  2500 recommendation is withdrawn: one static cap cannot express
  both envelopes — raised to 10,000 it admits the all-solo geometry
  that fails near 5,000; held at 2,500 the 15-instance plan is
  unreachable. The follow-up code item is a weighted
  subscription-memory admission budget (connections ×
  certified-connection-bytes + feeds × certified-feed-bytes +
  retained bytes, with write-transient headroom), which can admit
  10,000 connections over 100 feeds while refusing 10,000 singletons.
  Until it exists, the universal cap is whatever the worst-case
  breadth axis certifies.
- **Fleet sizing for 100k subscriptions is a PLANNING ESTIMATE, not a
  certified size:** ~15 instances at the product geometry ASSUMES the
  geometry-aware admission above; with today's single static cap the
  honest arithmetic is `100k/(0.7×S_universal)` — ~58 instances at
  the current all-solo figure.
- Dominant bottleneck, in order: write-transient RSS at high feed
  breadth (absorber machinery, unchanged by the rewrite); shared-feed
  retention depth (§2, config fix); CPU (full-run indicator;
  per-phase numbers pending re-measurement).

## 4. Field campaign (2026-08-29, real Compute + real Tigris)

Four rungs on real fra Compute instances against the fra Tigris
bucket, `ad4ba1ff` binaries, the SHIPPED profile (64/32 MiB budgets),
out-of-region generators, and the same exact-reconciliation mandate —
awsbench's cert shape now stamps every fanout append with a
per-stream sequence, keeps exact acked/unacked/shed ledgers, resumes
subscribers from the control `nextCursor`, and gates every
generator's clock on a synchronized release
(`bench/soak/evaluate-cert.py` is the gate).

| Rung | Shape | Verdict |
|---|---|---|
| F1 `1200x1` solo, 15 min | control through the edge | **Integrity + server EXACT-PASS**: 275,654 deliveries, 0 loss / 0 dups, server↔client Δ4 (in-flight at freeze), peak RSS 411 MB, 0 sheds. Edge write path FAILS the latency gates (below). |
| F2 `2500x1`, 2 gens | edge-wall probe | **Wall reproduced EXACTLY at 1,536** total conns/origin: one gen froze at 601 while the other climbed until the SUM hit 1,536 (per-origin budget, matching L3d's 619+658+259). Server healthy at the freeze (103 MB, 0 shed). 2.5k–10k rungs stay UNMEASURABLE through this edge. |
| F3 `100x12` combined, 30 min | sustained product mix | Integrity EXACT (547,092 delivered = server count, 0 loss / 0 dups through 18% shed); **found the field bound**: RSS climbs ~20 MB/min at 1,000-stream write breadth on WAN Tigris (cache fill + resident-stream state, NOT retention — reserved peaked 58 MB, 0 uncached, 0 lag-cuts) and crosses the 500 MB shed line at t≈21 min; typed RSS-shed then holds it flat with delivery p50 steady at 159 ms. Protective behavior correct; charter RSS gate exceeded. |
| F4 = F3 + the L1 writes-only diet | diet falsified for subscriber cells | RSS fixed (peak 286 MB, 0 sheds, integrity exact 232,440 = server count) but the full cache trim (−128 MB) put WAN store reads on the append path: **append p50 137 ms → 7.3 s**. The L1 "subscribers want the ring" caveat, quantified — do NOT ship the writes-only diet on subscriber cells. |
| F5 = F3 + tuned trim (−80 MB) | the closing arm | Write path healthy (append p50 120–133 ms throughout); the line-crossing moved t≈21→24 min only. Steady state: RSS pinned 492–495 MB by typed shed (2.05%), 156 lag-disconnects ALL resumed by cursor with complete tails, **674,568 delivered = server count, zero loss / zero dups through the whole pressure stack** — §2's resume contract field-proven under real retention pressure (13 uncached publishes at the plateau). |

**The edge is the field bottleneck, twice over.** (1) The per-origin
connection budget of exactly 1,536 caps any single service at
~1.2–1.4k subscriptions with write headroom — `S_server_instance`
certification above that needs an in-VPC path or a platform budget
change (ask #3, bench/edge-repro/README.md, evidence re-dated
2026-08-29). (2) The edge write path costs append p50 152 ms /
p99 929 ms with ~1% 30-second timeouts (L2i's signature, now typed
exactly), while the server adds only ~30–45 ms commit→delivery on
top — consistent with the local 54 ms p99. The latency SLO
conversation is an edge conversation, not a server one.

**The 1-GiB sustained-combined steady state is the plateau, not a
margin.** At 300 writes/s over 1,000-stream breadth on WAN Tigris,
RSS grows ~20 MB/min (cache fill + per-stream resident state, NOT
retention) until the 500 MB shed line, then typed RSS-shed pins it
there with flat p50s and exact integrity — the R25-H plateau model,
reproduced at wide breadth. Moderate cache trims only delay the
crossing (F5: +3 min for −80 MB); the trim big enough to prevent it
destroys the write path (F4). Consequences: (a) the
`SSE_MAX_CONNECTIONS=1200` pin in compute-1g.env remains correct for
edge-fronted deployments — do NOT raise it until the wall moves;
(b) keep the shipped cache posture — the trims buy nothing worth
their risk; (c) the genuine optimization item, if sustained
wide-breadth combined cells become a real workload class, is
per-stream resident-state cost (absorber/memtable/handle mass), not
caches and not the livefeed engine — whose retention accounting,
typed cutoffs, and cursor-resume contract all held exactly at the
plateau.

Raw manifests: `$SOAK_HOME/results/wc12-20260829T*` (stage JSONs,
RSS timelines, wall-evidence.txt, eval verdicts).

**Directed follow-up legs (review 2026-08-29 — all fit under the
1,536 wall):** (1) `500x2` one project at the exact 64/32 profile;
(2) `100x10` one project; (3) `500x2` across 500 projects, one feed
each; (4) the product distribution, ~one hot subscribed feed per
project across 100–1,000 projects; (5) noisy-project isolation — one
project fills its 32 MiB allowance while compliant victims stay
unaffected; (6) retained bytes driven near the 64 MiB cell cap under
mixed writes. Each records project-cap vs global-cap uncached
publications, lag EOFs/reconnects, unique catch-up records, peak
process RSS and cgroup memory.current, and write shed.

**RC note:** v0.2.0-rc.2 predates the profile change; the certified
system is now server-binary sha + compute-1g profile sha + harness
sha. After the follow-up legs, mint **v0.2.0-rc.3** anchored at the
commit containing the operating profile and harness, with all three
hashes in every campaign manifest.
