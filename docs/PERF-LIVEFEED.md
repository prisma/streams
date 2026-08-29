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
| `10x100` (product target) | **Parity.** Append p99 −4.8%, delivery p99 0%, CPU/delivery ±0%, idle RSS −10% for livefeed. |
| `1x1000` (max sharing) | Parity (delivery +6%, CPU +1.6%). |
| `1000x1` (breadth) | Dual-commit livefeed dipped (+30% append p99, +12% CPU); **the final RC recovered it fully** (C ≈ A). |
| `500x2`, `100x10` (many shared feeds) | Delivery p99 3–8× worse on livefeed **under the default retention budgets** — see §2; with adequate retention: parity. |

- Memory per subscriber: **43–60 KB across every shape and arm**
  (matches the workload-cert 55–70 KB estimate). Livefeed ≤ legacy.
- CPU per delivered record: ~400–500 µs at fanout 10–100, ~330 µs at
  fanout 1000, ~800–1400 µs at fanout 1–2 (breadth-dominated);
  livefeed ≤ legacy at most shapes (−9% at `500x2`).
- Idle: 1,000 parked subscribers cost no measurable CPU on either
  engine; RSS flat over the 10-minute idle windows; teardown returns
  feeds/reserved bytes to zero every run.

**B vs C regression gate (±5%):** met at the product shapes; at
`1000x1` C is BETTER than B (the 11.8 hardening); the only
above-noise drift is `1x1000` delivery p99 +23% (95→116 ms, two runs
each, absolute values small) — noted, not actioned.

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

## 3. The 1-GiB envelope (arm C, production-share write load)

Ladder at ~300 writes/s (the per-instance share of the production
workload), writes confined to 100 streams, subscribers scaling:

| Rung | Result | Idle RSS | Peak RSS |
|---|---|---|---|
| `1000x1` | PASS | 86 MB | 389 MB |
| `2500x1` | PASS | 135 MB | 417 MB |
| `5000x1` | FAIL (peak 498 MB, 440 admission sheds) | 206 MB | 498 MB |
| `25x100` (2,500 subs) | PASS | 111 MB | 284 MB |
| **`100x100` (10,000 subs)** | **PASS** | **307 MB** | **399 MB** (p99 54 ms, zero reconnects) |

- Memory model: **base ≈ 46 MB (post-create) + ~26 KB/connection +
  ~8–10 KB/feed.** The `5000x1` failure is the write-phase absorber
  transient stacking on 206 MB of feed+connection state — the ceiling
  is FEED COUNT (breadth), not connections: 10,000 connections at
  fanout 100 pass with 101 MB of headroom.
- **`S_server_instance`:** ≥ **10,000** at the product fanout
  geometry; **2,500** at the all-solo worst case, on the current
  profile. Safe `SSE_MAX_CONNECTIONS`: **2500** as the
  geometry-agnostic profile cap (up from the 1200 placeholder);
  fanout-dense cells can run 10k+ behind a geometry-aware admission.
- **Fleet sizing for 100k subscriptions** (`K = ceil(100k/(0.7·S))`):
  **15 instances** at the product geometry; 58 at the all-solo worst
  case. The earlier plan's placeholder assumed the public-edge wall;
  these are direct-instance numbers.
- Dominant bottleneck, in order: write-transient RSS at high feed
  breadth (absorber machinery, unchanged by the rewrite); shared-feed
  retention depth (§2, config fix); CPU at ~0.4–0.8 core per 1,000
  deliveries/s.

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
