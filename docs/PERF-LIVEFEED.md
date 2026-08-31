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

| Shape | Verdict (re-measured 2026-08-29) |
|---|---|
| `10x100` (product target) | **Parity**: subscribed append p99 A 26.0 / B 25.5 / C 25.5 ms; delivery p99 ±0%; idle RSS −10% for livefeed. |
| `1x1000` (max sharing) | Parity (delivery +6%). |
| `1000x1` (breadth) | Dual-commit dip was real and larger than first reported (**+38% append p99**, 442→610 ms); the final RC recovers it fully (C 434 ≈ A 442 ms). |
| `500x2`, `100x10` (many shared feeds) | Delivery p99 3–8× worse on livefeed **under the old retention budgets** — see §2; retention-bound, not read-path-bound. |

**Re-measured (review 2026-08-29; two harness defects fixed).** The
original append-p99 numbers were withdrawn — background appends
recorded ~0 ms samples against an unassigned timer, and CPU/delivery
divided whole-run CPU by four phases' deliveries. With per-attempt
timing (split subscribed/background histograms), per-phase CPU
windows against the proc/cgroup series, an open-loop scheduler with
scheduled-intent denominators, and unique-delivery counts, the
affected shapes were re-run (2 runs/arm, alternated):

- `10x100` subscribed append p99: **A 26.0 / B 25.5 / C 25.5 ms**
  (A→B −1.9%, B→C 0%) — parity re-established on honest data.
- `1000x1` subscribed append p99: **A 442 / B 610 / C 434 ms** — the
  dual-commit dip was real and LARGER than first reported (+38%, not
  +30%), and the final RC fully recovers it (C ≈ A, −1.8%).
- Per-phase fanout CPU per unique delivery: **~142–152 µs at
  fanout 100; ~700 µs at breadth 1** (breadth-dominated). A→B −2.3%.
- New B→C flag at `10x100`: +7.4% CPU/delivery and +6.2% idle RSS
  (n=2, small absolutes) — joins the noted-not-actioned B→C drift
  family (plausibly the 11.8 raw-pairing hardening).
- Idle-phase CPU, now measured per-phase: **~11.6% of a core at
  1,000 parked subscribers** (~116 µs/sub/s of heartbeat+timer cost),
  identical across all three arms. The earlier "no measurable idle
  CPU" claim was an artifact of whole-run accounting and is
  corrected. **This is a first-class capacity dimension:** it is
  engine-independent (HTTP/socket/heartbeat work, not LiveFeed
  reading), and IF it scaled linearly, 10,000 parked connections
  would idle at ~one full core on a 1-vCPU instance. Native Compute
  may be materially cheaper than this Rosetta container — the native
  heartbeat CPU slope must be measured before any 10k-connection
  planning number is used, and `SSE_MAX_CONNECTIONS` stays a
  CPU/socket safety boundary independent of any memory-based
  admission.
- Append-latency labeling: histograms measure LAUNCHED
  attempts/responses; client-concurrency drops are accounted
  separately against scheduled intent and never enter the latency
  distributions.
- Memory per subscriber: **43–60 KB across every shape and arm**
  (matches the workload-cert 55–70 KB estimate). Livefeed ≤ legacy.
- Teardown returns feeds/reserved bytes to zero every run.

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

**Status: 64/32 is the CURRENT CANDIDATE profile, not a certified
final** — the six multi-project field legs (§4) are the decisive
evidence for whether it fits the production tenant distribution; the
local evidence below is a single-project stress geometry.

**Measured 64/32 point (2026-08-29, two runs, review-directed):** at
the SHIPPED 64/32 combination the hostile `500x2` shape is **NOT at
parity**: mixed delivery p99 321 ms (vs 137 ms at 64/64 and 134 ms
legacy; improved from 354 ms at 16/4), ~5.5k typed lag-disconnects and
~7.3k cursor resumes per run — zero missing records and ZERO resume
overlaps, peak RSS 511–531 MB. Reading: the 32 MiB per-project
backstop BINDS when a single tenant holds 500 hot shared feeds — the
isolation cap doing its declared job against an intentionally hostile
single-tenant geometry, at the cost of typed churn, never loss. Do
not retune from this point alone: the product target spreads feeds
across many projects, where only the 64 MiB global cap is in play —
the multi-project field legs (§4 follow-ups, legs 3–5) are the
decisive measurement for whether 32 MiB is the right backstop.

## 3. The 1-GiB envelope (arm C — local container)

**Scope:** `S_local_container` numbers — linux/amd64 under Rosetta,
s3lite at 2 ms, auth off, effectively one project. `S_server_instance`
on real Compute is pending (blocked above ~1.4k by the edge wall,
§4). The original ladder derived its write rate from the delivery
target (all-solo rungs got ~6× the fanout rungs' writes) and its
closed-loop writer could shed offered load silently; it was re-run
2026-08-29 on two independent axes with the open-loop scheduler, the
SHIPPED 64/32 MiB budgets, and gates on scheduled intent.

**Residency axis — fixed 300 w/s (100 subscribed + 200 background) at
EVERY geometry:**

| Rung | Result | Idle RSS | Peak RSS | mixed dl p99 |
|---|---|---|---|---|
| `1000x1` | PASS | 90 MB | 362 MB | 33 ms |
| `2500x1` | PASS | 131 MB | 347 MB | 39 ms |
| **`5000x1`** | **PASS** | 208 MB | 405 MB | 51 ms |
| `7500x1` | FAIL (RSS 487 MB, 840 typed sheds) | 300 MB | 487 MB | 69 ms |
| `2500x2` attr | FAIL (RSS 471 MB only; 0 shed) | 193 MB | 471 MB | 52 ms |
| `50x100` attr (10k del/s) | FAIL (dl p99 1,027 ms — CPU) | 177 MB | 468 MB | 1,027 ms |
| `100x100` @1k del/s | FAIL (RSS 473 MB only; 0 shed, dl p99 73 ms) | 304 MB | 473 MB | 73 ms |

- **The old `5000x1` failure was the write-rate confound**: at equal
  load it PASSES with 45 MB of gate headroom. The all-solo envelope
  is **S_local_container(worst-geometry) = 5,000**, double the
  confounded figure; the binding axis at 7,500 is total resident
  state (per the model below), not breadth per se.
- **The shipped 64/32 budgets cost the 10k-conn geometry its
  450 MB-gate margin**: `100x100` now peaks 473 MB at just 1k del/s
  (vs 399 MB at the old 16/4 budgets) — zero sheds, under the 500 MB
  line, delivery p99 73 ms, but no longer inside the conservative
  gate. Retention headroom and connection headroom trade against the
  same GiB.
- **Open-loop artifact receipt** (`results-residency-openloop-1200wps`):
  `1000x1` at a TRUE sustained 1,200 w/s offer runs 27% typed shed,
  dl p99 334 ms, peak 512 MB — quantifying exactly what the
  closed-loop writer used to hide. The original ladder's nominal
  1,200 w/s PASSes were partly writer self-throttling.
- Memory model, FINAL (documented fit over 8 independent geometries,
  1k–10k subscribers × 10–7,500 feeds, both axes varied —
  identifiable): **RSS ≈ 46.4 MB + 26.28 KB/connection +
  7.95 KB/feed, R² 0.9987, max residual 5.3 MB.** Point set and
  residuals in the analyzer output (`analyze.py … --fit-extra`).

**Delivery-throughput axis — fixed delivery targets, RSS-light
`25x100` geometry (2,500 conns; never a feed-memory claim):**

| Target | fanout dl p99 | mixed dl p99 | CPU/unique delivery | Peak RSS |
|---|---|---|---|---|
| 1,000/s | 33 ms | 44 ms | 157 µs | 238 MB |
| 2,500/s | 36 ms | 56 ms | 74 µs | 274 MB |
| 5,000/s | 51 ms | 77 ms | 54 µs | 294 MB |
| **7,500/s** | **72 ms** | **106 ms** | **48 µs** | **338 MB** — PASS |

- **CPU per delivery AMORTIZES DOWN with rate** (batch effects):
  157 → 48 µs across the ladder; at 7,500 del/s the delivery path
  uses ~0.4 core. The delivery path is NOT the 1-CPU bottleneck up to
  at least 7,500 del/s at this geometry.
- The 10,000 del/s failure at `50x100` (5,000 conns, dl p99
  1,027 ms) happened with peak RSS 468 MB — tail collapse near the
  RSS line, where allocator/compaction pressure bites, not raw
  delivery CPU. **Delivery throughput and resident memory are
  coupled**; the product-throughput receipt (10k del/s at product
  distribution) needs either headroom below the line or the
  admission budget below.
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
  certified size.** With today's single static cap, `S_universal` by
  the conservative 450 MB gate is geometry-dependent between 2,500
  (every geometry passes) and 5,000 (all-solo passes; 5,000-conn
  SHARED geometries peak 468–473 MB — over the gate, under the line,
  zero sheds): **29–58 instances** by strict-gate arithmetic. The
  ~15-instance figure requires the geometry-aware admission budget
  above.
- Dominant bottleneck, in order: **total resident state against the
  1-GiB line** (connections × 26 KB + feeds × 8 KB + retention +
  write transients — the model above, and what tail-collapsed the
  10k del/s leg); shared-feed retention depth at hostile
  single-project geometries (§2); delivery CPU only beyond ~7.5k
  del/s (and it amortizes down with rate).

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

**The six directed legs (run 2026-08-30 on real fra Compute + Tigris,
`ae020ad9` harness, 64/32 profile, exact reconciliation throughout —
zero lost acked records in EVERY leg):**

| Leg | Shape | Retention verdict |
|---|---|---|
| 3 | `500x2`, 500 projects | **CLEAN**: zero project-cap hits, zero global uncached, zero lag-cuts, zero reconnects. The isolation model's intended geometry never touches the caps. |
| 1 | `500x2`, ONE project | Project cap binds as declared: 67 project-cap uncached, 134 typed cuts (~0.2% of 57k deliveries), all cursor-resumed, global cap never binds. |
| 2 | `100x10`, ONE project | **The decision datum: the 32 MiB cap still binds at ordinary fanout breadth** — 13 project-cap uncached, 130 cuts (0.045% of 287k deliveries), lossless. |
| 4 | Product distribution, 30 min | Caps effectively clean (0 project-cap hits across 500 projects; 207 global uncached only after the shed storm began). The binding constraint is the KNOWN 1-GiB resident-state plateau: RSS crossed the line at t≈20 min → 49.5% typed shed, integrity exact throughout, p50s steady 166/197 ms. |
| 6 | Global-cap ceiling | **Exact**: reserved bytes pinned at precisely 64.0 MB, zero accounting drift, project caps correctly bypassed, RSS bounded (401 MB), write shed 0%, teardown to zero. At-cap operation degrades delivery p99 to ~17 s (churn + WAN catch-up) — losslessly. |
| 5 | Noisy-project isolation | **Retention isolation PERFECT / memory isolation ABSENT.** The noisy project (100 shared feeds, 100 w/s × 8 KB) took every retention consequence itself: ALL 2,971 lag-cuts, ALL 1,488 project-cap uncached, and all 1,106 catch-up-in-progress records at freeze are noisy-class; victims: ZERO cuts, ZERO uncached, tails 100% complete. BUT the noisy write volume drove the INSTANCE to the RSS plateau (506 MB → 18.4% admission shed), and admission shed is instance-global — victim writers shed too. Per-project admission weighting is the missing isolation layer (the MT-TENANTS "10>1" finding, reproduced from the retention side). |

Every leg also reproduces the standing edge write tail (append p99
~1.0–1.2 s through cv-*), which fails the 250 ms latency gate
independently of retention behavior.

**Profile decision input (the review's fork).** The product
distribution NEVER touches the caps (legs 3, 4: zero project-cap hits
across 500 projects); the caps bind only when ONE project
concentrates many hot shared feeds — mildly at ordinary breadth
(leg 2: 0.045% typed churn at `100x10`) and firmly at hoarding scale
(leg 1). Global-cap accounting is exact to the byte (leg 6). Per the
review's own branches this is the "normal projects can hit 32 MiB"
case at concentrated-tenant geometries: the recommendation is a
DOCUMENTED per-project retained-byte quota (option a) — the behavior
past it is typed, lossless, resume-exact churn — rather than raising
the backstop, because legs 4/5 show the global GiB is already
contended by the resident-state plateau and by noisy-tenant memory
pressure that no retention knob controls. The decision (and the rc.3
mint it gates) is the reviewer's call; the leg-5 memory-isolation gap
feeds the weighted-admission design when that work is scheduled.

**RC note:** v0.2.0-rc.2 predates the profile change; the certified
system is now server-binary sha + compute-1g profile sha + harness
sha. After the follow-up legs, mint **v0.2.0-rc.3** anchored at the
commit containing the operating profile and harness, with all three
hashes in every campaign manifest. (Done: v0.2.0-rc.3 @ 2039788a,
2026-08-31, evidence verified by scripts/verify-rc-evidence.py.)

## 5. Round 13: per-project memory-pressure admission (2026-08-31)

Round 13 closed the leg-5 memory-isolation gap. Four gated commits:
13.1 evidence verifier + pre-auth inflight split (df58de47,
caf4d2c7), 13.2 the admission backstop with its 16-test red battery
(541e5b28), 13.3 unconditional pressure counting (7b235c2f), 13.4
the CODE-RED postings fix (df4deda7). Mechanism reference:
docs/LIVE-FEED.md "Per-project memory-pressure admission".

**CODE-RED (found BY this battery, first real loss ever recorded).**
The first acceptance run lost 11 acked records. Forensics on the
dead field keyspace proved them durable — the loss was in delivery:
`PostingsCache::install_chunk`'s extend path dropped match-runs
STRADDLING the extension cut, so a keyed history read served the
missing tail as provably match-free and the subscriber skipped it
permanently. Product SSE sessions always read the keyed lane, so
this affected ordinary product subscribers under absorb/trim timing.
Fixed red-first (split the straddling run, unknown gap-bytes) with a
70 s deterministic repro; the rerun of the same leg: zero loss.

**Field acceptance battery (v3, on the fixed binary; 1-GiB Compute
eu-central-1, gen eu-west-3; exact reconciliation every leg).**
Watermark for legs A1–A6: 48 MiB; decisive pair A1b/A6b: 40 MiB
(calibrated to the noisy stable profile ≈ 46 MB).

| Leg | Shape | Verdict |
|---|---|---|
| A1 | leg-5 rerun, noisy 100 feeds @ 100 w/s vs 1,000 victims | ZERO loss; victim tails complete; gate un-engaged (noisy plateaus ~46 MB < 48 MiB) |
| A2 | 30-min product distribution | zero false engages, zero project-memory refusals |
| A3 | 100×10 one project, ordinary load | gate ENGAGED a legitimate profile: 1,000 subs alone model 32 MB — watermark must clear the largest supported tenant |
| A4 | write-heavy, no subs, 600 w/s | pressure peaks 8.1 MB — pure cache-fill is under-attributed; global RSS gate remains final boundary |
| A5 | sub-heavy, write-idle | no false engage at 33.9 MB peak; zero loss |
| A6 | dual mega-projects + noisy, 48 MiB | negative control: gate CANNOT engage above the noisy plateau → 67,414 global sheds, 3,277 lag cuts, victims hurt (lossless) |
| **A1b** | A1 shape @ 40 MiB | **PASS, the reviewer's finish line** — see below |
| **A6b** | A6 shape @ 40 MiB | **PASS** — zero global sheds (was 67,414); legit 33–34 MB projects untouched; noisy owns all 143,420 refusals |

**A1b, every criterion:** the noisy project engaged once (no
flapping), latched at 41,937,034 bytes against a 41,943,040
watermark, and owned all 72,249 typed `project_memory_pressure`
refusals. First engage preceded the first global RSS shed by 728 s.
Victims: zero project-memory refusals, zero global RSS refusals,
zero lag cuts, zero reconnects, complete acked tails, append p99
762 ms vs 822 ms un-gated (improved). Versus the identical un-gated
leg: global sheds 82,161 → 165 (all landing on noisy traffic),
victim throttle rate 21.75% → 0.066%, victim append success
78% → 98.9%.

**A6b attribution:** the 15 "dead subs" are noisy-class subs that
parked after the engage froze their own project's feeds (each such
feed's full acked history was verified observed by its pair sub);
the 368 victim reconnects are the two mega-projects' own 32 MiB
retention-allowance churn (round-12 contract, lossless, present in
both A6 variants). Zero global sheds: the instance never reached
the global line.

**Calibration lessons** (now in the LIVE-FEED.md sizing guidance):
the watermark is a deployment decision sized BETWEEN the largest
legitimate tenant profile and the noisy plateau; `0` (off) is the
default until a profile pins it; the resident-state model does not
attribute pure write-rate cache-fill (A4), so the global RSS gate
stays layered behind it. Field services torn down post-battery
(both projects deleted, verified 404).
