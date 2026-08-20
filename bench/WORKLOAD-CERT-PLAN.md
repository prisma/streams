# Workload certification plan — single Compute instance (2026-08-19)

**Target workload** (Søren):

| dimension | target |
|---|---|
| W1 resident tenants | 10,000 projects on one cell |
| W2 writes | 1,000 wps aggregate; 100 tenants active per 5s window, rotating over the 10k population |
| W3 subscribers | 1,000 tenants with live subscribers, 100 subscribers each → **100,000 concurrent subscriptions** (REVISED 2026-08-20 per Søren: 1M direct SSE connections on a 1-GiB host is not a realistic target — ~1 KB/conn all-in; 1M LOGICAL subscriptions requires the edge/mux tier, Phase 3 of the SSE investigation) |
| W4 shed budget | < 0.1% of offered work refused |

## 1. Arithmetic against measured envelopes (before any field minute)

From mtten-20260819T102840Z (same instance class, product surface,
enforce): capacity ≈ **893–1,185 req/s** at 100 active tenants and
1 KiB singles; byte ceiling **≥ 16 MB/s** payload; sub-capacity p50
**73 ms**; typed shed only, zero errors.

- **W2 is in range but tight**: 1,000 wps ≈ 85–110% of the measured
  100-tenant capacity — before subscriber wakeup cost. Batch or a
  faster write path is headroom if needed; the campaign measures the
  real margin with subscribers attached.
- **W3 fan-out has a hard byte bound**: a write to a stream with
  1,000 subscribers is 1 write + 1,000 deliveries. At 1 KiB, the
  16 MB/s envelope caps deliveries at ~16k/s ⇒ **at most ~16 wps may
  land on fully-subscribed streams**. If all 1,000 wps hit subscribed
  streams, that is 1 GB/s of fan-out — no single instance serves
  that. The workload contract must therefore fix the OVERLAP: my
  proposed default is 10 of the 100 active writers per window are
  subscriber-tenants (10 wps × 1,000 = 10k deliveries/s ≈ 10 MB/s,
  inside the envelope). **Knob for Søren to confirm.**
- **W3 residency is the open question that decides feasibility**: at
  even an optimistic 2 KB resident per parked subscription, 1M subs
  ≈ 2 GB — over the 1-GiB profile before counting sockets and fds
  (1M fds is itself beyond any default ulimit), and the platform
  edge's per-service connection ceiling is unknown and probably the
  first wall. **The plan measures the per-subscriber cost curve and
  the actual ceiling rather than assuming either.** The honest prior:
  1M on ONE 1-GiB instance is unlikely without subscription-state
  slimming and/or an edge fan-out tier; the campaign will produce
  the number that says how many instances (or how much slimming)
  W3 requires.
- **W1 touches known 4,096-entry backstop trackers** (quota admission
  FIFO churn is designed-in, but 10k residents churn constantly) and
  a ~3.6 MB policy/grant feed — cheap to verify, easy to miss.
- **W4 (<0.1%) is currently blocked by an open observation**: today's
  warm instance shed ~30% at HALF capacity (typed admission shed,
  flat p50s) after ~2M appends — suspected RSS-line proximity on the
  survival profile. Root-causing that is the FIRST gate; the 0.1%
  target is unmeasurable until a sub-capacity run sheds ~zero.

## 2. Phases

**P0 — warm-shed root cause (gate for everything).** Restart the fra
rig; rerun the sub-capacity control (500 wps, 1 KiB). Expected: shed
≈ 0. Then re-warm (create 10k streams, run 10 min at capacity) and
rerun; if shed returns, chase the resident-memory line (phys_footprint
vs shed line, handle/cache eviction) until sub-capacity shed < 0.01%.
Deliverable: either "clean posture" or a fix.

**P1 — instrumentation + micro-audit (local, ~half day).**
- Per-subscription resident bytes: park 1k/10k SSE subscriptions on a
  local server, measure RSS delta per subscription + per-wakeup CPU.
- Ceiling audit: every limiter a parked subscriber crosses —
  admission inflight, subscription pool, per-project
  max_live_subscriptions (feed-delivered), fd soft limits, tokio task
  per connection, edge idle-timeout behavior for SSE.
- 10k-project residency: feeds at 10k (mtgen --projects 10000),
  tracker churn cost at 10k live projects, feed parse/refresh time.

**P2 — harness build (~1 day).**
- awsbench subscriber mode: `BENCH_SUBS_N`, `BENCH_SUBS_TENANTS` —
  open N product-surface subscriptions spread over M tenants from the
  co-located gen; count deliveries, disconnects, reconnects; emit
  per-window delivery-lag histograms. One gen instance realistically
  holds 10–50k client connections — higher rungs deploy K gen
  services (the fleet harness already supports multiple services).
- Rotating-writer mode: `BENCH_ROTATE_WINDOW_MS=5000`,
  `BENCH_ROTATE_ACTIVE=100` — deterministic window→active-set
  schedule over the 10k tenant space, offered 1,000 wps total.
- Shed accounting to 0.01% resolution: typed 429/503-origin counts vs
  offered, per window, per stage (already in the stats lines; add an
  offered-ops counter so the denominator is exact, not derived).

**P3 — laddered field campaign (one instance, fra, ~1 day).**
- L1 writes-only: 10k projects resident, 1,000 wps rotating 100/5s,
  30 min. Gate: shed < 0.1%, p50 ≤ ~150 ms, flat RSS.
- L2 subscriber ladder: 1k → 10k → 50k → (100k if the edge allows)
  parked subscribers across 1,000 tenants, sparse writes. Measures:
  RSS/sub, wakeup p99 (ring target: single-digit ms), reconnect
  churn. Produces S_max(1 GiB) and the cost curve.
- L3 combined: L1 writes + S_max subscribers + fan-out at the agreed
  overlap (default 10 wps on subscribed streams ⇒ 10k deliveries/s),
  60 min. Gates: total shed < 0.1%, delivery lag p99 < 250 ms,
  no RSS trend, zero errors.
- L4 (if platform offers a larger memory tier): repeat L2/L3 rungs on
  it to get $/subscriber scaling.

**P4 — verdict + report.** Per-dimension go/no-go for ONE instance;
the measured S_max against the 1M target expressed as "K instances at
S_max each, or these specific slimming items" (candidate list already:
subscription state size, shared tail parking per stream — 1,000 subs
on one stream should park on ONE wakeup source, not 1,000 — and edge
multiplexing). Campaign becomes the repeatable `workload-cert`
harness.

## 3. Contract fixed (Søren, 2026-08-19)

1. Fan-out overlap: **10 wps onto fully-subscribed streams** (10k
   deliveries/s at 1 KiB) — CONFIRMED.
2. Certification mode: **durable-cursor subscribe** — CONFIRMED.
3. Instance tier: **1 GiB is the only tier** — L4 dropped; the verdict
   is expressed as K × 1-GiB instances (or named slimming work).

P0/P1 results (2026-08-19; the earlier same-day bullet claiming
~1.3 KB/sub was a PROBE BUG — it measured idle keep-alive connections;
the product surface takes `:sse` as a COLON VERB, and a plain read at
tail returns `[]` immediately. Corrected below, all on true `:sse`):

- **P0 CLEARED**: sub-capacity shed is 0.000% on a FRESH instance
  (90,980 attempts, 505 rps, p50 75 ms) AND on a WARM one (after a
  10-min capacity burn: 0.000%, p50 76 ms). The one 30%-shed
  observation followed a much heavier 7-stage/2M-append accumulation
  and did not reproduce; suspected mechanism is absorber catch-up
  reservations against the shed line in the post-overload recovery
  window — a watch-item for L1/L3, not an open gate.
- **True parked-SSE cost: ~55–70 KB resident per subscription**
  (5k rung: +276 MB; 10k rung: 780 MB total RSS). Each subscription
  spawns a dedicated tokio task whose async state machine inlines two
  large read branches and captures its own StreamDesc clone, plus a
  64-slot Bytes channel. Consequence: **S_max(1 GiB) ≈ 8–9k parked
  subscriptions today**, and crossing it sheds WRITES (observed
  directly: at ~21k local subs the marker append got 429) — parked
  subscribers spend the same shed-line budget writes need.
  1M subscribers at current cost ≈ 110+ instances.
- **Named slimming items** (the deciding lever, est. 5–15× cheaper):
  (1) Box::pin the two read branches so the parked future is small;
  (2) share one Arc<StreamDesc> per stream instead of a clone per
  subscription; (3) channel 64→8 slots (also caps worst-case queued
  frames per slow subscriber 64 KiB→8 KiB); (4) longer-term: one
  reader task PER STREAM fanning to N subscriber channels — collapses
  per-sub cost to channel+response state (~2–4 KB), putting 1M in the
  4–8 instance range.
- **W1 blocker FOUND AND FIXED — tracker capacity vs the rotation.**
  The 10k-project residency itself is cheap (0.6 s boot-to-live,
  48 MB RSS, 3.8 MB feeds), but the certification rotation demands
  20 first-seen projects/s × 300 s un-evictable recency = 6,000
  tracked admission entries against the old 4,096 cap: a
  cert-pacing churn probe shed 19% with typed TrackerCapacity
  (matching arithmetic exactly). Fixed red-first —
  cert_rotation_over_ten_thousand_tenants_never_hits_tracker_capacity
  pins the shape (red: 1,904/10,000 refused) — by raising
  MAX_TRACKED_PROJECTS to 16,384 (~8 MiB worst case; the cap now
  holds the certified tenant POPULATION). MULTITENANCY tracker
  posture updated.
- **Fan-out latency is a non-issue at the workload's shape**: 1,000
  subscribers on one stream all received an appended marker with
  lag p50 23 ms / p99 33 ms from append start (append ack 32 ms);
  tail-parking is a shared per-stream Notify (no busy-poll), and
  appends into 1,000-parked-sub streams ack in 5–24 ms.

## 4. L1 verdict (writes-only gate, 2026-08-19 — five 30-min runs)

| run | posture | shed | evidence |
|---|---|---|---|
| L1 | default 1-GiB survival posture | 36.44% | RSS 456 MB vs 600 MB shed line straddled by reservations |
| L1diet | + memory diet (caches −160 MB, ring off, handles 6k) | 14.88% | RSS 381 MB but bursts persist |
| L1d2 | diet + 2 shards | 17.51% | fewer commit lanes ⇒ deeper stall pileups (RSS ≤ 406 — line never crossed; hypothesis falsified) |
| **L1d3** | **diet + 4 shards + ADMIT_MAX_INFLIGHT 2048** | **1.56%** | shed ~0 until t≈1340 s, then escalating bursts tracking absorb_lag 3 s→128 s |
| L1d4 | + ABSORB_AGE 300 s / PASS 16 MB | 5.18% | deferral synchronizes 10k streams into thundering-herd gathers (lag 169 s) |

**Mechanism (triangulated):** the write path holds 1,000 wps at
p50 ~100 ms with ZERO errors in all five runs; every refusal is
admission shed from IN-FLIGHT PILEUP during commit-path stalls, and
the dominant stall source at this shape is the ABSORBER: 10k sparse
streams accumulating ~1 KB/s each are its documented worst case, and
its gather passes contend with the append path — as unabsorbed debt
grows, passes lengthen and the bursts escalate. Memory is NOT the
binding constraint under the diet posture (RSS ≤ 414 MB throughout).

**Named code item (gates L1 and everything above it):** bound the
absorber's append-latency impact at sparse-many-stream shapes —
yield/pacing between per-stream gathers, cap streams per pass,
jittered age thresholds (never synchronized waves), and/or move
gather I/O off the commit runtime. Acceptance: L1d3 posture reruns
at ≤ 0.1% shed with absorb_lag bounded, plus a deterministic DST
regression pinning append p99 during a forced sparse-absorption wave.

Post-verdict runs (overnight 2026-08-20): L1d5 (same posture as
L1d3) = **0.85%** — run-to-run variance on the best posture is
0.85–1.56%, so the fix's acceptance is the LAG CORRELATION vanishing,
not a lucky absolute. L1d6 (SLATEDB_RT_THREADS=4) = 5.38% — more
slatedb runtime threads do NOT remove the stalls (and cannot be
distinguished from harm at N=1), falsifying thread starvation and
leaving slatedb-INTERNAL serialization between the gather read path
and WAL writes as the mechanism. The new DST gate
(sparse_absorption_wave_bounds_append_latency) EXONERATES the
streams-side committer lane and tokio runtime: baseline p99 21 ms vs
during-wave 24 ms. Every zero-code axis is now mapped: memory diet,
shard count, admit headroom, absorber deferral, slatedb threads.
The fix is OUR-side gather micro-pacing — split sweeps into small
per-pass slices with append windows between, bounding any
slatedb-internal serialization to one micro-pass — plus the committed
DST gate as the permanent regression.

**W1 VERDICT (2026-08-20): PASSED.** Root cause of the L1 shed was the
RSS write-shed counting reservation FICTION: every gather reserved the
worst case (96 MiB x up to 4 shards) against the 600 MB line while
actual gathers ran 2-17 MB (CHAOS-3's documented trade, now measured
end-to-end). Fixes: parallel per-stream gather reads (read phase max
35.8 s -> 1.2 s), adaptive reservations with grow-on-demand (OOM pool
invariant exact), split admit_shed counters. Acceptance: L1d12
(regime B) 0.0000% shed at 976/s + L1d13 (regime A) 0.0000% at
1,019/s — zero increments on BOTH shed classes in both regimes,
reserved-now peaks 17-64 MB vs the old fixed 384 MB. Thirteen-run
evidence chain in this section's history.

W2 interim verdict: **1,000 wps over 10k tenants runs at 1.56% shed
on the best documented posture — fails the 0.1% gate pending the
absorber item.** L2/L3 (subscribers) wait behind it: they inherit the
same commit path.

## 5. Superseded: original asks

1. Confirm the **fan-out overlap** default (10 wps onto
   fully-subscribed streams). Any number implies deliveries/s =
   overlap × 1,000; the byte envelope says ≤ ~16 wps at 1 KiB today.
2. Subscriber semantics to certify: durable-cursor SSE subscribe
   (default) vs deliver=applied — I'll run durable unless told
   otherwise.
3. Whether a larger Compute memory tier exists for L4 (1-GiB is the
   only profile we've deployed).

Everything else proceeds without input; P0 and P1 start immediately.


## SSE execution-model program (2026-08-20, from Søren's investigation)

Target revised: **100k concurrent subscribers** per certification (1M
logical = Phase 3 edge/mux tier, out of cell scope). Root cause of the
55–70 KB/sub: each subscriber is a dormant copy of the complete read
pipeline (inlined read future sized for the largest suspension state,
private StreamDesc clone, 64-slot channel, private 15 s timer), not a
cursor on a shared pipeline.

- **Phase 1 (slim the current path):** box the read futures out of the
  parked task; Arc'd compact SseContext instead of per-sub StreamDesc
  clones; channel 64→4 with disconnect-on-lag (durable cursor resume =
  the correct slow-consumer policy); shared heartbeat ticker; instance
  SSE budget (SSE_MAX_CONNECTIONS / SSE_MEMORY_BUDGET_BYTES → typed 503
  subscription_capacity) so subscriber RSS can never push the WRITE
  path over its shed line. Expected 8–15 KB/sub.
- **Phase 2 (shared live hubs):** per-stream LiveHub — decrypt + format
  ONCE into reference-counted prepared batches on the existing durable
  tail ring; subscribers hold only a cursor + generation; gap-free
  catch-up handoff (durable catch-up → drain hub backlog → live);
  lag = disconnect, never private buffering; hubs exist only while
  subscribed. Expected 2–4 KB/sub application state; 100k+ per
  connection node measured as CGROUP slope, not Rust heap.
- **Phase 3 (edge/mux, deferred):** connection tier owns client
  sockets; cell sees O(origin tails). Required for 1M logical.

### Phase 1 measurement (2026-08-20, local idle-slope probe, N=2000)

| shape | slope | note |
|---|---|---|
| parked `:sse`, pre-Phase-1 (P1 baseline) | 55–70 KB/sub | inlined read future + 64-slot queue + per-sub timer |
| parked `:sse`, post-Phase-1 | 61.5 KB/conn | but see decomposition below |
| **plain idle keep-alive conn (no SSE)** | **52.9 KB/conn** | hyper/axum per-connection floor — control probe |
| **SSE-specific increment (Phase 1)** | **≈8.6 KB/sub** | IN the 8–15 KB target band |
| spawned SSE task future (`sse_future_bytes`) | 3,560 B | was the read machinery's largest suspension state |

The subscriber future was never the bulk: ~53 KB/conn sits in the
HTTP stack below the handler (axum::serve default path — hyper h1
conn state + tower service future + buffers; no h1 tuning surface in
use at main.rs:1988). Consequences: (1) Phase 1 delivered its target;
(2) Phase 2 LiveHub's value is DELIVERY amplification (decrypt/format
once vs N times per record) and stays justified; (3) the 100k-per-
node idle ambition is gated on the hyper floor (100k × 53 KB ≈ 5.3 GB)
— either a manual hyper_util serve loop with tuned h1 buffers or the
investigation's honest fallback of 4–8 connection nodes. RSS after
mass disconnect returns partially (mimalloc idle retention, known).


### Phase 2 review verdict (Søren, 2026-08-20)

Phase 1: real success (numbers internally consistent). Phase 2:
right architecture, NOT ready for the 100k battery or default
enablement — SSE_LIVE_HUB stays gated; findings 1-5 fixed before
#265 runs as acceptance. Findings (all confirmed against the code):
(1) closed hubs immediately marked dead — subscribers drop the final
batch/sealed control; (2) registry keyed by name-route, not
incarnation (delete/recreate attach + ABA removal + last-subscriber
race); (3) filtered scan progress lost — matching-only batch ranges
lag the pump, debug assert panics on mixed keys, offset=now derived
from pump progress not durable tail; (4) catch-up never emits
upToDate + duplicate final control once (1) is fixed; (5) subscribers
still hold desc/key/engine/handle/billing clones — not cursor-only;
(6) read_from clones the whole ring suffix per subscriber; (7) ring
cap excludes last_flagged/overhead, keeps any-size batches, no global
cap; (8) 100k×1 regresses vs Phase 1 (hub+pump+timer per stream) —
adaptive promotion; (9) disconnect teardown lags 15-20s (no
tx.closed() in park, 5s pump poll). Plus: billing identity frozen per
subscriber (transfer regression vs Phase 1); the pump is NOT
ring-preferring (Some("") bypasses ring_read — needs ring_read_keyed);
env OnceLock flag should be injectable AppState state. Patch sequence
and 15-leg red battery tracked as tasks #270-#275; L2a (5k subs, hub
on) downgraded to prototype smoke — NOT acceptance.

**L2a prototype smoke (2026-08-20, PRE-fix binary 6843b8de, 5k subs /
1000 streams, hub on):** empirically confirms the review — subscribers
never stabilized (peak 1,267 of 5,000, decaying; ~983 hubs = one per
stream as designed; pumps exiting via the F1/F2 lifecycle bugs mass-
disconnected their subscribers; delivered_total 10, max window lag
34 s). Memory fine (304 MB, 0.0000% shed). L2 reruns AFTER the
#270-#275 battery closes.

**#273 measurement (post F5-F7, local smoke N=2000):** hub subscriber
task future = **760 bytes** (sse_hub_future_bytes — cursor + small
connection state; was carrying full desc/key/engine/handle clones);
hub SSE increment ≈5 KB/sub over the 53 KB hyper floor; RSS after
mass disconnect **54 MB** vs ~130 MB retained pre-F5 (the per-
connection descriptor clones were the unreclaimable residue).
Delivery 10/10 through one-batch reads + uncached-posture ring.

**L2b (2026-08-20, FIXED hub b95170b5, 5k subs):** server side healthy
(0.0000% shed, RSS 310 MB, hubs 683 peak -> clean teardown to 0, hub
prepared bytes max 130 KB) — but the GEN is the blocker, on BOTH
binaries: ~96% of writes hard-ERROR from the first 20 s window (L2a
18,768 / L2b 19,549 errors at t=20), subsOpen collapses (peak 1,522 of
5,000 -> single digits) with reconnects=0 (subscriber tasks die
permanently). The L1 stages run the SAME rotation writers clean, so
the failure is specific to SUBS_N>0 on the single 1-GiB gen instance —
prime suspect fd exhaustion / client-side resource collapse under
5,000 held-open SSE connections plus writer churn. Next: pull the
gen's remote stderr (Compute logs) for the error class; likely fixes
are gen-side setrlimit / connection budget / multi-gen topology (the
plan's L2 shape needs several gen instances anyway — one 1-GiB client
cannot hold 100k sockets either).

**L2 root cause (2026-08-20, CLOSED — harness, not server):** the
platform edge (cv-*.prisma.build) rate-limits concurrent TLS
handshake establishment per client. Reproduced from a workstation:
sequential connects 15/15 OK; concurrent bursts 66-76% connect
timeouts (33/50, 76/100, 127/200); established conns unaffected
(114/114 held through the probe). The unpaced 5k-task swarm
(synchronized 500 ms retries) was a permanent handshake storm that
also starved the writer pool's new connections -> ~96% apErr (client
connect timeouts, NOT server rejections), subsOpen 0->6, delivered 21.
Server exonerated: with the run's real stream key, appends to the
run's own streams return 200 across s- and w-ranges; feeds fresh
(10k credentials, age 19 s); the hub served ~968k frames to the few
parked subscribers. (Investigation detour: probing with a WRONG
encryption key returns 403 whose product mapping collapses wrong_key
into stale_or_wrong_credentials - cost an hour chasing auth; noted.)
Gen fixes (awsbench cert): BENCH_CERT_CONNECT_CONC connect-permit
(default 48) held across tail-learn + SSE establishment, jittered
1.5-3 s backoff, typed error classes in stats (errConnect/errTimeout/
errStatus/errOther + subErrConnect/subErrStatus, first-3 samples to
stderr), RLIMIT_NOFILE raised to hard max on Linux. Edge ALPN offers
h2 (future option: multiplexed subs if per-conn stream caps allow).
Rerun pending as L2b-r2 on tag wcfix2.

**L2b-r2 (2026-08-20, wcfix2 paced gen, hub on, 20 min): GEN FIX
VALIDATED + first real server capacity finding.** apErr=0 (was 1.6M),
apOk 1,173,963 / offered 1,223,800, subsOpen 2,193 (edge meters parks
at ~2/s sustained; subErrConnect 195 total, plateaued after the first
ramp burst; reconnects 0 - parked conns never die), delivered 56,830,
teardown clean (hubs 0 post-stop). NEW FINDING: append shed 4.06% cum,
fully server-attributed (admit_shed_inflight 40,605 + admit_shed_rss
9,151). Dose-response: ~0 shed below ~500 parked subs; 1,700-3,700
thr/window at 1,650+ subs; append p99 degrades 390 ms -> 1.0-1.5 s.
At ~2.2 subs/stream, ~1,000 subs are DIRECT-path pollers (adaptive
promotion keeps sub #1 direct): ~440 poll-reads/s of full pipeline
cost; delivery lag p50 2.3 s / p99 4.4 s = the direct 2000-2500 ms
poll cadence, not the hub notify path. RSS component = the 53 KB/conn
hyper floor (#269). Next: L2c1/L2c2 discriminator - same 1,000 parked,
1 sub/stream (pure direct) vs 2 subs/stream via WC_SUB_TENANTS=500
(pure hub) - no server change; decides the promotion-policy question
(promote-on-first vs task-per-stream cost) on data.

**L2c1 (pure direct, 1,000 subs / 1 per stream, 20 min): shed 7.22%**
(87,941 thr / 1,218,800 offered; apErr 0; 955 parked) — WORSE than
L2b-r2's 4.06% at 2,193 subs. Attribution flips: admit_shed_rss
47,717 > admit_shed_inflight 40,231 (L2b-r2 was 9k/40k). Shed
ACCELERATES at flat sub-count: thr/window 1,382 (t=600) -> 3,294
(t=1000) -> 8,014 (t=1200) with subs pinned at ~950 — progressive
memory pressure under the ~430 poll-reads/s direct storm, marching
RSS to the 600 MB shed line. Direct-path polling is the confirmed
cost driver; the hub exists to remove exactly this. L2c2 next: same
1,000 parked over 500 streams (~500 direct + ~500 hub after F8
promotion; a pure-hub rung is unreachable without first-sub
reconnect — promotion leaves sub #1 on its direct conn).

## EDGE DOSSIER (2026-08-20): the platform edge is the L2+ blocker

Consolidated black-box evidence, all reproducible against a wc-ladder
deployment (cv-*.fra.prisma.build), server-side counters healthy and
the identical binary flawless locally:

1. Handshake rate limiting (per client): sequential TLS connects
   15/15 OK; concurrent bursts 66-76% connect-timeout (33/50, 76/100,
   127/200). Sustained grant ~2-4 establishments/s.
2. Zombie 200s: under bursty dials the edge completes the CLIENT leg
   (200 + SSE headers + catch-up bytes) while the ORIGIN leg dies;
   the client parks on a silent socket forever. Gen-side "parked"
   955 vs 60 real server conns (L2c1/L2c2).
3. Streaming-conn reaping under load: with writers at 1,000 rps,
   live SSE conns cap at ~12-22 (subsLive gauge; server sse_connections
   agrees at ~60 incl. probes) and established conns die ~35-60 s
   after their catch-up burst. The SAME service held 90/90 probes
   (137 total conns) flowing when idle. Not per-client: workstation
   probes on a different IP see the same behavior only under load.
4. Selective starvation: while loaded, plain GET /records answers in
   0.9 s and livez in 0.7 s, but a NEW SSE request on an empty stream
   receives ZERO bytes (not even headers) for 40 s. Locally the same
   request answers headers + control at +0.0 s and keep-alives at
   15 s cadence.

Implications: (a) the L2/L3 subscriber ladder cannot be certified
through the public edge - the workload target (100k concurrent
subscribers/cell) is unreachable THROUGH THIS EDGE regardless of
server capacity; (b) this is product-impacting for any tenant holding
more than a few dozen SSE subscriptions on a busy cell (durable-cursor
resume makes it survivable but turns parked fleets into reconnect
storms). Needs platform escalation and/or an in-VPC gen->server path
(none exists in Compute today per harness inventory).

Server-side facts banked along the way (all still valid): gen pacing
fixed (apErr 0), teardown clean at every rung, 0% shed to ~500 real
subs; the L2c1-vs-L2c2 shed split (7.2% vs 5.0%, RSS component -84%)
was measured under ~equal LIVE conn counts (~60) with different
CHURN mixes, so it primarily evidences churn/catch-up cost, not
parked-subscriber cost - re-run behind a fixed edge before drawing
promotion-policy conclusions.

**#275 BATTERY CLOSE-OUT (2026-08-20): 15/15 legs green.** Final four:
leg 14 workspace transfer (delivery survives, stale-ws token 401,
re-mint works, per-batch billing lands in the workspace at event
time, verified to rollup rows); leg 12 global ring exhaustion over
HTTP (cap AppState-injectable, over-cap batches uncached but
delivered, per-batch posture, both hubs alive; ring-walked gauge
sse_hub_ring_bytes_walked added as accounting cross-check); leg 6
cursor=now honesty (no history replay, control ack first, post-
subscribe appends only; `offset` is a rejected legacy field on this
surface - first draft's 400 passed a bare not-contains assert,
now hardened to demand 200+control); leg 15 residency probe
(bench/sse-probes/sse-1per.sh): 1000x1 direct 78.2 KB/sub vs 500x2
hub 75.9 KB/sub at equal conns - parked cost is floor-dominated
(#269's 53 KB), 500 live pumps measure ~free, teardown clean both
arms, no hub-specific idle ratchet. Hub value = poll-CPU elimination
+ the field RSS-ratchet removal, not parked bytes. SSE_LIVE_HUB
remains default-OFF per review instruction; the flag-default decision
and the edge escalation are the two open calls. Suite 485.

**#269 CLOSED (2026-08-20): manual h1 serve loop, bounded buffers.**
axum::serve replaced by http::serve_h1 (hyper http1::Builder,
max_buf_size = SSE_H1_MAX_BUF, default 64 KiB; caps per-READ chunk
size, not body size). Measured (sse-1per, 1000 conns): direct
78.2 -> 44.3 KB/sub (-43%), hub 75.9 -> 39.5 KB/sub (-48%);
16 KiB cap buys only ~1.3 KB more, so 64 KiB stands - the floor is
now task/future/slab, not hyper buffers. Every test rig serves
through the SAME function (the suite exercises the real connection
path; previously only out-of-tree probes did). Projected: 10k parked
subs ~ 430 MB -> the 1-GiB class holds ~12-13k direct or ~15k+
hub-covered subscribers before the shed line, pre-#269 it was ~7k.

**EDGE ROOT CAUSE SUPERSEDES THE DOSSIER FRAMING (2026-08-20 late):**
the four dossier findings are ONE cause — the edge buffers streaming
responses in ~8-16 KB increments (bench/edge-repro: /sse-once passes,
/sse never; pad bisection 4/16/32/64/128 KB; origin accepted 115/115
while clients saw 0). "Handshake rate limiting" was buffered headers;
"zombies" were buffered-forever streams; "streaming reaping" was the
edge reaping buffered-idle origin legs; "~60 survivors" were subs on
actively-written streams that kept refilling the buffer. The edge
honors X-Accel-Buffering: no — server now sends it on every SSE
response (2fe76450, wire-asserted). L2/L3 UNBLOCKED pending rung
validation on the workaround binary (wcfix4).

**REVIEW ROUND 2 CLOSED (2026-08-21): all six findings answered.**
V1 durable-frontier upToDate (immutable batches, last_flagged gone,
subscriber-side status, will_end = scan_next>=end — 3 wire reds via
the per-registry pump gate); V2 CAS cap reservation (32-thread
barrier red showed 81,920>65,536); V3 conservative charge + logical
gauge (red: 1.2 MiB retained under a 1 MiB bound); V4 subscription
auth lease — live SSE terminates on transfer/suspension/revocation/
expiry (t1 red: both paths survived a full transfer indefinitely;
now 4 legs pin all causes; leg 14 renamed to workspace-at-event
billing); V5 gate fail-closed (caught a parallel flake same day);
V6 SSE_HUB_PROMOTE_AT knob (default 2 per review) + promote-at-1
canary leg. SSE_LIVE_HUB now DEFAULT ON per pre-approval (kill
switch = 0). Suite 494. Open from the round: the matched-shape
promotion experiment (needs the knob rungs; probe currently uses a
workstation path — rework with it), and the canary rollout with the
named gauge checklist once a build ships.

**HAIRPIN FINDING (2026-08-21 01:45): X-Accel-Buffering is ignored
in-region.** L2f/L2g plateau (~105-120 subsLive) fully explained: the
gen is IN-REGION and the hairpin path to cv-* buffers streams even
with the opt-out header (server sends it on every SSE since 2fe76450).
Proof matrix (same server, same minute): out-of-region h1 AND h2 curls
receive keep-alives at 15 s cadence; an in-region Bun probe conn gets
the initial burst then 56 s+ of silence (edge-repro /probe-report,
live15s=0). Falsified en route: gen h2 multiplexing (h1-only gen,
wcfix6 — same plateau), cursor-shape difference (explicit-tail probe
flows from outside). Program implication: L2/L3 gens must run
OUT-of-region (or off-Compute) until the platform honors the opt-out
on the internal tier; in-region product consumers are broken TODAY —
added as ask #4 in bench/edge-repro/README.md.

**MATCHED-SHAPE PROMOTION EXPERIMENT (2026-08-21, local,
bench/sse-probes/sse-matched.sh, 4000 conns/arm):**
| arm | shape | thr | park KB/sub | idle CPU | idle slope |
|-----|-------|-----|-------------|----------|------------|
| A | 4000x1 | 2 (all direct) | 27.1 | 0.8% | 1088 KB/min |
| B | 4000x1 | 1 (4000 pumps) | 28.5 | 0.7% | 459 KB/min |
| C | 2000x2 | 2 (2000 hubs)  | 26.3 | 0.7% | 1003 KB/min |
Penalty side of the review's decision rule is now measured: 4000
hub pumps cost +1.4 KB/sub and NO idle CPU vs all-direct, and the
all-hub arm's idle RSS growth is 2.4x LOWER (direct pollers churn
the allocator every 2-2.5 s; parked pumps are notify-woken).
Delivery-latency column EXCLUDED (probe artifact: append-loop
duration dominates the measurement). Still open for the win side:
a loaded matched comparison (the field L2c1-vs-L2c2 shed split,
7.2% vs 5.0% + RSS-ratchet elimination, points the same way but was
churn-contaminated). Recommendation: promote-on-first is SAFE by
these numbers; keep default 2 per the review until a loaded matched
run (or a hairpin-fixed field canary at SSE_HUB_PROMOTE_AT=1)
confirms the benefit.
