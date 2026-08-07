# Prisma Streams on SlateDB — Pilot & Hardening Report

> **HISTORICAL (2026-07-14).** Kept as the record of the SlateDB pilot
> and its hardening rounds. The surfaces, figures, and telemetry planes
> described below predate the product-surface cutover and the round-20+
> billing system (docs/OBSERVABILITY-BILLING.md). Do not operate from
> this file.

2026-07-14. Covers: the production pilot on Prisma Compute (runs 1–4 +
follow-ups), the DynamoDB-operator design review (3 rounds → READY), every
change implemented, and the scale assessment.

---

## 1. How the system behaved: before → after

| dimension | before (run 1–3 state) | after (run 4 + follow-ups) |
|---|---|---|
| durable-append ack, server-side p50 | 21 ms fresh, **rotting to 0.3–1 s** within minutes on loaded instances (uniform, per-DB) | **22–31 ms, stable for the entire 30-min run**; one transient shard excursion (773 ms) that self-healed — incidence down ~16× |
| client p50 through router (light load) | ~300 ms (run-1 Bun harness artifact), 21–37 ms (Rust harness) | 37–51 ms (the deliberate 25 ms-flush trade) |
| error rate over a full ramp | 0.62 % (run 3); congestion collapse possible (run 1: open-loop client died at 512 rps offered) | **0.13 %**, all inside scale-transition windows; collapse impossible (closed-loop + admission model spec'd) |
| autoscaling | rps-only → **deadlock**: one congested server suppressed the scale-out signal for 2 full ramp levels | rps + sustained ack-p50 dimensions → desired reached 4 by conc 32; congestion cannot hide demand |
| scaling model | all 4 servers always awake, rendezvous over everything (wrong model) | fleet publishes its own `desired.json` from heartbeats; router routes to exactly that many; the rest stop receiving traffic and release ownership (verified: routing and ownership scale-in, single-server wake, full staircase 1→4 twice, descent, crash detection ≤10 s). **Correction (round 19):** this is ROUTING/OWNERSHIP scale-in, not economic scale-to-zero — a non-desired instance keeps heartbeating every 2 s, which keeps its VM from suspending, so the preview's cost floor is a WARM fleet of FLEET_MAX instances. Standby behavior (final standby heartbeat, stop control-plane traffic, allow suspension, out-of-band wake) is a pre-GA item |
| wedged instance | routed to indefinitely (ordinal set) | dropped from the live-set ring within 30 s, rejoins on heartbeat |
| billing/metrics | per-instance local appends — fence-fights in a shared namespace | routed through the router like tenant writes; single writer per shard; verified multi-instance records in one stream; zero-byte reads now metered |
| scale-from-0 | first append after wake 0.9–1.7 s (dead pooled TLS) | pool-idle timeout 4 s < snapshot threshold → wake cost ≈ restore (~0 ms) + one fresh dial |
| design review status | pilot-grade; 6 operator-blocking design holes | **READY** at the DynamoDB-operator bar (3-round adversarial review; final round: zero blocking) |

## 2. Everything implemented (ledger)

**Server (Rust, `src/`):**
bearer-token auth; per-tenant metrics module + flusher (evolved local →
**routed** appends via `METRICS_LB_URL`); bucket sharing via `PATH_PREFIX`;
object-store client `pool_idle_timeout=4 s` (snapshot-safe pools);
`flush_interval` 5 → **25 ms** (D22 amended, WAL-churn root-cause);
**lazy shard opening** with fence-close eviction + 3 s anti-flap holdoff;
**R2 ring-ownership check** + **R3 `Streams-Replay-To`** on the server;
`fleet.rs`: 2 s heartbeats (rps + ack-p50 load vector), desired.json CAS
with rps **and sustained-latency** dimensions (20 s breach damping),
**live-set-filtered ring** (30 s dark-instance eviction) published to the
request path; commit-pipeline instrumentation (`/v1/debug/timings` —
write vs durable-wait split, the tool that cracked the latency mystery);
success-only load counting; zero-byte-read metering; env-configurable
shards/L0/flush/scaling knobs.

**Pilot harness (Rust `pilot` binary):** LB mode — shard-aligned routing
(name-hash → topology prefix → rendezvous over the live active set), R3
replay, desired/heartbeat polling from object storage, live dashboard off
the heartbeat bus; gen mode — closed-loop concurrency-doubling generator
with HdrHistogram windowed percentiles; rotating HTTP clients (stale
replica pinning); `GEN_UPSTREAMS` env isolation (platform env-merge
leaks); HTTP/1.1-only to the edge (h2 single-connection pitfall).

**Specs:** README — D22 amendment, L8 bounded staleness (≤ 5 s, mechanism
stated), C9 stated-and-enforced limits table, cell-scoped path layout,
O12–O15 (provider contract, customer-scoped naming, SlateDB defect, tail
API shape — all GA gates), honest spec'd-vs-implemented status.
COMPUTE-SPEC — **§10 cells** (placement, global `cells.json`, heartbeat
aggregation, control-plane arithmetic at 10²/10³/10⁴ instances, blast
radius), **§10.1a enforced isolation** (per-cell principals, prefix-scoped
IAM, role split, break-glass, bucket regionality), **§11 deployment**
(waves, canary gates, auto-rollback, N/N-1 + flip-lag rule, version-skew
contract), **§12 tenant isolation** (token buckets with distributed
semantics, committer fair-share, 429 contract, hot-stream/shard
automation, poison-shard quarantine). OPERATIONS.md (new) — storage
dependency analysis + SLA math + registry residence; ciphertext
backup/PITR (RPO ≤ 5 min / stated RTOs), restore + provider-failover
drills, scrubber, deletion protection, GDPR; tenant principals +
key-service contract + key-compromise runbook + audit; **edge read
contract** (capability URLs, ciphertext-only CDN, log-reconciled
metering) + tail-mux plan B; SLOs/alarms/runbooks; capacity & unit
economics. EXPERIMENT-PILOT.md — full run history and findings.

**Root causes found en route:** SlateDB per-DB durable-watermark
degradation (correlates with fence/reopen cycles, NOT WAL backlog —
refuted by measurement; 25 ms flush reduces incidence ~16×, now
self-healing; upstream reproducer is GA gate O14). Platform behaviors
documented for the Compute team: stopped-version replicas pinned by
keep-alive (and kept alive by KeepAwakeGuard), additive env-merge across
deploys, h2-to-edge single-connection routing, restore-vs-TLS-pool
interaction, no-`chmod` images, `versions promote` vs deploy binding.

## 3. Judge loop (— "ready for AWS to operate at DynamoDB-like scale?")

- **Round 1: NOT_READY** — 6 blockers: control-plane O(N²) with no scale
  math; no cells/deploy design; no per-tenant admission; unquantified
  storage dependency + no backup/PITR; authn a single token + uncontracted
  key service; unverified CDN with no fallback.
- **Round 2: NOT_READY** — all six resolved; two new: CDN path
  contradicted authn/metering; cell isolation unenforced.
- **Round 3: READY** — both resolved (capability-URL edge contract;
  IAM-enforced cells); zero blocking; 8 spec-precision advisories, all
  folded the same day. Judge's basis: every trust-bearing mechanism is
  either verified live (fencing/CAS, clone/union, scale-from-0 economics,
  autoscaling, 40× coalescing analog) or is a designed contract with an
  explicit GA gate and deployable fallback.

## 4. Realistic scale assessment (given enough hardware)

Grounded in measurement where it exists; arithmetic beyond that.

**Fundamental ceilings (by design):**
- **One totally-ordered stream** (measured, see EXPERIMENT-PILOT bench +
  charts/): **~400–840 requests/s** request-bound and — today —
  **~0.4–1.7 MB/s sustained** byte-bound (12 MB/s burst), because SlateDB
  L0 compaction underperforms (O14); batching converts the byte budget
  into events (measured 1.7k ev/s; modeled 6k–40k ev/s once compaction is
  fixed, i.e., the originally-stated 6k/8 MB/s ceiling is the
  post-O14-fix number — same order as a DynamoDB partition). Per-key
  streams multiply by segment count.
- **Durable-ack latency floor: ~40–60 ms** (25 ms flush window + one
  object-store PUT). This is the architecture's honest product point —
  commit-to-object-storage-before-ack buys zero stateful infrastructure
  and per-stream crypto isolation at the cost of never being single-digit
  milliseconds. (~20–30 ms is recoverable with the 5 ms flush once the
  upstream watermark defect (O14) lands.)

**Measured / spec'd unit capacities:** 6k appends/s per 1-CPU instance
(batched; 18k/s measured on a laptop core; pilot's unbatched
request-per-record HTTPS path saturates a 1-core instance nearer 150–300
req/s — batching is where throughput lives), 8k tail connections per
instance, 32 shards per instance, 64 instances per cell → **~384k
records/s ingest and ~500k direct tails per cell**; reads above that ride
the CDN/mux tier (125 origin long-polls per 1M subscribers, measured-40×
coalescing in the touch analog).

**Fleet arithmetic (cells are independent by construction — verified
per-prefix control-plane load is flat ~112 req/s regardless of fleet
size):** 600 cells ≈ 38k instances ≈ **~230M records/s ingest ceiling ≈
2×10¹³ records/day**, tens of millions of concurrent tails, billions of
streams (registry cost is O(1) object per stream) — i.e., DynamoDB-order
traffic *in records*, with per-stream ceilings competitive per-partition.

**What that claim is conditioned on (the honest part):**
1. **Object-store capacity commitments (O12)** — the provider must absorb
   ~60k PUT/s per full cell (36M/s at 600 cells). Structurally sound on
   S3-class auto-partitioning (shard prefixes are independent), but it is
   a contract, not physics, until signed.
2. **Live validation stops at 4 instances / 1 cell.** Everything beyond
   is arithmetic on verified per-unit numbers. The cell design exists
   precisely so scale-out multiplies verified units, but a 64-instance
   cell soak and a multi-cell drill are the next empirical steps.
3. **Multi-region active-active writes are designed, not built** —
   single-region append SLA today is 99.9 %; DynamoDB-class 99.99+ needs
   the multi-region path (the fencing model permits it per-shard).
4. **GA gates open:** V5 CDN coalescing (plan-B mux deployable now), O12
   provider contract, O13 naming scope, O14 SlateDB watermark defect,
   O15 tail API shape, independent crypto review (V9), V8 GC soak.

**Bottom line:** with enough hardware, provider capacity commitments, and
the six named gates executed, this design credibly operates at
DynamoDB-like aggregate scale for streaming workloads — hundreds of
millions of durable records/s across a fleet, latency floor ~40–60 ms,
per-stream ordering ceilings that mirror DynamoDB's per-partition ones,
and a cost structure (object storage + stateless scale-from-zero compute)
that undercuts node-based streaming systems at rest.

---

# Addendum (2026-07-14, evening): single-stream throughput round — can we match the Bun architecture's 5k req/s / 50k ev/s / 50 MB/s?

## Verdict up front

**Yes on all three, with one architecture-honest qualifier on latency.**
The old numbers are not just reachable — the engine now beats two of the
three by a wide margin on the same laptop-class hardware the earlier
"~0.4–1.7 MB/s ceiling" was measured on. That ceiling was two
configuration defaults interacting, not a compaction defect, and the
"compaction overhead" hypothesis is formally retired (compaction was
idle during the stalls; a 6-L0 merge took 3.1 s).

| target (Bun architecture, sustained) | measured now (local engine) | status |
|---|---|---|
| 5,000 req/s on one stream | **12,368 req/s** (1 KB bodies, p50 61 ms, 0 errs); avg 12,494 flat over 7.3 min | **2.5× target** |
| 50,000 events/s on one stream | **55,146 ev/s** at 1 KB (0 errs); **134,816 ev/s** at 256 B batch=16; 177k ev/s at batch=4096 | **1.1–3.5× target** |
| 50 MB/s on one stream | **56.5 MB/s** closed-loop 45 s (0 errs); 37.5 MB/s avg over 7.8 min pinned with absorber+trim live (test-host-bound, not engine-bound) | **at target (45 s); multi-minute runs land 75 % of it on this test host** |

The qualifier: the Bun architecture acked from local disk; this design
acks only after object storage. The durable-ack floor is flush/2 + PUT ≈
**40–60 ms** (25 ms flush, fast backend) and ~**70–100 ms** on Tigris with
the 50 ms flush the real PUT latency demands. Sub-10 ms acks are not on
the menu at any hardware budget — that is the deliberate product trade.

## What was actually wrong (and fixed)

1. **`l0_max_ssts_per_key` default (8)** gated the memtable flusher: an
   ordered stream rewrites its tail row in every memtable → every L0
   overlaps on that key → per-key overlap == L0 count. We had raised
   `l0_max_ssts` and never the per-key cap. Fix: `L0_MAX_SSTS_PER_KEY`
   env, default = `L0_MAX_SSTS`.
2. **`manifest_poll_interval` at 60 s** (set for idle-cost reasons) froze
   the flusher's view of L0 space for up to a minute after compaction had
   already freed it → 14 s dispatch stalls → 137 MB imm-memtable pileups
   → backpressure timeouts → 408 storms. Fix: `MANIFEST_POLL_MS`,
   default 2 s (1 s under load). Upstream ask: in-process compaction
   notification instead of polling.
3. **Committer minted one commit group per arrival** under trickle load
   (one group ≈ one WAL SST ≈ one serial PUT ≈ 40/s ceiling × group
   size). Fix: pacing — gather up to 15 ms when ≥32 requests queue.
   Batch=1 throughput went 841 → 12,368 req/s.
4. **Encrypt/decrypt hooks — the user's direct question**: a real but
   minor factor. The per-request AES key schedule was rebuilt per record;
   `FrameCipher` hoists it. With hardware AES (armv8 flag / x86 AES-NI
   auto-detect) the envelope runs 270–617 MB/s/core: at 50 MB/s that is
   ≤ ~18 % of one core, ~8 % at large records. Crypto was never the
   ceiling; it is now cheaper still.
5. **Absorber couldn't track ingest** (serial 8 MB reads ≈ 10k rec/s):
   pipelined ranged reads (4-way), per-pass byte cap, `TRIM_PER_OP` so
   hot-log deletion tracks ingest. Steady state now holds the hot tier
   bounded at high rates — the precondition for "sustained for hours."
6. **WAL flush interval must respect backend PUT RTT** (cloud finding):
   25 ms flush against Tigris (~45 ms PUT p50) mints WAL SSTs faster than
   the serial PUT pipe ships them — durable-wait p90 was 518 ms. At 50 ms
   flush: p50 68 ms / p90 82 ms. D22 now reads
   `flush_interval = max(25 ms, PUT p90)`.

## Sustained evidence

- Local request shape: **avg 12,494 req/s flat for 7.3 min** (window
  min 12,113 / max 12,975), zero errors, no drift.
- Local byte shape (batch=64 × 1 KB, offered load pinned): **7.8 min
  continuous at 37.5 MB/s / 36.5k ev/s average with the absorber
  absorbing and trimming the same stream live** (quartile trend flat:
  39.6 → 37.8 MB/s — no debt spiral), peaking 47–53 MB/s; the 50 MB/s
  point itself is demonstrated at 45 s scale (56.5 MB/s, 0 errors).
  Closed-loop *max* (unpinned) oscillates 25–56 MB/s as the pipeline
  rides its backpressure ceiling; that is the correct shape for a system
  whose admission layer (§12) pins tenants at their contracted rate.
- "Hours": the longest clean local windows are tens of minutes — bounded
  by the in-RAM S3 emulator and this laptop's swap pressure (the emulator
  was OOM-killed twice at closed-loop max; the engine itself never wedged
  post-fix). Steady state — absorption, trim, WAL GC, L0 compaction all
  concurrently live at ~50 MB/s — is demonstrated; nothing in the engine
  accumulates debt once absorb/trim track ingest.

## Cloud reality check (1 CPU / 1 GB Compute + Tigris)

The same engine on the pilot's smallest instances is edge- and
RTT-bound, not engine-bound: the platform edge delivers ~390–430 req/s
per instance to a closed-loop client (server-side queue is empty; commit
groups are size 1; client p50 ~1 s is edge queueing), and the serial WAL
PUT pipe bounds bytes at roughly 7–15 MB/s per shard (after the 50 ms
flush fix; at 25 ms flush the durable watermark fell 500+ ms behind). A
25-minute pinned run completed end-to-end but with 24 % 408 churn at
~2.9 MB/s — one core running TLS+AES+LSM+compaction against 45 ms PUTs,
on a shard carrying three earlier benches' debt. Those are per-instance
envelopes of the *pilot hardware*, not the engine — the fleet design
(shards × instances, verified in the autoscaling runs) multiplies them,
and expressing the single-node 5k/50k/50 MB figures in cloud needs
≥4-core instances plus producer→owner routing that bypasses the edge
concurrency cap (both platform matters, documented in
EXPERIMENT-PILOT.md).

## Realism statement

On server-class hardware (≥ 4 cores, NVMe-free, real object storage,
no edge cap between producer and shard owner) the three Bun-era numbers
are **realistic and now demonstrated at the engine level**: 5k req/s
(2.5× headroom), 50k ev/s (1.1× at 1 KB events, 2.7× at 256 B), 50 MB/s
(at target, with the steady-state machinery live). The prices that remain
structural: ~40–100 ms ack latency (object-storage commit), and batching
is how event rates above ~12k req/s are reached (the protocol's batch
path, unchanged).

---

# Addendum 2 (2026-07-14, late evening): flush-interval A/B + fleet staircase re-run

**Flush interval**: tested 25 ms vs 10 ms vs 10 s with the new tunables.
10 ms is *not better* — identical p50 (the serial WAL PUT pipe, not the
flush timer, sets the durable cadence below the backend RTT) and ~16 %
worse on the byte path from WAL churn. 10 s pins p50 at interval/2 ≈ 5 s
— unusable. D22 is closed from both directions:
`flush_interval = max(25 ms, backend PUT p90)`; 25 ms local, 50 ms Tigris.

**Fleet staircase (1→4 servers, same harness as run 4):**

| | run 4 (before) | run 5 (after, tuned engine) |
|---|---|---|
| errors | 513 / 401,790 (0.13 %) | **1 / 484,892 (0.0002 %)** |
| low-load levels (c=8/16) | 45–154/s, p50 35–39 ms | **153–258/s**, p50 52–59 ms |
| top-level p50 | 1,915 ms (c=512) | 892 ms (c=256) |
| staircase | desired flapped 1–4 early; live stuck at 3 most of the run | desired 1→2→3→4 strictly with load; **live == desired at every step**; clean scale-in after |
| plateau | ~290–320/s | ~340/s — both runs saturate the single 1-CPU generator box, not the fleet |

One real bug found by the re-run: the post-run-4 live-set ring deadlocked
scale-out wake (a newly-desired instance stays dark because only routed
traffic wakes it, and the filtered ring never routes to dark instances).
The pilot LB now emulates the platform scaler with an out-of-band wake
ping. Production note added to COMPUTE-SPEC: dark-instance ring eviction
assumes the *platform*, not routing, performs instance start.

Charts re-created in `charts/`: `chart-staircase-before-after.png`,
`chart-size-sweep.png` and `chart-batch-sweep.png` (post-fix series with
the pre-fix series as muted reference; single-stream sweep now clean at
every event size 64 B–1 MB and monotonic to 173k ev/s at batch=1024).

---

# Addendum 3 (2026-07-15): utilization-based scaling, multi-generator stress, chaos, and a 2-hour soak

**The scaling question ("why did it scale at 5 % load?") is answered and
fixed.** Run 5 scaled early because `SCALE_RPS_CAPACITY=150` described an
engine build 10× slower than the one deployed. Assumed-capacity constants
rot; the fleet now scales on **measured signals**: CPU utilization
(scale-out as the fleet nears **75 %**, per the stated target), a
hot-instance rule (any loaded instance ≥ 75 % sustained), the platform's
per-instance delivery envelope, server ack latency, and — added after
run 7 caught the fleet scaling IN during client-side congestion — the
router's observed client latency, which also blocks scale-in while hot.
Verified live: at 394 req/s on one instance (21 % CPU) the fleet now
correctly holds desired=1 where the old signal demanded 4; under
overload it holds desired=4 without flapping.

**The load-generator bottleneck is addressed**: 4 generators × 4 LBs
(one per generator) × 4 servers, batch=16 appends. The harness now
delivers enough load that the *platform edge* (~400 req/s/instance
delivery envelope), not the generator, is the first ceiling; server CPU
peaks at ~25 %. Stressing the engine itself past that needs
direct-to-owner routing or bigger edge allotments — a platform matter.

**Chaos**: at maximum offered load, with one instance already dead by
platform fault, we destroyed a second — half the fleet. The survivors
absorbed all 16 shards in ~90 s and served 620–720 req/s at 51–61 ms
acks, no operator action. Fencing/ring behavior under compound failure
is validated.

**Soak**: ~2 h continuous at conc 64×4. Post-memory-fix window: 490
req/s ≈ 7k records/s, client p50 flat at ~406 ms across quartiles
(zero drift — no cumulative degradation), acks 57–67 ms, ~0.4 % errors.

**The biggest engineering find**: SlateDB's default block cache is
**512 MB per database**; the pilot ran 8–16 DBs per 1 GB instance
without the §1.1 shared cache, so instances died of cache-fill every
20–40 min under load — presenting as platform "zombies" (version
running, domain unbound, needs redeploy). One shared 192 MB cache fixed
the epidemic (3 of 4 instances ran the soak clean; one late death
remains under investigation — RSS heartbeat + budget enforcement are
the follow-ups). The recurring zombie mode itself is filed as the top
platform ask: crashed replicas must restart, and version status must
reflect replica health.

---

# Addendum 4 (2026-07-15): container fleet — the engine without the edge

Running the fleet as local Docker containers (1 CPU / 1 GB cgroups, no
platform edge) answered what the cloud never could:

- **One container: 10.6–11.3k req/s at p50 63–66 ms** (batch=1 appends,
  zero errors) — ~26× the through-edge cloud ceiling, confirming the
  edge-slots finding from the other side.
- **Fleet of four: ~20k req/s at p50 47–48 ms, zero errors**, with
  desired stepping 1→3→4 on measured 62–77 % CPU — the utilization-based
  scaling loop validated end-to-end at last.
- **Four production fixes came out of the stress**, each proven by
  re-running the exact failure: mimalloc (RSS ~4× lower; musl malloc
  fragmentation was most of the OOM story), RSS-reactive write shedding
  (the flood that OOM-killed a container now degrades gracefully and
  self-heals), Retry-After-honoring clients + a 25 ms shed tarpit
  (admission control without both melted the fleet into a reject storm
  that starved heartbeats), and an LB fix for plain-http object stores.
- The remaining local ceiling is the test rig itself (single-process
  object-store emulator with a global lock; single LB process), not the
  engine.

---

# Addendum 5 (2026-07-15): platform edge fix verified — 2.5–4×

Post-fix verification (calibrated probe + full 4×4×4 staircase, identical
harness to run 9): single-source admitted concurrency 49.5 → ~124 slots
with zero queueing below the new ceiling; fleet throughput at the top
levels 398–480 → 1,217–1,240 req/s average (2.6–3.1×) with client p50
improving 4.3–5.2× (1,963 → 455 ms at peak level); best window 2,760 req/s
(4.3×); max per-instance delivery 300 → 1,186 req/s (4×). The binding
constraint is no longer the platform edge — it is our own durable path
under multi-shard churn (O14a), which is the next engine work item.
Details in PLATFORM-EDGE-REPORT.md Part 4.

---

# Addendum 6 (2026-07-15): O14a hardening — oscillation eliminated at the client, root excursions remain

Three-arm A/B/C on Compute (fixed 128-conc × 4 generators): the v16
engine changes (WAL GC 5× tighter, per-shard flush stagger) plus the
admission guards the cloud fleet was missing (in-flight cap + RSS shed)
transformed behavior under the post-edge-fix load levels:

- deaths: arm B (no guards) lost all 4 instances in 2 minutes; arm C
  (guards) finished 4/4 alive
- stalls: 4 windows + one total collapse (A/B) → **zero** (C)
- client p90: **793 ms → 146 ms (5.4×)**
- throughput stability: p10:p90 spread **33× → 2.4×** at 2× the load

Honest residual: per-instance ack excursions >600 ms still occur (30/50
samples) — O14a's root cause is open; the fleet now degrades gracefully
around it. Next: per-PUT latency split (our pipeline vs Tigris tail).

## Addendum 7 — O14a root-caused and fixed: it was the event loop (2026-07-18)

The oscillation we could no longer blame on the network is closed. The
chase ran four instrumented fleet runs on Compute:

1. **Per-op store timing** (v17): every ack excursion co-occurred with
   *all* op classes slowing together (109/360 windows; C=0 "our
   pipeline quiet" verdicts; Spearman ack↔WAL-PUT-p99 0.79) — at 15–35 %
   CPU and no outbound-concurrency pinning. Ruled out: watermark logic,
   Tigris WAL tail, any hard ~50 egress cap (gauge hit 148 freely).
2. **Concurrency cap A/B** (v18, STORE_MAX_CONCURRENT=48): no
   improvement — excursions at outbound peaks of 19–34. Ruled out:
   HTTP/1.1 connection/handshake storms (and h2 was already out — Tigris
   negotiates HTTP/1.1).
3. **Timer sentinels** (v19): the discriminator. A raw OS thread kept
   4 ms timing while tokio timers on the same vCPU ran ~230 ms late,
   chronically; steal ≈ 0.05 %. The "slow store ops" were completions
   waiting on a starved event loop. On 1-vCPU instances,
   `#[tokio::main]` = one worker: any inline blocking quantum (SST
   build/compress inside SlateDB polls) freezes every future, including
   the durable-watermark acks. **O14a in one line: commit acks were
   hostages of a single-threaded event loop, not of the storage.**
4. **The fix** (v20): explicit runtime with `worker_threads = max(2, N)`
   (TOKIO_WORKERS=3 deployed). Delta at identical load: **excursion
   windows 30 % → 10 % (3×), median-window WAL-PUT p99 617 → 141 ms
   (4.4×), typical tokio drift ~230 ms → tens of ms** (thread-drift
   control unchanged). Residuals: genuine compaction-storm CPU
   saturation (needs blocking-quanta hygiene / bigger instances) and a
   now-cleanly-visible small true-Tigris-tail class.

Operational lessons folded back: Compute is x86_64 (aarch64 uploads
crash-loop into silent zombies — deploy wrappers now verify ELF arch
after download); redeploy-under-load can still zombie an instance
(healed by redeploy; the crash-loop-status platform ask stands).
