# Prisma Compute edge: per-instance admission-concurrency behavior

**From:** Prisma Streams team (SlateDB pilot)
**Date:** 2026-07-15
**Region / setup:** ap-southeast-1 (Singapore), 1 CPU / 1 GB instances, public
service URLs (`*.sin.prisma.build`), HTTP/1.1 clients
**Status:** measured behavior report + questions. Nothing here is a complaint —
the mechanism we found is arguably protecting our backends. We need to
understand and plan around it, and ideally see it documented.

---

## TL;DR

The edge appears to enforce a **per-instance admission-concurrency budget of
roughly 48–50 in-flight requests** (at ~100 ms backend latency, single client
source). Requests beyond the budget are **queued in the edge**, not rejected
and not delivered. The budget appears **adaptive** (latency-target driven) and
roughly **per source path**: it shrinks to ~2–3 for very fast responses,
sits ~50 at 100 ms responses, and grows past 100 aggregate with multiple
client sources or degraded backends.

Practical consequence: a 1-CPU instance serving 60 ms requests is capped at
~400–450 delivered req/s **at 16–25 % CPU**, and clients experience latency
that grows linearly with offered load (edge queueing) while every
backend-side metric looks healthy. None of this is documented in
`/docs/compute` or `/docs/compute/limitations`.

---

## How we found it (symptom)

During fleet load tests (4 load generators → 4 router instances → 4 app
instances), delivered throughput per app instance plateaued at ~400–450
req/s while:

- app-side CPU was 16–25 % of one core,
- app-side request handling was healthy (our internal ack p50 ≈ 60 ms),
- app-side request queue was EMPTY (commit batches of size 1 — the app was
  starved, not backlogged),
- client-observed p50 climbed with offered load: ~1 s at 384 offered
  concurrent, ~2 s at 1,000+.

So the queue was somewhere between client and app. We instrumented the app
with an in-flight request gauge (middleware counter, sampled every 2 s) and
a calibrated-latency endpoint, and probed.

## Measurement (reproducible)

Probe target: one instance of service `cps_iktpl0x0o5pz4mv90hhda819`
(kept awake), endpoint `GET /v1/debug/sleep?ms=100` — holds the request
100 ms, no other work. Load: closed-loop client on another Compute instance
in-region (HTTP/1.1, one connection per in-flight request), 40 s
measurement windows per point, 2026-07-15 ~03:00–03:30 UTC.

### Result 1 — calibrated 100 ms ladder (single client source)

| offered concurrency | delivered req/s | client p50 | implied in-flight at the app (rate × 0.1 s) |
|---|---|---|---|
| 8 | 77 | 103 ms (no queueing) | 7.7 — all admitted |
| 32 | 310 | 103 ms (no queueing) | 31.1 — all admitted |
| 128 | 495 | 254 ms | **49.5 — ceiling** |
| 512 | 475 | 1,040 ms | **47.5 — same ceiling** |

Below ~50 offered, the edge adds ~3 ms and admits everything. Above it,
delivered concurrency pins at ~48–50 and the surplus waits in the edge:
client p50 matches queue-depth ÷ rate exactly (128/495 ≈ 0.26 s,
512/475 ≈ 1.08 s). Our app-side gauge confirms the app never sees more
than ~50–64 concurrent in steady state (with one transient burst spike of
~300 at the start of the 512 point before the limit engaged).

### Result 2 — fast path (GET /health, ~1.5 ms service time)

| offered | delivered | client p50 |
|---|---|---|
| 4 | 2,197/s | 1.5 ms |
| 16 | 1,339/s | 1.8 ms (p99 61 ms) |
| 64 | 1,252/s | 18.8 ms (p99 246 ms) |

Effective delivered concurrency (rate × latency) stays ~2–3; the app-side
gauge reads 1–2. Fast responses are delivered nearly serially over what
looks like a very small connection window, capping even a trivial endpoint
at ~1.3–2.2k req/s per instance from one source — with queueing (p99)
already visible at 16 offered.

### Result 3 — multiple client sources

With four distinct source instances (each its own router VM) driving one
app instance, the app-side gauge showed 91–120+ concurrent admitted
(transient bursts to ~1,000 during a step change), i.e. the ~50 budget is
**not a global per-instance constant** — it scales with source paths
and/or drifts upward when the backend slows (we also observed windows
widening while our backend's latency was temporarily degraded).

### What it is NOT

- **Not bandwidth**: payloads in these tests are ≤ 4 KB; ~2 MB/s at peak.
- **Not a rate limit**: the fast path delivered 2.2k req/s when service
  time was small; the slow path capped at ~475 req/s — the constant across
  conditions is *concurrency × latency*, not rate.
- **Not the app**: the app's own gauge shows it idle-waiting below the
  ceiling (16–25 % CPU, empty internal queues, 60 ms internal handling).

## The model that fits all observations

An **adaptive concurrency limiter in the edge, per instance and roughly per
source path** — latency-target driven (Envoy adaptive-concurrency /
gradient-style): the window converges to keep observed latency near a
baseline, so fast backends get tiny windows, ~100 ms backends get ~50, and
windows widen with additional sources or drifting baselines. Excess
requests are buffered in the edge rather than 429/503'd.

## Impact on us

1. **Capacity per instance is a function of the limiter, not our engine**:
   ~400–450 req/s at our 60–100 ms commit latency, per instance, regardless
   of instance CPU. Fleet capacity = instances × slots ÷ latency.
2. **Overload is silent for backends**: when offered load exceeds
   slots × instances, clients see seconds of latency and eventually
   timeouts while every backend metric is green. We initially misdiagnosed
   this for a full day.
3. We now **scale on the measured resource**: our instances report their
   in-flight gauge in fleet heartbeats and scale out at 75 % of the
   measured ~48-slot budget — this works well (validated in a staircase
   run), but it's calibrated against an unpublished, adaptive number.

## Questions / asks for the platform team

1. **Is this an adaptive concurrency limiter, and what is the algorithm /
   target?** (Envoy adaptive concurrency? gradient2? custom?) Knowing the
   controller lets us predict the budget instead of measuring it.
2. **What are the actual limits** — per instance, per source/edge-node, per
   connection — and can they be **documented** in `/docs/compute/limitations`?
3. **Is the budget configurable per service?** Our workload is
   latency-bound-by-design (durable commits to object storage before ack,
   ~60–100 ms), so per-instance throughput is directly proportional to
   this window. Even 2× would double our per-instance capacity.
4. **Queue vs reject**: can the edge return 429/503 + `Retry-After` beyond
   a configurable queue depth instead of buffering seconds of latency?
   We'd much rather shed than queue (we're adding our own admission
   control, but edge-buffered requests consume budget before we ever see
   them).
5. **Burst semantics**: we observed transient admissions of ~300–1,000
   in-flight at step changes before the limit engages. Intentional
   (fail-open probe) or a gap? It briefly slams cold backends.
6. **Observability**: is there any way for a service to see its edge queue
   depth / admitted-window size? We currently infer it from client-side
   latency; a header or metrics endpoint would make scaling signals exact.
7. Related (reported separately, mentioning for cross-reference): replicas
   that die hard leave the version "running" with the service domain
   unbound (platform HTML 404) until a fresh deploy; combined with edge
   queueing this is hard to distinguish from congestion at the client.

## Repro kit

- Any HTTP service with a `sleep(ms)` endpoint deployed on Compute.
- A closed-loop client (fixed N workers, next request only after the
  previous completes) on another in-region instance, HTTP/1.1 with one
  connection per in-flight request.
- Sweep N ∈ {8, 32, 128, 512} against `sleep(100)`; plot delivered rate ×
  0.1 s. The plateau is the admission budget; client p50 above it is
  queue-depth ÷ rate.
- Our exact runs: service `cps_iktpl0x0o5pz4mv90hhda819` (probe app) and
  `cps_z2rh81y2ivf8wcgxsle9tuzt` (load client), ap-southeast-1,
  2026-07-15 ~03:00–03:30 UTC, plus the fleet runs on 2026-07-14/15 if
  edge-side logs are retained.

---

# Part 2 (2026-07-15): probable cause, given the substrate

We've since learned Prisma Compute runs on an on-prem deployment of the
**Unikraft platform** (Firecracker microVMs under the hood). The public
Unikraft material lines up with our measurements closely enough to name
the mechanism with some confidence.

## What the public material says

1. **The platform fronts every service with a custom, HTTP-aware
   load balancer plus an in-house controller.** Their scale-to-zero
   description: a request for a standby instance arrives, "a custom,
   front-end load balancer **buffers the request** and signals the
   platform's controller," which asks Firecracker to resume the microVM
   — request held open, single-RTT wake. (unikraft.com, scale-to-zero
   blog + how-it-works.)
2. **Per-instance in-flight and queue accounting are first-class
   platform concepts.** The instance-metrics API exposes, per instance:
   "number of **in-flight HTTP requests**" and "number of **queued
   inbound connections and HTTP requests**" (both drop to 0 in standby).
   That is exactly the two-level structure we measured from the outside:
   delivered concurrency pinned at ~48–50 while the remainder queued
   upstream. The proxy is not an opaque pipe — it maintains an explicit
   per-instance delivery window and a queue behind it.
3. **Autoscale on the platform is CPU-only today; connection/in-flight
   based scaling is on their public roadmap** ("Connection-based
   autoscale metrics"). A per-instance delivery window is a natural
   companion to that design (the queue length is the intended scaling
   signal).
4. **No public document states the window's size, adaptivity, or
   configurability.** Nothing in their docs or limitations pages
   mentions per-instance concurrency limits.
5. **Firecracker's own device rate limiters are the wrong shape to be
   our cause.** They are token buckets on bandwidth and ops/s
   (ingress/egress per device) — they would cap packets/throughput
   independent of service latency. Our measurements follow a concurrency
   law (delivered rate = window ÷ latency; window latency-dependent), so
   the NIC-level limiters are unlikely to be primary — but whether the
   platform configures them at all is worth one question (they would
   explain any *additional* flat pps/bandwidth ceilings).

## The likely "why"

The platform's signature economics are density (their material cites
100k–1M scaled-to-zero microVMs per server) and millisecond wake. Both
push toward exactly what we observed:

- **Buffer-and-wake, never reject**: instant 429s from the edge would
  break transparent scale-to-zero; holding requests is the feature.
- **A bounded per-instance delivery window**: every held request costs
  the proxy memory/fds; at 100k+ instances per host, per-instance
  budgets are structurally necessary. ~50 concurrent per instance is a
  sensible protective default for that regime — it just becomes the
  throughput ceiling for latency-bound workloads like ours
  (durable-commit acks of 60–100 ms ⇒ ~400–450 req/s/instance).
- **The apparent adaptivity** (window ~2–3 at 1.5 ms responses, ~50 at
  100 ms, >120 with multiple source paths, transient bursts ~1000 at
  step changes) is consistent with either an explicit latency-target
  controller or an HTTP/1.1 upstream connection pool that grows under
  queue pressure up to a cap. Our black-box data cannot distinguish
  these; the platform team can.

## Sharpened questions for the platform team

1. What sizes the per-instance delivery window (fixed constant, config,
   or adaptive controller — and if adaptive, on what signal)? Where does
   ~48–50 come from at ~100 ms service latency?
2. Is the window per instance, per (edge-node × instance), or per
   source path? (Our multi-source data suggests it multiplies with
   source paths.)
3. Can it be raised per service? Latency-bound services pay for it
   linearly in per-instance throughput.
4. Can queue depth / queue latency be bounded per service, with
   429 + Retry-After beyond the bound? (Their metrics already track the
   queue; we'd like it to shed.)
5. Are the in-flight / queued per-instance metrics available to tenants
   on this on-prem deployment? They're in the platform's public API
   (`GET /instances/metrics`) — plumbing them through Prisma Compute
   would let our fleet scale on the platform's own numbers instead of
   our in-process gauge.
6. Are Firecracker device rate limiters configured (rx/tx bandwidth or
   ops buckets)? If so, what values?
7. Related but separate: the platform supports
   `restart_policy: never | always | on-failure` (exponential backoff,
   default **never**). Which policy do Prisma Compute instances run
   with? See our companion crash-loop reproduction (`repro-no-restart/`)
   — `on-failure` at the platform layer looks like a one-flag fix for
   the zombie incidents.

## Public sources

- Scale-to-zero (LB buffers + controller + Firecracker wake):
  unikraft.com/blog/scale-to-zero and /docs/guides/features/scaletozero/
- How it works (proxy holds request open; modified Firecracker; density
  figures): unikraft.com/how-it-works/
- Instance metrics (in-flight vs queued, per instance):
  unikraft.com/docs/tutorials/instance-metrics
- Instance states + restart_policy (default `never`; `on-failure` with
  exponential backoff): unikraft.com/docs/platform/instances
- Autoscale is CPU-only today; connection-based metrics on the roadmap:
  unikraft.com/docs/features/autoscale, roadmap item
  "Connection-based autoscale metrics"
- Firecracker rate limiters (token buckets, bandwidth + ops):
  github.com/firecracker-microvm/firecracker docs/design.md

---

# Part 3 (2026-07-15, evening): reconciliation with the platform team's investigation — we were measuring our own instruments

The platform team's investigation (thank you — it was decisive) found that
the "~50 per-instance ingress cap" we reported was a measurement artifact:
**our load sources were themselves Compute instances, and a small Compute
instance cannot hold more than ~50 outgoing requests.** Their direct
measurements put real ingress at ~145 simultaneous / 1,271 rps through the
front door and ~188 / 1,616 rps direct to the host, with the front door
adding ~60 ms per request at peak from inefficient connection reuse.

We re-ran our calibrated probe to test this against our own app, with
**six** source instances simultaneously driving one destination
(sleep=100 ms, 96 workers each, 576 offered):

| measurement | value |
|---|---|
| per-source delivered (6 concurrent sources) | 201–295 req/s each (~20–30 slots — squeezed below the solo ~48) |
| aggregate delivered | **1,487 req/s ≈ 149 concurrent slots** |
| destination in-flight gauge | p50 211, p90 298, max 374 |
| single source, same probe (Part 1) | ~475 req/s ≈ 48 slots |

The aggregate lands almost exactly on the platform team's 145 front-door
figure. The complete two-layer model, all measurements reconciled:

1. **Source egress: ~48–50 concurrent outgoing requests per (small)
   Compute instance.** This is what every single-source ladder measured,
   and what bounded every in-platform load test we ran (with ≤3–4
   sources, 3–4 × 48 ≈ the front-door budget, so this layer always bound
   first — which is why we never saw layer 2).
2. **Destination ingress: ~145–150 concurrent through the front door**
   (~188 direct; the front door trims ~20 % and adds ~60 ms at peak).
   With 6 sources offering 288+, this layer binds and squeezes per-source
   windows to ~25.
3. Earlier "adaptive window" observations dissolve into these two static
   budgets plus queueing; the transient burst admissions (~300–1,000 at
   step changes) remain real and unexplained.

**Corrections to Part 1/2:** the per-instance *ingress* admission model is
withdrawn; the questions about window adaptivity are answered by the
two-layer model. What stands: the queue-not-reject behavior, the burst
semantics question, the observability ask, and — now sharpened — the
**egress limit** as the primary constraint for any service whose instances
call other services (our routers, our metrics path, any service mesh
pattern on this platform).

**On "is the egress limit expected?"** (the question from the thread):
nothing public documents it — not the Unikraft instance docs, limitations,
or metrics pages. Given the architecture (all instance traffic traverses
the platform proxy/NAT tier, which must hold per-connection state at
100k+ instances/host density), a per-instance outbound session budget of
~50 is a plausible protective default, likely the same budget class as
the ingress hold-queue. It is not a client-runtime artifact: our
generator is a Rust binary (Bun's fetch pool limit doesn't apply to child
processes), and the identical binary holds 768+ outgoing requests in
local Docker. The platform team's plan to raise it with the vendor is the
right path; we'd add: please also ask whether it's configurable per
service, since it caps any east-west calling pattern at
~50 ÷ callee-latency req/s per caller instance.

**Recalibration on our side:** our fleet's edge-slots scaling dimension is
recalibrated from 48 → 140 (75 % trigger ≈ 105 in-flight per instance),
and per-instance capacity planning through the front door moves from
~400–450 req/s to **~1.4–2.1k req/s at our 60–100 ms acks** — provided
callers are spread across enough distinct source instances. Router-tier
sizing now has its own rule: each router instance can deliver at most
~48 concurrent downstream, so routers must scale with
ceil(target_concurrency / 48) regardless of server count.

---

# Part 4 (2026-07-15, late): fix verification — confirmed, 2.5–4× across the board

Re-ran both the calibrated probe and the full 4×4×4 fleet staircase after
the platform team's edge fix.

**Calibrated single-source ladder (sleep=100 ms), before → after:**

| offered | before | after |
|---|---|---|
| 128 | 495 req/s, p50 254 ms (49.5 slots, queueing) | **1,238 req/s, p50 103 ms (123.8 slots, ZERO queueing)** |
| 512 | 475 req/s, p50 1,040 ms | 970 req/s, p50 512 ms (~97–124 slot ceiling) |

The old ~48-slot single-source budget is gone; the new soft ceiling sits
around ~100–125 concurrent per source path (2–2.5×), and below it the edge
adds no measurable queueing at all.

**Fleet staircase (4 gens × 4 routers × 4 servers, identical harness to
the pre-fix run):**

| level | pre-fix avg (client p50) | post-fix avg (client p50) | change |
|---|---|---|---|
| conc 128×4 | 480 req/s (883 ms) | **1,240 req/s (169 ms)** | **2.6× / 5.2×** |
| conc 256×4 | 398 req/s (1,963 ms) | **1,217 req/s (455 ms)** | **3.1× / 4.3×** |
| best observed window | 635 req/s | **2,760 req/s** | **4.3×** |
| max single-instance delivered | ~300 req/s | **1,186 req/s** | **4.0×** |

**The constraint has flipped.** Single instances now sustain windows of
600–1,186 req/s through the edge at healthy 59–62 ms acks — the ceiling we
previously could only reach by bypassing the platform entirely. The
oscillation still visible at the top levels is now OUR side: individual
instances' durable-commit paths degrade under multi-shard churn (server
ack p50 600–900 ms on affected instances — the long-standing SlateDB
watermark issue, O14a), which the edge fix has made unmaskable and which
is now our top engine work item. Thanks — this is exactly the outcome we
hoped for.

---

# Part 5 (2026-07-16): assessment of the platform team's updated report

Their update names the mechanism — **Conduit** (the shared proxy on all
public addresses) was doing a fresh connection + full TLS handshake for
nearly every request; the July 16 fix adds deliberate connection reuse
with a pooled cap per destination. Their numbers (Singapore 611→2,220
rps, p99 wait 11.3 s→0.99 s) corroborate our independent verification
(2.5–4×). This also *replaces* our adaptive-limiter hypothesis: the
"tiny window for fast responses" was handshake churn, not a gradient
controller. Excellent, honest work — root cause, fix, measurements, and
a stated list of what they did not address.

## Scorecard against our asks

| ask | status |
|---|---|
| root cause + fix + quantification | ✅ done (Conduit reuse) |
| document the numbers | 🟡 committed, not yet landed — includes per-path capacity + queue model |
| queue vs reject (bounded queue → 429 + Retry-After) | ❌ explicitly retained as queue-not-reject |
| observability (tenant-visible in-flight/queue) | ❌ explicitly not addressed — while the report itself advises "monitor in-flight requests rather than error rates," which tenants cannot do today |
| per-service configurability | ❌ not addressed |
| burst semantics (300–1,000 transient admissions) | ❌ unmentioned |
| egress limit resolution (vendor follow-up) | ❌ unmentioned; per-source ceiling remains (~half of direct path per Conduit machine) |
| crash-loop status truthfulness | separate track (repro-no-restart/), still open |

## New facts worth recording

- **Direct-path capacity of a 1-CPU instance: ~509–511 simultaneous /
  ~4,900 rps** — the platform's own measurement of the instance envelope,
  consistent with our docker engine numbers on slower silicon.
- **Through-Conduit ≈ half the direct path per traffic source**, bounded
  by a single Conduit machine per source; with 3 Conduit machines,
  few-heavy-client workloads (exactly our router-tier shape) land
  unevenly. Our fleet's per-source throughput will vary by Conduit
  assignment until this is spread or documented.
- **Fresh routes pay a one-time connection-setup burst** — interacts
  with scale-from-zero wakes and with our own 60 s client rotation
  (which now re-pays setup costs every rotation; we should re-test
  whether rotation is still needed post-fix).

## What we want them to work on next, in order

1. **Shed, don't queue** (bounded queue depth/time per service, then
   429 + Retry-After). The update *codifies* queue-not-reject; the
   misdiagnosis trap that cost us a day is now documented behavior
   rather than fixed behavior.
2. **Expose in-flight/queued per instance to tenants** — their own
   testing guidance requires it; it is also the input our autoscaling
   design (AUTOSCALING-DESIGN.md) assumes.
3. **Land the documentation commitment**, explicitly including the
   per-source/egress ceilings and expected Conduit-assignment variance,
   not just the happy-path numbers.
4. **Conduit balance for few heavy sources** (per-flow spreading or
   source re-hashing) — or a documented way for a service to know which
   Conduit it landed on.
5. **First-touch/burst semantics**: the fresh-route setup cost plus the
   step-change over-admission transients, both of which hit
   scale-from-zero services hardest.
6. Per-service pool/window configurability (nice-to-have once 1–3 land).

## Our own follow-ups (not theirs)

- Re-calibrate `SCALE_EDGE_SLOTS` (140 → ~250, per "half of direct
  path" ≈ 255 simultaneous through Conduit) with a quick post-fix
  6-source ladder; raise `ADMIT_MAX_INFLIGHT` (256) toward the measured
  direct envelope (~510) for guarded headroom.
- Re-test whether the 60 s RotatingClient workaround is still needed;
  it now costs a handshake burst per rotation.
- Re-run the O14a per-PUT latency split — with Conduit exonerated, any
  remaining ack excursions are ours or Tigris's.
