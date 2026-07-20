# Pilot experiment: scale-from-0 fleet on Prisma Compute

Date: 2026-07-13 · Region: Singapore (`ap-southeast-1` Compute, `sin` Tigris)
Project: `streams-pilot` (`proj_cmrj2f0ij3snqwfdvtof3w2v0`), workspace
`prisma-streams-slatedb`.

## Purpose

First production-shaped deployment of the SlateDB streams server:
1. Validate the **bearer-token** data plane and the **`__metrics__` internal
   stream** (per-tenant billing counters) on real infrastructure.
2. Test the **scale-from-0 mechanism**: Prisma Compute snapshots an instance
   to zero after ~5 s of inactivity and restores the memory snapshot on the
   next request. Four independent streams servers ride this; a pilot load
   balancer mimics the COMPUTE-SPEC routing (rendezvous hash by stream name,
   R1/R2) so each stream is pinned to one server and idle servers sleep.
3. Drive a ramping generic workload (rate doubles every 5 minutes) that can
   push all four 1-core servers, with a **live dashboard** showing per-server
   traffic, latency, and detected cold starts in real time.

## Topology

```
generator (Bun app) ──► lb (Bun app: rendezvous route + dashboard at /)
                          ├──► streams-1 ─┐  each: Rust binary (musl x64,
                          ├──► streams-2 ─┤  downloaded from Tigris at boot),
                          ├──► streams-3 ─┤  own PATH_PREFIX namespace in the
                          └──► streams-4 ─┘  shared Tigris bucket, 1 shard
```

- Servers: `streams-slate` cross-compiled for `x86_64-unknown-linux-musl`,
  spawned by a 15-line Bun wrapper that downloads it from a presigned Tigris
  URL. Env: Tigris creds, `AUTH_TOKEN`, `METRICS_KEY`, `INSTANCE_NAME`,
  `PATH_PREFIX=pilot/streams-N` (independent namespaces in one bucket via an
  object-store prefix wrapper).
- LB: consistent (rendezvous) hashing on the stream name → four upstream
  app URLs; per-upstream counters; cold-start heuristic = first request
  after >8 s idle taking >1.5 s; dashboard at `/` (1 s polling, stacked
  req/s chart, cold-start counts).
- Generator: 32 streams (`pilot-0..31`, spread across servers by the hash),
  ~220-byte JSON appends + 10 % `offset=now` reads, target rate
  `min(512, 2·2^(elapsed/5min))`/s, stats endpoint feeding the dashboard.

## Method

1. Deploy all six apps; verify health + auth (401 without bearer).
2. Let servers idle >5 s → snapshot to 0; confirm wake on demand.
3. Start the generator; observe ~4 ramp levels (≥20 minutes).
4. Verify the `__metrics__` stream on each server records per-stream
   (per-tenant) append/read counters — the billing feed.
5. Capture dashboard + `/stats`; update this report with observations.

## Expected outcomes

- **Correctness across snapshot/restore**: appends before sleep are readable
  after wake (all durable state is in Tigris; the restored memory image can
  only be *behind*, never wrong — worst case SlateDB fencing/retry noise on
  first use of stale TLS connections to Tigris).
- **Cold starts visible but bounded**: first request to a sleeping server
  pays snapshot-restore + possibly TLS reconnects; expected O(1–3 s), then
  steady-state in-region latencies (~20–40 ms durable appends).
- **Routing pins streams**: each `pilot-N` consistently lands on one server;
  the stacked chart should show all four servers active with roughly 8/32
  streams each.
- **Ramp tracks the schedule**: total req/s doubles every 5 minutes; the
  4×1-core fleet should absorb the 512/s cap comfortably (measured ~18 k/s
  per core locally with 25 ms storage).
- **Risks we're probing**: snapshot restore vs. live tokio timers and open
  Tigris connections (unknown interaction — this is the point of the test);
  LB-added latency; Compute request timeouts vs long-polls.

## Observations

### Deployment findings (before the run)

- **Compute image is minimal**: there is no `chmod` executable in `$PATH`.
  The wrapper's `Bun.spawnSync(["chmod", "+x", bin])` crashed every boot and
  all four services boot-looped, with the platform serving
  `404 Service not found` after ~5 s. Fix: `node:fs/promises` `chmod()`.
  Lesson: assume no external binaries at all; the wrapper must be pure Bun.
- **`deploy` does not attach the first version to the service endpoint by
  itself** in all cases; `compute versions promote --version … --service …`
  is the explicit way to bind the running version to the service URL.
- Cross-compiled musl binary (18 MB) downloads from Tigris and starts in
  well under a second in-region; total first-boot (source unpack + vars +
  Bun + binary download + SlateDB shard open) ≈ 5–7 s.

### Scale-from-0, measured directly (45 s idle, then probe)

| server | `/health` after wake | first durable append after wake |
|---|---|---|
| streams-1 | 200 in 0.27 s | 204 in 1.73 s |
| streams-2 | 200 in 0.24 s | 204 in 0.93 s |
| streams-3 | 200 in 0.25 s | 204 in 1.62 s |
| streams-4 | 200 in 0.26 s | 204 in 1.42 s |

(Times measured from a laptop ~230 ms RTT away; warm requests measure
0.24–0.34 s, so **snapshot restore itself adds ≈ 0 ms** to a trivial
request.) The cost hides in the *first Tigris round-trip*: the restored
memory image holds TLS connections that are dead, so the first durable
append pays reconnect + retry, 0.9–1.7 s. Subsequent appends drop back to
~0.15–0.25 s (≈ 20–50 ms in-region + laptop RTT). Correctness held: all
pre-sleep data was readable after wake, no fencing errors — consistent with
the design position that a restored image can only be *behind* Tigris,
never wrong.

### Auth + metrics (verified on live fleet)

- No/wrong bearer → `401` on every `/v1/*` route; with token: create `201`,
  append `204`, read returns decrypted records — on all four servers, both
  direct and through the LB.
- `__metrics__` stream (read with the separate metrics key): one JSON record
  per 15 s per instance with per-stream counters
  `{appends, append_bytes, reads, read_bytes, queue_ops}` — e.g. streams-1
  reports exactly its 6-of-32 rendezvous-assigned `pilot-*` streams. This is
  the per-tenant billing feed working as designed.
- One append during smoke testing returned `409` — stream had been created
  without a content type and appended with `application/json`. The protocol
  worked as specified; noted because it is an easy operator mistake.

### Ramp run (43 minutes, 9 levels, ~225 k requests)

Measured by a 30 s sampler against the generator and LB stats endpoints;
"achieved" is the delta of successful ops between samples.

| target req/s | achieved ok/s | client err/s | generator mean ms (cumulative) |
|---|---|---|---|
| 2 | 10.0¹ | 0 | 294 |
| 4 | 10.0¹ | 0 | 312 |
| 8 | 10.0¹ | 0 | 325 |
| 16 | 19.2 | 0 | 393 |
| 32 | 29.3 | 0 | 447 |
| 64 | 58.5 | 0 | 505 |
| 128 | 127.6 | 0 | 544 |
| 256 | 257.2 | 0 | 617 |
| 512 | ~130–330 | ~215 | 1794 |

¹ Generator fires ≥1 op per 100 ms tick, so low levels floor at 10/s.

**Through 256 req/s: zero errors anywhere.** ~162 k requests to that point,
0 generator errors, 0 LB upstream errors, 0 server 5xx. Achieved rate
tracked target within noise, and latency grew sub-linearly while load grew
128× (294 ms → 617 ms mean): each doubling was progressively absorbed by
group commit — more appends share each Tigris WAL flush, so the per-op cost
falls as concurrency rises. Requests spread across servers proportionally to
their rendezvous share of the 32 streams (streams-2/3 ≈ 9–10 streams each,
streams-1/4 ≈ 6–7; visible as a stable stacked band on the dashboard).

**At 512 req/s the pilot fleet hit its knee.** The four 1-core servers
kept serving — LB-side upstream error count stayed **0** and the LB was
still moving ~250–290 req/s — but per-request latency ballooned
(upstream ewma 1–3.9 s), and the open-loop generator (fires on a clock,
ignores completions) let its in-flight requests grow without bound; queued
requests began hitting their own 20 s client timeouts (~46 k aborts in
~3 min). Classic congestion collapse of the *client*, not the service:
every request the servers accepted was durably committed and acknowledged.
No cold starts were recorded during the entire run — sustained traffic kept
all instances awake, so scale-from-0 only appears at the idle edges (as
designed).

Reading of the knee: with ~220-byte single-record appends, TLS-per-hop and
1 shard per server, the pilot saturates around **~65–80 req/s per core**
with ~0.6 s mean end-to-end latency. This is the unbatched worst case —
the local bench does 18 k/s appends per core by batching many records per
request; the production remedy at this layer is admission control at the LB
(shed/queue above a per-server in-flight cap) plus the COMPUTE-SPEC
autoscaler adding instances, neither of which this pilot implements
deliberately.

### Post-run: durability across a full sleep/wake cycle

Generator stopped; after 120 s all four servers snapshotted to zero. All
four then woke on demand in **0.25–0.27 s** (`/health` 200, laptop RTT
included). `pilot-0` read back **in full through the LB after wake**:
1.63 MB, starting from the very first record of the run — everything
written during the ramp, including under peak congestion, was durable.

Per-server `__metrics__` totals for the run (the billing feed):

| server | pilot streams hosted | appends | append bytes |
|---|---|---|---|
| streams-1 | 6 | 35,410 | 8.44 MB |
| streams-2 | 11 | 66,898 | 15.95 MB |
| streams-3 | 10 | 62,711 | 14.96 MB |
| streams-4 | 5 | 31,366 | 7.48 MB |
| **total** | **32** | **196,385** | **46.8 MB** |

Two notable cross-checks:

- **Server-side appends (196 k) exceed client-acked ops (179 k).** The gap
  is the congestion phase: clients aborted at their 20 s timeout, but the
  server had already accepted and group-committed many of those appends.
  Commit-before-ack means an abandoned request is still a durable,
  well-ordered record — nothing tears. For billing this is the correct
  number to charge: the work was done.
- **`reads` ≈ 0 in metrics despite ~10 % read traffic**: `?offset=now`
  tail probes return `up-to-date` without touching records and currently
  skip the metrics hook. Read *requests* should be countable for billing
  even when they return no bytes — small follow-up in the metrics hook.

## Conclusions

1. **Scale-from-0 works with this architecture, and cheaply.** Snapshot
   restore adds ~0 ms to request handling; the only wake cost is the first
   Tigris round-trip re-establishing dead TLS connections (0.9–1.7 s once,
   visible in server logs as `object_store` retry bursts, all absorbed by
   the retry layer). Because all durable state lives in Tigris, a restored
   stale memory image is safe by construction — 43 minutes of writes plus
   a sleep/wake cycle produced zero inconsistencies.
2. **The bearer-token plane and per-tenant metrics stream are
   production-shaped**: 401 enforcement everywhere, and `__metrics__`
   yielded a complete, per-stream, per-15 s billing ledger that survived
   the run and reconciles with client-observed traffic.
3. **COMPUTE-SPEC routing behaves**: rendezvous hashing pinned every
   stream to one server for the entire run with zero routing errors and
   load split proportional to stream assignment.
4. **Known limits confirmed, not surprises**: unbatched 1-record appends
   saturate ~65–80 req/s/core at ~0.6 s mean latency; past the knee the
   fleet degrades by queueing (slow, correct) rather than erroring. The
   missing pieces are the ones deliberately out of scope: LB admission
   control and the autoscaler (desired.json) that would add servers at
   the knee instead of queueing.

Artifacts: LB dashboard `https://xnwv17i8sts9ljzdp1knlt2e.sin.prisma.build/`
(live), raw 30 s samples in the session scratchpad (`samples.jsonl`),
services `streams-1..4`, `pilot-lb`, `pilot-gen` in project
`streams-pilot` (Singapore).

---

# Run 2: closed-loop Rust harness

Run 1's knee (~330 req/s) was the harness, not the fleet: the Bun generator
and Bun LB each cap simultaneous outbound fetches at 256 by default, giving
a ~256/latency ≈ 425 req/s ceiling through *each* of two sequential stages,
and the open-loop generator collapsed once latency exceeded its firing rate.
Run 2 replaces both with one Rust binary (`src/bin/pilot.rs`, ~5 MB musl):

- **`MODE=lb`** — axum/hyper reverse proxy, same FNV-1a rendezvous hash
  (stream→server pinning identical to run 1), per-upstream stats +
  cold-start detection + the live dashboard, connection pool up to 8192
  idle per host, no artificial in-flight cap.
- **`MODE=gen`** — **closed-loop** generator: N workers each issue one
  request at a time (90 % single-record ~220 B appends / 10 % `offset=now`
  reads across the same 32 streams). N starts at 16 and **doubles every
  5 minutes** to 4096. Offered load self-paces to what the fleet absorbs —
  congestion collapse is impossible by construction. Latency is recorded
  in an HdrHistogram (p50/p99 exposed at `/`); requests still flow through
  the LB. (`UPSTREAMS` on the generator enables a direct-routing mode that
  bypasses the LB — reserved for a follow-up to isolate LB overhead.)

## Expected outcomes

- Throughput ≈ concurrency ÷ latency until the fleet knee: with ~300 ms
  floor latency, concurrency 16→4096 sweeps offered load ~50 → ~10 000+
  req/s. Early levels should show flat ~300 ms latency and doubling
  throughput.
- The knee appears as: latency (p50 first, then p99) rising with
  concurrency while achieved req/s plateaus. Rough prior: servers knee
  somewhere in the 2–10 k req/s aggregate range (1 core each); the 1-core
  Rust LB may knee in the same band — if LB ewma stays low while generator
  latency rises, the servers are the limit; if they diverge the LB is.
- Zero server-side errors expected throughout (degradation by queueing,
  as in run 1). Client errors should stay ~0 because closed-loop workers
  wait rather than pile up (30 s timeout as backstop).
- Servers stay awake (no cold starts) once the ramp starts.

## Harness findings while bootstrapping run 2

Three platform interactions surfaced before a clean run was possible; all
three are relevant to anyone load-testing (or serving) on Compute:

1. **h2 to the edge = one TCP connection = one replica.** The edge
   negotiates HTTP/2 via ALPN; reqwest then multiplexed *all* workers over
   a single connection, capped by max-concurrent-streams and pinned to a
   single LB replica. Measured closed-loop throughput *fell* as workers
   doubled (219 → 151 → 96 req/s at conc 16/32/64) with p50 flat at
   ~40 ms and p99 exploding — head-of-line queueing, not server
   saturation. Fix: `http1_only()` + large pool (one connection per
   in-flight request).
2. **The platform runs multiple replicas per service and pins clients by
   connection.** The generator's connections landed on one LB replica
   while browser/curl reached another (its in-memory stats read near-zero
   — dashboards lie unless stats are attributed at a layer that sees all
   traffic). Fixed by having the generator attribute per-server rates
   client-side using the same rendezvous hash (`ATTR_UPSTREAMS`).
3. **Harness instances must hold a keep-awake guard.** With h1 spreading
   sparse low-concurrency traffic across many LB replicas, each replica
   idled >5 s between requests, slept, and every wake cost snapshot
   restore + cold outbound TLS pools: conc-16 baseline degraded to
   ~22 req/s at p50 634 ms. `KeepAwakeGuard` from `@prisma/compute`
   (held for process lifetime in the Bun wrapper, `KEEP_AWAKE=1`) is the
   documented fix — the generator also needs it regardless, since it does
   outbound-only work with no inbound traffic to keep it alive.
   The *streams servers* deliberately do NOT hold guards: scale-from-0 is
   the behavior under test.

Run 2d = h1 + attribution + keep-awake + per-level windowed HdrHistogram
percentiles (cumulative histograms smear cold-start samples across levels).

## Observations

Run 2 was superseded before completing: its LB spread rendezvous over all
four servers unconditionally, which keeps every server awake at any load —
not the intended model. Partial results (closed-loop, all four active):
clean scaling 364/s @ conc 16 (p50 36 ms) through ~850/s @ conc 1024, zero
server errors; see run 3 for the corrected experiment. Two run-2 artifacts
worth keeping: (a) h2-to-the-edge multiplexes all load over one connection
— use HTTP/1.1 + large pools for load generation; (b) a low-request-rate
ack-latency degradation (~0.7–1 s appends at <30 req/s/server on aged
instances, ~35 ms when fresh or busy) was observed and instrumented
(`/v1/debug/timings`); diagnosis continues in run 3, which keeps the
commit-pipeline timing endpoint.

---

# Run 3: fleet-coordinated autoscaling (the corrected model)

The streams servers now coordinate their own scale (COMPUTE-SPEC §2/§4)
and the LB emulates the platform converging to it:

- **Servers** (all four share ONE data namespace `pilot/fleet-data`,
  8-shard topology): heartbeat `pilot/fleet/fleet/<instance>.json` every
  2 s with their req/s; every instance recomputes
  `desired = clamp(ceil(fleet_rps / (0.7 × 150)), 1, 4)` from the live
  heartbeat set (<10 s old) and CASes `fleet/desired.json` on change —
  scale-out immediate, scale-in after 60 s of sustained lower need
  (pilot-scaled hysteresis). A scaled-to-zero instance stops heartbeating
  and ages out of the live set: sleeping == scaled away, no extra state.
- **Shards open lazily on first routed request** and fence the previous
  owner (manifest CAS); a fenced-away shard gets a 3 s reopen holdoff
  (anti-flap). Shard choice keys off the stream NAME hash so the router
  computes placement without the stream epoch; keyspaces still use
  storage/segment hashes. Streams therefore MOVE between servers when the
  active set changes, with fencing as the only arbiter (spec §0.1).
- **LB**: polls `desired.json` + heartbeats (2 s) and `topology.json`
  (60 s) from Tigris; routes by shard (R1: name-hash longest-prefix) with
  rendezvous over ONLY the first `desired` upstreams. Dashboard shows
  per-server req/s from heartbeats (§9: heartbeats are the metrics bus)
  and live/sleeping state.
- Servers also got the 4 s object-store `pool_idle_timeout` (stale-pool
  fix) in this build. The `__metrics__` internal stream is disabled for
  this run: with a shared namespace its local-append path can fence-fight
  (each instance appending to a stream whose shard another instance owns);
  the billing feed needs to go through routed appends — noted as follow-up.
- Generator: unchanged closed-loop concurrency ramp via the LB
  (conc 4 → 1024 doubling every 5 min).

## Expected outcomes (written before the run)

1. Idle: all four servers sleep (stale heartbeats), desired stays at its
   last value, zero storage traffic. First request wakes server-1 only.
   (Verified during smoke: create through the LB woke server-1; servers
   2–4 stayed asleep. Cold create = 11.7 s: instance wake + first lazy
   8-shard-namespace open + registry create; subsequent appends ~1 s
   while the shard warms, then normal.)
2. As the ramp pushes fleet req/s past ~105 / ~210 / ~315 (0.7 × 150 per
   instance), desired steps 1→2→3→4; the LB widens the active set; newly
   routed servers cold-start (visible one-time latency blip), shards move
   to them by fencing, and the heartbeat chart shows load rebalancing.
3. During each transition, a brief spike of 503 `shard_moving` /
   fence-noise errors is expected at the client (the pilot LB has no R3
   replay buffering); it should clear within a few seconds.
4. On the way down (after the ramp), desired steps back down after the
   60 s hysteresis and unneeded servers sleep again.
5. Correctness: no lost acked appends across all shard moves — fencing
   guarantees single-writer per shard log at all times.

## Run 3 observations

### The low-rate ack-latency mystery: root cause isolated

`/v1/debug/timings` on a congested instance (8 shards, ~30–70 req/s
total), mid-run:

| shard | db.write p50 | durable-wait p50 |
|---|---|---|
| 000 | 0 ms | 15 ms |
| 010 | 0 ms | 19 ms |
| 111 | 0 ms | 27 ms |
| 110 | 0 ms | 334 ms |
| 101 | 0 ms | 430 ms |
| 001 | 0 ms | 590 ms |
| 011 | 0 ms | 664 ms |
| 100 | 0 ms | 855 ms |

Write path clear everywhere; the wait for SlateDB's durable watermark
degrades **per shard DB** — same process, same core, same Tigris (PUTs
measured 9–24 ms in-region). This exonerates CPU starvation, the platform
and the object store, and localizes the defect to per-DB state in the
WAL-flush→watermark pipeline. Consistent with the WAL bookkeeping numbers
(13k+ un-GC'd WAL SSTs per shard in runs 1–2; GC deletes sequentially at
~30/s while a 5 ms flush interval can mint up to 200 WAL SSTs/s under
load): the WAL GC sweep grows with backlog and interferes with the
flusher. Explains every prior sighting: fresh instances fast, busy
instances degrade within minutes, high-batch periods amortize it.
**Mitigation to validate**: flush_interval 20–25 ms (4–5× less WAL churn,
ack floor ≈ 40 ms — within spec targets) and/or SlateDB GC tuning;
upstream issue-worthy.

### Autoscaler finding: rps-only load vectors deadlock with closed-loop clients

Mid-ramp, one congested instance served everything at ~374 ms p50; the
closed-loop generator's offered *rate* therefore stayed low (~40 req/s),
below the scale-out threshold — so the fleet never scaled, which kept the
instance congested. COMPUTE-SPEC §4.2's queue-depth / p99 dimensions are
not optional extras: **a latency or queue dimension is required for the
inverted autoscaler to break this equilibrium.** (The pilot's single-dim
rps vector was a deliberate simplification; the spec had it right.)

### The staircase (run 3d, 50 min, 452,016 ok / 2,801 errors = 0.62 %)

Closed-loop concurrency doubling 4 → 1024 every 5 min, via the LB, with
the fleet deciding its own size (window percentiles at level end;
`desired` = values the fleet published during that level):

| conc | achieved ok/s | win p50 | win p99 | desired |
|---|---|---|---|---|
| 4 | 82 | 21 ms | 653 ms | 2 |
| 8 | 20 | 34 ms | 1.0 s | 1–2 |
| 16 | 35 | 492 ms | 1.1 s | 1 |
| 32 | 59 | 609 ms | 1.2 s | 1 |
| 64 | 165 | 91 ms | 1.1 s | 1→2→3 |
| 128 | 113 | 616 ms | 13.8 s | 1–3 |
| 256 | 243 | 769 ms | 2.5 s | 3→4 |
| 512 | 301 | 1.6 s | 4.1 s | 3–4 |
| 1024 | 266 | 3.3 s | 22.5 s | 2–4 |

What happened, level by level:

- **Fresh fleet, low load: 21 ms p50 durable appends through the LB** —
  the best latency of the whole pilot (4 s pool-idle timeout + spec shard
  settings in effect, no accumulated WAL backlog).
- **Levels 16–32: the rps-only autoscaler deadlock** described above —
  one degrading server, latency-capped throughput below the scale-out
  threshold, desired stuck at 1.
- **Level 64: the staircase engaged.** desired 1→2: server-2 woke,
  achieved jumped 123→245/s within seconds, p50 recovered 704→141 ms.
  Then 2→3 error-free. Each scale-out = instance wake + shard fencing
  handoffs; transition cost ≈ 100–250 client errors and a 2–3 s p99 blip,
  then clean.
- **Level 128: an LB-replica outage** (transport errors from the
  generator's pinned connections; the platform replaced a replica). The
  60 s client rotation healed it; the fleet correctly scaled in on the
  measured zero-load window and back out on recovery. p99 13.8 s is that
  window.
- **Level 256: full fleet.** desired 4; the just-woken server-4
  immediately carried the most load (168 rps vs 39–71 on the older
  three) — the freshest instance has no WAL-GC backlog and acks fastest,
  independently corroborating the degradation diagnosis.
- **Levels 512–1024: saturation of the degraded fleet**, desired
  oscillating 3⇄4 around the threshold (hysteresis damping downward
  moves), throughput ~300/s with deep queueing. One instance's heartbeat
  went stale mid-level and the count adjusted within ~10 s (§5.3 crash
  semantics); it rejoined on wake.
- **Post-run**: generator stopped → measured load → 0 → desired stepped
  down after hysteresis and instances went to sleep (descent recorded).

### Verdicts on the expected outcomes

1. ✅ Idle fleet sleeps; first request wakes exactly one server.
2. ✅ desired steps with load; LB widens the active set; servers wake on
   first routed request; shards move by fencing. (With the rps-only
   caveat above — the thresholds engage later than offered load because a
   congested instance hides demand.)
3. ✅ Transitions cost a brief error blip (no R3 buffering for in-flight
   requests at the moment of handoff; total 0.62 % over the run).
4. ✅ Scale-in after hysteresis; instances age out and sleep.
5. ✅ No acked data lost: fencing kept single-writer per shard through
   every transition (registry + shard logs shared; reads after handoffs
   returned full history). Final check: after the whole fleet slept, one
   read through the LB returned `pilot-0` in full (3.65 MB from record
   zero, 19 s cold: instance wake + shard reopen + stream) and woke ONLY
   the routed server — the other three stayed asleep.

## Run 3 conclusions

1. **The inverted-autoscaling model works end to end on Prisma Compute.**
   The fleet measured itself, published `desired.json`, the router
   converged, instances woke on first routed request and slept when
   routed around — twice up the staircase, once down, plus crash
   detection and full-sleep recovery, with 0.62 % transition-window
   errors and zero data loss.
2. **The load vector needs more than rps** (§4.2 as specced): a latency
   or queue-depth dimension is required, or a congested instance
   suppresses the very signal that would relieve it.
3. **SlateDB per-DB durable-watermark degradation under 5 ms flush churn
   is the single biggest performance defect found** — root-caused to
   per-shard state (write path clear, durable-wait degrading per DB, WAL
   GC backlog the prime suspect). Mitigations: 20–25 ms flush interval,
   GC tuning, and/or an upstream fix. Fresh instances deliver 21 ms p50
   durable appends through the LB — the architecture's floor is excellent;
   keeping instances at that floor is the work remaining.
4. **Platform lessons for the real router/autoscaler**: version replicas
   can keep serving stale code pinned by keep-alive connections (rotate
   clients, or drain-by-connection on deploy — and note KeepAwakeGuard
   keeps stale replicas alive too); env vars merge across deploys (pass
   the full set or unset explicitly); R3 replay is not optional — it is
   what makes stale routing harmless; the active set must be derived from
   the LIVE instance set, not an ordinal prefix.
5. Deferred follow-ups: metrics/billing stream via routed appends (shared
   namespace made local appends fence-fight); flush-interval validation
   run; drain protocol (§5.2) for graceful scale-in (current scale-in
   relies on fencing alone).

---

# Run 4: the fixes (25 ms flush + latency scaling dimension)

Changes under test, both landed in this build:

1. **`flush_interval` 5 ms → 25 ms** (D22 amended in README/COMPUTE-SPEC):
   WAL SSTs are minted ≤ 40/s/shard instead of ≤ 200/s — at or below the
   WAL GC's reap rate — so the backlog that degraded the per-DB durable
   watermark should never form. Ack floor moves from ~20 ms to ~40–60 ms
   (flush window + Tigris PUT), an accepted trade.
2. **Second scaling dimension**: each heartbeat now carries `ack_p50_ms`
   (p50 of commit durable-wait over the last 15 s, from the run-3
   instrumentation). Desired-count formula becomes
   `max(need_rps, need_latency)` where a live instance doing ≥5 rps with
   ack p50 > 250 ms (`SCALE_LATENCY_MS`) demands `live + 1` instances —
   a congested instance can no longer suppress the scale-out signal by
   capping its own throughput.

Method: fresh namespace (`pilot/v2-data`, `pilot/v2-fleet`) so run-3's
existing WAL backlog can't confound the flush-interval result; same
closed-loop ramp shape (conc 8 → 512, doubling every 4 min, via the LB).

## Expected outcomes (written before the run)

- Ack p50 ≈ 40–60 ms at low levels (up from 21 ms — the flush-window
  trade), and — the actual test — **it stays in that band for the whole
  run** on long-lived instances instead of rotting to 0.3–1 s.
- No repeat of the run-3 deadlock: if latency does climb past 250 ms on
  a loaded instance, desired increments within ~2 heartbeats even at low
  measured rps (`reason` strings in desired.json will attribute the
  dimension).
- Staircase reaches 3–4 instances earlier (offered load isn't throttled
  by a degraded solo server), transition blips similar (~0.5 % errors).

## Run 4 observations (30 min, 401,790 ok / 513 errors = 0.13 %)

| conc | ok/s avg | client p50 | client p99 | server ack p50 range | desired |
|---|---|---|---|---|---|
| 8 | 90 | 37 ms | 439 ms | 24–31 ms | 1→3 |
| 16 | 137 | 39 ms | 810 ms | 22 ms (one shard spike, below) | 2–4 |
| 32 | 179 | 51 ms | 1.1 s | 24 ms (one spike) | 4 |
| 64 | 291 | 78 ms | 1.1 s | 26 ms (one spike) | 4 |
| 128 | 260 | 439 ms | 1.5 s | 24–120 ms | 4 |
| 256 | 289 | 817 ms | 2.1 s | 27 ms (one spike) | 2–4 |
| 512 | 255 | 1.9 s | 4.2 s | 28 ms (one spike) | 4 |

- **25 ms flush: validated.** Server-side ack p50 sat at **22–31 ms for
  the whole 30-minute run** on healthy shards (client p50 37–51 ms through
  the LB at low/mid levels — the predicted trade vs run 3's fresh-instance
  21 ms, and vastly better than run 3's degraded 0.6–3.3 s). High-level
  client latency (439 ms+) is queueing at 1-core saturation, not the
  commit path — server acks stayed ≈ 30 ms underneath it.
- **Latency dimension: validated.** It fired repeatedly and correctly:
  every time an instance's ack p50 crossed 250 ms (all during shard-handoff
  churn), desired incremented within ~2 heartbeats — at rps levels that
  would have left run 3 deadlocked at 1 instance. The fleet reached
  desired=4 by conc 32 (run 3: still at 1 until conc 64+). `desired.json`
  `reason` strings attribute each decision to its dimension.
- **The rot is now rare and transient, and the WAL-count theory is
  refuted.** Exactly one shard (`101` on streams-2) degraded during the
  run — durable-wait p50 773 ms while its sibling shard in the *same
  process* sat at 24 ms — and it **self-healed** ~10 min later (run 3's
  rot never recovered). Decisively: the rotted shard had **2,176** WAL
  objects while healthy shards carried **5,896–6,723** — WAL backlog does
  not predict degradation. The better correlate is **fence/reopen
  frequency**: shard 101 was among the most-moved shards during desired
  oscillations. Upstream investigation should reproduce open→fence→reopen
  cycles against object storage and watch the durable watermark; our 25 ms
  flush reduced incidence ~16× but the underlying per-DB state remains.
- **Error rate 0.13 %** (vs 0.62 % in run 3) — all transition-window
  blips; zero errors outside scale events.
- Remaining rough edges, both known-shape: transitions themselves spike
  ack latency and re-trigger the latency dimension (the pilot's 60 s
  hysteresis is twitchy; the spec's 10 min + §5.2 drain would damp), and
  one instance twice went heartbeat-dark under load and rejoined on wake
  (needs the live-set-aware ring rather than ordinal active set, plus
  §5.3-style investigation).

## Run 4 conclusions

Both fixes shipped and validated: **D22 amended to 25 ms flush** (docs
updated in README + COMPUTE-SPEC) holds server ack p50 at ~22–31 ms
indefinitely instead of rotting to ~1 s, and the **ack-p50 heartbeat
dimension** (SCALE_LATENCY_MS=250) removes the rps-only scaling deadlock —
the two changes compound: the autoscaler now reacts to congestion, and
there is far less congestion to react to. Error rate improved 5×. The
sharpened SlateDB durable-watermark issue (fence/reopen-correlated, not
WAL-count-correlated, self-healing under low churn) is the remaining
upstream item.

## Follow-ups (implemented and deployed after run 4)

1. **Live-set-aware ring** (replaces the ordinal active set): both router
   and servers compute active = first `desired` ordinal instances MINUS
   any heartbeat-dark >30 s, with an unfiltered fallback so a fully-asleep
   fleet still wakes on the first request. A wedged instance (run 3/4's
   dark-server incidents) is now routed around within 30 s and rejoins on
   its next heartbeat; both sides derive the set from the same heartbeat
   data, and disagreement windows stay safe via R3 replay + fencing.
2. **Latency-dimension damping**: the ack-p50 breach must be sustained
   ≥20 s (`SCALE_LAT_SUSTAIN_SECS`) before it scales the fleet, breaking
   the transition→latency-spike→transition feedback loop observed in
   run 4. Scale-in hysteresis raised to 120 s in the deployment.
3. **Billing metrics via routed appends**: `metrics_flusher` now POSTs
   its 15 s records through the router (`METRICS_LB_URL`) like any tenant
   write, so the `__metrics__` shard has exactly one writer — its ring
   owner. Verified live: records from multiple instances landing in one
   stream, readable with the metrics key, no fencing conflicts.

Still deferred (design-level, tracked in the spec): §5.2 graceful drain
(scale-in still relies on fencing alone), and the upstream SlateDB
durable-watermark reproducer.

---

# Bench: single-ordered-stream ceilings (event size × batch)

Question: how does the per-stream ceiling respond to (1) event size and
(2) events per request? Setup: ONE stream, ONE shard, dedicated
1-CPU/1-GB server (no fleet, no LB), in-region closed-loop Rust driver,
25 ms flush. Charts: `charts/chart-size-sweep.png`,
`charts/chart-batch-sweep.png`; raw data `charts/sweep-data.json`.

## The two regimes

1. **Request-bound** (small events, small batches): the commit pipeline
   sustains **~400–840 requests/s** regardless of event size up to ~4 KB
   (replicated across 5 independent runs; best fresh-shard run 841 req/s,
   p50 303 ms at conc 256). Event size is nearly free here: MB/s grows
   linearly with size (0.03 → 1.7 MB/s from 64 B → 4 KB) at flat req/s.
2. **Byte-bound**: sustained payload throughput is capped by **SlateDB
   L0-compaction throughput, measured at only ~0.4–1.7 MB/s** on this
   hardware — far below the WAL/PUT path itself, which demonstrably moves
   **12 MB/s in bursts** (64 KB × 189 req/s) while L0 headroom lasts.
   When cumulative L0 formation outruns compaction (~8 × 4 MB L0s), the
   shard **write-stalls hard** (hangs, then errors) rather than
   backpressuring; drained re-runs of every ≥16 KB point measured ~0
   sustained.

## Batching (256 B events)

| batch | req/s | events/s | MB/s | regime |
|---|---|---|---|---|
| 1 | 841 | 841 | 0.22 | clean |
| 4 | 122 | 487 | 0.13 | errors (L0 debt) |
| 16 | 101 | **1,619** | 0.42 | errors |
| 64 | 24 | 1,502 | 0.39 | errors |
| 256 | 0 | 0 | 0 | stall |
| 1,024 | 1.7 | **1,706** | 0.44 | errors |
| 4,096 | 0 | 0 | 0 | stall |

Batching does exactly what the design predicts — it converts the byte
budget into events and slashes request count (1,706 ev/s at 1.7 req/s) —
but today the byte budget itself is pinned at ~0.4 MB/s by the compaction
defect, so measured batching gains cap at ~2× instead of the modeled
10–50×. **With compaction fixed (byte ceiling back to PUT-bound
~10+ MB/s), the model gives batch-16 ≈ 13k ev/s and batch-64+ ≈ 40k ev/s
per ordered stream.**

## Engine defects found and fixed/filed by this bench

- **OOM under byte-flood (fixed):** SlateDB's `max_unflushed_bytes`
  defaults to 512 MB — a 1 GB instance is OOM-killed before any
  backpressure fires. Now capped at 16 MB (`MAX_UNFLUSHED_BYTES`), per
  the §1.1 budget.
- **Unthrottled absorber starves the request path (confirmed §1.2 need):**
  absorb cycles of a hot stream saturate the single core; the spec's
  ≤15 % rate limit is now empirically mandatory, not advisory.
  (`ABSORB_BYTES`/`ABSORB_AGE_SECS` env knobs added.)
- **L0-full surfaces as stalls/hangs, not 429s:** the §12 admission
  backstop must convert write-stall into per-stream `429 + Retry-After`.
  Filed with O14 (upstream compaction throughput + stall behavior) —
  compactor logs showed `progress=0%, throughput=0 B/s` on a 6 MB job.

## Revised per-stream ceiling (supersedes the C3/C9 "6k rec/s or 8 MB/s")

**Today, sustained:** ~400–840 req/s AND ~0.4–1.7 MB/s per ordered
stream, whichever binds first; batching to ~1.7k ev/s; bursts to 12 MB/s.
**After the O14 compaction fix:** request ceiling unchanged, byte ceiling
→ PUT-bound (≥10 MB/s), giving the originally-modeled 6k–40k ev/s batched.
Streams needing more use per-key ordering (segments × these ceilings).

---

# Bench round 2 (2026-07-14, afternoon): the byte ceiling was two config defaults, not a compaction defect

The "compaction underperforms" conclusion above was wrong. With debug
logging on the flusher path, the stall reproduced locally and the
timeline shows compaction was *idle* during the stalls: a 6-L0 merge
completed in 3.1 s, L0 count was 2, and the memtable flusher still sat on
137 MB of frozen memtables for 14 s. Two interacting defaults explain
everything:

1. **`l0_max_ssts_per_key` (upstream default 8).** The flusher's dispatch
   gate checks the max number of *overlapping* L0 SSTs per key, not just
   the L0 count. A totally-ordered stream rewrites its tail/meta row in
   every memtable, so every L0 overlaps on that key: overlap == L0 count,
   and the per-key cap — which we had not raised alongside `l0_max_ssts`
   — becomes the real gate.
2. **`manifest_poll_interval` (we had set 60 s for idle-cost reasons).**
   The flusher learns that compaction freed L0 slots via manifest poll.
   With a 60 s poll, dispatch stayed gated on a stale L0 view for up to a
   minute after compaction had already cleared the debt. Result: writes
   pile into `max_unflushed_bytes`, appends see 30 s backpressure
   timeouts, our HTTP layer returns 408 storms.

Fixes (server env knobs): `L0_MAX_SSTS_PER_KEY` (0 = follow
`L0_MAX_SSTS`), `MANIFEST_POLL_MS` (default 2 s; 1 s under load; the old
60 s remains fine for idle shards but must not be global).

Also landed this round:

- **Committer pacing** (`pace_min_reqs=32`, `gather_window=15 ms`): the
  committer had been minting one commit group per arrival under trickle
  load; each group ≈ one WAL SST ≈ one serial PUT. Pacing gathers up to
  15 ms of queue when ≥32 requests wait. Local effect: batch=1 appends
  went from ~840 req/s to **12.4–13k req/s sustained** (p50 61 ms).
- **FrameCipher**: per-request cipher construction (AES key schedule)
  hoisted out of the per-record loop; with hardware AES
  (`--cfg aes_armv8` on ARM, auto-detected AES-NI on x86_64) crypto runs
  270–617 MB/s/core — at a 50 MB/s target the hooks cost ≤ ~18 % of one
  core. **Encrypt/decrypt was a minor factor, now smaller.**
- **Absorber pipelining**: the absorber read the hot log in serial 8 MB
  chunks (~10k rec/s) while ingest ran at 150k rec/s — hot tier grew
  without bound. `read_frames_range` + 4-way buffered window reads +
  `ABSORB_PASS_BYTES` pass cap + `TRIM_PER_OP` (deletes per Absorbed op)
  let absorption and trim track ingest.
- **Driver fixes**: concurrency ramp across warmup (a cold step to
  hundreds of workers was itself triggering the first stall), windowed
  progress logging, first-error logging.

## Post-fix single-stream numbers (local, s3lite @ 25 ms, M-series core)

45 s points, closed loop, 1 KB events unless noted:

| shape | req/s | events/s | MB/s | p50 | errs |
|---|---|---|---|---|---|
| batch=1, tiny | 12,637 | 12.6k | — | 60 ms | 0 |
| batch=1, 1 KB | 12,368 | 12.4k | 12.7 | 61 ms | 0 |
| batch=16, 1 KB | 2,452 | 39.2k | 40.2 | 99 ms | 0 |
| batch=64, 1 KB | 862 | **55.1k** | **56.5** | 257 ms | 0 |
| batch=16, 256 B | 8,426 | **134.8k** | 34.7 | 82 ms | 0 |
| batch=256, 256 B | 593 | 151.7k | 39.0 | — | 733* |
| batch=4096, 256 B | 43 | **177.1k** | 45.5 | — | 0 |

*errors were the step-load transient at conc 733.

The pre-fix table above ("~0.4 MB/s sustained") is superseded: the same
box, same emulator, same protocol now sustains **50+ MB/s and 50k+
events/s on one totally-ordered stream**, and the request-rate target
(5k req/s) is exceeded 2.5× at 1 KB bodies.

## Sustained runs

Closed-loop max is a stress shape — it slams the pipeline into its
backpressure ceiling and oscillates. Pinning offered load at the target
(Little's law: in-flight ≈ target × latency) is the honest "sustained"
test:

- **Request shape (batch=1, tiny)**: 7.3 min flat — avg 12,494 req/s
  (window min 12,113 / max 12,975), zero errors, no drift. (Run
  truncated only to repurpose the driver.)
- **Byte shape (batch=64 × 1 KB, in-flight pinned to 12 MB, absorber ON
  age 45 s, trim 262k/op, history tier receiving the full stream)**:
  first minutes at 47–53 MB/s, then **7.8 min continuous at 37.5 MB/s /
  36.5k ev/s average** (quartiles 39.6 / 34.7 / 37.7 / 37.8 — no debt
  spiral), 567 errors over the span, ended by the emulator being killed
  under host swap pressure — the engine and its steady-state machinery
  were clean at kill time. 45 s closed-loop at the same shape: 56.5 MB/s,
  0 errors.

Local sustained caveats: the in-RAM emulator dies (host swap pressure)
if the hot set outgrows a few GB — two earlier attempts at closed-loop
max (45 MB/s avg, 12–46 MB/s oscillation) ended with the emulator killed,
not the engine. On real object storage this failure mode does not exist;
the engine itself never wedged with the fixes in.

## Cloud (Tigris, 1 CPU / 1 GB Compute instances, Singapore)

The same protocol against real Tigris — direct driver→server, no LB:

- **The WAL flush interval must match the backend PUT RTT.** At 25 ms
  flush the flusher mints ~40 WAL SSTs/s; one serial Tigris PUT pipe
  (~45 ms p50) sustains ~22/s — the durable watermark falls behind
  (durable-wait p90 518 ms, 408 storms). At **50 ms flush** durable-wait
  is p50 68 ms / p90 82 ms / max 90 ms — healthy. Rule: flush_interval ≥
  PUT p90. (D22 gains a per-backend-RTT rider.)
- **The platform edge caps request concurrency per instance.** Server-side
  queue_wait is ~0 ms and commit groups are size 1 while client p50 sits
  at ~1 s: requests queue at the edge, not the server. The 384-worker
  closed loop achieves ~390–430 req/s regardless of server headroom —
  a delivery ceiling of roughly `edge_concurrency / server_latency`, not
  an engine limit. (The same engine takes 12.6k req/s locally.) Fleet
  scale-out multiplies this per-instance edge allotment; verified in the
  4-instance pilot runs.
- Cloud quick points (60 s, closed loop): batch=1 391 req/s (0 errs);
  batch=16 429 req/s / 6.9k ev/s / 7.0 MB/s; batch=64 at full conc
  collapses (24 MB in flight > box ceiling) — pinned sustained run below.
- **Platform lesson (cost us one broken run + diagnosis): `--env` writes
  PROJECT-scope variables.** Both services shared `BINARY_URL`, so
  redeploying the server after a driver deploy booted the *driver binary*
  on the server service (its axum app 404s everything but `/`; the
  platform serves headerless 404s for the rest). Fix: per-service names
  (`SERVER_BINARY_URL` / `GEN_BINARY_URL`). Also: verify `/health` by
  BODY, not by curl exit code.

## Sustained finals (all runs)

| run | shape | span | result |
|---|---|---|---|
| local opt8s | batch=1 tiny, conc 768 | 7.3 min | avg 12,494 req/s (12.1–13.0k), 0 errors |
| local opt9 | batch=64×1 KB, 12 MB in-flight, absorb age 45 s | 7.8 min | avg 37.5 MB/s / 36.5k ev/s, quartiles flat, 567 errs; emulator killed |
| local opt10 | batch=64×1 KB, 16 MB in-flight, absorb age 20 s | 7.8 min | avg 35.2 MB/s / 34.4k ev/s (window max 53), 506 errs; emulator killed |
| cloud cb15 | batch=64×1 KB, 6 MB in-flight, Tigris, 1 CPU/1 GB | 25 min | 2.9 MB/s / 2.8k ev/s, p50 250 ms, 24 % errors (408 churn) |

Reading: the engine's steady state holds mid-30s MB/s on the test laptop
for as long as the in-RAM emulator survives (it was jetsam-killed at
~6–7 GB RSS in every long run; host swap was pre-loaded with ~10 GB from
an earlier crash — the engine never wedged). The pilot-class cloud box
completes a 25-minute run but with heavy 408 churn: one core must run
HTTP+TLS, AES, the LSM, and compaction against ~45 ms PUTs, and the
shared-prefix shard carried three earlier benches' compaction debt.
Cloud validation of the full 5k/50k/50 MB envelope needs ≥4-core
instances, per-bench prefixes, and producer→owner routing that bypasses
the edge concurrency cap — engine-side, nothing further is implicated.

---

# Flush-interval A/B (2026-07-14, evening): 25 ms vs 10 ms vs 10 s

Question: with the flusher-gate fixes in, does a lower flush interval durably
help? Three arms, identical config otherwise (fresh namespaces, 1-core
server, 25 ms-latency store, absorber active, 40 s closed-loop points):

| shape | 25 ms (control) | 10 ms | 10 s (for completeness) |
|---|---|---|---|
| batch=1 tiny | 12,624 req/s, p50 60.3 ms | 12,570 req/s, p50 60.5 ms | 154 req/s, p50 4,997 ms |
| batch=1 × 1 KB | 12,163 req/s, p50 61.9 ms | 12,092 req/s, p50 62.6 ms | 154 req/s, p50 5,001 ms |
| batch=16 × 1 KB | 44.8 MB/s | 37.6 MB/s (−16 %) | — |
| batch=64 × 1 KB | 44.7 MB/s (759 errs)* | 55.6 MB/s (0 errs)* | — |

*the batch=64 points straddle the backpressure ceiling and are noisy
between runs; the batch=16 regression is the signal.

Verdict: **10 ms is not better — it is the same latency and slightly worse
on bytes.** The serial WAL PUT pipe (~40 PUTs/s at 25 ms RTT) sets the
durable cadence; an interval below the backend PUT latency just mints
more, smaller WAL SSTs (more churn, more GC) without moving the ack
floor. And a 10 s interval pins p50 at exactly interval/2 ≈ 5 s with
closed-loop throughput collapsing to conc/5 s — the flush interval is the
ack latency floor, so large values are unusable for the product. This
closes D22 from both directions: `flush_interval = max(25 ms, PUT p90)`,
no benefit below, direct latency damage above.

---

# Run 5 (2026-07-14, evening): the 1→4 staircase re-run on the tuned engine

Same harness as run 4 (32 streams via the LB, closed-loop generator
doubling every 5 min c=8…256, four scale-from-zero 1-CPU/1-GB servers,
Tigris): server v10 (per-key L0 cap, 1 s manifest poll, committer pacing,
pipelined absorber, FrameCipher) + 50 ms flush (Tigris PUT-p90 rule).

**Found and fixed en route: the live-set ring deadlocks scale-out wake.**
The post-run-4 follow-up made ring routing live-set-filtered (dark >30 s
⇒ evicted). But a *newly desired* instance is dark precisely until it
gets its first request — which the filtered ring never sends. First
attempt of run 5 sat at desired=4 / live=1 indefinitely. On real
platforms the scaler starts instance N+1 out of band; the pilot LB now
emulates exactly that (out-of-band `/health` wake ping to desired-but-
stale ordinals, pilot v16). Run 4 never hit this because it predated the
live-set follow-up — the re-run caught a real interaction bug.

| level (conc) | run 4 achieved / p50 | run 5 achieved / p50 | desired→live (run 5) |
|---|---|---|---|
| 8 | 45/s / 35 ms | 153/s / 52 ms | 2→2 |
| 16 | 154/s / 39 ms | 258/s / 59 ms | 3→3 |
| 32 | 160/s / 52 ms | 84/s* / 77 ms | 4→4 |
| 64 | 318/s / 77 ms | 338/s / 114 ms | 4→4 |
| 128 | 247/s / 412 ms | 283/s / 412 ms | 4→4 |
| 256 | 294/s / 759 ms | 337/s / 892 ms | 4→4 |
| 512 | 283/s / 1,915 ms | (not offered) | — |
| **errors** | **513 / 401,790 = 0.13 %** | **1 / 484,892 = 0.0002 %** | |

*the conc=32 level average includes the 3→4 scale-out transition window.

Reading:
- **Errors: 650× better.** One failed request in the entire 33-minute run
  (run 4 had 513, concentrated in scale transitions and the top level).
- **Low-load throughput 2–3× better per level** (pacing + healthy durable
  pipe); the +15–25 ms p50 at low conc is the deliberate 50 ms-flush
  trade, and it buys the flat error line: run 4's durable watermark fell
  behind at load (25 ms flush vs Tigris PUTs), run 5's never did.
- **The staircase itself is clean**: desired 1→2→3→4 strictly with load,
  live tracks desired at every step (run 4's live count was stuck at 3
  for most of the run), and desired returns to 1 after the generator
  stops.
- **Above conc≈64 both runs converge to ~300–370/s: that is the single
  1-CPU generator box saturating, not the fleet** — the servers stopped
  being the constraint, which is the point of the exercise.

Charts: `charts/chart-staircase-before-after.png` (both runs, same
scales), plus the re-created single-stream sweep charts
(`chart-size-sweep.png`, `chart-batch-sweep.png`) with the pre-fix series
kept as the muted reference. Post-fix sweep highlights: every event size
64 B–1 MB clean at 12.9k req/s (small) / 33–48 MB/s (large); batching
monotonic to 173k ev/s clean at batch=1024 (175k burst at 4096).

---

# Run 6 (2026-07-14, night): utilization-based scaling + a 4-generator harness

Changes under test, per the "why did it scale at 5 % load?" question:
- **Heartbeats now carry measured CPU** (`cpu_pct`, getrusage over the 2 s
  interval). Run 5's early scale-out was the stale `SCALE_RPS_CAPACITY=150`
  constant — the engine had gotten 10× faster and the constant still
  described the old one. Assumed-capacity constants rot; measurements
  don't.
- **New desired-count dimensions**: capacity planning
  `ceil(fleet cores-in-use / 0.75)`, hot-instance (any loaded instance
  sustaining ≥ 75 % CPU asks +1, 20 s damped), ack-latency backstop
  (unchanged), legacy rps dimension disabled (0). Scale-in uses a
  conservative divisor (50 %) so the fleet doesn't flap at the boundary.
- **Harness scaled out**: 4 generators × 4 LBs (1:1), distinct stream
  namespaces (`STREAM_PREFIX`), batch=16 appends so each request carries
  real work.

Findings:
1. **The 75 % signal behaves exactly as specified — in both directions.**
   At 394 req/s aggregate (≈6.3k records/s, all on streams-1) the server
   measured 21 % CPU and desired stayed 1. Run 5's config would have
   demanded 4 instances here. Local check: an 8k req/s load measures
   ~85–109 % CPU and correctly publishes desired=2 (util dimension).
2. **CPU alone is insufficient on this platform — the edge saturates
   first.** At conc 64×4 the single routed-to instance plateaued at its
   ~400 req/s edge delivery envelope with **16.7 % CPU** and 54 ms acks:
   clients queued at the edge (p50 500–830 ms, first errors) while the
   server idled and the fleet — correctly, per its signal — refused to
   scale. The per-instance edge concurrency limit is a real scarce
   resource that CPU cannot see. Fix (run 7): re-purpose the rps
   dimension as the **edge delivery envelope** (a platform property,
   ~400 req/s/instance, NOT engine capacity): desired ≥
   ceil(fleet_rps / (0.75 × 400)). Multi-dimensional scaling — util for
   the engine, envelope for the platform, latency for everything else —
   is the production shape (§4.2 gains this).
3. **Platform failure mode (took the run down at t≈20 min): a replica
   that dies hard leaves its version "running" but the service domain
   unbound.** Requests get the platform's HTML 404; wake pings can't
   revive it (nothing reaches the process); only a fresh deploy rebinds.
   This also retro-explains the cb12 zombie earlier today. Likely
   trigger here: **our own §1.1 violation** — the single-shard bench's
   `MAX_UNFLUSHED_BYTES=64 MB` was carried into an 8-shard config
   (8 × 64 MB of permitted unflushed bytes on a 1 GB box). Run 7 restores
   the spec budget (16 MB/shard, 8 MB L0s). Platform asks filed: version
   status should reflect replica health, and crashed replicas should
   restart.

---

# Runs 7–8 + soak (2026-07-15, overnight): multi-signal scaling, chaos, and the 512 MB default that was killing instances

## Run 7 (edge-envelope dimension added)

With CPU alone, run 6 proved the edge saturates first; run 7 added the
delivery-envelope dimension (`ceil(fleet_rps / (0.75 × 400))`). Staircase
tracked load correctly (desired 2 at ~400 total, 3 at ~600, 4 at ~900),
BUT at full overload (conc 256×4) two new defects appeared:
1. **The fleet scaled IN mid-congestion** (desired 4→2): measured rps
   falls when clients queue at the edge, and server-side ack latency
   (60–80 ms, healthy) cannot see client pain (p50 1.6–2 s). Fixed: the
   router now publishes what clients experience (`routers/<n>.json`,
   client_p50_ms) and the fleet folds it in — breach ⇒ +1 AND scale-in
   blocked while hot (`SCALE_EDGE_LATENCY_MS`, default 1000).
2. **Rendezvous stranding**: 8 shards over 4 instances left streams-3
   with zero shards (~10 % probability at that ratio). 16+ shards ≈ 1 %;
   production's 24–32 shards/instance makes it negligible. (§3.2 note.)

## The zombie generator, found

Five instance deaths during the day's runs, all with the same signature:
version "running", domain unbound, platform HTML 404, unrevivable by
traffic or wake pings — only a redeploy rebinds. The cause was OURS:
**SlateDB's default block cache is 512 MB PER DATABASE**
(`DEFAULT_BLOCK_CACHE_CAPACITY`), and the pilot ran 8–16 shard DBs plus
per-stream history DBs per 1 GB instance with no shared cache — §1.1
("one shared cache, `with_db_cache`") was specified and never
implemented. Instances died of cache-fill in 20–40 min under load. Fix:
one 192 MB `FoyerCache` across all shard DBs (`SHARED_CACHE_BYTES`) and
a 32 MB one across history DBs (`HISTORY_CACHE_BYTES`). Measured effect:
pre-fix lifespan under load 20–40 min; post-fix, one instance lasted
60–80+ min (one late-soak death remained — see open items), the other
three ran the full 2 h+ soak clean.

Platform ask (filed): replica crash must not leave a "running" version
with an unbound domain, and crashed replicas should restart — every
occurrence needed an operator redeploy.

## Run 8: staircase → overload → chaos kill → 2 h soak

4 generators × 4 LBs × 4 servers, batch=16 (~230 B records, ≈16× the
per-request work of run 5), multi-signal scaling live.

- **Staircase**: desired tracked offered load through the envelope
  dimension; CPU stayed 15–25 % (the edge, not the engine, is this
  platform's per-instance ceiling — expected and now measured twice).
- **Overload (conc 256×4)**: goodput fell (~600 → ~400/s) with client
  p50 2–2.5 s — offered load beyond the edge envelope degrades service
  because the pilot has no §12 admission control; the fleet correctly
  held desired=4 (edge dimension) instead of flapping.
- **Chaos kill at peak**: destroyed streams-2's running version at
  16:47:06 with streams-1 already platform-dark — HALF the fleet gone at
  maximum offered load. The two survivors absorbed all 16 shards within
  ~90 s and served 620–720 req/s at 51–61 ms acks — better than the
  overloaded 4-instance state. Zero operator action for the failover
  (recovery redeploys restored N=4 afterwards).
- **Soak (conc 64×4 ≈ the healthy plateau)**: 2 h continuous. Post-fix
  window (v13, 78 min sampled): avg 490 req/s ≈ 7k records/s delivered,
  client p50 dead flat across quartiles (402/405/409/410 ms — no
  drift, no durable-watermark rot at this level), server acks 57–67 ms,
  ~0.4 % errors (closed-loop timeout tail through the edge + one
  instance-dark window; liveMin=3, liveAvg=3.9).
- **Scale-in bookend**: generators destroyed 19:09:55; desired 4 → 1
  within ~4 min; instances back to sleep.

Charts: `charts/chart-run8-stress.png` (full arc with event
annotations).

## Open items from this round

- One v13 instance still went dark late in the soak (60–80+ min):
  remaining memory pressure (16 shards × memtable/WAL budgets + absorber
  transients ≈ 500–700 MB worst case) or a second platform failure mode.
  Needs: RSS in the heartbeat (trivial now that getrusage is wired),
  §1.1 budget enforcement (max_unflushed scaled by shard count), and a
  self-watchdog (an instance that cannot bind its domain should
  self-report; the fleet already routes around it).
- Admission control (§12) is now the top missing runtime piece: the
  overload phase showed the failure mode it prevents.
- The generator boxes saturate at ~350–450 req/s each: stressing the
  engine (not the edge) needs either bigger generator instances or
  direct-to-owner routing (bypassing the per-instance edge envelope).

---

# Edge probe (2026-07-15): the per-instance ceiling is an ADMISSION-CONCURRENCY budget, not rate, CPU, or network

Question: how can throughput degrade at 16–25 % CPU? Instrumented the
server with an in-flight request gauge (+ RSS) in every heartbeat, added
a calibrated-latency endpoint (`/v1/debug/sleep?ms=`), and ran
concurrency ladders through the platform edge against a single isolated
instance.

**The decisive measurement (sleep=100 ms ladder, single source):**

| offered conc | delivered | client p50 | admitted slots (rate × 0.1 s) |
|---|---|---|---|
| 8 | 77/s | 103 ms (zero queueing) | 7.7 — all admitted |
| 32 | 310/s | 103 ms (zero queueing) | 31.1 — all admitted |
| 128 | 495/s | 254 ms | **49.5 — ceiling** |
| 512 | 475/s | 1,040 ms | **47.5 — same ceiling** |

Below the budget the edge adds ~3 ms; above it, clients queue in the
edge (p50 = depth/rate, pure Little's law) while the server sees a fixed
~50-deep pipeline — hence 400–450 req/s appends (50 slots ÷ ~105 ms
ack+edge) at 16–25 % CPU with healthy 60 ms server acks. Not network
(bandwidth at these sizes is ~2 MB/s), not a rate limit (the fast path
delivered 2.2k/s), not the engine.

**Refinement from the fast path + multi-source runs: the budget is
adaptive, not fixed.** GET /health (1.5 ms service) was delivered over
an effective window of only ~2–3 (rate ceiling ~1.3–2.2k/s, server gauge
1–2, p99 queueing at conc ≥ 16); with four source LBs, instances carried
91–120+ in-flight (transient bursts to ~1000); and windows drift up when
backend acks degrade. This is the signature of a latency-target-driven
adaptive concurrency limiter in the edge, roughly per source path, that
converges near ~50 at our append latencies. Exact algorithm unknown
(undocumented — the docs list no limits); for scaling purposes the
number that matters is the measured working budget ≈ 48 at healthy
latency.

**Scaling on the real resource (implemented, validated in run 9):** the
heartbeat's in-flight gauge measures admitted concurrency directly —
`desired ≥ ceil(Σ inflight / (0.75 × 48))` (`SCALE_EDGE_SLOTS`, replaces
the rps envelope; rps dimension now 0/off). Run 9 (staircase, 4
generators): the slot dimension scaled the fleet ahead of queue
formation through the low/mid levels — desired=4 by conc 32×4 with the
per-instance in-flight sums crossing the 36-slot threshold, while acks
held 57–60 ms. At conc 256×4 (≈ 2× the whole fleet's slot budget)
overload behavior matches run 8 — the missing piece there remains §12
admission control, not scaling.

**The measured per-instance resource table (this platform, this class):**

| resource | capacity (measured) | utilization signal (in heartbeat) | scale-out at |
|---|---|---|---|
| edge admission slots | ~48–50 (adaptive; healthy-latency working point) | `inflight` / 48 | 75 % |
| CPU | 1 core | `cpu_pct` (getrusage) | 75 % |
| memory | 1 GB (instance dies near cap) | `rss_mb` / 1024 | alarm 78 %; shed/scale before |
| durable pipe | ~20 WAL PUTs/s/shard at 50 ms flush | `ack_p50_ms` vs 250 ms threshold | sustained breach ⇒ +1 |
| client experience | — | router `client_p50_ms` vs 1 s | breach ⇒ +1, blocks scale-in |

---

# Docker fleet campaign (2026-07-15): the engine without the edge

Per the "test scale-out locally in containers" direction: 4 × Docker
containers (`--cpus 1 -m 1g` — faithful 1-core/1-GB replicas with real
cgroup OOM), s3lite object store on the host (25 ms/op), pilot LB +
generators native on the host. No platform edge anywhere in the path.

## What one container does without the edge

**batch=1 appends: 10,590–11,294 req/s at p50 63–66 ms, zero errors** —
~26× the ~400–450 req/s the same binary delivers through the platform
edge. CPU 57–69 % at that rate. The cloud per-instance ceiling is
conclusively the edge's admission budget, not the engine.

## Findings → fixes (each validated by rerun)

1. **cgroup OOM reproduced the cloud zombie in 90 s** (RSS 218→1030 MB
   at full throughput, OOMKilled=true). Two fixes:
   - **mimalloc as global allocator**: musl's malloc fragments ~4× under
     this workload — identical load re-run: RSS 190 MB where it had been
     850 MB. Adopted for all builds.
   - **RSS-reactive write shedding** (`ADMIT_RSS_SHED_MB`, 429s writes
     while RSS > threshold; 500 ms cached sampler): the identical byte
     flood that OOM-killed the container now sheds 28k requests, serves
     11 MB/s goodput throughout, and RSS self-heals (797→412 MB).
     OOMKilled=false.
2. **Admission control without client backoff is a reject storm.** First
   staircase: 2 × 2,048 closed-loop workers with instant-retry-on-429
   drove goodput to ~1/s (2.7 M rejects), starved /health and the
   heartbeat loop — the fleet couldn't even publish its own overload.
   Fixes: generators honor `Retry-After` + jitter (§12.2 client
   contract), and the shed path **tarpits 25 ms** before responding so
   non-compliant closed-loop clients self-pace. Re-run at the same
   offered load: goodput held, zero errors, clean throttle counters.
3. **Pilot LB was blind on plain-http object stores** (`fleet_store`
   lacked `allow_http`; every heartbeat/desired read failed silently and
   the LB routed the whole fleet to instance 1 — which served 10.3k
   req/s at 99 % CPU rather than falling over). Fixed; also proved the
   server-side scaling had been correct all along: desired.json read
   `count=4, hot_cpu=93%` while the LB ignored it.
4. **CPU-driven scaling validated end-to-end** (the thing the cloud edge
   never allowed): desired stepped 1→3→4 tracking measured 62–77 %
   instance CPU, fleet delivered **~20k req/s at p50 47–48 ms with zero
   errors** (2 gens × ~10k), and at 2× overload (2,048×2 offered) held
   ~15k with clean shedding. Scale-in followed load release.
5. **The rig's ceiling is the rig**: at ~4,096 offered workers fleet-wide,
   s3lite (single global-mutex map, one process) melted — server acks
   14–17 s. Real object stores partition by prefix; the emulator doesn't.
   Also the single LB process starves its own /stats endpoint at ~4k
   connections — observability must not share the proxy's fate
   (production: separate metrics listener/port).
6. Incidental: `docker kill` doesn't trigger `--restart on-failure`
   (matches the platform's no-restart behavior); operator `docker start`
   rejoins the fleet cleanly. cgroup-OOM exits DO trigger restart
   policies — a restart policy is exactly what we're asking the platform
   for.

## Where the per-instance numbers now stand (1 CPU / 1 GB, 16 shards, 25 ms store)

| path | ceiling | limiter |
|---|---|---|
| direct, batch=1 | ~10.5–11.3k req/s, p50 63 ms | CPU (~100 % at 10.3k with 682 in-flight) |
| direct, batch=16 × 1 KB | ~11 MB/s goodput under RSS-shed | memory budget (by design now, not death) |
| through pilot LB, fleet of 4 | ~20k req/s, p50 48 ms | shared host + emulator, not the engine |
| through platform edge (cloud) | ~400–450 req/s | edge admission budget (~48 slots) |

---

# O14a hardening round (2026-07-15, afternoon): A/B/C on Compute

Target: the durable-ack degradation under multi-shard churn (600–900 ms
server acks on affected instances), now unmasked by the platform edge fix.
Changes tested (v16): WAL GC tightened (min_age 300→60 s, sweep 60→30 s —
5× fewer retained WAL objects per shard), per-shard flush-tick stagger
(base..1.5×base by prefix hash), plus — after arm B — the docker-proven
admission guards (ADMIT_MAX_INFLIGHT=256, ADMIT_RSS_SHED_MB=800) which the
cloud fleet had never received.

All arms: 4 gens × 4 LBs × 4 servers, fixed conc 128/gen, batch=16,
fresh namespaces.

| arm | build | outcome |
|---|---|---|
| A | v15, no guards | classic O14a: fleet oscillated 68 → 2,274 req/s (p10/p90), 4 full-stall windows, client p50 median 121 ms but **p90 793 ms**; 2 of 4 gen instances zombied during the run |
| B | v16, no guards | **2,104 req/s at p50 107 ms for one minute — then all four instances died** (post-edge-fix load kills unguarded 1 GB instances in ~2 min; RSS shed was docker-only until now) |
| C | v16 + guards | **zero deaths, zero stall windows** across the full run at 2× arm A's offered load; fleet 1,257 req/s avg, p10 704 / p90 1,715 (spread 2.4× vs A's 33×); client p50 123 ms, **p90 146 ms (5.4× better than A)** |

**What improved (measurable, client-facing):** survival (4/4 vs mass
death), stall elimination (0 windows vs 4 + a total collapse), client
tail latency 793→146 ms p90, throughput stability 33×→2.4× p10:p90
spread — the oscillation as experienced by clients is gone.

**What did NOT improve (honest):** the underlying ack excursions persist
— 30 of 50 arm-C samples still showed ≥1 instance with ack p50 > 600 ms
(fleet ack median 73 ms, p90 863 ms). The WAL-retention and flush-stagger
hypotheses are insufficient; the system now degrades *gracefully* around
the excursions (shed + fleet routing) instead of stalling or dying, but
O14a's root cause is still open. Next diagnostic: per-PUT latency
instrumentation on the object-store client to split our-pipeline delay
from Tigris tail latency (if excursions track raw PUT p99, the fix is
hedged/parallel WAL PUTs or provider escalation, not our scheduling).

**Also learned:** MODE=gen needs the same 404/unavailability backoff the
429 path got — boot-window hammering inflates error counters and burns
gen egress slots on dead targets.

## Run 12 — O14a split: per-PUT store instrumentation (v17, pilot12)

**Question.** The C-arm left ack excursions (>600 ms server-side p50) in
30/50 samples with the edge exonerated. Three suspects remain: (a) raw
Tigris PUT tail, (b) the platform's per-instance *egress* budget (~50
concurrent outbound — 16 shards × WAL/L0/GC/absorber/manifest can want
more), (c) our commit pipeline (flusher scheduling / watermark).

**Instrument (v17).** `TimingStore` wraps every object store beneath the
prefix layer: per (op, path-class) latency cells over a trailing window
(put/mpu/get/head/delete/list/copy × wal/manifest/sst/fleet/other), a
slow-op ring (≥300 ms, with paths), and an instance-wide outbound
in-flight gauge (now + peak) — all ops, all stores, one gauge, because
the egress budget is per instance. Surfaced at `/v1/debug/store`
(?window=, ?swap=1) and in heartbeats (`wal_put_p50/p99_ms`,
`out_inflight`, `out_inflight_peak`).

**Design.** Fleet + load identical to arm C (4 servers, guards
256/800 MB, gens fixed conc 128×4, BATCH=16, STREAMS=32) — only the
binary changes (v17 = v16 + instrumentation). Fresh keyspace pilot12.
Sampler: every 20 s × 90, per instance `/v1/debug/store?swap=1` (clean
20 s windows) + LB `/stats`. Verdict per excursion sample:
wal-PUT p99 spikes alone → Tigris; all classes spike / gauge pinned
≥45 → egress; store-side flat → our pipeline.

**Expected decision.** Tigris-tail → hedged/parallel WAL writes or a
provider conversation; egress → platform ask (raise/document budget) +
our own outbound admission; pipeline → back to flusher/watermark work
with the excursion now file-and-line attributable.

### Run 12 results (v17 baseline, 90 ticks × 4 instances = 360 samples)

**The three-way split answered on the first run — and it's door #4.**

| metric | value |
|---|---|
| ack excursions (>600 ms p50) | **109 / 360** instance-samples (30 %) |
| verdict A — WAL-PUT tail alone (Tigris) | **2** |
| verdict B — broad all-class slowdown | **107** |
| verdict C — store quiet, ack slow (our pipeline) | **0** |
| Spearman(ack p50, wal-PUT p99), same 20 s windows | **0.793** |
| wal-PUT p99 across samples | p50 617 ms · p90 1,623 ms · max 3,603 ms |
| outbound in-flight peak | p50 32 · p90 67 · p99 117 · **max 148** |

Readings:
1. **Our commit pipeline is exonerated** (C = 0): whenever acks excurse,
   the object-store client itself is slow. The durable-wait IS the store.
2. **Not a Tigris WAL tail** (A = 2): the slowdown is class-agnostic —
   GETs, LISTs, HEADs, manifest PUTs and SST reads all hit 300–800 ms
   together (normal ≈ 50 ms), on the same instance, at 15–38 % CPU.
3. **No hard ~50 egress cap for server-originated traffic**: the gauge
   freely reaches 148. The platform's "~50 egress" number (their
   load-generator measurement) does not bind us here.
4. **Transport churn hypothesis** (the one the data supports): Tigris
   negotiates **HTTP/1.1** (curl ALPN check) → one connection per
   in-flight op. Our pool prunes idles at 4 s (deliberate: platform kills
   idle flows ~5 s). Multi-shard op cadence is bursty: steady ~30 in
   flight, bursts to ~148. Every burst past the warm set opens dozens of
   fresh TLS connections through the egress NAT on 1 vCPU — mass
   handshakes, everything slows at once. It is the *outbound edition of
   Conduit's per-request-TLS bug* we just helped the platform fix inbound.

**v18 A/B** (running): `STORE_MAX_CONCURRENT=48` — a global semaphore at
the store boundary keeps a small connection set continuously warm and
turns bursts into millisecond queueing instead of handshake storms.
Everything else identical (same fleet, same guards, same gens, same
keyspace continuing).

### Run 12b — v18: STORE_MAX_CONCURRENT=48 (negative result, load-bearing)

Same fleet/load; global semaphore at the store boundary. Gauge capped as
designed (max 73 vs 148) — and excursions did NOT improve: 153/360 (vs
109/360), wal-PUT p99 p50 690 ms, excursions now occurring at outbound
peaks of 19–34. **Kills the handshake-storm/burst theory** (and the h2
theory died earlier: Tigris ALPN-negotiates HTTP/1.1). Whatever slows the
ops doesn't care how many are in flight.

### Run 13 — v19: the discriminator (timer sentinels + steal)

Two 10 ms sentinels per instance (raw OS thread vs tokio task) + /proc/stat
steal, sampled per 25 s window. 300 samples, 83 excursions:

| signal | excursion windows | calm windows |
|---|---|---|
| thread-drift max | **4 ms** | 4 ms (max 13) |
| tokio-drift max | **p50 232 ms · max 974 ms** | p50 227 ms · max 831 ms |
| steal % | 0.05 | 0.07 |

**Verdict: the host and network are innocent; our async runtime is the
bottleneck.** A raw thread keeps 4 ms timing while tokio timers on the
same vCPU fire ~230 ms late, chronically, on every instance. Store-op
"latency" at the client boundary includes completion-processing delay on
a starved event loop — which is why every op class "slows" identically,
why capping concurrency changed nothing, and why CPU averaged only
15–35 % (hundreds-of-ms full-CPU quanta inside 20 s windows). On a
1-vCPU instance, `#[tokio::main]` = ONE worker thread: any inline
blocking work (SST build/compression/checksum quanta inside SlateDB
polls, our encode/crypto) freezes every future — including the
durable-watermark notification that acks commits. **O14a was never
watermark logic and never the network: it is event-loop monopolization.**

### Run 14 — v20: TOKIO_WORKERS=3 (the fix candidate)

One change: build the runtime with `worker_threads = max(2, env)` and
deploy with TOKIO_WORKERS=3 — a blocked worker no longer freezes the
loop; the OS timeslices the other workers through. Success criteria:
tokio-drift collapses toward thread-drift (≤ tens of ms), excursion rate
and wal-PUT p99 fall with it.

### Run 14 results — v20 (TOKIO_WORKERS=3): the fix, measured

70 ticks × 4 instances = 280 samples, identical load/guards/keyspace.
(Sampler lost its last 5 ticks to a laptop power-down; n is ample.)

| metric | v17 baseline | v19 (sentinels) | **v20 (3 workers)** | delta v17→v20 |
|---|---|---|---|---|
| ack excursions (>600 ms windows) | 109/360 = 30.3 % | 83/300 = 27.7 % | **28/280 = 10.0 %** | **3.0×** |
| wal-PUT p99, median window | 617 ms | 458 ms | **141 ms** | **4.4×** |
| wal-PUT p99, p90 window | 1,623 ms | 1,702 ms | **679 ms** | 2.4× |
| wal-PUT p99, worst window | 3,603 ms | 2,503 ms | **1,528 ms** | 2.4× |
| tokio-drift, typical | — | ~230 ms chronic | **tens of ms typical** | ~10× |
| thread-drift (control) | — | 4 ms | 4–9 ms | unchanged ✓ |

The event-loop-starvation mechanism is confirmed end to end: adding two
worker threads changed nothing about the store, the network, or the
engine — and excursions fell 3× while the WAL-PUT tail (as seen by a
client of the store) fell 4.4×.

**Honest residuals (the remaining 10 %):**
1. **True CPU saturation windows**: e.g. t45 streams-2 at cpu 91 %,
   tokio-drift 1.5 s — compaction storms genuinely exhaust the 1-vCPU
   box; more workers cannot help. Remedy: move SST build/compress fully
   off-runtime (spawn_blocking hygiene, possibly upstream in SlateDB)
   and/or right-size instances.
2. **A real Tigris-tail class finally visible**: 3 samples show clean
   400–630 ms WAL PUTs at 11–32 ms drift and idle CPU — genuine provider
   tail, now measurable in isolation. Small; hedged WAL writes remain a
   candidate but are second-order.
3. Drift spikes still occur (p90 of excursion windows 1.4 s) — 3 workers
   usually absorb the blocking quanta; they don't remove them.

**O14a disposition**: root cause identified (async-runtime
monopolization on 1-worker runtimes; blocking quanta from SST
build/compaction inline with commit-ack notification), fix shipped
(worker floor ≥2, default now in main.rs) and validated at 3×/4.4×.
Follow-up engine work: blocking-quanta hygiene; provider-tail hedging
if the A-class grows.

## Run 15 — the slate-codex on-platform campaign (2026-07-19/20)

Goal: deploy slate-codex to the regular 4-instance setup and compare
against the v20 baseline. Outcome: **no valid benchmark window — but the
campaign root-caused one production-blocking codex bug, produced nine
committed integration fixes, and three platform findings.**

### The blocking bug: history-absorption debt war

On any keyspace that has experienced shard ownership movement, codex's
absorption-debt recovery reconstructs overlapping work-sets on multiple
instances. Each instance's absorber opens the same per-stream history
DBs; SlateDB fencing makes every opener kill the previous one
("detected newer DB client"); all sides retry forever.

Evidence (c-fleet, pilot17, 2026-07-20):
- ~10 absorb-fence failures/second sustained; **223 in a 15 s window on
  a freshly restarted, stable, quiet fleet** — reconstruction itself
  double-claims; not a churn artifact, does not self-heal.
- 20–60 % CPU on "idle" instances (the war), rising to caps under load.
- Creates and appends hang until the platform front door's 30 s kill
  (502) even with 9 ms store ops; only registry-conflict 409s complete.
- Held requests accumulate to ADMIT_MAX_INFLIGHT; the instance then
  429s everything ("connections, observed 257/256") — total cell
  unavailability. Every zero-goodput load test of the campaign (both
  regions, old and fresh services, platform and laptop generators)
  reduces to this mechanism.

Fix direction: absorption work must have a single claimant — strictly
follow current ring ownership; a fenced absorber must DROP its claim
(handing off via durable marker) instead of retrying; debt-marker
reconstruction must be CAS-claimed per epoch so exactly one
reconstructor wins. The gate's "idle debt survives process movement"
test covers single-mover choreography, not N-way post-churn
reconstruction — add a churn matrix.

### Also found and fixed on-platform (committed, 9 fixes)

download-stall ranged S3-direct binary fetch; METRICS_CUSTOMER_ID env;
storage-format keyspace gate (fresh keyspace path); aggregator lease
6 s→20 s; parallel heartbeat fan-in; snapshot-generation-time heartbeat
aging (server); the same two consumer fixes in the router; router
liveness window 10 s→20 s. Plus one config finding: per-customer
admission defaults (inflight 64) bind instantly under single-principal
benchmarks — ADMIT_MAX_INFLIGHT_PER_CUSTOMER=0 is the documented
benchmark escape.

### Platform findings (handed to the Compute team)

1. Bun 1.4-canary runtime rollout broke SigV4 presigned fetches from
   instances (403; laptop-verified same URL 206).
2. Service wedging: existing services stopped waking/deploying; fresh
   services worked; later platform fix verified (all recovered).
3. Instance-egress latency oscillation: store ops from instances flapped
   6 ms → 241 ms → 9 ms over hours (minute-by-minute log captured)
   while laptop→Tigris stayed ~50 ms; long-lived flows silently killed;
   front door kills long-polls at 30 s with 502.

### Verdict revision

The earlier "merge with the fence fix" recommendation is WITHDRAWN.
slate-codex must not merge until the absorption-debt war is fixed: it
is a data-plane-killing regression that only manifests after ownership
churn — precisely the condition production creates and local CI does
not. The branch's hardening remains valuable and the bug is precisely
localized with a minimal reproduction.
