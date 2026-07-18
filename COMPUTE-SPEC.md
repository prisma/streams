# Streams on Prisma Compute — Routing, Autoscaling & Storage-Tiering Spec

Target platform: Prisma Compute instances with **1 CPU core / 1 GB RAM** each.
Object storage: **Tigris** (Standard tier for WAL/manifests/hot SSTs; Archive
Instant Retrieval for settled history — §7).

This spec covers what the platform's router and autoscaler must provide, what
the Streams service provides in return, and the coordination protocol between
instances. It assumes the architecture agreed so far: consistent-hash shards,
each shard = one SlateDB (the *shard log*, ingest + tails), absorber →
per-stream WAL-less SlateDBs under shared-bucket prefixes (history tier,
block-encrypted per stream key, block-zstd compressed).

---

## 0. Design principles

1. **The router and autoscaler are never correctness mechanisms.** SlateDB
   manifest CAS fencing is the sole arbiter of shard ownership. A stale
   router, a slow autoscaler, or a split-brain moment costs latency, never
   data. Every mechanism below is allowed to be eventually consistent.
2. **All instances are symmetric.** No leaders, no controller process. Any
   coordination state lives in the ops bucket as CAS-updated objects.
3. **The service computes its own capacity.** The platform autoscaler does
   not interpret Streams metrics; it obeys a single number the fleet
   publishes (§4, "inverted autoscaling").
4. **Fan-out belongs to the CDN**, not to instances (§3.1).

---

## 1. Instance model and budgets

One instance = gateway + shard owner + absorber, all in one process.
Compaction and GC of *shard logs* run in a separate service (§6).

### 1.1 Memory budget (1 GB)

| component | budget | enforcement |
|---|---:|---|
| shared SlateDB block cache (all DBs) | 384 MB | `with_db_cache`, one cache |
| shard-log memtables + WAL buffers | 192 MB | ~24 shards × `l0_sst_size_bytes=4MB`, `max_unflushed_bytes=8MB`/shard |
| per-stream DB absorber transients | 96 MB | LRU of open history DBs (≤ 16), bulk write then close |
| connection state (SSE/long-poll) | 96 MB | ≤ 10,000 conns × ~8 KB; hard cap, then 503 reads |
| HTTP/TLS/runtime/misc | 128 MB | — |
| headroom (spikes, fragmentation) | 128 MB | RSS alarm at 800 MB |

### 1.2 CPU budget (1 core)

Request path work per record: AES-GCM (≥1 GB/s/core with AES-NI) + zstd
level 3 in the absorber (~200 MB/s/core) — negligible at target volumes.
The two CPU hogs are kept off the request path:

- **Shard-log compaction/GC** → detached compactor service (§6). Shard-log
  values are record-encrypted, so compaction merges them *without any tenant
  key* — it never decrypts values.
- **History-tier compaction** requires the stream key (block transformer), so
  it can only run where keys are in hand: on the shard owner, opportunistically,
  piggybacked on absorber batches, rate-limited to ≤ 15% of core. Pressure is
  inherently low because the absorber writes large, pre-sorted, pre-compressed
  L0s. (This key-gated compaction schedule is also what makes archive tiering
  safe — §7.)

### 1.3 Shard constants

- **Dynamic shard topology** (SPEC.md D3/§3.2): shards are bit-prefixes of the
  stream hash in a CAS'd `topology.json`; the service starts at one shard and
  splits hot shards (doubling that shard) via clone-with-projection, merges
  cold siblings via manifest-union (§5.4). The ring and router consume
  `topology.json` alongside the heartbeat set.
- Shard-log settings: `flush_interval = max(25 ms, backend PUT p90)`
  (amended twice — pilot run 3: 5 ms mints WAL SSTs ~7× faster than WAL GC
  reaps them, degrading the durable watermark to 0.3–1 s; bench round 2:
  the WAL flusher PUTs serially, so the interval must not undercut the
  backend's PUT latency — Tigris ≈ 50 ms, local emulators 25 ms.
  EXPERIMENT-PILOT.md), `l0_sst_size_bytes=4MB`,
  `max_wal_flushes_before_l0_flush=64` (bounds WAL replay so a shard move
  stays < ~1 s), compression off in the shard log (values are ciphertext).
- **Ring wake semantics (run 5)**: the live-set ring's dark-instance
  eviction (>30 s) assumes the PLATFORM starts a newly-desired instance;
  routing must not be the wake mechanism, because the filtered ring never
  routes to a dark instance (deadlock observed: desired=4/live=1). Any
  environment where first-request is the wake path needs an out-of-band
  wake probe to desired-but-dark ordinals (pilot LB does this).
- **Flusher-gate settings (bench round 2 — these two defaults were the
  entire "byte ceiling")**: `l0_max_ssts_per_key` must be raised with
  `l0_max_ssts` — an ordered stream rewrites its tail row in every
  memtable, so per-key L0 overlap equals L0 count and the per-key default
  (8) silently becomes the flush gate. `manifest_poll_interval ≤ 2 s` on
  loaded shards: the flusher learns compaction freed L0 slots only via
  manifest poll, and a long poll turns every compaction into a
  poll-length write stall (60 s poll → 14 s stalls → backpressure 408s).
  Idle shards may poll lazily; the setting follows load, not a global.
- **Absorber throughput (amends §1.2)**: absorption must track ingest in
  steady state or the hot tier grows without bound. The absorber reads
  the hot log as disjoint offset windows with bounded concurrency
  (4-way; serial 8 MB chunks measured ~10k rec/s vs 150k rec/s ingest),
  caps per-pass buffering (`ABSORB_PASS_BYTES`, default 256 MB — a pass
  is held in RAM), and trims up to `TRIM_PER_OP` (default 8k, throughput
  shards ≥ 256k) hot records per Absorbed op so trim also tracks ingest.
  The ≤ 15 % core budget still binds; on 1-core instances the budget, not
  the pipeline, is the sustained ceiling.

---

## 2. Coordination substrate (ops bucket)

All objects JSON, all writes conditional (`If-Match` / `If-None-Match: *`).

| object | writer | cadence | content |
|---|---|---|---|
| `fleet/<instance-id>.json` | that instance | every 2 s | heartbeat: generation, `draining` flag, load vector (§4.2), owned shards, build version |
| `fleet/desired.json` | any instance, CAS | on change | `{count, reason, epoch, computed_at}` |
| `overrides.json` | any instance, CAS | rare | shard pins, quarantines, split/merge intents |
| `streams/<name>.json` | provisioning path, CAS | rare | stream registry (bucket prefix, config, lifecycle) |

**Liveness:** an instance is *live* if its heartbeat is < 10 s old (5 missed
beats). Heartbeats double as the metrics feed — no separate telemetry channel
is needed for scaling decisions.

**The ring is derived, not stored.** Shard → instance assignment is
**weighted rendezvous hashing** over (live, non-draining instance set +
overrides), computed identically by every instance and by the router. There
is no assignment document to contend on; the live set *is* the assignment.
Ring epoch = hash of (sorted live instance ids + overrides etag), included in
heartbeats so convergence is observable.

**Ops-bucket outage degrades, never stops:** instances freeze their last ring,
keep serving owned shards, stop volunteering for new ones. Fencing still
protects everything.

---

## 3. Request routing

### 3.1 Layer 0: CDN (read fan-out)

- Catch-up reads are canonical-chunked (fixed boundaries), `ETag` +
  `Cache-Control: immutable` → cache hits, zero origin load.
- Durable live tails: collapsed long-polls (request coalescing) — one origin
  long-poll per stream per POP; the CDN fans out. 50k subscribers ≈ #POPs
  origin requests.
- **Never cached:** speculative tails (`no-store`), appends, admin.
- **Edge authorization, cache keys, metering:** capability-URL contract in
  OPERATIONS.md §7 (ciphertext-only at the edge; per-cohort HMAC URLs so
  per-tenant credentials never break coalescing; CDN-log-reconciled
  billing; V5 GA gate covers it; plan-B mux tier at parity).

### 3.2 Layer 1: platform router — requirements

- **R1 Route key:** extract stream name from `/v1/stream/{name}[/...]`;
  map stream → shard by longest-prefix match of the stream hash against
  `topology.json` (§1.3); route by **shard**.
  Non-stream routes (`/v1/streams`, `/health`) → any ready instance.
- **R2 Placement function:** the router runs the same weighted rendezvous
  ring over the fleet heartbeat set (poll ops bucket or a
  `/v1/admin/ring` endpoint on any instance, every ~2 s).
- **R3 Replay primitive (the important one).** Router correctness must not
  depend on R2 freshness. Any instance receiving a shard it doesn't own
  responds `409` with `Streams-Replay-To: <instance-id>`; the router MUST
  replay the (buffered) request to that instance without involving the
  client, and SHOULD cache the learned `shard → instance` hint until the
  next ring epoch change. This is the Fly-Replay pattern: the router needs
  no authoritative map at all — it has a guess (R2), a correction channel
  (R3), and a safety net (fencing).
- **R4 Long-lived responses:** SSE and long-polls held ≥ 30 s, streamed
  unbuffered, no idle timeout below 60 s.
- **R5 Drain:** on `draining` heartbeat (or platform-initiated stop), stop
  new routes to that instance; existing SSE connections are ended by the
  *instance* (§5.2), not severed by the router.
- **R6 Slow start:** ramp a joining instance's rendezvous weight 0 → 1 over
  60 s so its cold cache isn't hit with full load.
- **R7 Health model:** `/healthz` = process up (restart if failing);
  `/readyz` = participating in ring (route only if ready).

**Append body limit at router:** 32 MB, matching the engine.

---

## 4. Autoscaling — inverted control

**Amendment (runs 6–8, 2026-07-14/15): the load vector is multi-signal,
and every signal earns its place by a measured failure.**

| dimension | source | why it exists (measured) |
|---|---|---|
| utilization: `ceil(Σ cpu_cores_used / 0.75)` | heartbeat `cpu_pct` (getrusage) | assumed-capacity constants rot: run 5 scaled out at ~5 % actual utilization because `SCALE_RPS_CAPACITY=150` described a 10×-slower engine build |
| hot instance: any loaded instance ≥ 75 % CPU sustained 20 s ⇒ +1 | heartbeat `cpu_pct` | shard skew loads one instance while the fleet average stays low |
| edge admission slots: `ceil(Σ inflight / (0.75 × 140))` | heartbeat `inflight` (in-flight gauge); capacity = front-door ingress budget, measured ~145–150 concurrent/instance (6-source probe + platform team investigation). NOTE: each SOURCE instance is separately egress-capped at ~48–50 outgoing — router-tier sizing rule: routers ≥ ceil(target_concurrency / 48) | run 6: the edge saturated at 16 % CPU; the edge probes (2026-07-15) identified the two-layer budget — source egress ~48, destination ingress ~145 (the earlier 48 calibration was the measuring instance's own egress; platform-team investigation + Part 3 of PLATFORM-EDGE-REPORT) |
| server ack latency: sustained ack-p50 breach ⇒ +1 | heartbeat `ack_p50_ms` | object-store slowness that shows in neither CPU nor rps |
| edge latency: router-observed client p50 breach ⇒ +1, and **blocks scale-in** | router report `routers/<name>.json` (client_p50_ms) | run 7: clients at p50 1.6–2 s while server acks sat at 60–80 ms — and the fleet SHRANK mid-congestion because measured rps falls when clients queue. Server-side signals cannot see edge queueing; the router must contribute what clients experience |

Scale-out publishes immediately; scale-in uses a conservative divisor
(50 %), a 60 s sustain, and is blocked outright while the edge dimension
is hot. All thresholds are env knobs (`SCALE_OUT_CPU_PCT`,
`SCALE_IN_CPU_PCT`, `SCALE_CPU_SUSTAIN_SECS`, `SCALE_EDGE_LATENCY_MS`,
`SCALE_RPS_CAPACITY` = envelope, 0 = off).


### 4.1 Mechanism

The platform autoscaler implements exactly one behavior: **converge the fleet
to `fleet/desired.json`**, with guardrails `min=3`, `max=64` per cell
(§10), scale-out ≤ +4 per 60 s, scale-in ≤ −1 per 300 s, and never below
the count at which any surviving instance's drain preconditions (§5.2)
would be violated.

Every instance recomputes the desired count each heartbeat from the *fleet's*
heartbeats (same inputs → same output, so CAS conflicts are no-ops):

```
need(dim)  = ceil( fleet_usage(dim) / (target_util(dim) × per_instance_capacity(dim)) )
desired    = clamp( max over dims( need(dim) ), min, max )
```

Scale-in requires **all** dims < 40% of target for 10 consecutive minutes
(hysteresis). Whichever instance notices a change first CASes `desired.json`;
losers re-read and agree.

### 4.2 Scaling dimensions (the load vector in every heartbeat)

| dim | per-instance capacity target | notes |
|---|---|---|
| CPU utilization | 70% | platform-visible too, sanity check |
| RSS | 800 MB | hard ceiling 1 GB |
| open tail connections | 8,000 | fd + memory bound |
| append requests/s | 6,000 | from bench: comfortable per-core rate |
| ingest queue depth / 429 rate | queue < 50% capacity, 429s ≈ 0 | leading indicator |
| memtable/unflushed bytes | < 60% of per-shard caps | write-side backpressure signal |
| absorber lag (shard-log bytes not yet absorbed) | < 256 MB/instance | rises when keys are absent or CPU starved |
| block-cache hit rate + read p99 | hit > 85%, p99 < 100 ms | cache pressure → more instances = more aggregate cache |
| WAL replay estimate per shard | < 1 s | affects failover/drain speed; tightens flush caps rather than scaling, alarm only |

Not a scaling dim but an **override trigger**: a single shard near its write
ceiling → shard split (§5.4), because more instances won't help a hot shard.

Compaction debt of shard logs scales the **compactor service** (§6), not this
fleet.

---

## 5. Lifecycles

### 5.1 Join
Boot → read registry/ring → write first heartbeat (weight-ramped) → acquire
shards the ring assigns: open shard log (fences previous owner), replay WAL
(< 1 s by construction), set `/readyz`. Stagger shard opens (4 concurrent) to
smooth object-store load.

### 5.2 Drain (scale-in, deploys)
1. Platform sends SIGTERM (or instance sees itself absent from desired set).
2. Instance sets `draining: true` in heartbeat; ring recomputes everywhere;
   router stops new routes (R5).
3. Shards hand off one at a time: stop accepting appends on the shard (409 +
   replay header → new owner), let the new owner fence, confirm, next shard.
4. SSE/long-poll connections receive a final control event
   (`event: reconnect`) and are closed with jittered pacing (≤ 500/s) to
   avoid a reconnect stampede.
5. Exit. Total budget ≤ 30 s for 24 shards.

**Precondition:** never drain if it would push any surviving dim > 90%.

### 5.3 Crash
Heartbeat TTL expires (≤ 10 s) → ring recomputes → new owners fence and
replay (< 1 s/shard, staggered) → clients reconnect through router.
Unacked appends surface as errors/timeouts → client retry (existing
ambiguity contract). Target: p99 < 15 s from crash to full shard availability.

### 5.4 Shard split/merge (override-gated, self-triggered)
Trigger: shard sustained > 60% of single-shard write ceiling (split), or two
cold siblings (merge). The shard's owner executes: CAS a split intent →
return retryable 503 for that shard and close local admission → drain an
ordered barrier through remote durability → flush → clone the shard log into two children with hash-range
projections (bit-prefix `p` → `p0`, `p1`; hash-first keys make each child a
single `projection_range`) → CAS `topology.json` → resume. Children are
placed by the ring (typically one stays local, one hands off via normal
fencing); the retired parent's SSTs stay referenced by the children until
compaction ages them out. Before releasing a parent ACK, every owner checks
the shard-store intent after remote durability; this is the safety fence for
a stale ring owner. Intents use renewable 12 s leases and new clone paths on
cross-process takeover. The intent atomically records each superseded,
never-published operation and materializes a durable GC-candidate marker.
Candidates are retained for 24 hours; the five-minute GC re-reads topology
plus every active intent immediately before deletion and fails closed above
100,000 listed objects per run. Published ancestor paths are never inferred
disposable because descendant clone manifests may still reference their SSTs.

Merge is the reverse and is available through the operator gate: create one
parent-scoped renewable intent, CAS-occupy both children's per-shard intent
paths, drain/flush each child, create a non-overlapping SlateDB manifest-union
clone, verify it reopens, and publish `p0,p1 → p` in one topology CAS. The
per-child objects are also the post-durability ACK fences, so a stale owner
can contribute only ambiguous/unacknowledged data after the snapshot. The
barrier writes a reserved 17-byte tombstone outside the service key grammar
before its ordered WAL→L0 flush; this advances replay state even when a
projected child contains only out-of-range inherited WAL rows. Split and merge
locks become CAS-written released tombstones instead of being deleted, which
prevents a delayed claimant from deleting a later operation on a reused
prefix. Merge takeover/abandoned-generation GC follows the split protocol.
The sustained-cold automatic merge trigger remains to be implemented.

---

## 6. Compactor service (separate Prisma Compute app)

- Scales independently on: shard-log compaction debt (L0/sorted-run counts,
  read from manifests), GC backlog, absorber lag alarms.
- Work claims: `compact-claims/<shard>.json` CAS objects with TTL — leaderless
  work distribution, same pattern as everything else.
- Runs SlateDB's detached compactor against shard logs only (no tenant keys
  needed — values stay ciphertext). History-tier DBs are never touched here
  (key-gated, §1.2).
- Same 1 CPU/1 GB instances; a compactor instance saturates its core, which
  is fine — no latency SLO applies to it.

---

## 7. Storage tiering (Tigris Archive Instant Retrieval)

Prices: Standard $0.02/GB/mo; Archive-IR $0.004/GB/mo + $0.03/GB retrieval,
90-day minimum billing, same request prices, instant GETs.

**Policy — history tier only, by quiet horizon:**

- Always Standard: shard logs (WAL + SSTs; short-lived, hot), all manifests,
  history-tier L0s and any SST younger than the **quiet horizon Q = 30 d**.
- Archive-IR: history-tier SSTs untouched for Q. Because record keys are
  offset-ascending, new data never overlaps old runs, and history compaction
  is deliberately front-loaded (absorber writes big sorted L0s, key-gated
  compaction settles them early) — an SST quiet for 30 d has effectively
  left the compaction lifecycle.
- Mechanism: per-object storage class on PUT if Tigris supports it for
  server-side copy (verify; then the "mover" is a same-key copy request),
  else a paired archive prefix + read-through in the ObjectStore wrapper
  (try standard, fall back to archive; the wrapper also serves as the
  migration actor). Either way this lives in a ~100-line ObjectStore
  decorator; SlateDB never knows.

**Economics guardrails:**
- Retrieval fee $0.03/GB = 1.5 months of Standard storage — an archived SST
  read more than ~once per 6 weeks (after cache/CDN) should not be archived;
  track per-SST cold-read counters in the wrapper and exempt hot ones.
- A rewrite of an archived object wastes ≤ 90 d × $0.004 = $0.36/GB phantom
  charge + $0.03/GB retrieval — tolerable if rare; alarm if > 1% of archived
  bytes are rewritten per month (means Q is too short or compaction isn't
  settling).
- Expected steady state for append-only streams: all bytes older than
  ~Q+60 d at 5× lower storage cost, i.e. blended storage → ~$0.005/GB/mo
  asymptotically, with reads of old data absorbed by CDN/disk cache first.

---

## 8. Failure matrix

| failure | behavior |
|---|---|
| router has stale ring | 409 + replay header corrects per request; no client impact |
| two owners briefly (split-brain) | loser fenced on WAL/manifest CAS; during split, the post-durability shard-intent check withholds stale parent ACKs → retry |
| ops bucket unavailable | freeze ring, keep serving, no scaling; alarm |
| heartbeat write storms / CAS conflicts | all state converges from derived functions; conflicts are benign re-reads |
| reconnect stampede after crash | client jitter (SDK) + router slow-start + CDN absorbs durable tails |
| autoscaler runaway | guardrails in §4.1; desired.json is auditable (epoch + reason) |
| Tigris regional degradation | watermark stalls → 429s (correct); tails stall; no corruption |

## 9. Observability

Heartbeats are the metrics bus (scrapeable via `/v1/admin/fleet` on any
instance). Additionally: per-shard durable-watermark lag, flush PUT latency
histograms, replay-header rate (router staleness), fence events (should be
≈ shard moves; excess = flapping), archive rewrite rate (§7), CAS conflict
rate. Control-plane actions (ring epochs, overrides, splits, desired-count
changes) are appended to an internal audit stream — dogfooding the product.

---

## 10. Cells — the unit of scale, failure, and deployment

Everything in §§2–5 describes ONE CELL. A cell is a self-contained copy of
the service: 16–64 instances, its own ops **prefix** (heartbeats,
`desired.json`, `topology.json`, `overrides.json`), its own compactor pool,
its own audit/metrics streams. Nothing inside a cell reads another cell's
coordination state, ever.

### 10.1 Placement and the global layer

- **Stream → cell is pinned at create** and recorded in the registry
  descriptor (`cell: c-041`). Default placement: rendezvous over cells
  weighted by published cell headroom. A customer may be soft-affinitized
  to ≤ 4 cells so one tenant's blast radius and one cell's tenant count
  stay bounded.
- The **global control plane is one small object**: `cells.json` — the
  cell directory `{cell_id, region, ops_prefix, weight, state:
  active|draining|frozen}` — O(#cells) ≈ tens of KB at 600 cells, CAS'd
  only on cell add/drain (operator-rate, not request-rate), polled by
  routers every 60 s. There is no global topology, no global ring, no
  global heartbeat set.
- Routers resolve stream → cell from the registry descriptor (cached with
  the descriptor); in-cell routing is R1/R2 unchanged. Cross-cell stream
  moves exist only as an operator migration (copy-then-cutover via
  fencing) — never automatic. Before the fleet reaches tens of cells,
  the migration tooling and rebalancing policy ship: a cell saturating at
  max=64 instances stops taking new-stream placement (weight→0 in
  cells.json) and its hottest movable tenants are queued for migration;
  the tooling is the same copy-then-cutover path, driven by a runbook.

### 10.1a Isolation is ENFORCED, not conventional

Cell boundaries are credential boundaries, or they are nothing:

- **Per-cell service principals.** Every instance boots with a
  cell-scoped credential (platform-issued workload identity). IAM grants
  that principal read/write ONLY under `cells/<cell-id>/…` for ops state
  and that cell's data prefixes. Writing another cell's `topology.json`
  is denied by policy, not by convention — a prefix-computation bug or a
  leaked wave-1 canary credential is contained to its cell.
- **Role split within a cell:** serving role (data + ops RW), compactor
  role (shard-log data RW, no registry writes), copy-actor role
  (primary read-only + backup-bucket write-only), GC role (the only
  deleter, soft-delete window per OPERATIONS §2.4), scrubber role
  (read-only). Registry writes (stream create/delete) are a separate
  provisioning principal.
- **Break-glass:** cross-cell/administrative access exists only via a
  two-person, time-boxed elevation with audit (OPERATIONS §3.4).
- **Bucket regionality:** buckets are per-region; a cell lives entirely
  in its region's buckets (ops, data, and its backup target in the
  OTHER provider/region). cells.json states the mapping. "Cross-region
  cells unaffected" in §10.3 refers to regional events; the
  provider-level failure domain is handled in OPERATIONS §1.

### 10.2 Control-plane arithmetic (the §1.1 discipline, applied to the fleet)

Per cell of N ≤ 64 instances, R ≤ 32 router replicas assigned to it:

| flow | rate | notes |
|---|---:|---|
| heartbeat PUTs | N/2 s = ≤ 32/s | one writer per object, no CAS |
| heartbeat fan-in | aggregator: N GETs/2 s; readers: 1 aggregator PUT/2 s + (N+R) GETs/2 s of `fleet.json` ≈ ≤ 80/s total | see below |
| desired.json | ≤ 1 CAS/2 s + piggybacked reads | converges, conflicts benign |
| topology.json | read piggybacked on fleet.json etag; CAS only on split/merge | ≤ 1,536 shards/cell ≈ 30 KB |
| ring computation | rendezvous O(N) per shard, cached per ring epoch | 1,536 × 64 evals ≈ 10⁵ hashes, sub-ms |

**Shards-per-instance ceiling: 32** (memtable/WAL budget, §1.1). The
desired-count formula includes it as a floor —
`desired ≥ ceil(shard_count / 32)` — so scale-in can never assign an
instance more shards than its memory budget holds, and sustained
`shard_count > 32 × max` is the merge-pressure signal (§5.4) rather than
a silent budget breach.

**Heartbeat aggregation:** instances still write individual heartbeats,
but readers consume `fleet.json`, written every 2 s by an aggregator
(lowest live instance id CAS-claims an aggregator lease object; failover
by lease TTL). Read amplification therefore does not scale with N²: the
pilot's every-reader-reads-every-heartbeat pattern is only acceptable at
N ≤ 8 and is replaced above that.

Fleet-level totals (cells are independent; per-prefix load is constant):

| fleet size | cells (N=64) | ops-bucket load per cell prefix | global objects |
|---|---|---|---|
| 10² instances | 2–7 | ~112 req/s | cells.json, ~KB |
| 10³ instances | 16–63 | ~112 req/s | cells.json, ~10 KB |
| 10⁴ instances | 157–625 | ~112 req/s | cells.json, ~60 KB |

~112 req/s per prefix sits far under S3-class per-prefix baselines
(≥3,500 PUT/s + 5,500 GET/s with auto-partitioning); the design leaves
> 30× headroom before any prefix-sharding of coordination state is needed.
Data-plane prefixes shard naturally (per-shard, per-stream paths).

### 10.3 Blast-radius statements

| failure | radius |
|---|---|
| bad binary (caught by canary) | 1 instance, then ≤ 1 cell (§11 waves) |
| poisoned cell coordination object (topology/desired) | that cell only: ring freezes (§2 outage rule), served from last ring |
| poisoned `cells.json` | routers keep 60 s cache + last-known-good on parse failure; no new cell placement until fixed |
| object-store regional event | all cells in region degrade to 429-on-write (correct-by-construction), reads serve from CDN/cache; cross-region cells unaffected |
| one tenant's traffic/requests | bounded by §12 quotas to its streams' shards; ≤ its ≤ 4 cells |

---

## 11. Deployment

The deploy unit is the cell. All coordination objects are versioned and
all changes ship read-compatible first.

- **Waves:** one-box (1 instance, 1 cell, 2 h bake) → 1 cell (24 h) →
  10 % of cells → 50 % → 100 %, ordered by cell criticality (internal
  dogfood cells first, largest customers last). Wave promotion is
  automatic on green metrics; any regression halts the pipeline
  fleet-wide.
- **Canary gates (evaluated per wave, against the prior wave's baseline):**
  append error rate, ack p50/p99, fence-event rate, replay-header rate,
  CAS-conflict rate, absorber lag, RSS. Breach ⇒ automatic halt +
  rollback of that wave (previous binary redeploy; instances drain per
  §5.2, shards fence back).
- **Schema evolution (N/N-1 contract):** every coordination object
  (heartbeat, fleet.json, desired, topology, overrides, cells.json)
  carries `v`. Readers MUST ignore unknown fields and accept `v` and
  `v-1`; writers write `v-1`-compatible payloads until a release fully
  bakes, then flip — and the FLIP LAG RULE makes rollback safe: no writer
  flips to `v` until the release that READS `v` has fully baked
  fleet-wide AND the previous release also reads `v` (i.e., read support
  ships one release before write support), so rolling back one release
  never strands unreadable coordination state. Emergency un-flip is a
  documented operator action (rewrite the object at `v-1` via
  break-glass). The ring function itself is versioned: heartbeats
  carry `ring_fn`; a mixed cell computes with `min(ring_fn)` so both
  versions agree on placement during the wave.
- **Version-skew hazards learned in the pilot, now contract:** replicas
  of stopped versions can keep serving pinned keep-alive connections —
  the router MUST drain by connection on version retirement, and clients
  rotate connections (SDK default ≤ 5 min); deploy tooling MUST pass the
  full env-var set (merge semantics are additive and leak across
  deploys).

---

## 12. Tenant isolation, quotas, and admission control

Per-instance overload protection (G9's queue-full 429) is the LAST line.
The first line is per-tenant, enforced before work is queued.

### 12.1 Quota objects and enforcement points

- Limits live in the registry: per-stream defaults on the descriptor,
  per-customer overrides in `customers/<id>/limits.json` (cached 60 s).
  Dimensions: `appends/s`, `append_bytes/s`, `reads/s`, `read_bytes/s`,
  `live_connections`, `streams_count`, `queue receives/s`.
- **Gateway admission (first hop):** token buckets per (stream) and per
  (customer, cell), refilled from the cached limits. Over limit ⇒ 429
  before any queue or storage work. **Distributed semantics:** buckets
  are per-instance with share = limit ÷ |active instances| (from
  fleet.json, refreshed 2 s) — cheap, no coordination, worst-case
  transient overshoot ≤ one refresh interval during scale events. The
  429 body's `limit` reports the customer-level limit; `observed` is the
  cell-wide estimate (local rate × active count).
- **Shard-owner backstop:** the committer's batch assembly is a weighted
  deficit round-robin across streams (weight = stream's limit), so one
  stream can never occupy more than its share of a shared commit group
  even if admission lags. A stream at its ceiling gets per-stream 429,
  not shard-wide 429.
- **`streams_count`** is currently enforced at create by a per-customer CAS
  lease around an authoritative by-customer descriptor recount and the
  descriptor CAS. This is exact and crash-bounded (a canceled holder expires
  within 30 s), but serializes creates for one account. Block-reserved exact
  counters remain a throughput optimization once measured create traffic
  justifies their extra recovery protocol.

### 12.2 The 429 contract

429 responses carry standards-compliant `Retry-After` seconds plus a body
`retry_after_ms` mirror. SDKs add jitter before retrying. The body is
`{"error":{"code":"throttled","scope":"stream|customer|shard|instance",
"dimension":"connections|streams_count|write_burst_bytes|queue_depth|memory_bytes",
"limit":n,"observed":n,"retry_after_ms":n}}`. `limit` and `observed` use
the units named by `dimension`; stream-count `observed` includes the rejected
create. Instance- and shard-scope 429s are alarmable (they mean capacity, not
tenant behavior).

### 12.3 Hot streams, hot shards

Shard owners publish per-stream EWMA share in their heartbeat load
vector. Sustained (60 s):

- stream > 60 % of shard write ceiling AND stream below its own limit ⇒
  **split the shard** (§5.4) — automatic, override-gated.
- stream at its documented per-stream ceiling (C3) ⇒ **throttle at
  admission** (scope=stream); customers needing more use per-key streams
  (PER-KEY-ORDERING.md) which spread segments across shards.
- shard > 60 % aggregate ⇒ split.

### 12.4 Poison shards and crash loops

Every shard open is journaled (`opens/<shard>.json`, CAS counter of
{instance, ts, boot_id}). An instance that crashes within 60 s of opening
a shard increments a suspicion counter on reopen; **3 crash-correlated
opens across ≥ 2 distinct instances within 10 min ⇒ automatic quarantine**
record in `overrides.json`: appends to that shard fail fast
(503 `quarantined`), reads continue from the last checkpoint if the shard
can be opened read-only, and the on-call is paged. Un-quarantine is a
human action. This bounds a request-of-death to one shard instead of a
serial crash-loop across the cell.
