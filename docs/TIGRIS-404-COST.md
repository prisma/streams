# Tigris 404 economics: probe-GETs, LISTs, and the poll stretch

Findings from the 2026-08-01..03 investigation into Tigris miss-path
latency (opened by the SIN soak anomaly), consolidated with the pricing
analysis that answered "what does a 404 actually cost in dollars?", and
the resulting change: the idle poll-cadence stretch (this commit). The
LIST-side economics were fixed earlier and separately by cost campaign 2
(docs/COST-CAMPAIGN-2.md); this doc covers the GET/404 side and the
decision record for why we stretched cadences instead of switching
primitives.

## 1. Where the 404s come from

SlateDB discovers "latest manifest/compactions version" by **probing**:
version ids are dense (CAS create at predecessor+1), so each poll GETs
`id+1` — a deliberate miss when nothing changed — then 304-revalidates
the cached anchor. LIST remains only the cold-start / anchor-deleted /
>8-versions-behind fallback (probe-cached latest reads, upstream main;
COST-CAMPAIGN-2 §2). This is not a defect: it is the request-dollar-
optimal design on the S3 cost model, and §3 shows it stays optimal on
Tigris. Every idle Streams instance therefore emits a steady stream of
intentional 404s: one miss per manifest poll per DB, one per compactions
poll per DB.

At the pre-stretch field posture (`MANIFEST_POLL_MS=1000`,
`COMPACTOR_POLL_MS=500`, 4 shard DBs) that was **~13 miss-GETs/s per
idle instance** (manifest 1/s/DB + compactions 2/s/DB + reader probes)
≈ 1.18 M requests/day — forever, load-independent. (History DbReaders
already poll at 300 s and contribute ~nothing since history v2.)

## 2. Latency: what a 404 costs Tigris (and us)

Measured 2026-08-03 with `Server-Timing` capture (see
docs/PLATFORM-SIN-404-REPORT.md and the localbkt verification):

| operation | wall (warm) | Tigris-internal |
|---|---|---|
| GET hit (healthy region) | 5-10 ms | ~2-6 ms |
| **GET miss (404), global bucket** | ~287 ms | **~240 ms** (`miss;dur`) |
| **GET miss (404), region-bound bucket** | — | **fra 174-179 ms, sin 237-241 ms** |
| LIST `start-after` `MaxKeys=1`, warm | ~51 ms | ~6 ms (first cold ~247 ms) |

The miss path is a distributed existence proof — multiple metadata
servers are consulted before Tigris will say "not found" (their
explanation, 2026-08-03). Two consequences:

- Each idle instance was buying **~3 seconds of Tigris-internal work
  per wall-clock second** (13/s × ~230 ms) to learn, over and over,
  that nothing changed.
- **Bug (with Tigris, owner: Søren) — RESOLVED 2026-08-06:**
  region-bound (“local”) buckets paid 174-241 ms per miss because an
  eager fallback consulted a sibling metadata cluster (kept to support
  global↔SRC conversion). Tigris disabled the fallback; the rerun
  matrix (§7) measures SRC misses at **2-10 ms internal** (~100×),
  global control unchanged. Exactly as predicted: the miss now costs a
  local metadata lookup, the latency motivation evaporates, and the
  dollar posture below stands on its own.

## 3. Dollars: the story inverts

Tigris pricing (fetched 2026-08-03 from tigrisdata.com/pricing):

- **Class A $0.005/1,000** ($5.00/M): ListObjects/V2, PutObject, Copy,
  CreateBucket, …
- **Class B $0.0005/1,000** ($0.50/M): GetObject, HeadObject, …
- Failed requests are not charged **only** for 301, 307, 400, 403, 405,
  409, 411, 412, 416, 304, 500, 501. **404 is not on the list — a
  404 GET is billed** as a normal Class B request.
- Storage $0.02/GB-mo; zero egress.

So per request a LIST costs **10× the 404 it would replace** — the
exact inverse of the latency relation. Replacing probe-GETs with LISTs
("ListFirst") at unchanged cadence would have cut Tigris-internal burn
40× while **10×-ing our request bill**:

| strategy (per idle instance) | misses/day | $/month | Tigris-internal burn |
|---|---|---|---|
| pre-stretch: probe-GET @ 1000/500 ms | ~1.18 M | ~$18 | ~3.0 s/s |
| ListFirst @ same cadence | ~1.18 M | ~$177 | ~0.07 s/s |
| **probe-GET @ 2000/2500 ms (this commit)** | **~0.31 M** | **~$4.7** | **~0.8 s/s** |
| ListFirst @ 2000/2500 ms | ~0.31 M | ~$47 | ~0.02 s/s |

Conclusions the numbers force:

1. **The 404s were never a dollar problem.** $18/mo/instance is noise
   next to compute and to loaded-day WAL PUTs (Class A: one soak-day at
   the 490 rps ceiling ≈ $19/day of PUTs). The real costs are Tigris-
   side burn and our telemetry drowning in idle misses.
2. **The probe primitive is already dollar-optimal.** Keep it. A
   ListFirst mode is a *vendor-relief* trade, not a savings — worth
   revisiting only if Tigris asks us to reduce miss-path load and
   finding 1 stays unfixed.
3. **The only move that wins on every axis is fewer polls.** Hence the
   stretch.

## 4. The stretch (this commit) and its bounds

Binary defaults are now the single source of truth
(`DEFAULT_MANIFEST_POLL_MS = 2000`, `DEFAULT_COMPACTOR_POLL_MS = 2500`
in main.rs); the deploy/bench scripts (bench/soak/deploy-region.sh,
bench/docker/harness/cluster-deploy.sh, bench/costab/*.sh) **no longer
override them**. Idle instance: ~13 → ~3.6 miss-GETs/s (−73%).

Neither value is arbitrary, and neither should be stretched further
without redoing the analysis:

- **Manifest 2000 ms — bounded by flush liveness, not cost.** The
  manifest poll is also how the memtable flusher learns compaction
  freed L0 slots; 60 s polls produced 14 s flush stalls (bench finding
  2026-07-14, RUNBOOK knob table). Loaded shards want 1-2 s; 2 s is the
  top of the measured-safe band. Going to 5 s would save ~$0.10/day and
  risk p99 under L0 pressure — refused.
- **Compactor 2500 ms — half the upstream default.** The old 500 ms pin
  dated from pre-limiter double-digit-MB/s single-stream pushes. At the
  enforced 5 MB/s/shard, a 2.5 s scheduling gap bounds L0 accumulation
  to ~12.5 MB ≈ 3 L0 SSTs against `L0_MAX_SSTS=64`; drain continuity
  comes from concurrent compactions, not scheduling latency.
- Already long, untouched: history DbReader manifest poll 300 s,
  GC sweeps static 600 s (`GC_QUIET_INTERVAL_SECS`,
  `HISTORY_GC_INTERVAL_SECS`; COST-CAMPAIGN-2 addendum), WAL GC 30 s
  (retention pacing, LIST-based, ~$0.06/day — deliberate).

## 5. Verification

- **DST pin:** `idle_engine_store_traffic_is_bounded_by_the_poll_cadence`
  asserts (a) the default constants themselves, (b) an idle engine's
  total store GETs over a 120 s paused-time window stay within the
  cadence budget (measured 363; old posture ≈ 3× the ceiling), (c) zero
  idle WAL reads, WAL mints, and — pinning campaign 2's fix — at most 2
  idle LISTs. Per poll tick the writer costs 2 GETs (probe + anchor
  revalidation), the compactor ~5; only ~1/3 of the counted GETs are
  404s, which is why the DST twin's totals run higher than the field
  miss counts in §1.
- **Field (soak10):** see §6 — filled in from the production
  verification run.

## 6. Field validation — soak10 (2026-08-05): PASS

fra + sjc, fresh projects with region-local buckets (first soak on the
new local-bucket platform default — note the confound below), standard
10-tier 30-min ramp at the stretched posture, `/v1/debug/store`
window=300 idle samples before and after the ramp. Binary: slate HEAD
200e8973 + this commit, built in an isolated worktree (another
session's unrelated WIP was excluded; provenance in the campaign
workspace).

**Idle probe rate (the headline):** post-ramp steady-state idle, two
samples per region five minutes apart, all four within 2%:

| sample | total GETs/s | miss-GETs/s (404s) | LISTs/s | notes |
|---|---|---|---|---|
| fra b1/b2 | 4.51 / 4.56 | **2.20 / 2.27** | 0.07-0.09 | GC deletes draining normally |
| sjc b1/b2 | 4.59 / 4.53 | **2.22 / 2.26** | 0.07-0.09 | identical shape |

**~2.2 miss-GETs/s per instance vs the ~13/s measured at the old
posture on 2026-08-03 — a −83% reduction**, better than the −73%
prediction because this workload's single hot stream opens one shard
engine, not all four (engines and their pollers start lazily). Idle
LISTs stay at GC-sweep noise (~0.08/s). Per-instance idle spend at
Class B: ~$2.90/month.

**Pre-traffic idle (window A) is its own finding: a virgin instance
performs ZERO store operations.** Both freshly-deployed servers showed
`ops: {}` over 300 s with no shard engines open — pollers only exist
once first traffic opens an engine. Scaled-to-zero-traffic instances
cost nothing in requests.

**Ramp parity vs soak9** (tier-median p50s; soak9 ran the 1000/500
posture on global buckets — bucket locality changed platform-wide
between the runs, so the delta bundles both effects):

| region | append p50 s9→s10 | rt p50 s9→s10 | p99 | ceiling | errors |
|---|---|---|---|---|---|
| fra | 58 → 62 (+5%) | 82 → 91 (+11%) | 206 → 229 | 490 = 490 | **0** (411,055 ok) |
| sjc | 63 → 55 (−12%) | 88 → 79 (−11%) | 186 → 185 | 490 = 490 | **0** (454,545 ok) |

Deltas are inside the historical soak-to-soak variance band (fra sits
within its 58-82 s5..s9 spread) and point in opposite directions in
the two regions — noise, not a poll-stretch signature. The specific
regression the manifest bound guards against (flush stalls under L0
pressure) did not appear: c48/c64 were the *healthiest* tiers in both
regions (fra c48 p50 76.6 ms; sjc c48 66.8 ms), and both regions held
the 490 rps per-stream-limiter ceiling.

**Integrity:** server-durable ≥ client-acked in both regions (fra
+640, sjc +510 — ambiguous-timeout retries that committed; the safe
direction), absorb backlog 0 streams at end, GC visibly retiring WAL
and compacted SSTs in the idle windows. All soak10 infrastructure torn
down and verified (0 projects remaining).

## 7. Open items

- **Tigris local-bucket 404 latency — RESOLVED 2026-08-06.** History:
  reported 2026-08-05; Tigris's initial conversion-fallback explanation
  was narrower than measured (a 08-05 four-way matrix showed two
  BORN-single-region buckets paying the full ~240 ms miss — the eager
  sibling-cluster fallback fired regardless of bucket history; Ovais
  then confirmed the fallback ran eagerly because conversion is allowed
  anytime, and disabled it). Rerun 2026-08-06 03:45 UTC, same buckets,
  same client, same-minute: **born-SRC misses 2-10 ms internal** (was
  238-246), global control unchanged at ~250-320 ms (the change is
  SRC-scoped; methodology apples-to-apples). Streams impact: combined
  with the poll stretch, per-instance idle Tigris burn fell ~350×
  (13/s × 240 ms ≈ 3.1 s/s → 2.2/s × 4 ms ≈ 0.009 s/s). One residue
  handed back to Tigris, low priority: unauthorized requests (403) to
  the converted SRC bucket still burn ~235-240 ms internally — the
  auth-denied path or converted buckets may not be covered by the
  disable; a data-plane key on that bucket would distinguish.
  Evidence: SRC-404-FIX-VERIFICATION.md (with x-amz-request-id per
  request).
- Tigris served-from header value bug (reported earlier).
- SIN backend degradation episode 2026-08-02..03 (idle GETs 473 ms,
  writes 240 ms internal; recovered next day) — context for why SIN
  soak numbers from those days are not baselines.
- If Tigris fixes finding 1, revisit nothing here: the probe stays
  dollar-optimal and the burn argument disappears on its own. If they
  instead ask for less miss-path load, the lever order is: this
  stretch (done) → ListFirst mode at 10× request price → upstream
  `LatestDiscovery` knob proposal with this data.
