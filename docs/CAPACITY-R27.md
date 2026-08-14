# R27-4: Incompressible overload campaign — the bulk-transfer OOM, its fix, and the gate

Status: **PASSED** — cap-20260813T085718Z-55853 (Singapore, binary
4da313a1, commits b6c04a79 + 7acd6f01). Verdict at the bottom.

## The finding (this is what the campaign was FOR)

The first incompressible campaign (cap-20260812T041158Z-47832, Singapore,
binary 355fda30-era) was OOM-killed (exit 137) at +26 min of stable
~1.6 MB/s incompressible ingest. The mechanism the campaign was designed
to exercise — the durable maintenance ledger and its typed backpressure —
was HEALTHY the whole way down:

- maintenance ledger: 50–86 MB against a 512 MB instance cap (bound never
  engaged, correctly — the absorber was keeping up)
- rss_shed active (11k→22k), flat p50s, store healthy (p50 27 ms)
- RSS 423→656 MB in ~60 s, then the kill; out_inflight_peak=74

The kill was NOT the backlog. It was a different, unbounded memory
consumer that only appears at WAN store latency.

## Root cause: in-flight bulk store transfers do not compose across DBs

Attribution chain, each step reproduced locally:

1. **No-latency control** (Docker 1 GiB, host MinIO, same knobs, same
   incompressible load): survived far past the SIN death point — 4.6 GB
   ingested, RSS pinned at the shed line. Store RTT is the discriminating
   variable.
2. **Latency-injected repro** (toxiproxy, 35 ms downstream / 10 ms
   upstream to MinIO): reproduced the exact SIN signature at ~5.6 GB
   ingested — ledger healthy at 59–64 MB, RSS 510→757 MB in ONE 5 s
   sample as concurrent store ops burst 14→22 (peak 53), exit 137.
   ~250 MB / 22 ops ≈ 11 MB per op: SST-scale payloads.
3. **Class attribution** (rerun with per-class op sampling, 419 active
   samples): every RSS wave peak coincides with an SST-op storm —
   137–351 `get:sst` + 14–22 `put:sst` per 6 s window — while calm
   samples show near-zero SST ops. The waves are compaction reading
   source SSTs while writing outputs.

Why latency is the trigger: the instance hosts many SlateDBs (4 shard
DBs + history + telemetry + registry + usage). Each has its own flush
and compaction tasks with per-DB concurrency limits, and each task
buffers MB-scale payloads (8 MiB L0 SSTs at the survival profile). At
sub-ms RTT a compaction finishes before the next DB's begins; at
20–40 ms RTT every transfer lives ~30× longer, all DBs' waves overlap,
and the instance-wide buffered-byte peak scales with store latency.
**Per-DB compactor limits do not compose into an instance bound**, and
the RSS shed line only stops *customer appends* — internal I/O sails
through it.

## The fix: STORE_BULK_INFLIGHT_MAX_BYTES (commit b6c04a79)

A byte-weighted admission gate in the TimingStore wrapper — the one seam
every DB shares, and therefore the only place an instance-wide bound can
exist without forking SlateDB again.

- **Scope: sst-class ops only.** WAL (ack path), manifest (CAS
  liveness), and fleet (cluster liveness) never queue behind compaction.
- **Weights are honest where it matters:** put payloads and mpu parts
  exactly; `get_ranges` by the exact requested byte total (the buffers
  materialize inside the call); unbounded-length sst gets at one nominal
  L0 (8 MiB).
- **Deadlock-freedom:** permits are held only across the leaf await of
  the inner store call, never across stream consumption, so every
  waiter is eventually satisfied by ops that complete on pure network
  I/O. An op larger than the cap clamps to the whole cap (serializes,
  never starves).
- **Observability:** `/v1/debug/store` exports `bulk_gate`
  {cap_bytes, inflight_bytes, waits_total, wait_ms_total}.
- compute-1g profile: 48 MiB (~6 concurrent L0 SSTs).

Unit gates (store_timing.rs): concurrent-byte bound holds under 12
contending tasks; oversized op clamps and completes; non-sst classes
never take a permit; waiter liveness on release.

## Local validation (same latency rig, gate on)

Survived past BOTH prior kill points: 6.3 GB ingested at full pace
(~2.4 MB/s, identical to ungated throughput — the gate queues bursts,
it does not throttle steady state), RSS 467–486 MB at the shed line
during waves that previously spiked to 930 MB. The bound was genuinely
exercised: 5,806 gate waits, 427 s cumulative queueing. One residual
wave peaked at 807 MB RSS (survived) — the gate caps the SST-transfer
component, not every consumer; margin exists but is not lavish on a
1 GiB instance.

## Campaign infrastructure findings (platform)

- **Artifact bucket revoked mid-campaign** (2026-08-13): the external
  Tigris-org bucket accepted build-upload's PUT + ranged-GET at 04:50 Z
  and denied the SIN instances' boot downloads by 04:52 Z; the key never
  recovered (GetObject AccessDenied from all vantage points; the key
  itself stayed alive — list_buckets worked). Artifacts are now
  platform-homed (bench/soak/provision-artifacts.py) in a bucket of the
  same kind the per-run stream-data buckets use, which have had zero
  credential failures across all campaigns. The R25-G diagnostic wrapper
  (serve-the-download-failure-instead-of-exiting) turned this from a
  half-day platform goose chase into a one-request diagnosis.

## Second SIN kill: the gate alone is not enough (cap-20260813T051338Z)

The gated binary (b6c04a79, cap 48 MiB) was killed on SIN at ~+28 min /
2.5 GB ingested — essentially the same point as the ungated first kill.
The samples show RSS steady at 360–400 MB, one sample at 500 MB, then
death within the next 31 s interval; ledger healthy (34–73 MB),
maintenance shed 0. Locally the same binary had survived to 6.3 GB with
one residual 807 MB wave.

Two lessons:

1. **Queue-held buffers are un-gateable.** The gate bounds bytes IN
   transfer; a task queued at the gate has already built its 8–16 MiB
   payload. ~7 resident DBs × (compactions + flush) can hold 200–340 MB
   at the gate during an overlapped wave. The lever for that mass is
   task count, not the cap.
2. **The SIN kill line is far below 1 GiB.** The Bun wrapper and
   platform agent share the instance; the binary's real budget is
   roughly 700 MB where the local container gives ~950 MB. The local
   807 MB wave that "survived" would kill SIN.

Posture change (compute-1g.env): STORE_BULK_INFLIGHT_MAX_BYTES 48→32 MiB
AND COMPACTOR_MAX_CONCURRENT=1 (was 2 via campaign scripts; binary
default 4; the profile now owns it). Escalation held in reserve if the
local rig still peaks above ~650 MB: L0_SST_SIZE 8→4 MiB, which halves
both the per-task buffer and the per-op transfer.

## Third component: the compaction worker's own memory model

With the gate pinned at cap and one compaction per DB, the local rig
STILL waved 478→886 MB in 30 s (and SIN run 2, cap-20260813T051338Z,
died at the same ~2.5 GB point as run 1). The mass was upstream
CompactionWorkerOptions defaults, sized for big instances:
`max_subcompactions: 4` (four concurrent pipelines inside ONE
compaction), `max_fetch_tasks: 4 × bytes_to_fetch: 2 MiB` (~8 MiB
read-ahead per input-SST iterator — a 32-input L0 merge can stage
~1 GB of completed prefetch the gate cannot see; it bounds bytes in
transit, not buffers already fetched), and 256 MiB output rolls. The
worker's internal `max_concurrent_compactions` default (4) was also
silently overriding our compactor setting.

Commit 7acd6f01 exposes all four as env
(COMPACT_MAX_SUBCOMPACTIONS / COMPACT_MAX_FETCH_TASKS /
COMPACT_BYTES_TO_FETCH / COMPACT_MAX_SST_SIZE_BYTES), mirrors the outer
concurrency into the worker, and pins the 1 GiB profile to
1 subcompaction, 1×1 MiB read-ahead, 32 MiB rolls. Local rig with the
full stack: survived past 7 GB, peak RSS 886→723 MB.

## Verdict: PASS (cap-20260813T085718Z-55853)

Both criteria, not just one:

- **B — hard bound exercised and held:** peak ledger 540,191,131 bytes
  = 100.6% of the 512 MiB instance cap (within the 1.05× in-flight
  allowance; peak shard 185 MB ≪ 256 MiB shard cap), typed maintenance
  shed 16,613, stabilized under overload, drained after healing.
- **A — catch-up:** retirement 2.27 MB/s vs steady ingest 1.47 MB/s =
  1.545× (gate 1.25×).
- Rate limiter silent (all limit_* zero — the right mechanism shed).
- **No process exit** (unexpected_reset=false) — the item both prior
  runs failed. ~7.5+ GB incompressible ingest; observed RSS peak 556 MB
  on the 1 GiB instance.
- Recovery: backlog + latches clear **0.8 s** after generators
  finished; pause wall 319 s.
- Exact op-ledger reconciliation: verdict OK (exactly-once, ambiguous
  ops resolved 0-or-1-complete).

## Post-verdict classification (R29 review items)

**The six generator errors**, classified by origin and outcome: all six
were origin-less HTTP 502s from the platform edge (`infrastructure
status 502` — no `prisma-streams-origin` header, so never counted as a
Streams response). The exact op-ledger reconcile proves none landed:
walked records 9,461,360 == acked ops × batch exactly, zero
unknown-op records, zero duplicates, zero landed-ambiguous. All 17
pause-window availability probes served (0 failures).

**cgroup memory.peak**: the binary now exports `cgroup_peak_bytes`
(v2 `memory.peak`, v1 fallback) in /v1/debug/load, but Compute's
sandbox does not expose cgroupfs to the workload — the field reads
null in the field. Kernel-peak capture is therefore recorded as a
PLATFORM dependency alongside the digest-verified-readiness contract;
until then the margin evidence is the local cgroup-limited Docker rig
(886→723 MB peak across the posture change) plus sampled RSS in the
field.

## R27-5: fleet handoff at peak backlog — PASS (handoff-fh185257)

Four-instance fra fleet + rendezvous LB, R29 binary (503bc59f, commit
5efc5790), identity-verified on every instance. Sequence and results:

- All four absorbers paused; fleet backlog built under LB load until
  the target owner (s4) held **365 MB of durable maintenance backlog**
  (requirement: >= 250 MB; shortfall now FAILS the gate).
- s4 killed with a real SIGABRT (`/v1/debug/abort` — no WAL flush, no
  fencing handoff, no absorber drain).
- Successors restored **every pre-kill shard at >= 100% of its exact
  pre-kill gauge** (aggregate ratio 1.0017) within 37 s — survivor
  absorbers stayed paused through verification, so the comparison is
  monotone-exact, not a cross-time approximation.
- Survivors resumed; catch-up under CONTINUED generator load returned
  the fleet to its steady band; after `/stop`, absolute drain reached
  3 MB in 73 s.
- Exact op-ledger reconciliation through the LB: 17,080 acked ops ->
  170,800 records walked exactly; **all 2,879 kill-window ambiguous
  ops resolved to zero-landed**; zero duplicates, zero unknown-op
  records.

Also exercised on the way (runs 3-4): hard-kill diagnosis via the
supervisor's `binary_exited` surface, and **kill-and-replace** — a dead
ordinal deployed back (ONLY=1), urls re-published, LB refreshed, fleet
healthy. A quarter-dead fleet degraded generator goodput ~10x through
LB retry latency, which is itself useful operational data: a dead
upstream must be replaced or removed from rotation promptly.

Caveat recorded honestly: run 5's pre-pause baseline (575 MB) was
inflated by residue from the two aborted prior attempts sharing the
namespace, which made the under-load band generous; the rigorous drain
evidence is the absolute post-load drain to 3 MB.

## Cost regression on the final profile (R29 item 8)

w10k wide regime, identical load, 600 s steady windows, s3lite request
ledger; steady-window deltas (defaults -> certified 1 GiB profile):
PUT 64,227 -> 67,870 (+5.7%), DELETE 57,411 -> 60,868 (+6.0%), LIST
2,581 -> 2,596 (+0.6%), GET 45,252 -> 40,559 (-10.4%), get_bytes
-3.5%, put_bytes +0.7%, multipart 0 -> 36 (the 32 MiB rolls). At a
Class-A ~= 5x Class-B price weighting this is **+4.7% cost units**:
smaller output rolls cost more compaction Class-A requests, partially
offset by less read-ahead over-fetch. No monotone growth; the LIST
economy from the cost review is preserved. Accepted as the price of
the survival posture on the 1 GiB class.
