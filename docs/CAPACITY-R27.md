# R27-4: Incompressible overload campaign — the bulk-transfer OOM, its fix, and the gate

Status: campaign in flight (cap-20260813T051338Z-94762); verdict section
pending. Everything above the verdict is settled evidence.

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

## Verdict

PENDING — tightened posture in local latency-rig validation; SIN rerun
follows. Acceptance: strengthened criterion B (peak ≥ 0.75× instance
cap, ≤ 1.05× overshoot, typed maintenance shed observed, stabilized at
the line, drained after healing), rate limiter silent, no unexpected
counter reset, exact op-ledger reconciliation OK, recovery clean — plus
`bulk_gate.waits > 0` observed in the field (the new bound must engage,
not merely exist).
