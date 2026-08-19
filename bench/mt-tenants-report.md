# MT-tenants campaign — tenant-cardinality vs throughput (2026-08-19)

**Question** (Søren): on one cell, how do throughput (rps, bytes/s)
and latency respond as the same workload is split across 1 → 10 →
100 → 1000 active tenants?

**Rig**: one `streams-slate` (commit f7e5fd91 lineage, binary sha in
`binaries.json`) on Prisma Compute eu-central-1, compute-1g memory
profile, real Tigris bucket, co-located awsbench generator
(docs/SOAK-REGIONS.md topology). Run id `mtten-20260819T102840Z`;
raw artifacts in `$SOAK_HOME/results/mtten-20260819T102840Z/`.

**Workload — identical in every stage**: 1000 streams, all active,
one paced single-record append per stream per 167ms (offered ≈ 6k
rps), 1 KiB JSON records, 300s steady window, fresh stream prefix per
stage. The only moving axis is HOW MANY projects own those streams
(stream i → project i mod N; every request carries that project's
RS256 customer JWT). Stage 0 is the control: auth off, legacy raw
surface, same physical workload.

| stage | surface / auth | active projects | achieved rps | payload MB/s | p50 | p99 (median win) | throttles | errors |
|---|---|---|---|---|---|---|---|---|
| 0 | raw, off | 1 | **1,331** | 1.36 | 757 ms | 1,585 ms | 276 | 0 |
| 1 | product, enforce | 1 | **974** | 1.00 | 1,059 ms | 2,265 ms | 55 | 0 |
| 10 | product, enforce | 10 | **1,185** | 1.21 | 854 ms | 1,850 ms | 1,887 | 0 |
| 100 | product, enforce | 100 | **893** | 0.91 | 745 ms | 1,787 ms | 128,199 | 0 |
| 1000 | product, enforce | 1000 | **788** | 0.81 | 989 ms | 2,000 ms | 80,738 | 0 |

Reading guide: the harness is per-stream closed-loop (at most one
in-flight append per stream), so at saturation achieved rps ≈
1000 / latency — Little's law checks out in every stage (e.g. stage 0:
1,331 × 0.757s ≈ 1,008 outstanding). "Throttles" are typed server
refusals (429/503 with the streams origin header); zero transport
errors anywhere. The server's own counters attribute ALL shed to
ADMISSION (`admit_shed=210,879` cumulative across the enforce stages;
`stream_shed=0`, `wedge_shed=0` — maintenance backpressure never
engaged).

## Findings

1. **Surface + auth cost (stage 0 → 1): −27% rps** (1,331 → 974),
   p50 +40%. This is the bundled cost at saturation of enforce-mode
   verification (RS256 + policy/grant snapshot checks per request),
   product-route processing, per-project admission, and usage
   accounting — for an unbatched 1 KiB single-append shape, the
   worst case for fixed per-request overhead.

2. **Tenant cardinality itself is NOT monotonically expensive.**
   Stage 10 is FASTER than stage 1 (+22%): with one tenant, that
   tenant's per-project admission budget is the choke point for all
   1000 streams; ten tenants get ten budgets and recover parallelism.
   This is working as designed (per-project isolation), but it means
   a single very-wide tenant on one cell self-limits — worth knowing
   for placement policy, and worth revisiting the per-project
   admission default for whale tenants.

3. **Past ~100 tenants the cost is admission fragmentation + per-
   request tenant state, not maintenance.** Stage 100 shows the
   throttle peak (128k refusals, 30% of attempts) as fixed per-project
   budgets fragment; stage 1000 (one stream per project ⇒ at most one
   in-flight per project) throttles less but pays more per request —
   net **−19% rps from 1 → 1000 tenants** on the product surface
   (974 → 788), and −41% vs the raw/off control.

4. **Nothing broke.** Zero errors in ~1.55M acked appends across the
   five stages; refusals were all typed admission shed; the cell held
   its memory profile; every stage's 1000 enforce-mode creates
   completed in under ~1s (registry group commit, not per-op PUTs).

## Regression posture

No prior CLOUD run of this exact shape exists (July's wide runs were
local; R25-H's plateau used batch=10 on the raw surface), so stage 0
is now the PINNED baseline for this shape: **1,331 rps / 1.36 MB/s /
p50 757ms @ 1000 closed-loop streams, fra, compute-1g**. Nothing in
this run contradicts the R25-H-era envelopes; the known ~4.9k rec/s
plateau was a batched shape and is not comparable record-for-record.

## Caveats

- Closed-loop-per-stream (coordinated omission): latencies are
  saturation latencies at ~1000 outstanding; sub-capacity latency was
  not measured here.
- One run per stage; day-of-week/object-store variance not averaged.
- Stage-10 anomaly replicated only within this run (single sample).

## Campaign traps recorded (for the next operator)

- `provision.py` per run: teardown deletes the project; stale service
  ids in the project cache deploy as "Resource Not Found".
- Enforce refuses `PROJECT_ID` defaults at boot (correct); the
  campaign env must set an explicit deployment project id.
- `verify_server_live` gates every server deploy now — a crash-looping
  binary serves diagnostics on $PORT and looks deployed otherwise.
- Cell token-lifetime ceiling is 24h; campaign tokens mint at 12h.
- Rig left UP for follow-ups (teardown pending, like sinmax).
