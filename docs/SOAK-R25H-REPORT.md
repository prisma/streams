# Corrected six-region soak — R25-H field report

Run `soak-20260811T141001Z-27978`, 2026-08-11, on the R25 build
(commit `1708d0e2` lineage, `0.2.0-preview.8`). 35-minute synchronized
sampling; tier ramp t01–t10 (concurrency 1→64), 10 × 1 KiB records per
request, one stream per region, six fresh region-pinned cells
(us-east-1, us-west-1, eu-central-1, eu-west-3, ap-southeast-1,
ap-northeast-1), FRAME_COMPRESS=1, INITIAL_SHARDS=4, compute-1g memory
profile. Full artifacts: `$SOAK_HOME/results/soak-20260811T141001Z-27978/`.

## Limits in force (essential context for every number below)

| Limit | Value | Consequence |
|---|---|---|
| `LIMIT_RECS_PER_SEC` | **5,000 (default)** | per-stream record limiter; with one stream this capped the whole cell |
| `LIMIT_BURST_SECS` | 2 | |
| `MAX_UNABSORBED_BYTES_PER_SHARD` | 256 MiB | maintenance bound (durable) |
| `MAX_UNABSORBED_BYTES_PER_INSTANCE` | 512 MiB | |
| `MAX_ABSORB_LAG_SECS` | 900 s | no-progress latch |

## Integrity — zero observed loss (bounded claim)

All six regions: durable ≥ acked, one request error fleet-wide (a
single platform-edge SRC-404 in ap-southeast-1).

| region | acked records | durable records | excess |
|---|---|---|---|
| us-east-1 | 2,352,510 | 2,353,150 | +640 |
| us-west-1 | 4,642,080 | 4,642,350 | +270 |
| eu-central-1 | 4,150,420 | 4,151,060 | +640 |
| eu-west-3 | 3,594,070 | 3,594,710 | +640 |
| ap-southeast-1 | 2,277,700 | 2,278,340 | +640 |
| ap-northeast-1 | 3,697,660 | 3,697,960 | +300 |

**Method and its limit.** The check was one-sided: durable tail count
(HTTP HEAD `Stream-Next-Offset`) ≥ generator-sampled acked count. The
uniform +640 excess is exactly one final request per worker at
concurrency 64 — the generator's last stats window systematically
missed in-flight completions, so this is strong "no observed
acknowledged deficit" evidence, **not** exact zero-loss proof: a lost
acked write could in principle hide behind uncounted completions. R26-8
closed this: generators now emit a post-join final record and an exact
per-op ledger, and the reconciler walks every stream verifying each
acked op exactly-once (validated locally: 55,850 records, 0 problems).
The next campaign's integrity claim will be exact.

## Throughput — CORRECTION of the original attribution

Accepted throughput plateaued at ~4,900 rec/s in the four fast regions
while `throttled` grew (us-west-1: 655k). The original report credited
the maintenance backpressure for this plateau. **That attribution was
wrong**: 5,000 rec/s is the DEFAULT per-stream `LIMIT_RECS_PER_SEC`,
the workload wrote ONE stream per region, and the harness merged all
429/503 responses into one `throttled` counter — nothing recorded which
refusal fired. The plateau is fully explained by the ordinary limiter.

What the run DOES support:

1. surplus load was refused with typed, retryable responses — zero
   errors, flat p50s (us-west-1 72.9→70.8 ms across t08→t10);
2. memory posture held (RSS ~265–277 MiB all regions, all tiers);
3. the durable maintenance ledger never grew unboundedly, and drained
   after load stopped.

Whether the maintenance bound also engaged, and for how much of the
shed, was NOT measurable in this run. R26-7 added per-code counters
(client + server) and per-poll `/v1/debug/load` sampling; R26-9 raises
the campaign limiter (`SOAK_LIMIT_RECS_PER_SEC`, default 100k) and
sprays ≥32 streams so the maintenance bound is the binding constraint
under test, not the per-stream limiter.

| region | t08 rec/s | t10 rec/s | throttled (cum) | p50 t08→t10 ms |
|---|---|---|---|---|
| us-west-1 | 4,299 | 4,908 | 655,106 | 72.9 → 70.8 |
| eu-central-1 | 3,661 | 4,902 | 554,837 | 85.5 → 81.7 |
| eu-west-3 | 2,984 | 4,894 | 156,799 | 106.8 → 101.9 |
| ap-northeast-1 | 3,124 | 4,903 | 149,453 | 84.9 → 90.9 |
| ap-southeast-1 | 2,012 | 4,100 | 14,367 | 86.3 → 98.8 |
| us-east-1 | 1,870 | 3,326 | 0 | 137.2 → 154.2 |

## Recovery — observed, not controlled

~15 minutes after the (staggered) end of load, `unabsorbed_frame_bytes`
was 0 in five regions with no latch engaged. us-west-1 held a 154 KiB
residual at `no_progress_secs=938` — under the deleted sparse-deferral
policy that residual would NEVER retire, and at 938 s the region was
one evaluator tick past the 900 s LagSecs bound: the run demonstrated
the exact deadlock R26-1 removed. The recovery window itself was
uncontrolled (generators started and ended at deploy-order-staggered
times); R26-9's synchronized release + fixed `SOAK_RECOVERY_SECS`
window makes the next run's drain time a real measurement.

## CHAOS-5 disposition (bounded)

The Singapore campaign's "9.4% absorption / 3.87 GB deficit" is
retired as a measurement: it compared uncompressed payload bytes added
against compressed frame bytes retired (all-'x' records,
FRAME_COMPRESS=1). This run — exact frame bytes both directions —
showed no unbounded backlog growth and full post-load drain at this
workload's ACCEPTED rate. That accepted rate was capped by the
per-stream limiter, so this is NOT yet a supported-capacity number for
the absorber. The decisive test remains the pause/catch-up capacity
gate (R23-8/R26-11): history depth at Singapore scale, ≥2 h sustained,
3-minute absorber pause, catch-up under load, exact reconciliation.

## Campaign infrastructure notes

- The automated reconcile first reported LOSS in all six regions: the
  harness probed the tail with `GET ?head=1`, which the raw route
  ignores — it served a full page from the horizon and the header
  named the END OF THAT PAGE. The tail probe is HTTP HEAD. Fixed,
  re-run against the preserved namespace before teardown.
- Teardown verified: 12 services, 6 buckets, 6 projects deleted;
  creation receipts retired; platform project list clean.
- Platform: the SPARK_NONROOT_UID crash-loop regression that blocked
  the first attempt was fixed platform-side before this run.
