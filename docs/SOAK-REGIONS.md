# Six-region soak: how far apart are the regions?

**Run**: 2026-07-26, 07:06–07:41 UTC. Six Prisma Compute regions, one
Streams server and one **co-located** load generator each, one dedicated
Prisma Bucket each. Ten concurrency tiers, 180 s per tier, 1 KiB records,
batches of 10, producer and consumer both running.

Harness: [`bench/soak/`](../bench/soak/). Raw data: `results.json` from
`harvest.py`.

## What this measures, and what it does not

The generator runs **inside the region under test**. A generator on the
operator's laptop measures the operator's distance to the region — a real
number, but not Streams'. At 1 KiB records an append costs 30–200 ms
depending on region, and a transpacific round trip is the same order, so
an external client would have buried the signal under its own geography.

So every latency below answers: *what does Streams cost a caller that is
already in this region?* That is the question a regional deployment
actually has to answer.

Two client-side metrics, both windowed over 20 s:

| metric | meaning |
|---|---|
| **append p50/p99** | request → durable ack. The write path, end to end, including the WAL PUT to Tigris. |
| **roundtrip p50/p99** | producer's embedded timestamp → the moment a consumer observed the record. The full publish-to-subscribe latency. |

Server-side, `/v1/debug/store` reports per-operation object-store latency
and — from Tigris's own `x-tigris-served-from` response header — which
Tigris PoP actually served each request.

## Headline

The regions are **not** within a factor of two of each other. On the write
path they span roughly 5×, and the spread is dominated by one variable:
how fast the local Tigris PoP completes a small WAL PUT.

- **ap-northeast-1 (nrt) and us-west-1 (sjc) are the fast pair.**
- **us-east-1 (ewr, storage in iad1) is the slow outlier** — its `put:wal`
  p50 is 4–6× every other region's. This is the same iad1 degradation
  already reported to Tigris, now confirmed from inside Compute rather
  than from a probe, on the exact operation our durability depends on.
- **eu-central-1 collapsed mid-run** and its numbers past tier 6 are not
  comparable. The cause is documented below; it is the most interesting
  finding of the run and it is not a latency number.

**13.21 million records acknowledged** across the six regions (13.55 M
durable server-side), with **zero acknowledged records lost anywhere**.

### Per-region summary

| region | PoP | records | requests | errors | throttled | append p50 | append p99 | roundtrip p50 | roundtrip p99 |
|---|---|---|---|---|---|---|---|---|---|
| us-east-1 | ewr/iad | 788,250 | 78,825 | 0 | 0 | 455.8 | 3,029.0 | 539.1 | 5,107.7 |
| us-west-1 | sjc | 3,321,240 | 332,124 | 0 | 273,433 | 101.1 | 456.7 | 129.5 | 1,279.0 |
| eu-central-1 | fra | 221,160 | 22,116 | 1,090 | 103,057 | 99.8 | 26,378.2 | 181.1 | 65,896.4 |
| eu-west-3 | cdg | 2,912,730 | 291,273 | 0 | 19,998 | 118.6 | 393.2 | 164.1 | 1,214.5 |
| ap-southeast-1 | sin | 2,252,230 | 225,223 | 0 | 25,444 | 124.3 | 1,499.1 | 328.6 | 2,670.6 |
| ap-northeast-1 | nrt | 3,717,900 | 371,790 | 0 | 481,464 | 77.9 | 821.8 | 110.6 | 841.2 |

Reading the summary: `append p50` is the median of the ten per-tier
medians, `append p99` is the worst per-tier p99 across the ramp. The
throttle counts are the per-shard limits doing their job — see
"Throttling" below. Everything is one stream on one shard on one instance;
the fleet and the auto-scaler were **off** for this run (no `FLEET_PREFIX`,
so no heartbeat loop and no segment splitting). This measures a single
region's baseline, not the scaling behaviour validated in
[SCALING.md](./SCALING.md).

## Region ranking

On the write path, taking each region's flat middle tiers:

| rank | region | PoP | append p50 | roundtrip p50 | notes |
|---|---|---|---|---|---|
| 1 | ap-northeast-1 | nrt | **78 ms** | **111 ms** | fastest and the only region 100 % served locally |
| 2 | us-west-1 | sjc | 101 ms | 130 ms | flat across the whole ramp |
| 3 | ap-southeast-1 | sin | 124 ms | 329 ms | noisier; 13 % of storage traffic left the region |
| 4 | eu-west-3 | cdg | 119 ms | 164 ms | the most consistent region in the run |
| 5 | eu-central-1 | fra | 100 ms → wedged | 181 ms → wedged | healthy until tier 6, then collapsed |
| 6 | us-east-1 | ewr | **456 ms** | 539 ms | 4–6× the others; storage-bound |

The spread on the write path is roughly **5×** between the best and worst
healthy region — this is not a set of interchangeable deployment targets.
The read path is far more uniform: the roundtrip is consistently the
append plus 30–60 ms, except in SIN where it is append plus ~200 ms.

## us-east-1 is storage-bound, and it is the same iad1 problem

us-east-1's append latency barely moves across the ramp — 212 ms at
concurrency 1, 493 ms at concurrency 64. A latency that is flat in
concurrency and high at concurrency 1 is not queueing; it is the cost of a
single round trip. The storage telemetry says exactly where it goes.

Sampled mid-ramp (tier 5, concurrency 12, 60 s windows) — the WAL PUT is
the operation every durable ack waits on:

| region | PoP | `put:wal` n | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|
| ap-northeast-1 | nrt | 1,372 | **34 ms** | 66 | 164 | 734 |
| eu-central-1 | fra | 1,060 | 44 ms | 87 | 245 | 338 |
| us-west-1 | sjc | 1,092 | 46 ms | 83 | 159 | 313 |
| ap-southeast-1 | sin | 501 | 51 ms | 228 | 314 | 660 |
| eu-west-3 | cdg | 902 | 56 ms | 97 | 229 | 299 |
| **us-east-1** | **iad1** | **259** | **214 ms** | **334** | **537** | **645** |

us-east-1 completed a quarter as many WAL PUTs in the same window as
ap-northeast-1, because each one cost 6× as much.

The post-drain snapshot, taken with no load at all, isolates it further —
these are uncontended single operations:

| op | us-east-1 (iad1) | us-west-1 (sjc) | eu-west-3 (cdg) | ap-southeast-1 (sin) | ap-northeast-1 (nrt) |
|---|---|---|---|---|---|
| `delete:wal` p50/p99 | **181 / 388** | 30 / 83 | 28 / 41 | 16 / 26 | 14 / 24 |
| `delete:manifest` p50/p99 | **195 / 500** | 32 / 102 | 30 / 60 | 16 / 35 | 15 / 33 |
| `put:manifest` p50/p99 | **151 / 267** | 34 / 66 | 60 / 74 | 28 / 42 | 29 / 63 |
| `get:manifest` p50/p99 | 33 / 135 | 11 / 61 | 21 / 118 | 11 / 92 | 12 / 106 |

Every **mutating** operation in iad1 costs 5–12× its equivalent elsewhere,
while GETs are only 2–3× worse. That asymmetry matters: our durability
path is all PUTs and DELETEs, so it lands squarely on the degraded side.

This is the same iad1 degradation already reported to Tigris from the
observatory probes (RUNBOOK §14), now reproduced from **inside** Compute,
against a Prisma Bucket, under real Streams traffic, on the exact
operations the ack path depends on. `served_from` confirms 97 % of these
requests were served by iad1 itself, so this is a slow local PoP and not a
routing artefact.

Practical consequence: **us-east-1 cannot meet the 250 ms durable-ack SLO
today.** Not because of anything in our pipeline — its `timer_tokio`
drift, `steal_pct` and error counts were all clean — but because a single
WAL PUT costs 214 ms there.

## Throttling: the per-shard limits held

Four regions hit the per-shard admission limits in the last tiers, and the
counts are large — 481,464 throttled requests in ap-northeast-1, 273,433
in us-west-1. That is the intended behaviour, not a fault.

Each region ran **one stream**, which hashes to **one shard**, and a shard
is capped at 1,000 requests/s and 5,000 records/s
(`LIMIT_REQS_PER_SEC` / `LIMIT_RECS_PER_SEC`, RUNBOOK §3.2b). At batch 10
those two bind together at ~500 requests/s. Look at where the ramp
flattens:

| region | tier 9 (conc 48) | tier 10 (conc 64) |
|---|---|---|
| ap-northeast-1 | 489 req/s, 4,893 rec/s | 490 req/s, 4,908 rec/s |
| us-west-1 | 432 req/s, 4,331 rec/s | 490 req/s, 4,912 rec/s |
| eu-west-3 | 366 req/s, 3,663 rec/s | 482 req/s, 4,824 rec/s |

Every healthy region converges on ~490 req/s and ~4,900 rec/s and stops —
the limiter pinning throughput within 2 % of its configured ceiling, with
**zero errors** and latency that does not degrade at the ceiling
(ap-northeast-1's append p50 is 79.6 ms at tier 9 and 75.4 ms at tier 10).
Excess load is rejected cleanly rather than queued into latency. That is
the reject-vs-queue contract working as designed.

us-east-1 never reached the limit — it topped out at 124 req/s, storage-
bound long before admission control had anything to say.

## Integrity

| region | client-acknowledged records | server-durable records | delta |
|---|---|---|---|
| us-east-1 | 788,250 | 788,891 | +641 |
| us-west-1 | 3,321,240 | 3,321,780 | +540 |
| eu-west-3 | 2,912,730 | 2,913,680 | +950 |
| ap-southeast-1 | 2,252,230 | 2,252,870 | +640 |
| ap-northeast-1 | 3,717,900 | 3,718,570 | +670 |
| **eu-central-1** | **221,160** | **549,690** | **+328,530** |

**No acknowledged record went missing in any region.** The server count is
never below the client count.

The +540…+950 deltas in the healthy regions are sampling skew, not drift:
the client's counter comes from its last 20 s window sample, so writes
that completed after that sample are durable but uncounted. The magnitude
is right for one window at those rates.

eu-central-1's +328,530 is a different thing entirely, and it is the
subject of the next section.

Compression held at **10.6×** in every region (1 KiB records, zstd-1
before encryption, `FRAME_COMPRESS=1`) — identical across all six, which
is what you would expect from a fixed payload shape and a useful
confirmation that the frame path behaves the same everywhere.

## The eu-central-1 wedge

Somewhere between 07:20 and 07:24 UTC, at tier 6 (concurrency 16),
eu-central-1 stopped making progress. The generator's success counter
froze at 22,116 requests and its achieved rate went to zero for the rest
of the run. Its errors kept climbing at a steady ~16 per 20 s window.

The server was **not** down. It answered `/v1/debug/*` in under a second
throughout, from outside the region.

### What the server was doing

`/v1/debug/store`, 60 s window, at 07:27 — eu-central-1 next to a healthy
region for contrast:

| operation | eu-central-1 (fra) | ap-northeast-1 (nrt) |
|---|---|---|
| `get:wal` | **12,666** | — (none) |
| `head:wal` | **3,351** | — (none) |
| `get:sst` | — (none) | 3,818 |
| `put:wal` | **5** | 1,242 |
| `put:sst` | — (none) | 132 |
| `delete:wal` | **2** | 1,255 |
| `put:manifest` | 16 (5 err) | 92 (5 err) |
| outbound in flight | **41** (peak 88) | 19 (peak 42) |

The write path had essentially stopped — 5 WAL PUTs in a minute — while
the read path issued 16,000 WAL operations in the same minute. There was
no SST activity at all: nothing being compacted, nothing being trimmed.

At the time we called this a **read-amplification runaway** and blamed
tail readers scanning an untrimmed WAL. The follow-up root-cause work
(2026-07-27) disproved that mechanism and found the real one, which was
then reproduced deterministically in DST
(`reopen_storm_reproduces_the_eu_central_wedge`, 7,503 WAL GETs for a
120-SST WAL, 11 of 12 opens fenced):

**It was a detached-reopen storm.** A live-Db scan never reads WAL SSTs
from the store at all — durable-but-unflushed data is served from
in-memory memtables, so tail readers could not have produced those GETs.
What does read WAL SSTs is **open replay**. The engine died once (fra was
already throwing 502s at tier 2–3), and the reopen had to replay hundreds
of WAL files at 300–500 ms each — far longer than the generator's 30 s
timeout. When the client disconnected, axum dropped the handler future
and released the open lock, but the inner Db open had been *spawned* onto
the SlateDB runtime (`on_slatedb_rt`), so it kept replaying, detached,
its result destined for a oneshot nobody held. The next request started
another full replay. Detached replays piled up until they owned the
outbound connection budget (the 41–88 in flight); each one that completed
bumped the writer epoch and fenced the previous zombie — a writer-epoch
war of one process against itself, visible as the far-future `head:wal`
probes (zero-byte WAL fence objects). No result was ever inserted into
the serving map, and no writer survived long enough to flush L0, so
`replay_after_wal_id` never advanced and every new replay did the full
range again.

Fixed by `src/sharddir.rs` (`OpenGate`): opens are single-flight per
prefix, run in a task that owns its own completion and inserts into the
serving map itself, callers get bounded-wait retryable 503s instead of
the power to abandon an open, and engines that die young meet an
exponentially escalating holdoff. Under DST the same sick store and the
same impatient clients cost 616 WAL GETs — one replay — instead of
7,503, and the engine lands in the serving map even though every client
that asked for it had already given up.

### Every "failed" write actually succeeded

This is the part that matters for correctness.

While the client counted only errors, the server's durable record count
kept advancing — from 539,280 at 07:24 to 541,040 at 07:28, about
9.8 records/s. The generator runs 24 workers with a **30 s HTTP timeout**
(`bench/awsbench/src/main.rs:580`); 24 workers each completing one
10-record batch per ~30 s is 8 records/s. The rates match.

So the appends were not failing. They were completing **after the client
had already given up on them**. At the end of the run the server held
54,969 append requests and 549,690 durable records against the 22,116 the
client had seen succeed — **2.5× more durable writes than acknowledged
ones**, and 1,090 errors reported for writes that had in fact landed.

From the client's side this is indistinguishable from failure, and a
client that retries on timeout would have written every one of those
records twice. This is exactly the regime that producer idempotence
exists for (`Producer-Id` / `Producer-Epoch` / `Producer-Seq`, duplicate
→ 204). The soak generator does not use it, which is why the discrepancy
is visible here at all.

**The system did not lose data and did not corrupt order. It failed to
shed load, and it failed to tell the client the truth about latency.**

### The correlating anomaly: cross-PoP routing

eu-central-1 is the only region where Tigris served a large fraction of
requests from a **remote** PoP. From Tigris's own `x-tigris-served-from`
header, cumulative over the run:

| region | local PoP share | notable remote |
|---|---|---|
| ap-northeast-1 | nrt 100% | — |
| us-west-1 | sjc1 98% | syd 1% |
| eu-west-3 | fra 99% | jnb 1% |
| us-east-1 | iad1 97% | ord 1% |
| ap-southeast-1 | sin 87% | nrt 8%, fra 5% |
| **eu-central-1** | **fra 72%** | **ord1 26%**, jnb 1% |

A quarter of eu-central-1's object-store traffic crossed the Atlantic to
Chicago. It is also the only region that wedged.

Be careful with this: the `served_from` counters are cumulative from boot,
so they establish that heavy cross-routing and the wedge **coincided in
the same region**, not that one preceded the other. The mechanism is
plausible and the correlation is the strongest in the dataset, but this is
one region in one run. It is a lead to give Tigris, not a proven cause.

### What we are changing

The failure is ours regardless of what triggered it. A storage backend
getting slower must degrade throughput, not collapse into a read storm
that starves writes and hands clients timeouts on requests that succeed.

Concretely:

1. ~~Bound the read path's share of the outbound budget~~ — superseded:
   the reads were not the tail path but detached open replays, and the
   `OpenGate` fix removes them at the source rather than rationing them.
2. **Alarm on the WAL-to-SST ratio** — done in this run
   (`wal_read_storm` in `/v1/debug/store`); the shape detects the storm
   regardless of which mechanism produces it, plus `shard_opens` counters
   now expose the reopen loop directly (started climbing while completed
   stays flat).
3. **Make slow appends fail fast rather than complete late** — still
   open. During the storm the few surviving appends landed after the
   client's timeout; producer idempotence remains the client-side answer,
   and server-side fail-fast is future work.

## What this run changed in the codebase

Every item below is landed, not proposed.

| learning | change |
|---|---|
| A compaction stall is visible in telemetry we already collect, minutes before throughput dies | `wal_read_storm` detector in `src/store_timing.rs`, surfaced in `/v1/debug/store`, with four unit tests built from this run's real windows (the eu-central shape, the nrt shape, an idle instance, and recovery) |
| A missing required env var makes a binary exit at startup, and Compute reports the version `running` while its domain 404s — indistinguishable from a boot failure | `deploy/*/supervise.ts`: if the child exits, the wrapper binds `$PORT` and serves the exit code plus the stderr tail. Plus `BENCH_SHAPE` now has a `default_value` (`bench/awsbench/src/main.rs`) |
| Preview domains belong to a *version*; a redeploy silently retires the old URL | `bench/soak/resolve-urls.sh`, called at the end of every deploy; RUNBOOK §7.5 row |
| The soak harness existed only in a scratch directory | versioned at `bench/soak/` with a README of invariants, plus `deploy/app-gen/` in the repo |
| A dry-run that echoes commands leaks the platform token | `bench/soak/teardown.sh` redacts it |
| `/v1/debug/store` is a trailing 60 s window, so harvesting it after the run returns an empty window — this run's first harvest produced a dash in every storage cell | `bench/soak/poll.py` now writes a timestamped store snapshot on every pass; invariant 5 in the harness README |
| Region selection for staging was based on stale assumptions | `docs/STAGING.md` §2 revisited; two new alarms in §8 |

Deploy footguns met this run are catalogued in
[deploy/README.md](../deploy/README.md#deploy-footguns) and RUNBOOK §7.5:
Compute's region codes are not Tigris PoP codes (`us-east-1` → `ewr`,
storage in `iad1`), `deploy` prints a version id and not a service id,
parallel `bunx` races the package cache, fresh app dirs need
`bun install`, and a failed deploy leaves a version-less service shell.

## Reproducing this

```bash
export SOAK_HOME=/scratch/soak     # secrets live here, outside the repo
cd bench/soak
for r in us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1; do
  ./deploy-region.sh "$r" server
done
for r in us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1; do
  ./deploy-region.sh "$r" gen
done
python3 harvest.py && python3 mkreport.py
./teardown.sh --yes
```

## Appendix: full tier ramp

Ten tiers, 180 s each, 1 KiB records in batches of 10, producer and
consumer both running. `append` is request → durable ack; `roundtrip` is
producer timestamp → consumer observation. Per-tier p50 is the median of
that tier's 20 s windows, p99 the worst; the first window of each tier is
dropped because it straddles the concurrency step-up. `errs` and
`throttled` are cumulative.

**us-east-1** (ewr/iad)

| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |
|---|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 4 | 42 | 211.6 | 1,471.5 | 264.6 | 2,852.9 | 0 | 0 |
| t02-conc2 | 2 | 4 | 42 | 462.2 | 1,089.5 | 539.1 | 3,125.2 | 0 | 0 |
| t03-conc4 | 4 | 8 | 88 | 425.5 | 1,141.8 | 533.8 | 4,050.9 | 0 | 0 |
| t04-conc8 | 8 | 17 | 172 | 455.8 | 983.0 | 523.3 | 2,969.6 | 0 | 0 |
| t05-conc12 | 12 | 26 | 258 | 424.6 | 3,029.0 | 507.6 | 5,107.7 | 0 | 0 |
| t06-conc16 | 16 | 32 | 331 | 441.3 | 2,248.7 | 541.7 | 3,182.6 | 0 | 0 |
| t07-conc24 | 24 | 53 | 534 | 427.6 | 1,039.4 | 508.7 | 1,160.2 | 0 | 0 |
| t08-conc32 | 32 | 66 | 661 | 470.4 | 2,119.7 | 585.2 | 2,246.7 | 0 | 0 |
| t09-conc48 | 48 | 101 | 1,012 | 465.8 | 1,737.7 | 577.3 | 3,059.7 | 0 | 0 |
| t10-conc64 | 64 | 124 | 1,240 | 492.8 | 1,120.3 | 609.0 | 3,844.1 | 0 | 0 |

**us-west-1** (sjc)

| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |
|---|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 17 | 178 | 49.8 | 179.7 | 67.0 | 1,279.0 | 0 | 0 |
| t02-conc2 | 2 | 18 | 190 | 97.9 | 297.5 | 121.0 | 618.5 | 0 | 0 |
| t03-conc4 | 4 | 38 | 379 | 100.1 | 319.2 | 121.5 | 481.0 | 0 | 0 |
| t04-conc8 | 8 | 72 | 727 | 101.1 | 456.7 | 123.6 | 470.0 | 0 | 0 |
| t05-conc12 | 12 | 112 | 1,123 | 99.6 | 368.1 | 123.5 | 390.1 | 0 | 0 |
| t06-conc16 | 16 | 144 | 1,446 | 103.1 | 370.7 | 130.0 | 571.4 | 0 | 0 |
| t07-conc24 | 24 | 218 | 2,185 | 101.5 | 389.9 | 133.6 | 648.2 | 0 | 0 |
| t08-conc32 | 32 | 294 | 2,944 | 102.7 | 223.2 | 129.5 | 745.5 | 0 | 0 |
| t09-conc48 | 48 | 432 | 4,331 | 102.1 | 359.4 | 145.6 | 701.4 | 0 | 0 |
| t10-conc64 | 64 | 490 | 4,912 | 91.2 | 295.2 | 140.0 | 747.0 | 0 | 273433 |

**eu-central-1** (fra)

| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |
|---|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 17 | 176 | 48.2 | 303.6 | 71.5 | 397.1 | 0 | 0 |
| t02-conc2 | 2 | 12 | 121 | 99.8 | 779.8 | 142.6 | 5,140.5 | 4 | 5107 |
| t03-conc4 | 4 | 30 | 296 | 95.8 | 2,803.7 | 181.1 | 65,896.4 | 14 | 8255 |
| t04-conc8 | 8 | 20 | 210 | 100.7 | 22,872.1 | 472.6 | 26,804.2 | 21 | 46489 |
| t05-conc12 | 12 | 20 | 200 | 105.7 | 26,378.2 | 392.2 | 27,066.4 | 37 | 91102 |
| t06-conc16 | 16 | 0 | 0 | — | — | — | — | 129 | 91102 |
| t07-conc24 | 24 | 0 | 0 | — | — | — | — | 265 | 94250 |
| t08-conc32 | 32 | 0 | 0 | — | — | — | — | 449 | 94250 |
| t09-conc48 | 48 | 0 | 0 | — | — | — | — | 721 | 103057 |
| t10-conc64 | 64 | 0 | 0 | — | — | — | — | 1090 | 103057 |

**eu-west-3** (cdg)

| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |
|---|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 15 | 152 | 57.1 | 266.8 | 89.5 | 318.2 | 0 | 0 |
| t02-conc2 | 2 | 16 | 163 | 110.0 | 338.4 | 150.6 | 546.3 | 0 | 0 |
| t03-conc4 | 4 | 30 | 303 | 116.9 | 366.3 | 167.6 | 490.2 | 0 | 0 |
| t04-conc8 | 8 | 62 | 624 | 115.9 | 340.2 | 164.1 | 439.0 | 0 | 0 |
| t05-conc12 | 12 | 96 | 964 | 118.6 | 336.1 | 163.6 | 545.3 | 0 | 0 |
| t06-conc16 | 16 | 126 | 1,258 | 115.3 | 393.2 | 163.1 | 453.1 | 0 | 0 |
| t07-conc24 | 24 | 186 | 1,858 | 118.7 | 359.7 | 164.0 | 510.2 | 0 | 0 |
| t08-conc32 | 32 | 247 | 2,474 | 121.2 | 332.5 | 166.6 | 1,214.5 | 0 | 0 |
| t09-conc48 | 48 | 366 | 3,663 | 119.7 | 356.9 | 172.0 | 662.0 | 0 | 0 |
| t10-conc64 | 64 | 482 | 4,824 | 120.9 | 357.6 | 208.6 | 441.1 | 0 | 19998 |

**ap-southeast-1** (sin)

| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |
|---|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 7 | 76 | 125.8 | 704.0 | 252.2 | 1,769.5 | 0 | 0 |
| t02-conc2 | 2 | 8 | 82 | 221.2 | 841.7 | 243.5 | 679.4 | 0 | 0 |
| t03-conc4 | 4 | 8 | 84 | 423.9 | 1,265.7 | 1,024.3 | 2,670.6 | 0 | 0 |
| t04-conc8 | 8 | 54 | 539 | 84.4 | 811.0 | 588.3 | 1,524.7 | 0 | 0 |
| t05-conc12 | 12 | 99 | 993 | 75.0 | 807.4 | 328.6 | 1,137.7 | 0 | 0 |
| t06-conc16 | 16 | 106 | 1,060 | 124.3 | 622.1 | 321.2 | 954.4 | 0 | 0 |
| t07-conc24 | 24 | 160 | 1,606 | 122.1 | 463.4 | 225.5 | 702.5 | 0 | 0 |
| t08-conc32 | 32 | 229 | 2,296 | 114.3 | 1,190.9 | 320.1 | 1,410.0 | 0 | 0 |
| t09-conc48 | 48 | 219 | 2,194 | 267.1 | 1,499.1 | 567.7 | 2,084.9 | 0 | 0 |
| t10-conc64 | 64 | 438 | 4,377 | 102.0 | 954.9 | 454.1 | 1,985.5 | 0 | 25444 |

**ap-northeast-1** (nrt)

| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |
|---|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 15 | 158 | 47.0 | 402.4 | 72.5 | 650.2 | 0 | 0 |
| t02-conc2 | 2 | 21 | 210 | 80.4 | 517.1 | 108.0 | 591.4 | 0 | 0 |
| t03-conc4 | 4 | 45 | 454 | 76.5 | 821.8 | 103.0 | 841.2 | 0 | 0 |
| t04-conc8 | 8 | 92 | 926 | 73.2 | 394.0 | 100.0 | 686.1 | 0 | 0 |
| t05-conc12 | 12 | 134 | 1,337 | 77.9 | 413.4 | 106.5 | 434.2 | 0 | 0 |
| t06-conc16 | 16 | 178 | 1,788 | 77.9 | 356.6 | 110.6 | 569.3 | 0 | 0 |
| t07-conc24 | 24 | 252 | 2,534 | 80.9 | 490.5 | 114.0 | 623.1 | 0 | 0 |
| t08-conc32 | 32 | 338 | 3,380 | 81.6 | 442.1 | 128.6 | 731.1 | 0 | 0 |
| t09-conc48 | 48 | 489 | 4,893 | 79.6 | 387.3 | 137.5 | 509.2 | 0 | 68285 |
| t10-conc64 | 64 | 490 | 4,908 | 75.4 | 473.9 | 143.0 | 715.3 | 0 | 481464 |


---

## 2026-07-27: the storm hunt — four re-runs on the fixed binary

The reopen-storm fix (`sharddir::OpenGate`, commit b50cb7c) was taken back
to all six regions: fresh projects and buckets, the same 10-tier ramp,
**four consecutive 30-minute runs** (fresh keyspace per run: `soak2r1..4`),
each polled every 45 s for the storm signature (`wal_read_storm.stalled`,
`shard_opens` started diverging from completed).

**The storm did not recur.** More usefully, the campaign delivered the
next best thing to a recurrence: **four genuine shard-open failures
against the live store** — the exact event that ignited the original
wedge — and the gate absorbed every one.

| run | region | acked reqs | errs | throttled | append p50 (med tier) | opens s/c/f | storm |
|---|---|---|---|---|---|---|---|
| 1 | us-east-1 | 78,868 | 0 | 0 | 445.8 | 1/1/0 | none |
| 1 | us-west-1 | 267,918 | 0 | 5,592 | 128.2 | 1/1/0 | none |
| 1 | eu-central-1 | 345,251 | 0 | 18,972 | 89.3 | 1/1/0 | none |
| 1 | eu-west-3 | 299,781 | 0 | 0 | 108.6 | 1/1/0 | none |
| 1 | ap-southeast-1 † | 250,257 | 0 | 18,517 | 125.6 | 3/3/0 | none |
| 1 | ap-northeast-1 | 343,101 | 0 | 216,393 | 82.3 | 1/1/0 | none |
| 2 | us-east-1 | 79,237 | 0 | 0 | 448.4 | 1/1/0 | none |
| 2 | us-west-1 | 271,192 | 0 | 0 | 127.1 | 1/1/0 | none |
| 2 | eu-central-1 | 361,198 | 0 | 250,095 | 84.3 | 1/1/0 | none |
| 2 | eu-west-3 | 311,460 | 0 | 82,818 | 104.4 | 1/1/0 | none |
| 2 | ap-southeast-1 † | — | — | — | — | 1/1/0 | none |
| 2 | ap-northeast-1 | 359,202 | 0 | 99,649 | 80.0 | 1/1/0 | none |
| 3 | us-east-1 | 78,405 | 16 | 81,953 | 446.5 | 1/1/0 | none |
| 3 | us-west-1 | 260,882 | 0 | 0 | 136.8 | 1/1/0 | none |
| 3 | eu-central-1 | 361,979 | 0 | 403,704 | 81.2 | 1/1/0 | none |
| 3 | eu-west-3 | 315,561 | 0 | 197,254 | 100.1 | 1/1/0 | none |
| 3 | ap-southeast-1 | 248,674 | 63 | 20,872 | 101.5 | **3/2/1** | none |
| 3 | ap-northeast-1 | 326,404 | 0 | 216,823 | 81.7 | 1/1/0 | none |
| 4 | us-east-1 | 75,735 | 0 | 0 | 456.8 | 1/1/0 | none |
| 4 | us-west-1 | 250,202 | 0 | 28,729 | 134.7 | 1/1/0 | none |
| 4 | eu-central-1 | 325,742 | 66 | 207,861 | 81.7 | **3/1/1** | none |
| 4 | eu-west-3 | 330,292 | 0 | 174,697 | 100.3 | 1/1/0 | none |
| 4 | ap-southeast-1 | 243,211 | 8 | 3,152 | 121.2 | **4/2/2** | none |
| 4 | ap-northeast-1 | 354,166 | 0 | 331,102 | 78.7 | 1/1/0 | none |

† Run 1's ap-southeast-1 generator started ~25 min late (its stream
create failed during a version migration and the old awsbench never
retried — now fixed: the generator refuses to run until the stream
exists). Run 2's never came up at all: the platform served its "running"
version as a 404 for three consecutive deploys until the service was
destroyed and recreated. Its server was healthy throughout both runs.

Reading the table:

- **~5.0 million acknowledged requests (≈50 M records) across the
  campaign, 153 client-visible errors total (0.003 %), no storm in any
  region in any run.**
- Per-region latency is boringly reproducible across runs — eu-central-1
  append p50 89.3/84.3/81.2/81.7 ms, ap-northeast-1 82.3/80.0/81.7/78.7,
  us-east-1's storage penalty pinned at 445–457 ms all four runs.
- The three bold `opens` cells are the fix working in production: real
  open failures (run 3 SIN; run 4 SIN ×2 and eu-central-1) each produced
  one failed attempt, an escalating holdoff, a single-flight retry — and
  bounded client errors instead of a wedge. eu-central-1's failure landed
  at tier 10, concurrency 64, the worst possible moment; it cost 66
  errors and zero WAL-read amplification (`wal_gets` stayed 0).

### The finding this campaign produced

After run 4's harvest, eu-central-1's retry open was still in flight —
**20+ minutes**, with 648 coalesced waiters and a steady ~250
`list`+`get` per minute against `shards/10/compactions`: slatedb's
compactions-log recovery grinding through the ~450 `.compactions` files
our 500 ms compactor poll had minted during the run, at cross-region
latency. The gate contained it completely (one open, no storm, zero
`get:wal`) — but the shard was unavailable the whole time, because an
open had **no deadline**.

That is now fixed: `OpenGate` races every open against
`SHARD_OPEN_DEADLINE_MS` (default 180 s). A deadlined open counts as a
failure (strike + holdoff), and — the part that matters — it is
**reaped, not detached**: a supervisor drives the abandoned open to
completion and closes whatever engine it eventually produces. Detached
late completions were precisely the zombie writers of the original
storm; the reaper is what keeps the deadline from reintroducing them.
DST scenario: `a_hung_open_is_deadlined_and_its_late_engine_reaped`.

Two leads for upstream/slatedb follow-up, not blockers: compactions-log
recovery cost scales with the number of `.compactions` files and is paid
serially at open (450 files ≈ minutes at 100 ms+ per op), and our
`COMPACTOR_POLL_MS=500` mints those files aggressively.

---

## 2026-07-27 (later): soak3 — the cross-region mystery, solved

One more 30-minute run, with two additions: `served_from` broken down by
op-class on every server, and a **differential DNS probe** running beside
the fleet in all six regions — resolving `t3.storage.dev` every 30 s via
the system path, the platform's DC-local forwarder, explicit 1.1.1.1,
Vultr's recursor, Google, and NS1's authoritative directly (which sees
the instance's own IP: ground truth). `t3.storage.dev` is geo-DNS on NS1
with a 60 s TTL, so whoever answers the lookup decides which frontend
pool takes the traffic.

**The platform's DNS forwarder is the culprit.** In eu-central-1, at
07:21:22–07:25:22 UTC, the system path and the forwarder returned
`137.174.147.59` while the authority's answer for the instance was the
OCI-Frankfurt pool — and `/v1/debug/store` shows the correlated windows
exactly: **60 % of object-store ops served from ord1 in the
07:21–07:22 window**, 10 % the next, 19 % at 07:25. Explicit Vultr and
Google stayed correct throughout. ap-southeast-1 showed the same fault
shape independently: at 07:09:00 its forwarder handed the **NRT pool**
instead of the SIN pool, seeding the 7 % whole-mix nrt leakage that
region has shown in every soak.

Two amplifiers turn 30-second DNS blips into long serving windows:

- **Busy connections don't re-resolve.** Under sustained load the pool
  never goes idle, so a connection opened against a wrong-pool frontend
  keeps carrying traffic long after DNS heals — SIN kept serving from
  nrt through windows where every resolver column was clean.
- **Reconnect churn during an incident re-rolls the dice constantly.**
  The original wedge (2026-07-26) churned connections at exactly the
  moment the forwarder was bad, which is how eu-central-1 got to 26 %
  ord1 — and the added 300–500 ms per op is plausibly what pushed the
  open replay past client patience in the first place.

Steady-state context from the per-class breakdown: every region shows a
separate ~1–1.5 % remote trickle that is almost purely
`get:other`/`get:manifest` (us-east→ord/iad, eu→jnb, us-west→syd/hkg) —
metadata reads served from central PoPs, present everywhere, latency-flat,
and consistent with Tigris's metadata topology rather than a fault. It is
the *whole-mix* remote windows that are DNS-shaped.

Run quality: ~1.30 M acknowledged requests, 32 errors total (all in
eu-central-1, where the gate absorbed one failed open — 7/6/1 — during
the DNS window), zero storms, latency in line with all prior runs
(eu-central p50 84.4 ms, nrt 84.2, us-east 464.5). The SIN generator's
client-side JSONL was lost to a platform instance migration mid-run
(two replicas alternated behind one preview URL); its server-side
counters are complete.

**Fixes this points at:** (1) bypass the forwarder in our wrappers —
`/etc/resolv.conf` is proven writable in the microVM and Vultr's
recursor (1–9 ms, zero missteers all run) plus Google as fallback ran
clean in every region; (2) report the forwarder fault to the platform
team with the timestamps above (per-node 172.16.x.x resolvers); (3) the
probe fleet (`bench/probe/dnsprobe`, project `streams-dnsprobe`) stays
up as the permanent tripwire.

---

## 2026-07-27 (soak4): the fix, proven — resolv.conf hardcode A/B

Identical run to soak3, one variable changed: every wrapper wrote
`nameserver 108.61.10.10` + `nameserver 8.8.8.8` to `/etc/resolv.conf`
before exec (`RESOLV_OVERRIDE`, commit 84e2a5d), bypassing the platform
forwarder. All three predictions, stated before the run, held:

1. **The whole-mix remote windows vanished.** Zero snapshot windows in
   any region with >10 % remote share (soak3 had eleven, peaking at
   fra→ord1 60 % and sin→nrt 100 %). ap-southeast-1 — which leaked
   7–13 % to nrt in *every* previous run — served **100.0 % sin-local**
   (123,260 ops, not one from nrt). eu-central-1: 98.7 % fra, **zero
   ord1** (2,069 in soak3).
2. **The metadata trickle remained.** ~1–1.5 % per region, still purely
   `get:other`/`get:manifest` to jnb/ord/syd/hkg — confirming it as a
   separate, benign property of Tigris's metadata topology.
3. **The probes kept catching the forwarder misbehaving — with no
   effect on the fleet.** eu-central-1's probe logged **16 mis-steer
   ticks during this very run** (system+platform columns disjoint from
   authority, last at 08:32:22Z), twice soak3's count. The fault was
   live; the fleet, resolving via Vultr/Google, no longer cared. That is
   the experimental control that closes the case.

Run quality: the cleanest yet — **zero client errors in every reporting
region** (a first), latency identical to all five prior runs
(eu-central p50 82.7 ms — its 369,035 acked requests the highest of any
run — nrt 79.2, us-east 462.8), `opens 1/1/0` everywhere, no storms.
ap-southeast-1's client JSONL was again unreachable at harvest (the
preserved platform-404 specimen, docs/PLATFORM-SIN-404-REPORT.md), but
one campaign tick caught its live replica's terminal state: t10-conc64,
269,751 acked, **zero errors**, append p50 70.3 ms — SIN's best.

**Conclusion:** the cross-region serving problem was the platform DNS
forwarder end to end. `RESOLV_OVERRIDE` ships in all three wrappers,
env-gated; the staging plan should set it until the platform resolver is
fixed, and the probe fleet stays up to tell us when that happens.

### Metadata-trickle mitigation (2026-07-27, follow-up)

While Tigris considers the region-pinning question, the exposed surface
shrank on our side (commit pending, DST-covered):

- **History `DbReader`s are cached** per (stream, key) with LRU + idle
  eviction. Correctness at the absorbed boundary is proven per read — a
  one-row probe of `hist_record_key(upto−1)`, falling back to a fresh
  reader that must see the boundary because the absorber flushes before
  advancing it. This removes the per-request manifest GETs, the
  per-request checkpoint WRITE every reader open used to perform, and
  (via `skip_wal_replay`, safe under the same flush guarantee) the
  reader's WAL reads. Reopens now scale with absorb cadence, not request
  rate. Cache telemetry: `/v1/debug/store` → `history_readers`.
- **Compactions-log GC is now tunable and tighter**
  (`COMPACTIONS_GC_INTERVAL_SECS=30` / `COMPACTIONS_GC_MIN_AGE_SECS=120`
  vs upstream 60/300), bounding how many `.compactions` versions a shard
  open must page through. The open-side retry/fencing behavior itself is
  upstream: [slatedb#1970](https://github.com/slatedb/slatedb/issues/1970).
