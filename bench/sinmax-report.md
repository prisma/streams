# Single-region max-out: one Prisma Stream vs the AWS single-unit walls

**Date:** 2026-07-23 · **Region:** ap-southeast-1 (SIN) — chosen from the
6-region Tigris observatory as the best loaded-write region (hourly
16-concurrent 256 KB burst PUTs: 41 ms p50 / 140 ms p99, coldconn 10 ms).
**Setup:** fresh project `streams-sin-max`; one server instance
(standard ~1 GB Compute instance), one generator instance, same region;
Prisma Bucket (Tigris global) `user-z15tnqhou33wscrhy7do1szo`; single
stream, batched JSON appends (20 × 2 KB per request), durable-ack
(response only after the WAL PUT is durable in Tigris). The long-running
observatory fleet was untouched.

## Goals (user-set)

≥ 500 req/s · ≥ 10,000 events/s (with batching) · ≥ 20 MB/s ingested —
sustained, on ONE ordered stream. Latency: as good as possible.

## Headline result (run 15, 8.3 min continuously sampled, zero errors)

| metric | target | achieved (median of 20 s windows) | worst window |
|---|---|---|---|
| requests/s | ≥ 500 | **662** (peak 757) | 296 |
| events/s | ≥ 10,000 | **13,256** | 5,926 |
| ingest MB/s | ≥ 20 | **27.1** | 12.1 |
| ack p50 | — | **137 ms** | 267 ms |
| ack p99 | — | **386 ms** | 972 ms |
| errors / throttles | — | **0 / 0** over the whole run | — |

16 of 24 steady windows clear all three targets *simultaneously*; every
window's p99 stays under 1 s; the dips are compaction bursts, not errors.
The same configuration previously ran 70+ minutes of repeated 10-minute
load cycles without an error or a restart (run 10 marathon).

Configuration of record: `WAL_GROUP_COMMIT=1`, `WAL_FLUSH_GAP_MS=10`,
`FRAME_COMPRESS=1`, 16 MB L0 / 32 MB unflushed / L0_MAX_SSTS 64,
`COMPACTOR_POLL_MS=500`, `COMPACTOR_MAX_CONCURRENT=2`,
`SHARED_CACHE_BYTES=64M`, per-stream inflight 256, generator conc 96.
History absorption deferred during the run (see "Two operating modes").

## Updated comparison vs the AWS single-unit walls

| | Kinesis (1 shard) | SQS FIFO (1 group) | **Prisma (1 stream, this work)** |
|---|---|---|---|
| record ceiling | 1,197 rec/s (hard wall) | 3,582 msg/s (batch-10) | **13,256 ev/s sustained** (11× / 3.7×) |
| byte ceiling | ~1 MB/s (wall) | 5.4 MB/s | **27.1 MB/s sustained** (27× / 5×) |
| request ceiling | n/a (record-coupled) | 300 batch-tx/s | **662 req/s** |
| ack floor p50 | 7.3 ms | 7.0 ms | 131–137 ms (durable-in-object-store; see below) |
| ack p99 under full load | — | — | 386 ms |
| overload behavior | 100 % goodput, clean throttles | 84 %, clean throttles | **100 % goodput, graceful queueing** (retested) |
| cost @ 1k rec/s ordered | ~$48/mo | ~$350/mo | ~$73/mo (unchanged basis) |

On throughput, one Prisma stream now beats both AWS primitives on every
axis by 3.7–27×. Latency remains the one axis where AWS wins: their ack
floor is a replicated-in-RAM 7 ms; ours is a durable object-store PUT
(~25 ms Tigris write + serial-commit pipelining), landing at ~130 ms p50
under full load (≈65–90 ms lightly loaded with the new group-commit
pump). That is the structural trade of acking only on object-store
durability.

## Two operating modes (disclosed)

A Kinesis shard is a retention-bounded hot log — the comparison row runs
our stream the same way: appends land in the compressed, encrypted hot
log (fully readable, fully durable), with **history-tier absorption
deferred/paced** rather than racing peak ingest. Continuous live
absorption (a tier Kinesis does not have: per-stream queryable history
DBs) currently sustains ~300 req/s / ~7,000 ev/s / ~15 MB/s at p50
~120 ms on this instance size — the absorber's decompress-and-re-encode
pipeline competes for the same 1–2 vCPUs. Production guidance: pace
absorption behind load (it is designed to lag by design: `ABSORB_AGE`),
or size instances for concurrent absorb. Closing the remaining gap needs
SlateDB-internal work (SST builds off the async runtime) — upstreamable.

## What it took (change ledger, all committed to `slate`)

1. **Frame v3 — compress-then-encrypt** (`FRAME_COMPRESS=1`): zstd-1 on
   the record payload *before* AES-GCM (ciphertext can't compress).
   Cuts WAL, L0, compaction, absorber, and history bytes together; ~30×
   on padded bench data; killed the ~5–6× NIC amplification that
   saturated the instance. AAD-protected version byte; readers accept
   v2+v3; no migration.
2. **WAL group-commit pump** (`WAL_GROUP_COMMIT=1`): flush self-clocks
   on the in-flight PUT instead of a fixed tick (sequential append p50
   55→28 ms local A/B; on Tigris the loaded cadence self-clocks).
3. **History tier memory envelope**: upstream 512 MB unflushed default
   → 32 MB (OOM-killed 1 GB instances at ≥10 MB/s absorb).
4. **Absorber pass bounded by plaintext bytes** (compression made the
   raw-byte bound a ~30× memory landmine).
5. **Absorber keeps history DB handles open** (LRU 4 + 120 s idle):
   open/close-per-pass cost 1–2 s of manifest round-trips per pass and
   capped absorb below ingest — the backlog OOM spiral.
6. **Absorber decode/decrypt on the blocking pool + 4 MB history SSTs**:
   tokio timer p99 848 ms → the ack path starved behind cooperative
   CPU hogs; OS preemption restored it.
7. **Compactor knobs** (`COMPACTOR_POLL_MS`, `COMPACTOR_MAX_CONCURRENT`):
   upstream 5 s scheduling poll starves L0 drain at double-digit MB/s.

## Overload / goodput

"Goodput at overload" = when clients offer more than the ceiling, what
fraction of the normal ceiling still completes, and does the excess fail
clean (fast 429s) or dirty (hangs/timeouts). Kinesis holds 100 % because
its wall is enforced cheaply upstream; our earlier number was 78 %.
Retest at ~4× ceiling (conc 256): median **702 req/s achieved — 100 % of
the 701 req/s ceiling**. All throttles/errors confined to the first
(boot) window; steady overload windows show zero 429s and zero errors —
the excess concurrency is absorbed as bounded queueing (p50 137→335 ms,
p99 still < 1 s) rather than lost work. The old 78 % number was measured
against the pre-fix build whose overload path collided with the (since
eliminated) commit stalls; with the stalls gone, being overloaded no
longer costs capacity. For latency-SLO enforcement the per-stream
admission cap still sheds at its configured bound — this test sat
exactly at that boundary (conc 256 = cap), deliberately exercising the
queueing regime.

## Error-provenance note (Conduit forensics)

Every error class seen during tuning was attributed via the Compute
ingress (Conduit) ClickHouse logs, jointly with the Compute-side
session: 429s = our own admission tarpit + wedge sheds (since-fixed
stalls); 408s = our `APPEND_TIMEOUT=10s`; 502s = an in-guest platform
component fast-failing while our acceptor lagged (5-question handoff
filed); exactly one response-loss event campaign-wide. The final
validation runs are error-free end to end. Platform constants measured:
front-door kill = 30.0 s exactly; Conduit upstream-response wait ~15 s.

## Follow-ups

- Upstream SlateDB: move SST build (zstd+AES) off the async runtime;
  the residual throughput dips and the live-absorb gap both trace there.
- Client SDK: idempotent create + retry on 502/dead-socket; client
  timeouts > 15 s (platform verdict beats racing it).
- Teardown when done (server+gen+gen2 services, bucket, keys) — kept
  running for now alongside the observatory fleet.
