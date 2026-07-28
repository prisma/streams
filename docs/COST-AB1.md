# Cost A/B 1 — first cuts from the object-store request review

**2026-07-28, local twin of the field soak** (`bench/costab/`): s3lite at
25 ms per op, the soak7 server environment (gather 6 ms, ring 32 MiB,
4 shards, absorber 4 MiB/60 s), the field generator binary
(`awsbench`, batch 10 × 1 KiB, consumer on), tiers `1,2,3,4 × 450 s` =
30 minutes per run. Field deployment was not available (soak7's teardown
reclaimed the campaign projects and the artifact bucket), and for
request *counts* the local substrate is the sharper instrument anyway:
every physical request lands in one process's ledger, cumulative, by
tier/kind/op/status. Latency figures here are local and only guardrails;
the field envelope is unchanged by construction (see "what did not
move").

Two changes from the review, A/B'd against `slate` @ `583ec28`
(baseline) vs the same plus this commit:

1. **Unkeyed records no longer get a `k!` index copy in history**
   (review §"empty routing-key duplicate"). The index is a full payload
   duplicate; for unkeyed workloads it doubled history bytes. Empty-key
   filtered reads are served from the primary `r!` range and filtered
   (`read_history`), so the API is unchanged — DST-pinned by
   `empty_key_records_skip_the_index_copy_but_still_filter_read`.
2. **Registry TTL refreshes are conditional GETs** (review Immediate
   #3). The descriptor cache keeps the object ETag and revalidates with
   `If-None-Match`; unchanged descriptors — the overwhelmingly common
   case — come back 304, which Tigris does not charge. Pinned by
   `ttl_refresh_of_unchanged_descriptor_is_a_free_304`.

Both runs: **zero errors, zero throttles, decoded == acked×10**
(baseline 1,388,920 records, after 1,382,400 — −0.5 % run-to-run
noise).

## Requests (s3lite physical-request ledger, whole 30-minute run)

| rollup | baseline | after | delta |
|---|---|---|---|
| total Class A | 81,730 | 79,367 | **−2.9 %** |
| total Class B | 49,490 | 37,733 | **−23.8 %** |
| hist Class A | 10,502 | 8,291 | **−21.1 %** |
| hist Class B | 26,807 | 16,293 | **−39.2 %** |
| registry Class B | 1,238 | **0** | **−100 %** |
| shard Class A | 71,226 | 71,074 | −0.2 % (noise) |
| shard Class B | 21,445 | 21,440 | ±0 |

Notable cells (2xx counts):

| cell | baseline | after | delta |
|---|---|---|---|
| hist/sst/put | 1,593 | 1,047 | −34 % |
| hist/sst/get | 15,075 | 6,616 | **−56 %** |
| hist/manifest/put | 1,092 | 648 | −41 % |
| hist/compactions/put | 2,022 | 1,572 | −22 % |
| hist/meta/list | 5,843 | 5,031 | −14 % |
| registry/meta/get 2xx→304 | 1,238 → 0 | 0 → 1,239 | all free |
| shard/wal/put | 55,749 | 55,588 | −0.3 % |

Bytes at the store: put 1.34 → 0.96 GB (**−28 %**), get 1.10 → 0.76 GB
(**−31 %**), end-of-run objects 5,309 → 4,648. RSS max 525 → 474 MB —
half the history bytes is also less compaction and cache pressure.

## What did not move (guardrails)

Per-tier medians of the 20 s windows — p50 within 0.3 ms, p99 within
1 ms, throughput within 0.7 %:

| tier | p50 b/a (ms) | p99 b/a (ms) | records/s b/a |
|---|---|---|---|
| conc1 | 31.9 / 31.9 | 36.7 / 36.8 | 311 / 312 |
| conc2 | 32.0 / 32.0 | 39.2 / 38.9 | 622 / 620 |
| conc3 | 32.1 / 32.3 | 41.9 / 41.7 | 926 / 922 |
| conc4 | 32.4 / 32.7 | 43.1 / 42.2 | 1,225 / 1,217 |

The shard ingest path (WAL PUT count, shard manifest/compaction
traffic) is byte-identical between runs, as it must be — neither change
touches it.

## Reading the totals honestly

Total Class A only moves −2.9 % because this soak is a **hot ingest**
soak: 68 % of its Class A is WAL PUTs, which are already amortized by
the gather and are untouched here. The history-tier savings scale with
absorbed volume (−21 % Class A, −39 % Class B, −50 % logical bytes for
unkeyed workloads); the registry saving scales with stream-descriptor
poll traffic, which in a many-streams deployment is a per-stream ×
per-instance × 12/minute standing cost. The review's remaining
timer-driven items (fleet tick, 5 s L0 flush ticker, GC LIST cadence,
compactions-state churn) are invisible in this workload by construction
— fleet coordination is off in the field soak env, and a hot stream
flushes on bytes regardless — and stay open.

## Findings along the way

- **The field tier ladder wedges a local rig, reproducibly.** At local
  speeds (no edge RTT), conc16 ≈ 42 MB/s of plaintext into one stream's
  absorber — past the validated single-stream envelope. Both attempts
  wedged the same way: the history DB flush stalls with the imm
  memtable pinned at ~35 MB > `max_unflushed_bytes`, slatedb applies
  permanent backpressure, absorb backlog drives RSS through the 600 MB
  shed line, and the stream 429s **without recovering even after load
  stops** (14 minutes of shed in attempt 1). This is the soak7 sjc
  stall shape, on demand, locally — worth its own investigation.
- Batch deletes surface in the ledger under `other/meta/delete`
  (~57 k per run, free class): WAL GC delete volume is large but
  costless on Tigris; its LIST cost lands in `shard/meta/list`
  (12.9 k per run, Class A — a follow-up target).

## Follow-ups (in review priority order)

1. Fleet tick: 1 PUT + 2 LISTs + N GETs per 2 s per instance when
   `FLEET_PREFIX` is set — adaptive cadence needs care with the
   liveness window and scaler coupling.
2. 5 s L0 flush ticker → recovery budget (WAL count/bytes/age) for
   sparse shards.
3. GC LIST cadence / exact-candidate GC (`shard/meta/list` +
   `hist/meta/list` ≈ 18.8 k Class A per 30 min here).
4. Compactions-state churn (3.2 k Class A per 30 min across tiers).
5. The wedge: non-recovering absorber backpressure under saturation.
