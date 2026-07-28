# Wide test 2 — the active axis (10 / 100 / 1000 active per cardinality)

**2026-07-28, local rig, slate @ 31b6cf4.** Same method as
[COST-WIDE1.md](./COST-WIDE1.md) ([methodology](./COST-METHODOLOGY.md)
Test 2), now sweeping the ACTIVE-set size at every cardinality with
**total workload held constant**: 200 append req/s ≈ 2,000 records/s in
every run — 10 active × 50 ms interval, 100 × 500 ms, 1000 × 5000 ms.
Only how many streams the same load is spread across changes. Nine runs
(3 cardinalities × 3 active levels; the a100 column is WIDE1's), all
clean: zero errors, zero throttles, 1.8 M records each. The absorber
gradient is deliberate: 10 active = byte-triggered passes, 100 ≈ 1.7
age-triggered passes/s (under the ~4.5/s ceiling), 1000 ≈ 16.7/s
demanded (3.7× over it).

## The matrix (steady 15-minute window)

**Class A requests:**

| streams \ active | 10 | 100 | 1000 |
|---|---|---|---|
| 1k | 199,328 | 221,201 | 254,407 |
| 10k | 287,250 | 290,413 | 287,773 |
| 100k | 287,681 | 294,537 | 296,089 |

**Append p50 ms (medians of 20 s windows):**

| streams \ active | 10 | 100 | 1000 |
|---|---|---|---|
| 1k | 37.4 | 47.7 | 58.4 |
| 10k | 37.0 | 48.0 | 58.8 |
| 100k | 36.5 | 48.8 | 58.7 |

RSS max MB: w1k 716/654/595, w10k 679/682/815, w100k 1047/989/940.
Scan p50 stays bimodal as in WIDE1 (~330 ms absorbed / ~28 ms
shard-log); w1k-a1000 has no scanner (zero inactive streams).

## Findings

**1. Active-set size is a pure latency knob at constant throughput.**
Append p50 climbs 37 → 48 → 59 ms (p99 68 → 87 → 91 ms) with active
count at *every* cardinality — while `shard/wal/put` stays flat
(89–101 k across all nine runs). Spreading the same load across more
streams doesn't buy more WAL PUTs; it degrades **arrival alignment**:
10 hot streams at 50 ms intervals ride the pump's self-clocked flush
cycle (and its post-ACK gather), 1000 streams at 5 s intervals arrive
Poisson-style and each waits ~half a WAL interval. Cost is unchanged;
latency is the price of sparseness per stream. (Corollary: the
narrow soak's 32 ms p50 is the aligned-closed-loop best case.)

**2. Cost responds to the active mix only below the absorber ceiling.**
At 1k total streams (backlog clears in minutes, absorber has headroom)
the bill tracks pass count: +28 % Class A from a10 to a1000, driven by
per-DB-open churn — `hist/meta/list` (the GC scans each history-DB
open triggers) doubles from 50,887 to 99,323, and `hist/sst/put` rises
2,274 → 3,171 as one big pass per stream becomes many tiny ones. At
10k and 100k the matrix rows are FLAT (±3 %): the absorber is
saturated at ~4.5 passes/s by the backlog regardless of the active
mix, so the 15-minute bill is pinned at the grind ceiling — the same
ceiling that stretches backlog completion toward never (WIDE1 §2).
The absorber is simultaneously the cost cap and the completion
bottleneck.

**3. The fixed grind tax amortizes across active streams.** Class A
per active-stream-minute at 10k: 1,915 (a10) vs 194 (a100) vs 19
(a1000). A deployment with few active streams over a wide keyspace
carries the entire absorber+maintenance tax on those few streams'
economics.

**4. The absorb-lag observable is now proven broken outright.** WIDE1
left room for KeyCache-TTL and signal-overflow explanations. w1k-a1000
closes them: 1000 streams with permanently fresh keys (a request every
5 s), absorber demanded at 3.7× its ceiling for 15 minutes — a real,
growing backlog of *active* streams — and `absorb_lag_secs` still read
**0 for every stream** at run end. The rebalancer's victim signal
cannot see any of the backlog shapes this harness creates. This is the
hardened top observability follow-up.

**5. RSS inverts with active count where the absorber has headroom.**
w1k: 716 MB (a10) vs 595 MB (a1000) — few-active means big absorb
passes (4 MiB memtables, compaction work) per history DB; many-active
means the data mostly *stays in the shard log* because the absorber
can't reach it. w100k-a10 is the fleet-wide peak (1,047 MB): full
backlog grind plus big active passes. Every cell is above the 600 MB
field shed line (WIDE1 §6).

## Addendum 2026-07-28 — follow-ups implemented and verified

The top three follow-ups landed the same day (absorber concurrent small
lane + per-tick caps, handle re-discovery sweep, lag-join fix +
`absorb_backlog` aggregate, key-missing backoff) and were verified by
rerunning w10k-a100 and w100k-a100 on the new binary:

| | w10k before | w10k after | w100k before | w100k after |
|---|---|---|---|---|
| absorb passes/min | ~272 | **~870 (3.2×)** | ~272 | **~870** |
| inactive absorbed in-window | ~24 % | **~99 %** (91 left, max 64 s lag) | ~3 % | ~13 % |
| absorb-lag observable | 0 everywhere | truthful | 0 everywhere | **86,947 lagging / max 1,038 s** |
| append p50 / p99 ms | 48.0 / 87.0 | 48.0 / 87.6 | 48.8 / 87.3 | 47.8 / 86.7 |
| errors / throttles | 0 | 0 | 0 | 0 |
| RSS max MB | 682 | 966 | 989 | 1,098 |

The w100k aggregate arithmetic closes the loop on §4: 99.9k seeded −
~13k absorbed ≈ 86.9k lagging — the sweep finds even the ~35k streams
whose signals the bounded channel dropped, and the aggregate is immune
to the 65,536-entry listing cap (52,483 of the lagging streams appear
in the per-stream list; the aggregate carries the truth).

Trade-offs, stated plainly: steady-window Class A roughly doubles
(290k → 608k at 10k; 295k → 686k at 100k) because 3.2× more per-stream
passes complete per window at the unchanged ~43-Class-A-per-stream
price — total cost to drain a given backlog is the same, it just
finishes 3.2× sooner. The per-stream history price itself (the $215/M
economics) is untouched; partitioned history remains the structural
fix. RSS rises with the concurrency (bounded: per-tick lane caps +
chunked eviction keep open DBs ≤ LRU + chunk; the first, uncapped
version grew 2.3 GB in seven minutes and is why those caps exist) —
1 GB field instances should run `ABSORB_CONCURRENCY` at 2-3, not the
default 6. w10k's scan p50 moving 28 → 321 ms is the completion showing
up on the read side: the scanned population is now absorbed, and every
cold read pays the per-stream reader open — follow-up 4 unchanged.

One correction to §4 as originally written: the REBALANCER was never
fully blind — the heartbeat's `absorb_lag_max` reads the lag map
directly and always worked for signal-delivered streams. What was
broken: the per-stream `/v1/debug/usage` join (usage counters key by
name hash, lag by engine hash — never matched), and streams whose
signals were dropped were invisible to everything. Both fixed.

## Implications for the follow-up list

Unchanged in order, sharpened in evidence: (1) absorber batching /
partitioned history — the ceiling is DB-opens, and finding 2 shows the
bill and the backlog are both pinned by it; (2) absorb-lag
truthfulness — finding 4 removes every benign explanation; (3) the
KeyCache-TTL contract for sparse streams; (4) cold-reader cost. The
active-axis data adds one design note: arrival alignment (finding 1)
means any future per-stream batching/pacing work should measure
latency at the 1000-active shape, not the closed-loop soak shape.

Raw tables: `bench/costab/wide-report.py <a10-out> <a100-out>
<a1000-out>` per cardinality; nine run dirs archived in the session
scratchpad.
