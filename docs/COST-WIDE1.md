# Wide test 1 — carrying streams vs serving them (1k / 10k / 100k)

**2026-07-28, local rig, slate @ 3a95377 + wide harness.** Method:
[COST-METHODOLOGY.md](./COST-METHODOLOGY.md) Test 2 — per regime: create
N streams, seed each with one 1 KiB record, then a 15-minute steady
window (100 active streams appending batch 10 every 500 ms ≈ 2,000
records/s; a scanner cold-reading random inactive streams at 2/s).
Regimes differ ONLY in total cardinality. All three ran clean: zero
errors, zero throttles, 1.8 M records appended each.

## The table

| | **1k** | **10k** | **100k** |
|---|---|---|---|
| create+seed | 2 s | 22 s | 214 s |
| setup Class A / stream | 1.46 | 1.20 | 1.31 |
| steady Class A | 221,201 | 290,413 | 294,537 |
| steady Class B | 114,976 | 150,460 | 156,301 |
| — shard tier A (active path) | 127,404 | 129,877 | 127,208 |
| — hist tier A (absorber) | 93,797 | 160,536 | 167,329 |
| — registry billable | **0** | **0** | **0** |
| Class A / M records | 122,899 | 161,349 | 163,640 |
| append p50 / p99 ms | 47.7 / 86.9 | 48.0 / 87.0 | 48.8 / 87.3 |
| scan p50 ms | **326** | 28 | 28 |
| inactive absorbed by end | 100 % | ~24 % | ~3 % |
| RSS max MB | 654 | 682 | 989 |

## What the regimes actually revealed

**1. The absorber has a hard per-stream ceiling: ~4.5 streams/s.**
Per-minute `hist/sst/put` deltas are flat at ~272/min in both w10k and
w100k from first due minute to last — the sequential per-stream pass
(open a per-stream history DB through an LRU of 4, write, flush,
advance) is the ceiling, independent of backlog size. Draining the
seeded backlog takes ~3.3 min at 1k, ~37 min at 10k, **~6.2 hours at
100k** — per instance, at ~43 Class A + ~27 Class B per pass for ONE
seeded record (manifest + compactions-state + GC LIST churn per DB
open dominates; the record itself is one SST PUT — 3,745 passes over
w10k's window carried 160,536 hist-tier Class A). At list prices that
is ~$215/M sparse records absorbed, against ~$0.02/GiB for
well-batched WAL ingest. This is the review's "per-stream history
architecture is the largest long-term cardinality risk", now with a
measured rate and price.

**2. The 15-minute KeyCache TTL makes a deep backlog unabsorbable.**
The absorber needs the stream key (decrypt shard frames, re-encrypt
history blocks); keys arrive per keyed request and expire after 900 s.
At 4.5 streams/s the absorber reaches ~4k streams per 15 minutes —
every seeded key beyond that expires before its turn, and those
streams stop absorbing until the next keyed request re-supplies the
key. Consequence: at wide cardinality the shard log becomes the
long-term home of sparse data (correct, never lost, but per-shard DB
size and GC surface grow), and absorption work re-arrives with the
next touch. Steady Class A therefore *plateaus* between 10k and 100k
(+1.4 %) — cardinality doesn't raise the 15-minute bill, it stretches
how long (or whether) absorption completes.

**3. The wide backlog is invisible to the scale-out signal.** With
~76 % (w10k) / ~97 % (w100k) of seeded streams unabsorbed, the
`absorb_lag_secs` observable read **0** for every listed stream, and
`/v1/debug/usage` truncates at 65,536 entries. One certain
contributor: the absorber's signal channel is a 65,536-capacity
`try_send` — at 100k, ~35k seed signals were silently dropped and
never became pending entries. The rest (w10k shows lag 0 with only
10k signals, well under capacity) needs its own investigation: either
the pending map or the lag pipeline loses the backlog. Either way the
rebalancer's victim signal cannot see exactly the load shape this
test creates. Follow-up filed.

**4. Round 1's registry conditional GETs held at cardinality: zero
billable registry requests in every steady window.** The registry
refresh volume at 100k (~118k conditional 304s across the run, all
free) would have been billable Class B before 3a95377 — this test is
the cardinality-side validation of that change.

**5. Reads of absorbed sparse streams cost ~330 ms; unabsorbed ones
28 ms.** w1k's scan p50 is 326 ms because its whole population
absorbed (every cold read = per-stream DbReader open: manifest GETs +
checkpoint PUT through an 8-entry cache facing a 900-stream working
set); w10k/w100k scan at 28 ms because most scanned streams still
live in the shard log. Per-stream history makes the *absorbed* state
the slow and expensive one for sparse readers — backwards from what
tiering should give.

**6. Memory scales with carried cardinality.** RSS max 654 → 682 →
989 MB (handles, registry cache, absorber pending, reader churn; the
wide-active workload also fills all four engines' 32 MiB rings, +96 MiB
vs the one-hot-shard soak). At the field posture (600 MB shed line on
1 GB instances) even the 1k regime sheds: a first attempt at exactly
field settings ran clean for 11 minutes and then shed ~23 % of append
requests (archived as the `shed600` datapoint). Wide runs therefore
raise the shed line to 1400 MB and report RSS instead; a 1 GB
instance would have shed in every regime.

## Setup cost, for the record

Creating and seeding costs ~1.2–1.5 Class A per stream (descriptor
PUT + a 404-probe GET on create, WAL share of the seed append, plus
early absorber passes that slip into the setup window). 100k streams
provisioned + seeded in 3.6 minutes at 64-way concurrency.

## Follow-ups (ordered)

1. **Absorber batching across streams** (review §6/§7): the ceiling is
   per-stream DB opens, not bytes. Shared/partitioned history DBs (or
   at minimum batched multi-stream passes with a bigger LRU) is the
   structural fix; a min-batch-per-stream threshold only defers work
   the TTL then cancels.
2. **Absorb-lag truthfulness**: bounded signal channel drops + lag=0
   on backlogged streams + 65,536-entry usage truncation — the
   scale-out signal must see (or at least count) the wide backlog.
3. **KeyCache TTL vs absorber**: decide the intended contract for
   sparse streams — never-absorb-until-touched is defensible but
   should be explicit (and the pending map shouldn't retry key-less
   streams every tick forever).
4. Scanner-visible cold-reader cost (~330 ms, checkpoint PUT per
   open) — the review's reader-cache concern, now quantified.
