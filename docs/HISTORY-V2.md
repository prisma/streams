# History v2 — shared, partitioned history (design anchor)

**Status: first implementation landed 2026-07-29** (same-day as the
design acceptance; see the scorecard below). New streams absorb into
the shared per-shard partition by default; legacy v1 streams
(absorbed > 0 without the v2 flag) keep the per-stream path and its
DST coverage (`force_v1` in tests). This page holds the design, the
acceptance gates, and the first Arm C measurements.

## Arm C scorecard (w100k: 100k one-record streams + 100 active, 15 min)

| gate | target | measured | verdict |
|---|---|---|---|
| history Class A (setup+steady) | ≤ 5,000 | **9,490** (was ~4.3 M implied v1) | ~2× over target, **~450× under v1**; 7,489 of it is GC LISTs on the partition DBs — the queued exact-candidate-GC work, not the layout |
| Class A per stream | ≤ 0.05 | **0.095** (0.020 excl. GC LISTs) | as above |
| history DB opens | O(shards) | 4 partitions, kept open | **pass** |
| per-stream checkpoint PUTs | 0 | 0 (no DbReaders) | **pass** |
| 100k drain | ≤ 15 min | **complete in-window** (backlog 0, deferred 0) | **pass** |
| append p50 / p99 | ≤ 5 % / 10 % | 47.8/85.9 vs 47.8/86.7 ms | **pass** |
| cold-read p50 | ≤ 2× unabsorbed | **55 ms vs 28 ms (1.96×)**; p99 83 vs v1's 331 | **pass** — the 321 ms cliff is gone |
| WAL request cells | unchanged | shard tier 125,149 vs 124,9xx | **pass** |
| errors / integrity | 0 / exact | 0 / exact | **pass** |
| RSS (1 GiB posture) | < 600 MiB | 961 MB on the wide rig (4×32 MiB rings etc.) | open — field-posture sizing pass still owed |

Steady total Class A across the arms: **685,647 (A: v1 absorb-all) →
272,791 (B: defer sparse) → 133,808 (C: v2 absorb-all)** — v2 absorbs
everything for half of what deferral-that-absorbs-nothing cost.

The two open items are exactly the already-queued follow-ups: GC LIST
cadence (79 % of v2's residual history cost) and field-posture memory
sizing. The design accepted below is otherwise implemented as written,
with one simplification: instead of a distinct `AbsorbedBatch` op, the
gather lane's per-stream `Absorbed{v2}` advances coalesce into one
committer batch — same one-tracker-write property, less machinery.
(Newtypes for the three hash roles remain TODO — the route/incarnation
distinction is currently enforced by field names and the set-once route
freeze, not types.)

## Why this is a blocker, not an optimization

Campaign 1 established four independent failures of per-stream history,
all with the same cause — one SlateDB and one reader/checkpoint
lifecycle per stream:

1. ~43 Class A requests to absorb a one-record stream (≈ $215/M sparse
   streams at public Tigris prices) — cost tied to stream cardinality,
   not data volume, which is incompatible with pricing on data
   processed and retained.
2. A per-stream pass ceiling (serial ~4.5/s; concurrent lane ~14.5/s)
   that pins both the bill and backlog completion — more concurrency
   only pays the same bill sooner.
3. The 900 s KeyCache TTL expires before deep backlogs are reached, so
   absorption can require the customer to touch the stream again.
4. Cold reads of absorbed sparse streams cost ~330 ms (per-stream
   DbReader open + checkpoint PUT) — the absorbed state is the SLOW
   state for sparse readers. This item folds INTO v2; it is not a
   separate cache project.

Interim policy until v2 ships (landed 2026-07-29):
`ABSORB_MIN_BYTES_FOR_AGE` (default 256 KiB) keeps tiny streams in the
shard log — already durable there, cheaper, and faster to read — with
`deferred_sparse_{streams,bytes}` reported separately from lag. Field
deployments on 1 GiB instances: `ABSORB_CONCURRENCY=2`.

**Arm B measured (w100k, 100 active, 15 min, vs the same binary
without the policy):** steady hist Class A 559,470 → 147,872 (−74 %),
total steady Class A −60 %; scan p50 27 ms with the ~330 ms
cold-history mode gone from the p99 entirely (28→30 ms p99 median);
RSS max 1,098 → 892 MB; append p50/p99 and error counts unchanged;
`deferred_sparse` reported 99,7xx streams / ~100 MB pending across the
four shards (after fixing the summary gauge to aggregate per shard —
the first version was last-writer-wins across the four absorbers and
reported a quarter of the truth). One behavioral note: a stream whose
pending crosses the gate mid-write can absorb a prefix and leave a
sub-gate residue in the shard log; the residue stays readable and
defers until it has volume — correct, but visible in absorbed < next.

**Gate 0 status:** the wedge liveness gate exists
(`bench/costab/wedge-liveness.sh`) and currently records the expected
**FAIL**: driven past the envelope at field posture it wedges (~10 min
at conc24), and 300 s after ALL load is removed the instance still
rejects every append with RSS frozen at ~647 MB — above the 600 MB
shed line, never draining. That non-recovery signature (soak7 sjc,
now scripted) is the bug to fix; v2 does not ship until this gate
passes.

## The design

**One history DB per shard ownership partition** (exactly one writer),
not per stream:

```
history/<shard-partition-or-generation>
```

Keyspace begins with the hash that determines ownership, so range
clone/split works when a shard divides:

```
<route16><incarnation16> r <offset>        -> encrypted frame
<route16><incarnation16> k <rk> <offset>   -> keyed index entry
```

Introduce newtypes — `RouteHash([u8;16])`, `IncarnationHash([u8;16])`,
`SegmentHash([u8;16])` — instead of bare `[u8;16]`. (Campaign
precedent: the absorb-lag observable was broken for its whole life by
exactly a name-hash/engine-hash confusion between bare arrays.)

**Store the existing encrypted frame unchanged.** The shard log already
holds compressed, stream-key-encrypted frames; the absorber copies
those bytes rather than decrypt → re-encrypt into a key-bound DB.
Consequences, each load-bearing:

- absorption no longer needs the customer key → the KeyCache TTL
  problem disappears, and background absorption works after restarts;
- the service still never persists customer keys;
- deterministic frame bytes make retries idempotent;
- compression is preserved (it happens before frame encryption);
- reads reuse the shard tail's frame decoder/decryptor.

**Flush many streams together.** The unit of work changes from
open-write-flush-advance per stream to: gather records across hundreds
of streams → one shared WriteBatch → one flush → one
`CommitOp::AbsorbedBatch { streams: Vec<(IncarnationHash, Offset)> }`
advancing every covered boundary in one shard commit. Trim stays
deferred and bounded as today; retries stay idempotent via the
submitted high-water mark.

**Read through the already-open partition.** The shard owner serves
history reads from its own open Db — no per-stream DbReader, no
per-stream checkpoint, no reader-cache churn, no 321 ms cold-open
cliff. A read-only reader (future detached read tier) would be one per
partition.

**Keyed indexing stays simple initially.** Keep the duplicate index
value for keyed streams if read performance needs it; unkeyed streams
store only the canonical record (as today, post-3a95377). A compact
postings index is a separate, later measurement-driven project. Do not
couple v2 to a new secondary-index design.

## The three-arm comparison (campaign 2)

| Arm | Behaviour |
|---|---|
| A | Current dedicated per-stream history |
| B | Defer absorption below a minimum byte threshold (the interim policy) |
| C | Shared history v2 |

Arm B doubles as the interim policy and answers whether the shard DB
can economically retain sparse populations outright. Campaign-1 data
already answers its read side (unabsorbed sparse reads are 28 ms vs
321 ms absorbed); its open question is shard-DB growth/compaction cost
as the sparse population accumulates.

Workload matrix beyond the current wide tests: 100k → 1M one-record
streams; a dense control with identical logical bytes in 10 streams; a
Zipf mix; 10/100/1000 active; reads before/after absorption; restart
after key expiry; fencing and movement; split/merge; overload then
load removal.

## Acceptance gates

Cost (the invariant: **at fixed logical bytes, stream count must not
materially move history request count**):

```
100k x one-record streams:
  history Class A                <= 5,000     (current ≈ 4.3 M implied)
  Class A per stream             <= 0.05      (current ≈ 43)
  history DB opens               O(shards), not O(streams)
  per-stream checkpoint PUTs     0
10x stream count, same bytes     <= +20% history Class A
history Class A per logical GiB  <= 2x batched WAL request cost
```

Performance and resources:

```
100k-stream drain                <= 15 min at ABSORB_CONCURRENCY <= 3
RSS on a 1 GiB instance          < 600 MiB
append p50                       <= 5% regression
append p99                       <= 10% regression
cold-read p50                    <= 2x unabsorbed control
WAL request cells                unchanged
errors                           0; decoded == acknowledged
```

The wide sweep — not a unit test — remains the memory gate (the first
concurrent-lane implementation reached 2.3 GiB RSS in seven minutes;
only the workload run caught it).

Correctness: keep the self-validating merged read (round 0's boundary
race: storage visibility and published handle state move at different
times; batched boundaries do NOT make that race disappear). Add
deterministic crash/fault points at: after shared write before flush;
after flush before AbsorbedBatch; after AbsorbedBatch durable before
publication; after publication before trim; during trim + concurrent
merged read; during fencing; during split/clone. At every point: acked
records readable; never `completed=true` across a gap; re-absorption
idempotent; no client-visible duplication; restart rediscovers every
unfinished boundary without customer keys.

## Rollout

No dual-writes (that doubles exactly the cost being removed). Persist
per-stream in the durable tail state:

```
history_layout   = dedicated_v1 | partitioned_v2
history_v2_from  = offset
```

Reads merge `[0, v2_from)` from v1, `[v2_from, absorbed)` from v2,
`[absorbed, next)` from the tail. New streams start on v2; existing
streams move future absorbed ranges only. Migrating old v1 history is
optional and later.

## Priority order around this project (round-2 verdict)

| Priority | Work |
|---|---|
| Gate 0 | Field-validate current changes (needs credentials); wedge liveness gate (`bench/costab/wedge-liveness.sh`) must pass before v2 ships |
| Main | Shared history v2 (this page) |
| Parallel quick win | Adaptive GC cadence → exact-candidate GC. *Cadence landed 2026-07-29 (fork `gc-adaptive-backoff`, GC_MAX_INTERVAL_SECS/HISTORY_GC_MAX_INTERVAL_SECS, default 600 s): empty sweeps back off, work snaps back, and the gate skips the per-tick expired-checkpoint manifest load. Verified on the capped 30-min soak vs the pre-v2 binary: Class B −31.7 % (manifest GETs: shard −34 %, hist −59 %), hist LISTs −17 %, latency and RSS improved. Shard LISTs barely moved — most originate outside the GC scheduler — so exact-candidate GC stays open as the structural fix.* |
| Before fleet mode | Fleet tick redesign (O(N) steady-state, 10 s stable heartbeat, conditional GETs; ≈ $1,286/mo per 32-instance cell as-is) |
| After | 5 s L0 timer → recovery budget |

Explicitly NOT next: more per-stream absorber concurrency, per-stream
cold-reader caching, per-stream arrival pacing, optimizing the
already-free registry 304s.
