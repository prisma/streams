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
| history Class A (setup+steady) | ≤ 5,000 | **9,490** at first landing → **1,203** after the LIST-free GC round (docs/COST-CAMPAIGN-2.md; hist LISTs 7,489 → 128) | **pass** |
| Class A per stream | ≤ 0.05 | 0.095 → **0.012** | **pass** |
| history DB opens | O(shards) | 4 partitions, kept open | **pass** |
| per-stream checkpoint PUTs | 0 | 0 (no DbReaders) | **pass** |
| 100k drain | ≤ 15 min | **complete in-window** (backlog 0, deferred 0) | **pass** |
| append p50 / p99 | ≤ 5 % / 10 % | 47.8/85.9 vs 47.8/86.7 ms | **pass** |
| cold-read p50 | ≤ 2× unabsorbed | **55 ms vs 28 ms (1.96×)**; p99 83 vs v1's 331 | **pass** — the 321 ms cliff is gone |
| WAL request cells | unchanged | shard tier 125,149 vs 124,9xx | **pass** |
| errors / integrity | 0 / exact | 0 / exact | **pass** |
| RSS (1 GiB posture) | < 600 MiB | honest footprint peaks 820 MB at full perf knobs (the old 961 MB ps-RSS overstated by the reusable-page gap); absorption is NOT the driver — rings + 100k metadata + caches are (COST-CAMPAIGN-2 §5) | measured; 100k-wide-per-1GiB needs trimmed knobs + the (now provably recovering) shed, or fleet splitting |

Steady total Class A across the arms: **685,647 (A: v1 absorb-all) →
272,791 (B: defer sparse) → 133,808 (C: v2 absorb-all)** — v2 absorbs
everything for half of what deferral-that-absorbs-nothing cost.

The two open items are exactly the already-queued follow-ups: GC LIST
cadence (79 % of v2's residual history cost) and field-posture memory
sizing. The design accepted below is otherwise implemented as written,
with one simplification: instead of a distinct `AbsorbedBatch` op, the
gather lane's per-stream `Absorbed{v2}` advances coalesce into one
committer batch — same one-tracker-write property, less machinery.
(Newtypes landed 2026-07-29: `crypto::RouteHash` / `crypto::SegmentHash`
type the v2 keyspace functions, `read_history2`, and the usage-link join
— the two seams where a bare-`[u8;16]` swap had already caused or could
silently cause a measured bug. Engine internals keep bare arrays;
conversion happens at those boundaries.)

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

**Gate 0 status: PASS** (2026-07-29, slate a105408). The historical
FAIL was the shed line reading memory gauges that structurally cannot
decrease: macOS rss_bytes() returned the getrusage PEAK, Darwin keeps
mimalloc's MADV_FREE_REUSABLE pages in resident_size, and an idle
process never runs mimalloc's allocation-path purges. With the honest
gauge (task_vm_info.phys_footprint) plus a forced mi_collect whenever
the reading is over the line, the original wedge is unreproducible —
the same conc24 load runs at a throttled equilibrium (footprint flat
~285 MB; ps-RSS ~490 — the gap is OS-reclaimable pages) and never
collapses. The gate now counts sustained shedding as its overload
precondition and PASSES: overloaded at t=152 s against a 280 MB line,
load removed, probes 429 at fp=294 MB, then five consecutive
successes as the footprint drains through 272 MB — recovered 197 s
before the deadline. Linux/musl semantics (statm + MADV_DONTNEED)
validate with the deferred field batch.

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

**Implementation status (round 3):** the shipped mechanism is the
boolean `history_v2` flag plus the zero-route guard (streams without a
name-level route stay v1 so future route-range splits cannot
misclassify them); the cutover-offset scheme below remains the design
for migrating deployments that hold REAL v1 history. None exist today
— every production-bound deployment is greenfield-v2 — so the offset
machinery is deliberately deferred until a migration actually needs it.

**Before physical range splitting (round-4 note):** the durable
maintenance index (dirty/trim markers) lives under an all-0xFF sentinel
that sorts OUTSIDE every stream's route range — correct for whole-shard
ownership handoff (the new owner opens the same shard DB and scans
once), but a key-range split cannot carry a stream's marker into the
child range. Before splits land, the index needs a route-local
representation or its own small tracker partition, and the all-FF keys
should stop riding hot write batches (they currently widen L0 SST key
ranges by construction, a compaction-overlap tax accepted for now).

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
