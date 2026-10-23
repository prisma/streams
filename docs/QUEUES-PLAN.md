# Native queues — build and verification plan

Status: **2026-10-23.** How the design in [QUEUES-DESIGN.md](QUEUES-DESIGN.md)
(revision 8) gets built and proven: the packages, Q0's go/no-go experiment,
and the verification of the first release. The cost goal and its benchmark
are in [QUEUES-PRICING-BENCHMARK.md](QUEUES-PRICING-BENCHMARK.md). Section
references (§) without a file name point into the design.

## 1. Packages

| Package | Contents | Estimate |
|---|---|---|
| Q0 Storage feasibility | §2: storage rows and guards, the collection pass, churn beside a co-tenant on Tigris, timer rotation, cold open, the pricing preliminary | 9-11 days |
| Q1 Engine | queue storage mode and every guarded consumer, the tail flag, queue clock, pages and rows, cursor, delivery rows and indexes, ready cache, claim, settle, backoff and retention rules, `maxInFlight`, `rate`, pause, settle bucket | 22-30 days |
| Q2 Queue resource | `kind: queue` and refusals, routes for `default`, scopes and clamps, dedup, scheduled messages, failed view, redrive, republish, discard, cancel, peek, audit, admission budget and redrive admission, reservations, the `deleting` fence | 10-14 days |
| Q3 Sweep and reclamation | the sweep and queue roster, gaps and lifetimes, hard expiry under load (1 million deliveries within 2 h under the trim budget while serving 1,000 msg/s), dedup expiry, retirement, deletion, the collection scheduler and trigger policy, queue delete lifecycle, accounting of the three quantities | 19-27 days |
| Q4 Execution | SDK API and runner, wake-up subscriptions (secrets, egress, re-arming), the bounded-drain handler for Compute and Workers, Workers CI | 12-16 days |
| Q5 Verification | §4, and the pricing benchmark's decisive run | 13-20 days |
| Q6 Ordered and named groups (after) | design §6.4, then build; §8.5; stream consumers onto the engine | after design |

**First release (Q0-Q5): about 85-118 engineering days**, before workload
migration. Q1 starts only when Q0 passes. Then Q1 → Q2 → Q3, with Q4 against
Q2's routes and Q5 alongside.

Costs to plan for: edits to `src/shard/transaction/append.rs`, the absorber,
the tail codec and the shard transaction core stale formal receipts (about
150 min serial per round); new key families go in new modules because
`src/queue.rs` and `src/product_cursor.rs` are by-path includes; routes live
in `src/product/queues/` because `src/product.rs` has no headroom; mutants
run at about 5 minutes each with a per-push cap, so each package lands in
several pushes. SPEC D17 and D23 describe a compactor service and
maintenance-free shard logs that the tree does not have; correcting SPEC is a
separate change.

## 2. Q0: storage feasibility (the gate for Q1)

**The question.** Can queue pages and rows live in the shard database, be
retired, deleted, compacted and garbage-collected on a quiet shard within the
completion deadline, at a cost and interference the pricing goal allows? Q0
answers only that. It builds no HTTP routes, SDK, claims, settles, leases,
dedup, failure or redrive: a harness drives the committer's WriteBatch with
synthetic `send`, `retire` and `delete` batches that write and delete exactly
the rows the design specifies.

**Where it lives.** An isolated experiment revision: branch `exp/queues-q0`
in a worktree under `~/.streams-k2/wt/q0`, never merged to `slate`. What
survives Q0 is its measurements and its decisions, written into the design;
Q1 then builds the production code red-first on `slate`.

**Stubbed in Q0:** the trigger policy (passes run on demand or on a fixed
schedule), the instance-wide pass permit (a mutex). **Deferred to Q3:** hard
expiry under load, dedup expiry, the full queue delete lifecycle (Q0 deletes
every row under a storage hash).

### 2.1 Build order

1. **Contract (½ day).** The design's revision 8 already writes the pass
   primitives, the completion rule (D22), the rotation bound and the trigger
   floor. Q0 confirms each against the pinned SlateDB before building on it.
2. **Storage rows and guards (2 days).** The harness: one-message pages,
   metadata, `A`, `X`, tombstones and the debt row, the tail flag and every
   guarded consumer of design §5.6. Gate: stored overhead.
3. **The collection pass (2-3 days).** Memtable flush → `Db::manifest()`
   boundary → spec → `Admin::submit_compaction` → poll the compactor state
   view and retry on overlap → HEAD each superseded SST until gone (except the
   D22 residual) → clear debt to `g`. In the deterministic runtime with the
   real GC intervals (600 s quiet, 300 s `min_age`): the quiet-shard case is
   the go/no-go; plus a restart at each stage boundary, an ownership move
   mid-pass, retirements during a pass, a scheduler job overlapping the pass,
   and a close mid-pass.
4. **Churn beside a co-tenant on Tigris (2-3 days).** `compute-1g`, the
   co-tenant baseline below, 10, 100 and 1,000 msg/s for 2 h each and one
   message a minute overnight. Derives the launch age, the minimum pass size,
   the per-shard bound, the queues-per-shard cap and the unit cost.
5. **Timer rotation (1 day).** `probe_debt` reads a roster deadline; a DST
   shows a sweep-opened shard evicted at its quantum and reopened within one
   rotation, and a resident shard served within 1 s.
6. **Cold open (½-1 day).** 10 million queue rows: open in ≤ 30 s, no tail
   repair.

### 2.2 The co-tenant baseline

The history tier is a separate database (`src/shard.rs:1895-1908`), so the
stable data a queue pass rewrites is the **shard log's resident set**: about
10,000 stream tails and their unabsorbed backlog of up to 60 s, with
ordinary Streams appends at Layer B's compliant rates driving the shard near
its working memory. That set is fixed before the runs and identical across
arms; rewrite amplification is measured against it.

### 2.3 Cases

| Case | Establishes |
|---|---|
| a few sends and retirements below the ordinary compaction thresholds, then silence | collection does not depend on later traffic |
| a large queue drains completely; separately, to one live message | empty and nearly empty queues reclaim without losing live work |
| every row under a hash deleted, then restart or ownership move | cleanup is found without a registry entry |
| retirements during a pass | a pass never erases newer debt |
| a message retired before pass selection and deleted after input selection, with restarts at each stage boundary | debt survives until a pass that covered the deletion completes GC |
| a size-tiered job overlapping the pass | the pass waits or retries and the GC watermark still advances |
| shard closed mid-pass | the compactor resumes the pass on reopen |
| churn beside the co-tenant baseline, up to the per-shard bound | rewrite amplification and co-tenant interference |
| one message a minute overnight, and idle hours | passes a day, fixed idle cost, quiet-shard behaviour |
| a scheduled deadline with zero debt on a sweep-opened shard | timers are served within the rotation bound |
| everything at `compute-1g` | memory, latency and maintenance progress fit the posture |

Workload shape, fixed before measuring: 100 B, 1 KiB and 64 KiB bodies;
writes in batches of 1 and 100; retire-and-delete in batches of 100; the
pricing scenario's message mix for the cost runs.

### 2.4 Measurements

Pass duration distribution, bottom-run size against duration, L0 count
during a pass and the L0 stall counter, `InvalidCompaction` retries, compactor
state and manifest churn, the GC LIST baseline, passes a day at one message a
minute, RSS peak during a pass, Class A and B requests, bytes into Tigris,
compaction rewrite bytes (the co-tenant's included), CPU, and the physical
bucket size sampled every 5 minutes.

### 2.5 Gates (any failure stops Q1 and reopens design §5 and §8)

| Measure | Pass |
|---|---|
| Physical convergence after silence | physical size ≤ the expected baseline (live data, metadata, recovery overhead) + 64 MiB per shard (the D22 residual included), and ≤ 1.25× live for live data over 1 GiB, within the completion deadline on a resident shard, with no new periodic LIST |
| Collection correctness | no debt cleared before its covering pass completes GC, at every restart point |
| No writer stall | zero L0 stalls (`l0_stall_count_num_ssts` = 0) during any pass |
| Stored overhead | ≤ 600 B per message at 100 B bodies; ≤ 1.5× at 1 KiB; ≤ 1.1× at 64 KiB |
| Steady churn | physical shard size ≤ co-tenant baseline + 2 × (queue live bytes + 1 h of queue churn) at 1,000 msg/s over 2 h, 50 queues |
| Latency during a pass | p99 queue batch commit ≤ 150 ms; the co-tenant's append p99 within 10% of its baseline; no backpressure |
| Memory | RSS under the shed line throughout, including the largest pass |
| Timer service | ≤ 1 s on a resident shard; within one rotation (`ceil(owned ÷ 8) × 300 s` + open) on a sweep-opened one |
| Cold open | 10 million queue rows open in ≤ 30 s, no tail repair, roster resumes |
| Cost preliminary | marginal cost of the pricing scenario at 10× ≤ $0.238 per million messages (pricing benchmark §8) |
| Per-shard bound | set from the measured pass budget and working set; the queues-per-shard cap reported for D21 |

## 3. After Q0

Q0's numbers go into the design (launch age, minimum pass size, per-shard
bound, default budgets for D21, the D22 residual) and the pricing benchmark's
estimate. The owner reads the verdict before Q1 starts (performance and cost
acceptance are the owner's).

## 4. Verification of the first release (Q5)

- **TLA+**, two models within CI's formal budget (bound today: TLA-002 at
  40-50 min). **`QueueDelivery`**: queue clock with clock steps and moves,
  availability index, cursor, delivery rows and indexes, fencing and replayed
  settles, bounded release, the sweep's expiry tasks, failure, redrive,
  republish, cancel, pause, crash/restart. **`QueueReclamation`**: retirement
  by every path with restarts between retirement, deletion and collection;
  debt generations with retirements during a pass; queue delete across a
  restart; the `deleting` fence; redrive admission; the last-rows rule; group
  creation (§8.5). Invariants: no message lost; no send at or below a cursor;
  a replayed settle changes nothing; nothing outlives `hardExpiresAt`; no
  message retires twice and accounting moves exactly once; nothing open or
  ahead of a cursor is deleted; every retired message is eventually deleted
  and every deletion's debt is cleared only after a compaction and GC that
  covered it; undone messages never exceed the admission budget; no send or
  redrive is accepted after `deleting`; ready work is claimable while
  `maxInFlight` allows.
- **Kani**: codecs (big-endian order), the id order, lease tokens with group
  generation, backoff and lifetime arithmetic, the tail flag decoder.
- **DSTs**: the colleague's evidence lists (recoverable failure without keys,
  delayed backlog not hiding ready work, rollover, replayed settles, a noisy
  queue not stopping other tenants, an unconsumed queue freeing its budget,
  hard expiry fencing a long-extended lease, explicit retry past retention,
  dedup expiry on a quiet queue, wake-up re-arm after drain, restart and
  move) and the storage-mode guards: an absorber tick gathers nothing from a
  queue; a queue tail is never a repair candidate; queue traffic does not
  mask a stalled absorber; a cold queue shard is not absorption debt; a
  deleted queue's rows are gone after restart and move; a queue roster
  resumes the sweep on a new owner; double-retirement attempts move
  accounting once; a redriven message retries after the original retention
  end; redrive past the budget stops at capacity; sends and redrives racing a
  delete are refused; a drained receiver is woken again with no external
  request; a delayed message on a shard with zero debt is served after the
  sweep evicts the shard; an empty claim writes nothing. Plus clock steps,
  mixed-success batches, crash windows, claim read failures, release loops,
  reservations and egress refusals.
- **SDK**: runner and wake-up handler against the real server; Workers in CI,
  including a drain that outlasts the acknowledgement.
- **Layer B queue arm**: queue workers, a poison tenant, a send flood and a
  collection pass under the shared-cell bars.
- **Pricing benchmark**: the decisive run (pricing benchmark §6); the goal is
  MET or the first release is not priced at parity.
- **Pilot**: `cp-plain-support-sync` beside the old queue for a week; every
  message the old queue processed is processed by the new one (by business
  id), none lost, failures match, p99 send-to-start within the old queue's.
