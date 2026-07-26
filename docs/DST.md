# Deterministic Simulation Testing: what to adopt, and why

Status: proposal. Motivated by the task #53 validation campaign, where
14 docker ladder passes (~21 h of wall clock, plus supervision) found
seven real defects — most of them **races and crash windows** that a
seeded simulator explores in seconds.

## Why this is cheap for us

**Our own SlateDB fork already ships a DST harness** (`slatedb-dst`),
and we already pin that fork. It provides exactly the machinery this
would otherwise cost weeks to build:

| slatedb-dst gives us | maps onto our problem |
|---|---|
| seeded deterministic current-thread runtime | replayable failures (`seed` reproduces an interleaving exactly) |
| `MockSystemClock` + `Harness::advance_time()` | our 60 s rebalance threshold, 3 s anti-flap holdoff, 600 s cooldown, 2 s heartbeats — all become instant |
| `FailPointRegistry` (fail-parallel) | generalizes our two ad-hoc env fault points |
| `FailingObjectStore` / `ToxicKind` (latency, bandwidth, reset-peer, slow-close, synthetic HTTP errors) | the Tigris conditions our cloud rung exposed (8–185 ms/op, and iad1 at 139–185 ms) |
| `Harness::swap_db()` | **a shard move** — old owner's `Db` replaced by the new owner's |
| `DbFencerActor`, `SuppressFenced` | fencing, which is where our worst bug lived |
| `AuditorActor` (see `tests/bank.rs`) | our order-check invariant, run continuously instead of once at the end |

## What it would have caught

Of the seven product defects this campaign found:

| defect | class | would DST have caught it? |
|---|---|---|
| zombie `Db` after move; its GC deleted live SSTs | race (old owner's GC vs new owner's open) | **yes, quickly** — `swap_db` + store latency toxic |
| in-flight work hung on move | lifecycle at handoff | **yes** — fencer actor + auditor |
| absorb-lag gauge froze after fence | lifecycle | **yes** |
| split crash between seal and map-save | crash window | **yes** — this is literally a fail point; we hand-built one (`SCALE_FAULT_POINT=after_seal`) |
| backpressure starves the lag signal | emergent under load + slow store | **likely** — bandwidth toxic + clock control |
| scaler `owns()` ring-only | pure logic | no — but now unit-tested |
| absorb lag keyed by wrong hash | pure logic | no — but now unit-tested |

The two pure-logic bugs are now covered by unit tests (`src/fleet.rs`,
`src/usage.rs`). Everything else in that list is timing-dependent, which
is precisely the category where a 90-minute integration pass is both
slow and unreliable — several of those bugs hid for multiple passes
because the race did not happen to fire.

## Invariants (the oracles)

We already articulated these; the ladder checker asserts them once at
the end of a run. Under DST they become continuous assertions:

1. **No acknowledged record is unreadable.** Every append that returned
   2xx must be readable afterwards. (This is the property the C3 scare
   was about.)
2. **Per-key order is total and gapless** across splits, merges and
   moves — including the full segment lineage.
3. **No duplicates**, except where producer idempotence explicitly
   permits a retry to be absorbed.
4. **At most one writer commits per shard.** Fencing must make the
   losing writer's commits fail, not silently succeed.
5. **The segment map is always a total partition of `[0, 2^64)`** with
   no overlap (already unit-tested in `segmap.rs`; belongs here too as a
   post-condition after every simulated transition).

## Adoption plan (incremental, each step useful alone)

### Step 1 — invariant harness over one shard (days)

Write `tests/dst_shard.rs` using `slatedb-dst`: an append actor driving
records with producer idempotence, an absorber actor, a fencer actor
that swaps the `Db` mid-write, and an auditor asserting invariants 1–4
after every transition. Sweep seeds. This alone covers the fencing and
GC-race class that produced our worst defect.

### Step 2 — make our own time and randomness injectable (days)

The blocker for simulating *our* control loops is that we call the
clock directly: 73 `Instant::now()`, 46 `now_ms()`, 50 `sleep`/
`interval`, 8 `rand`. Introduce a small handle (or adopt slatedb's
`SystemClock`) threaded through `fleet.rs`, `scaler.rs`, `history.rs`.
Then the rebalancer, scaler, absorber and heartbeat loops can be driven
by `advance_time()` — a 30-minute soak becomes milliseconds, and the
cooldown/threshold interactions become exhaustively explorable.

### Step 3 — generalize fault points (hours, after step 2)

Replace `SCALE_FAULT_POINT` / `ABSORB_PAUSE` env hooks with named
`FailPointRegistry` points: `after_seal`, `before_map_save`,
`after_map_save`, `during_fence`, `during_absorb`, `before_ack`. The
seed selects which fire. Our D4 rung becomes one scenario among
hundreds, and the test-only env vars leave the production binary.

### Step 4 — multi-instance simulation (larger)

Run N `AppState` instances in one process against the shared simulated
store to exercise ring formation, ownership handoff and the
possession-vs-ring windows deterministically. This is the step that
would have caught the ring-convergence loss without needing a cloud
deploy.

## What DST does NOT replace

Be honest about the boundary — two of this campaign's most valuable
findings could not have come from simulation:

- **Ring-convergence data loss** was found by deploying to real Compute
  where instances cold-start one at a time over minutes. Step 4 would
  model it, but only because we now know to look.
- **The 42× history-read regression** (84 → 3,528 rec/s) was a
  *performance* property of real network round-trips; a simulator with
  synthetic latency would not have flagged the one-block-per-GET
  pattern as wrong.
- Resource ceilings (1 GB RSS, the ~50-slot platform edge, egress
  budgets) need the real docker/cloud rungs.

So: DST replaces most of the *integration ladder's* value for
correctness under concurrency and faults, and it is far faster and
replayable. Keep the docker rung for resource limits and the cloud rung
for platform behaviour and performance.

## Related discipline we already adopted

The ladder's hardest-won lesson — **"a rung that cannot fail proves
nothing"** — is the DST discipline of asserting invariants rather than
outcomes. D3 and D4 both passed their order checks for several passes
while never exercising their mechanism, until assertions were added
(`bench/docker/harness/README.md`). That instinct transfers directly.
