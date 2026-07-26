# Deterministic Simulation Testing

## What DST is, and why it is worth the trouble

Deterministic Simulation Testing runs the system inside a simulator where
**every source of nondeterminism is controlled by a seed** — clocks,
randomness, task scheduling, I/O latency, and fault placement. Given the
same seed you get bit-for-bit the same execution. That buys two things
ordinary testing cannot: you can explore *thousands* of rare
interleavings quickly, and when one fails you can **replay it exactly**
instead of hoping it recurs.

The canonical treatment is TigerBeetle's, and it is worth reading before
touching this code:

- **[TigerBeetle: Deterministic Simulation Testing](https://docs.tigerbeetle.com/concepts/safety/#deterministic-simulation-testing)**
  — their VOPR ("Viewstamped Operation Replicator") simulates an entire
  cluster with faulty disks, partitioned networks and crashing replicas,
  accelerating time so a simulated run covers far more than wall clock
  would allow.
- **[TigerBeetle: "A Friendly Abstraction Over io_uring and kqueue"](https://tigerbeetle.com/blog/)**
  and their [SimTigerBeetle talk](https://www.youtube.com/watch?v=Vch4BWUVzMM)
  make the central argument: if the system is deterministic given a seed,
  a bug found once is a bug you can reproduce *forever*, and the
  simulator becomes a machine for manufacturing rare failures on demand.
- The wider ecosystem is catalogued at
  [awesome-deterministic-simulation-testing](https://github.com/ivanyu/awesome-deterministic-simulation-testing).
  FoundationDB pioneered the approach; Antithesis commercialises it.

The insight that matters for us: **distributed-systems bugs are usually
races and crash windows, not logic errors.** A logic error fails every
time and a unit test catches it. A race fails one run in fifty, at 3am,
under load — which is exactly the profile of the defects that cost this
project the most.

## Why we adopted it: what the integration ladder cost us

Task #53 (auto-scaling) was validated by a docker "ladder" of five rungs
run to two consecutive green passes, then a 4-instance Prisma Compute
cluster. It worked — but it took **14 passes, roughly 21 hours of wall
clock**, plus continuous supervision. Of seven product defects found:

| defect | class | unit-testable? | DST-catchable? |
|---|---|---|---|
| zombie `Db` after a shard move; its GC deleted live SSTs (**data loss**) | race | no | **yes** |
| in-flight work hung on a move | lifecycle race | no | **yes** |
| absorb-lag gauge froze after fencing | lifecycle | no | **yes** |
| split crashed between seal and map-save | crash window | no | **yes** |
| backpressure starved the lag signal | emergent under load | no | likely |
| scaler `owns()` checked the ring, not possession | logic | **yes** | — |
| absorb lag keyed by the wrong hash | logic | **yes** | — |

The two logic bugs now have unit tests (`src/fleet.rs`, `src/usage.rs`).
Everything else is timing-dependent, and several **hid for multiple
passes** because the race simply did not fire that run. That is the
argument for DST in one sentence: a 90-minute pass samples the
interleaving space once, badly.

## What we built

`src/dst.rs` — a self-contained harness that runs our **real**
`ShardEngine` against a seeded fault-injecting object store. No cfg
flags, no separate binary: it runs in `cargo test`.

### Components

**`FaultStore`** — an `ObjectStore` decorator wrapping any inner store
(tests use `InMemory`). Per operation it consults a seeded `StdRng` and
either passes through, injects latency, or fails with a retryable error.
Latency values model what we measured against real Tigris: 8–185 ms per
op across regions, with iad1 at 139–185 ms. Reads are never faulted — it
models a flaky network, not a lying disk.

It exposes `injected_latency` / `injected_errors` / `ops` counters so a
scenario can **assert that faults actually fired**. A fault store that
never injects is the DST form of a vacuous test, and this project learned
that lesson expensively (see `bench/docker/harness/README.md`).

**`AckLedger`** — the oracle. Records the payload sequence numbers a
workload received *durable acks* for, per routing key, then audits them
against what a reader actually drained:

| invariant | meaning |
|---|---|
| **I1** | no acknowledged record is unreadable |
| **I2** | per-key order is preserved (acks appear as an in-order subsequence) |
| **I3** | no duplicates |
| **I4** | at most one writer commits per shard — fencing is honoured |

I1 is precisely the property the Compute C3 investigation was about; I3
is the shape of the pass-2b at-least-once duplication.

**`drive_appends` / `drain_observed`** — the workload and reader. Appends
go through the real commit path and are recorded in the ledger **only
after a durable ack**, so the ledger is ground truth. The reader decodes
frames exactly as the absorber does (per-`(epoch, routing_key,
key_version)` subkey derivation).

### Scenarios

| test | what it establishes |
|---|---|
| `fault_schedule_is_reproducible_from_the_seed` | same seed replays identically; different seeds diverge — without this nothing else is DST |
| `faults_actually_fire` | the harness is not vacuous |
| `survives_injected_faults_without_losing_written_data` | the fault store corrupts nothing it claims to have written |
| `acked_records_survive_store_faults` | I1+I2+I3 for a single writer under injected faults, across seeds |
| **`acked_records_survive_a_fencing_handoff`** | **I1+I4 across a shard move.** Opening a second engine on the same prefix fences the first — exactly what the rebalancer does. Records acked by the old owner must remain readable through the new one. This is the class that produced the pass-3 zombie-GC data loss |
| `oracle_accepts_a_faithful_read` / `oracle_catches_loss` / `oracle_catches_duplicates` / `oracle_catches_reordering` | the oracle itself can fail — negative controls built from the real failure shapes we hit |

The oracle's negative controls are not ceremony. They are mutation tests
in spirit: we verified that disabling I1 detection makes
`oracle_catches_loss` fail, and restoring it makes all nine pass.

### It already earned its keep

The first run of `acked_records_survive_a_fencing_handoff` failed with
`I3 violated: key x has 20 duplicate record(s)`. The cause was a bug in
the *harness* — both phases wrote the same payload sequence range, so the
"duplicates" were self-inflicted. The oracle caught a mistake in the test
that a less strict harness would have silently accepted. Phases now write
disjoint sequence ranges (`seq_base`).

## Running

```bash
cargo test --release dst
```

Nine tests, a few seconds. To widen a sweep, add seeds to the arrays in
the scenario tests — each seed is an independent execution.

When a seed fails, it fails **reproducibly**: rerun with just that seed
and you get the identical interleaving, which is the entire point.

## Roadmap

Implemented above is step 1. The remaining steps are ordered by value,
and each is useful alone.

### Step 2 — make our own time and randomness injectable

The blocker for simulating *our* control loops is that we call the clock
directly: **73 `Instant::now()`, 46 `now_ms()`, 50 `sleep`/`interval`,
8 `rand`** across `src/`. Thread a clock handle (or adopt slatedb's
`SystemClock`) through `fleet.rs`, `scaler.rs` and `history.rs`.

The payoff is large. Our control loops are gated on a 60 s rebalance
threshold, a 3 s anti-flap holdoff, 600 s cooldowns and 2 s heartbeats —
that is *why* a ladder pass takes 90 minutes. With a mock clock a
30-minute soak becomes milliseconds and the threshold/cooldown
interactions become exhaustively explorable rather than sampled once.

### Step 3 — generalise fault points

We have two ad-hoc env hooks left over from the ladder:
`SCALE_FAULT_POINT=after_seal` (scaler.rs) and `ABSORB_PAUSE`
(history.rs). Replace them with named fail points — `after_seal`,
`before_map_save`, `after_map_save`, `during_fence`, `during_absorb`,
`before_ack` — selected by seed. `fail-parallel` is already in the
dependency tree via slatedb. This also removes test-only env vars from
the production binary.

### Step 4 — multi-instance simulation

Run N `AppState` instances in one process against one simulated store to
exercise ring formation, ownership handoff, and the possession-vs-ring
settling windows deterministically. This is the step that could have
caught the ring-convergence data loss without a cloud deploy.

## Reusing slatedb-dst

Our pinned SlateDB (`e255cff`, v0.14.1 + [PR #1964](https://github.com/slatedb/slatedb/pull/1964))
ships **`slatedb-dst`**, an upstream crate — not something we maintain,
so **adopting it does not deepen our fork**; when #1964 lands upstream
and we drop the patch, it remains available.

It offers, behind `#![cfg(dst)]`:

| slatedb-dst | maps onto |
|---|---|
| seeded deterministic current-thread runtime | replayable scheduling, not just replayable faults |
| `MockSystemClock`, `Harness::advance_time()` | step 2 above |
| `FailPointRegistry` (fail-parallel) | step 3 above |
| `FailingObjectStore` / `ToxicKind` (latency, bandwidth, reset-peer, slow-close, synthetic HTTP errors) | a richer `FaultStore` |
| `Harness::swap_db()` | a shard handoff, directly |
| `DbFencerActor`, `AuditorActor` (`tests/bank.rs`) | our fencing scenario and oracle |

We deliberately built `FaultStore` first rather than starting there: it
is dependency-free, needs no cfg gating, and proves the invariants are
expressible. Adopting slatedb-dst is the natural way to deliver steps
2 and 3 rather than reimplementing a mock clock and fail-point registry.

## What DST does **not** replace

Be honest about the boundary. Two of this campaign's most valuable
findings could not have come from simulation:

- **Ring-convergence data loss** (371,900 acknowledged records) was found
  by deploying to real Compute, where instances cold-start one at a time
  over minutes while the ring re-forms under load. Step 4 would model it
  — but only because we now know to look.
- **The 42× history-read regression** (84 → 3,528 rec/s) was a
  *performance* property of real network round-trips. A simulator with
  synthetic latency would not flag one-block-per-GET as wrong.
- Resource ceilings — 1 GB RSS, the ~48–50 concurrent platform edge
  slots, egress budgets — need the docker and cloud rungs.

So: DST replaces most of the integration ladder's value **for
correctness under concurrency and faults**, far faster and replayably.
Keep the docker rung for resource limits, and the cloud rung for platform
behaviour and performance.

## Related discipline

The ladder's hardest-won lesson — **"a rung that cannot fail proves
nothing"** — *is* the DST discipline of asserting invariants rather than
outcomes. D3 and D4 both passed their order checks for several passes
while never exercising their mechanism, until explicit assertions were
added. Every scenario here carries the same guard: the fault counters and
the oracle's negative controls exist so the suite cannot quietly become
decorative.
