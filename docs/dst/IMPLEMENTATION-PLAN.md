# Prisma Streams DST Expansion — Implementation Plan

This plan turns `DST-EXPANSION-SPEC.md` and `SCENARIO-CATALOG.md` into an ordered set of implementation changes. It assumes the pre-launch clean-cutover posture and does not allocate work to compatibility or migration testing.

---

# 1. Guiding rules

1. **Do not add another ad hoc test hook.** New pause/crash controls go through the semantic failpoint registry.
2. **Do not copy production state machines into tests.** Tests drive production code and audit it against the reference model.
3. **Do not use sleeps to force races.** Every forced ordering has entered/release handshakes.
4. **Do not equate a green timeout with liveness.** Recovery has named progress measures and an event budget.
5. **Do not accept a bare seed as a reproducer.** Store the serialized scenario and choice trace.
6. **Do not weaken cost checks after a correctness change.** Every scenario reports request/resource budgets.
7. **Do not delay current known regressions until the full simulator exists.** Add the L1-now cases immediately, then migrate them onto shared simulator infrastructure.

---

# 2. Repository shape

Target workspace:

```text
crates/
  streams-core/
    src/
      collection/
      lifecycle/
      shard/
      history/
      topology/
      consumer/
      watch/
      registry/
      runtime.rs
      failpoints.rs
  streams-server/
    src/
      main.rs
      http.rs
      product.rs
      adapters/
  streams-sim/
    src/
      lib.rs
      world.rs
      scheduler.rs
      scenario.rs
      trace.rs
      state_hash.rs
      failpoints.rs
      stores.rs
      transport.rs
      platform.rs
      node.rs
      clients.rs
      model/
        mod.rs
        collection.rs
        lifecycle.rs
        topology.rs
        history.rs
        consumers.rs
        objects.rs
      auditor/
      workloads/
      modes/
      shrink/
    bin/
      streams-sim.rs
artifacts/
  corpus/
  failures/
docs/
  DST.md
  DST-EXPANSION-SPEC.md
  DST-SCENARIO-CATALOG.md
```

An incremental `src/lib.rs` split is acceptable before the workspace move. The architectural boundary matters more than the first directory layout.

---

# 3. Workstream 0 — baseline, inventory, and test naming

## Deliverables

- Record the exact baseline commit and pinned SlateDB commit in `docs/DST.md`.
- Generate a machine-readable inventory of current focused tests:

```json
{
  "test": "idempotent_successes_wait_for_durability",
  "scenario_ids": ["DUR-003"],
  "layer": "L1",
  "mechanisms": ["remote_durable_advanced", "producer_duplicate"],
  "invariants": ["D2", "P2"]
}
```

- Rename or annotate tests whose names overstate their proof.
- Add `#[scenario("DUR-003")]` or an equivalent metadata macro.

## Acceptance

- Every current DST test maps to at least one scenario ID.
- Every scenario marked Existing maps to a concrete test.
- No two scenario IDs accidentally point to a manually reconstructed test when a production failpoint is required.

---

# 4. Workstream 1 — semantic failpoint registry

## New API

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum FailPoint {
    AppendAfterValidationBeforeEnqueue,
    CommitAfterLocalStagingBeforeDbWrite,
    CommitAfterDbWriteBeforeInflight,
    CommitAfterAppliedBeforeRemoteDurable,
    CommitAfterRemoteDurableBeforeDispatch,
    CreateAfterDescriptor,
    CreateAfterSourceRefInstall,
    CreateBeforeReady,
    SealAfterClaim,
    SealBeforeFinalAppend,
    SealAfterFinalDurableBeforeMark,
    SealBeforeTerminalCas,
    TopologyAfterPendingIntent,
    TopologyBeforePhaseBCas,
    ForkAfterTombstoneBeforeParentRelease,
    HistoryAfterFlushBeforeAbsorbed,
    // ...
}

pub struct FailPointSelector {
    pub node: Option<NodeId>,
    pub stream: Option<String>,
    pub epoch: Option<StreamEpoch>,
    pub operation: Option<OperationId>,
    pub shard: Option<ShardPrefix>,
    pub segment: Option<SegmentId>,
    pub occurrence: Option<u64>,
}
```

## Actions

```rust
pub enum FailAction {
    Pause,
    ReturnError(SimError),
    CrashNode,
    CancelTask,
    Delay(Duration),
    DropResponse,
}
```

## Requirements

- Multiple tests may arm the same failpoint for different selectors.
- Arming a second selector MUST NOT disarm the first.
- `wait_entered` must prove the production path reached the exact point.
- Release is selector-specific.
- A guard releases or disarms its selector on panic.
- Counters increment at application time, not at rule selection time.

## Migration targets

Move these first:

- `http::fork_failpoints`;
- `scaler3::failpoints`;
- dispatch gate;
- absorber pause;
- FaultStore class holds.

## Acceptance

- No test uses a fixed sleep to establish ordering.
- Existing tests pass through the new registry.
- The registry itself has tests for concurrent selectors, nested points, cancellation, and panic cleanup.

---

# 5. Workstream 2 — immediate L1 correctness additions

These tests should land before the full deterministic substrate.

## Priority P0

1. **DUR-002:** same-group duplicate plus failed group write.
2. **DUR-004:** same-group idempotent close plus failed group write.
3. **DUR-005:** applied-but-not-durable idempotent close.
4. **DUR-006:** producer-sequence-reuse rejection barrier.
5. **DUR-008:** Stream-Seq conflict barrier.
6. **SEL-019:** phase-B merge under sealing.
7. **SEL-021:** close and fence in one group with failed write.
8. **SEL-022:** prior-group applied but remote durability paused.
9. **SEL-026:** fence safety with queued stale request and arbitrary time advance.
10. **SEL-027:** concurrent final attempts with identical payload but different semantic coordination.
11. **CRT-007:** create duplicate fast path plus failed initial-content group.
12. **FRK-013:** child deletion between stamp and source reference.
13. **FRK-016:** new child racing last-child cascade.

## Priority P1

- hard postings amplification adversary;
- empty-root history path;
- consumer settle durability;
- catalog transient GET;
- ready fork idempotence after source soft-delete;
- stale scaler phase-B and autonomous decision path.

## Acceptance

- Every test red-verifies against an isolated canary or per-fix revert.
- Every test includes failpoint-entered proof.
- Every success/rejection assertion audits durable model state, not only HTTP status.

---

# 6. Workstream 3 — unified barriered results in the committer

The repeated durability findings point to one missing primitive.

## Goal

Any result whose truth depends on state established by the current or prior non-durable commit group must wait for the correct durability barrier.

## Data model

```rust
pub struct DeferredResult {
    pub response: ResponseSender,
    pub result: Result<AppendAck, AppendErr>,
    pub dependency: DurabilityDependency,
}

pub enum DurabilityDependency {
    CurrentGroup,
    InFlightGroup { seq: u64 },
    AlreadyDurable,
}
```

## Applies to

- ordinary append success;
- producer duplicate success;
- idempotent close success;
- producer reuse conflict;
- stale producer epoch/gap when based on staged or applied state;
- Stream-Seq conflict;
- closed-stream result when closed state is not yet durable;
- seal fence result.

## Rules

- Intrinsic syntax/body errors remain immediate.
- `CurrentGroup` results fail with the group if `db.write` fails.
- `InFlightGroup` results fail if the DB closes before the target sequence is durable.
- `AlreadyDurable` may respond immediately.
- The committer records the provenance of producer/sequence/closed state: durable, previous in-flight group, or current group.

## Acceptance

- DUR-001–DUR-008 green.
- No successful or definitive state-dependent result bypasses the barrier abstraction.
- Mutation `send_batch_local_duplicate_immediately` is caught.

---

# 7. Workstream 4 — reference model and online auditor

## Model modules

### Collection/lifecycle

- epoch;
- config;
- init intent;
- seal execution;
- deletion state;
- fork references and debts.

### Routing/log

- per-key logical sequence;
- segment lineage;
- physical ownership;
- product cursors;
- raw default-key offsets.

### Producer/sequence

- producer lane state;
- exact request hash;
- original offset;
- Stream-Seq lane.

### Tiering

- canonical durable records;
- history coverage;
- postings coverage;
- trim boundary;
- object reachability.

### Consumer/watch

- cursor, leases, generation, attempts;
- watch journal and resync horizon.

## Online checks

Run after:

- every external response;
- every durable watermark advance;
- every descriptor CAS;
- every topology phase transition;
- every history boundary/trim;
- every process crash/restart;
- explicit checkpoints.

## Acceptance

- Current oracle negative controls are migrated.
- Model catches count-preserving loss/extra combinations.
- Model distinguishes logical operation from attempt.
- Raw and product views are audited against one canonical state.

---

# 8. Workstream 5 — trace and replay

## Trace output

Use a compact binary format plus JSON rendering.

```text
trace.bin
scenario.json
choices.bin
coverage.json
budgets.json
failure.json
```

## Failure fingerprint

```text
invariant ID
primary object ID
operation ID
first divergent event kind
normalized top stack/site
```

## Replay CLI

```bash
cargo run -p streams-sim -- replay failure.json
cargo run -p streams-sim -- explain failure.json
cargo run -p streams-sim -- render failure.json --out trace.md
```

## Acceptance

- A focused scenario replays identically 100 times.
- Changing only human-readable logging does not change state hash.
- Choice trace detects code changes that alter scheduler choices.

---

# 9. Workstream 6 — deterministic runtime extraction

## Step 1: core library

Move state-machine modules behind `src/lib.rs` without changing behavior.

## Step 2: inject clock and entropy

Replace:

- `Instant::now()`;
- `SystemTime::now()`;
- `tokio::time::sleep/interval`;
- `rand::rng()`;
- environment lookups inside hot state-machine paths.

## Step 3: task ownership

Replace direct `tokio::spawn` and `spawn_blocking` with owned runtime and CPU executor handles.

## Step 4: SlateDB integration

- Use deterministic executor for SlateDB tasks in simulation.
- Preserve dedicated real runtime in production adapter.
- Model CPU-heavy encode/compress as explicit deterministic jobs with yield points.

## Lints

Add disallowed methods for core crates.

## Acceptance

- Full single-node create→append→absorb→postings read→seal→delete runs under one deterministic scheduler.
- No task escapes to the global Tokio runtime.
- Trace and final hashes are stable across 100 replays.

---

# 10. Workstream 7 — lifecycle crash enumerator

Manual one-off crash tests do not scale. Build a generic enumerator.

## API

```rust
let plan = CrashPlan::for_operation(OperationKind::Seal)
    .at_every_durable_boundary()
    .with_retries(RetryPolicy::ExactPublicRequest)
    .with_concurrent(ConcurrentAction::DeleteAndRecreate);

run_crash_matrix(plan).await?;
```

## Operations

- create with initial body;
- create-and-close;
- fork create;
- seal-only;
- seal-with-final;
- raw close-with-content;
- direct delete;
- fork cascade delete;
- split;
- merge;
- history absorption;
- consumer lease/settle/DLQ.

## Boundary source

Production code registers boundaries in the semantic registry. The enumerator obtains the ordered list from trace events rather than duplicating it.

## Acceptance

- Every boundary in section 10 of the main spec is enumerated.
- Exact public retry or background recovery converges.
- No manual storage planting is needed for the primary scenario.

---

# 11. Workstream 8 — multi-node world

## Components

- N node runtimes;
- simulated platform start/stop;
- router with cached ownership/map;
- fleet store and heartbeat/desired objects;
- per-node store connectivity;
- owner possession state;
- client locality.

## First corpus

1. old/new owner in-flight append;
2. staggered 1→4 cold starts under write load;
3. stale router replay during movement;
4. crash owner with dirty unabsorbed streams;
5. split across distinct owners;
6. half-fleet crash;
7. scale-in while clients remain routed to removed owners;
8. old owner zombie task/object activity;
9. pending summary cleanup;
10. desired-state CAS conflict.

## Acceptance

- Known ring-convergence and reopen-storm incidents are reproduced by canary/broken configurations and prevented by current code.
- At least one viable healed core always converges in liveness mode.

---

# 12. Workstream 9 — object-store and cost model

## Physical attempt ledger

```rust
struct PhysicalAttempt {
    node: NodeId,
    logical_operation: StoreOperationId,
    attempt: u32,
    method: StoreMethod,
    class: ObjectClass,
    status: StoreStatus,
    billing: BillingClass,
    request_bytes: u64,
    response_bytes: u64,
}
```

## Faults to add

- reset/truncate GET and LIST;
- wrapped multipart per-part/completion faults;
- CAS conflict;
- conditional 304/412;
- per-node partition;
- slow body/close;
- bandwidth.

## Budget gates

Port from cost/routing campaigns:

- shared-history Class A cardinality slope;
- LIST steady state;
- postings storage and amplification;
- reader-cache open budgets;
- WAL grouping;
- wide-cardinality request/resource budgets;
- fleet control-plane budget.

## Acceptance

A cost regression creates the same kind of first-class failure artifact as a correctness invariant.

---

# 13. Workstream 10 — workload generator and modes

## Modes

```text
focused-durability
focused-create
focused-seal
focused-fork
focused-topology
focused-history
focused-consumer
focused-security
focused-fleet
liveness
performance-cost
swarm
canary
```

## Generator grammar

The generator maintains preconditions so it can deliberately reach deep states:

```text
Open collection
→ append producers
→ split
→ fork
→ soft-delete ancestor
→ create consumer leases
→ begin seal
→ crash at boundary
```

It also generates invalid operations from the same state.

## Acceptance

- Every invariant family is exercised in swarm runs.
- Coverage dashboard reports state/transition coverage, not only line coverage.
- Boundary values dominate the generation distribution.

---

# 14. Workstream 11 — liveness engine

## Heal policy

At safety-phase end:

1. stop injecting new faults;
2. choose a viable node/store core;
3. restore those nodes;
4. heal their store/router connectivity;
5. keep unrelated nodes down or isolated;
6. advance until quiescence or event budget.

## Progress watchdog

Each measure records:

```text
last value
last change event
last change time
causal pending work
```

A failure report says which subsystem stopped and why no runnable action can advance it.

## Acceptance

- Eu-central reopen-storm canary fails liveness.
- Memory-shed high-water canary fails liveness.
- Pending topology+seal canary fails liveness.
- Current code converges after transient store failures, owner crash, and stale routes.

---

# 15. Workstream 12 — shrinking and corpus

## Shrink passes

- operations;
- concurrency;
- nodes;
- streams/keys;
- payload sizes;
- faults;
- failpoint occurrences;
- time;
- configuration.

## Semantic preservation

The shrinker retains required mechanism coverage. A smaller scenario that no longer reaches the failure boundary is rejected.

## Corpus policy

- Every production escape becomes a minimized artifact.
- Every audit-found interleaving becomes a named artifact even if found before launch.
- Corpus entries are immutable except for schema migration tooling.

---

# 16. CI rollout

## Initial PR gate

- L1-now cases;
- failpoint registry tests;
- current full suite/conformance;
- no timing-assisted races.

## After deterministic runtime

- replay hash test;
- fixed corpus;
- 500 swarm cases per PR;
- canaries.

## Nightly

- 100k scenarios;
- all lifecycle crash matrices;
- multi-node liveness;
- cost campaigns;
- shrink failures automatically.

## Continuous

- latest main;
- active lifecycle/topology PRs;
- upload dashboard and artifacts.

---

# 17. Definition of done

The implementation program is complete when:

- every scenario in the catalogue has an owner and status;
- every **L1-now** scenario is green;
- full single-node replay is deterministic;
- multi-node ownership/liveness is deterministic;
- all canary mutations are caught;
- every failure is reproducible from an artifact;
- no current test relies on sleeps for race ordering;
- no safety state is manually planted as the primary crash proof;
- official Durable Streams conformance remains at the pinned expected outcome;
- field tests remain as a separate launch gate for real adapters/platform behavior.

