# Prisma Streams Deterministic Simulation Testing Expansion Specification

**Status:** implementation handoff specification  
**Baseline reviewed:** `681ea0fe73ca49c74fc10a61846b9dbf7195d443` (`streams-slate 14.zip`)  
**Applies to:** the unified, pre-launch Prisma Streams product surface and the pinned Durable Streams raw protocol surface  
**Compatibility posture:** destructive pre-launch cutover; no mixed-version, migration, or legacy-layout simulation is required

## 0. Normative language

The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative.

This document extends `docs/DST.md`. It does not replace the official Durable Streams conformance suite, the local full Rust suite, cost campaigns, or real-cloud field tests. It defines the simulation and focused-fault coverage required so that the failure classes already encountered by this project become permanent, deterministic release gates.

---

# 1. Executive requirement

Prisma Streams has repeatedly found defects in narrow interleavings between individually sensible durable transitions:

- an acknowledgement was sent before the write it described was durable;
- a duplicate response observed batch-local or applied state and escaped before the original group reached object storage;
- a topology transition was fenced at its start but could still publish at phase B;
- a sealing claim existed, but its final append was no longer bound to the same stream incarnation;
- a timeout was treated as a lease without fencing the old writer;
- a fence lived in an evictable handle or was expired by wall clock while stale requests remained queued;
- a descriptor was created before its initial content and a replay returned the half-created resource;
- a fork child, source reference, tombstone, and recursive reference release were each durable, but not recoverable as one lifecycle;
- a cached view, refresh floor, or list policy suppressed GC or multiplied object-store requests;
- a test claimed to exercise a mechanism that never fired, or reconstructed a post-crash state by hand instead of crashing production code at the real boundary.

The expansion MUST make those classes systematic rather than episodic.

The target is:

> **Every externally visible success, rejection, lifecycle transition, topology transition, and background-maintenance decision is checked against a reference model at every deterministic yield and durable boundary; every multi-step operation is crashed or raced at each boundary; every fault-heavy run is followed by a liveness phase that requires convergence.**

---

# 2. Completion criteria

The DST program is complete only when all of the following are true.

## 2.1 Exact replay

For one commit and one serialized scenario:

```text
same scenario + same choice trace
→ identical event trace hash
→ identical final state hash
→ identical failure fingerprint
```

This MUST include the absorber, SlateDB background work, compaction, GC, task scheduling, and process lifecycle. A deterministic object-store fault schedule on top of nondeterministic task scheduling is not sufficient.

## 2.2 Whole-system scope

The simulator MUST support, in one process:

- several Streams nodes;
- one router/edge actor;
- the fleet membership and ownership loop;
- multiple shard databases and history partitions;
- clients using both the product API and the raw Durable Streams API;
- automatic split and merge;
- consumer groups and watches;
- forks and deletion cascades;
- process crash, pause, restart, and scale-to-zero restore;
- a reference model and online auditor.

## 2.3 State-machine coverage

Every durable multi-step operation MUST have:

1. a named intent or claim;
2. enumerated semantic boundaries;
3. a deterministic crash/race scenario at every boundary;
4. a liveness recovery scenario;
5. mechanism counters proving the boundary was reached;
6. at least one canary mutation that the scenario catches.

## 2.4 Non-vacuity

A scenario MUST be invalid, not passing, when its intended mechanism did not run. Examples:

- a fencing scenario with zero fences;
- a crash-window scenario that never reached the failpoint;
- a history scenario with zero history reads;
- a takeover scenario in which no old request remained in flight;
- a split scenario in which both children stayed on one physical owner;
- a GC scenario that generated no obsolete objects;
- a cache-stampede scenario in which only one caller reached the cache.

## 2.5 Safety and liveness

Every whole-system scenario MUST have two phases:

```text
safety phase:
  faults, crashes, stale routes, overload, transitions

liveness phase:
  stop new faults
  heal a viable core
  restart selected nodes
  advance deterministic time
  require convergence within an event budget
```

“No invariant failed before timeout” is not a liveness result.

## 2.6 Cost and resource budgets

DST MUST enforce deterministic budgets for:

- physical Class A and Class B request attempts;
- successful billable response classes;
- LIST pages;
- WAL, manifest, compaction, and history PUTs;
- bytes fetched versus bytes returned;
- canonical spans per postings read;
- open DBs, readers, tasks, timers, cache bytes, handles, and pending operations;
- queue depth and oldest request age;
- logical work required to drain a backlog.

A change that preserves correctness by shifting the cost from storage bytes to per-record GETs MUST fail a budget gate.

---

# 3. Scope and non-goals

## 3.1 In scope

- Current final storage layout only.
- Current product routes and the pinned raw Durable Streams route.
- Correctness under supported object-store semantics and client/network failures.
- Multi-node ownership, routing, split/merge, and scale transitions.
- Lifecycle state machines: create, initialize, seal, delete, expiry, fork.
- Tiering: shard log, shared history, postings, trim, compaction, GC.
- Consumer and watch semantics.
- Cost and resource invariants.
- Security-relevant route classification and capability boundaries where they are pure server logic.

## 3.2 Explicitly out of scope for simulation

These remain outer-loop tests:

- real TLS and certificate behavior;
- actual Prisma Compute edge registration and preview-domain routing;
- DNS/ECS and provider PoP selection;
- actual cgroup/RSS accounting and allocator release behavior;
- real Tigris wall-clock latency distributions;
- binary architecture and deployment-wrapper behavior;
- browser CORS enforcement by a real browser engine;
- npm registry publication.

The simulator MAY model their logical consequences, but real adapters MUST still be exercised by compiled-binary field tests.

## 3.3 No compatibility program

Because the product is pre-launch:

- no mixed binary versions;
- no old descriptor decoding;
- no old history layout migration;
- no legacy routing modes;
- no rolling-format compatibility scenarios.

A scenario starts from a clean namespace using the current format. Rollback means deleting that namespace.

---

# 4. Testing layers

The project MUST maintain four complementary layers.

| Layer | Purpose | Uses real core code | Deterministic | Typical duration |
|---|---|---:|---:|---:|
| **L0 — model/property tests** | codecs, planners, maps, state transitions | selected modules | yes | milliseconds |
| **L1 — focused semantic-failpoint tests** | one exact boundary or regression | yes | mostly; current runtime limitations remain initially | seconds |
| **L2 — whole-system simulator** | nodes, router, stores, workload, crashes, liveness | yes | yes | seconds to minutes of simulated time |
| **L3 — external binary harness** | Axum, SDKs, process kills, real providers/platform | complete binary | no | minutes to hours |

L1 MUST not become a second whole-system framework. New shared capabilities belong in L2 and are exposed to L1 as thin focused-scenario helpers.

---

# 5. Target simulator architecture

```text
World
├── Scheduler
│   ├── logical monotonic clock
│   ├── wall-clock model
│   ├── deterministic runnable queue
│   ├── deterministic CPU-job queue
│   └── choice trace
├── Stores
│   ├── ops store
│   ├── shard store
│   ├── data/history store
│   └── physical request ledger
├── Router
│   ├── route cache
│   ├── stale-route behavior
│   └── replay limits
├── Platform
│   ├── instance desired state
│   ├── start/stop/pause/crash
│   └── resource signals
├── Nodes[N]
│   ├── AppState
│   ├── task group
│   ├── serving map / OpenGate
│   ├── shard engines
│   ├── absorbers and history partitions
│   ├── scaler
│   ├── fleet loop
│   └── caches
├── Clients[M]
│   ├── raw Durable Streams clients
│   ├── product SDK clients
│   ├── positive-space behavior
│   └── adversarial behavior
├── ReferenceModel
├── OnlineAuditor
├── MechanismCoverage
└── TraceRecorder
```

## 5.1 Node task ownership

Every spawned task MUST have an owner:

```rust
struct TaskOwner {
    node: NodeId,
    component: ComponentId,
    generation: u64,
}
```

A simulated process crash MUST:

- cancel all tasks owned by that node;
- drop in-memory handles, keys, caches, fences, and pending responses;
- preserve only object-store state;
- not call graceful DB close;
- abandon client requests whose response channel belongs to that process;
- leave already-dispatched object-store operations according to the configured fault model.

A node pause freezes owned tasks while retaining memory. A restart constructs a new task generation from persistent state.

## 5.2 Environment interfaces

Production core code MUST receive these capabilities explicitly:

```rust
trait Clock {
    fn monotonic_now(&self) -> MonoTime;
    fn wall_now(&self) -> WallTime;
    fn sleep(&self, d: Duration) -> SimFuture<()>;
}

trait Entropy {
    fn next_u64(&self, scope: RandomScope) -> u64;
    fn fill(&self, scope: RandomScope, out: &mut [u8]);
}

trait TaskRuntime {
    fn spawn(&self, owner: TaskOwner, task: SimFuture<()> ) -> TaskId;
    fn cancel_owner(&self, owner: TaskOwner);
}

trait CpuExecutor {
    fn submit(&self, owner: TaskOwner, job: CpuJob) -> SimFuture<CpuResult>;
}

trait ProcessMetrics {
    fn cpu(&self, node: NodeId) -> f64;
    fn memory(&self, node: NodeId) -> u64;
}
```

Direct uses of OS time, `tokio::spawn`, `spawn_blocking`, global randomness, environment reads, and process metrics MUST be forbidden in `streams-core` by lint, except inside production adapters.

## 5.3 Multi-database registry

The simulator MUST support many simultaneous SlateDB instances:

```rust
enum DbRole {
    Ops,
    Shard { prefix: ShardPrefix },
    History { shard: ShardPrefix },
}

struct DbInstanceId {
    node: NodeId,
    role: DbRole,
    writer_epoch: u64,
}
```

A handoff opens the same persistent prefix from a new node while the old node may still have tasks. It is not modeled as replacing one global DB handle.

---

# 6. Deterministic event model

## 6.1 Event record

Every meaningful transition MUST emit a trace event:

```rust
struct TraceEvent {
    seq: u64,
    logical_time: MonoTime,
    actor: ActorId,
    causal_parent: Option<u64>,
    operation: Option<OperationId>,
    kind: EventKind,
    object: Option<ObjectId>,
    before_hash: Option<StateHash>,
    after_hash: Option<StateHash>,
    mechanism: Option<MechanismId>,
}
```

Events include:

- request accepted/rejected;
- queue enqueue/dequeue;
- commit group built;
- SlateDB write submitted/applied/durable;
- response dispatched/dropped;
- descriptor CAS attempted/won/lost;
- lifecycle intent installed/renewed/fenced/completed;
- topology phase transitions;
- history write/flush/boundary/trim;
- object-store request dispatch/result;
- task spawn/exit/cancel;
- process pause/crash/restart;
- liveness measure change.

## 6.2 State hashing

The state hash MUST include logical state, not incidental addresses:

- descriptors and epochs;
- segment map and pending transitions;
- shard durable tails;
- producer and sequence state;
- consumer state;
- fork graph and debts;
- history boundaries and postings ranges;
- live object reachability graph;
- node ownership and serving map;
- pending client operations and outcomes;
- relevant task states and queue positions.

It MUST exclude nondeterministic heap addresses and wall-clock formatting.

## 6.3 Choice trace

The replay artifact MUST store scheduler choices separately from the root seed. A seed alone is not stable across code changes that alter the number or order of choices.

---

# 7. Scenario DSL

Scenarios MUST be serializable and human-readable.

```rust
struct Scenario {
    schema_version: u32,
    name: String,
    root_seed: u64,
    event_budget: u64,
    world: WorldConfig,
    workload: Vec<Step>,
    faults: Vec<FaultRule>,
    required_mechanisms: Vec<MechanismRequirement>,
    invariants: Vec<InvariantId>,
    liveness: Option<LivenessConfig>,
    budgets: BudgetConfig,
}
```

## 7.1 Steps

```rust
enum Step {
    CreateCollection { ... },
    Append { ... },
    AppendMany { ... },
    RawAppend { ... },
    Read { ... },
    Subscribe { ... },
    Scan { ... },
    Seal { ... },
    Delete { ... },
    Fork { ... },
    ConsumerPull { ... },
    ConsumerSettle { ... },
    AdvanceTime { ... },
    StartNode { ... },
    CrashNode { ... },
    PauseNode { ... },
    HealPartition { ... },
    ForceScaleEvaluation { ... },
    AssertCheckpoint { ... },
}
```

## 7.2 Fault actions

```rust
enum FaultAction {
    PauseAtFailpoint,
    CrashNode,
    CancelRequest,
    LoseResponse,
    FailBeforeDispatch,
    SucceedThenLoseStoreResponse,
    Delay(Duration),
    TruncateBody { after_bytes: usize },
    ResetStream,
    Partition { from: ActorId, to: ServiceId },
    DuplicateDispatch,
    ReorderCompletion,
    ForceCasConflict,
    ExhaustResource(ResourceKind),
}
```

## 7.3 Entered/release handshake

Every pause failpoint MUST expose:

```text
arm(name, operation-id)
await_entered(name, operation-id)
release(name, operation-id)
```

A fixed sleep is never accepted as proof that a race was forced.

## 7.4 Operation identity

Every logical client operation and every attempt MUST have distinct IDs:

```rust
struct LogicalOperationId(Uuid);
struct AttemptId { op: LogicalOperationId, n: u32 }
```

The reference model stores exact request bytes, routing key, content type, producer identity, sequence, key version, timestamp, expected incarnation, and response outcome.

---

# 8. Reference model

The simulator MUST maintain an implementation-independent model.

## 8.1 Collection model

```rust
struct ModelCollection {
    name: String,
    epoch: StreamEpoch,
    config: CanonicalConfig,
    state: ModelLifecycle,
    segments: ModelSegmentMap,
    keys: BTreeMap<RoutingKey, ModelKeyLog>,
    producers: BTreeMap<ProducerLane, ModelProducerState>,
    consumers: BTreeMap<ConsumerName, ModelConsumerState>,
    watches: BTreeMap<WatchName, ModelWatch>,
    fork: Option<ModelForkRef>,
    fork_children: BTreeSet<ForkReferenceId>,
}
```

## 8.2 Lifecycle model

```rust
enum ModelLifecycle {
    Initializing(ModelInitIntent),
    Open,
    Sealing(ModelSealIntent),
    Sealed,
    SoftDeleted,
    HardDeleted,
}
```

A model transition is keyed by stream epoch and complete semantic operation identity.

## 8.3 Per-key logs

The canonical product guarantee is ordering per routing key. The raw Durable Streams route is the default empty-key sequence. The model MUST never infer a global order between non-empty routing keys.

## 8.4 Attempt outcomes

```rust
enum ModelOutcome {
    Pending,
    Acked { offsets: Range<u64>, duplicate: bool },
    Rejected { class: RejectClass },
    Unknown,
}
```

`Unknown` may result from client deadline, response loss, process crash, or ambiguous movement. It does not authorize more than one committed copy when producer idempotence is used.

## 8.5 Persistent object graph

The model MUST track logical reachability:

- descriptor objects;
- shard WAL/SST/manifest generations;
- history canonical and postings rows;
- fork ancestors and child references;
- pending deletion debts;
- topology parents/children;
- checkpoints and reader pins.

GC invariants are checked against reachability, not merely record results.

---

# 9. Invariant catalogue

The following invariant families are mandatory. Detailed scenarios are in `SCENARIO-CATALOG.md`.

## 9.1 Durability and response linearizability

| ID | Invariant |
|---|---|
| **D1** | Every acknowledged append is remotely durable before the success response becomes observable. |
| **D2** | A duplicate success is not observable before the original write’s durability barrier. |
| **D3** | An idempotent close success is not observable before the close’s durability barrier. |
| **D4** | A state-dependent rejection derived from batch-local or applied state is barriered behind the state that establishes it. |
| **D5** | If the responsible commit group fails, no response may rely on state from that group. |
| **D6** | A response lost after durability produces `Unknown`, never data loss. |
| **D7** | A client deadline may expire while the server commits; an idempotent retry resolves to exactly one operation. |
| **D8** | Every acknowledged offset refers to the exact operation bytes and routing key acknowledged. |
| **D9** | Every readable record belongs to an issued operation. |
| **D10** | A definitively rejected request never appears unless the rejection was based on non-durable state, in which case the response itself must have waited for that state’s durability. |

## 9.2 Producer, sequence, and idempotence

| ID | Invariant |
|---|---|
| **P1** | Producer state is scoped to collection incarnation, routing key, producer ID, epoch, and sequence. |
| **P2** | An exact producer retry commits at most once and returns the original offset. |
| **P3** | A duplicate consumes no new offset. |
| **P4** | Producer state survives owner movement, split, merge, and restart. |
| **P5** | `Stream-Seq` state is scoped to routing key and resolves through predecessor lineage. |
| **P6** | Internal lifecycle idempotence uses a namespace inaccessible to public producer IDs. |
| **P7** | Operation identity covers every semantic field that affects acceptance or persisted bytes. |
| **P8** | A producer gap is not treated as irrevocable while an admitted predecessor can still commit. |

## 9.3 Create and initialization

| ID | Invariant |
|---|---|
| **C1** | A descriptor with `init != None` is never visible as Ready, regardless of claim age. |
| **C2** | A replay of the same create joins or resumes the same initialization. |
| **C3** | A wrong key cannot resume initialization. |
| **C4** | A stale creator cannot publish readiness for a later incarnation. |
| **C5** | The resource is Ready only after initial body, close-on-create, fork seed, and reference installation are complete. |
| **C6** | A concurrent delete cannot cause create to return success for a deleted target or leak a source reference. |
| **C7** | Successful create replay cannot outrun initial-content durability. |
| **C8** | Catalog and metadata do not expose initializing resources as live. |

## 9.4 Sealing and lifecycle

| ID | Invariant |
|---|---|
| **L1** | All deterministic validation completes before a seal intent is published. |
| **L2** | A final-bearing seal intent records a complete semantic operation identity. |
| **L3** | A plain seal cannot complete an owed final-bearing intent. |
| **L4** | New writes are rejected during Sealing/Sealed, but exact duplicates may resolve. |
| **L5** | The seal claim is bound to stream epoch, operation ID, and generation through claim, final append, mark, physical close, and terminal publication. |
| **L6** | Takeover fences the old generation before a new generation can write. |
| **L7** | A fence response is a remote-durability barrier for all preceding decisions. |
| **L8** | Only the newest reserved takeover generation may install. |
| **L9** | Fence safety survives handle/cache eviction and remains until queue progress proves it unnecessary. |
| **L10** | No wall-clock timeout alone decides whether an old write may still land. |
| **L11** | Raw close-with-content is resumable at every crash boundary through the public route. |
| **L12** | A duplicate of a prior non-closing operation cannot satisfy a final close. |
| **L13** | `Sealed` is published only when all live segments are durably closed and no topology transition remains pending. |
| **L14** | No stale lifecycle operation can mutate a recreated incarnation. |
| **L15** | A successful seal response proves the exact expected incarnation and operation reached terminal state. |

## 9.5 Forks

| ID | Invariant |
|---|---|
| **F1** | A fork reads exactly the source prefix up to its boundary plus its own suffix. |
| **F2** | Binary sub-offset forks materialize the partial record exactly once. |
| **F3** | Every ancestor hop validates source epoch and uses the correct decryption epoch. |
| **F4** | Fork chains are cycle-free and depth-bounded. |
| **F5** | A source with live forks soft-deletes and returns 410; recreation is blocked. |
| **F6** | Hard deletion occurs only after the last child reference is durably released. |
| **F7** | Child initialization, source reference installation, and Ready publication are crash-resumable. |
| **F8** | Deletion debt is recoverable by retrying the original public operation. |
| **F9** | Recursive cascade debt survives crashes at every ancestor. |
| **F10** | Concurrent fork creation and source deletion serialize; no live child points to a hard-deleted source. |
| **F11** | The raw fork view contains only the default routing key. |
| **F12** | Fork participants cannot enter unsupported topology states. |

## 9.6 Topology, routing, and ownership

| ID | Invariant |
|---|---|
| **T1** | Segment ranges form a complete, non-overlapping partition. |
| **T2** | A pending split or merge is recoverable after every phase boundary. |
| **T3** | A split cannot look like permanent stream closure to GET, HEAD, long-poll, SSE, or scan. |
| **T4** | Phase B cannot publish under Sealing or Sealed. |
| **T5** | Sealing cannot install over pending topology; one transition serializes before the other. |
| **T6** | Children persist independent physical routes and increase capacity when load is splittable. |
| **T7** | A dominant single routing key is reported as unsplittable and does not create useless children. |
| **T8** | Stale scaler heat or decisions cannot mutate a recreated incarnation. |
| **T9** | Producer and sequence state remain exact through split and merge lineage. |
| **T10** | A stale router may add a replay but cannot lose, duplicate, or reorder a key’s records. |
| **T11** | Exactly one owner epoch may acknowledge for a shard. |
| **T12** | Serving possession, not ring preference alone, determines whether a node may act. |

## 9.7 History, postings, trim, and GC

| ID | Invariant |
|---|---|
| **H1** | History is an exact prefix and shard tail an exact suffix of each segment log. |
| **H2** | Merged reads cover every durable offset exactly once. |
| **H3** | `absorbed` advances only after canonical frames and postings are durable. |
| **H4** | Trim never passes the previously safe absorbed boundary. |
| **H5** | Global gather and trim work are bounded by configured memory/work budgets. |
| **H6** | Budget-deferred streams remain pending without requiring a later customer signal. |
| **H7** | Dirty-stream recovery discovers untouched backlog after restart under production policy. |
| **H8** | A failed dirty scan is retried until convergence. |
| **H9** | Large records always make cursor progress. |
| **H10** | Postings reads issue bounded canonical spans and bounded read amplification; never one GET per record. |
| **H11** | Corrupt/missing postings cannot produce a false complete result. |
| **H12** | Reader/postings caches are single-flight, cancellation-proof, store-scoped, and process-budgeted. |
| **H13** | Nothing reachable from a live manifest, topology parent/child, fork, reader checkpoint, or history boundary is deleted. |
| **H14** | GC converges without periodic LIST storms and cannot be suppressed by a stale inventory or refresh dead zone. |
| **H15** | Pending summaries and lag gauges clear on ownership loss and never produce phantom fleet backlog. |

## 9.8 Consumer groups

| ID | Invariant |
|---|---|
| **Q1** | Delivery is FIFO per routing key. |
| **Q2** | Keys may progress independently. |
| **Q3** | Lease generation fences stale ack/retry/extend operations. |
| **Q4** | Acknowledged messages are never redelivered. |
| **Q5** | Unacknowledged messages may redeliver after visibility timeout, preserving at-least-once semantics. |
| **Q6** | Retry, max-attempt, and DLQ transitions are atomic and crash-resumable. |
| **Q7** | Consumer state follows split/merge lineage without gaps or duplicate settlement. |
| **Q8** | DLQ target identity is pinned to a stream incarnation or uses an in-stream reserved sequence. |

## 9.9 Watches

| ID | Invariant |
|---|---|
| **W1** | A watch publishes only after the matching append is durable and readable. |
| **W2** | A missed watch journal causes an explicit resync, never a false no-change result. |
| **W3** | Signed watch capabilities remain valid across restart without exposing decryption keys. |
| **W4** | Only the exact watch-wait route may use capability authorization. |
| **W5** | Watch routing and cursor behavior survives split/merge. |

## 9.10 Security, API, and Durable Streams compliance

| ID | Invariant |
|---|---|
| **S1** | Every product route requires bearer authorization except an exact, valid signed watch-wait capability. |
| **S2** | Authentication happens before large body allocation. |
| **S3** | Actual product responses and preflights carry the required CORS headers. |
| **S4** | Account token and encryption key remain separate authorization domains. |
| **S5** | Wrong keys never yield plaintext or mutate lifecycle state. |
| **S6** | The raw route is exactly the default-key Durable Stream across product splits, forks, sealing, and deletion. |
| **S7** | Product cursors are bound to stream epoch, routing key, snapshot/map version, and operation type. |
| **S8** | Catalog pagination is complete under tombstones, expiry, initialization, vanished objects, and transient errors. |
| **S9** | Legacy headers/modes are rejected under the clean-cutover posture. |
| **S10** | The pinned Durable Streams server conformance result remains unchanged after every raw-surface or lifecycle change. |

## 9.11 Resources and cost

| ID | Invariant |
|---|---|
| **R1** | Every task owned by a closed, crashed, or fenced engine terminates. |
| **R2** | Task, DB, reader, handle, cache, timer, and pending-operation counts are bounded. |
| **R3** | Safety state required by queued work is not evicted or expired prematurely. |
| **R4** | Idle cardinality state is eventually evicted without losing restart discovery. |
| **R5** | Overload sheds instead of queueing into unbounded latency and recovers after healing. |
| **R6** | Usage tracking never silently fails open after its tracked-stream bound. |
| **R7** | Billing checkpoints never advance past un-emitted deltas. |
| **R8** | History/request cost is bounded by bytes/work, not stream or routing-key cardinality. |

---

# 10. Semantic failpoint registry

All current ad hoc controls MUST be consolidated:

- object-store class holds in `FaultStore`;
- `scaler3::failpoints`;
- `http::fork_failpoints`;
- `ShardEngine::test_hold_dispatch`;
- absorber pause flags;
- any environment-variable failpoint.

## 10.1 API

```rust
trait FailPointRegistry {
    fn arm(&self, point: FailPoint, selector: Selector, action: FailAction);
    async fn wait_entered(&self, point: FailPoint, selector: Selector, count: u64);
    fn release(&self, point: FailPoint, selector: Selector);
    fn count(&self, point: FailPoint, selector: Selector) -> u64;
}
```

Selectors support stream name, epoch, operation ID, node, shard, segment, routing key, producer, and occurrence.

## 10.2 Required failpoints

### Append and durability

```text
append.after_validation_before_enqueue
append.after_enqueue_before_group
commit.after_local_staging_before_db_write
commit.after_db_write_before_inflight
commit.after_applied_before_remote_durable
commit.after_remote_durable_before_dispatch
commit.after_dispatch_before_http_response
commit.before_state_dependent_response
```

### Create

```text
create.after_descriptor_before_init_work
create.after_initial_body_durable
create.after_close_durable
create.after_fork_tail_seed
create.after_fork_id_stamp
create.after_source_ref_install
create.before_ready_cas
create.after_ready_cas_before_response
```

### Seal

```text
seal.after_validation_before_claim
seal.after_claim_before_final_append
seal.after_final_append_durable_before_mark
seal.after_mark_before_segment_close
seal.after_each_segment_close
seal.before_terminal_cas
seal.after_terminal_cas_before_response
seal.takeover_after_reservation_before_fence
seal.takeover_after_fence_before_install
seal.fence_after_raise_before_durable_barrier
```

### Fork/delete

```text
fork.after_target_init
fork.after_tail_seed
fork.after_child_id_stamp
fork.after_source_ref_install
fork.before_ready
fork.delete_after_tombstone_before_parent_release
fork.cascade_after_intermediate_tombstone
fork.cascade_after_parent_release_before_clear_debt
```

### Topology

```text
topology.after_pending_intent
topology.after_parent_seal
topology.after_clone_or_seed
topology.before_phase_b_cas
topology.after_children_publish
topology.before_parent_retire
topology.before_parent_gc
```

### History and GC

```text
history.after_gather_before_write
history.after_write_before_flush
history.after_flush_before_absorbed_batch
history.after_absorbed_before_trim
history.during_trim
history.reader_after_cache_lookup
history.reader_after_probe
history.reader_during_open
history.reader_before_cache_insert
gc.after_inventory_before_delete
gc.after_delete_before_inventory_update
```

### Fleet and ownership

```text
ownership.after_ring_change_before_open
ownership.after_new_open_before_old_fence
ownership.after_old_fence_before_serving_eviction
fleet.after_heartbeat_write
fleet.after_membership_read
fleet.before_desired_cas
router.after_stale_route_before_replay
```

---

# 11. Fault model

## 11.1 Object-store semantics

The simulator MUST support per operation and object class:

- deterministic latency;
- fail before dispatch;
- succeed then lose response;
- connection reset before headers;
- truncated streaming body after N bytes/items;
- slow body and slow close;
- conditional `304`/`412` outcomes;
- explicit CAS conflict;
- pagination and partial LIST;
- per-node store partition;
- operation completion after caller cancellation;
- bounded bandwidth.

It MUST distinguish:

```text
logical object_store operation
physical HTTP attempt
billable response class
```

Incorrect object contents are not injected unless the production provider contract permits them. Corruption tests belong at codec/block-transform boundaries.

## 11.2 Process and scheduler faults

- hard process crash;
- graceful process stop;
- pause/resume;
- task cancellation;
- task starvation;
- CPU-job delay;
- wall-clock jump while monotonic time remains stable;
- resource signal change;
- simultaneous crash of selected nodes;
- start/restore delay.

## 11.3 Client/transport faults

- client deadline;
- body reset after successful headers;
- response loss after server dispatch;
- duplicate request dispatch;
- stale route;
- replay loop;
- request cancellation after intent but before response;
- SDK speculative read discarded after prior body failure.

---

# 12. Workload generation

## 12.1 Positive-space clients

Generate valid operations that follow returned cursors, retry rules, producer sequences, and consumer lease tokens.

## 12.2 Negative-space clients

Generate:

- wrong/missing bearer tokens;
- wrong encryption keys;
- malformed and partial producer trios;
- stale producer epochs;
- sequence gaps and reuse;
- stale/tampered cursors;
- invalid routing keys and header values;
- bodies exactly at, one below, and one above limits;
- `null`, empty arrays, large arrays, and oversized records;
- stale consumer lease generations;
- duplicate ack/retry/extend operations;
- invalid fork offsets/sub-offsets;
- create/delete/recreate ABA attempts;
- concurrent seal operations with same bytes but different semantics.

## 12.3 Boundary-biased generation

Every numeric setting MUST heavily sample:

```text
0, 1, 2,
limit-1, limit, limit+1,
maximum-1, maximum,
empty and full,
one and many,
first and last offset,
pre- and post-expiry,
just before and after claim/fence thresholds
```

## 12.4 Workload distributions

Named distributions:

- one hot key;
- eight balanced keys;
- Zipf hot-key distribution;
- one million sparse keys, 100 active per five-minute window;
- one million sparse streams, 100 active;
- batch 1, 10, 100, 1,000;
- large contiguous runs;
- highly fragmented postings;
- repeated split/merge oscillation;
- many short-lived consumer groups;
- long fork chains and wide fork trees.

---

# 13. Safety and liveness execution protocol

## 13.1 Online audit

After every durable event and every externally visible response, run the relevant invariant subset. Whole-world audits run at checkpoints and quiescence.

## 13.2 Liveness measures

Each subsystem exposes monotonic progress measures:

```text
commit:          durable sequence / pending group count
history:         absorbed, trimmed, dirty count
GC:              obsolete reachable count / deletion debt
ownership:       stable owner epochs / unserved shards
create:          initializing count and age
seal:            sealing operations and phase
fork:            unpaid references and initializing children
topology:        pending transitions
consumer:        settled prefix / active leases
watch:           journal head / waiter completion
fleet:           desired/live/serving convergence
```

A liveness failure MUST report the measure that stopped changing and the events since its last progress.

## 13.3 Quiescence

The world is quiescent only when:

- no client operation remains Pending unless intentionally long-lived;
- no recoverable lifecycle or topology intent remains;
- all acknowledged data is readable;
- history/trim/GC have reached configured convergence;
- no stale owner task remains;
- control-plane ownership is stable;
- no unbounded retry timer or runnable task remains;
- all budgets are within limits.

---

# 14. Mechanism coverage and anti-vacuity

Required counters include, at minimum:

```text
append_acked
append_rejected
append_unknown
append_retried
response_lost
client_deadline
producer_duplicate
state_dependent_result_barriered
commit_group_failed
remote_durable_advanced
old_owner_fenced
stale_request_rejected_by_fence
fence_waited_for_durability
seal_takeover
seal_claim_renewed
seal_final_committed
seal_intent_abandoned
create_resumed
create_takeover
fork_reference_installed
fork_debt_repaired
split_phase_a
split_phase_b
merge_phase_a
merge_phase_b
stale_route_replay
history_flush
absorbed_batch
trim_delete
postings_cache_hit
postings_cache_miss
reader_open_coalesced
gc_inventory_reused
gc_list_fallback
node_crash
node_restart
liveness_heal
```

A scenario declares minimum counts. Example:

```yaml
required_mechanisms:
  - id: fence_waited_for_durability
    min: 1
  - id: stale_request_rejected_by_fence
    min: 1
  - id: seal_takeover
    min: 1
```

---

# 15. Mutation and canary suite

The test system MUST prove that it catches deliberate defects. Required canaries:

```text
ack before remote durability
send duplicate success immediately from batch-local state
send producer conflict immediately from batch-local state
ignore a fencing generation
expire a fence by wall clock while requests remain queued
bind product final append to a fresh descriptor lookup
allow phase-B split under Sealing
publish Sealed with pending topology
publish Ready before initial body durability
skip source-epoch validation on fork hop
clear fork deletion debt before parent release
advance absorbed before history flush
trim to current absorbed rather than previous safe boundary
delete topology parent objects immediately after split
serve corrupt postings as complete empty result
publish watch before append durability
use ring ownership without possession
fail open after usage-map capacity
```

Each canary MUST be caught by a named scenario within a fixed deterministic event/seed budget.

---

# 16. Deterministic performance and cost budgets

Budgets MUST be scenario-specific and reported as deltas from a named baseline.

## 16.1 Append path

```text
WAL PUTs / million records
manifest PUTs / million records
Class A / GiB ingested
records per WAL
queue generations crossed
```

## 16.2 History/postings

```text
history Class A / GiB absorbed
postings bytes / canonical bytes
canonical spans / keyed response
canonical bytes scanned / matching bytes
LISTs / 30 simulated minutes
reader opens / active-key window
```

## 16.3 Cardinality

At fixed active cardinality, increasing total known streams/keys MUST not cause unbounded:

- handles;
- metrics maps;
- key caches;
- registry cache;
- postings cache;
- dirty state;
- timers;
- background object-store traffic.

## 16.4 Fleet

Steady-state control-plane traffic MUST be measured per instance-hour and cell-hour. The simulator MUST fail O(N²) peer-read patterns above the chosen bound.

---

# 17. Failure artifacts and shrinking

A failure artifact MUST include:

```text
commit SHA
simulator schema version
serialized scenario
root seed
choice trace
trace hash
final state hash
failure fingerprint
mechanism counters
budget counters
minimal relevant object-state snapshot
```

One command reproduces it:

```bash
cargo run -p streams-sim -- replay artifacts/failures/<id>.json
```

The shrinker MUST attempt, in order:

1. remove operations;
2. remove clients;
3. remove nodes;
4. remove streams/keys;
5. remove faults;
6. shorten time;
7. reduce batches and payloads;
8. simplify configuration;
9. replace probabilistic rules with exact event-index faults.

Minimized production escapes become permanent corpus entries.

---

# 18. CI and continuous simulation

## 18.1 Pull requests

Required:

- existing full Rust suite;
- official pinned Durable Streams conformance;
- dual-surface equivalence corpus;
- all focused lifecycle regressions;
- fixed minimized simulator corpus;
- 100–1,000 bounded swarm scenarios;
- canary mutations;
- deterministic trace replay check;
- request-cost and resource budgets.

Target wall time: under 15 minutes with sharding.

## 18.2 Nightly

- 100,000+ scenarios across named modes;
- crash-point enumeration;
- liveness campaigns;
- multi-node ownership and topology;
- wide-cardinality resource campaigns;
- automatic shrinking.

## 18.3 Continuous fleet

Workers continuously test current main and selected pull requests. They upload failure artifacts and minimized reproducers. The runner implementation lives in this repository.

## 18.4 Flake policy

A deterministic scenario may not be retried to make CI green. A replay mismatch is itself a P0 simulator defect.

---

# 19. Implementation plan

## Phase A — consolidate current focused controls

1. Add one semantic `FailPointRegistry`.
2. Move existing HTTP, scaler, store, dispatch, and absorber gates onto it.
3. Require entered/release handshakes.
4. Add mechanism counters to every existing regression.
5. Add the immediate lifecycle/durability cases from the scenario catalogue marked **L1-now**.

**Acceptance:** no timing-assisted race remains in the focused suite; every historical escape has one named scenario.

## Phase B — reference model and scenario runner

1. Implement `ReferenceModel` for collection, per-key log, lifecycle, forks, segments, consumers, and history.
2. Wrap existing single-node test helpers in `ScenarioRunner`.
3. Emit canonical trace events and state hashes.
4. Serialize/replay scenario artifacts.

**Acceptance:** existing focused scenarios can run from serialized descriptions and produce stable model audits.

## Phase C — deterministic environment

1. Extract `streams-core` library.
2. Inject clock, entropy, runtime, CPU executor, and process metrics.
3. Run SlateDB and absorber work through deterministic owned executors.
4. Eliminate direct nondeterministic calls from core.

**Acceptance:** one complete single-node create→append→absorb→read→seal→delete scenario replays 100 times with identical hashes.

## Phase D — multi-node world

1. Add several `AppState` instances.
2. Add simulated router, fleet store, membership, and platform lifecycle.
3. Add process crash/pause/restart and per-node store partitions.
4. Add ownership handoff, stale route, split/merge, and liveness scenarios.

**Acceptance:** all known ring, reopen-storm, fencing, and ownership failures are deterministic corpus entries; healed worlds converge.

## Phase E — swarm, shrinking, and continuous operation

1. Boundary-biased generator.
2. Scenario modes.
3. Hierarchical shrinker.
4. Mutation suite.
5. PR/nightly/continuous runners.

**Acceptance:** every deliberate canary is caught, every failure emits a one-command minimized reproducer, and no deterministic flake remains.

---

# 20. Release gate

Before launch, the release report MUST state:

```text
official Durable Streams conformance: exact pinned result
focused regression suite: all historical escapes green
simulator fixed corpus: green with identical replay hashes
swarm budget: N scenarios, zero failures
liveness budget: N scenarios, zero non-convergence
cost/resource budgets: green
outer-loop field gate: green in at least two healthy placements
```

A cloud-platform outage may block the outer-loop gate. It does not waive local deterministic gates.

---

# 21. Deliverables

The implementation is complete when the repository contains:

```text
crates/streams-core/
crates/streams-sim/
  world.rs
  scheduler.rs
  scenario.rs
  trace.rs
  model/
  faults/
  workloads/
  auditor/
  shrink/
  modes/
artifacts/corpus/
docs/DST.md
docs/DST-SCENARIO-CATALOG.md
```

The existing `src/dst.rs` and `src/dst/dst_tests.rs` become focused consumers of `streams-sim`, not an independent architecture.

