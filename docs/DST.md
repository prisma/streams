# Deterministic Simulation Testing

## The model we are working towards

Deterministic Simulation Testing runs a system inside a simulator where
**every source of nondeterminism is controlled by a seed** — clocks,
randomness, task scheduling, I/O latency and fault placement. The same
seed replays the same execution, so a rare interleaving that fails once
can be reproduced forever.

The canonical treatment is TigerBeetle's, and it is worth reading before
touching this code:

- **[TigerBeetle: Deterministic Simulation Testing](https://docs.tigerbeetle.com/concepts/safety/#deterministic-simulation-testing)**
  — their VOPR simulates an entire cluster with faulty disks, partitioned
  networks and crashing replicas, accelerating time so one run covers far
  more than wall clock would allow. They run a continuously updated fleet
  of them.
- **[Simulation Testing For Liveness](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness)**
  — a fault-heavy run can prove safety and miss a livelock entirely. The
  fix is an explicit second phase: stop injecting faults, heal a viable
  core, and *require* convergence.
- **[A Tale Of Four Fuzzers](https://tigerbeetle.com/blog/2025-11-28-tale-of-four-fuzzers/)**
  and **[Fuzzer Blind Spots (Meet Jepsen!)](https://tigerbeetle.com/blog/2025-06-06-fuzzer-blind-spots-meet-jepsen/)**
  — one whole-system fuzzer is not enough, and several fuzzers can share a
  blind spot. Assume yours do.
- **[A Descent Into the Vörtex](https://tigerbeetle.com/blog/2025-02-13-a-descent-into-the-vortex)**
  — simulation substitutes the network and storage adapters, so a second,
  nondeterministic harness is needed to test the real ones.
- The wider ecosystem is catalogued at
  [awesome-deterministic-simulation-testing](https://github.com/ivanyu/awesome-deterministic-simulation-testing);
  FoundationDB pioneered the approach and Antithesis commercialises it.

The insight that matters for us: **distributed-systems bugs are mostly
races and crash windows, not logic errors.** A logic error fails every
time and a unit test catches it. A race fails one run in fifty, under
load — which is the profile of every defect that has cost this project
real time.

## 1. Current status — and what it is not

`src/dst.rs` + `src/dst/dst_tests.rs` is a **seeded fault-injection suite
over the real single-node data plane**. Thirty-four scenarios, ~40 seconds:

```bash
cargo test --release dst
```

It is **not** whole-system deterministic simulation, and an earlier
version of this document overstated what had been built. Precisely:

| property | status |
|---|---|
| fault placement is a pure function of the seed | **yes**, and tested |
| injected latency is realistic (8–185 ms) and costs no wall clock | **yes** — scenarios run with paused virtual time |
| scenarios drive the real `ShardEngine`, absorber and merged reader | **yes** |
| task scheduling is seed-controlled | **no** — it is Tokio's |
| whole-scenario replay yields an identical trace | **no** |
| multiple nodes, router, ring, topology | **no** |

**Multi-node whole-system simulation is the target architecture, not an
optional final step.** Early milestones may enable a single node or a
single subsystem, but they must run inside the same world, scheduler,
store registry, task-lifecycle model, reference model and tracing system
that will eventually run the whole service. The alternative — growing
`src/dst.rs` into a larger pile of seeded `#[tokio::test]` functions —
builds a second testing architecture that has to be discarded later.

## 2. The determinism contract

What the seed controls **today**:

- which object-store operation is delayed, and by how much;
- which fails before dispatch (definitely did not apply);
- which succeeds and then loses its response (may have applied).

Fault decisions are derived from `(seed, path, op, occurrence)` through an
explicit, toolchain-stable mixing function — **not** drawn in sequence
from one shared RNG. That distinction is load-bearing. With a shared
stream, the *identity* of the operation consuming each random number
depends on which task reaches the mutex first, so under concurrency the
same seed does not reproduce the same fault placement.
`fault_placement_is_a_pure_function_of_the_seed` proves the fix by issuing
the same paths in two different orders and requiring identical decisions.

(`DefaultHasher` is deliberately avoided: its output is not stable across
Rust releases, and a replay that changes with the toolchain is not a
replay.)

**The precise claim, and its limit.** For a fixed sequence of
`(path, op, occurrence)` tuples, fault decisions are toolchain-stable and
reproducible — that is what the order-independence test establishes, by
issuing *distinct* paths in two orders. It is **not** yet "concurrent
fault placement is independent of task scheduling": the occurrence index
is assigned when an operation reaches the fault store, so two concurrent
operations *on the same path* can still swap occurrence numbers if
scheduling changes. Closing that needs either a deterministic scheduler
or a semantic operation id assigned before dispatch — M1, not today.

What the seed does **not** control: task scheduling, and therefore the
order concurrent engine work interleaves in. Scenarios run on a
**current-thread runtime with paused, auto-advancing time**, which makes
ordering single-threaded and makes realistic latency free — the whole
suite runs 8–185 ms operations and finishes in seconds. That is a large
improvement on a two-worker runtime with real sleeps. It is not replay.

### The concrete blocker

Production deliberately runs SlateDB on a **separate, process-global,
multi-threaded runtime** (`main::slatedb_runtime`, reached through
`on_slatedb_rt`), because SlateDB's encode/compress work stalls the timer
and IO driver when it shares a runtime (sinmax run 12: tokio timer p99
848 ms against 3.6 ms on a raw OS thread). The absorber additionally uses
`spawn_blocking`.

So the determinism goal and the two-runtime architecture are in direct
tension: a scenario that drives the absorber escapes the test runtime's
clock and scheduler entirely. `acked_records_survive_absorption_into_history`
therefore runs multi-threaded and is explicitly **not** replayable.

This is not something to work around later. It is why milestone M1 is an
injected `TaskRuntime`/`CpuExecutor`, and why that step cannot be skipped.

## 3. Target simulator architecture

```
World
  logical clock + deterministic event scheduler
  clients (positive-space and negative-space)
  edge / router
  N Streams nodes, each with an owned task group
  platform autoscaler
  ops store | shard store | data store
  reference model
  online auditor
  mechanism coverage
  event trace
```

Each node owns its tasks explicitly — heartbeat, ring refresh, shard
opener, engine committer/acker/maintenance, absorber, scaler, usage
publisher, tail sessions. A simulated crash aborts every task owned by
that node, drops its caches and keys, abandons in-flight requests, and
does **not** close databases gracefully. A pause freezes it without
destroying memory; a restart rebuilds it from object-store state alone.
That is how a simulator finds a leaked zombie task, rather than merely
checking that a public method returns a fencing error.

The primary simulator need not serialise HTTP. Requests can cross a typed
service interface wrapped in a simulated transport that models request and
response delay and loss, duplicate dispatch, stale routing,
`409 Streams-Replay-To`, replay loops and limits, cold starts and node
unavailability. Real Axum, TLS, SDK behaviour and real providers belong to
the outer-loop harness (§13).

### Reusing `slatedb-dst`

Our pinned SlateDB (`e255cff`, v0.14.1 + [PR #1964](https://github.com/slatedb/slatedb/pull/1964))
ships **`slatedb-dst`**, an **upstream** crate — adopting it does not
deepen our fork, and it survives dropping the patch.

| slatedb-dst | maps onto |
|---|---|
| seeded deterministic current-thread runtime | replayable scheduling |
| `MockSystemClock`, `Harness::advance_time()` | injected clock |
| `FailPointRegistry` (fail-parallel) | named crash points |
| `FailingObjectStore` / `ToxicKind` | a richer fault store |
| `DbFencerActor`, `AuditorActor` | our fencing scenario and oracle |

Its top-level harness owns **one** installed `Arc<Db>`, and `swap_db()`
replaces that single database. We need many at once — ops, one per shard
prefix, one per `(stream, epoch)` history DB — and a shard handoff is not
a swap: the new node opens the same prefix, the old engine must observe
fencing, its committer, acker and absorber must terminate, its history
handles must close, its serving-map entry must clear, and the router may
stay stale meanwhile. So the plan is to build `streams-sim` above
slatedb-dst's clock, randomness, failpoint and object-store primitives
while owning a separate multi-DB registry, rather than adopting its world
model.

## 4. The production-code boundary

The core must not know whether time comes from the OS or the simulator.
The boundary is *deterministic state-machine logic* versus
*nondeterministic environment adapters* — not "production versus tests".

Capabilities to inject: `Clock` (monotonic, wall, sleep), `Entropy`
(scoped, per-actor substreams), `TaskRuntime` (spawn with an owner, cancel
by owner), `CpuExecutor` (replacing `spawn_blocking`), `ProcessMetrics`
(CPU, RSS).

The current core reaches directly for all of them — counted across `src/`
excluding the harness itself: 45 `Instant::now()`, 40 `now_ms()`, 28
`sleep`/`interval`/`tick`, 3 `rand::rng()` sites (history block nonces,
touch, http), one `spawn_blocking`, the global SlateDB runtime, and
environment variables. A `clippy.toml` disallowed-method list should
enforce the boundary once it exists, with the production adapter as the
only exception.

Two structural steps precede that: add `src/lib.rs` so the core is a
library rather than an assembly of binaries, and eventually split
`streams-core` / `streams-server` / `streams-sim` into a workspace.

## 5. The reference model

The oracle tracks **operations and attempts**, not payloads.

A client retrying an ambiguous append resends the same bytes, so payload
equality cannot distinguish "the system duplicated my write" from "I
deliberately wrote it twice". Every attempt carries `(op, attempt)`: `op`
is the logical operation, `attempt` the try. A non-idempotent retry is a
second attempt of the same operation — legitimately storable twice — while
an idempotent one must be suppressed. The suite asserts both directions,
including one test whose entire job is to confirm the oracle **permits** a
non-idempotent double-write; an oracle that flagged it would be tuned
until it stopped testing ambiguity at all.

Outcomes are three-valued, matching the spec's append contract:

| outcome | meaning |
|---|---|
| `Acked` | durably acknowledged, with the reported offset |
| `Rejected` | the server decided against it before committing anything |
| `Unknown` | it may or may not have committed — no response, or an ambiguous fencing error |

`Unknown` is what the producer-idempotence contract exists to resolve, and
the state the eu-central-1 soak wedge put every client into for twenty
minutes (docs/SOAK-REGIONS.md).

## 6. Safety invariants

Implemented today, over one shard:

| id | invariant |
|---|---|
| **I1** | every acknowledged append is readable |
| **I2** | per routing key, acknowledged order is preserved |
| **I3** | no attempt is stored twice |
| **I4** | a fenced owner acknowledges nothing |
| **I5** | a definitively rejected append never appears |
| **I6** | an idempotent producer's retry commits at most once — verified across a handoff, at the original offset, consuming no new offset |
| **I7** | every readable record belongs to an attempt the workload issued |

I4 deserves its own note. The previous version of the fencing test built a
ledger of writes attempted through the fenced owner and then **never
asserted on it** — an old owner that acknowledged every one of them would
still have passed, so the test that claimed to establish I4 could not fail
on I4. It now asserts both that the ghost ledger is empty and that the old
engine observed its own closure. The zero is meaningful because the same
workload is shown to acknowledge writes through the same engine
immediately beforehand.

Families still to build, in rough priority order:

- **Tiering** — history is an exact prefix of the durable log and the
  shard tail an exact suffix; the merge covers every offset exactly once;
  the trim boundary never passes data not durably in history; absorber
  retries are idempotent; nothing reachable from a live manifest, clone,
  union, parent or child is ever deleted; a fenced DB cannot mutate live
  state.
- **Ownership, routing, topology** — at most one owner epoch acknowledges
  per shard; a stale route costs latency but never a record; replay-to
  terminates; the segment map is always a complete non-overlapping
  partition; a crash between seal, clone, map CAS and parent retirement
  leaves a recoverable intent, never a hole; the serving map reflects
  possession, not ring preference.
- **Profiles and lifecycle** — registry epochs across create/delete/
  recreate; key versions and fingerprints; wrong keys never yield
  plaintext; crypto-erasure; queue lease uniqueness, visibility deadlines,
  retries and DLQ transitions; touches emitted only after durability;
  missed touch history forces an explicit resync.
- **Resources** — bounded open DB handles, spawned tasks, pending absorber
  entries, retry timers; nothing owned by a crashed or fenced node stays
  alive; no operation spins without advancing logical time.

## 7. Liveness

Not yet implemented, and after determinism it is the largest gap: every
scenario here asserts safety, and a system that wedges forever satisfies
all six invariants trivially. That is not hypothetical for us — the
eu-central-1 soak wedge lost no data and violated no ordering. It simply
stopped making progress.

The protocol, following TigerBeetle: run a **safety phase** with faults,
then a **liveness phase** that stops injecting them, selects a viable core
of nodes with store access, heals their connectivity, restarts them,
leaves everything else down, and advances logical time until the system
converges or exhausts a deterministic event budget. Convergence means
every request reaches a terminal state, every acknowledged append is
readable, ownership stabilises, fenced engines and absorbers exit, history
catches up, trim and GC settle, split/merge intents resolve, and desired
capacity converges.

A failure must name the stalled measure, not report a timeout:

```
liveness failure:
  seed=... scenario=...
  node_2 absorber unchanged for 75,000 events
  stream=... absorbed=912 durable_next=1148
  engine_closed=false pending_store_ops=0
```

## 8. Fault model

Implemented:

| fault | why it matters |
|---|---|
| latency (8–185 ms, the measured Tigris range) | timing windows; free under paused time |
| failure before dispatch | definitely not committed |
| success then lost response | **append ambiguity** — the only fault that manufactures it |
| explicit hold on a class | parks an operation mid-flight so a handoff can happen underneath it |

Faults are selectable per `(verb, object class)` using the **same
classifier as production telemetry** (`store_timing::classify`), so a
scenario that targets "the WAL" targets what `/v1/debug/store` calls the
WAL.

Exact coverage, since "everything is faulted" was an overclaim:

| operation | latency | error before dispatch | lost response |
|---|---|---|---|
| `put_opts` | yes | yes | yes |
| `get_opts` (incl. `head`) | yes | yes | yes |
| `delete_stream` (per item) | yes | yes | yes |
| `list_with_delimiter` | yes | yes | yes |
| `list` (streaming) | yes | yes | yes — as truncation after partial results plus a terminal error |
| `copy_opts` | yes | yes | yes |
| `put_multipart_opts` | yes | yes | **no** — a lost response would leak an undrivable upload; per-part and completion faults need a wrapped `MultipartUpload` (not built) |

Two rules keep the counters honest. **A mechanism counter increments only
when the behaviour was actually applied and became caller-visible** — the
decision is no longer counted at roll time, because that let a scenario
satisfy `require(STORE_LOST_RESPONSE)` on a verb that ignored the
decision. And streaming `list` is genuinely gated: it used to delegate
straight through, so a scenario could believe it was faulting listings
while GC and recovery walked an untouched store.

Reads are faulted for **availability only**, never for content: a store
that returns wrong bytes is outside the object-store contract, and
simulating one would test a system we neither have nor ship against.

Still to add: reset and truncated bodies, bandwidth limits and slow close,
CAS/precondition conflicts, per-node store partitions, and — importantly —
**systematic crash boundaries** rather than random ones: `before_wal_write`,
`after_durable_watermark`, `before_ack_send`, `after_history_flush`,
`before_absorbed_marker`, `before_tail_trim`, `after_shard_seal`,
`after_child_clone`, `before_topology_cas`, `before_parent_gc`,
`before_fence`, `after_engine_close` — each enumerable as "crash after the
Nth durable action". Random percentages almost never hit a narrow window;
enumeration always does. The two ad-hoc env hooks left over from the
ladder (`SCALE_FAULT_POINT`, `ABSORB_PAUSE`) should fold into that
registry, which also removes test-only env vars from the production
binary.

### A finding the fault model produced — and its correction

**Object-store faults are absorbed by SlateDB's internal retry loop.**
Measured directly: at a 95 % injected WAL error rate, with 2,329 injected
errors, **20 of 20 appends were still acknowledged**. Pinned as
`store_errors_surface_as_latency_not_as_failed_appends`.

An earlier version of this document over-generalised that into "storage
faults cannot produce client-visible ambiguity — only fencing can". That
was wrong, and the reason is instructive: the measurement used a caller
with **no deadline**. What it actually establishes is *given an
indefinitely patient caller, appends eventually succeed*. Real callers
have deadlines, and under heavy store latency the append outlives them —
the client records `Unknown` while the server commits anyway, with no
fencing event anywhere.

`storage_latency_creates_client_ambiguity_resolved_by_idempotence` now
covers that path explicitly: 3–6 s simulated WAL writes, a 1 s client
deadline, storage healing afterwards, an idempotent retry, and exactly one
committed operation. The two layers are worth keeping distinct:

| layer | who retries | client-visible? |
|---|---|---|
| object-store response lost | SlateDB, internally | no |
| **append response lost / deadline expired** | the client | **yes — this is the public ambiguity** |

So ambiguity has (at least) three sources: **client deadlines under
storage slowness**, **fencing**, and a dropped response channel. The
eu-central-1 wedge was the first of those — nothing failed, everything
took longer than any client would wait.

Idempotence is tested against the hardest of them.
`producer_state_survives_a_handoff_and_suppresses_duplicates` commits
producer sequence N through owner A at a known offset, fences A by opening
B, and replays **the identical request bytes and producer identity**
against B. B must answer `duplicate = true` **at the original offset**;
the stream must contain the operation exactly once; the duplicate must
consume no offset; and sequence N+1 must then commit. Producer state lives
in the shard log, so surviving the handoff means the new owner reading it
back from storage — and the duplicate response is the only way to observe
that.

An earlier version of this scenario could have passed without any of that
(the fenced owner could reject everything and the new owner commit each
retry fresh); it also embedded the attempt number in the payload, so a
"retry" did not resend identical bytes. Both are fixed.

### A production incident, reproduced then fixed here

The eu-central-1 soak wedge (docs/SOAK-REGIONS.md) was root-caused with
this harness. `reopen_storm_reproduces_the_eu_central_wedge` recreates
the exact loop — a WAL too big to replay inside a client's patience,
clients that disconnect, and the old open path that turned every
disconnection into a fresh, detached, full-WAL replay: 7,503 WAL GETs
for a 120-SST WAL, 11 of 12 opens fenced by their successors, serving
map still empty at the end. The fix (`sharddir::OpenGate`) is validated
by the same scenario shape: identical sick store, identical impatient
clients, 616 GETs (one replay), one open started, engine served.

Two things made the reproduction possible, both from this file's design:
per-(op, class) protocol-cost counters (the storm IS a budget violation),
and paused virtual time (a storm that takes twenty minutes of wall clock
in production takes half a second here at the real simulated latencies).

The same protocol-cost discipline now guards the metadata-read surface
(the Tigris "metadata trickle", docs/SOAK-REGIONS.md): the history
DbReader cache carries budget scenarios (repeat reads must not reopen
readers; reopens must track absorb cadence, not request rate), a
**deterministically stale** reader test (the reader's poll is pinned to
an hour so the coverage probe MUST be what saves the read — for filtered
reads too, where offset-contiguity checks cannot), an eviction-cap test,
and a reopen-after-compactor-churn budget that fails if the compactions
log stops being reaped (upstream: slatedb#1970).

## 9. Swarm and focused modes

Not yet built. The shape: a serialised `Scenario` (schema version, seed,
event budget, node/client/stream ranges, workload, per-category fault
configuration, profile distribution, release matrix) driving named modes —
`swarm` (randomise settings *and* fault distributions), `focused-fencing`,
`focused-tiering`, `focused-topology`, `focused-queue`, `liveness`,
`performance`, `compatibility`, `canary`.

Generation should be biased hard toward boundaries: 1, 2, maximum, empty,
exactly-full, one either side of every threshold. Uniform random numbers
are poor at finding boundary defects.

Workloads should include negative-space clients that send stale offsets,
repeated sequences, wrong epochs, malformed frames and out-of-order queue
acknowledgements — not only well-behaved ones.

## 10. Coverage and mutation

Every scenario carries **mechanism coverage counters** and declares which
must be non-zero. A fencing scenario in which nothing was fenced is not a
passing run, it is an invalid one — the ladder's hardest-won lesson (*"a
rung that cannot fail proves nothing"*: D3 and D4 passed their order
checks for several passes while never once triggering their mechanism).

Counters today: injected error / lost response / latency; append acked /
rejected / unknown / retried; producer duplicate suppressed; old owner
fenced; append in flight at fence; read served from history.

Lifecycle gets the same treatment. `a_fenced_owners_absorber_exits`
asserts the old owner's absorber **task actually finishes** after a
handoff, not merely that the engine reports itself closed — those are
different claims, and only the second one catches a zombie that will fight
the new owner for its history DB ("the absorption war", 2026-07-20).

The oracle's negative controls are built from failure shapes this project
actually hit: loss (the C3 shape), duplication, reordering, a rejected
write that committed, an idempotent operation stored twice — plus the
permissive control described in §5, and one that catches a
self-contradictory ledger so a harness bug cannot silently weaken every
other check.

Still to add: **canary mutations** — deliberately broken builds (ack
before the durable watermark, ignore a fencing error, advance `absorbed`
before history durability, trim to the current instead of the previously
safe boundary, delete parent SSTs immediately after a split, use ring
ownership without possession, publish a touch before durability) that the
suite must catch within a fixed seed budget. That tests the test system,
which is the only way to know the suite has not quietly become decorative.

## 11. Reproduction and shrinking

Not yet built, and a bare seed is not enough: code changes alter both
random consumption and event structure. A failure artifact needs the
commit SHA, simulator schema version, serialised scenario, root seed,
choice trace, final state hash, failure fingerprint and coverage counters
— reproduced by one command, with a hierarchical shrinker that strips
operations, clients, nodes, faults and simulated time before simplifying
configuration. Minimised escapes become permanent corpus entries.

## 12. CI

Today CI runs `cargo check`, the release test suite (which includes these
thirty-four scenarios) and one s3lite HTTP smoke test. The intended tiers:

- **Pull request** — fixed regression corpus, a bounded seed sweep,
  replay-hash comparison, canary/mutation tests, and the focused fencing,
  tiering and topology scenarios; budgets counted in simulated events, not
  wall clock.
- **Nightly** — broad swarm, focused campaigns, liveness runs,
  deterministic performance scenarios, mixed-version scenarios, automatic
  shrinking of new failures.
- **Continuous fleet** — workers running against main and selected PRs,
  implemented in this repository rather than hidden in infrastructure.

## 13. Performance, external testing, and non-goals

DST cannot measure wall-clock latency against a real provider. It *can*
assert **protocol-cost budgets**, and should: object-store GETs per 1,000
records, bytes fetched versus returned, manifest GETs per operation, WAL
and SST PUT counts, router hops and replays, open/close cycles, logical
ticks to absorb a fixed backlog, peak task and handle counts.

That corrects an earlier claim here. The 42× history-read regression
(84 → 3,528 rec/s) was described as un-findable by simulation, on the
grounds that synthetic latency would not flag one-block-per-GET as wrong.
Only half true: it would have violated a GET-count budget with no network
at all. What simulation genuinely cannot do is predict the wall-clock cost.

Keep real benchmarks for CPU, compression and encryption throughput,
allocator behaviour and RSS, real provider latency, HTTP and TLS overhead,
and Compute cold starts and edge-slot limits. Keep the docker ladder for
resource ceilings and the cloud rung for platform behaviour. And keep a
nondeterministic **outer-loop harness** running compiled binaries, real
Axum, official clients, real process kills and rolling deploys — because
simulation substitutes exactly the adapters that harness exists to test.

Two of this project's most valuable findings could not have come from
simulation as it stands: the ring-convergence data loss (371,900 records,
found by deploying to real Compute, where instances cold-start one at a
time over minutes while the ring re-forms under load) and the eu-central-1
WAL read storm (found under 30 minutes of real regional load). The first
is squarely in scope for M3 below — but only because we now know to look.

## 14. Delivery milestones

Each milestone has an acceptance criterion that can be checked, not a
feeling of completeness.

**M0 — correct the foundations.** *Done.* Scope claims corrected; I4
asserted; the handoff exercised with a request genuinely in flight; engine
scenarios inject errors and lost responses rather than latency alone; the
latency range matches measurement; records identified by attempt rather
than payload; reads go through the production merged reader; deletes
faulted; retries real; every claim in the code matches the code.

**M0.1 — the second review's correctness pass.** *Done.* I6 rebuilt as
real duplicate suppression across a handoff (identical bytes, original
offset, no offset consumed, N+1 continues) — which required the engine to
persist the commit offset alongside producer state, so a duplicate ack no
longer answers with whatever the tail happens to be. Client-deadline
ambiguity covered, correcting the over-broad "only fencing causes
ambiguity" claim. **Every engine-owned task now terminates provably**:
a level-triggered close signal broke the committer's retain cycle (it
held the engine, the engine held its channel sender, so its channel could
never close — one resident committer and engine allocation per shard move,
forever), queued appends are answered on close instead of hanging, and
`await_terminated` joins every handle rather than trusting
`is_finished()`, which is also true after a panic. Streaming `list`
faulted; mechanism counters fire only on applied behaviour. Tiering
proven by trim, not just absorption. Oracle gained I7 (issued-set
membership) and records acked offsets.

**M0.2 — the performance changes came with their invariants.** The
2026-07-27 latency work (post-ACK gather pump, durable-tail ring,
TAIL_MAX_BYTES) landed with DST coverage in the same commits:
`gather_pump_preserves_invariants_under_faults` (the pump's barrier +
gather under WAL errors/lost responses/latency, full audit, task
termination), `tail_ring_serves_live_reads_and_survives_eviction`
(2 KiB budget forces constant eviction; the audit runs over ring-backed
reads), and `tail_ring_matches_the_db_scan_and_restarts_cold` (ring/DB
byte-equality across a reopen; publish-before-ack). The ring is a cache
whose only fallback is the canonical scan — those scenarios are what
license calling it that.

**M0.3 — the reader cache became a service, and its concurrency became
provable.** Review #3's P0 was correct and production-relevant:
`acquire()` was check→probe→open→insert with no per-key single-flight, so
cold and stale stampedes scaled reader opens with request concurrency —
the metadata storm the cache exists to prevent, recreated at the moment
64 subscribers wake after an absorb. The cache is now `HistReaders`: one
instance per store (AppState-owned in production; per simulated node in
DST — the process-global OnceLock both blocked multi-node modeling and
could have served a wrong-store read), per-key single-flight slots whose
probe/open runs in a cache-owned task (caller cancellation cannot detach
it — the read-only cousin of the shard reopen storm, closed the same way
as OpenGate), three-way probe classification (`Ok(None)` is staleness;
`Err` is an error that must NOT evict the healthy reader), explicit
closes on the SlateDB runtime with a counted lifecycle, and configurable
capacity. Scenarios: 64-cold→1 open (≥63 coalesced); 64-stale→1 probe,
1 reopen, 1 open; all-callers-cancel→the open still lands and the next
read hits; probe-error retention; measured hot-set bounds (fits: N opens
total; thrash at cap<set: bounded by reads, asserted to actually thrash
so the eviction policy can't drift silently); two independent nodes over
two stores (same hash) each reading their OWN store; eviction closes run
to completion. A finding pinned along the way: a transient STORE outage
cannot surface as a probe `Err` at all — SlateDB retries reads
internally — so the reachable probe-error class is data/transform
errors, and that is what the scenario injects. Also added the I6
composite review #3 asked for: durable commit, response lost to a client
deadline, handoff, identical-bytes retry → duplicate at the recovered
original offset, exactly one copy across both owners' tenures.

**M1 — deterministic substrate.** `src/lib.rs`; injected clock, entropy,
task ownership, CPU execution and process metrics; a seeded current-thread
runtime everywhere *including* the absorber path; named per-actor random
substreams; event traces and state hashes.
*Acceptance: replaying one serialised scenario 100 times on one commit
yields an identical event-trace hash and final-state hash.*

**M2 — complete single-node data plane.** Registry, create/delete/
recreate, absorber, history, trim, GC, restarts, tails, key rotation,
profile behaviour.
*Acceptance: I1–I7 and the tiering invariants hold under process crashes
and every supported object-store fault class.*

**M3 — multi-node control plane.** Several `AppState` instances, router
and replay-to, heartbeat membership, possession versus ring, desired
capacity, crash/pause/restart, in-flight fencing handoffs, liveness mode.
*Acceptance: the known ladder failure classes exist as deterministic
regression scenarios, and the system converges after healing any viable
node/store core.*

**M4 — topology, profiles, compatibility.** Split/merge, clone/union and
parent retention, detached compactor and GC, queue/state/touch semantics,
deletion and expiry, rolling releases and mixed persisted formats,
deterministic performance budgets.

**M5 — operationalise.** Failure corpus, shrinker, mutation suite,
PR/nightly/continuous runners, failure deduplication, and the external
whole-binary harness.

## Running

```bash
cargo test --release dst
```

To widen a sweep, add seeds to the arrays in the scenario tests; each seed
is an independent execution with an independent fault schedule.

When a seed fails, the **fault schedule** replays exactly. The task
interleaving does not — see §2, and do not claim otherwise in a commit
message.
