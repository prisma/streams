# Durable Streams Rust Restructuring Work Package

**Repository snapshot:** `streams-slate (53)(1).zip`
**Snapshot commit:** `685ea0354f123864154b46e585f9c22664929763` (from the ZIP archive comment)
**Package:** `streams-slate` `0.2.0-rc.4`, Rust edition 2024
**SlateDB pin:** `0717cc1e4e9bad10a4773760f66bac4264ecf05e`
**Review posture:** behavior-preserving, pre-launch clean-cutover restructuring
**Document status:** implementation-ready work package

---

## 1. Executive verdict

The code base contains a substantial amount of hard-won correctness machinery. The implementation is not casually written: it records failure history, explicitly protects remote-durability acknowledgement, uses project-qualified identities, fences ownership and seal transitions, makes billing state atomic with append state, and has unusually broad deterministic and field-oriented verification.

That correctness work is now trapped inside an architecture that is too concentrated and too directionally coupled to remain healthy as the product grows.

The current structure should be treated as a **presumptive blocker for further large feature work** until the first restructuring phases are complete. The issue is not cosmetic file size alone. The deeper problems are:

1. **Transport is acting as the application core.** `http::AppState` is the dependency hub for product logic, billing, fleet control, scaling, SSE, audit, operator paths, and background work. Higher-level behavior depends on HTTP-owned types and helpers, while HTTP depends back on those same subsystems.
2. **Correctness state machines are encoded as incidental control flow.** Creation, sealing, deletion, lineage reads, consumer lifecycle, and commit processing are expressed through long functions, optional fields, booleans, and early returns rather than explicit transition models.
3. **The commit lane is correct in concept but overloaded in representation.** One serialized lane is required for ordering and atomicity; however, `AppendReq`, `CommitOp`, and the 2,106-line `commit_group` make unrelated command shapes and staged effects coexist in one function.
4. **Configuration and process state are ambient.** An 84-field CLI structure coexists with 71 direct environment lookups across source modules and many process-global `OnceLock`/atomic controls. That weakens determinism and makes parallel test rigs interfere unless every new feature remembers to be instance-scoped.
5. **Tests cannot provide local architectural pressure.** The package is binary-owned and one DST file is 36,922 lines with 368 test functions. That encourages private, cross-module access and makes bounded-context ownership difficult to see.

The primary “code-judo” move is therefore not “split every large file.” It is:

> **Move behavior out of transport-owned modules into typed application services and explicit state/transaction models; make HTTP, fleet loops, and test harnesses adapters around the same core.**

The second move is:

> **Keep the single commit/watermark pipeline, but replace multi-purpose request bags and a monolithic commit function with typed commands, a batch-local transaction object, and explicit applied-versus-durable effects.**

This plan deliberately avoids a rewrite. It uses small, mergeable, behavior-preserving changes, with exact wire/storage characterization and the existing correctness/cost campaigns as the migration oracle.

---

## 2. Assessment basis and limitations

### 2.1 Sources reviewed

The assessment is grounded in:

- all Rust source under `src/`;
- `README.md`, `SPEC.md`, `DESIGN.md`, and relevant operational/design documents;
- `docs/dst/IMPLEMENTATION-PLAN.md`, `docs/dst/SCENARIO-CATALOG.md`, and `docs/dst/STATUS.md`;
- the existing local/release gates and GitHub Actions workflows;
- the attached Thermo-Nuclear Code Quality Review standard.

### 2.2 What was and was not validated

This was a static architecture and maintainability review. The environment did not contain `cargo` or `rustc`, so this review did **not** compile the snapshot or rerun its test, conformance, performance, cost, or field suites. No claim in this document that a behavior is currently passing should be read as a fresh test result. Existing suites are referenced as required migration gates because they are present in the repository.

### 2.3 Review standard applied

The work package applies the attached review standard literally:

- search for restructurings that delete concepts and branches rather than merely redistribute them;
- treat files over 1,000 lines as a strong smell;
- treat ad hoc condition growth as a design problem;
- prefer direct and explicit code over wrappers or generic magic;
- clean type and ownership boundaries;
- keep logic in its canonical layer;
- preserve atomicity and avoid unnecessary orchestration.

---

## 3. Current-state evidence

### 3.1 Size concentration

A source scan found **100,890 lines across 53 Rust files**. Twenty-one Rust files are at least 1,000 lines; eighteen of those are production files outside `src/dst` and `src/bin`. Eleven files are at least 2,000 lines, and four are at least 5,000 lines.

| File | Lines | Current responsibility concentration |
|---|---:|---|
| `src/dst/dst_tests.rs` | 36,922 | shared harness, 368 tests, most correctness domains, certification cases |
| `src/http.rs` | 8,374 | runtime state, routing, raw HTTP, product bridges, lifecycle, reads, debug/operator behavior |
| `src/product.rs` | 8,132 | route parsing, auth policy, lifecycle, append, reads, scan, consumers, watches, catalog, usage |
| `src/shard.rs` | 5,472 | key codecs, stream state, engine lifecycle, append/queue/billing/maintenance commit logic, watermarks, cache/ring |
| `src/billing.rs` | 2,984 | billing vocabulary, time/identity, keys/codecs, outbox scan/drain, metrics helpers, tests |
| `src/history.rs` | 2,685 | history storage, reader service, absorber scheduling/planning/execution, config, tests |
| `src/main.rs` | 2,515 | module root, 84 CLI fields, validation, stores, bootstrap, service construction, task startup |
| `src/rollup.rs` | 2,385 | ledger materialization, month closure, artifact publication, reads, tests |
| `src/sse/feed.rs` | 2,265 | feed state, ring memory, subscriber behavior, framing-related behavior, tests |
| `src/auth.rs` | 2,148 | token verification, policy/JWKS snapshots, request principals, refresh behavior, tests |
| `src/registry.rs` | 2,131 | descriptor schema, lifecycle types, registry I/O/CAS/cache, topology, tests |

File size is not itself the architectural diagnosis, but here it accurately reflects mixed ownership.

### 3.2 Function concentration

The largest functions are workflows, not isolated algorithms:

| Function | Location | Approx. span | Structural signal |
|---|---|---:|---|
| `ShardEngine::commit_group` | `src/shard.rs:2422-4527` | 2,106 lines | 127 `if`s, 38 `match` expressions, 17 awaits; applies append, queue, billing and maintenance commands |
| `create_stream` | `src/http.rs:3235-4279` | 1,045 lines | 64 explicit returns; validation, ownership, descriptor claim, fork setup, initial content, close and readiness publication |
| `async_main` | `src/main.rs:1720-2515` | 796 lines | parse/validate/configure/open/build/start/run in one composition function |
| `fleet::start` | `src/fleet.rs:376-1137` | 762 lines | snapshot loading, policy, reconciliation, action execution and publication in one loop |
| `read_v3_lineage_inner` | `src/http.rs:7711-8276` | 566 lines | parsing, key auth, lineage planning, refresh/retry, fan-out and response mapping |
| `read_inner` | `src/http.rs:6684-7133` | 450 lines | raw query policy, key checks, billing, live behavior, storage read and HTTP output |
| `history::Absorber::start` | `src/history.rs:894-1298` | 405 lines | scheduler, work selection, gather execution and feedback |
| `product_consumer_delete` | `src/product.rs:5848-6251` | 404 lines | distributed lifecycle orchestration and response mapping |

These functions are too large to enforce local invariants by inspection. Each should become a small coordinator over named operations, not be split into arbitrary “part 1/part 2” helpers.

### 3.3 God objects and ambient state

Three structures expose the current ownership problem:

- `http::AppState` (`src/http.rs:113-308`) has **55 fields**, including registry access, shard opening, request admission, live-feed budgets, RSS pressure, fleet state, peer URLs, object stores, keys, touch feeds, auth, quotas, billing, rollup, tenant identity and internal credentials.
- `ShardEngine` (`src/shard.rs:1054-1243`) has **63 fields**, spanning storage, stream state, commit machinery, failpoints, pump metrics, ring cache, trim maintenance, postings cache, task ownership and timing.
- `Args` (`src/main.rs:74-593`) has **84 fields**, while 71 additional direct `std::env::var`/`var_os` reads are spread across production and test modules.

Merely nesting those fields into sub-structs would make the declarations shorter without fixing ownership. The work must move each responsibility behind the component that owns its invariants.

### 3.4 Dependency direction is inverted

A lexical dependency scan shows the strongest problematic edges:

| Source module | Dependency | Approx. direct `crate::...` references | Why it is unhealthy |
|---|---|---:|---|
| `product` | `http` | 107 | product behavior imports `AppState`, read/append helpers, authorization helpers, peer transport and HTTP-owned response logic |
| `billing` | `http` | 47 | billing identity/drain behavior depends on transport-owned state and even HTTP-owned randomness |
| `scaler3` | `http` | 14 | core topology policy/execution depends on transport runtime state |
| `sse/source` | `http` | 14 | subscription source behavior depends on HTTP-owned read/runtime machinery |
| `sse/session` | `http` | 10 | session state depends on transport-owned runtime |

At the same time, `http` depends on product, shard, history, billing, registry, crypto, usage and SSE. This is a conceptual cycle even though Rust module compilation can tolerate the references inside one crate.

Concrete examples:

- `product.rs` imports `crate::http::AppState` at the top and returns Axum `Response` throughout (`src/product.rs:1-35`).
- `billing::boot_id` calls `crate::http::rand_epoch()` (`src/billing.rs:100-112`), placing process identity generation under HTTP ownership.
- `fleet::start` accepts `Arc<AppState>` (`src/fleet.rs:376`).
- `billing::drain_once` accepts `Arc<crate::http::AppState>` (`src/billing.rs:1101`).

The target must make these edges impossible, not merely discouraged.

### 3.5 Type shapes encode impossible combinations

`AppendReq` (`src/shard.rs:712-773`) has 22 fields and doubles as both an actual append and a seal-fence control message. The source explicitly states that `entries` and `close` are ignored when `seal_fence_to` is set. That means the type permits combinations that have no defined meaning.

`CommitOp` (`src/shard.rs:819-899`) combines:

- append/fence behavior;
- usage acknowledgements;
- billing closure and retention;
- queue operations;
- absorber batches;
- trim pulses and trim steps.

The serialized lane is correct, but the command algebra is not.

`ReadParams` (`src/http.rs:1970-2013`) combines a deserialized public query with serde-skipped internal policy and credentials: `max_bytes`, `deliver`, `no_fanout`, two authorization leases and an internal-metering flag. A public wire DTO should not also be the trusted internal request context.

`StreamDesc` (`src/registry.rs:15-140`) is the persisted layout-4 JSON shape and contains 24 flat fields spanning immutable identity, retention, fork relations, initialization, segment topology, seal lifecycle, watches, cleanup debt and layout version. The flat persisted shape may need to remain unchanged, but it should not be the only in-memory domain model.

### 3.6 Duplicated validation and panic-based typed reconstruction

`product::canonical_name` duplicates structural rules from `tenant::CanonicalStreamName`, then uses a `debug_assert!` to keep the validators aligned (`src/product.rs:45-113`). `ProjectId::stream_ref` accepts `&str` and reconstructs a typed name with `expect` (`src/tenant.rs:161-173`). `StreamDesc::sref` similarly reconstructs with `expect` (`src/registry.rs:425-439`).

These are not current proof of a correctness defect; they are a maintainability smell. The type boundary should ensure checked values remain checked rather than repeatedly converting typed identities back into strings and reconstructing them later.

### 3.7 Test ownership is obscured

The package has no `src/lib.rs`; `src/main.rs` declares all modules. This makes the binary the crate root and encourages large in-crate test modules with private access.

`src/dst/dst_tests.rs` is 36,922 lines, contains 368 test attributes and 453 functions, and mixes durability, lifecycle, routing, queues, multitenancy, billing, live-feed, resource and certification concerns. The repository's own DST implementation plan already proposes `streams-core`, `streams-server` and `streams-sim`, and explicitly says an incremental `src/lib.rs` split is acceptable before the workspace move (`docs/dst/IMPLEMENTATION-PLAN.md:19-80`). This work package adopts that boundary instead of creating a competing one.

### 3.8 The code carries too much review archaeology locally

Many comments explain real failures and should not simply be deleted. However, production source is saturated with round numbers, incident labels and review-item narratives. The important invariant is often buried in a history lesson several paragraphs long.

The target documentation model is:

- concise local comments stating the invariant and why the immediate ordering matters;
- stable invariant IDs in code and tests;
- ADRs or an invariant catalogue containing the incident history, rejected alternatives and proof links.

This preserves the knowledge while making the implementation scannable.

---

## 4. Non-negotiable behavior and architecture constraints

The restructuring is allowed to change ownership and code shape. It is **not** allowed to weaken the following contracts.

### 4.1 Durability and commit contracts

1. A successful append response still means the relevant bytes have passed the remote durable watermark. The current append path is documented in `SPEC.md:138-165`.
2. Shared stream state is not published after a failed write.
3. Batch-local or applied-but-not-durable facts do not escape as definitive success or conflict results.
4. The single serialized shard commit lane remains the authority for operations whose relative order or atomic batch membership matters.
5. Billing state and its dirty/outbox marker remain in the same shard `WriteBatch` as committed customer append state where the billing contract requires it (`docs/OBSERVABILITY-BILLING.md`, especially the append-atomicity section around lines 304-340).
6. Applied readers and durable readers retain their current visibility distinction; durable remains the default.

### 4.2 Ownership and lifecycle contracts

1. Object-store CAS/fencing remains the authority for stale-owner rejection.
2. Creation remains crash-resumable through persisted initialization state.
3. A descriptor cannot claim Ready/Sealed while required physical work is missing.
4. Seal claim generations remain monotonic and fence stale claim-authorized writes.
5. Fork references and cleanup debts remain idempotent and restart-repairable.
6. Consumer generation/fence semantics and stale-token behavior remain unchanged.
7. One-hop peer fan-out remains bounded; an internal relay cannot recursively relay.

### 4.3 Security and multitenancy contracts

1. Every customer data identity remains project-qualified.
2. Auth occurs before expensive/body-consuming work where the current contract requires it.
3. Account authorization and stream-key possession remain separate authorities.
4. Internal/system-stream identity remains unconstructable from a customer principal.
5. Internal relays are not double-metered.
6. Typed identity checks remain fail-closed on corrupted persisted data.

### 4.4 Wire, storage and release contracts

1. Both HTTP surfaces remain behaviorally compatible with their current contracts:
   - product: `/v1/streams/{name}`;
   - pinned raw Durable Streams: `/v1/stream/{name}`.
2. Exact status codes, headers, error bodies, cursor encodings and CORS behavior are characterized before moving handlers.
3. Layout 4 key/value and descriptor encodings do not change during the restructuring.
4. No compatibility bridge for old layouts or retired product inputs is introduced. The repository is explicitly in a pre-launch hard-cutover posture.
5. Existing conformance, field, multitenancy, live-feed, capacity, cost and release gates remain required.

### 4.5 Structural constraints on the refactor itself

1. No big-bang rewrite.
2. No long-lived dual implementation selected by a feature flag.
3. No “new architecture” that merely wraps the old `AppState` in traits.
4. No trait for every component. Use concrete handles internally; introduce a trait only at a volatile/external boundary or where deterministic substitution is required.
5. No mechanical file split that leaves the same dependency graph and state model intact.
6. Do not mix large code movement with semantic changes in one pull request.
7. Every temporary adapter must have a deletion issue and should normally be deleted in the same or immediately following pull request.

---

## 5. Target architecture

### 5.1 Dependency rule

The target dependency direction is:

```text
binaries / bootstrap
        |
        v
transport adapters       background adapters       simulator adapters
        |                        |                          |
        +------------------------+--------------------------+
                                 v
                         application services
                                 |
                                 v
                          domain + policies
                                 |
                                 v
                  storage/runtime implementation ports
```

More concretely:

- `transport` knows Axum, HTTP headers/statuses, CORS and wire DTOs.
- `application` owns use-case orchestration: create, append, read, seal, delete, consumers, watches, catalog and usage.
- `domain` owns checked identities, state models, transition rules and typed errors that do not mention HTTP.
- `shard` owns the ordered transaction/commit runtime and stream-local state.
- `storage` owns physical key construction and versioned encoding/decoding.
- `background` invokes application/runtime capabilities through narrow interfaces; it does not receive HTTP state.
- `sim` drives production application/domain/shard code through injected time, entropy, stores, transport and failpoints.

Forbidden edges:

```text
application/domain/storage/shard/background -> transport
billing/history/fleet/scaler/sse             -> http::AppState
transport-specific Response/HeaderMap        -> application/domain
std::env                                      -> any module except config/bootstrap/test launcher
raw storage key tag assembly                  -> any module except storage::keyspace
```

### 5.2 Incremental source layout

Use one crate first so the team can establish clean boundaries without a workspace-wide move:

```text
src/
  lib.rs
  config/
    mod.rs
    cli.rs
    validate.rs
    profiles.rs
  domain/
    mod.rs
    identity.rs
    descriptor.rs
    lifecycle.rs
    routing.rs
    producer.rs
    consumer.rs
    cursor.rs
    error.rs
  application/
    mod.rs
    create.rs
    append.rs
    read.rs
    seal.rs
    delete.rs
    consumers.rs
    watches.rs
    catalog.rs
    usage.rs
  storage/
    mod.rs
    keyspace.rs
    codec/
      mod.rs
      tail.rs
      producer.rs
      consumer.rs
      billing.rs
      maintenance.rs
      descriptor.rs
  runtime/
    mod.rs
    services.rs
    ownership.rs
    admission.rs
    peer.rs
    telemetry.rs
    tasks.rs
    clock.rs
    entropy.rs
  shard/
    mod.rs
    engine.rs
    config.rs
    state.rs
    read.rs
    ring.rs
    maintenance.rs
    commit/
      mod.rs
      command.rs
      executor.rs
      stream_txn.rs
      append.rs
      queue.rs
      billing.rs
      maintenance.rs
      effects.rs
  transport/
    mod.rs
    http/
      mod.rs
      router.rs
      context.rs
      middleware.rs
      extract.rs
      error.rs
      raw/
        mod.rs
        lifecycle.rs
        records.rs
      product/
        mod.rs
        routes.rs
        lifecycle.rs
        records.rs
        consumers.rs
        watches.rs
        catalog.rs
        usage.rs
      internal/
        mod.rs
        segments.rs
        telemetry.rs
      operator.rs
      health.rs
  background/
    mod.rs
    fleet.rs
    scaler.rs
    absorber.rs
    billing.rs
    rollup.rs
    auth_feed.rs
  sse/
    mod.rs
    feed.rs
    registry.rs
    session.rs
    source.rs
    wire.rs
  bin/
    streams-slate.rs
    ...
```

After the dependency gates are green and the core no longer imports transport, complete the workspace move already proposed by the DST plan:

```text
crates/
  streams-core/      # domain, application, shard, storage-facing runtime
  streams-server/    # Axum, config/bootstrap, production adapters, binaries
  streams-sim/       # deterministic world, stores, transport, model, auditor
```

The workspace move is an outcome of clean boundaries, not the mechanism used to pretend those boundaries exist.

### 5.3 Runtime capabilities instead of `AppState`

`AppState` must be deleted, not renamed. Build capabilities that own coherent invariants:

```rust
pub struct ServerRuntime {
    pub streams: StreamServices,
    pub ownership: OwnershipService,
    pub admission: AdmissionController,
    pub live: LiveFeedService,
    pub telemetry: TelemetryService,
    pub peers: PeerClient,
    pub auth: AuthService,
    pub config: Arc<ServerConfig>,
}

#[derive(Clone)]
pub struct ProductApi {
    create: CreateService,
    append: AppendService,
    read: ReadService,
    lifecycle: LifecycleService,
    consumers: ConsumerService,
    watches: WatchService,
    catalog: CatalogService,
}

pub struct RequestContext {
    pub request_id: RequestId,
    pub project: ProjectId,
    pub principal: Option<RequestPrincipal>,
    pub surface: Surface,
}
```

`ServerRuntime` is a composition-root object. It must not become a new service locator passed to every function. Each transport handler and background controller receives only the application service/capability it needs.

Recommended ownership split:

| Capability | Owns |
|---|---|
| `ShardDirectory` | shard map, open gate, opener, ownership checks, engine lookup |
| `OwnershipService` | ring active set, overrides, instance identity, peer owner resolution |
| `AdmissionController` | global/project/stream/SSE/RSS limits, counters and guards |
| `PeerClient` | peer URLs, internal credentials, token refresh, bounded relay/retry policy |
| `LiveFeedService` | feed registry, memory budget, connection budget, heartbeat policy |
| `TelemetryService` | billing meter, read spool, rollup access, audit/ops emission |
| `AuthService` | immutable snapshots, verification, lease refresh/revalidation |
| `TaskGroup` | cancellation, child-task ownership, failure propagation and graceful shutdown |

### 5.4 Persisted DTOs versus domain models

Do not change the layout-4 JSON shape merely to improve internal code. Introduce an explicit boundary:

```rust
#[derive(Serialize, Deserialize)]
struct PersistedStreamDescV4 {
    // Exact current flat fields and serde behavior.
}

pub struct StreamDescriptor {
    pub id: StreamIdentity,
    pub key: KeyBinding,
    pub retention: RetentionPolicy,
    pub lifecycle: LifecycleState,
    pub routing: RoutingState,
    pub forks: ForkRelations,
    pub watches: Arc<[WatchDefinition]>,
    pub billing: BillingBinding,
    pub cleanup: CleanupDebts,
}

impl TryFrom<PersistedStreamDescV4> for StreamDescriptor {
    type Error = DescriptorCorruption;
    // Validate field combinations once.
}
```

A possible lifecycle model is:

```rust
pub enum LifecycleState {
    Initializing(Initialization),
    Open,
    Sealing(SealClaim),
    Sealed(CompletedSeal),
    SoftDeleted(Tombstone),
    Deleted(Tombstone),
}
```

The precise variants must be derived from valid current combinations; do not invent a new state transition during this refactor. Cleanup obligations such as `parent_ref_pending` should remain explicit debts rather than being hidden in a generic “status.”

### 5.5 Application results, not HTTP responses

Application services should return domain/application outcomes:

```rust
pub enum CreateOutcome {
    Created(StreamMetadata),
    JoinedInitialization(StreamMetadata),
    AlreadyReady(StreamMetadata),
}

pub enum CreateError {
    AlreadyExists,
    InitializationConflict,
    NotOwner { owner: InstanceId },
    InvalidKey,
    SourceUnavailable,
    Storage(StorageError),
}
```

Only transport maps these to product/raw status codes, bodies and headers:

```rust
impl ProductErrorMapper {
    fn create(&self, error: CreateError) -> Response { /* exact product wire */ }
}

impl RawErrorMapper {
    fn create(&self, error: CreateError) -> Response { /* exact pinned wire */ }
}
```

This makes dual-surface equivalence deliberate while preserving the surfaces' intentional differences.

### 5.6 Command/query and effect separation

Use explicit commands for state changes and immutable query inputs for reads:

```rust
pub struct AppendCommand {
    pub stream: TenantStreamRef,
    pub payload: AppendPayload,
    pub routing: RoutingInput,
    pub producer: ProducerAttempt,
    pub collection_gate: CollectionWriteGate,
    pub billing: Option<BillingAttribution>,
}

pub enum ShardCommand {
    Stream(StreamCommand),
    Consumer(ConsumerCommand),
    Billing(BillingCommand),
    Maintenance(MaintenanceCommand),
}

pub enum StreamCommand {
    Append(PhysicalAppend),
    RaiseSealFence(RaiseSealFence),
    Close(CloseSegment),
}
```

The exact variant boundaries may evolve during implementation, but a fence command must not carry ignored append fields, and an append command must not represent a trim pulse.

---

## 6. Program structure

### 6.1 Priority classes

- **P0 — architectural blockers:** baseline characterization, library/config boundary, capability ownership, transport inversion, typed descriptor/lifecycle boundary, append/read use cases, commit transaction extraction and test architecture.
- **P1 — major consolidation:** consumers, watches/live feed, billing/rollup/history, fleet/scaler, task supervision, injected time/entropy/failpoints and storage codec ownership.
- **P2 — hardening and finish:** workspace split, documentation archaeology cleanup, strict architecture gates, removal of temporary adapters and residual warning debt.

P0 does not mean “rewrite all P0 work before merging anything.” Each workstream below is decomposed so its first pull requests create seams and its final pull request deletes the old ownership.

### 6.2 Workstream map

| ID | Workstream | Priority | Main dependencies |
|---|---|---|---|
| WP-00 | Baseline, characterization and refactor guardrails | P0 | none |
| WP-01 | Library crate, configuration model and thin bootstrap | P0 | WP-00 |
| WP-02 | Runtime capabilities and `AppState` deletion | P0 | WP-01 |
| WP-03 | Identity, descriptor and error boundaries | P0 | WP-00, WP-01 |
| WP-04 | Transport/application split and one-time request admission | P0 | WP-02, WP-03 |
| WP-05 | Creation, fork and deletion coordinators | P0 | WP-03, WP-04 |
| WP-06 | Seal lifecycle state machine | P0 | WP-03, WP-04 |
| WP-07 | Append/producer application service | P0 | WP-03, WP-04 |
| WP-08 | Read/scan planner and executor | P0 | WP-03, WP-04, WP-02 |
| WP-09 | Consumer and queue application service | P1 | WP-03, WP-04 |
| WP-10 | Watch and live-feed application boundary | P1 | WP-02, WP-03, WP-04, WP-08 |
| WP-11 | Typed shard commands and batch transaction executor | P0 | WP-00, WP-03, WP-07, WP-09 |
| WP-12 | Storage keyspace and codec ownership | P1 | WP-00, WP-03, preferably WP-11 |
| WP-13 | Billing, rollup and history controllers | P1 | WP-02, WP-11, WP-12 |
| WP-14 | Fleet, scaler and background reconciliation | P1 | WP-02, WP-03 |
| WP-15 | Clock, entropy, failpoints, metrics and task supervision | P1 | WP-01, WP-02 |
| WP-16 | DST/test decomposition and simulator boundary | P0 | WP-01, WP-03, incremental with all others |
| WP-17 | Architecture enforcement, workspace move and documentation finish | P2 | all preceding workstreams |

### 6.3 Rules for every pull request

1. State the preserved invariants and affected scenario IDs in the PR description.
2. Separate mechanical movement from behavior edits. A PR may contain both only where a move cannot compile without a small signature change, and that change must be called out line by line.
3. Do not maintain two active implementations behind a runtime flag. Move one caller, delete the old branch, and proceed to the next caller.
4. Add characterization before changing ownership when an exact behavior is not already pinned.
5. Run the full existing local gate, plus focused tests for the moved use case. Commit-pipeline changes additionally run the cost/capacity legs described in WP-11.
6. Report source metrics in the PR: largest touched file, largest touched function, dependency edges removed, direct environment reads removed and temporary adapters introduced/deleted.
7. Any exception to file/function budgets must name an owner, rationale and deletion milestone. “It was already large” is not an exception.

---

## 7. WP-00 — Baseline, characterization and refactor guardrails

> **Status: DONE** (2026-09-01, gate green: 645 passed / 0 failed).
> Delivered: `docs/refactor/BASELINE.md`, `scripts/architecture-report.py`
> (warning-only CI job `architecture-report`),
> `docs/refactor/architecture-baseline.json`, `docs/refactor/WIRE-MATRIX.md`,
> `docs/refactor/test-scenario-map.json` + `docs/refactor/SCENARIO-MAP.md`,
> `docs/refactor/COMMIT-ORDER.md`, 37 layout-4 golden tests
> (`src/golden_tests.rs`), and the `TraceStore` object-store trace adapter
> (`src/dst.rs`, 7 tests). No production behavior changed.
> Recorded gaps owned by the catalogue: DUR-005, SEL-022, SEC-002 legs.
> **Corrected (PR 3.1):** scenario-map counts are generated, not
> hand-written — 189 inventoried, **138 mapped** (111 full, 25 partial,
> 2 external), **51 unmapped** (`scripts/scenario-map-report.py --check`
> validates IDs/consistency/symbols against the catalogue); the scenario
> summary is generated from the JSON. The interval-cursor test moved out
> of the golden suite into an exact deterministic pin on
> `http::interval_cursor_at` (it was never byte-exact). TraceStore's
> `delete_stream` was rewritten to delegate exactly once (its first
> implementation fanned calls out per item and could fabricate a
> success), and `reset()` now refuses while operations are in flight.
> The architecture report distinguishes test-only files via
> `#![cfg(test)]`, gained `--self-test`, and CI now prints the delta vs
> the WP-00 baseline instead of just re-printing known debt.
> WIRE-MATRIX.md is an inventory of current behavior, not a pin; routes
> gain executable characterization as they move (PR rule 4).
> **Corrected (PR 3.2, Commit A):** the review found `TraceStore`'s
> state model concurrency-unsafe — seq allocation used relaxed atomics
> BEFORE the vector lock, so concurrent starts could insert out of id
> order, completion (which derived a vector index from the id) resolved
> neither event, `in_flight` never drained, and `reset()` panicked
> forever; `reset()` itself was racy against `begin()`. Fixed: ONE
> mutex (`TraceLog`) owns id allocation, event insertion, an id→index
> map, active-lifetime tracking, completion, and reset — no
> position-from-id arithmetic remains, and ids are unique/monotonic
> (not dense; the ordering contract is trace-lock acquisition order at
> dispatch). Stream LIFETIME is now distinct from observed OUTCOME: a
> list item error records the outcome (first fact wins) but the stream
> stays active — reset refuses until exhaustion or drop retires it
> exactly once; delete streams carry the same lifetime token. Delete
> accounting is typed (`TraceEventKind::{Operation,DeleteInput,
> DeleteResult}`) instead of `detail`-string phases: `operation_counts()`
> counts an attempted delete as each Ok INPUT the inner store consumed —
> never the result observation (no more double-counted deletes) and
> never an input error; `events()` remains the full observation report.
> New battery: barrier-forced concurrent begins with reverse-order
> completions (exactly-once resolution), reset-vs-begin race soak
> (no orphan, no poisoned lock — reset releases the lock before its
> refusal panic), errored-list-still-open refusal, errored-list
> completion coherence, active-delete-stream refusal, dropped-stream
> exactly-once retirement, delete single-count ledger pins.
> Scenario-map validation is now IN the commit gate and CI
> (`scenario-map-report.py --check`), the validator checks JSON status
> equality against the catalogue and matches test symbols exactly
> (`\bfn NAME\b`, not substring), the baseline-diff keys carry counts
> (growth inside an already-flagged file/function/static is a visible
> diff), and the accepted baseline was refreshed at the PR 3.2 tree.
> **Corrected (PR 3.2.1, Commit A):** the review found async
> CANCELLATION could still poison the trace — point-operation and
> multipart futures used a plain id, so a future dropped mid-await
> never reached `finish()`, leaving a permanent Pending event and a
> stuck active entry (reset refused forever). Every begin now returns
> an owning RAII `TraceOperation` guard: `finish` consumes it exactly
> once, `note` records stream facts without retiring, and Drop retires
> with `Cancelled` as the fallback; multipart `put_part` begins at
> dispatch and moves the guard INTO the returned future so even an
> unpolled drop records Cancelled (an unpolled `async fn` records
> nothing — documented: the trace records operations that STARTED).
> Battery: poll-to-Pending-then-drop for get/put/list_with_delimiter/
> copy against a permanently-pending store, unpolled + polled
> multipart drops, reset after every cancellation, completed outcomes
> never overwritten. And the 2,750-line dst.rs catch-all is decomposed:
> `src/dst/{mod,fault_store,trace_store,trace_store_tests,runtime}.rs`
> (pure moves, mt-audit/clippy baselines regenerated for the 3 path
> moves each). Scenario symbol matching is now LEXICAL — comments,
> block comments, strings, raw strings and char literals are masked
> before the `\bfn NAME\b` match — with a 7-case `--self-test`
> (commented-out fn, fn-in-string, prefix collision, real async fn,
> attributed test, block-comment cases) run by the gate and CI. The
> architecture baseline-diff gained a GROWTH section: line-count
> deltas for over-budget files and functions present on both sides,
> so growth inside a known offender is now visible; the accepted
> baseline was deliberately NOT refreshed in PR 3.2.1 — the printed
> delta against the pre-3.2.1 snapshot (bootstrap.rs and dst.rs
> RESOLVED; validation.rs and the relocated trace tests appear; every
> move visible) is the review artifact, to be refreshed on acceptance.

### Objective

Create an executable safety perimeter before changing ownership. The refactor must be able to prove “same wire, same durable state, same object-store behavior” instead of relying only on broad end-to-end green tests.

### Why this is required

The implementation has extensive tests, but the largest test file mixes many domains and several source comments distinguish a test that proves an invariant from one that actually forces a mechanism. The DST plan already requires scenario IDs, entered-proof and non-vacuity. The restructuring should use that vocabulary as its baseline.

### Deliverables

1. Add `docs/refactor/BASELINE.md` containing:
   - snapshot commit and SlateDB pin;
   - Rust/toolchain version used by CI;
   - source-size/function-size/dependency metrics;
   - existing CI and release-gate inventory;
   - benchmark/cost artifacts selected as the comparison baseline.
2. Add `scripts/architecture-report.py` or a small `xtask` command that reports, initially without failing:
   - Rust files over 1,000 lines;
   - functions over 200 lines;
   - forbidden module edges;
   - `std::env` reads outside allowed paths;
   - Axum types outside `transport`;
   - raw key-tag construction outside `storage::keyspace`;
   - mutable process statics.
3. Build a **wire characterization matrix** for both surfaces. For each route/action, pin:
   - method and parsed route;
   - status;
   - exact required/forbidden headers;
   - error code/body shape;
   - cache and CORS headers;
   - whether body reading occurs before or after auth;
   - whether the operation is metered and by which identity.
4. Add storage golden tests for the current layout-4 encodings:
   - registry descriptor JSON, including omitted/default fields;
   - tail v2/v3 encodings;
   - record, sequence, producer, consumer, billing, dirty, maintenance, history and postings keys;
   - cursor and capability encodings;
   - corrupt/unsupported-layout rejection.
5. Add commit-order characterization around the current implementation:
   - write failure publishes no applied state;
   - applied publication precedes durable response dispatch exactly as today;
   - same-group and applied-not-durable duplicate/conflict results remain barriered;
   - billing/maintenance rows share the intended batch;
   - fence-only responses wait on the correct barrier.
6. Map current focused tests to scenario IDs from `docs/dst/SCENARIO-CATALOG.md`. Do not wait for the full simulator.
7. Add an object-store trace adapter for tests that records operation type, path and ordering without changing behavior. Redact credentials and payloads; include byte counts/hashes where useful.

### Example characterization record

```yaml
case: product-records-append-success
surface: product
request:
  method: POST
  route: /v1/streams/orders/records
  auth: valid-account-token
  key: valid
expected:
  status: 204
  headers:
    required: [Prisma-Next-Cursor, Cache-Control]
    forbidden: [Stream-Next-Offset]
  meter:
    operation: append
    count: 1
  storage:
    same_write_batch: [record, tail, producer_if_present, billing_meta, usage_dirty]
  acknowledgement:
    after_remote_durable: true
```

### Acceptance criteria

- The exact snapshot commit and SlateDB pin are machine-readable in the baseline.
- Every product/raw route involved in subsequent work has a characterization test before it moves.
- Every layout-4 key/codec moved later has a golden fixture.
- Architecture reporting runs in CI but is warning-only until WP-17.
- No production behavior changes in this workstream.

---

## 8. WP-01 — Library crate, configuration model and thin bootstrap

> **Status: DONE** (PRs 2, 3, corrective PR 3.1, and corrective
> PR 3.2, 2026-09-01).
> PR 2: `src/lib.rs` is the crate root for all production modules;
> `src/main.rs` is a thin composition root (55 physical lines after
> PR 3.2: allocator, tracing, parse → validate → runtime → run);
> bootstrap/config live in `src/bootstrap.rs`. Gate scripts and CI now
> test the stable `--lib` target. Clippy fingerprint baseline refreshed:
> 54 dead-code entries dropped — note this was NOT a pure move: making
> every module `pub` suppressed the dead-code class via public
> reachability (PR 3.1 narrowed the boundary intentionally; see below);
> 2 entries moved `main.rs`→`bootstrap.rs`; 3 newly visible
> (KeyCache len/is_empty; two private_interfaces on AppState fields).
> MT audit baseline regenerated for the six moved sites.
> PR 3: `src/config/` owns every environment read — 71 baseline read
> sites centralized into **69 distinct knob names** in `ServerConfig`
> (13 sub-configs + the 84-flag `CliArgs`), parsed ONCE at startup
> (`load()` = knob-defaults + overlay). Pinned by `config::tests`:
> default snapshot, legacy parse semantics, the COMPACT_MAX_SST_SIZE_BYTES
> divergent dual-reader, the BILLING_MODE/ROLLUP/PATH_PREFIX dual-channel
> quirks, redacted-summary secret scan, and the full CLI surface via
> clap's own registry. Fleet's two env-mutating tests now drive the pure
> `valid_peer_url_with`.
> **PR 3.1 (corrective, review-driven):** the first WP-01 cut had made
> configuration ambient in a new way and opened the whole crate. Fixed:
> (a) lib exposes ONLY the facade (`CliArgs`, `ServerConfig`,
> `Environment`, `ProcessEnvironment`, `run`) — every module is private
> again, so dead-code analysis works and the boundary is deliberate;
> (b) `config::CURRENT/install/current()` are deleted — `run(config:
> ServerConfig)` takes the owned graph, AppState carries it, and the
> only init-once holders left are documented process-wide infrastructure
> (slatedb runtime, absorb budget/pause, history/telemetry/postings
> caches, store gates, usage limits, scaler policy), seeded once from
> the composition root with the old defaults for un-seeded tests;
> (c) config parsing reads an explicit `Environment` source — tests use
> `MapEnvironment`, and exactly ONE RAII-guarded smoke test touches the
> real process environment (the earlier "no set_var in tests" claim was
> wrong; it is now true except that one smoke test);
> (d) crypto takes an explicit `FrameCompression` policy — no codec
> dependency on application config, and the duplicated
> `src/bin/shared/config_shim.rs` is deleted;
> (e) the redacted summary is an explicit projection, not a derived
> serialization, with a sentinel-secret test;
> (f) two ServerConfigs coexist in one process (test-pinned), and the
> DST rig builds its state from `ServerConfig::load`.
> Residual: `Args`→`CliArgs` moved to `config/cli.rs` as planned;
> `backpressure::Limits::from_env` renamed `from_config`; five
> `std::env` reads remain by design — main.rs TOKIO_WORKERS, the three
> cfg(test) DST_DRAIN_TRACE debug flags, dst_tests' MT_CERT_PROJECTS.
> Clippy baseline: refreshed once more for intentional visibility.
> **PR 3.2 (corrective, review-driven — Commit B):** the review found
> WP-01 incomplete: `load` returned an unvalidated `Self`, validation
> was distributed through main.rs and bootstrap AFTER process-global
> init, store opens, remote canaries and a spawned watchdog, some
> invalid paths called `process::exit`/`panic!` from library code, and
> the smoke test still mutated the process environment in-process.
> Fixed: the boundary is two typed stages —
> `ServerConfig::load(cli, env)` parses (infallible BY the pinned
> legacy-lenient parse contract: bad values fall back to defaults;
> this is the one deliberate deviation from the review's
> `load -> Result` sketch, recorded here rather than a can't-fail
> Result), then `ServerConfig::validate(self) ->
> Result<ValidatedServerConfig, ConfigError>` proves the graph pure-ly
> and returns EVERY problem (sweep residency, memprofile
> certification, both engine-settings tiers, project-id/reserved
> tenant, auth mode + files + refresh cadence, cursor-key decode,
> fleet-auth posture, record ceiling, the descriptor-free half of
> release capacity, certification delay) and carries the derived
> values (tenant, auth mode, cursor key, delay) so bootstrap cannot
> re-derive differently. `run()` accepts ONLY `ValidatedServerConfig`
> — validation precedes every startup side effect by type; its first
> act is the one OS preflight (raise_nofile + descriptor clamp), then
> the effective-config log, then process-global init/stores/canaries.
> No `process::exit` or config `panic!` remains in library code (the
> binary prints `ConfigError` and chooses the exit status).
> `run()` now ENFORCES its transitional process-singleton contract
> loudly (second invocation errors, naming the WP-02 fix) instead of
> the config-mod comment overclaiming independent instances — the
> comment now claims value-coexistence only.
> Hermeticity: NO in-process `set_var`/`remove_var` anywhere; the
> process-environment smoke test and the new CLI-fixture drift guard
> run their subjects in subprocesses whose environments are
> established (or cleared) before start; ordinary config tests use the
> explicit `CliArgs::deterministic()` fixture, so ambient Clap `env=`
> variables cannot leak into them (the drift guard proves the fixture
> equals a scrubbed-environment parse).
> Doc corrections: main.rs line count, lib.rs facade example
> (by-value + validate), TraceStore ordering contract, seq
> non-density, and the environment module's no-mutation claim (true
> again).
> **PR 3.2.1 (Commit B) — the validated boundary is complete:** the
> review found `ValidatedServerConfig` meant "some major validations
> ran", not "startup cannot hit another configuration failure". Now:
> CELL_ID is proven by `validate()` into a typed
> `tenant::CellId` that `Registry::new` CONSUMES (the
> `expect("valid cell id (checked at startup)")` is deleted, not
> softened); the effective INITIAL_SHARDS count is resolved (incl.
> the fleet-mode default) and proven nonzero-power-of-two as
> `InitialShards` — `load_or_init_topology` takes the type and its
> assert is gone; MAX_REQUEST_BODY_BYTES bounds are proven by
> `validate_body_ceiling` (the 32-MiB wire pin moved to
> `protocol_pin.rs` beside the other pins) and
> `http::install_max_body_bytes` is INFALLIBLE; the pure
> BILLING_MODE=required prerequisites (usage key, no placeholder
> identities) are proven before any store opens (the spool/rollup
> OPENS remain store I/O in bootstrap). Validation itself moved to
> `src/config/validation.rs` (+`validation_tests.rs`) — bootstrap
> consumes `ValidatedServerConfig`, it no longer defines it, and
> bootstrap.rs dropped from 2,126 to ~915 lines (under budget for the
> first time). A 17-test central-validator matrix drives
> `ServerConfig::validate()` itself: every rejection category (cell
> id, shards, body limit, billing identity, project id, auth files,
> refresh cadence, sweep residency, cursor key, record ceiling, fleet
> auth, cert delay, memprofile, engine settings), the multi-error
> collection contract, the fleet-shard-default resolution, and
> deterministic-defaults-are-valid. The singleton latch is renamed
> `RUN_WAS_INVOKED` and documented as a once-EVER process latch (a
> failed first run consumes the right to call run() again; no
> reset-on-error — WP-02 removes the holders instead). Behavioral
> note, recorded: a non-power-of-two INITIAL_SHARDS previously
> asserted only on the FRESH-topology path; it now refuses at
> validation even when a topology exists — the stricter posture is
> deliberate (a malformed value should never boot).

### Objective

Turn the server into a library-driven application with a thin binary composition root, and make configuration a parsed, validated value rather than ambient process state.

### Current evidence

- All modules are declared by `src/main.rs:3-40`; no `src/lib.rs` exists.
- `Args` spans `src/main.rs:74-593` with 84 fields.
- `async_main` spans `src/main.rs:1720-2515` and performs parsing, validation, storage canaries, component construction and task startup.
- A source scan found 71 direct environment lookups across modules; the largest concentrations are `main.rs`, `billing.rs`, `history.rs`, `http.rs` and `fleet.rs`.

### Target design

```rust
// src/config/mod.rs
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub storage: StorageConfig,
    pub shard: ShardConfig,
    pub history: HistoryConfig,
    pub auth: AuthConfig,
    pub fleet: FleetConfig,
    pub admission: AdmissionConfig,
    pub live_feed: LiveFeedConfig,
    pub billing: BillingConfig,
    pub telemetry: TelemetryConfig,
    pub runtime: RuntimeConfig,
}

impl ServerConfig {
    pub fn load(cli: CliArgs, env: &dyn Environment) -> Result<Self, ConfigErrors>;
    pub fn validate(&self) -> Result<(), ConfigErrors>;
    pub fn redacted_summary(&self) -> RedactedConfigSummary;
}
```

Use Clap `flatten` groups so the CLI remains compatible while ownership becomes visible:

```rust
#[derive(Parser)]
struct CliArgs {
    #[command(flatten)]
    storage: StorageArgs,
    #[command(flatten)]
    shard: ShardArgs,
    #[command(flatten)]
    fleet: FleetArgs,
    // ...
}
```

### Implementation instructions

1. Add `src/lib.rs` and move module declarations there. Keep the existing binary name and argument surface.
2. Create `src/bin/streams-slate.rs` or reduce `src/main.rs` to allocator setup, config loading, runtime construction and `server.run()`.
3. Extract current defaults **without changing them**. Pin every default with config snapshot tests before deleting the original definition.
4. Centralize all environment reads in `config::load` or explicit binary/test launchers. Where a module currently lazily reads an environment variable via `OnceLock`, pass the parsed value in its config instead.
5. Aggregate validation errors before opening object stores or binding the listener. Preserve the existing fail-closed checks, including settings that would otherwise create a healthy-looking but permanently failing data plane.
6. Keep storage canaries and startup ordering, but express them as bootstrap stages:

```rust
let config = ServerConfig::load(CliArgs::parse(), &ProcessEnvironment)?;
config.validate()?;
let stores = StoreSet::open(&config.storage)?;
stores.run_startup_canaries(&config.storage.canary).await?;
let server = ServerBuilder::new(config, stores).build().await?;
server.run().await
```

7. Emit one redacted effective-configuration event after validation. Never log access keys, tokens, stream keys, cursor keys or HMAC material.
8. Replace test-only environment mutation with a `TestConfigBuilder`. Process-environment tests must be serialized and limited to the config parser itself.
9. Keep command-line compatibility tests that compare old and new parsed values for every option.

### Acceptance criteria

- `src/lib.rs` is the crate root for production logic.
- the server binary/composition root is at most 250 lines, excluding generated CLI help text;
- no direct `std::env::var`/`var_os` remains outside `config`, binaries and explicitly marked config-parser tests;
- startup ordering and fail-closed checks are pinned;
- all existing command-line names, environment names and defaults remain unchanged unless a separate product decision explicitly changes them;
- unit and integration tests can instantiate the server from a value without mutating global environment state.

---

## 9. WP-02 — Runtime capabilities and deletion of `http::AppState`

### Objective

Move runtime state to components that own its invariants and remove transport as the service locator for the entire process.

### Current evidence

`AppState` at `src/http.rs:113-308` owns 55 fields from at least eight domains. Product, billing, fleet, scaler, SSE, audit and operator code depend on it. Several comments note that previous process-global state coupled parallel HTTP rigs, which shows that instance ownership is already an active correctness concern.

### Target ownership

Create concrete, cloneable handles with narrow methods:

```rust
#[derive(Clone)]
pub struct ShardDirectory {
    inner: Arc<ShardDirectoryInner>,
}

impl ShardDirectory {
    pub async fn engine_for(&self, stream: &TenantStreamRef) -> Result<Arc<ShardEngine>, OwnershipError>;
    pub fn effective_owner(&self, shard: &ShardPrefix) -> Option<InstanceId>;
}

#[derive(Clone)]
pub struct AdmissionController {
    global: GlobalAdmission,
    projects: ProjectAdmission,
    streams: StreamAdmission,
    memory: MemoryAdmission,
    subscriptions: SubscriptionAdmission,
}
```

Suggested extraction from current fields:

- `registry`, typed identity/config and descriptor cache -> `RegistryService`;
- `shard_prefixes`, `shards`, `gate` -> `ShardDirectory`;
- `ring_active`, `ring_overrides`, `instance_name` -> `OwnershipService`;
- `peer_urls`, fleet token source/credentials -> `PeerClient`;
- inflight/RSS/project/stream/SSE gates and shed counters -> `AdmissionController`;
- `live_feeds`, feed budget/ring/heartbeat/connection counters -> `LiveFeedService`;
- billing/read spool/rollup/audit/ops -> `TelemetryService`;
- auth snapshots and quotas -> `AuthService` and `QuotaService`;
- process task handles/cancellation -> `TaskGroup`.

### Implementation instructions

1. Introduce capability types around existing fields without changing call behavior.
2. Move methods to the owner before moving fields. For example, move `effective_owner` and engine lookup policy to `OwnershipService`/`ShardDirectory` before changing handlers.
3. Add per-surface adapter state rather than handing the composition root to every handler:

```rust
#[derive(Clone)]
struct ProductHttpState {
    api: ProductApi,
    auth: AuthService,
    admission: AdmissionController,
}

#[derive(Clone)]
struct RawHttpState {
    api: RawApi,
    auth: WorkloadAuthService,
    admission: AdmissionController,
}
```

4. Convert background entry points one at a time. `fleet::start`, billing drain, scaler execution and SSE source code must take narrow capabilities/config, never `AppState`.
5. Do not replace direct field access with dozens of `AppState` getter methods. That preserves the service locator and adds indirection.
6. Move metrics with the component whose operation they count. Export them through immutable snapshots for operator endpoints.
7. Delete each field from `AppState` when its final caller is migrated. Keep a CI counter showing the field count decreasing.
8. Delete `AppState` once no caller needs it. Do not retain an alias or “legacy state” wrapper.

### Acceptance criteria

- `crate::http::AppState` no longer exists.
- no production module outside `transport/http` imports a transport module;
- no handler receives the full `ServerRuntime` unless it is the top-level router/bootstrap function;
- each background loop can be instantiated in a test with only its required capabilities;
- metrics and mutable budgets are instance-scoped unless they are demonstrably process-wide by contract;
- parallel server rigs do not share mutable runtime controls.

---

## 10. WP-03 — Identity, descriptor and error boundaries

### Objective

Make invalid identity/lifecycle combinations unrepresentable inside the application core, while retaining the exact persisted layout-4 representation and wire behavior.

### Current evidence

- `product::canonical_name` duplicates structural identity validation and relies on a debug assertion to agree with `CanonicalStreamName` (`src/product.rs:45-113`).
- `ProjectId::stream_ref(&str)` reconstructs a checked name using `expect` (`src/tenant.rs:161-173`).
- `StreamDesc::sref` and `ref_in_project` reconstruct typed names from strings (`src/registry.rs:425-455`).
- `StreamDesc` combines identity, lifecycle, routing, fork, watch and billing state in one persisted struct (`src/registry.rs:15-140`).
- many core operations use `Result<_, String>` or return HTTP responses directly, losing structured cause information.

### Target types

1. Retain `CanonicalStreamName` as the structural identity type.
2. Add a product addressability type for the extra product-route restrictions:

```rust
pub struct ProductStreamName(CanonicalStreamName);

impl TryFrom<&str> for ProductStreamName {
    type Error = ProductNameError;
    // structural validation + reserved suffix/subresource-shape rules
}
```

3. Change constructors to accept checked values:

```rust
impl ProjectId {
    pub fn stream_ref(&self, name: CanonicalStreamName) -> TenantStreamRef;
}

impl StreamDescriptor {
    pub fn ref_in_project(&self, name: CanonicalStreamName) -> TenantStreamRef;
}
```

Use references/cheap clones as appropriate; the key rule is that a request/persisted string is validated once at its boundary.

4. Split the persisted descriptor DTO from the validated domain descriptor as described in section 5.4.
5. Model lifecycle and cleanup debt explicitly. Validate impossible combinations during decode and return `DescriptorCorruption`, not a panic.
6. Define use-case-specific typed errors. Avoid one giant `StreamsError` with dozens of unrelated variants; share only genuinely common causes such as ownership, storage and corrupt descriptor errors.
7. Introduce typed time and identity where confusion is costly:
   - `UnixMillis`/`TrustedNow` for server-controlled lifecycle/billing time;
   - `StreamEpoch`, `SegmentId`, `SealGeneration`, `ConsumerGeneration`;
   - `RequestId`/`OperationId` rather than free-form strings at internal boundaries.
8. Keep serialization at explicit adapters. Domain types should not derive `Deserialize` merely because the persisted/wire DTO does.

### Descriptor conversion rules

The conversion from `PersistedStreamDescV4` must verify at least:

- path identity equals embedded project/name;
- name is structurally canonical;
- layout version is exactly 4;
- epoch and fingerprints decode to the required widths;
- lifecycle booleans/options form a valid current state;
- a sealing state has the required generation/intent fields;
- stored fork/DLQ references are canonical and project-relative by contract;
- terminal cleanup debt remains visible and recoverable.

Do not “repair” corrupt combinations silently. Return the same fail-closed class currently used for corrupt descriptors.

### Acceptance criteria

- request and persisted identity inputs are validated once, then remain typed;
- no `expect("caller passed a canonical...")`-style panic remains on request/persisted identity flows;
- the persisted JSON bytes/default/omission behavior remain golden-compatible;
- application/domain errors contain no Axum `StatusCode`, `HeaderMap`, `Body` or `Response`;
- product and raw adapters preserve their exact current error mappings;
- lifecycle transition code pattern-matches explicit states instead of repeatedly coordinating `deleted`, `soft_deleted`, `init`, `sealed` and `sealing` booleans ad hoc.

---

## 11. WP-04 — Transport/application split and one-time request admission

### Objective

Make HTTP a thin adapter: parse once, authorize once, build a trusted request context, invoke one application use case, and serialize one result.

### Current evidence

- `product_entry` (`src/product.rs:1017-1308`) handles OPTIONS, repeats authorization, selects tenant identity, rejects retired inputs, parses route/verb, checks quotas, invokes use cases, inspects HTTP success to meter operations, attaches subscription guards and builds errors.
- The source explicitly says the product auth gate runs twice by design (`src/product.rs:552-556`). Defense in depth is valuable, but repeating a side-effect-free auth computation inside the application entry point is compensating for an unclear trust boundary.
- Product handlers return Axum responses and call raw HTTP helpers from `http.rs`.

### Target request pipeline

```text
Axum route/wildcard
  -> exact route parser
  -> preflight handler OR auth middleware
  -> tenant/request context extractor
  -> body limit + body decode
  -> quota/admission guard
  -> application command/query
  -> outcome metering
  -> surface-specific response mapper
```

The route parser can remain a pure suffix parser because hierarchical names make ordinary static routing awkward. What must disappear is the subsequent giant method/verb dispatcher and duplicated cross-cutting policy.

### Implementation instructions

1. Move pure route types/parsing to `transport/http/product/routes.rs`. Return a typed parse error rather than an already-built response.
2. Resolve method + `:verb` into an action during parsing:

```rust
pub enum ProductAction {
    Create,
    Metadata,
    Delete,
    Seal,
    Scan,
    Append { batch: bool },
    Read { mode: ReadMode },
    Consumer(ConsumerAction),
    Watches(WatchAction),
    Usage,
}

pub struct ProductRouteMatch {
    pub stream: ProductStreamName,
    pub action: ProductAction,
}
```

This removes a second method/verb match later.
3. Make authorization produce a trusted `RequestContext` exactly once. Direct unit tests should call the application service with a constructed context; they should not call a second hidden auth gate.
4. Preserve preflight-before-auth behavior and exact CORS headers in a dedicated preflight handler/middleware.
5. Ensure body-reading routes authorize and pass survival/admission gates in the same order as the current security contract. Add a body probe test proving an unauthorized request does not consume a large body where that is required.
6. Split wire DTOs from application inputs. For reads, replace `ReadParams` with:

```rust
#[derive(Deserialize)]
pub struct RawReadQuery {
    offset: Option<String>,
    format: Option<String>,
    live: Option<String>,
    timeout: Option<String>,
    key: Option<String>,
}

pub struct ReadRequest {
    stream: TenantStreamRef,
    start: ReadStart,
    mode: ReadMode,
    visibility: ReadVisibility,
    page_budget: PageBudget,
    fanout: FanoutPolicy,
    authorization: ReadAuthorization,
}
```

No trusted internal field should be `#[serde(skip)]` on a public query type.
7. Move operation metering to the application result boundary. Do not infer semantic success by checking `Response::status().is_success()` after serialization.
8. Keep product and raw response mappers separate and small. Share only a transport-neutral `ReadPage`, `AppendOutcome`, `MetadataView`, etc.
9. Replace `product_entry` with per-action handlers or a small typed dispatcher. The dispatcher should contain no business branches and should be under 100 lines.
10. Add a compile/architecture check that application/domain modules cannot import `axum` or `transport`.

### Example handler

```rust
async fn append_records(
    State(state): State<ProductHttpState>,
    Extension(ctx): Extension<RequestContext>,
    ProductRoute(route): ProductRoute,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let wire = ProductAppendWire::parse(&headers, body);
    let command = match wire.and_then(|w| w.into_command(&ctx, route)) {
        Ok(command) => command,
        Err(error) => return state.errors.append_input(error),
    };

    match state.api.append.execute(command).await {
        Ok(outcome) => state.responses.append(outcome),
        Err(error) => state.errors.append(error),
    }
}
```

The example is intentionally direct. Do not hide every operation behind a generic “execute route” framework.

### Acceptance criteria

- product authorization is computed once per request;
- route classification is exact and pure, and method/verb resolution happens once;
- application services receive trusted typed context, never raw bearer headers;
- product/raw handlers contain transport parsing and mapping, not lifecycle/commit orchestration;
- product code no longer imports `crate::http`;
- response-status inspection is not used to decide billing/quota semantics;
- exact wire characterization remains unchanged.

---

## 12. WP-05 — Creation, fork and deletion coordinators

### Objective

Replace the 1,045-line `create_stream` workflow and related fork/delete helpers with explicit, resumable coordinators whose persisted states and cleanup debts are visible in their types.

### Current evidence

`create_stream` (`src/http.rs:3235-4279`) currently owns:

- reserved-name/key/config validation;
- ring-owner validation before registry mutation;
- create request identity;
- descriptor creation or exact-retry/takeover decisions;
- fork source validation and reference installation;
- initial content materialization;
- optional close;
- Ready publication;
- compensation and orphan-reference repair.

The persisted `InitState` is already a durable recovery marker (`src/registry.rs:215-235`). Fork descriptors also contain exact source incarnation, fork boundary and idempotent child-reference identity (`src/registry.rs:273-300`). The code has the right raw ingredients; the missing piece is an explicit coordinator around them.

### Target design

```rust
pub struct CreateService {
    registry: RegistryService,
    shards: ShardDirectory,
    forks: ForkService,
    appends: AppendService,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
}

pub struct CreateCommand {
    pub stream: TenantStreamRef,
    pub key: StreamKey,
    pub config: CreateConfig,
    pub initial_content: Option<InitialContent>,
    pub fork: Option<ForkRequest>,
    pub close_after_create: bool,
    pub request_id: CreateOperationId,
}

pub enum CreateStep {
    Validate,
    ClaimInitialization,
    InstallSourceReference,
    MaterializeInitialState,
    CloseIfRequested,
    PublishReady,
    RepairCleanupDebt,
    Complete,
}
```

`CreateStep` need not be persisted as a new field if the current descriptor plus physical facts already determine the next step. It is a coordinator decision model, not a demand for a storage migration.

### Implementation instructions

1. Extract deterministic request parsing/validation from `create_stream` into pure functions. Invalid key, TTL/expiry, watch, fork and body requests must fail before registry mutation, matching scenario `CRT-011`.
2. Define a complete `CreateOperationId` from the same semantic inputs currently used. Pin it with tests corresponding to `CRT-012`.
3. Extract registry claim logic:

```rust
pub enum InitClaimOutcome {
    Created { descriptor: StreamDescriptor },
    Resume { descriptor: StreamDescriptor },
    JoinedActive { descriptor: StreamDescriptor },
    Conflict(CreateConflict),
}
```

The claim operation must remain incarnation-aware and cancellation-safe.
4. Make every coordinator step idempotent and independently retryable. A restart calls `resume(command)` and derives the next owed action from persisted descriptor state plus exact physical readback.
5. Move fork-source reference install/release into `ForkService` with typed outcomes:

```rust
pub enum SourceRefOutcome {
    Installed,
    AlreadyInstalled,
    SourceGone,
    SourceIncarnationChanged,
}
```

6. Preserve the current debt model. If the child becomes unavailable after source-reference installation, either release immediately or persist/retain the cleanup debt so ordinary deletion/recovery can finish it. Never rely on an in-memory compensation future being allowed to run.
7. Route initial content through the same append/commit service used elsewhere, with a synthetic idempotence identity and the current durability barrier. Do not create a second “creation write” implementation.
8. Publish Ready through an exact-incarnation CAS only after all owed physical work is durable. An old creator must not publish Ready into a recreated name (`CRT-005`).
9. Extract deletion into `DeleteService`. Separate:
   - access-state transition/tombstone;
   - logical close timestamp;
   - child/parent reference decisions;
   - billing close debt;
   - bounded physical cleanup.
10. Express cleanup as named debt records/outcomes, not catch-all retries. A 2xx/204 must mean the same completion boundary it means today.
11. Remove HTTP response construction from lifecycle coordination. Product/raw adapters map typed outcomes independently.
12. Delete `create_stream`, `product_delete` bridge helpers and obsolete compensation branches once all callers use the coordinator.

### Example coordinator loop

```rust
pub async fn execute(&self, command: CreateCommand) -> Result<CreateOutcome, CreateError> {
    let mut state = self.claim_or_resume(&command).await?;

    loop {
        state = match self.next_step(&command, &state).await? {
            CreateStep::InstallSourceReference => self.install_source_ref(&command, state).await?,
            CreateStep::MaterializeInitialState => self.materialize(&command, state).await?,
            CreateStep::CloseIfRequested => self.close(&command, state).await?,
            CreateStep::PublishReady => self.publish_ready(&command, state).await?,
            CreateStep::RepairCleanupDebt => self.repair_debt(&command, state).await?,
            CreateStep::Complete => return Ok(CreateOutcome::Ready(state.metadata())),
            CreateStep::Validate | CreateStep::ClaimInitialization => unreachable!(),
        };
    }
}
```

The loop is bounded by monotonic persisted progress. Tests must fail if a step repeats without a progress measure.

### Acceptance criteria

- no lifecycle coordinator function exceeds 200 lines; individual transition functions target 80 lines;
- every create crash boundary in the scenario catalogue maps to a named step/failpoint;
- exact retries join/resume, different requests conflict, and wrong-key retries cannot resume;
- initial content and close success remain remote-durability-barriered;
- old-incarnation creators/deleters cannot mutate replacements;
- fork reference install/release and cleanup debt are idempotent;
- `src/http.rs` contains no creation/fork/delete business workflow.

---

## 13. WP-06 — Seal lifecycle state machine

### Objective

Turn seal claim, takeover, fencing, final-record obligation, physical segment close and terminal publication into an explicit state machine with one canonical transition implementation.

### Current evidence

The persisted model is already sophisticated:

- `SealIntent` distinguishes empty seals from final-bearing seals and records whether the final is durably committed (`src/registry.rs:180-199`);
- `SealState` carries operation ID, claim time and monotonic claim generation (`src/registry.rs:147-178`);
- the product implementation has separate claim/takeover/resume helpers and `run_seal` (`src/product.rs`, especially around `1714-3060`).

The problem is that correctness is distributed across several long functions, descriptor mutations and HTTP responses. The rules are hard to prove because readers must reconstruct the state machine from incidental branches.

### Target model

```rust
pub enum SealPhase {
    Open,
    Claimed(SealClaim),
    FinalOwed(SealClaim),
    FinalDurable(SealClaim),
    SegmentsClosing(SealClaim),
    Publishable(SealClaim),
    Sealed(CompletedSeal),
}

pub enum SealAction {
    AcquireOrRenewClaim,
    FenceSupersededGeneration,
    AppendFinal,
    MarkFinalDurable,
    CloseSegments,
    PublishSealed,
    Complete,
}
```

These can be derived views over the current persisted descriptor and segment facts. Do not add transient persisted phase markers unless exact readback cannot determine progress.

### Implementation instructions

1. Create `SealService` with one public `execute(SealCommand)` method for both plain and final-bearing seals.
2. Parse and deterministically validate body, routing key, producer identity, content type, capacity and key version **before** publishing an intent, preserving `SEL-001`.
3. Represent “no final record” separately from a final JSON `null`. Never use `Option<Value>` in a way that conflates the two (`SEL-002`).
4. Build a framed, complete `SealOperationId`; retain current hash semantics and pin all fields with golden tests.
5. Extract `claim_or_resume` as a typed registry mutation. Outcomes must distinguish:
   - acquired new claim;
   - renewed same operation with a new generation;
   - joined/resuming same operation;
   - another live operation owns the claim;
   - stale operation eligible for takeover;
   - already sealed by same/different operation;
   - incarnation changed/missing.
6. Make takeover a source-owned sequence:
   - reserve a generation;
   - issue ordered seal-fence commands to every relevant segment;
   - inspect whether any old-generation close already became durable;
   - install the replacement claim only under the exact descriptor version/incarnation;
   - never let an aborted reservation make a future legitimate generation stale.
7. Route final record writes through `AppendService`/typed shard command with `SealAuthorization { generation }`. The committer remains the physical authority that rejects stale generations before record or close staging.
8. Make “final committed” publication a distinct CAS after durable append acknowledgement. A plain seal may never complete an intent that still owes a final record (`SEL-005`).
9. Close all current segments through a bounded, retryable operation. Re-read topology when required so a racing split/merge does not leave a writable successor outside the seal.
10. Publish terminal Sealed only after exact readback/receipts prove required final and close work. Preserve the current one-time sealed event/EOF behavior for subscribers.
11. Remove seal decisions from HTTP handlers and from unrelated append branches. The append path should receive a typed collection write gate, not inspect descriptor booleans ad hoc.
12. Add one transition table to `docs/invariants/seal.md`, with links from failpoints/tests.

### Transition table example

| Observed state | Matching operation | Required action | Allowed public result |
|---|---|---|---|
| Open | any valid seal | acquire claim | pending/retryable until work completes |
| Claimed, final owed | exact same operation | append/verify final | no terminal success yet |
| Claimed, final owed | plain/different seal | refuse or takeover only under defined expiry/fence policy | never publish Sealed |
| Final durable, segments open | exact/resumer | close segments | pending/retryable |
| All segments durably closed | exact/resumer | publish Sealed CAS | success after publication |
| Sealed by same operation | retry | idempotent success | success |
| Sealed by different final | retry | semantic conflict | conflict |

### Acceptance criteria

- one canonical seal coordinator owns every transition;
- all seal scenario catalogue cases map to a state/action pair;
- stale claim generations cannot write or close after takeover;
- a final-bearing seal cannot lose or duplicate its final record;
- descriptor terminal state cannot lead physical segment state;
- subscriber terminal notification remains exactly once;
- no seal coordinator function exceeds 200 lines and no transition function exceeds 100 lines.

---

## 14. WP-07 — Append and producer application service

### Objective

Create one transport-neutral append use case that owns validation ordering, producer/idempotence semantics, routing resolution, admission, billing attribution and mapping to a typed physical shard command.

### Current evidence

Product append behavior is spread across `product_append`, `product_append_inner`, sealing-specific append helpers and `translate_append_response` (`src/product.rs:3074-3605`). Raw append behavior and shared helpers live in `http.rs`. The physical `AppendReq` contains valid append data plus fence-control and deferred-error fields (`src/shard.rs:712-773`).

Some of the complexity is essential: duplicate detection must precede certain validation errors, same-group facts must be barriered, producer lineage crosses split predecessors, and sealed collections must allow exact duplicates while rejecting genuinely new sequences. The workstream must model those rules rather than “simplify” them away.

### Target layers

```text
product/raw wire parser
  -> AppendCommand (semantic request)
  -> AppendService (auth already trusted)
       validate deterministic fields
       resolve descriptor/topology/segment
       build producer + duplicate policy
       obtain admission guard
       build PhysicalAppend
       submit to shard lane
       convert AppendAck/AppendFailure to application outcome
  -> surface response mapper
```

### Type recommendations

```rust
pub enum AppendPayload {
    Valid(Vec<RecordInput>),
    DeferredError(DeferredAppendError),
}

pub enum ProducerAttempt {
    None,
    Sequenced {
        id: ProducerId,
        epoch: ProducerEpoch,
        sequence: ProducerSequence,
        request_hash: Option<RequestHash>,
        predecessor_segments: Arc<[SegmentHash]>,
    },
}

pub enum CollectionWriteGate {
    Open,
    SealAuthorized { generation: SealGeneration },
    DuplicateOnly { reason: SealedReject },
}

pub struct PhysicalAppend {
    pub segment: SegmentIdentity,
    pub records: Vec<PreparedRecord>,
    pub producer: ProducerAttempt,
    pub gate: CollectionWriteGate,
    pub billing: Option<BillingAttribution>,
    pub touch: Option<TouchEffects>,
    pub response: AppendReply,
}
```

`DeferredError` is retained as a first-class concept because current semantics intentionally check exact producer duplicates before returning content-type/body errors. The improvement is to name that ordering in the type, not remove it.

### Implementation instructions

1. Write an append decision table covering raw/product, ordinary/batch/final writes, producer/no-producer, open/sealing/sealed, single/dynamic topology and local/remote owner.
2. Extract common semantic parsing into transport-neutral records without merging product/raw wire names.
3. Move request-hash construction to a single framed implementation and pin exact product producer-hash behavior.
4. Resolve routing and predecessor lineage in `RoutingService`; return a typed segment target and ownership result.
5. Apply auth and admission once before submitting. Keep per-stream guard lifetime tied to the actual operation future.
6. Construct billing/touch/usage effects from the descriptor and trusted request context; do not let transport pass arbitrary identities into the committer.
7. Split fence control out of `AppendReq`. A `RaiseSealFence` command has its own request and response type.
8. Replace optional booleans/options that encode exclusive modes with enums as above.
9. Preserve duplicate-before-deferred-error ordering inside the commit applicator. Add comments with invariant IDs rather than incident chronology.
10. Return a typed outcome:

```rust
pub enum AppendOutcome {
    Appended { first: Offset, next: Offset, closed: bool },
    Duplicate { original: ProducerPosition, closed: bool },
}

pub enum AppendFailure {
    SequenceConflict { current: Option<StreamSeq> },
    ProducerSequenceReused,
    ProducerGap { expected: u64, received: u64 },
    ProducerStale { current_epoch: u64 },
    Closed { next: Offset },
    SealSuperseded,
    NotOwner { owner: InstanceId },
    Overloaded(OverloadReason),
    Unavailable(UnavailableReason),
    Invalid(DeferredAppendError),
}
```

11. Product/raw response mappers translate this outcome independently.
12. Delete direct calls from product to raw HTTP append helpers and delete `translate_append_response` once the application result is shared.

### Acceptance criteria

- one append application service is used by product, raw, initial-content, final-seal and internal append callers where their semantics genuinely match;
- wire parsing remains surface-specific;
- fence commands no longer inhabit the append request type;
- producer duplicate/conflict ordering remains exactly barriered;
- internal billing/tenant identities cannot be supplied by an untrusted wire DTO;
- product no longer calls `crate::http::append` or response translators;
- append application functions stay under 150 lines; physical applicators are handled by WP-11.

---

## 15. WP-08 — Read and scan planner/executor

### Objective

Replace long, response-building read functions with an explicit planner/executor that handles visibility, topology lineage, descriptor refresh, one-hop fan-out, local storage merge, live waiting and page construction independently of HTTP.

### Current evidence

- `ReadParams` mixes wire query and trusted policy (`src/http.rs:1970-2013`).
- `read_inner` is 450 lines (`src/http.rs:6684-7133`).
- `read_v3_lineage_inner` is 566 lines and recursively re-enters itself after descriptor refresh (`src/http.rs:7711-8276`, including the boxed call around lines 7810-7830).
- `product_read`, `product_scan` and watch/source code translate or reuse portions of raw read behavior.

### Target design

```rust
pub struct ReadQuery {
    pub stream: TenantStreamRef,
    pub start: ReadStart,
    pub selector: RecordSelector,
    pub mode: ReadMode,
    pub visibility: ReadVisibility,
    pub page: PageBudget,
    pub fanout: FanoutPolicy,
}

pub enum ReadStep {
    ServeLocal(LocalReadPlan),
    FetchPeer(PeerReadPlan),
    RefreshDescriptor { reason: RefreshReason },
    AdvanceLineage { segment: SegmentId, next: Offset },
    Wait(WaitPlan),
    Complete(ReadPage),
}

pub struct ReadPage {
    pub records: Vec<Record>,
    pub next: Cursor,
    pub caught_up: bool,
    pub closed: bool,
    pub cache: CacheDisposition,
    pub source_counts: ReadCounts,
}
```

### Implementation instructions

1. Split public query parsing from trusted policy. Product `maxBytes` and applied visibility become application inputs constructed by the product adapter, not hidden serde fields.
2. Create a pure `ReadPlanner` that consumes a validated descriptor/topology and returns the next step. It must not perform I/O or build responses.
3. Replace recursive descriptor refresh with a bounded loop:

```rust
let mut refreshes = 0;
loop {
    match planner.plan(&descriptor, &query, &progress)? {
        ReadStep::RefreshDescriptor { reason } if refreshes < 1 => {
            descriptor = registry.refresh_same_incarnation(&query.stream).await?;
            refreshes += 1;
        }
        step => return executor.execute(step, &mut progress).await,
    }
}
```

The existing one-refresh rule remains explicit and testable.
4. Model the seal gap and pending topology transition as planner states. A closed physical segment under an unpublished successor transition must not yield false terminal/closed output.
5. Keep keyed and keyless lineage semantics explicit. A scalar cursor cannot represent keyless live progress across multiple segments; preserve the current typed refusal.
6. Move local history/tail merge into a `LocalReader` returning records and source counts. It should know storage, not HTTP cache headers.
7. Move peer relay into `PeerReadClient` with a typed target and `FanoutPolicy::LocalOnly | OneHop`. Internal handlers always construct `LocalOnly`, making relay cycles impossible by type/policy.
8. Meter once at the public application coordinator using returned `ReadCounts`. Internal peer work returns counts and never meters itself.
9. Model live behavior separately:
   - one-shot page;
   - bounded long poll;
   - subscription source.
   Do not put SSE body/framing into the read core.
10. Let `ScanService` compose snapshot-tail capture and repeated `ReadService` pages with a typed scan cursor. Do not duplicate lineage/peer logic.
11. Produce one transport-neutral page; product/raw serializers own their exact cursor/header/body formats.
12. Add property tests for cursor monotonicity, no duplicate/skip across lineage, and refresh boundedness.

### Acceptance criteria

- no read application type derives `Deserialize` from a public query;
- no trusted auth/internal/fan-out fields are serde-skipped on a wire DTO;
- descriptor refresh is iterative and explicitly bounded;
- local/peer reads share a page result and public metering occurs exactly once;
- raw/product exact headers and cache behavior remain characterized;
- `read_inner`, `read_v3_lineage_inner` and duplicated response-building logic are deleted;
- planner functions are pure and target under 100 lines; executor functions target under 150 lines.

---

## 16. WP-09 — Consumer and queue application service

### Objective

Move consumer configuration, pull, settle and generation-fenced deletion into a typed application service; keep physical queue mutations serialized in the shard commit lane.

### Current evidence

`product_consumer_put/get/delete/pull/settle` span several hundred lines each. `product_consumer_delete` alone is 404 lines (`src/product.rs:5848-6251`) and contains a carefully ordered, distributed deletion saga. `QueueOp` mixes configuration and record-state operations, and commit-group branches implement both.

The deletion comments identify a sound protocol:

1. version token names stream epoch + consumer generation;
2. Active -> Deleting parent transition;
3. generation fence and bounded cleanup across every current/predecessor segment;
4. topology re-read until a fan-out round is stable;
5. Deleting -> Deleted tombstone.

That protocol should become the code structure.

### Target design

```rust
pub struct ConsumerService {
    registry: RegistryService,
    routing: RoutingService,
    shards: ShardDirectory,
    peers: PeerClient,
}

pub enum ConsumerCommand {
    Put(PutConsumer),
    Pull(PullMessages),
    Settle(SettleMessages),
    Delete(DeleteConsumer),
}

pub enum ConsumerDeleteStep {
    ValidateTarget,
    MarkDeleting,
    SweepTopology(TopologyVersion),
    RecheckTopology,
    PublishDeleted,
    Complete,
}
```

Physical commands should be separated:

```rust
pub enum ConsumerShardCommand {
    Pull(PullCommand),
    Settle(SettleCommand),
    InstallGenerationFence(GenerationFence),
    DeleteGenerationRows(DeleteGenerationRows),
    PutConfig(PutConsumerConfig),
    TransitionConfig(ConsumerConfigTransition),
}
```

### Implementation instructions

1. Extract `ConsumerVersion` as a typed token containing stream epoch and consumer generation. Parse once at transport; verify semantics in application.
2. Preserve the current delete ordering where a stale stream-incarnation token yields no-touch idempotent success before validating a now-obsolete encryption key.
3. Model config lifecycle explicitly: `Active`, `Deleting`, `Deleted`. Keep generation monotonicity and tombstones.
4. Implement deletion as a monotonic loop over named steps. Record progress via existing durable state; do not add an in-memory checklist.
5. Create a `TopologySweep` helper that:
   - takes a topology snapshot/version;
   - dispatches independent segment cleanup concurrently with a bounded concurrency limit;
   - propagates any local/remote failure;
   - rereads the descriptor and repeats if the same stream incarnation has a changed topology;
   - aborts without touching a replacement incarnation.
6. Keep each physical cleanup step bounded by rows and bytes. A retry resumes from remaining durable rows.
7. Split queue config operations from message-state operations in the command algebra and commit applicators.
8. Return semantic outcomes rather than responses. Product mapping handles 204, conflict, retryable 503 and version headers.
9. Move DLQ target resolution through typed same-project references. No bare string may choose a cross-project target.
10. Centralize local/remote segment command execution in `SegmentCommandClient`; consumer code should not manually build peer URLs or clone HTTP requests.
11. Add anti-vacuity counters for every distributed deletion phase and topology-repeat branch.

### Acceptance criteria

- consumer lifecycle orchestration exists in one service, not in product handlers;
- targeted deletion success implies every relevant segment is fenced and bounded cleanup reached the current stable topology;
- a stale deletion cannot erase a recreated generation or stream incarnation;
- local and peer segment execution use one typed interface;
- queue physical commands contain only fields meaningful to their variant;
- no consumer handler exceeds 120 lines and no saga coordinator exceeds 200 lines.

---

## 17. WP-10 — Watch and live-feed application boundary

### Objective

Separate immutable watch semantics and subscription authorization from feed memory/session/framing, and remove HTTP runtime dependencies from SSE source/session code.

### Current evidence

The SSE implementation is already partially decomposed into `feed`, `registry`, `session`, `source`, `wire`, `auth` and `budget`, but several files remain around or above 1,000 lines and source/session modules import HTTP-owned behavior. Live-feed budgets, connection counters, heartbeat values and certification controls live on `AppState`.

Watch operations also share `product.rs` with unrelated lifecycle, append, scan and consumer logic. Signed observation capability handling is correctly route-specific but crosses route/auth/descriptor/session concerns.

### Target separation

```text
WatchService
  - list/get immutable definitions
  - validate watch capability against descriptor verifier
  - produce WatchSubscriptionRequest

ReadService / ChangeSource
  - produce initial value/page and change events
  - enforce auth lease and topology/ownership cutoff

LiveFeedService
  - feed registry
  - retained-byte budget
  - subscriber/connection admission
  - feed lifecycle and publication

SseTransport
  - HTTP status/headers
  - event framing/keepalive
  - body lifetime guards
```

### Implementation instructions

1. Move watch definitions and watch-key matching into a transport-neutral domain/application module.
2. Parse capability carriers in transport, but verify the capability in `WatchService` against the exact route, descriptor verifier, expiry and current stream incarnation.
3. Build a typed `SubscriptionAuthorization` that contains only the lease/cutoff information the source needs. Never pass raw bearer/capability headers into session code.
4. Move feed registry, memory budget, per-feed ring size, payload ceiling, connection budget and heartbeat config into `LiveFeedService`.
5. Keep certification-only delay controls in an explicit `CertificationControls` capability that cannot exist in release posture unless configuration validation allows it. Do not leave arbitrary atomics on general runtime state.
6. Define a shared source event model:

```rust
pub enum StreamEvent {
    Records(ReadPage),
    Persisted { through: Cursor },
    Aborted { from: Cursor, reason: AbortReason },
    Sealed { at: Cursor },
    Cutoff(SubscriptionCutoff),
    KeepAlive,
}
```

7. Keep wire framing separate for product/raw surfaces if event names/header semantics differ.
8. Make heartbeat ownership explicit in the body/session layer so blocked reads cannot suppress it, preserving the current rationale.
9. Replace HTTP-dependent source callbacks with `ReadService`, `OwnershipService`, `AuthLeaseValidator` and `Clock` capabilities.
10. Make connection/subscription guards RAII values held by the response body/session lifetime, not the handler future.
11. Split existing long files by responsibility only after these boundaries exist. Do not create `feed_helpers.rs` as a miscellaneous spill file.
12. Preserve current memory-budget and payload-ceiling tests, then add instance-isolation tests proving two server rigs share nothing mutable.

### Acceptance criteria

- no `sse/*` module imports `crate::http` or Axum except `sse/wire` if it is explicitly retained as a transport adapter;
- watch capability verification occurs once against an exact typed route;
- feed memory and connection admission are owned by an instance-scoped service;
- auth expiry/generation change, ownership movement, sealing and shutdown produce typed cutoffs;
- heartbeat and guard lifetimes remain correct under blocked sources and client cancellation;
- each SSE source/session/feed production file is below 1,000 lines, with a target below 700.

---

## 18. WP-11 — Typed shard commands and batch transaction executor

### Objective

Preserve the single serialized commit lane and durable-watermark pipeline while decomposing command collection, batch-local state, physical mutation, applied publication and durable dispatch into explicit stages.

This is the highest-risk workstream. It must be approached as a correctness refactor, not a cleanup sprint.

### Current evidence

`ShardEngine::committer_loop` collects/paces a group (`src/shard.rs:2269-2396`), then `commit_group` (`src/shard.rs:2422-4527`) expands maintenance commands, lazy-loads stream/consumer/billing state, applies every operation into one `WriteBatch`, computes provisional results, writes once, publishes applied state on success and enqueues an `InFlightGroup` for durable dispatch.

Important existing properties include:

- one group failure path replaces every provisional result (`send_group_failure`, `src/shard.rs:2399-2420`);
- same-group results do not escape before the establishing batch succeeds;
- applied state is published only after `db.write` succeeds (`src/shard.rs:4425-4479`);
- responses and durable-tail/ring/touch effects are held until the durable watermark passes (`src/shard.rs:4504-4521`, `dispatch_durable` around `4997-5053`);
- absorber boundary advances and maintenance accounting can share a batch;
- queue generation/fence state and billing state are serialized with appends.

Do **not** split these into independent committers.

### Target pipeline

```text
ShardCommand channel
  -> GroupCollector
       bounded drain + pacing, no business logic
  -> CommitBatchExecutor::prepare(commands)
       lazy-load batch-local state
       apply typed commands
       build WriteBatch + provisional outcomes + effects
  -> ShardDb::write(prepared.batch)
       failure => fail every reply, publish nothing
       success => publish applied effects
  -> InFlightCommit { seqnum, durable effects, replies }
  -> DurabilityPump / durable watermark
  -> DurableDispatcher
       durable state, ring, wakeups, signals, replies
```

### Recommended types

```rust
pub enum ShardCommand {
    Stream(StreamCommand),
    Consumer(ConsumerShardCommand),
    Billing(BillingShardCommand),
    Maintenance(MaintenanceCommand),
}

pub struct CommitBatchExecutor<'a> {
    engine: &'a ShardEngine,
    config: &'a CommitConfig,
    batch: WriteBatch,
    streams: HashMap<SegmentHash, StreamTxn>,
    effects: CommitEffects,
    trim_budget: TrimBudget,
}

pub struct StreamTxn {
    handle: Arc<StreamHandle>,
    base: TailFields,
    staged: TailFields,
    producers: ProducerOverlay,
    sequences: SequenceOverlay,
    consumers: ConsumerOverlay,
    billing: Option<BillingOverlay>,
    ring_records: Vec<(Offset, Bytes)>,
    accounting: StreamAccounting,
}

pub struct PreparedCommit {
    pub batch: WriteBatch,
    pub applied: AppliedEffects,
    pub durable: DurableEffects,
    pub replies: PendingReplies,
    pub barrier: BarrierPlan,
    pub stats: CommitStats,
}
```

`StreamTxn` is the only place allowed to combine durable base state with changes staged by earlier commands in the same group.

### Command applicators

Implement one applicator per semantic family:

```rust
impl CommitBatchExecutor<'_> {
    async fn apply_stream(&mut self, command: StreamCommand) -> Result<(), PrepareFailure>;
    async fn apply_consumer(&mut self, command: ConsumerShardCommand) -> Result<(), PrepareFailure>;
    async fn apply_billing(&mut self, command: BillingShardCommand) -> Result<(), PrepareFailure>;
    async fn apply_maintenance(&mut self, command: MaintenanceCommand) -> Result<(), PrepareFailure>;
}
```

Then split further by meaningful operation, for example `apply_append`, `apply_raise_seal_fence`, `apply_pull`, `apply_settle`, `apply_usage_ack`, `apply_absorbed` and `apply_trim_step`. Avoid a generic visitor framework; explicit dispatch is easier to audit.

### Applied versus durable effects

Make the publication boundary visible:

```rust
pub struct AppliedEffects {
    pub stream_overlays: Vec<AppliedStreamState>,
    pub maintenance: Option<ShardMaintenance>,
    pub trim_debt: Vec<TrimDebtChange>,
    pub applied_wakeups: Vec<Arc<Notify>>,
}

pub struct DurableEffects {
    pub durable_tails: Vec<DurableTailUpdate>,
    pub ring_publications: Vec<RingPublication>,
    pub absorber_signals: Vec<AbsorbSignal>,
    pub touches: Vec<TouchEvent>,
    pub replies: PendingReplies,
}
```

No method in `prepare` may mutate published engine state or send a client reply. No durable effect may run immediately after `db.write`; it must remain attached to the returned seqnum until the durable watermark crosses it.

### Barrier plan

Fence and state-dependent refusal behavior needs an explicit model:

```rust
pub enum BarrierPlan {
    CommitSeq,
    CurrentInFlightHighWatermark,
    NoBarrier,
}
```

The actual variants should match the current protocol. The goal is to stop encoding fence-only behavior through special vectors and comments. A fence following a close in the same group must be grounded in that group's write; a fence-only command must wait for the correct already-in-flight boundary.

### Implementation sequence

1. **Characterize first.** Add exact traces for group composition, provisional result barrier, write failure, applied publication, durable dispatch, fence-only groups, mixed append+absorb accounting and queue/billing mutations.
2. Extract `GroupCollector` from `committer_loop` without changing `CommitOp`.
3. Introduce `PreparedCommit`, initially populated by the existing monolithic function.
4. Move post-write applied publication into `AppliedEffects::publish` and durable in-flight construction into `DurableEffects`, still fed by old logic.
5. Introduce `StreamTxn` and migrate the existing local aggregate to it. Pin lazy-load and same-group overlay behavior.
6. Replace `CommitOp` with typed command families one variant at a time. Update shutdown failure handling so every command with a reply has an explicit moved/fenced failure.
7. Extract command applicators in this order:
   - seal fence/close;
   - append/producer/sequence;
   - consumer pull/settle;
   - consumer config/delete step;
   - billing ack/close/retained;
   - absorbed/trim maintenance.
8. After each extraction, compare storage operation trace, WriteBatch row set, replies and effect ordering against the baseline.
9. Split `ShardEngine` into real owners:
   - `ShardStorage`;
   - `StreamTable`;
   - `CommitRuntime`;
   - `DurabilityRuntime`;
   - `TailRing`;
   - `MaintenanceRuntime`;
   - `ShardTaskGroup`;
   - `ShardMetrics`.
   These types must expose behavior, not public fields.
10. Delete `commit_group` only after every applicator is migrated.

### Performance and cost gates

Commit refactors can be behaviorally correct and economically disastrous. For every PR that changes group collection, WriteBatch contents, WAL flush/pump behavior or durable dispatch:

- run all deterministic durability/idempotence scenarios;
- run the existing isolated `post_split_throughput_scales` gate;
- run local append latency/throughput at the repository's representative concurrencies and payload sizes;
- compare WAL/object-store operation counts and requests-per-WAL;
- run cost-budget tests for append, absorption, trim and idle poll traffic;
- check RSS/ring/postings/handle budgets;
- run at least one failure campaign with a blocked/lost store response.

Use existing locked thresholds where present. For additional microbench comparisons, record at least five runs and investigate a median throughput regression over 5% or p99 latency increase over 10%; do not automatically waive a smaller regression if object counts or tail behavior changed.

### Acceptance criteria

- one serialized command lane and one durable watermark authority remain;
- `commit_group` is deleted;
- no prepare/applicator function sends replies or mutates published state;
- a failed write publishes no applied/durable effects and all dependent provisional results fail;
- applied and durable effects are separate types and phases;
- every command variant has only meaningful fields;
- `ShardEngine` top-level fields are reduced through ownership, not cosmetic nesting; target at most 20 cohesive handles/config values;
- no commit function exceeds 200 lines; applicators target under 120 lines;
- existing correctness, capacity and cost gates show no unexplained regression.

---

## 19. WP-12 — Storage keyspace and codec ownership

### Objective

Centralize physical key construction and versioned encoding/decoding so storage contracts are explicit, golden-tested and unavailable for ad hoc reproduction elsewhere.

### Current evidence

Physical key tags are distributed across modules:

- shard tail/record/sequence/producer/dirty/maintenance: `t`, `r`, `s`, `q`, `D`, `M` in `src/shard.rs`;
- consumer config/fence and state keys in `src/queue.rs`;
- billing meta/dirty/month-final: `B`, `U`, `V` in `src/billing.rs`;
- history record keys in `src/history.rs`;
- postings: `p` in `src/postings.rs`.

`TailFields` also implements multiple storage versions in `shard.rs`. This makes physical layout knowledge leak into business and maintenance code.

### Target layout

```text
storage/
  keyspace.rs
  codec/
    tail.rs
    record.rs
    producer.rs
    consumer.rs
    billing.rs
    maintenance.rs
    descriptor.rs
```

Use explicit functions/types, not a generic ORM-like table abstraction:

```rust
pub fn tail_key(segment: SegmentHash) -> ShardKey;
pub fn record_key(segment: SegmentHash, offset: Offset) -> ShardKey;
pub fn producer_key(segment: SegmentHash, key: RoutingKeyHash, producer: &ProducerId) -> ShardKey;
pub fn consumer_lease_prefix(segment: SegmentHash, consumer: &ConsumerName, generation: ConsumerGeneration) -> KeyPrefix;
```

### Implementation instructions

1. Add golden tests from WP-00 before moving any constructor or codec.
2. Move one key family at a time, preserving exact bytes and range ordering.
3. Return `Key`, `KeyPrefix` or `KeyRange` newtypes where it prevents accidentally using a point key as a prefix. Keep conversion to byte slices straightforward.
4. Centralize tag constants and document collision/range constraints.
5. Move tail v2/v3 decoding to `codec::tail`. Keep accepted trailing defaults/version behavior exactly as today; corruption remains fail-closed.
6. Separate persisted DTO encoding from domain conversion for descriptors.
7. Keep encryption/compression concerns outside key assembly. A codec may encode a row, but keyspace functions should not know stream keys.
8. Add round-trip, golden-byte, lexicographic-range and corruption tests for every family.
9. Replace manual prefix construction in scans (`push(b'U')`, `push(b'V')`, etc.) with canonical builders.
10. Add an architecture scan that flags byte tag construction outside `storage::keyspace`, with narrow allowlists for unrelated protocol bytes.
11. Do not change layout version, tags, endianness, hash domains or object paths in this workstream.

### Acceptance criteria

- all physical row keys are constructed by `storage::keyspace`;
- all persisted row codecs live under `storage::codec` or an explicitly justified storage adapter;
- exact existing bytes and ordering are golden-pinned;
- corrupt data produces typed decode/corruption errors, not panics or silent defaults beyond the current documented compatibility within layout 4;
- business/application modules operate on typed records, not raw key bytes;
- no generic storage abstraction obscures which rows share a WriteBatch.

---

## 20. WP-13 — Billing, rollup and history controllers

### Objective

Separate data-plane accounting models from background orchestration and make billing/history workers depend on narrow runtime ports rather than `AppState`.

### Current evidence

`billing.rs` currently contains identity/time, month math, segment billing metadata, usage schemas, key constructors, read meters, append/queue meters, read spool, usage outbox draining, rollup startup, sweeps, tombstone walking and internal system-stream clients. Many functions accept `AppState`, including `drain_once`, `open_read_spool`, `spawn_telemetry`, `rollup_step`, `spawn_rollup`, sweep helpers and tombstone walk.

`history.rs` contains process-global budget/config controls, key cache, history storage and a 405-line absorber start loop. `rollup.rs` contains row models, materialization, month closure, correction/reconciliation and operational rollup behavior.

### Target modules

```text
billing/
  model.rs          # identities, payloads, trusted time inputs
  meter.rs          # data-plane semantic counters
  outbox.rs         # durable row model and scan/ack protocol
  publisher.rs      # _usage append client
  drain.rs          # one drain operation
  read_spool.rs

rollup/
  model.rs
  apply.rs
  month_close.rs
  reconcile.rs
  artifacts.rs
  reader.rs

history/
  storage.rs
  reader.rs
  key_cache.rs
  budget.rs
  planner.rs
  absorber.rs
```

### Billing instructions

1. Inject `Clock` and process `BootId`; remove `billing_now_ms` test override and `boot_id` dependency on HTTP randomness.
2. Make `BillingMeter` consume trusted `RequestContext` + validated descriptor/application outcome. Transport cannot supply account/project/stream IDs directly.
3. Preserve exact data-plane atomicity: segment billing metadata, version/dirty marker and record/tail changes remain in the same shard transaction.
4. Refactor `drain_once` into:

```rust
pub struct BillingDrainer {
    scanner: BillingOutboxScanner,
    publisher: UsagePublisher,
    acker: BillingAckSubmitter,
    clock: Arc<dyn Clock>,
}

pub struct DrainReport {
    pub scanned: usize,
    pub published: usize,
    pub acknowledged: usize,
    pub retained_dirty: usize,
}
```

5. Do not clear dirty state directly from the background worker. Submit the version-conditional acknowledgement through the shard commit lane, preserving races with new appends.
6. Make system-stream append/read a typed internal client with fixed system project identity and no public-wire construction.
7. Separate read spooling from usage publication; each gets its own bounded queue/backpressure/reporting.

### Rollup instructions

1. Split immutable row models/codecs from transactional application.
2. Make one input page produce a `RollupPlan` that names row updates, cursor movement and month closures.
3. Apply row changes and cursor atomically in one rollup batch, preserving replay safety.
4. Extract month closure and artifact publication from the page loop.
5. Return typed reconciliation reports and corruption errors; do not encode control flow in log strings.
6. Pin correction, duplicate, month-boundary and crash/replay behavior before movement.

### History/absorber instructions

1. Move budget calculation to a pure `AbsorbBudgetConfig -> AbsorbBudget` function. Instance runtime owns current reservations/counters.
2. Split the absorber into:

```rust
pub struct AbsorbScheduler { /* signals, fairness, cadence */ }
pub struct GatherPlanner { /* pure candidate selection and packing */ }
pub struct GatherExecutor { /* reads, transforms, history writes */ }
pub struct AbsorbCommitter { /* submit boundary receipts to shard lane */ }
```

3. The planner receives an immutable candidate snapshot, packing/budget limits and trusted time. It returns a bounded `GatherPlan` plus explicit reasons for skipped work.
4. The executor performs exact history write/readback behavior required by current invariants and returns per-stream receipts. It must not mutate shard published state directly.
5. Submit one gather's receipts as one maintenance command/batch where current atomicity requires it.
6. Inject pause/failure controls through the semantic failpoint registry, not module statics.
7. Preserve fairness, reservation accounting, global trim budget and object-store operation budgets.

### Acceptance criteria

- billing/history/rollup modules do not import transport or `AppState`;
- trusted billing time and boot identity are injected;
- append billing rows remain transactionally coupled to append state;
- outbox acknowledgement remains version-conditional and serialized with appends;
- rollup row updates and cursor movement remain atomic;
- absorber planning is pure and executor/commit phases are explicit;
- no controller loop exceeds 200 lines; model/codec and orchestration files are separately owned;
- cost and exact billing reconciliation tests remain green.

---

## 21. WP-14 — Fleet, scaler and background reconciliation

### Objective

Turn fleet and scaling loops into pure policy plus side-effect execution, make ownership snapshots explicit and remove process-global state/HTTP dependencies.

### Current evidence

`fleet::start` is 762 lines and takes `Arc<AppState>` (`src/fleet.rs:376-1137`). It combines heartbeat generation, store listing/reads, liveness, ring publication, desired count calculation, override management, rebalancing and sleeps. Fleet also has process-global last-good URL and store slots.

`scaler3` reads policy lazily from environment, stores sketches/counters in process globals, performs transition execution and starts from `Weak<AppState>` (`src/scaler3.rs:31-60`, `111-140`, `874+`). Its split/merge protocol is correctness-sensitive and should remain, but policy/state ownership must become explicit.

### Fleet target

```rust
pub struct FleetController {
    loader: FleetSnapshotLoader,
    policy: FleetPolicy,
    executor: FleetActionExecutor,
    publisher: OwnershipPublisher,
    clock: Arc<dyn Clock>,
}

pub struct FleetSnapshot {
    pub self_heartbeat: Heartbeat,
    pub peers: Vec<PeerHeartbeat>,
    pub desired: Option<Versioned<Desired>>,
    pub overrides: Option<Versioned<Overrides>>,
}

pub fn reconcile(snapshot: &FleetSnapshot, config: &FleetConfig, now: TrustedNow)
    -> FleetDecision;
```

`FleetDecision` should contain desired state and a list of explicit actions/CAS attempts. It should be serializable for tests and diagnostics.

### Fleet instructions

1. Extract heartbeat measurement from store/control logic. It consumes metrics snapshots from owned components.
2. Load independent fleet objects concurrently with bounded I/O when doing so does not affect semantics.
3. Make peer URL validation/fallback instance-owned. Remove global last-good URL storage.
4. Extract liveness, desired-count, ring/override and victim/target policies into pure functions.
5. Separate CAS execution from decision generation. CAS conflict is an explicit result fed into the next reconcile cycle.
6. Publish an immutable `OwnershipSnapshot` atomically to request/background consumers.
7. Model drain/shutdown as controller actions owned by `TaskGroup`, not scattered flags.
8. Add snapshot-golden tests for every known fleet campaign topology and failure case.

### Scaler target and instructions

1. Parse `ScalePolicy` through `ServerConfig`; delete lazy environment access.
2. Replace global sketch state with an instance-owned `ScaleTracker` keyed by typed stream incarnation/segment identity.
3. Keep append admission feed cheap and lock scope bounded; expose an immutable evaluation snapshot.
4. Separate:
   - `ScaleEvaluator`: pure rate/sketch -> split/merge/no-op decision;
   - `TopologyTransition`: Phase A intent, physical seal/fence, Phase B CAS;
   - `ScaleController`: schedule, resume pending transitions and execute decisions.
5. Preserve the current crash-resumable two-phase transition protocol and exact incarnation fences.
6. Reuse `SealService`/segment-close command where semantics match; do not maintain a scaler-specific close path.
7. Move peer segment-close transport into `PeerClient` typed RPC.
8. Put counters on `ScalerMetrics`, not process statics.
9. Add decision tests with injected time and deterministic sketches; add transition tests with semantic failpoints.

### Acceptance criteria

- `fleet` and `scaler` do not import transport or `AppState`;
- fleet/scaler policies are deterministic pure functions over snapshots/config/time;
- mutable state is instance-owned and stream-incarnation typed;
- independent snapshot I/O is not unnecessarily serialized;
- CAS conflicts and ownership movement are explicit controller outcomes;
- existing fleet, handoff, noisy-neighbor and routing-v3 campaigns remain required;
- each controller loop is under 200 lines and each pure policy function under 100 lines.

---

## 22. WP-15 — Clock, entropy, failpoints, metrics and task supervision

### Objective

Remove ambient process behavior that undermines deterministic tests and graceful ownership, and establish one lifecycle for every spawned task.

### Current evidence

Time currently flows through helpers such as `shard::now_ms`, billing test overrides and direct Tokio timers. Entropy/boot IDs cross module boundaries through HTTP helpers. Failpoint behavior is spread across modules, while the DST implementation plan already specifies a semantic failpoint registry. Several modules use `OnceLock`, mutable statics or atomics for policy, counters and test controls. Task spawning is distributed across engine, fleet, telemetry, rollup, scaler, SSE and bootstrap code.

### Runtime interfaces

```rust
pub trait Clock: Send + Sync {
    fn now(&self) -> TrustedNow;
    fn monotonic(&self) -> MonotonicInstant;
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()>;
}

pub trait Entropy: Send + Sync {
    fn fill(&self, dest: &mut [u8]);
}

pub struct RuntimeIdentity {
    pub boot_id: BootId,
    pub instance: InstanceId,
}

pub struct TaskGroup {
    cancellation: CancellationToken,
    tasks: Mutex<HashMap<TaskName, JoinHandle<TaskResult>>>,
}
```

Use traits only here because production and deterministic simulation genuinely need interchangeable implementations.

### Implementation instructions

1. Inject `Clock` into lifecycle, billing, fleet, scaler, absorber, auth leases and timeout policies. Keep customer-controlled timestamps as distinct metadata types.
2. Inject `Entropy`/`IdGenerator` where epochs, operation IDs, nonce material or boot IDs are generated. Cryptographic production entropy remains the implementation; deterministic tests use seeded entropy.
3. Implement the semantic failpoint registry already designed in `docs/dst/IMPLEMENTATION-PLAN.md`:
   - typed failpoint enum;
   - selectors for node/stream/epoch/operation/shard/segment/occurrence;
   - pause/error/crash/cancel/delay/drop-response actions;
   - entered/release handshakes;
   - multiple simultaneous selectors;
   - panic-safe disarm/release.
4. Migrate ad hoc flags in a documented order: group-write, commit hold/dispatch, creation, fork, seal, topology, absorber, SSE and object-store fault holds.
5. Move counters into component-owned metrics structs. Export snapshots to operator/fleet/telemetry code.
6. Allow process-global state only for true immutable constants, the global allocator and narrowly justified process instrumentation. Every mutable global gets an explicit review.
7. Make every spawned task a child of `TaskGroup` or a shard-owned child group. A task has:
   - name;
   - owner;
   - cancellation trigger;
   - shutdown deadline;
   - result/error policy;
   - metrics.
8. On a critical task failure, fail readiness and cancel the owning subsystem/process according to policy. Do not leave a nominally healthy server with a dead acker, fleet loop or billing worker.
9. Make shutdown ordered: stop admission, seal external timer/source where relevant, drain or fail queued requests, stop background producers, close engines, await owned tasks, then release stores/listener.
10. Replace sleep-based race tests with failpoint handshakes as modules migrate.

### Acceptance criteria

- production use cases do not call ambient wall clock or random helpers directly;
- deterministic tests can control time and entropy per runtime instance;
- all fault injection uses the semantic registry or a documented store adapter;
- no test-only mutable process global couples parallel rigs;
- every spawned task has an owner and is awaited/cancelled on shutdown;
- critical background task death is observable and fail-closed;
- simulator extraction no longer requires copying production state machines.

---

## 23. WP-16 — DST/test decomposition and simulator boundary

### Objective

Turn the giant binary-internal test corpus into bounded-context suites that exercise the library's real production code, share one test-support API and map directly to scenario/invariant metadata.

### Current evidence

`src/dst/dst_tests.rs` is 36,922 lines with 368 tests. The existing DST plan already requires:

- no copied production state machines;
- no sleeps to force races;
- entered/release handshakes;
- named liveness measures;
- serialized scenarios and choice traces;
- cost/resource budgets;
- an eventual `streams-sim` crate.

This workstream implements that plan in step with the structural refactor.

### Incremental test layout

```text
tests/
  behavior/
    product_surface.rs
    raw_surface.rs
    dual_surface.rs
  dst/
    durability.rs
    creation.rs
    sealing.rs
    forks.rs
    routing.rs
    consumers.rs
    watches.rs
    history.rs
    billing.rs
    multitenancy.rs
    live_feed.rs
    ownership.rs
    resources.rs
  gates/
    capacity.rs
    cost.rs
    release_posture.rs

test-support/
  runtime.rs
  server.rs
  stores.rs
  clock.rs
  entropy.rs
  failpoints.rs
  clients.rs
  assertions.rs
  scenarios.rs
```

The exact layout may move under `crates/streams-sim`, but scenario ownership must be visible.

### Test-support API

```rust
let rig = TestRuntimeBuilder::new()
    .with_projects(2)
    .with_shards(4)
    .with_clock(FixedClock::at(...))
    .with_store(FaultStore::new())
    .build()
    .await?;

let gate = rig.failpoints.arm(
    FailPoint::CommitAfterAppliedBeforeRemoteDurable,
    selector().stream(&stream).occurrence(1),
    FailAction::Pause,
);

gate.wait_entered().await?;
// assert intermediate state
 gate.release();
```

### Implementation instructions

1. Add `src/lib.rs` first so integration tests can use public/crate test-support APIs rather than private binary internals.
2. Inventory every existing test with:
   - stable scenario ID(s);
   - layer (`unit`, L1 focused, L2 simulator, L3 field);
   - mechanism/failpoint;
   - entered-proof counter;
   - invariant IDs;
   - resource budget.
3. Introduce `#[scenario("DUR-003")]` or equivalent metadata. Generate a machine-readable catalogue and fail CI on unmapped tests/scenarios.
4. Split test support from scenarios before moving tests. Helpers should model clients/stores/runtime, never reimplement lifecycle/commit logic.
5. Move one bounded context at a time. Preserve test names where scripts depend on them or update scripts/workflows atomically.
6. Replace the hard-coded exact path used for the capacity test with a stable dedicated test target, for example:

```bash
cargo test --release --test capacity_gate post_split_throughput_scales -- --exact
```

7. Keep focused unit tests next to small pure modules. Move multi-component behavior and failure scenarios to integration/simulator suites.
8. Eliminate process-environment/global-clock locks by using injected runtime values.
9. Replace sleeps with failpoint entered/release handshakes. A timeout remains a watchdog, never the proof that an event occurred.
10. Add reusable exact assertions:
    - no response before durable boundary;
    - exact acknowledged operation set;
    - exact descriptor/row state;
    - exact object-store operation multiset/order where required;
    - no leaked task/lease/guard;
    - progress measure advanced;
    - budget not exceeded.
11. Build the simulator around production ports after WP-15:
    - deterministic scheduler/time;
    - deterministic stores/transport/platform;
    - serializable scenario and choice trace;
    - reference model/auditor;
    - state hashes and shrinking.
12. Keep L3 field campaigns for real process/platform phenomena; use L2 to prove logical consequences where possible.

### Acceptance criteria

- no test file exceeds 1,000 lines; target under 700;
- `src/dst/dst_tests.rs` is deleted;
- every DST test maps to at least one scenario ID and every Existing scenario maps to a concrete test;
- no scenario claims a mechanism without entered-proof;
- production state machines are not copied into test helpers/models;
- test rigs are parallel-safe and instance-isolated;
- gate scripts use stable test targets rather than private module paths;
- failing deterministic runs emit a serialized scenario and choice trace.

---

## 24. WP-17 — Architecture enforcement, workspace move and documentation finish

### Objective

Turn the desired architecture into enforceable repository rules, complete the core/server/sim workspace split and preserve historical rationale without leaving source files as incident journals.

### Hard architecture gates

Make the warning-only report from WP-00 fail CI when the relevant migration is complete.

1. **File budget**
   - no production or test Rust file over 1,000 lines;
   - target 700 lines;
   - exceptions require owner, reason and expiry in `architecture-exceptions.toml`.
2. **Function budget**
   - no function over 200 lines;
   - target 120 lines for orchestration and 80-100 for transitions/applicators;
   - generated code is excluded by path, not comment.
3. **Dependency direction**
   - `streams-core` cannot depend on `streams-server`;
   - application/domain/shard/storage/background cannot import transport;
   - product/billing/fleet/scaler/SSE cannot import `http::AppState` (the type should already be gone).
4. **Transport leakage**
   - no Axum `Response`, `HeaderMap`, `StatusCode` or `Body` outside server transport adapters;
   - no wire DTO accepted by application methods.
5. **Configuration**
   - no direct process environment reads outside config/binaries/config tests.
6. **Storage layout**
   - no raw layout tag/key assembly outside `storage::keyspace`;
   - no descriptor serde outside `storage::codec::descriptor`/registry adapter.
7. **Mutable globals**
   - no new process-level mutable static/`OnceLock` state outside approved runtime instrumentation;
   - existing allowlist trends to zero.
8. **Quality baseline**
   - no new Clippy warning fingerprints;
   - every removed warning refreshes the baseline downward;
   - no dead-code allow added without a removal issue.
9. **Cycle check**
   - generate a module/crate dependency graph and reject forbidden cycles/edges.
10. **Scenario coverage**
    - scenario catalogue and test metadata are bidirectionally complete.

### Workspace move

After core imports no server types:

1. Move domain/application/shard/storage/runtime ports to `crates/streams-core`.
2. Move Axum, CLI/config production adapters, object-store construction, operator endpoints and binaries to `crates/streams-server`.
3. Move deterministic scheduler/stores/platform/model/auditor/scenarios to `crates/streams-sim`.
4. Keep shared test-support pieces in `streams-sim` or a dev-only support crate; do not make production core depend on simulator features.
5. Update CI to test each crate and the workspace, then run integration/conformance gates against the produced server binary.
6. Verify Cargo feature/default behavior so release binaries do not include simulation/failpoint-only code beyond intentional semantic hooks.

### Invariant documentation

Create `docs/invariants/` with stable IDs, for example:

```text
INV-DUR-ACK        success only after remote durability
INV-DUR-STAGE      failed batch publishes no staged state
INV-PROD-DUP       duplicate verdict barrier and body-hash rules
INV-INIT-READY     Ready follows all initialization obligations
INV-SEAL-FENCE     stale seal generation cannot write/close
INV-FORK-DEBT      source-reference cleanup is durable/idempotent
INV-CONS-GEN       consumer deletion/recreation generation isolation
INV-MT-IDENTITY    every customer state identity is project-qualified
INV-BILL-ATOMIC    append and billing observation share required batch
INV-FANOUT-ONEHOP  internal segment relay never relays again
```

Local comments become concise:

```rust
// INV-DUR-STAGE: publish the overlay only after db.write succeeds.
prepared.applied.publish(&self.streams);
```

The invariant document contains the incident history, rejected alternatives, scenario IDs and relevant source/test links. Do not delete useful provenance; move it to a durable, searchable home.

### Acceptance criteria

- the workspace matches the core/server/sim boundary already endorsed by the DST plan;
- architecture gates fail on forbidden dependencies, oversize growth, ambient env access and storage-key leakage;
- all exceptions are explicit, owned and expiring;
- source comments primarily explain current invariants, while incident chronology lives in invariant docs/ADRs;
- no temporary bridge modules, aliases or legacy `AppState` paths remain;
- `cargo test --workspace`, server conformance, field gates, capacity/cost gates and release certification all operate on the new structure.

---

## 25. Delivery sequence and pull-request plan

The numbered work packages describe ownership domains, not permission to work on all of them concurrently. The repository should be migrated in the sequence below so each pull request has one source of truth and a bounded proof obligation.

### 25.1 Phase A — Freeze the observable system

#### PR 1 — Architecture and behavior baseline

**Implements:** WP-00.

Deliver:

- the source-size/function-size/dependency/environment/global-state report;
- exact wire snapshots for the product and raw APIs;
- golden storage-key/descriptor encodings for layout 4;
- a machine-readable invariant-to-test index;
- recorded local performance/cost baselines using the repository's existing gates;
- a temporary CI job that reports, but does not yet fail, structural violations.

Do not move production code in this PR. The review question is whether the baseline is sufficient to detect accidental behavior, layout, cost and dependency changes in every later PR.

#### PR 2 — Establish `src/lib.rs` and stable test-support access

**Implements:** the first slice of WP-01 and WP-16.

Move module declarations from `main.rs` to `lib.rs`, make the server binary call one public bootstrap entry point, and expose only deliberate test-support ports. Do not make internal fields public merely to keep the monolithic DST file compiling. Where a test needs an observation, introduce a semantic probe such as `ShardProbe::durable_tail()` or a deterministic failpoint—not a raw field getter.

Required proof:

- no wire or storage change;
- the existing binary entry points still construct the same runtime;
- all existing tests have an equivalent stable target;
- release builds do not expose test-only behavior.

#### PR 3 — Parse and validate one immutable configuration graph

**Implements:** WP-01.

Introduce `AppConfig` and its owned sub-configurations. Preserve every existing CLI/environment default and precedence rule through table-driven tests. The first migration may leave read-only adapters that return values from `AppConfig`, but no new direct environment read is permitted after this PR.

Do not rename user-facing flags while moving them. Flag cleanup is a separate semantic change.

#### PR 4 — Inject clock, entropy and runtime identity

> **Status: DONE (foundational slice, 2026-09-01, after PR 3.2.1).**
> `src/runtime.rs` defines the per-runtime capabilities — `Clock`
> (→ `TrustedNow`, a type DISTINCT from customer timestamps),
> `Entropy`, `RuntimeIdentity`, bundled as `RuntimeCaps` — with ZERO
> statics: no seed atomics, no `OnceLock` (the review constraint).
> `bootstrap::run` mints `RuntimeCaps::production` (OS clock, OS
> CSPRNG, boot id from that CSPRNG) and hands it to owners;
> `AppState.runtime` carries it. Migrated and RETIRED ambient state:
> `billing::boot_id()`'s process-global `OnceLock` is DELETED (six
> call sites — audit deny/deny-gap ids, debug/load, the
> x-streams-boot-id header, ops gap keys, the billing MeterSource —
> now read the runtime's identity); `http::rand_epoch()`'s ambient
> RNG is DELETED (epoch minting goes through `RuntimeCaps::epoch`);
> `TouchJournal` template-id entropy comes from the registry's
> injected capability; the unready watchdog is the retry-timing
> exemplar (cadence + elapsed through the injected clock).
> Proof tests: two runtimes in one process share no boot identity and
> no timing state (independent `ManualClock`s); production runtimes
> mint distinct unpredictable ids; a `ManualClock` sleep completes
> exactly on advance with no process-global clock lock; seeded
> entropy reproduces byte-for-byte. `SeededEntropy`/`ManualClock` are
> `cfg(test)`-only — release builds cannot NAME a predictable entropy
> source, so token/security code cannot accidentally receive one.
> Deferred to later WP-15 slices, recorded honestly: the ~124 direct
> `shard::now_ms` reads and remaining timer loops migrate as their
> owners are extracted (WP-02) — each migration retires its ambient
> read; the watchdog's survival `process::exit` awaits WP-15 task
> supervision (result policy), and crypto key generation keeps the OS
> CSPRNG directly until its owner moves.
> **PR 4.1 (corrective, review-driven, 2026-09-02):** the review found
> the watchdog migration had REGRESSED elapsed-time measurement to the
> wall clock, and the principal DST rig still selected OS entropy.
> Commit A: `Clock` has two DISTINCT domains — `now() -> TrustedNow`
> (wall, timestamps; private representation) and `monotonic() ->
> MonotonicNow` (elapsed only; per-runtime `Instant` origin in
> production); `sleep` lives in the monotonic domain; `ManualClock`
> moves wall and monotonic time independently (`advance`, `jump_wall`,
> `advance_monotonic`). The watchdog is a pure state machine
> (`UnreadyWindow` → `WatchdogDecision`) fed monotonic readings — tests
> pin: forward wall jump does not expire, backward wall jump does not
> postpone, expiry exactly at the monotonic limit, ready clears and a
> later unready period starts fresh, sleeps ignore wall jumps.
> `TrustedNow` gained its first production consumer (audit event
> timestamps) rather than being baselined dead. Commit B: the rig
> builds domain-separated seeded capabilities (identity / stream-epoch
> / touch-journal streams) over a `ManualClock`, never
> `RuntimeCaps::production`; `TouchRegistry::default` is deleted; the
> startup canary key is `<instance>-<boot_id>` (no pid, no wall nanos);
> the probabilistic identity test is replaced by a scripted-entropy
> proof (exactly one 16-byte draw per runtime from its OWN source); a
> same-seed end-to-end test reproduces boot id, stream epoch and the
> registry-class store trace across two fresh rigs. Commit C:
> `ValidatedServerConfig` fields are PRIVATE (bootstrap consumes
> `into_bootstrap_parts()`); validation emits NO logs — typed
> `ConfigNotice`s are collected and emitted by bootstrap after the
> whole configuration was accepted; the old `validate_release_capacity`
> is two functions (`validate_configured_capacity`, pure, and
> `resolve_effective_capacity`, the preflight) with no `nofile == 0`
> sentinel and no in-place mutation; `validate()` is 25 lines
> orchestrating seven domain helpers; bootstrap re-exports nothing
> from config (HTTP calls `config::profile::compactor_profile_json`
> directly, and the profile diagnostics own `config/profile.rs`);
> baseline-diff identities are presence-only with count movement
> reported as SHRINK/GROWTH; `trace_store_tests` is five behavior
> modules under 310 lines each; the product-side precedence rescan is
> DELETED — `CanonicalStreamName::new` checks every component before
> the reserved root (characterized by `name_error_precedence`). The
> architecture baseline remains unrefreshed.
>
> **PR 4.1.1 (corrective, review-driven, 2026-09-02):** the review of
> PR 4.1 found the principal HTTP rig collapsed every simulated
> process onto ONE incarnation (a fixed seed for every fresh rig), so
> restart / second-cold-server / two-instance tests shared a boot id
> and the first stream and touch-journal epochs, and the advertised
> touch-journal proof was missing. Commit A: every rig carries an
> explicit `RigIncarnation` — `RigRuntime` (caps + manual clock +
> touch entropy) is derived from (base seed, incarnation) through a
> splitmix fold, never a process-global counter; the ten-argument
> `http_rig_inner` is gone, replaced by `http_rig_build(store,
> RigRuntime, HttpRigOptions)` returning an `HttpRig { state, addr,
> clock }` fixture (the returned clock is proven to be the runtime's
> clock); every restart / peer-instance site names its incarnation
> (`http_rig_at`, `http_rig_named_at`, `http_rig_owner_at`,
> `http_rig_park`, `pm_enforce_rig`). Proofs: same incarnation
> reproduces boot id, stream epoch, touch-journal epoch and the
> registry-class trace; distinct incarnations are distinct AND pinned
> (golden boot ids / touch epochs for incarnations 0 and 1); a restart
> answers a foreign-incarnation touch cursor with the existing RESYNC;
> a stream epoch survives restart but not hard-delete + recreate.
> Commit B (non-blocking follow-ups the review named): `ConfigNotice`
> lives in `config/notice.rs` (the descriptor reserve travels in the
> notice, so the module depends on nothing), the unused
> `ValidatedServerConfig::notices()` accessor is deleted. Architecture
> status, stated precisely: the baseline-diff resolves the
> over-budget FILE finding for bootstrap.rs and dst.rs only;
> registry.rs is NOT resolved on size (it grew 2,131 → ~2,279) — what
> resolved for registry.rs is a forbidden-edge finding. The capacity
> posture typing (`ConfiguredCapacity::{Release, Development}`,
> optional descriptor limits, posture consumed once) is deferred to
> the AdmissionController extraction in PR 6, per the review.

> **PR 6-A (WP-02 first half, 2026-09-02):** the first two runtime
> owners exist as concrete handles with narrow methods, and their six
> `AppState` fields are DELETED (57 → 53; the architecture report now
> carries an `AppState fields` counter with a baseline of 57, so every
> extraction shows the count falling until the struct is gone).
> `ownership::OwnershipService` is the pure ring-policy resolver
> (instance name, active set, rebalancer overrides; `effective_owner`,
> `foreign_owner`, `is_mine`, `view`; rendezvous `ring_pick` moved out
> of the transport). `shard_directory::ShardDirectory` owns the
> topology prefixes, the serving map, the single-flight `OpenGate` and
> the ONE resolution policy — `resolve(hash, Adoption) ->
> Result<Arc<ShardEngine>, ResolveError>` (possession yields to the
> ring, external adoption stamps under the read guard, bounded
> single-flight opens) — plus `open_or_wait`, `open`, `is_open`,
> `engines`, `held_prefixes`, `evict`, `notify_closed`, and
> `remove_if`, which keeps the R30 custody protocol's one write guard
> through remove → decide → reinstate inside the owner (the sweep's
> CAS is the decision). The HTTP layer keeps exactly one transport
> mapping (`resolve_error_response`: not-owner → 409 +
> Streams-Replay-To, opening → 503 + Retry-After, failure → 500) behind
> thin `engine_for*` adapters that PR 8 deletes with `AppState`; fleet,
> billing, backpressure (`snapshot(&ShardDirectory)`), ops, operator,
> SSE source and scaler callers migrated to the owners. Owner proofs:
> ownership (no ring = serve everything; the pick is shared and
> "foreign" is relative; an override is honored only for an active
> target) and directory (a foreign shard is refused with its owner and
> the opener is never consulted; open failure typed; a slow open is a
> retryable typed refusal with one single-flight open; `remove_if` on
> an empty slot is Absent; routing is the topology hash). The rig
> builder composes the owners the way bootstrap does and its opener is
> a named helper (under the function budget). Remaining WP-02 owners
> follow in 6-B (AdmissionController, with the unforgeable capacity
> posture), 6-C (PeerClient, LiveFeedService, raw bearer), 6-D
> (RegistryService), 6-E (BillingService), 6-F (TaskSupervisor).
>
> **PR 6-B (WP-02, 2026-09-02):** `admission::AdmissionController`
> owns every request-admission gate and counter — the global in-flight
> gate, the pre-auth survival bound (4× the cap, stream paths only,
> never a capacity answer), the RSS write-shed (sampled RSS plus the
> absorber's reservation against the line), per-stream slots (bounded
> per stream, fail-open at 65,536 tracked streams, released at zero),
> the live-subscription budget, the maintenance-backpressure latch and
> the fleet load vector. Counters are PRIVATE: request paths hold RAII
> tickets (`InflightTicket`, `StreamSlot`, `SubscriptionTicket`) and
> ask typed questions (`survival_refused`, `admit_write_inflight`,
> `admit_write_memory`, `stream_slot`, `subscribe`); operator, debug
> and ops surfaces read one immutable `snapshot()`; the HTTP layer
> keeps only the wire shapes and the tarpit. Twenty `AppState` fields
> are DELETED (53 → 34). The capacity posture is UNFORGEABLE:
> `ConfiguredCapacity::{Release(NonZeroU64), Development(u64)}`
> encodes the posture once, `DescriptorLimits { soft, hard:
> Option<NonZeroU64> }` replaces the zero sentinel from the probe, and
> `resolve_effective_capacity(configured, limits, notices)` reads the
> posture from the value (no second boolean) — a development capacity,
> including the unlimited 0, can never be resolved as release, and an
> absent ceiling under release is a typed `DescriptorCeilingUnknown`
> warning. `EffectiveCapacity` now carries the configured value it was
> resolved from and is installed into the controller; bootstrap no
> longer mutates the configuration graph after validation. Proofs:
> controller units (tickets are RAII and record the peak; survival and
> ordinary gates are distinct mechanisms with distinct counters; the
> RSS gate counts reserved bytes; stream slots are bounded and
> released; the subscription budget is exact and a refusal never
> counts; the snapshot mirrors the controller) and
> `capacity_posture_is_unforgeable` beside the rewritten capacity
> matrix. The rig builder is 141 lines.
>
> **PR 6-C (WP-02, 2026-09-02):** three more owners, eleven more
> `AppState` fields DELETED (34 → 26). `peer::PeerClient` owns how this
> instance addresses and authenticates to its fleet peers: the trusted
> URL table (`url_for`, `has_peer`, `set_peers` from the fleet loop),
> the outbound bearer (workload identity when a source is configured,
> else the static bridge token), the SR3-1 exclusivity rule for an
> inbound static credential (`inbound_static_ok`: dead the moment a
> workload source exists), the fleet-internal `send` with its single
> 401 refresh-and-retry, and the fleet object store; `FleetTokenSource`
> lives with it. `sse::service::LiveFeedService` owns the feed
> registry, the per-runtime memory budget, the ring allowance and the
> keep-alive cadence (`registry`, `budget`, `ring_bytes`,
> `heartbeat_ms`, `snapshot`); two rigs in one process never share a
> budget. `deployment_bearer::DeploymentBearer` owns the raw surface's
> static account credential and the conformance default key
> (`authorizes(presented, mode)`: allow-if-unset only in Off mode;
> `default_key`). The per-record payload ceiling joined the admission
> controller (`record_ceiling`). Constant-time `secret_eq` moved to
> `crypto` so the two credential owners share one implementation; the
> HTTP layer keeps only header parsing and wire shapes. Proofs: the
> peer table is replaced wholesale and read by name; credential modes
> are exclusive (a leaked static token is dead in workload mode, the
> bridge token compares in constant time, nothing authorizes with
> neither); live-feed services are per runtime; the deployment bearer
> opens only Off mode when unset and compares in every mode when set.
>
> **PR 6-D (WP-02, 2026-09-02):** the registry owner already existed
> (`registry::Registry`, typed cell, cache, CAS); what `AppState` still
> held beside it was WHO this deployment is. `deployment::
> DeploymentIdentity` owns the deployment tenant, the billing account,
> the telemetry cell and the region, and `raw_adapter_sref` moved onto
> it under its own name (the lint's caller rule is by method name, so
> the raw-adapter confinement is unchanged). The deployment tenant is
> reachable only through `deployment_tenant()`, and mt-lint's
> `state-tenant-read` now flags that accessor exactly as it flagged the
> field it replaced — every one of the seven reviewed sites keeps its
> marker; the owner's own accessor body carries the eighth. Four more
> fields DELETED (26 → 23); audit, billing, fleet, ops, product and the
> HTTP raw adapters read the identity through the owner. Proofs: the
> raw adapter's identity source qualifies a canonical name under the
> deployment tenant and nothing else; a non-canonical name is refused
> loudly; the lint test itself pins the accessor rule against the tree.
>
> **PR 6-E (WP-02, 2026-09-02):** `billing_service::BillingService`
> owns the usage-ledger key, the read-usage accumulator, the read spool
> and usage rollup slots (installed exactly once by the telemetry loops
> — `install_read_spool` / `install_rollup` keep the once-only result
> shape — and read through `read_spool()` / `rollup()`), and the sweep
> scheduler's bookkeeping (`sweep()`: custody marks, quantum cycles,
> the walk cursor). Five more fields DELETED (23 → 19; 57 → 19 since
> the extraction began). billing, audit, fleet, ops, product, the HTTP
> debug/usage surfaces and the SSE metering path read billing state
> through the owner; the data store stays on `AppState` for the read
> and append services of PR 9. Proof: billing-off has no key and empty
> slots, and two services never share an accumulator.
>
> **PR 6-F (WP-02 + the first slice of WP-15 §7–9, 2026-09-02):**
> `tasks::TaskSupervisor` owns every long-lived loop a runtime spawns:
> the unready and runtime watchdogs, the auth refresher, the scaler,
> the RSS sampler, the fleet loop, the telemetry outbox sweep and
> drain, the usage rollup, and — in the rig — the HTTP accept loop.
> Each child has a name, a policy (Critical / Noncritical), a
> cooperative `Cancellation` handle and a join handle the supervisor
> keeps; `shutdown(grace)` is ordered and bounded (cancel, wait the
> grace, abort the rest) and reports finished / aborted / panicked by
> name; `critical_failure()` names the first critical loop that exited
> on its own (readiness adopts it in WP-15's remaining slice, and the
> debug surface shows it today); a shut-down supervisor spawns nothing.
> Request-scoped child tasks are deliberately NOT supervised here. The
> composition roots spawn through it (one field added, 19 → 20 — the
> supervisor's own handle, deleted with `AppState` in PR 8). The two
> restart proofs now TERMINATE the old process through its supervisor
> before starting the replacement incarnation, so restart evidence is
> literal. Proofs: shutdown is ordered and bounded (a polite loop
> finishes inside the grace, a stubborn one is aborted, the loop is
> gone afterwards, a second shutdown finds nothing); critical exits are
> failures, noncritical exits are not, panics are reported.
>
> **PR 6.1-A (Oracle corrective on PR 6, 2026-09-02): runtime shutdown
> is real.** The supervisor's registration and shutdown share ONE
> phase-locked state (`Running → ShuttingDown → Stopped`): a spawn
> after the drain began is refused (`SpawnRejected`), never registered.
> `spawn` builds each loop WITH its `Cancellation`
> (`FnOnce(Cancellation) -> Future<Output = TaskResult>`), so every
> supervised loop observes cancellation by construction — all nine
> production loops select on it at their iteration boundary. `shutdown`
> cancels, waits to one shared deadline, aborts the survivors and then
> JOINS every task, aborted ones included, recording a typed
> `TaskOutcome` (finished / failed / cancelled / panicked) per task; a
> drop probe proves an aborted future is destroyed before it returns.
> The HTTP accept loop owns its connections (a `JoinSet`): cancellation
> stops accepting, releases the listener, then aborts and joins every
> connection — proofs: the address rebinds immediately after shutdown, a
> live keep-alive connection and a live SSE subscription are closed
> before shutdown returns. Production reaches shutdown through a
> supervised signal task (SIGTERM / Ctrl-C request it through a weak
> `ShutdownRequest`; `serve_h1` returns once cancelled). No loop starts
> before the last fallible startup step: the required billing opens and
> the listener bind moved ahead of the first spawn and the unready
> watchdog starts with the other loops, so an early `?` strands nothing.
> `AppState` holds a read-only `TaskMonitor` (weak), not the supervisor:
> no state → supervisor → task → state cycle. The store-timing sentinels
> are the documented process-lifetime exception (instrumentation of the
> process, no runtime state). Proofs: shutdown is ordered, bounded and
> joins everything; registration cannot race shutdown (8 racing
> spawners × 20 rounds: every accepted spawn is in the report, every
> spawner is refused once, nothing outlives the drain); aborted tasks
> are destroyed before shutdown returns; critical exits, typed failures
> and panics are reported through the monitor; `cancel` closes
> registration before the join.
>
> **PR 6.1-B (Oracle corrective on PR 6): ShardDirectory ownership is
> complete.** The directory builds its serving map and open gate
> TOGETHER (`ShardDirectory::new(prefixes, ownership, OpenTiming,
> opener_factory)`): no constructor path can hand the directory and its
> gate different maps. The opener is a factory over the directory's
> `ShardCloseNotifier` — a weak handle to the directory's internals and
> the ONE capability an engine's close needs — so the production opener
> captures no `AppState`, weak or strong; the weak state slot and
> `http::ShardOpener` are deleted, and `OpenGate::shards()` with them.
> Every open attempt is minted an `EngineIncarnation`; the gate records
> the resident as `{engine, incarnation}`, the engine's close callback
> carries its incarnation, and `notify_closed(prefix, incarnation)`
> evicts (and arms the holdoff) only when the resident IS that
> incarnation — a stale close cannot remove a replacement. Proof: the
> forced ordering old-close-after-new-insert (A opens, A's first close
> evicts A, B opens as the new resident, A's late db-close arrives and
> changes nothing, B's own close evicts B); the existing gate, flap and
> directory proofs run over the fenced shape.
>
> **PR 6.1-C (Oracle corrective on PR 6): the service surfaces are
> honest.** `BillingService` owns the read-metering protocol
> (`meter_read`, `meter_read_chunk`, `seal_aged_reads`,
> `drain_sealed_reads`, `requeue_reads`, `unflushed_reads`,
> `read_seal_deferrals`), the durable spool protocol
> (`install_read_spool`, `read_spool_open`, `spool_sealed_reads`,
> `pending_spooled`, `remove_spooled`, `read_spool_stats`,
> `read_spool_health`) and the R30 sweep protocol (custody claim and
> release, cycle accounting, custody lookup, rotation, resident count,
> peak, walk cursor). The raw `ReadUsageAccumulator` and `ReadSpool`
> accessors are `#[cfg(test)]`; `SweepSched` is `pub(crate)` and nobody
> outside the service locks its fields. The usage rollup is a DATABASE,
> not a decision this service makes, so it became its own explicit
> install-once owner (`rollup::RollupSlot`) rather than a second facade
> — the plan's "keep the existing concrete owners explicit" option
> (AppState 20 → 21 fields; the dependency graph is what changed).
> `LiveFeedService::subscribe(key, src, project, bind)` creates the feed
> with ITS ring allowance and ITS budget, and `wake_all_sessions()`
> replaces reaching through the registry (`registry()`/`budget()` are
> `#[cfg(test)]`). `AdmissionController` owns the maintenance latch
> (`apply_maintenance`, `admit_maintenance`, `note_maintenance_shed`,
> `maintenance_engaged`, `maintenance_stats_json`); the latch accessor
> is private, so PR 7's application layer cannot import it.
>
> **PR 6.1-D (Oracle corrective on PR 6): the fleet globals are gone.**
> `fleet_store_slot()` and `last_good_urls()` are deleted. The
> coordination store is now `fleet::FleetRepository`, owned per runtime
> on `AppState` (`enabled`, `store` for the fleet module's own loop and
> the operator's fleet views, `read_doc`, `replace_doc` for the CAS
> outbox clear): the fleet loop, the event drainer and the operator
> surface read ONE authority, and `PeerClient` no longer carries a fleet
> store it did not own (peer routing, credentials and the one-refresh
> send stay with it). The last-good published-URL map became the fleet
> loop's own local history — one runtime's fallback cannot become
> another's, structurally. Proof: two runtimes with different fleet
> stores; A's drainer emits nothing from B's outbox and does not clear
> B's document; a runtime without fleet coordination has no repository
> and drains nothing.
>
> **PR 6.1.1-A (Oracle corrective on PR 6.1): shutdown is single-flight
> and cancellation-safe.** One internally spawned DRIVER owns the
> drained task handles: the first caller transitions
> `Running → ShuttingDown` under the state lock, moves the task map into
> the driver, starts it once, and then awaits its completion like every
> other caller. Later callers only await that same completion — they
> never drain, never re-run the sequence and never declare `Stopped`;
> only the driver does, after every handle is joined, and it stores the
> terminal report so a caller arriving later receives that report rather
> than an empty one. Because the driver is a spawned task, dropping or
> cancelling a waiting caller cannot detach the tasks: the caller never
> owned them. Outcomes are keyed by `TaskId`, so the report is in
> REGISTRATION order rather than completion order. Proofs: two
> concurrent callers stay pending while a stubborn task lives, both
> return only after it is aborted AND joined with the same report, and
> the monitor reads `ShuttingDown` throughout; polling the first
> shutdown until the drain begins and then dropping it still leaves a
> later caller waiting for real termination (drop probe fired);
> outcomes come back in registration order. Two review follow-ups land
> here too: the SIGTERM source is installed as a fallible preflight and
> the prepared listener handed to a Critical task (a registration
> failure used to panic a noncritical child and silently cost the
> runtime its graceful-shutdown input), and the restart proofs now say
> the old SERVER SURFACE and its supervised loops are terminated before
> the replacement starts — engine-internal and open-gate helper tasks
> join the supervisor with WP-15.

**Implements:** the foundational part of WP-15.

Replace HTTP-owned randomness and process-time lookups in core/background paths with explicit `Clock`, `Entropy` and `RuntimeIdentity` values. Migrate boot IDs, epochs, retry timing and test time first. Keep cryptographic randomness behind an appropriately strong production implementation; deterministic entropy is test-only.

Required proof:

- two runtime instances in one process do not share boot identity or mutable timing state;
- deterministic tests no longer acquire process-global clock/environment locks for migrated areas;
- token/security code does not accidentally receive predictable production entropy.

### 25.2 Phase B — Correct the dependency direction

#### PR 5 — Canonical identity and descriptor conversion boundary

> **Status: FIRST SLICE DONE (2026-09-01).** Landed:
> (1) `product::ProductStreamName(CanonicalStreamName)` per the WP-03
> target types — the identity layer is THE structural validator and
> the product type adds only the addressability extras (reserved
> final segments, subresource-shaped names). The duplicated
> structural validation block, the `debug_assert!` agreement pin, and
> `canonical_stream_name`'s `expect` are DELETED;
> `canonical_name` is a thin wire adapter over the type. Wire error
> messages are test-pinned per case, including the multi-violation
> precedence corner (`__ds/..` reports the dot segment, preserving
> the historical segment-scan order via an explicit fixup).
> (2) Descriptor decode-boundary completion: stored REFERENCES are
> proven at decode — fork `source` canonical, fork `source_epoch`
> 16 bytes, every `fork_children` entry canonical — refusing with the
> existing fail-closed corruption class (never a downstream panic,
> never a silent repair); red-tested by corrupting one field at a
> time from a valid descriptor (fresh-registry reads, since the 5s
> descriptor cache masks decode). With decode proving name, identity
> match, layout, and stored references, `StreamDesc::sref`/
> `ref_in_project`'s reconstructions are invariant-backed at ONE
> boundary.
> Deferred to the next WP-03 slices, recorded honestly:
> the persisted-DTO/domain-descriptor split (§5.4) — until it lands
> the two reconstruction `expect`s remain (backed by decode, no
> longer by convention); `ProjectId::stream_ref(&str)`'s ~51 request-
> flow call sites migrate to the typed parameter with WP-04's
> transport/application split (the name will be typed end-to-end
> there); `stream_epoch` WIDTH stays Option-tolerated at decode (the
> current `epoch_bytes()` contract — registry test descriptors mint
> short epochs today); consumer-config `dead_letter_stream`
> references validate where consumer configs decode (their boundary,
> not the stream descriptor's); lifecycle state modeling and typed
> use-case errors are their own WP-03 slices.

**Implements:** WP-03.

Introduce checked `ProjectId`, `StreamName`, `StreamRef`, segment and consumer identities as the only accepted application/domain inputs. Add an explicit conversion between the unchanged persisted descriptor DTO and a domain descriptor. Delete duplicate name validation and panic-based typed reconstruction as each caller migrates.

Keep layout-4 JSON byte-for-byte compatible. A descriptor conversion failure is typed persisted-data corruption, not a fallback to an unqualified string.

#### PR 6 — Extract concrete runtime owners behind a temporary state adapter

**Implements:** the first half of WP-02.

Construct `RegistryService`, `ShardDirectory`, `AdmissionController`, `OwnershipService`, `PeerClient`, `AuthService`, `QuotaService`, `BillingService`, `ReadService`, `LiveFeedService`, and `TaskSupervisor` as concrete handles with narrow methods. Initially, `AppState` may contain those handles so routes can migrate incrementally.

This is not complete if the new services merely receive `&AppState`, expose all old fields, or become a generic service locator. Every migrated owner must take its own configuration and dependencies at construction.

#### PR 7 — One-time request admission and transport-neutral outcomes

**Implements:** WP-04.

Create typed route parsing, `RequestContext`, one-time auth/admission, application commands/queries and a centralized HTTP error/reply mapper. Migrate a narrow vertical slice—preferably metadata or another low-risk route—through the full boundary before generalizing it.

The application layer must not import Axum. Direct application tests construct an already-authorized context; they do not call a second copy of the auth gate.

#### PR 8 — Delete `AppState` dependency edges subsystem by subsystem

**Implements:** the completion of WP-02.

Migrate product, billing, fleet, scaler and SSE callers to their explicit capabilities. Delete old fields immediately after the final caller moves. End this PR—or a short sequence of tightly bounded PRs—with zero production imports of `http::AppState` outside the adapter scheduled for deletion.

Do not leave `AppStateV2`, `Services`, `ContextBag` or another 50-field aggregate as the replacement.

### 25.3 Phase C — Extract the application protocols

#### PR 9 — Append and read vertical slices

**Implements:** WP-07 and WP-08 at the application boundary, without yet rewriting the shard committer.

Introduce `AppendService` and `ReadService` over the current shard/registry implementations. Separate public query DTOs from trusted read policy. Route both product and raw handlers through these services while preserving their distinct wire contracts.

Required proof includes duplicate/producer verdicts, durable versus applied visibility, bounded reads, lineage refresh, one-hop relay, billing/metering and exact response mapping.

#### PR 10 — Creation, fork and deletion coordinators

**Implements:** WP-05.

Convert the existing long create/delete flows into explicit, idempotent coordinator steps over persisted lifecycle facts. Each step must be restartable; each external side effect must either be guarded by a persisted transition or be idempotent by construction. Keep the existing descriptor representation and CAS authority.

Do not make a generic workflow engine. These protocols have different state and proof obligations and should remain named, typed coordinators.

#### PR 11 — Seal coordinator and generation-fenced capabilities

**Implements:** WP-06.

Move claim acquisition/renewal, fence installation, finalization, debt repair and publication into an explicit seal state machine. Pass an unforgeable typed seal capability to append/close paths rather than spreading generation comparisons across handlers.

Required proof includes stale claimant rejection, claimant loss at every suspension point, crash/restart after every durable step, exact final-content handling and no publication before physical obligations are complete.

#### PR 12 — Consumer/queue and watch/live-feed services

**Implements:** WP-09 and WP-10.

Extract consumer generation/fence/delete protocols first, then watch authorization and feed/session ownership. Keep queue mutations serialized through the shard lane. Make watch/SSE cutoffs typed and hold admission guards for the response/session lifetime.

Split this into two PRs if either diff mixes more than one protocol transition change with file movement.

### 25.4 Phase D — Simplify the physical commit core

#### PR 13 — Introduce `PreparedCommit` and explicit effect phases

**Implements:** the first half of WP-11.

Keep the existing `CommitOp` and operation logic initially. Change only the shape around it:

1. group collection returns an immutable group;
2. preparation returns `PreparedCommit`;
3. database write is the only fallible physical commit point;
4. applied effects publish only after write success;
5. durable effects and replies remain attached to the committed sequence until the durable watermark passes.

This PR should make the phase boundary mechanically enforceable before command logic is moved.

#### PR 14 — Introduce `StreamTxn` and typed command families

**Implements:** the completion of WP-11.

Migrate one semantic family at a time into command applicators and compare the exact batch/effect trace against the baseline. A recommended order is seal fence/close, append/producer, consumer, billing, then maintenance. Delete each old variant/branch as its typed replacement lands.

Do not run two commit implementations side by side behind a flag. The migration adapter may translate an old caller into the new command, but the lane has one executor at every commit.

#### PR 15 — Centralize keyspace and codecs

**Implements:** WP-12.

Move existing key construction and encoding into typed keyspace/codec modules without changing bytes. Perform this after the commit phases are explicit so codec movement is not mixed with control-flow movement. Replace all raw prefix/tag assembly, then enable the CI gate.

Required proof is byte-level golden equivalence, including malformed/corrupt decode behavior and ordered range boundaries.

### 25.5 Phase E — Turn long-running loops into reconcilers

#### PR 16 — Billing, rollup and history controllers

**Implements:** WP-13.

Separate snapshot/input acquisition, pure planning, side-effect execution and durable checkpoint publication. Keep append-observation atomicity in the shard command lane; the external billing drainer consumes the durable outbox rather than inventing a second truth.

Each loop must expose a single-step method that deterministic tests can drive without sleeping.

#### PR 17 — Fleet, scaler and supervised task ownership

**Implements:** WP-14 and the remaining task-supervision portion of WP-15.

Make fleet/scaler policy pure over snapshots, make actions typed and idempotent, and run every background task under an owning supervisor with explicit cancellation and failure policy. Replace detached-spawn semantics and process-global controls.

Prove that task failure is visible and that shutdown joins or deliberately abandons every owned task according to one documented policy.

### 25.6 Phase F — Finish tests, crates and enforcement

#### PR 18 — Decompose DST by invariant domain

**Implements:** the incremental half of WP-16.

Move tests into bounded scenario modules, introduce stable scenario IDs, replace timing guesses with semantic failpoints, and remove copied production state-machine logic from models/helpers. Delete migrated sections from `dst_tests.rs` immediately; do not leave a forwarding include tree that remains one conceptual file.

#### PR 19 — Build the production-port simulator

**Implements:** the simulator half of WP-16.

Use the now-explicit store, clock, entropy, platform and transport ports to run production application/state-machine code under deterministic scheduling. Add serializable choice traces, state hashes and an independent reference/audit model. Keep field tests for real platform/process phenomena.

#### PR 20 — Workspace cut, hard gates and archaeology cleanup

**Implements:** WP-17.

Move stable modules into `streams-core`, `streams-server` and `streams-sim`; enable the architectural budgets; delete temporary adapters; move long incident narratives to invariant documents/ADRs; and run the full release certification against the produced server artifact.

### 25.7 Merge discipline

For every PR above:

1. State the invariant IDs touched.
2. List exact observable contracts expected to remain unchanged.
3. Show before/after dependency and size metrics.
4. Separate pure moves from semantic edits where review would otherwise be ambiguous.
5. Delete superseded code in the same PR or the immediately following named PR.
6. Include failure-path tests, not only happy-path equivalence.
7. Stop the sequence if a baseline cannot explain a behavior difference. Do not normalize a mismatch as “probably harmless.”
8. Do not waive performance/cost changes merely because the refactor passes functional tests.

---

## 26. Verification matrix

This matrix is the minimum evidence package for declaring the program complete. Existing repository commands and scenario names should be inserted into the machine-readable gate catalogue created in WP-00; the table describes proof scope rather than inventing replacement test commands.

| Contract area | Deterministic/unit proof | Integration/conformance proof | Performance/field proof |
|---|---|---|---|
| Remote durability acknowledgement | blocked/lost write and watermark schedules; no early reply; failed batch publishes nothing | raw and product append success/failure parity | remote-store failure campaign; append latency and requests-per-WAL baseline |
| Producer/idempotency semantics | duplicate body match/mismatch, sequence gaps, retry after reply loss, same-group conflicts | exact status/header/body snapshots for both surfaces | sustained retry/replay campaign without duplicate durable rows |
| Stream creation/readiness | crash after every persisted init step; repair is monotonic/idempotent | create, initial content, fork and immediate read parity | repeated process-restart campaign during creation |
| Seal/finalization | claim generation/renewal/loss, fence ordering, final hash/readback, restart at every stage | exact close/seal/final responses and headers | concurrent writer/sealer field campaign |
| Fork lineage and cleanup debt | source/child race schedules, debt retry, stale epoch/descriptor refresh | lineage reads, bounded fan-out and deletion behavior | multi-cell/peer movement campaign where available |
| Read/scan semantics | planner boundary cases, applied/durable visibility, cursor/limit/byte caps | exact records/cursors/statuses for product and raw APIs | throughput/latency/RSS at representative payloads and routing-key counts |
| Consumer/queue lifecycle | generation/fence/lease/pull/settle/delete/recreate interleavings | exact consumer route and token behavior | long-running consumer churn and restart campaign |
| Watches/SSE | admission, auth expiry, ownership movement, heartbeat under blocked source, cancellation guard release | exact framing, CORS and terminal event/error mapping | connection/memory/payload budget campaign |
| Multitenancy/auth | project-qualified key/state assertions, wrong-cell distinction, token/key intersection, relay auth | negative matrix for product/raw/operator surfaces | two-project collision and ownership-transfer field campaign |
| Billing/usage | append+usage dirty row atomicity, replay/idempotence, close/retention and recovery | usage/catalog/operator response parity | ledger reconciliation against committed appends; object-request cost bounds |
| History/rollup/trim | pure plan tests, checkpoint monotonicity, absorption/trim crash schedules | read compatibility before/after background work | existing cost campaigns and idle/active request budgets |
| Fleet/scaler | pure policy table/property tests, stale snapshot/CAS conflict, retry idempotence | operator/control-plane integration fixtures | multi-cell soak; action rate/budget and convergence measurements |
| Configuration/startup | table-driven default/precedence/invalid-combination tests | binary startup/health/readiness/shutdown tests | no unexpected startup object-store or network operations |
| Task ownership/shutdown | child failure propagation, cancellation/join, no leaked guard/lease | process SIGTERM/graceful shutdown tests | repeated start/stop and blocked-dependency campaign |
| Storage compatibility | byte-level key/value goldens, ordered ranges, malformed decode | open/read/write a baseline-created layout-4 fixture | no new LIST/GET/PUT/DELETE classes or unexplained count changes |
| Simulator integrity | model-vs-production transition checks, anti-vacuity, trace replay/shrink | deterministic scenario catalogue completeness | compare selected L2 outcomes with L3 field campaigns |

### 26.1 Required comparison artifacts

Every behavior-sensitive PR should attach or generate:

- exact changed golden files, with an explanation for any intentional change;
- before/after module dependency edges;
- before/after source and function-size report;
- test/scenario IDs run and their results;
- object-store operation counts for touched storage paths;
- throughput/latency/RSS comparison for touched hot paths;
- a statement that no storage migration or compatibility path was introduced;
- a list of deleted branches/types/helpers, demonstrating that complexity was removed rather than hidden.

### 26.2 Failure-injection standard

A timeout may guard a test, but it is not evidence that a code path was reached. Each concurrency or crash test must have:

1. an **entered** signal proving the production hook/operation was reached;
2. a **release/fail** control owned by the test;
3. an assertion about state while execution is parked;
4. a terminal assertion after release, cancellation, reply loss or restart;
5. anti-vacuity evidence that the relevant command/object/key/transition actually occurred.

---

## 27. Quantitative completion criteria

The targets below make the architectural outcome reviewable. They are not permission to game line counts by creating meaningless wrappers.

### 27.1 Hard completion gates

- `http::AppState` no longer exists.
- No application/domain/shard/storage/background module imports `http`, Axum response/body/header/status types, or server bootstrap types.
- No direct `std::env::var`/`var_os` call exists outside the configuration loader and explicit configuration tests.
- No raw storage tag/prefix/key assembly exists outside the typed keyspace module.
- No persisted descriptor serde is performed outside its codec/registry adapter.
- No production or test Rust file exceeds 1,000 lines without a temporary, owned and expiring exception.
- `src/dst/dst_tests.rs`, the monolithic `commit_group`, and the monolithic `create_stream` implementation are deleted.
- Every long-running task has an owner, cancellation path, failure policy and deterministic single-step test seam.
- Every customer-facing identity passed to application/storage code is typed and project-qualified.
- The core crate has no dependency on the server or simulator crates.
- All exact wire and layout-4 storage goldens are unchanged unless an independently approved semantic change explicitly updates them.
- All required functional, deterministic, conformance, field, cost and release gates pass.

### 27.2 Design targets

- Production/test files target **700 lines or fewer**.
- Orchestration functions target **120 lines or fewer** and may not exceed 200 without an exception.
- Pure transition/applicator functions target **80-100 lines or fewer**.
- `main.rs` should contain only argument parsing invocation, logging/runtime setup invocation, bootstrap invocation and terminal error handling; target **under 250 lines**.
- The top-level shard owner should hold **at most about 20 cohesive handles/configuration values**, not dozens of unrelated fields.
- Application commands and queries should have no irrelevant optional fields; invalid combinations should be unrepresentable.
- Mutable process-global state should trend to zero, with a narrowly documented allowlist only for genuinely process-wide instrumentation.
- Every temporary migration adapter should survive no more than one subsequent planned PR.

### 27.3 Complexity-deletion scorecard

At the end of each phase, record:

| Measure | Baseline | Phase result | Expected direction |
|---|---:|---:|---|
| production files over 1,000 lines | 18 | — | monotonically down to 0 |
| all Rust files over 1,000 lines | 21 | — | monotonically down to 0 |
| functions over 200 lines | record in WP-00 | — | monotonically down to 0 |
| production imports of `crate::http` outside HTTP | record in WP-00 | — | down to 0 |
| direct environment reads outside config | 71 repository-wide scan baseline; classify test/production in WP-00 | — | down to approved config-only set |
| mutable global/`OnceLock` instances | record and classify in WP-00 | — | down to explicit allowlist |
| raw storage-key construction sites | record in WP-00 | — | down to keyspace module only |
| Axum type references outside server adapters | record in WP-00 | — | down to 0 |
| invariant catalogue entries with test links | 0 centralized | — | up to complete coverage |
| scenario catalogue entries with concrete tests | audit in WP-00 | — | up to 100% |

A phase does not pass merely because counts improve. Reviewers must verify that responsibilities became more cohesive and no generic indirection replaced direct coupling.

---

## 28. Risk register and controls

| Risk | How the refactor could introduce it | Required control / stop condition |
|---|---|---|
| Early success before remote durability | reply movement during handler/service or commit decomposition | exact parked-watermark tests; replies remain a durable effect; stop on any changed acknowledgement trace |
| Publication of failed provisional state | applicator mutates shared state during preparation | batch-local `StreamTxn`; publish methods unavailable before write success; fail-write snapshot equality test |
| Loss of same-group semantics | typed commands are processed against only persisted base state | one batch-local overlay per stream/consumer/billing identity; exact mixed-group trace comparison |
| Accidental second commit lane | subsystem extraction creates its own writer/queue | architectural rule: all ordering/atomicity-sensitive mutations enter `ShardCommand`; reject PR on second writer authority |
| Layout-4 drift | descriptor/key cleanup silently renames or omits fields/bytes | unchanged goldens and baseline fixture; separate codec move from lifecycle changes; stop on any unexplained byte difference |
| Auth/body-order regression | middleware extraction consumes body or opens shard before authorization | request-stage trace tests and negative large-body tests; one typed admitted context |
| Tenant identity collapse | service methods accept strings or infer project from ambient state | typed project-qualified IDs only; no default project; compile-time API review and two-project collision tests |
| Lifecycle ABA/stale-authority escape | coordinators centralize checks but lose generation capability across awaits | typed generation/fence capabilities, revalidation at suspension boundaries, drop/cancellation poisoning where required |
| Billing undercount/double count | application extraction meters both relay and origin or decouples dirty row | exact atomic row-set trace, internal-metering type, durable ledger reconciliation |
| Fan-out loop or duplicate work | read planner/executor loses one-hop relay marker | unrepresentable external/internal request modes; hop-limit tests; peer request goldens |
| SSE guard or memory leak | handler returns before ownership guard is attached to body/session | RAII session body tests under cancel/drop; instance-budget accounting returns to baseline |
| Background checkpoint inversion | planner/executor split publishes checkpoint before effects | explicit `Plan -> Execute -> CommitCheckpoint`; crash after every stage; monotonic checkpoint proof |
| Task failure becomes invisible | supervisor migration logs and restarts indefinitely or drops join handle | typed failure policy per task; fatal/degraded/retry classification; readiness and shutdown tests |
| Performance collapse from abstraction | allocations, dynamic dispatch, repeated descriptor/shard lookup | concrete handles on hot paths, trace lookup counts, benchmarks and object-request budgets per hot-path PR |
| Generic-trait explosion | desire for testability creates dozens of one-method traits | traits only at volatile/external or simulator substitution boundaries; require two real implementations or a named deterministic need |
| Permanent migration facade | compatibility adapters become new canonical APIs | owner + deletion PR on every adapter; architecture gate rejects old dependency after deadline |
| Test model repeats implementation | simulator passes because model copies production branch logic | independent semantic model/auditor; model reviews compare concepts, not source structure; mutation/anti-vacuity checks |
| Review becomes impossible due to mixed movement | huge rename plus semantic changes hides behavior drift | split pure moves and logic changes; reviewers may require a move-only precursor; no approval without explainable diff |

### 28.1 Program stop conditions

Pause subsequent structural PRs and investigate before continuing when any of the following occurs:

- an exact wire/storage golden changes without an approved product/storage change;
- a durability or lifecycle trace differs and the difference cannot be derived from an intentional invariant change;
- an operation class or object-store request count grows unexpectedly;
- the same responsibility exists in both old and new paths beyond the declared adapter window;
- a new service needs broad access to the runtime aggregate to proceed;
- a proposed abstraction increases the number of concepts required to understand the path;
- a deterministic test cannot prove it reached the mechanism it claims to test;
- a hot-path PR crosses the agreed performance/cost investigation threshold;
- the implementation requires a compatibility flag or dual writer to be safely merged.

---

## 29. Implementation and review checklist

### 29.1 Before writing a PR

- Identify the current canonical owner of every fact being moved.
- Name the invariant IDs and persisted/wire contracts touched.
- Locate all call sites, background callers, tests, scripts and gates that depend on the old path.
- Record the exact baseline behavior and failure schedules.
- Decide what concept, branch, field or dependency edge will be deleted—not only what new abstraction will be added.
- Decide whether a concrete type is sufficient. Introduce a trait only for a real boundary.
- Confirm that no storage format, external flag or API shape needs to change.

### 29.2 While implementing

- Keep route parsing/auth/admission, application decisions, storage mutation and HTTP mapping in separate stages.
- Keep batch-local state private to the commit preparation object.
- Do not send replies, update shared tails or publish wakeups from preparation code.
- Do not read environment/time/randomness through ambient process state in migrated code.
- Do not pass raw names, project IDs, cursor strings or descriptor JSON past the layer that validates/decodes them.
- Replace optional flags with enums/capabilities when variants have different invariants.
- Make retries repeat a named idempotent step; do not wrap an entire multi-stage protocol in an unstructured retry loop.
- Add semantic failpoints before relying on scheduling coincidence.
- Delete old branches as soon as callers migrate.
- Preserve direct code. A small explicit `match` over typed variants is preferred to a generic handler registry.

### 29.3 PR description requirements

Each PR description should include:

```text
Work package / phase:
Invariant IDs touched:
Observable contracts frozen:
Old concepts/branches deleted:
New types and why each earns its keep:
Temporary adapters and deletion PR:
Storage-format impact: none / approved change reference
Wire impact: none / approved change reference
Tests and failure schedules run:
Performance/cost comparison:
Before/after structural metrics:
Known residual debt:
```

### 29.4 Reviewer approval questions

A reviewer should not approve until they can answer yes to all applicable questions:

1. Did the change improve dependency direction and ownership, rather than only moving code?
2. Is there a simpler reframing that would delete more branches or concepts?
3. Are impossible command/request/state combinations now unrepresentable?
4. Does each abstraction own an invariant, or is it a pass-through wrapper?
5. Is transport absent below the adapter boundary?
6. Are persisted DTOs and domain state clearly separated without changing bytes?
7. Does the single commit lane remain the only ordering/atomicity authority?
8. Are applied and durable phases mechanically distinct?
9. Can every retry/cancellation/crash point be reasoned about from persisted progress?
10. Are auth, tenant qualification and metering explicit rather than inherited from ambient context?
11. Is every new task owned and supervised?
12. Do tests prove mechanism entry and exact outcomes, including failure paths?
13. Did file/function complexity fall in substance, not only by line shuffling?
14. Were old paths and temporary flags deleted?
15. Are performance, object-store operation counts and cost gates unchanged or deliberately explained?

---

## 30. Explicit anti-patterns to reject

The following are not acceptable implementations of this work package:

1. **`AppState` with a new name.** A `Services`, `Runtime`, `Dependencies` or `Context` struct containing every subsystem is still a service locator if passed throughout the application.
2. **Trait-per-module architecture.** Testability does not require making every internal call dynamically polymorphic. Prefer concrete handles and narrow external ports.
3. **Mechanical file shards.** `http_part1.rs`, `product_helpers.rs`, `shard_misc.rs` or an include tree that preserves one giant conceptual module does not solve ownership.
4. **Thin pass-through services.** A service that only forwards to `AppState`/`ShardEngine` without owning policy or invariants adds indirection and should be deleted.
5. **Generic workflow/state-machine framework.** Creation, sealing and consumer deletion should use explicit typed transitions, not a new interpreter or universal saga DSL.
6. **Multiple writers for convenience.** Billing, consumers, maintenance or sealing must not gain independent writers when their order/atomicity belongs in the shard lane.
7. **Compatibility flags for the refactor.** There must not be `USE_NEW_COMMITTER`, `NEW_PRODUCT_PATH` or long-lived dual execution.
8. **Storage cleanup mixed with layout migration.** Rename/restructure code while preserving bytes; propose layout 5 separately if needed.
9. **Public DTOs carrying trusted internals.** Do not add more serde-skipped authorization, metering, lease or relay fields to wire query structs.
10. **Boolean protocol modes.** Avoid `is_internal`, `skip_meter`, `fence_only`, `maybe_close` combinations when an enum or capability expresses the legal cases.
11. **Stringly typed identity reconstruction.** Do not turn `StreamRef` into a string and later `expect` it to be valid.
12. **Silent fallback on corrupted state.** Persisted-data conversion errors must be explicit and fail closed.
13. **Tests that sleep to create races.** Use entered/release failpoints and a timeout only as a watchdog.
14. **Copied production algorithms in the oracle.** An oracle that repeats the same state machine cannot detect the same bug.
15. **Detached tasks and log-only failure.** Every task needs an owner and a declared failure policy.
16. **Cosmetic field nesting.** Moving 63 fields into five public bags without changing who owns/mutates them is not decomposition.
17. **Incident-history deletion.** Do not erase hard-won rationale; move it to invariant documentation and leave concise local references.
18. **Line-count gaming.** Generated macro indirection, dense one-line expressions or indiscriminate helper extraction are not improvements.

---

## 31. Recommended first executable work package

The safest first implementation tranche is **PRs 1-4 only**. It creates proof and deterministic seams without moving a correctness-critical protocol.

### Deliverables for the first tranche

1. `docs/architecture/current-state.md` generated or refreshed from a checked-in script.
2. `docs/invariants/catalogue.toml` (or equivalent machine-readable index) connecting invariant IDs, source owners and scenario/test IDs.
3. Exact product/raw HTTP and layout-4 storage golden fixtures.
4. `src/lib.rs` plus a thin binary entry point.
5. `config::{AppConfig, ...}` with exhaustive precedence/default validation.
6. `runtime::{Clock, Entropy, RuntimeIdentity}` with production and deterministic implementations.
7. Stable semantic probes/failpoints needed by the existing tests.
8. Warning-only architecture CI report and recorded performance/cost baseline.

### What this tranche must not do

- no handler rewrite;
- no descriptor schema change;
- no commit-command redesign;
- no new runtime feature flag;
- no broad visibility expansion for tests;
- no deletion of a gate because its old module path became inconvenient.

### Exit review

Proceed to Phase B only when:

- the complete current test/gate inventory has stable invocation paths;
- exact external/storage behavior is captured;
- configuration is immutable and centrally constructed;
- migrated time/entropy/boot identity are instance-scoped;
- the architecture report can show whether later PRs actually reduce coupling and size;
- any inability to run a required gate is documented as an environment/tooling issue rather than treated as a pass.

---

## 32. Final definition of done

The restructuring is complete when the repository tells one coherent architectural story:

- binaries construct immutable configuration and explicit runtime owners;
- HTTP/raw/product/operator code parses and renders, but does not own business protocols;
- application services accept typed, already-admitted commands/queries and return transport-neutral outcomes;
- domain state makes legal lifecycle transitions explicit;
- registry/storage adapters alone own persisted DTOs, keyspace and codecs;
- one typed shard command lane owns ordered/atomic physical mutation;
- batch preparation, successful applied publication and durable effect dispatch are visibly separate phases;
- background systems reconcile snapshots through pure plans and idempotent executors under supervised tasks;
- deterministic tests and the simulator drive the same production protocols through explicit runtime ports;
- field, conformance, capacity and cost campaigns remain the external proof;
- files and functions are small because responsibilities are cohesive, not because code was mechanically scattered;
- old aggregates, duplicate validators, panic reconstruction, ambient configuration and temporary adapters are gone.

The expected result is not merely a tidier implementation. It is a code base in which the most important correctness properties are represented by ownership, types and phase boundaries, so a future feature is naturally forced into the correct layer and the number of special-case branches trends down rather than up.
