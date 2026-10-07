# DST contract ownership

The scenarios remain seeded fault-injection tests over the real data plane.
Tokio task scheduling is not globally deterministic. Existing seeds, failpoint
schedules, coverage requirements, assertions and test names were preserved.

| Contract | Modules |
|---|---|
| Persistence and durability | `persistence_faults`, `durability_failures`, `durability_fences` |
| Producer ordering and handoff | `producer_protocol`, `producer_handoff` |
| History absorption and budgets | `history_absorption`, `history_gather`, `history_recovery` |
| Read protocols and visibility | `reads_applied`, `reads_history`, `reads_product`, `reads_raw`, `reads_ring` |
| Registry read classification (a store failure is retryable, corruption is final) | `product_descriptor_reads` |
| JSON record fidelity (stored text, admission, identities, every read surface) | `json_fidelity` |
| Lifecycle and topology | `lifecycle_*`, `fork_*`, `seal_*`, `topology_*`, `product_lifecycle` |
| Cross-instance seal fence (NEXT-WORK §5 F1-a: the owner-side receiver of `POST /v1/internal/seal-fence` answers only after the fence is durable, and refuses wrong credentials, targets, generations and owners without touching the engine; a takeover coordinated on a non-owner relays its fence there, and only the owner's parsed closed-report is a verdict, across wrong ingress, owner movement, competing old finals and crashes around fence durability) | `seal_fence_receiver`, `seal_fence_relay` |
| Automatic scaling loop (NEXT-WORK section 10: `scaler3::start` over a running server evaluates a hot segment on its own cadence and splits it at the load-weighted median in the pass that follows, inside the pass deadline; selected by the `scaler` mutation owner) | `scaler_loop` |
| Consumer delivery and deletion | `consumer_atomicity`, `consumer_delete`, `consumer_generations`, `consumer_product`, `consumer_saga` |
| Parked waits (shared-cells PLAN step 5, M1: a waiting read long-poll, consumer pull or watch wait is parked, not in flight, so it neither sheds writes nor reaches the fleet heartbeat; waits park within the live pool the open subscriptions leave and within their project's live share, the rest wait as active requests; a waiting pull walks again only when a segment's durable tail or the consumer's queue state moved or a lease expires, two walks more per wait and none woken by an idle neighbour, never every 50 ms; A8 in `shared_cell_hostile`) | `parked_waits` |
| Read memory (shared-cells PLAN step 6, H3: a page read, scan or consumer pull holds its rendered page's exact bytes in its project's read bytes and the instance's read memory until its client reads it or leaves; a project whose held pages reach its memory line is refused reads (429 `project_memory_pressure`) while its neighbour reads; a read that finds the instance's read memory full waits for it and is refused after 2 s (503 `read_memory_busy`), and is admitted when a page is released while it waits; waiting long-polls hold no read memory; a parked wait weighs 48 KiB in its project's pressure, model version 2; a woken long-poll or pull takes its reservation again before it renders, so a project's fan-out wake stays inside its line and its neighbour reads, and a woken wait with no room by its deadline ends as a timeout holding its cursor (a long-poll) or empty, leasing nothing (a pull); a consumer pull reserves its walk's coverage at admission, 429 at its project's line (capacity review C1, C2 and F1)) | `read_memory` |
| Shared-cell capacity (the capacity review of 2026-10-07: a write's buffered bodies share their project's memory line, refused 429 `project_memory_pressure` past it (C3); a deleted watched stream's touch journal and flusher end with it (C9)) | `shared_cell_capacity` |
| Watches and live delivery | `watch_observation`, `sse_delivery`, `livefeed_*` (incl. `livefeed_engine_retired`) |
| Multitenancy and authorization (incl. the in-flight admission over the wire: the cap answers only after authentication, and the survival refusal above four times the cap covers reads and appends on both stream surfaces, at the default caps: `security_routes`) | `security_*`, `quota_enforcement`, `quota_read_volume` |
| Shared cells at scale (shared-cells PLAN section 4.1, Layer A: one enforce cell of 128 projects, 1,000 with `MT_CERT_PROJECTS=1000`, reusing every name, key and encryption key; each project's every read, list, consumer, producer, live, watch, fork and usage answer equals its own ledger, quotas refuse only their project, a project whose feed sets no quota still holds exactly bound / k of each shared bound (k = 8; A2b's inflight, stream and live-subscription axes), a policy naming the system project, the deployment's `PROJECT_ID` or its `ACCOUNT_ID` is dropped and counted (A12), books are exact per project and workspace, lifecycle events leave neighbours byte-identical, a cell that wakes with a stale policy, grant or key feed refuses its first request as retryable and that refusal wakes the refresher, so one pass out of cadence serves every project again (step 3, R2), an internal-audience token signed by the customer key is refused on the internal surface while a fleet-pinned kid reads the segment (A10 H6), a capability carrier whose capability does not verify is refused even with the stream key, while that key beside the project's token and its verified capability still observe (A10 M5), expired streams close within one walk circle and per-project state is released; a crowd's parked watch waits, long-polls and pulls never shed the victim's writes (A8, step 5); `shared_cell_hostile` and the ignored lifecycle leg hold what later plan steps turn green, each ignored with its step) | `shared_cell_scale`, `shared_cell_lifecycle`, `shared_cell_hostile` |
| Accounting and admission (incl. closure debts across recreation, crashes and instances: `billing_closure_debts`, `billing_closure_owners`; a close that settles after its months were invoiced: `billing_late_close`; the walk's shard custody: `billing_walk_custody`; the read spool's place under `--path-prefix` and the rollup report's split of the artifact outbox into publishable, blocked-corrupt and total rows: `billing_readiness`) | `billing_*`, `admission_*` |
| Runtime ownership and recovery (incl. a retiring engine's written-but-not-durable group over the wire: the raw `shard_moving` and product `temporarily_unavailable` answers, the successor runtime's single copy, the producer-keyed duplicate retry, and the owed final of a raw append-and-close or a product `:seal` retained until its exact retry completes it: `retiring_written_group`) | `runtime_isolation`, `runtime_open_gate`, `runtime_retirement`, `runtime_sweep`, `retiring_written_group` |
| Fleet coordination (heartbeat liveness, controller progress, withdrawal, ring view, planned drain) | `fleet_controller`, `fleet_desired`, `fleet_drain` |

`oracle_model` is registered under the reference-model owner
(`dst::runtime::tests`). `fault_substrate` is registered under the object-store
fault owner (`dst::fault_store::tests`). The other modules are integration
scenarios under `dst::dst_tests`; the existing trace-store contract tests and
review security/readiness modules retain their registrations.

Shared support is divided by capability: `fixture_storage`, `fixture_runtime`,
`fixture_http`, `fixture_requests`, `fixture_auth`, `fixture_livefeed`,
`fixture_failpoints`, and `fixture_cell` (the many-project shared-cell rig). Fixture visibility is limited to the test subtree with
`pub(super)`, and each module imports its dependencies explicitly. Helpers
used by just one contract remain private in that contract's module.

`docs/refactor/test-inventory.json` records all DST test names, attributes,
body hashes, scenario IDs, and mechanism/configuration references. CI checks
it with `python3 scripts/test-inventory.py --check`; deliberate test changes
must update the manifest in the same reviewed commit. Fixture includes hash
their resolved repository path and content, so moving a test cannot silently
swap its test data. `test-inventory-before.json` and `test-relocations.json`
preserve the extraction evidence and exact fully qualified symbol mapping.
The simultaneous R06 read-capability adaptation is recorded separately with
exact old/new hashes in `test-adaptations.json`; it is not a blanket exception
to the current inventory gate.

The capacity scenario still runs alone, now with the exact selector
`dst::dst_tests::topology_scaling::post_split_throughput_scales`. The full suite
continues to skip it only during parallel execution, and the separate
capacity gate runs it explicitly. Existing conformance/failpoint profiles were
not changed by the extraction.
