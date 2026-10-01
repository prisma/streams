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
| Watches and live delivery | `watch_observation`, `sse_delivery`, `livefeed_*` (incl. `livefeed_engine_retired`) |
| Multitenancy and authorization (incl. the in-flight admission over the wire: the cap answers only after authentication, and the survival refusal above four times the cap covers reads and appends on both stream surfaces, at the default caps: `security_routes`) | `security_*`, `quota_enforcement`, `quota_read_volume` |
| Accounting and admission (incl. closure debts across recreation, crashes and instances: `billing_closure_debts`, `billing_closure_owners`; a close that settles after its months were invoiced: `billing_late_close`; the walk's shard custody: `billing_walk_custody`; the read spool's place under `--path-prefix` and the rollup report's split of the artifact outbox into publishable, blocked-corrupt and total rows: `billing_readiness`) | `billing_*`, `admission_*` |
| Runtime ownership and recovery (incl. a retiring engine's written-but-not-durable group over the wire: the raw `shard_moving` and product `temporarily_unavailable` answers, the successor runtime's single copy, the producer-keyed duplicate retry, and the owed final of a raw append-and-close or a product `:seal` retained until its exact retry completes it: `retiring_written_group`) | `runtime_isolation`, `runtime_open_gate`, `runtime_retirement`, `runtime_sweep`, `retiring_written_group` |
| Fleet coordination (heartbeat liveness, controller progress, withdrawal, ring view, planned drain) | `fleet_controller`, `fleet_desired`, `fleet_drain` |

`oracle_model` is registered under the reference-model owner
(`dst::runtime::tests`). `fault_substrate` is registered under the object-store
fault owner (`dst::fault_store::tests`). The other modules are integration
scenarios under `dst::dst_tests`; the existing trace-store contract tests and
review security/readiness modules retain their registrations.

Shared support is divided by capability: `fixture_storage`, `fixture_runtime`,
`fixture_http`, `fixture_requests`, `fixture_auth`, `fixture_livefeed`, and
`fixture_failpoints`. Fixture visibility is limited to the test subtree with
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
