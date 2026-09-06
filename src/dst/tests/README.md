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
| Lifecycle and topology | `lifecycle_*`, `fork_*`, `seal_*`, `topology_*`, `product_lifecycle` |
| Consumer delivery and deletion | `consumer_atomicity`, `consumer_delete`, `consumer_generations`, `consumer_product`, `consumer_saga` |
| Watches and live delivery | `watch_observation`, `sse_delivery`, `livefeed_*` |
| Multitenancy and authorization | `security_*`, `quota_enforcement` |
| Accounting and admission | `billing_*`, `admission_*` |
| Runtime ownership and recovery | `runtime_isolation`, `runtime_open_gate`, `runtime_retirement`, `runtime_sweep` |

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
