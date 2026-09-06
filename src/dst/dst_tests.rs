//! Cross-owner integration scenarios, organized by their tested contract.
//!
//! Fixture capabilities are private to this test subtree. Each scenario
//! imports only the helpers it uses. Stable names, body hashes, attributes,
//! scenario IDs and mechanism settings live in docs/refactor/test-inventory.json.
//!
//! Scope remains seeded object-store fault injection; Tokio task scheduling
//! is not claimed to be globally deterministic (see docs/DST.md).

#[path = "tests/seal_cancellation.rs"]
mod seal_cancellation;

#[path = "tests/admission_maintenance.rs"]
mod admission_maintenance;

#[path = "tests/admission_memory.rs"]
mod admission_memory;

#[path = "tests/billing_attribution.rs"]
mod billing_attribution;

#[path = "tests/billing_maintenance.rs"]
mod billing_maintenance;

#[path = "tests/billing_usage.rs"]
mod billing_usage;

#[path = "tests/consumer_atomicity.rs"]
mod consumer_atomicity;

#[path = "tests/consumer_delete.rs"]
mod consumer_delete;

#[path = "tests/consumer_generations.rs"]
mod consumer_generations;

#[path = "tests/consumer_product.rs"]
mod consumer_product;

#[path = "tests/consumer_saga.rs"]
mod consumer_saga;

#[path = "tests/durability_failures.rs"]
mod durability_failures;

#[path = "tests/durability_fences.rs"]
mod durability_fences;

#[path = "tests/fixture_auth.rs"]
mod fixture_auth;

#[path = "tests/fixture_failpoints.rs"]
mod fixture_failpoints;

#[path = "tests/fixture_http.rs"]
mod fixture_http;

#[path = "tests/fixture_livefeed.rs"]
mod fixture_livefeed;

#[path = "tests/fixture_requests.rs"]
mod fixture_requests;

#[path = "tests/fixture_runtime.rs"]
mod fixture_runtime;

#[path = "tests/fixture_storage.rs"]
mod fixture_storage;

#[path = "tests/fork_cleanup.rs"]
mod fork_cleanup;

#[path = "tests/fork_lifecycle.rs"]
mod fork_lifecycle;

#[path = "tests/history_absorption.rs"]
mod history_absorption;

#[path = "tests/history_gather.rs"]
mod history_gather;

#[path = "tests/history_recovery.rs"]
mod history_recovery;

#[path = "tests/lifecycle_creation.rs"]
mod lifecycle_creation;

#[path = "tests/lifecycle_incarnation.rs"]
mod lifecycle_incarnation;

#[path = "tests/livefeed_basics.rs"]
mod livefeed_basics;

#[path = "tests/livefeed_history.rs"]
mod livefeed_history;

#[path = "tests/livefeed_ownership.rs"]
mod livefeed_ownership;

#[path = "tests/livefeed_swap.rs"]
mod livefeed_swap;

#[path = "tests/livefeed_tail.rs"]
mod livefeed_tail;

#[path = "tests/persistence_faults.rs"]
mod persistence_faults;

#[path = "tests/producer_handoff.rs"]
mod producer_handoff;

#[path = "tests/producer_protocol.rs"]
mod producer_protocol;

#[path = "tests/product_lifecycle.rs"]
mod product_lifecycle;

#[path = "tests/quota_enforcement.rs"]
mod quota_enforcement;

#[path = "tests/reads_applied.rs"]
mod reads_applied;

#[path = "tests/reads_history.rs"]
mod reads_history;

#[path = "tests/reads_product.rs"]
mod reads_product;

#[path = "tests/reads_raw.rs"]
mod reads_raw;

#[path = "tests/reads_ring.rs"]
mod reads_ring;

#[path = "tests/runtime_isolation.rs"]
mod runtime_isolation;

#[path = "tests/runtime_open_gate.rs"]
mod runtime_open_gate;

#[path = "tests/runtime_retirement.rs"]
mod runtime_retirement;

#[path = "tests/runtime_sweep.rs"]
mod runtime_sweep;

#[path = "tests/seal_convergence.rs"]
mod seal_convergence;

#[path = "tests/seal_coordination.rs"]
mod seal_coordination;

#[path = "tests/seal_fencing.rs"]
mod seal_fencing;

#[path = "tests/seal_incarnation.rs"]
mod seal_incarnation;

#[path = "tests/seal_recovery.rs"]
mod seal_recovery;

#[path = "tests/security_audit.rs"]
mod security_audit;

#[path = "tests/security_isolation.rs"]
mod security_isolation;

#[path = "tests/security_lineage.rs"]
mod security_lineage;

#[path = "tests/security_modes.rs"]
mod security_modes;

#[path = "tests/security_noninterference.rs"]
mod security_noninterference;

#[path = "tests/security_policy.rs"]
mod security_policy;

#[path = "tests/security_revocation.rs"]
mod security_revocation;

#[path = "tests/security_routes.rs"]
mod security_routes;

#[path = "tests/security_subscription.rs"]
mod security_subscription;

#[path = "tests/security_workload.rs"]
mod security_workload;

#[path = "tests/sse_delivery.rs"]
mod sse_delivery;

#[path = "tests/topology_lifecycle.rs"]
mod topology_lifecycle;

#[path = "tests/topology_scaling.rs"]
mod topology_scaling;

#[path = "tests/watch_observation.rs"]
mod watch_observation;

#[path = "review_security.rs"]
mod review_security;

#[path = "review_readiness.rs"]
mod review_readiness;

#[path = "tests/watch_admission.rs"]
mod watch_admission;

#[path = "tests/append_application.rs"]
mod append_application;
