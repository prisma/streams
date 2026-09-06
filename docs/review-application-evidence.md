# Application remediation evidence

## R15 — request body admission

`product_auth_gate` now returns an explicit authentication state. A capability carrier is distinct from a verified principal and never supplies project admission or audit attribution. The HTTP adapter only collects bodies for PUT and POST; OPTIONS, GET, HEAD, DELETE and unsupported methods discard bodies without polling. Existing authenticated mutation body limits and buffered-byte accounting remain in place.

Regression: `r15_bodyless_and_unauthorized_requests_never_poll_the_body` drives the real HTTP entry function with a pending body that records each poll and declares an oversized length. It covers preflight, missing bearer on read/write/watch, malformed and expired/forged capability carriers in both header and query forms. Every result must arrive within two seconds with zero body polls and CORS headers.

Validation is pending the shared Rust 1.98.1 dependency build; the ambient Cargo 1.84.1 cannot parse this repository's edition 2024 manifest and is not used as evidence.

## R16 — watch refusal indistinguishability

Key/query diagnostics now occur only after a capability or encryption key verifies. Missing/corrupt/deleted descriptors and failed verification share the same un-attributed 403 response. Capability project claims remain lookup hints only.

Regression: `r16_unauthorized_watch_shapes_are_independent_of_existence` compares status, all headers and complete structured body across missing/live/initializing/deleted descriptors, three malformed/expired/forged carriers, malformed/short/valid keys and valid/unknown/duplicate/malformed query parameters. All 144 requests also use the R15 never-finishing oversized body sentinel.

Executed both security tests with the Rust 1.98.1 debug test binary: `target/debug/deps/streams_slate-17369d5781ba9293 review_security --nocapture`: 2 passed, 0 failed. Local loopback binding required sandbox escalation; the first sandbox-restricted attempt failed at socket binding before the test request, not in product behavior. Full conformance and production auth-mode security matrix remain part of the final repository gates.

## R08 — conditional registry updates

All existing-descriptor update paths now construct an opaque conditional-update token; a missing or empty ETag is an explicit storage error, including recreation and test-only update paths. No mutation may select `PutMode::Overwrite`. Mutation retries now recognize typed object-store precondition conflicts; invalid data, missing conditional tokens, ordinary I/O failure, and ambiguous completion return immediately. Test-injected conflicts use the same typed precondition error as production.

Regressions: `r08_missing_conditional_token_fails_closed`, `r08_only_precondition_conflicts_are_retried`. The latter proves that changing human text to contain “precondition conflict” does not make an unclassified error retryable. Further application typed-error migration follows with lifecycle ownership.

## R04 — immutable validated descriptors

`PersistedDescriptor` remains the JSON/storage DTO. `TryFrom<PersistedDescriptor> for StreamDesc` is the single serving conversion: StreamDesc exposes immutable fields, a validated fixed-width epoch, and explicit lifecycle variants. Creation, recreation, decode, and registry mutation all validate before caching or writing. Mutations edit an attempt-local DTO and cannot mutate a serving snapshot. Existing DTO golden JSON remains byte-identical, including the synthetic all-fields fixture; invalid lifecycle combinations in that synthetic fixture are now correctly refused for serving.

Topology validation distinguishes absent implicit topology from an empty/invalid explicit map, validates allocators/ranges/references/transition shapes, and accepts sealed terminal coverage and legitimately pruned predecessor IDs. Unknown segment IDs return no route; public/internal callers reject them before engine access. An explicit map cannot fall back to invented implicit segment zero. Split rejects invalid points and exhausted IDs before modifying a parent.

Executed debug tests: registry filter 22 passed (including six SSE registry tests); segmap filter 7 passed; security filter 2 passed after the serving-state conversion. New tests `r04_invalid_descriptors_cannot_reach_storage` prove malformed identity/topology/lifecycle fails before object creation; `r04_valid_transition_and_sealed_predecessor_snapshots` covers pending split, split, merge, and sealed predecessors. Golden descriptor tests identified a defaults fixture that intentionally omits layout_version; it now decodes as the persisted DTO, preserving its original purpose independently from serving validation.
