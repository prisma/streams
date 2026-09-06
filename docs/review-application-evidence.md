# Application remediation evidence

## R15 — request body admission

`product_auth_gate` now returns an explicit authentication state. A capability carrier is distinct from a verified principal and never supplies project admission or audit attribution. The HTTP adapter only collects bodies for PUT and POST; OPTIONS, GET, HEAD, DELETE and unsupported methods discard bodies without polling. Existing authenticated mutation body limits and buffered-byte accounting remain in place.

Regression: `r15_bodyless_and_unauthorized_requests_never_poll_the_body` drives the real HTTP entry function with a pending body that records each poll and declares an oversized length. It covers preflight, missing bearer on read/write/watch, malformed and expired/forged capability carriers in both header and query forms. Every result must arrive within two seconds with zero body polls and CORS headers.

Validation is pending the shared Rust 1.98.1 dependency build; the ambient Cargo 1.84.1 cannot parse this repository's edition 2024 manifest and is not used as evidence.

## R16 — watch refusal indistinguishability

Key/query diagnostics now occur only after a capability or encryption key verifies. Missing/corrupt/deleted descriptors and failed verification share the same un-attributed 403 response. Capability project claims remain lookup hints only.

Regression: `r16_unauthorized_watch_shapes_are_independent_of_existence` compares status, all headers and complete structured body across missing/live/initializing/deleted descriptors, three malformed/expired/forged carriers, malformed/short/valid keys and valid/unknown/duplicate/malformed query parameters. All 144 requests also use the R15 never-finishing oversized body sentinel.

Executed both security tests with the Rust 1.98.1 debug test binary: `target/debug/deps/streams_slate-17369d5781ba9293 review_security --nocapture`: 2 passed, 0 failed. Local loopback binding required sandbox escalation; the first sandbox-restricted attempt failed at socket binding before the test request, not in product behavior. Full conformance and production auth-mode security matrix remain part of the final repository gates.
