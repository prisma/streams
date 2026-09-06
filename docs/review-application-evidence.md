# Application remediation evidence

## R15 — request body admission

`product_auth_gate` now returns an explicit authentication state. A capability carrier is distinct from a verified principal and never supplies project admission or audit attribution. The HTTP adapter only collects bodies for PUT and POST; OPTIONS, GET, HEAD, DELETE and unsupported methods discard bodies without polling. Existing authenticated mutation body limits and buffered-byte accounting remain in place.

Regression: `r15_bodyless_and_unauthorized_requests_never_poll_the_body` drives the real HTTP entry function with a pending body that records each poll and declares an oversized length. It covers preflight, missing bearer on read/write/watch, malformed and expired/forged capability carriers in both header and query forms. Every result must arrive within two seconds with zero body polls and CORS headers.

Validation is pending the shared Rust 1.98.1 dependency build; the ambient Cargo 1.84.1 cannot parse this repository's edition 2024 manifest and is not used as evidence.
