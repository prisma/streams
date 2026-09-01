//! Pinned Durable Streams protocol baseline (product-surface spec
//! appendix §1). Release notes MUST state these values; the release
//! gate fails while any value is the UNPINNED sentinel.
//!
//! The upstream suite is `@durable-streams/server-conformance-tests`
//! (239 tests as of the 2026-07-08 run; CONFORMANCE.md). The exact
//! versions are resolved and recorded when the final-gate conformance
//! job runs against the finished surface.

/// Upstream protocol repository commit this implementation targets.
pub const DURABLE_STREAMS_PROTOCOL_COMMIT: &str =
    "npm:@durable-streams/server-conformance-tests@0.3.6";
/// npm version of @durable-streams/server-conformance-tests the gate runs.
pub const DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION: &str = "0.3.6";
/// npm version of the client conformance package the gate runs.
pub const DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION: &str = "0.2.12";

/// True once every pin carries a real value (release-gate check).
pub fn pinned() -> bool {
    ![
        DURABLE_STREAMS_PROTOCOL_COMMIT,
        DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION,
        DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION,
    ]
    .iter()
    .any(|v| v.contains("UNPINNED"))
}

/// Protocol ceiling on a request body (wire pin). A deployment may
/// LOWER its effective limit (`MAX_REQUEST_BODY_BYTES`, proven by
/// `config::validation`) but never raise it above this.
pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Floor under the effective body ceiling — below this the product
/// surface stops being usable.
pub const MIN_BODY_BYTES: usize = 64 * 1024;
