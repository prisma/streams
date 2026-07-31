//! Pinned Durable Streams protocol baseline (product-surface spec
//! appendix §1). Release notes MUST state these values; the release
//! gate fails while any value is the UNPINNED sentinel.
//!
//! The upstream suite is `@durable-streams/server-conformance-tests`
//! (239 tests as of the 2026-07-08 run; CONFORMANCE.md). The exact
//! versions are resolved and recorded when the final-gate conformance
//! job runs against the finished surface.

/// Upstream protocol repository commit this implementation targets.
pub const DURABLE_STREAMS_PROTOCOL_COMMIT: &str = "UNPINNED-SET-AT-GATE";
/// npm version of @durable-streams/server-conformance-tests the gate runs.
pub const DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION: &str = "UNPINNED-SET-AT-GATE";
/// npm version of the client conformance package the gate runs.
pub const DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION: &str = "UNPINNED-SET-AT-GATE";

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
