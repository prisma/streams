//! The SSE subscription surface (LIVE-FEED transition, Stage 1+).
//! `auth` is the authoritative per-frame lease gate; `wire` owns the
//! SSE framing vocabulary. The LiveFeed engine lands in `feed`.

pub(crate) mod auth;
pub(crate) mod feed;
pub(crate) mod registry;
pub(crate) mod session;
pub(crate) mod source;
pub(crate) mod wire;

/// Process-global LiveFeed retention budget (SSE_FEED_TOTAL_BYTES;
/// zero = zero-retention posture). One instance per process, shared by
/// every feed.
use std::sync::Arc;

pub(crate) fn feed_budget() -> Arc<crate::sse::feed::FeedMemoryBudget> {
    static B: std::sync::OnceLock<Arc<crate::sse::feed::FeedMemoryBudget>> =
        std::sync::OnceLock::new();
    B.get_or_init(|| Arc::new(crate::sse::feed::FeedMemoryBudget::from_env()))
        .clone()
}
