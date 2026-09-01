//! LiveFeed retention budgets (round 11.8: relocated from the deleted
//! legacy hub module; the transition-era SSE_HUB_* fallbacks are gone
//! with it — one engine, one vocabulary).

/// Per-feed ring allowance. Env SSE_FEED_RING_BYTES (default 1 MiB —
/// the field-certified posture; see docs/LIVE-FEED.md §Retention
/// policy), parsed once by the WP-01 config loader: an unparseable
/// value warns there and takes the default — it must never masquerade
/// as a tuned budget. Release-posture boot validates the whole geometry
/// strictly (round 10e).
pub(crate) fn feed_ring_bytes(cfg: &crate::config::SseConfig) -> usize {
    cfg.feed_ring_bytes
}

/// Process-global shared-mode retention budget. Env
/// SSE_FEED_TOTAL_BYTES (default 16 MiB — the ladder-certified 1-GiB
/// posture; same WP-01 loader parse + warn contract as the ring).
/// Zero = singleton-only: a second subscriber to the same
/// feed is refused with a typed capacity error. A bounded
/// cache-RETENTION accounting unit, NOT an RSS bound.
pub(crate) fn feed_total_cap(cfg: &crate::config::SseConfig) -> u64 {
    cfg.feed_total_bytes
}
