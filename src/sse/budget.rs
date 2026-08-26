//! LiveFeed retention budgets (round 11.8: relocated from the deleted
//! legacy hub module; the transition-era SSE_HUB_* fallbacks are gone
//! with it — one engine, one vocabulary).

/// Per-feed ring allowance. Env SSE_FEED_RING_BYTES (default 1 MiB —
/// the field-certified posture; see docs/LIVE-FEED.md §Retention
/// policy). An unparseable value warns and takes the default — it
/// must never masquerade as a tuned budget. Release-posture boot
/// validates the whole geometry strictly (round 10e).
pub(crate) fn feed_ring_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| match std::env::var("SSE_FEED_RING_BYTES") {
        Ok(raw) => match raw.trim().parse() {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    "SSE_FEED_RING_BYTES={raw:?} does not parse as a byte count; \
                     using the 1 MiB default"
                );
                1024 * 1024
            }
        },
        Err(_) => 1024 * 1024,
    })
}

/// Process-global shared-mode retention budget. Env
/// SSE_FEED_TOTAL_BYTES (default 16 MiB — the ladder-certified 1-GiB
/// posture). Zero = singleton-only: a second subscriber to the same
/// feed is refused with a typed capacity error. A bounded
/// cache-RETENTION accounting unit, NOT an RSS bound.
pub(crate) fn feed_total_cap() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| match std::env::var("SSE_FEED_TOTAL_BYTES") {
        Ok(raw) => match raw.trim().parse() {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    "SSE_FEED_TOTAL_BYTES={raw:?} does not parse as a byte count; \
                     using the 16 MiB default"
                );
                16 * 1024 * 1024
            }
        },
        Err(_) => 16 * 1024 * 1024,
    })
}
