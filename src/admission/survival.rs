//! The pre-auth survival bound (Round-13; edge change #239): the active
//! requests above which a stream or queue request is refused before
//! authentication. A process this far over its cap is defending its
//! sockets, not answering capacity questions.

/// The survival bound is at most this many times the ordinary cap.
const CAP_MULTIPLIER: i64 = 4;

/// What one request-connection holds resident on HTTP/1.1 under the
/// bounded read buffer (#269: ~44 KB per parked connection).
const CONNECTION_BYTES: u64 = 44 << 10;

/// The share of the RSS shed line that request-connections refused only
/// at the survival bound may hold: two fifths.
const LINE_SHARE: (u64, u64) = (2, 5);

/// The survival bound for an ordinary cap `cap` and an RSS shed line of
/// `shed_mb` MiB: four times the cap, and no more request-connections than
/// two fifths of the line holds at [`CONNECTION_BYTES`] each (4,654 at the
/// 1 GiB profile's 500 MB line, 16,756 at 1.8 GB). A line of 0 (off) leaves
/// four times the cap.
pub(super) fn bound(cap: i64, shed_mb: u64) -> i64 {
    let by_cap = cap.saturating_mul(CAP_MULTIPLIER);
    if shed_mb == 0 {
        return by_cap;
    }
    let (share, of) = LINE_SHARE;
    let by_memory = shed_mb.saturating_mul(1 << 20).saturating_mul(share) / of / CONNECTION_BYTES;
    by_cap.min(i64::try_from(by_memory).unwrap_or(i64::MAX))
}
