//! The ceiling a cell puts on every project it serves (shared-cells
//! PLAN step 2, finding H1).
//!
//! Every project on a cell draws from the same instance bounds: the
//! inflight slots that parked requests hold, the SSE connections, the
//! 65,536-entry per-stream maps, and the request and byte envelope the
//! instance was measured to carry. A feed quota above a bound, or a 0
//! ("no project limit"), lets a handful of projects at their quotas
//! exhaust that bound for everyone. On a cell shared `k` ways a
//! project's EFFECTIVE quota on each bounded axis is therefore
//! min(feed value, bound / k), and a 0 (or absent) feed value takes
//! bound / k. An axis without a shared bound keeps the feed value.
//! `k = 1` is a dedicated cell: its one project may take every bound,
//! so no ceiling applies and 0 keeps meaning "no project limit".
//!
//! The ceiling is applied once, where `AuthService` publishes a policy
//! snapshot, so every reader of a published policy (token verification,
//! capability status, the lease) holds the project to the same quotas.

use crate::project_policy::{PolicySnapshot, ProjectQuotas};

/// The bounds every project on one instance shares; 0 means the axis
/// has no shared bound.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SharedBounds {
    /// Requests per second the instance carries (its measured envelope).
    pub(crate) requests_per_sec: u64,
    /// Appended payload bytes per second (the measured envelope).
    pub(crate) append_bytes_per_sec: u64,
    /// Read payload bytes per second (the measured envelope).
    pub(crate) read_bytes_per_sec: u64,
    /// Instance inflight slots (`ADMIT_MAX_INFLIGHT`); parked waits hold them.
    pub(crate) inflight: u64,
    /// Live SSE connections (the effective `SSE_MAX_CONNECTIONS`).
    pub(crate) subscriptions: u64,
    /// Entries of the per-stream maps (descriptor cache, limiter, key cache).
    pub(crate) streams: u64,
}

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct CellCeiling {
    share_k: u64,
    bounds: SharedBounds,
}

impl CellCeiling {
    /// A dedicated cell, as every cell runs until boot installs its own
    /// ceiling: quotas pass unchanged.
    pub(crate) fn dedicated() -> Self {
        Self {
            share_k: 1,
            bounds: SharedBounds::default(),
        }
    }

    /// A cell shared `share_k` ways (`PROJECT_SHARE_K`) over `bounds`.
    #[cfg(test)]
    pub(crate) fn shared(share_k: u64, bounds: SharedBounds) -> Self {
        Self { share_k, bounds }
    }

    /// bound / k on an axis with a shared bound, never below 1; `None` on
    /// a dedicated cell or an axis without a bound.
    fn ceiling(&self, bound: u64) -> Option<u64> {
        (self.share_k > 1 && bound > 0).then(|| (bound / self.share_k).max(1))
    }

    fn cap(&self, feed: u64, bound: u64) -> u64 {
        match self.ceiling(bound) {
            None => feed,
            Some(ceiling) if feed == 0 => ceiling,
            Some(ceiling) => feed.min(ceiling),
        }
    }

    /// A project's effective quotas on this cell. Append records and
    /// queued append bytes have no shared bound of their own.
    pub(crate) fn effective(&self, feed: &ProjectQuotas) -> ProjectQuotas {
        let b = &self.bounds;
        ProjectQuotas {
            requests_per_sec: self.cap(feed.requests_per_sec, b.requests_per_sec),
            append_bytes_per_sec: self.cap(feed.append_bytes_per_sec, b.append_bytes_per_sec),
            append_records_per_sec: feed.append_records_per_sec,
            read_bytes_per_sec: self.cap(feed.read_bytes_per_sec, b.read_bytes_per_sec),
            max_inflight_requests: self.cap(feed.max_inflight_requests, b.inflight),
            max_live_subscriptions: self.cap(feed.max_live_subscriptions, b.subscriptions),
            max_streams: self.cap(feed.max_streams, b.streams),
            queued_append_bytes: feed.queued_append_bytes,
        }
    }

    /// The snapshot this cell publishes: every project at its effective
    /// quotas.
    pub(crate) fn apply(&self, mut snapshot: PolicySnapshot) -> PolicySnapshot {
        for policy in snapshot.projects.values_mut() {
            policy.quotas = self.effective(&policy.quotas);
        }
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::{CellCeiling, SharedBounds};
    use crate::project_policy::ProjectQuotas;

    const BOUNDS: SharedBounds = SharedBounds {
        requests_per_sec: 1_411,
        append_bytes_per_sec: 2_060_000,
        read_bytes_per_sec: 30_000_000,
        inflight: 512,
        subscriptions: 1_200,
        streams: 65_536,
    };

    fn quotas(each: u64) -> ProjectQuotas {
        ProjectQuotas {
            requests_per_sec: each,
            append_bytes_per_sec: each,
            append_records_per_sec: each,
            read_bytes_per_sec: each,
            max_inflight_requests: each,
            max_live_subscriptions: each,
            max_streams: each,
            queued_append_bytes: each,
        }
    }

    /// Every field of `q`, in declaration order.
    fn fields(q: &ProjectQuotas) -> [u64; 8] {
        [
            q.requests_per_sec,
            q.append_bytes_per_sec,
            q.append_records_per_sec,
            q.read_bytes_per_sec,
            q.max_inflight_requests,
            q.max_live_subscriptions,
            q.max_streams,
            q.queued_append_bytes,
        ]
    }

    /// k = 8 over the plan's bounds: a 0 takes bound / 8, a value above
    /// it is cut to it, a value below it stands; the two axes without a
    /// shared bound keep the feed value, 0 included.
    #[test]
    fn a_shared_cell_holds_every_bounded_axis_to_its_share() {
        let cell = CellCeiling::shared(8, BOUNDS);
        let share = [176, 257_500, 0, 3_750_000, 64, 150, 8_192, 0];
        assert_eq!(
            fields(&cell.effective(&quotas(0))),
            share,
            "0 takes the share"
        );
        let huge = fields(&cell.effective(&quotas(u64::MAX)));
        let cut = share.map(|s| if s == 0 { u64::MAX } else { s });
        assert_eq!(huge, cut, "above the share is cut to it");
        assert_eq!(
            fields(&cell.effective(&quotas(5))),
            [5; 8],
            "below it stands"
        );
    }

    /// A bound smaller than k still leaves every project one unit, never
    /// the 0 that would mean "no project limit".
    #[test]
    fn a_share_never_rounds_down_to_unlimited() {
        let tiny = SharedBounds {
            inflight: 3,
            ..SharedBounds::default()
        };
        let cell = CellCeiling::shared(8, tiny);
        assert_eq!(cell.effective(&quotas(0)).max_inflight_requests, 1);
        assert_eq!(
            cell.effective(&quotas(0)).max_streams,
            0,
            "no bound, no ceiling"
        );
    }

    /// k = 1 and the dedicated default keep today's quotas exactly, a 0
    /// included, whatever the bounds.
    #[test]
    fn a_dedicated_cell_keeps_the_feed_quotas() {
        for cell in [CellCeiling::shared(1, BOUNDS), CellCeiling::dedicated()] {
            for each in [0, 5, u64::MAX] {
                assert_eq!(
                    fields(&cell.effective(&quotas(each))),
                    [each; 8],
                    "{cell:?}"
                );
            }
        }
    }
}
