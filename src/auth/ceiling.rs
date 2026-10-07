//! The ceiling a cell puts on every project it serves (shared-cells
//! PLAN step 2, findings H1 and L2).
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
//! No customer project may take an identity the cell reserves: the system
//! project, the deployment's `PROJECT_ID` (the raw surface's tenant, open
//! to fleet credentials) or its `ACCOUNT_ID` (the account every unowned
//! meter event bills to). A policy naming one is dropped before
//! publication, so the cell answers its tokens as for a project it does
//! not serve, and the number the published snapshot dropped is reported
//! (`/v1/debug/auth`, `policies.reservedDropped`).
//!
//! The ceiling is applied once, where `AuthService` publishes a policy
//! snapshot, so every reader of a published policy (token verification,
//! capability status, the lease) holds the project to the same quotas.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::project_policy::{PolicySnapshot, ProjectPolicy, ProjectQuotas};
use crate::tenant::ProjectId;

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
    deployment_project: Option<ProjectId>,
    deployment_account: Option<String>,
    /// Policies the last published snapshot dropped as reserved.
    reserved_dropped: AtomicU64,
}

impl CellCeiling {
    /// A dedicated cell, as every cell runs until boot installs its own
    /// ceiling: quotas pass unchanged, and of the reserved identities only
    /// the system project is known.
    pub(crate) fn dedicated() -> Self {
        Self::shared(1, SharedBounds::default())
    }

    /// A cell shared `share_k` ways (`PROJECT_SHARE_K`) over `bounds`.
    pub(crate) fn shared(share_k: u64, bounds: SharedBounds) -> Self {
        Self {
            share_k,
            bounds,
            deployment_project: None,
            deployment_account: None,
            reserved_dropped: AtomicU64::new(0),
        }
    }

    /// The same cell, also reserving its deployment's `PROJECT_ID` and
    /// `ACCOUNT_ID`.
    #[cfg(test)]
    pub(crate) fn reserving(mut self, deployment: &crate::deployment::DeploymentIdentity) -> Self {
        self.deployment_project = Some(deployment.deployment_tenant().clone());
        self.deployment_account = Some(deployment.account_id().to_string());
        self
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

    /// Whether `policy` takes an identity this cell reserves.
    fn reserves(&self, policy: &ProjectPolicy) -> bool {
        policy.project_id.is_system()
            || self.deployment_project.as_ref() == Some(&policy.project_id)
            || self.deployment_account.as_deref() == Some(policy.workspace_id.as_str())
    }

    /// The snapshot this cell publishes, and how many policies it dropped:
    /// no project that takes a reserved identity, every other one at its
    /// effective quotas.
    pub(crate) fn apply(&self, mut snapshot: PolicySnapshot) -> (PolicySnapshot, u64) {
        let mut dropped = 0;
        snapshot.projects.retain(|project, policy| {
            let reserved = self.reserves(policy);
            if reserved {
                tracing::warn!(%project, workspace = %policy.workspace_id.as_str(),
                    "policy feed names a reserved project or account; dropped");
                dropped += 1;
            }
            !reserved
        });
        for policy in snapshot.projects.values_mut() {
            policy.quotas = self.effective(&policy.quotas);
        }
        (snapshot, dropped)
    }

    /// Record how many policies the snapshot just published dropped.
    pub(crate) fn published(&self, dropped: u64) {
        self.reserved_dropped.store(dropped, Ordering::Relaxed);
    }

    /// Policies the last published snapshot dropped as reserved.
    pub(crate) fn reserved_dropped(&self) -> u64 {
        self.reserved_dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{CellCeiling, SharedBounds};
    use crate::project_policy::{PolicySnapshot, ProjectPolicy, ProjectQuotas, ProjectStatus};
    use crate::tenant::{CellId, ProjectId, WorkspaceId};

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

    fn policy(project: &str, workspace: &str) -> (ProjectId, ProjectPolicy) {
        let project_id = ProjectId::new(project).unwrap();
        let policy = ProjectPolicy {
            project_id: project_id.clone(),
            workspace_id: WorkspaceId::new(workspace).unwrap(),
            cell_id: "cell".into(),
            project_policy_version: 1,
            ownership_version: 1,
            status: ProjectStatus::Active,
            quotas: ProjectQuotas::default(),
        };
        (project_id, policy)
    }

    /// The projects `cell` publishes out of `snapshot`, sorted, and how
    /// many it dropped.
    fn served(cell: &CellCeiling, snapshot: &PolicySnapshot) -> (Vec<String>, u64) {
        let (published, dropped) = cell.apply(snapshot.clone());
        let mut ids: Vec<String> = published.projects.keys().map(|p| p.to_string()).collect();
        ids.sort();
        (ids, dropped)
    }

    /// L2: every cell drops a policy naming the system project; a cell
    /// that knows its deployment also drops one naming its `PROJECT_ID`
    /// and every project in its `ACCOUNT_ID` workspace. Each drop is
    /// counted; the rest publish at their effective quotas.
    #[test]
    fn reserved_identities_are_dropped_and_counted() {
        let deployment = crate::deployment::DeploymentIdentity::new(
            ProjectId::new("proj-deploy").unwrap(),
            "acct-deploy".to_string(),
            CellId::new("cell").unwrap(),
            "test".to_string(),
        );
        let snapshot = PolicySnapshot {
            projects: [
                policy(crate::tenant::SYSTEM_PROJECT, "ws-a"),
                policy("proj-deploy", "ws-a"),
                policy("proj-a", "acct-deploy"),
                policy("proj-b", "ws-a"),
            ]
            .into(),
            fetched_at_unix: 0,
            feed_version: 1,
        };
        let dedicated = served(&CellCeiling::dedicated(), &snapshot);
        let want = (
            vec!["proj-a".into(), "proj-b".into(), "proj-deploy".into()],
            1,
        );
        assert_eq!(dedicated, want, "dedicated: the system project only");
        let shared = CellCeiling::shared(8, BOUNDS).reserving(&deployment);
        assert_eq!(served(&shared, &snapshot), (vec!["proj-b".into()], 3));
        let (published, _) = shared.apply(snapshot);
        let quotas = &published.projects[&ProjectId::new("proj-b").unwrap()].quotas;
        assert_eq!(quotas.max_inflight_requests, 64, "the rest at the share");
    }
}
