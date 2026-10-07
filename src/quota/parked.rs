//! A project's share of the live-connection pool its parked waits draw on
//! (shared cells M1). A request parked in a wait (a read long-poll or a
//! consumer pull) takes one place in its project's share; its open
//! subscriptions and watch waits already hold theirs (`live_subs`). The
//! share is the project's live-subscription quota, which a cell shared
//! k ways publishes at most as the pool divided by k (`auth::ceiling`),
//! so k - 1 projects at their shares always leave the k-th its own. A 0
//! quota is no share limit: the instance pool still bounds the waits.
//!
//! The waits stay inside the project's in-flight quota: a parked request
//! keeps the request slot its admission charged.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{ProjectAdmission, QuotaRegistry};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

/// One served request's view of its project's share: the project's
/// tracker entry and the live-subscription quota the request was admitted
/// under.
pub(crate) struct ParkShare {
    admission: Arc<ProjectAdmission>,
    limit: u64,
}

/// RAII place in a project's share, held while one wait is parked.
pub(crate) struct ProjectParked {
    admission: Arc<ProjectAdmission>,
}

impl Drop for ProjectParked {
    fn drop(&mut self) {
        self.admission.parked.fetch_sub(1, Ordering::Relaxed);
    }
}

impl QuotaRegistry {
    /// `project`'s share under the `quotas` its request was admitted with;
    /// `None` for a project the tracker does not hold.
    pub(crate) fn park_share(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
    ) -> Option<ParkShare> {
        self.tracked(project).map(|admission| ParkShare {
            admission,
            limit: quotas.max_live_subscriptions,
        })
    }
}

impl ParkShare {
    /// Take one place for a parked wait: `None` when the project's live
    /// subscriptions, watch waits and parked waits already fill its share.
    pub(crate) fn park(&self) -> Option<ProjectParked> {
        let admission = &self.admission;
        let before = admission.parked.fetch_add(1, Ordering::Relaxed);
        let held = admission
            .live_subs
            .load(Ordering::Relaxed)
            .saturating_add(before);
        if self.limit > 0 && held >= self.limit {
            admission.parked.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(ProjectParked {
            admission: admission.clone(),
        })
    }

    /// The project's parked waits now.
    #[cfg(test)]
    pub(crate) fn parked(&self) -> u64 {
        self.admission.parked.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use crate::project_policy::ProjectQuotas;
    use crate::tenant::ProjectId;

    fn quotas(live: u64) -> ProjectQuotas {
        ProjectQuotas {
            max_live_subscriptions: live,
            ..ProjectQuotas::default()
        }
    }

    /// A project's parked waits, its open subscriptions included, stay
    /// within its live-subscription quota; a released place is reusable,
    /// and a 0 quota sets no share limit.
    #[test]
    fn parked_waits_and_subscriptions_share_the_live_quota() {
        let registry = super::QuotaRegistry::default();
        let project = ProjectId::new("proj-share").unwrap();
        let limits = quotas(3);
        let _request = registry.admit(&project, &limits, 0).unwrap();
        let share = registry.park_share(&project, &limits).expect("tracked");
        let subscription = registry
            .admit_subscription(&project, &limits)
            .unwrap()
            .expect("tracked");
        let a = share.park().expect("1 subscription + 1 parked of 3");
        let _b = share.park().expect("1 + 2 of 3");
        assert!(share.park().is_none(), "the share is full");
        assert_eq!(share.parked(), 2, "a refusal never counts");
        drop(a);
        let _c = share.park().expect("a released place is reusable");
        drop(subscription);
        let _d = share.park().expect("a closed subscription frees its place");
        assert!(share.park().is_none());
        assert_eq!(share.parked(), 3);

        let open = registry.park_share(&project, &quotas(0)).unwrap();
        let held: Vec<_> = (0..64).map(|_| open.park().expect("no limit")).collect();
        assert_eq!(open.parked(), 67);
        drop(held);
        assert_eq!(open.parked(), 3);
    }

    /// The tracker holds no entry for a project that was never admitted,
    /// so it has no share to park against.
    #[test]
    fn an_untracked_project_has_no_share() {
        let registry = super::QuotaRegistry::default();
        let project = ProjectId::new("proj-never").unwrap();
        assert!(registry.park_share(&project, &quotas(1)).is_none());
    }
}
