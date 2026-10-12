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
//!
//! The share also carries the project's store budget (public-beta phase B
//! step 14(b), owner question Q1 at its recommended (b')): the registry
//! store work a served request's own task made (`store_timing` weighs it:
//! a GET or a 404 1, a PUT, LIST or COPY 10) is debited from a second
//! bucket of the project's after the request is served, at the request
//! rate it was admitted under, one second's burst, debt allowed. Requests
//! that write the registry are refused while it is in debt
//! (`store_ops_available`); every other route only pays into it. The
//! catalog joins the refused routes once it binds its share (phase B item
//! B6); until then it neither pays nor is refused.

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
    /// The store budget's rate a second: the request rate (Q1 (b'); (b)
    /// would publish its own quota and read it here). 0 sets no budget.
    store_rate: u64,
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
            store_rate: quotas.requests_per_sec,
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

    /// Debit `weight` units of the registry store work one served request
    /// made from the project's store budget, refilled to `now_ms` first.
    /// The request was served, so the debt stands however deep (a cap
    /// would bound it by one request's work instead of the rate); a
    /// poisoned budget is left as it is.
    pub(crate) fn debit_store_ops(&self, weight: u64, now_ms: i64) {
        if self.store_rate == 0 || weight == 0 {
            return;
        }
        if let Ok(mut budget) = self.admission.store_ops.lock() {
            budget.refill(self.store_rate as f64, now_ms);
            budget.level -= weight as f64;
        }
    }

    /// Whether a request that writes the registry may be served at
    /// `now_ms`: refused while the project's store budget is in debt,
    /// with the whole seconds until it is not; a poisoned budget refuses
    /// for one second.
    pub(crate) fn store_ops_available(&self, now_ms: i64) -> Result<(), u64> {
        if self.store_rate == 0 {
            return Ok(());
        }
        let rate = self.store_rate as f64;
        let Ok(mut budget) = self.admission.store_ops.lock() else {
            return Err(1);
        };
        budget.refill(rate, now_ms);
        if budget.level < 0.0 {
            return Err(budget.retry_after(rate, 0.0));
        }
        Ok(())
    }

    /// The project's parked waits now.
    #[cfg(test)]
    pub(crate) fn parked(&self) -> u64 {
        self.admission.parked.load(Ordering::Relaxed)
    }

    /// The project's store budget now, unrefilled.
    #[cfg(test)]
    pub(crate) fn store_ops_level(&self) -> f64 {
        self.admission
            .store_ops
            .lock()
            .map_or(f64::NAN, |budget| budget.level)
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

    fn rated(requests_per_sec: u64) -> ProjectQuotas {
        ProjectQuotas {
            requests_per_sec,
            ..ProjectQuotas::default()
        }
    }

    /// Phase B step 14(b), owner question Q1 (b'): a served request's store
    /// work is paid from its project's store budget, full at its request
    /// rate, into debt. A registry write is refused while the budget is in
    /// debt, for the whole seconds its refill takes, and served again from
    /// zero. A request that made no store work leaves the budget as it is,
    /// and a rate of 0 sets no budget: nothing is debited or refused.
    #[test]
    fn store_work_is_paid_from_the_store_budget_into_debt() {
        let registry = super::QuotaRegistry::default();
        let project = ProjectId::new("proj-store").unwrap();
        let _request = registry.admit(&project, &rated(20), 1_000).unwrap();
        let share = registry.park_share(&project, &rated(20)).unwrap();
        assert_eq!(share.store_ops_level(), 20.0, "one second's burst");
        share.debit_store_ops(11, 1_000);
        assert_eq!(share.store_ops_level(), 9.0);
        share.debit_store_ops(0, 1_500);
        assert_eq!(share.store_ops_level(), 9.0, "no work, no refill");
        share.debit_store_ops(11, 1_000);
        assert_eq!(share.store_ops_level(), -2.0, "a debt stands");
        assert_eq!(share.store_ops_available(1_000), Err(1));
        assert_eq!(share.store_ops_available(1_050), Err(1));
        assert_eq!(share.store_ops_available(1_100), Ok(()), "zero is no debt");
        share.debit_store_ops(100, 1_100);
        assert_eq!(share.store_ops_level(), -100.0, "no cap on the debt");
        assert_eq!(share.store_ops_available(1_100), Err(5));
        assert_eq!(share.store_ops_available(5_000), Err(2));
        assert_eq!(share.store_ops_level(), -22.0);
        assert_eq!(share.store_ops_available(6_100), Ok(()));

        let unrated = registry.park_share(&project, &rated(0)).unwrap();
        unrated.debit_store_ops(100, 6_100);
        assert_eq!(unrated.store_ops_level(), 0.0, "no budget to debit");
        share.debit_store_ops(10, 6_100);
        assert_eq!(unrated.store_ops_available(6_100), Ok(()), "nor to refuse");
        assert_eq!(share.store_ops_available(6_100), Err(1));
    }

    /// A store budget whose lock a panic poisoned refuses a registry write
    /// for one second and takes no debit.
    #[test]
    fn a_poisoned_store_budget_refuses_and_takes_no_debit() {
        let registry = super::QuotaRegistry::default();
        let project = ProjectId::new("proj-poisoned").unwrap();
        let _request = registry.admit(&project, &rated(20), 1_000).unwrap();
        let share = registry.park_share(&project, &rated(20)).unwrap();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = share.admission.store_ops.lock();
            panic!("poison the store budget");
        }));
        assert!(poisoned.is_err() && share.admission.store_ops.is_poisoned());
        share.debit_store_ops(11, 1_000);
        assert_eq!(share.store_ops_available(1_000), Err(1));
        assert!(share.store_ops_level().is_nan(), "still poisoned");
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
