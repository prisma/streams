//! The requests each project offers the cell, which fair shedding judges
//! (`admission::fair_shed`): every verified request is counted at project
//! admission, whether its project's quotas admit it or refuse it.

use super::{QuotaGuard, QuotaRefusal, QuotaRegistry};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

impl QuotaRegistry {
    /// `admit`, the request counted in its project's offered load and the
    /// cell's at `now_ms` once its project is tracked (a project the
    /// tracker refused to track is counted nowhere but in the tracker's
    /// refusals, which the fleet scales on).
    pub(crate) fn admit_counted(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<QuotaGuard, QuotaRefusal> {
        let admitted = self.admit(project, quotas, now_ms);
        if matches!(admitted, Err(QuotaRefusal::TrackerCapacity)) {
            self.load.note_tracker_refusal();
        }
        if let Some(admission) = self.tracked(project) {
            self.load
                .count(&admission.load, quotas.request_weight, now_ms);
        }
        admitted
    }

    /// Projects the tracker refused to track since boot.
    pub(crate) fn tracker_refusals(&self) -> u64 {
        self.load.tracker_refusals()
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectQuotas, QuotaRegistry};
    use crate::admission::fair_shed::{EPOCH_MS, Verdict};
    use crate::tenant::ProjectId;

    /// A flood its own quota refuses still counts: at one request in
    /// flight, 30 more are refused, and the project is blamed on a cell
    /// shared 8 ways beside a neighbour's 3 requests, which is spared.
    #[test]
    fn a_flood_its_own_quota_refuses_is_counted_and_blamed() {
        let registry = QuotaRegistry::default();
        let flooder = ProjectId::new("flood").unwrap();
        let neighbour = ProjectId::new("calm").unwrap();
        let strict = ProjectQuotas {
            max_inflight_requests: 1,
            ..ProjectQuotas::default()
        };
        let now = 7 * EPOCH_MS;
        let held = registry.admit_counted(&flooder, &strict, now).unwrap();
        let refused = (0..30)
            .filter(|_| registry.admit_counted(&flooder, &strict, now).is_err())
            .count();
        assert_eq!(refused, 30, "its concurrency quota refuses them");
        drop(held);
        for _ in 0..3 {
            drop(registry.admit_counted(&neighbour, &ProjectQuotas::default(), now));
        }
        let verdict = |p: &ProjectId, k: u64| {
            let share = registry.park_share(p, &ProjectQuotas::default()).unwrap();
            share.judge(k, now)
        };
        assert_eq!(verdict(&flooder, 8), Verdict::Blamed, "31 of 34");
        assert_eq!(verdict(&neighbour, 8), Verdict::Spared, "3 of 34");
        assert_eq!(verdict(&flooder, 1), Verdict::Even, "a dedicated cell");
    }

    /// A project the tracker refuses to track is counted in the tracker's
    /// refusals, which the fleet scales on (item 6); a quota refusal of a
    /// tracked project is not one.
    #[test]
    fn a_project_the_full_tracker_refuses_is_a_tracker_refusal() {
        let registry = QuotaRegistry::default();
        let now = 7 * EPOCH_MS;
        let quotas = ProjectQuotas::default();
        let held: Vec<_> = (0..crate::quota::MAX_TRACKED_PROJECTS)
            .map(|n| {
                let project = ProjectId::new(&format!("p{n}")).unwrap();
                registry.admit_counted(&project, &quotas, now).unwrap()
            })
            .collect();
        assert_eq!(registry.tracker_refusals(), 0);
        let late = ProjectId::new("late").unwrap();
        for _ in 0..2 {
            let refused = registry.admit_counted(&late, &quotas, now);
            assert!(matches!(refused, Err(super::QuotaRefusal::TrackerCapacity)));
        }
        assert_eq!(registry.tracker_refusals(), 2);
        let strict = ProjectQuotas {
            max_inflight_requests: 1,
            ..ProjectQuotas::default()
        };
        let first = ProjectId::new("p0").unwrap();
        assert!(registry.admit_counted(&first, &strict, now).is_err());
        assert_eq!(registry.tracker_refusals(), 2, "a quota refusal is not one");
        drop(held);
    }
}
