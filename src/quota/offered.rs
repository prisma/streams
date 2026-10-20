//! The requests each project offers the cell, which fair shedding judges
//! (`admission::fair_shed`): every verified request is counted at project
//! admission, whether its project's quotas admit it or refuse it.

use super::{QuotaGuard, QuotaRefusal, QuotaRegistry};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

impl QuotaRegistry {
    /// `admit`, the request counted in its project's offered load and the
    /// cell's at `now_ms` once its project is tracked (a project the
    /// tracker refused to track is counted nowhere).
    pub(crate) fn admit_counted(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<QuotaGuard, QuotaRefusal> {
        let admitted = self.admit(project, quotas, now_ms);
        if let Some(admission) = self.tracked(project) {
            self.load.count(&admission.load, now_ms);
        }
        admitted
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
}
