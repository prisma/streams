//! The project tracker (tuning item 3b): a first-seen project's entry, and
//! the most projects a registry tracks at once, `MAX_TRACKED_PROJECTS` until
//! the instance's `PROJECT_TRACKER_MAX` sizes it at project admission
//! (`product::project_admission`). Every clone of a registry shares it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{MAX_TRACKED_PROJECTS, ProjectAdmission, QuotaRefusal, QuotaRegistry};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

/// A registry's tracker size, shared by its clones.
#[derive(Clone)]
pub(super) struct TrackerCap(Arc<AtomicUsize>);

impl Default for TrackerCap {
    fn default() -> Self {
        Self(Arc::new(AtomicUsize::new(MAX_TRACKED_PROJECTS)))
    }
}

impl QuotaRegistry {
    /// A new entry for first-seen `project` in the tracker `tracked`, its
    /// buckets full at `now_ms`. A full tracker first evicts its idle
    /// entries (review item 5: a tracker that filled once must not refuse
    /// project 1,025 until restart), then refuses to track, never to merge:
    /// an untracked project sharing a bucket with strangers would couple
    /// their fates.
    pub(super) fn track(
        &self,
        tracked: &mut HashMap<ProjectId, Arc<ProjectAdmission>>,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        now_ms: i64,
    ) -> Result<Arc<ProjectAdmission>, QuotaRefusal> {
        if tracked.len() >= self.tracker_cap() {
            tracked.retain(|_, a| a.in_use(now_ms));
            if tracked.len() >= self.tracker_cap() {
                return Err(QuotaRefusal::TrackerCapacity);
            }
        }
        let admission = self.new_admission(quotas, now_ms);
        tracked.insert(project.clone(), admission.clone());
        Ok(admission)
    }

    /// The most projects this registry tracks at once.
    pub(super) fn tracker_cap(&self) -> usize {
        self.tracker.0.load(Ordering::Relaxed)
    }

    /// This registry, its tracker sized to `max` projects (a value past
    /// the address space is the address space). Entries over a smaller size
    /// stay until idle eviction removes them. Written only on a change, so
    /// the admissions that size it on every request share the line read-only.
    pub(crate) fn sized_to(&self, max: u64) -> &Self {
        let max = usize::try_from(max).unwrap_or(usize::MAX);
        if self.tracker_cap() != max {
            self.tracker.0.store(max, Ordering::Relaxed);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_TRACKED_PROJECTS, QuotaRegistry};
    use crate::project_policy::ProjectQuotas;
    use crate::quota::QuotaRefusal;
    use crate::tenant::ProjectId;

    fn name(i: usize) -> ProjectId {
        ProjectId::new(&format!("p-{i}")).unwrap()
    }

    /// A registry tracks the default until sized, its clones share the
    /// size, and a tracker sized to 3 admits three projects and refuses a
    /// fourth while the three are in use.
    #[test]
    fn a_sized_tracker_refuses_the_project_past_its_size() {
        let registry = QuotaRegistry::default();
        assert_eq!(registry.tracker_cap(), MAX_TRACKED_PROJECTS);
        let clone = registry.clone();
        assert_eq!(registry.sized_to(3).tracker_cap(), 3);
        assert_eq!(clone.tracker_cap(), 3);
        let quotas = ProjectQuotas::default();
        let held: Vec<_> = (0..3)
            .map(|i| clone.admit(&name(i), &quotas, 1_000))
            .collect();
        assert!(held.iter().all(Result::is_ok));
        assert!(matches!(
            clone.admit(&name(3), &quotas, 1_000),
            Err(QuotaRefusal::TrackerCapacity)
        ));
        assert_eq!(clone.stats().0, 3);
        assert_eq!(registry.sized_to(4).tracker_cap(), 4);
        assert!(clone.admit(&name(3), &quotas, 1_000).is_ok());
        assert_eq!(clone.stats().0, 4);
    }
}
