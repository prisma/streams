//! Test-only knobs of the feed memory budget (sized, exhausted and released
//! per rig) and the ring charge its reservations are checked against.
#![cfg(test)]
use super::*;

impl FeedMemoryBudget {
    pub(crate) fn new_for_test(max: u64) -> Self {
        Self {
            reserved: AtomicU64::new(0),
            max: AtomicU64::new(max),
            // Unit rigs are single-project: the project backstop
            // equals the ceiling unless a test narrows it.
            project_cap: AtomicU64::new(max),
            by_project: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Test-only: shrink the cell ceiling so exhaustion scenarios stay
    /// cheap (per-rig — every AppState builds its own budget). The
    /// project backstop follows at the default quarter.
    pub(crate) fn set_max_for_test(&self, max: u64) {
        self.max.store(max, Ordering::SeqCst);
        self.project_cap.store(max / 4, Ordering::SeqCst);
    }

    pub(crate) fn project_entries_for_test(&self) -> usize {
        self.by_project.lock().unwrap().len()
    }

    /// Test-only exhaustion: reserve everything that remains, so the
    /// next publication takes the uncached path. Returns the amount to
    /// hand back via `release_for_test`.
    pub(crate) fn exhaust_for_test(&self) -> u64 {
        loop {
            let cur = self.reserved.load(Ordering::SeqCst);
            let take = self.max.load(Ordering::SeqCst).saturating_sub(cur);
            if self
                .reserved
                .compare_exchange(cur, cur + take, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return take;
            }
        }
    }

    pub(crate) fn release_for_test(&self, n: u64) {
        self.reserved.fetch_sub(n, Ordering::SeqCst);
    }
}

impl LiveFeed {
    /// Test-only stall report: the feed's lifecycle, cursor space,
    /// subscribers, driver permit and source, read without blocking. A
    /// lock another thread holds reads as `held`, which is itself the
    /// finding when a stalled test reports it.
    pub(crate) fn describe_for_test(&self) -> String {
        let state = match self.st.try_lock() {
            Ok(st) => format!(
                "{:?} head={} floor={} version={}",
                st.lifecycle, st.head, st.floor, st.version
            ),
            Err(_) => "state=held".to_string(),
        };
        let source = match self.src.try_read() {
            Ok(snapshot) => format!(
                "source gen={} frontier={} closed={} cut_off={:?} spans={:?}",
                snapshot.generation,
                snapshot.source.frontier(),
                snapshot.source.closed(),
                snapshot.source.cut_off(),
                snapshot.source.span_sig()
            ),
            Err(_) => "source=held".to_string(),
        };
        format!(
            "{state} subscribers={} driving={} reads={} {source}",
            self.subscribers.load(Ordering::SeqCst),
            self.driving.load(Ordering::SeqCst),
            self.source_reads.load(Ordering::Relaxed)
        )
    }

    /// Retention tests compare the budget's reservation with the charge the
    /// ring itself holds, the number `Drop` releases, never with a copy of it.
    pub(crate) fn retained(&self) -> usize {
        self.st.lock().unwrap().charge
    }
}
