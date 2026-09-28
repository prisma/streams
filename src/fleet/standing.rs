//! What this runtime's heartbeat states beyond its own liveness, written by
//! the task that knows it and read by the heartbeat task at every beat
//! (item 40). One per runtime, inside its fleet repository: two runtimes in
//! one process never share it.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub(crate) struct Standing {
    /// The monotonic origin of `progress`. Ages are measured on it, never
    /// on the wall clock, which a restore or a time sync can step.
    origin: Instant,
    /// Milliseconds from `origin` to the fleet tick's last completed pass,
    /// plus one; zero until its loop starts.
    progress: AtomicU64,
}

impl Default for Standing {
    fn default() -> Self {
        Standing {
            origin: Instant::now(),
            progress: AtomicU64::new(0),
        }
    }
}

impl Standing {
    /// The fleet tick completed a pass now (or its loop started). A single
    /// stamp read on its own: no ordering with other state.
    pub(crate) fn mark_progress(&self) {
        self.progress.store(self.elapsed_ms(), Ordering::Relaxed);
    }

    /// How long ago the fleet tick last completed a pass; `None` before its
    /// loop starts.
    pub(crate) fn progress_age_ms(&self) -> Option<u64> {
        let marked = self.progress.load(Ordering::Relaxed);
        (marked > 0).then(|| self.elapsed_ms().saturating_sub(marked))
    }

    /// Milliseconds since `origin`, plus one, so a mark is never zero.
    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }
}

#[cfg(test)]
mod tests {
    use super::Standing;
    use std::sync::atomic::AtomicU64;
    use std::time::{Duration, Instant};

    #[test]
    fn progress_ages_from_its_last_mark_and_is_absent_before_the_first() {
        assert_eq!(Standing::default().progress_age_ms(), None);
        // Marked one second after an origin five seconds ago.
        let standing = Standing {
            origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
            progress: AtomicU64::new(1_001),
        };
        let aged = standing.progress_age_ms().unwrap();
        assert!((4_000..4_500).contains(&aged), "{aged}");
        standing.mark_progress();
        assert!(standing.progress_age_ms().unwrap() < 500);
    }
}
