//! The supervised RSS sampler owns its one-at-a-time allocator purge worker.
use crate::{
    admission::AdmissionController,
    backpressure::Limits,
    shard_directory::ShardDirectory,
    tasks::{Cancellation, TaskResult},
};
use std::time::{Duration, Instant};

pub(super) async fn run(
    admission: AdmissionController,
    shards: ShardDirectory,
    shed_line_mb: u64,
    limits: Limits,
    cancel: Cancellation,
) -> TaskResult {
    let mut last_purge: Option<Instant> = None;
    let mut ticks = 0u64;
    loop {
        let mut mb = mib(crate::fleet::rss_bytes());
        if purge_due(mb, shed_line_mb, last_purge, Instant::now()) {
            #[expect(
                clippy::disallowed_methods,
                reason = "RSS sampler; at most one pointer-free purge is outstanding and its result is joined; allocator work must not block the async executor"
            )]
            let purge = tokio::task::spawn_blocking(|| {
                // SAFETY: mi_collect is thread-safe and takes no pointers. No
                // service state is retained by this finite blocking operation.
                unsafe { libmimalloc_sys::mi_collect(true) };
            });
            if let Err(error) = purge.await {
                return TaskResult::Failed(format!("allocator purge worker: {error}"));
            }
            last_purge = Some(Instant::now());
            mb = mib(crate::fleet::rss_bytes());
        }
        admission.record_rss_mb(mb);
        // Process-wide measurement; admission remains instance-local.
        crate::ops::RSS_PEAK_MB.fetch_max(mb, std::sync::atomic::Ordering::Relaxed);
        if ticks.is_multiple_of(8) {
            let snapshot = crate::backpressure::snapshot(&shards);
            admission.apply_maintenance(&snapshot, &limits);
        }
        ticks = ticks.wrapping_add(1);
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

/// Whole mebibytes in `bytes`, rounded down: the unit of the shed line, of
/// admission's RSS reading and of the RSS peak.
fn mib(bytes: u64) -> u64 {
    bytes / 1048576
}

/// A purge is due at `now` when a shed line is set (non-zero), the sample is
/// strictly above it, and no purge finished in the ten seconds before `now`
/// (`last_purge` is `None` before the first purge).
fn purge_due(mb: u64, shed_line_mb: u64, last_purge: Option<Instant>, now: Instant) -> bool {
    shed_line_mb > 0
        && mb > shed_line_mb
        && last_purge.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    use super::{mib, purge_due};
    use std::time::{Duration, Instant};

    /// Whether a purge is due `since` after the last one finished (`None`:
    /// no purge has run yet).
    fn due(mb: u64, shed_line_mb: u64, since: Option<Duration>) -> bool {
        let last = Instant::now();
        match since {
            None => purge_due(mb, shed_line_mb, None, last),
            Some(since) => purge_due(mb, shed_line_mb, Some(last), last + since),
        }
    }

    #[test]
    fn a_sample_counts_whole_mebibytes_rounded_down() {
        let bytes = [0, 1_048_575, 1_048_576, 3 * 1_048_576 + 1];
        assert_eq!(bytes.map(mib), [0, 0, 1, 3]);
        assert_eq!(mib(u64::MAX), 17_592_186_044_415);
    }

    #[test]
    fn a_shed_line_of_zero_never_makes_a_purge_due() {
        let hour = Duration::from_secs(3_600);
        let since = [None, Some(Duration::from_secs(10)), Some(hour)];
        for mb in [0, 1, u64::MAX] {
            assert_eq!(since.map(|s| due(mb, 0, s)), [false; 3], "{mb} MiB");
        }
    }

    #[test]
    fn a_purge_is_due_only_strictly_above_the_shed_line() {
        let mbs = [0, 1, 2, 999, 1000, 1001, u64::MAX];
        let at_one = [false, false, true, true, true, true, true];
        assert_eq!(mbs.map(|mb| due(mb, 1, None)), at_one);
        let at_thousand = [false, false, false, false, false, true, true];
        assert_eq!(mbs.map(|mb| due(mb, 1000, None)), at_thousand);
    }

    #[test]
    fn a_purge_is_due_again_ten_seconds_after_the_last_and_not_sooner() {
        let ten = Duration::from_secs(10);
        let since = [
            None,
            Some(Duration::ZERO),
            Some(Duration::from_millis(9_999)),
            Some(ten - Duration::from_nanos(1)),
            Some(ten),
            Some(Duration::from_secs(3_600)),
        ];
        let above = [true, false, false, false, true, true];
        assert_eq!(since.map(|s| due(1001, 1000, s)), above);
        assert_eq!(since.map(|s| due(1000, 1000, s)), [false; 6]);
    }
}
