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
        let mut mb = crate::fleet::rss_bytes() / 1048576;
        let purge_due = shed_line_mb > 0
            && mb > shed_line_mb
            && last_purge.is_none_or(|t| t.elapsed() >= Duration::from_secs(10));
        if purge_due {
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
            mb = crate::fleet::rss_bytes() / 1048576;
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
