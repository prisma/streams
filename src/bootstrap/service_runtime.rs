//! The service runtime's whole lifetime: build it, serve on it, and stop it
//! within a bound, so that process exit never waits on a thread that cannot
//! stop.

use std::future::Future;
use std::time::{Duration, Instant};

/// How long teardown waits for the service runtime's worker and blocking
/// threads after the served future has returned or panicked.
///
/// A clean `run` has already joined every supervised task and every shard
/// engine before it returns, so an idle runtime stops in milliseconds. The
/// bound matters only when a thread cannot stop. foyer-memory 0.22.3 is one
/// such case (docs/OPS-RELEASE.md §1). A cache miss that a worker polls while
/// the runtime is closing spawns its fetch into the closed task list. Tokio
/// then drops that fetch on the same thread, while `get_or_fetch` still holds
/// the in-flight lock that the fetch's destructor takes. The thread never
/// returns, and an unbounded runtime drop joins it for ever.
const TEARDOWN_BOUND: Duration = Duration::from_secs(5);

/// Serves `served` on a new multi-thread runtime with `workers` worker threads, then stops
/// the runtime within [`TEARDOWN_BOUND`], also when `served` panicked. A panic
/// resumes only after that teardown. A thread still running at the bound is
/// abandoned, and process exit ends it.
pub(crate) fn serve<T>(workers: usize, served: impl Future<Output = T>) -> std::io::Result<T> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(served)));
    let teardown = Instant::now();
    runtime.shutdown_timeout(TEARDOWN_BOUND);
    if teardown.elapsed() >= TEARDOWN_BOUND {
        tracing::warn!(
            bound = ?TEARDOWN_BOUND,
            "service runtime threads were still running at the teardown bound; \
             the process exits without them"
        );
    }
    match outcome {
        Ok(output) => Ok(output),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[cfg(test)]
mod tests {
    use super::{TEARDOWN_BOUND, serve};
    use crate::config::{Environment, ProcessEnvironment};
    use slatedb::db_cache::DbCache;
    use std::sync::{Arc, mpsc};
    use std::task::Poll;
    use std::time::{Duration, Instant};

    /// Present only in the environment the parent builds for its child.
    /// Without it the helper returns at once. The helper deadlocks a worker
    /// thread on purpose, so an ordinary suite process never runs it.
    const HELPER_MARKER: &str = "STREAMS_SERVICE_RUNTIME_TEARDOWN_HELPER";
    const HELPER_TEST: &str = "bootstrap::service_runtime::tests::held_scan_teardown_helper";
    const RETURNED: &str = "service runtime returned";

    /// Signals when the runtime drops the task that owns it. A multi-thread
    /// runtime closes its task list before dropping idle tasks, so a spawn
    /// that follows this signal lands in the closed list.
    struct ClosingProbe(mpsc::Sender<()>);

    impl Drop for ClosingProbe {
        fn drop(&mut self) {
            self.0.send(()).expect("the closing observer is alive");
        }
    }

    /// What the held scan reports to the helper after teardown.
    struct ScanReports {
        released: mpsc::Sender<()>,
        fetched: mpsc::Sender<()>,
    }

    /// A read that completes on its worker only after the runtime has closed
    /// its task list. It blocks the worker, not only the task, so the poll
    /// that completes it is still running when the close happens.
    fn held_read(
        entered: tokio::sync::oneshot::Sender<()>,
        closed: mpsc::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> {
        let mut entered = Some(entered);
        std::future::poll_fn(move |_| {
            if let Some(entered) = entered.take() {
                entered.send(()).expect("the helper awaits the held read");
            }
            closed
                .recv()
                .expect("the runtime drops the probe when it closes");
            Poll::Ready(())
        })
    }

    /// The part of an absorber scan that the race needs. The scan's poll is
    /// running on a worker when the runtime closes, and the same poll then
    /// takes a cache miss in the production block cache. In the observed
    /// hang, `Absorber::run` had moved on to the next table's index fetch
    /// (`FoyerCache::fetch_index`). Here the poll is held by a read, so the
    /// close overtakes it every time rather than in about one run in two
    /// hundred.
    #[expect(
        clippy::disallowed_methods,
        reason = "held_scan; the probe and the scan must be ordinary tasks in the service runtime's own task list, which is what the runtime's close drops; a supervised task would be joined before the close and never race it"
    )]
    async fn held_scan(reports: ScanReports) {
        let cache = Arc::new(slatedb::db_cache::foyer::FoyerCache::new());
        let (closing, closed) = mpsc::channel();
        let probe = ClosingProbe(closing);
        tokio::spawn(async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        });
        let (entered, reached) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            held_read(entered, closed).await;
            reports.released.send(()).expect("the helper is alive");
            let key = (slatedb::manifest::SsTableId::Wal(1), 0).into();
            let loader: slatedb::db_cache::CacheLoader = Box::new(|| {
                Box::pin(std::future::pending::<
                    Result<slatedb::db_cache::CachedEntry, slatedb::Error>,
                >())
            });
            let missed = cache.fetch_block(key, loader).await;
            assert!(missed.is_err(), "a closed runtime cannot run the fetch");
            reports.fetched.send(()).expect("the helper is alive");
        });
        reached.await.expect("the scan reached its held read");
    }

    /// Subject of `a_scan_on_a_cache_miss_cannot_hold_process_exit`. It is
    /// inert unless the parent set the marker.
    #[test]
    fn held_scan_teardown_helper() {
        if ProcessEnvironment.get(HELPER_MARKER).is_none() {
            return;
        }
        // The parent reads the overrun warning from this process's stderr.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
        let (released, released_rx) = mpsc::channel();
        let (fetched, fetched_rx) = mpsc::channel();
        let started = Instant::now();
        serve(2, held_scan(ScanReports { released, fetched })).expect("build the service runtime");
        let elapsed = started.elapsed();
        println!("{RETURNED} after {elapsed:?}");
        released_rx
            .try_recv()
            .expect("the closing runtime released the held read");
        // The canary for the upstream defect: if the miss returns, foyer no
        // longer deadlocks a closing runtime. Revisit TEARDOWN_BOUND's
        // rationale and the ledger row in docs/OPS-RELEASE.md §1.
        assert!(
            fetched_rx.try_recv().is_err(),
            "the cache miss returned on a closing runtime, so no worker deadlocked"
        );
        assert!(
            elapsed >= TEARDOWN_BOUND,
            "teardown returned in {elapsed:?} although a worker is deadlocked"
        );
    }

    /// A scan inside a foyer cache miss while the runtime closes deadlocks
    /// its worker thread (foyer-memory 0.22.3). The process must still exit.
    /// The child process runs the production teardown and leaves one worker
    /// deadlocked, then exits. An unbounded runtime drop would wait on that
    /// worker for ever, and this parent would kill the child at the deadline.
    #[tokio::test]
    async fn a_scan_on_a_cache_miss_cannot_hold_process_exit() {
        let exe = std::env::current_exe().expect("test binary path");
        let child = tokio::process::Command::new(exe)
            .args([HELPER_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env_clear()
            .env(HELPER_MARKER, "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn the teardown helper");
        let output = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
            .await
            .expect("the helper process did not exit: service runtime teardown hung")
            .expect("reap the teardown helper");
        let transcript = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "the helper failed ({}):\n{transcript}",
            output.status
        );
        assert!(
            transcript.contains(RETURNED),
            "the helper never reached its teardown:\n{transcript}"
        );
        assert!(
            transcript.contains("still running at the teardown bound"),
            "the overrun teardown logged no warning:\n{transcript}"
        );
        assert!(
            transcript.contains("1 passed"),
            "the helper test did not run:\n{transcript}"
        );
    }

    /// With nothing stuck, teardown takes a small fraction of the bound.
    #[test]
    fn an_idle_runtime_stops_without_waiting_for_the_bound() {
        let started = Instant::now();
        assert_eq!(serve(2, async { 7 }).unwrap(), 7);
        assert!(
            started.elapsed() < TEARDOWN_BOUND / 2,
            "idle teardown took {:?}",
            started.elapsed()
        );
    }

    /// A panicking served future still stops the runtime and drops its
    /// tasks. The panic reaches the caller only after that.
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "a_panic_resumes_only_after_the_runtime_is_torn_down; the probe must be an ordinary task in the served runtime, which is what teardown drops; a supervised task would be joined before the panic and prove nothing about teardown"
    )]
    fn a_panic_resumes_only_after_the_runtime_is_torn_down() {
        let (closing, closed) = mpsc::channel();
        let served = async move {
            let probe = ClosingProbe(closing);
            tokio::spawn(async move {
                let _probe = probe;
                std::future::pending::<()>().await;
            });
            panic!("served future failed");
        };
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| serve(2, served)))
            .unwrap_err();
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"served future failed"));
        closed
            .try_recv()
            .expect("the runtime dropped its tasks before the panic resumed");
    }
}
