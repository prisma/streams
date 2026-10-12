//! One physical storage executor per process, with explicit immutable sizing.

use std::sync::OnceLock;

struct StorageExecutor {
    threads: usize,
    runtime: tokio::runtime::Runtime,
}

impl StorageExecutor {
    #[expect(
        clippy::expect_used,
        reason = "StorageExecutor::new; building a multi-thread runtime fails only when the OS refuses threads at boot, before anything can run; a fallible constructor would add a branch boot cannot take"
    )]
    fn new(threads: usize) -> Self {
        Self {
            threads,
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .thread_name("slatedb-rt")
                .enable_all()
                .on_thread_park(super::allocator::collect_if_requested)
                .build()
                .expect("build slatedb runtime"),
        }
    }

    fn require_threads(&self, requested: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.threads == requested,
            "storage executor already owns {} threads; requested {requested}; initialize process infrastructure before opening storage",
            self.threads,
        );
        Ok(())
    }
}

static EXECUTOR: OnceLock<StorageExecutor> = OnceLock::new();

/// Called by the single process bootstrap before any storage opens. A prior
/// direct open may already own the default pool; never silently ignore a
/// conflicting validated configuration in that case.
pub(crate) fn init_slatedb_runtime_threads(threads: usize) -> anyhow::Result<()> {
    anyhow::ensure!(threads > 0, "storage executor requires at least one thread");
    EXECUTOR
        .get_or_init(|| StorageExecutor::new(threads))
        .require_threads(threads)
}

/// Runtime services share the physical executor, while their queues, caches
/// and admission state remain independent. Direct storage fixtures use two
/// threads without changing another service's configuration.
pub(crate) fn slatedb_runtime() -> &'static tokio::runtime::Runtime {
    &EXECUTOR.get_or_init(|| StorageExecutor::new(2)).runtime
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A storage worker that parks after a requested round collects for it.
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "slatedb_rt_workers_collect_after_their_next_park; the probe must run on a storage worker, whose park hook is the subject; no supervisor owns the storage runtime"
    )]
    fn slatedb_rt_workers_collect_after_their_next_park() {
        let executor = StorageExecutor::new(2);
        crate::bootstrap::allocator::request_collect();
        let round = crate::bootstrap::allocator::current_round();
        let probe = crate::bootstrap::allocator::collected_here(round);
        let collected = executor
            .runtime
            .block_on(async { tokio::spawn(probe).await.expect("the probe ran") });
        assert!(collected, "a storage worker never collected");
    }

    #[test]
    fn preexisting_process_executor_cannot_silently_ignore_new_sizing() {
        let existing = StorageExecutor::new(2);
        existing.require_threads(2).unwrap();
        assert!(existing.require_threads(3).is_err());
        assert_eq!(existing.threads, 2);
    }
}
