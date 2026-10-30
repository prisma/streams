//! The runtime every `#[tokio::test]` of this crate runs on, named by the
//! attribute's `crate = "crate::test_rt"`. It differs from tokio's in
//! one place: after the body returns or panics, it stops the runtime within
//! [`TEARDOWN_BOUND`] instead of joining its threads for ever.
//!
//! A runtime thread still running at the bound fails the test: a test that
//! leaves a worker that cannot stop has a defect (foyer-memory 0.22.3's
//! self-deadlock on a closing runtime was one, fixed by the pinned
//! foyer#1345: docs/OPS-RELEASE.md §1). Under tokio's own test runtime the
//! test thread would join that worker for ever and hang the suite; here the
//! test fails with its name, a panic in its body still wins, and the
//! process ends the thread at exit. Production bounds the same teardown
//! with `bootstrap::service_runtime`'s `TEARDOWN_BOUND`.

pub(crate) use tokio::pin;

/// How long teardown waits for the test runtime's threads after the body
/// returned or panicked. An idle runtime stops in milliseconds.
const TEARDOWN_BOUND: std::time::Duration = std::time::Duration::from_secs(2);

/// The builder and runtime `#[tokio::test]` expands to, under the names it
/// expects.
pub(crate) mod runtime {
    /// tokio's builder, building a [`Runtime`] with a bounded teardown.
    pub(crate) struct Builder(tokio::runtime::Builder);

    impl Builder {
        pub(crate) fn new_multi_thread() -> Self {
            Self(tokio::runtime::Builder::new_multi_thread())
        }

        pub(crate) fn new_current_thread() -> Self {
            Self(tokio::runtime::Builder::new_current_thread())
        }

        pub(crate) fn worker_threads(&mut self, threads: usize) -> &mut Self {
            self.0.worker_threads(threads);
            self
        }

        pub(crate) fn start_paused(&mut self, paused: bool) -> &mut Self {
            self.0.start_paused(paused);
            self
        }

        pub(crate) fn enable_all(&mut self) -> &mut Self {
            self.0.enable_all();
            self
        }

        pub(crate) fn build(&mut self) -> std::io::Result<Runtime> {
            self.0.build().map(Runtime)
        }
    }

    /// A test runtime that runs one body and then stops within the bound.
    pub(crate) struct Runtime(tokio::runtime::Runtime);

    impl Runtime {
        /// Runs `body` to completion, then stops the runtime within
        /// [`super::TEARDOWN_BOUND`], also when `body` panicked. A panic
        /// resumes only after that teardown, so the test still fails; a
        /// body that returned fails the test when a thread outlived the
        /// bound.
        pub(crate) fn block_on<F: std::future::Future>(self, body: F) -> F::Output {
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.0.block_on(body)));
            let teardown = std::time::Instant::now();
            self.0.shutdown_timeout(super::TEARDOWN_BOUND);
            let overran = teardown.elapsed() >= super::TEARDOWN_BOUND;
            match outcome {
                Ok(_) if overran => panic!(
                    "test runtime threads were still running at the teardown bound ({:?}): \
                     a thread the test left could not stop",
                    super::TEARDOWN_BOUND
                ),
                Ok(output) => output,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::runtime::Builder;
    use std::sync::mpsc;
    use std::time::Duration;

    /// A blocking task that cannot stop: it waits on a channel whose only
    /// sender it holds itself, like the self-deadlocked foyer worker.
    #[expect(
        clippy::disallowed_methods,
        reason = "park_a_thread_for_ever; the parked thread must be one of the test runtime's own threads, which an unbounded teardown joins for ever; a supervised or owned thread is joined before the teardown and so tests nothing"
    )]
    fn park_a_thread_for_ever() {
        let (sender, parked) = mpsc::channel::<()>();
        drop(tokio::task::spawn_blocking(move || {
            let _own_sender = &sender;
            parked.recv()
        }));
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "teardown_returns_while_a_runtime_thread_cannot_stop; the runtime runs on its own thread, so an unbounded teardown fails this test at its deadline; on the test thread a regressed teardown would hang the suite instead"
    )]
    fn teardown_returns_while_a_runtime_thread_cannot_stop() {
        let (done, finished) = mpsc::channel();
        std::thread::spawn(move || done.send(teardown_fails()).expect("the test waits"));
        assert_eq!(
            finished.recv_timeout(Duration::from_secs(10)),
            Ok(true),
            "a test runtime's teardown must return within its bound, failing the test, while a thread cannot stop"
        );
    }

    /// Whether a body that returned fails its test when a runtime thread
    /// cannot stop.
    fn teardown_fails() -> bool {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime");
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async { park_a_thread_for_ever() })
        }));
        failed.is_err_and(|panic| {
            panic
                .downcast_ref::<String>()
                .is_some_and(|m| m.contains("could not stop"))
        })
    }

    #[tokio::test(crate = "crate::test_rt")]
    #[should_panic(expected = "the body's own assertion")]
    async fn a_failed_assertion_still_fails_the_test_when_a_thread_cannot_stop() {
        park_a_thread_for_ever();
        panic!("the body's own assertion");
    }
}
