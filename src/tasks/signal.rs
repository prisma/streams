//! The runtime's termination input.
//!
//! One rule shapes this module: a signal source is PREPARED before any
//! supervised task starts, and the task only ever waits on sources that
//! are already installed. Registering a handler inside the task made a
//! registration failure a panic in a child — the runtime then lost its
//! one graceful-shutdown input silently, which is the failure this
//! module exists to make impossible.
//!
//! PR 6.1.2-C: 6.1.1-A preflighted SIGTERM but left Ctrl-C constructed
//! inside the wait, where its `Result` was discarded and its failure was
//! indistinguishable from a delivered signal. Every supported source is
//! now prepared together, fallibly, and no registration `Result` is
//! dropped.

/// Every termination source this platform supports, already installed.
///
/// Construction is the whole point: once a value of this type exists,
/// the sources are registered and waiting on them cannot fail.
pub struct TerminationSource {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    interrupt: tokio::signal::windows::CtrlC,
    /// Platforms with no preparable signal API: termination arrives from
    /// the process supervisor instead. Never fires.
    #[cfg(not(any(unix, windows)))]
    _unsupported: (),
}

impl TerminationSource {
    /// Install every supported termination source, or fail.
    ///
    /// This is a PREFLIGHT: it must run before the first supervised task
    /// is spawned, so that a registration failure returns from startup
    /// with nothing left running behind it.
    pub fn prepare() -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            use anyhow::Context;
            use tokio::signal::unix::{SignalKind, signal};
            // BOTH are fallible and BOTH are installed here. Ctrl-C used
            // to be `tokio::signal::ctrl_c()` created inside the wait,
            // so its registration error was never seen.
            let interrupt = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
            let terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
            Ok(TerminationSource {
                interrupt,
                terminate,
            })
        }
        #[cfg(windows)]
        {
            use anyhow::Context;
            let interrupt = tokio::signal::windows::ctrl_c().context("install Ctrl-C handler")?;
            Ok(TerminationSource { interrupt })
        }
        #[cfg(not(any(unix, windows)))]
        {
            tracing::warn!(
                "no preparable termination signal on this platform: \
                 graceful shutdown must come from the process supervisor"
            );
            Ok(TerminationSource { _unsupported: () })
        }
    }

    /// Wait for the first termination signal. Waits only on sources that
    /// `prepare` already installed, so this cannot fail.
    pub async fn recv(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => {}
                _ = self.terminate.recv() => {}
            }
        }
        #[cfg(windows)]
        {
            self.interrupt.recv().await;
        }
        #[cfg(not(any(unix, windows)))]
        {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TerminationSource;
    use std::time::Duration;

    /// PR 6.1.2-C: preparation installs EVERY supported source, and the
    /// wait afterwards is infallible.
    ///
    /// The interrupt source is the point. 6.1.1-A preflighted SIGTERM
    /// only, and built Ctrl-C inside the wait with its `Result`
    /// discarded — a failed registration there was indistinguishable
    /// from a delivered signal, so the runtime could believe it had been
    /// asked to stop. Both signals are raised here against a source that
    /// was prepared once, which is the contract bootstrap depends on.
    ///
    /// ONE test deliberately, not two: a raised signal is delivered to
    /// EVERY prepared source in the process, so a second signal test
    /// running in parallel would observe this one's raises. Splitting
    /// the quiescent case into its own `#[test]` failed exactly that way
    /// in the full suite while passing in isolation.
    #[cfg(unix)]
    #[tokio::test]
    async fn prepared_sources_answer_both_interrupt_and_terminate() {
        // Installing the handlers REPLACES the default disposition, so
        // raising these signals cannot kill the test process — but only
        // because preparation happens first. That ordering is the thing
        // under test.
        let mut source = TerminationSource::prepare().expect("install termination sources");

        // Nothing has been raised: `recv` waits, it does not return on
        // registration bookkeeping.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), source.recv())
                .await
                .is_err(),
            "no signal was raised, so nothing may be reported"
        );

        for sig in [libc::SIGTERM, libc::SIGINT] {
            // SAFETY: raise() targets this process, whose disposition for
            // both signals is now the installed handler.
            assert_eq!(unsafe { libc::raise(sig) }, 0, "raise {sig}");
            tokio::time::timeout(Duration::from_secs(5), source.recv())
                .await
                .unwrap_or_else(|_| panic!("signal {sig} was prepared but never observed"));
        }
    }
}
