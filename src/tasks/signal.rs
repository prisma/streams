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
pub(crate) struct TerminationSource {
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
    pub(crate) fn prepare() -> anyhow::Result<Self> {
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
    pub(crate) async fn recv(&mut self) {
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
#[cfg(unix)]
mod tests {
    //! The delivery proof runs in a CHILD process, never in this one.
    //!
    //! Tokio's first registration replaces a signal's default disposition
    //! for the whole process, and the replacement outlives the `Signal`
    //! that asked for it. A test that prepared a source and raised
    //! SIGINT/SIGTERM in the suite's own binary therefore left that
    //! binary permanently deaf to Ctrl-C and to a CI cancellation, by
    //! test order — the signal equivalent of mutating the process
    //! environment. So the parent test here never installs a handler and
    //! never raises anything: it starts this same test binary running one
    //! inert-by-default helper, and signals THAT process.

    use super::TerminationSource;
    use crate::config::{Environment, ProcessEnvironment};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    /// Present only in the environment the parent builds for its child.
    /// Without it the helper returns at once, so an ordinary suite run
    /// never installs a handler.
    const HELPER_MARKER: &str = "STREAMS_TERMINATION_SIGNAL_HELPER";
    const HELPER_TEST: &str = "tasks::signal::tests::termination_signal_helper";
    const READY: &str = "handlers ready";
    const OBSERVED: &str = "signal observed";

    /// Kills and reaps the helper on every exit path. `Child` has no
    /// drop-wait, so a failed assertion would otherwise leave the helper
    /// running and then a zombie for the rest of the suite.
    struct Helper(Child);

    impl Drop for Helper {
        fn drop(&mut self) {
            // Best effort: on the passing path the child has already
            // exited and been reaped, and both calls are no-ops.
            drop(self.0.kill());
            drop(self.0.wait());
        }
    }

    /// Runs one helper, sends it `signal` once its handlers exist, and
    /// returns everything it printed.
    fn deliver_to_helper(signal: libc::c_int) -> String {
        let exe = std::env::current_exe().expect("test binary path");
        // `--test-threads=1`: the child runs ONE filtered test and must
        // not start a worker pool inside an already-parallel suite.
        let child = Command::new(exe)
            .args([HELPER_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env_clear()
            .env(HELPER_MARKER, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the signal helper");
        let mut helper = Helper(child);
        let pid = i32::try_from(helper.0.id()).expect("a child pid fits pid_t");
        // kill(0) and kill(-1) address process GROUPS, this suite's
        // included. A real child is never either.
        assert!(pid > 1, "refusing to signal pid {pid}");

        let mut stdout = BufReader::new(helper.0.stdout.take().expect("piped stdout"));
        let mut transcript = String::new();
        // Matched by substring: under `--test-threads=1` libtest prints
        // `test <name> ... ` with no newline before the test runs, so
        // the helper's first line shares it.
        while !transcript.contains(READY) {
            let read = stdout
                .read_line(&mut transcript)
                .expect("read helper stdout");
            assert!(
                read > 0,
                "the helper exited before its handlers were installed:\n{transcript}{}",
                stderr_of(&mut helper)
            );
        }

        // The handlers exist, so the signal is caught rather than fatal —
        // and it is sent while the helper is provably NOT waiting on its
        // source (it is blocked on the go-line below). A source that
        // registered lazily inside its wait would die here.
        // SAFETY: `pid` is this test's own live, unreaped child (checked
        // above to be a single process, not a group); kill has no memory
        // preconditions.
        let sent = unsafe { libc::kill(pid, signal) };
        assert_eq!(sent, 0, "kill({pid}, {signal})");
        let mut stdin = helper.0.stdin.take().expect("piped stdin");
        stdin.write_all(b"go\n").expect("release the helper");
        drop(stdin);

        stdout
            .read_to_string(&mut transcript)
            .expect("drain helper stdout");
        let status = helper.0.wait().expect("reap the helper");
        assert!(
            status.success(),
            "the helper failed ({status}):\n{transcript}{}",
            stderr_of(&mut helper)
        );
        transcript
    }

    fn stderr_of(helper: &mut Helper) -> String {
        let mut text = String::new();
        if let Some(mut stderr) = helper.0.stderr.take() {
            // Diagnostic only, and tiny (a panic message at most).
            drop(stderr.read_to_string(&mut text));
        }
        text
    }

    /// Preparation installs EVERY supported source, and the wait that
    /// follows is infallible.
    ///
    /// The interrupt source is the point: SIGTERM alone was once
    /// preflighted while Ctrl-C was built inside the wait with its
    /// `Result` discarded, so a failed registration was indistinguishable
    /// from a delivered signal. Each signal gets its own child: an
    /// unregistered one keeps its default disposition and kills the
    /// helper, which is a failed exit status here rather than a hang.
    #[test]
    fn each_termination_signal_reaches_a_prepared_source() {
        for (name, signal) in [("SIGTERM", libc::SIGTERM), ("SIGINT", libc::SIGINT)] {
            let transcript = deliver_to_helper(signal);
            assert!(
                transcript.contains(OBSERVED),
                "{name} was prepared but never observed:\n{transcript}"
            );
            // A filter typo would run zero tests and still exit 0.
            assert!(
                transcript.contains("1 passed"),
                "{name}: the helper test did not run:\n{transcript}"
            );
        }
    }

    /// Subject of `each_termination_signal_reaches_a_prepared_source` —
    /// inert unless the parent set the marker.
    #[tokio::test]
    async fn termination_signal_helper() {
        if ProcessEnvironment.get(HELPER_MARKER).is_none() {
            return;
        }
        let mut source = TerminationSource::prepare().expect("install termination sources");

        // Nothing has been sent: `recv` waits, it does not return on
        // registration bookkeeping.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), source.recv())
                .await
                .is_err(),
            "no signal was sent, so nothing may be reported"
        );

        // Printed strictly AFTER `prepare` returned, which is what lets
        // the parent signal this process safely.
        println!("{READY}");
        let mut go = String::new();
        std::io::stdin()
            .read_line(&mut go)
            .expect("read the parent's go-line");

        // The signal arrived while this task was blocked above, not in
        // `recv`: a prepared source must have retained it.
        tokio::time::timeout(Duration::from_secs(10), source.recv())
            .await
            .expect("the signal preceded the go-line, so it is already pending");
        println!("{OBSERVED}");
    }
}
