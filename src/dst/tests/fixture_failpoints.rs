//! Fixture failpoints.

/// ONE serialization lock for every test that arms a GLOBAL failpoint
/// registry or reads a global parked-counter (fork_failpoints,
/// crate::failpoints). The registries are keyed per stream name, but
/// the parked COUNTERS are process-global: two parallel tests waiting
/// on "count changed" can wake on each other's parks (the reported
/// solo-pass/parallel-flake family). Serializing the armers makes the
/// suite parallel-green without weakening any assertion.
pub(super) fn gap_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Releases the publish failpoint even if the test panics — a parked
/// resume must never leak into sibling tests.
pub(super) struct FailpointGuard(pub(super) String);
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        crate::failpoints::release_scaler_before_publish(&self.0);
    }
}

// ---- R27-2: cold owned shards with maintenance debt must drain ------

/// The billing sweep and my tests share one process-global marks set
/// and one rotation counter; serialize the sweep-policy tests.
pub(super) fn sweep_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}
