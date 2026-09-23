//! Fixture failpoints.

/// Serializes the tests that share process-global state a stream name
/// cannot key. Two owners remain. `crate::sse::auth::LEASE_TERMINATIONS`
/// is a process-global per-reason counter array, and
/// `termination_reasons_count_exactly_once_per_subscription` asserts an
/// exact `TokenExpired` delta, so every test whose subscription can die
/// at token expiry holds this lock. `post_split_throughput_scales` holds
/// it so any unskipped run (a local cargo test, release-provenance.sh)
/// keeps the other holders off the machine while it measures a capacity
/// ratio; CI skips that test in the parallel suite and runs it alone.
///
/// Failpoints are not a reason: `crate::failpoints` keys arming,
/// arrivals and release by (point, stream name), pinned by
/// `failpoints::a_point_armed_for_one_name_never_reaches_another`, so a
/// failpoint test is isolated by a stream name no concurrently running
/// test uses, and several arm without this lock. The other holders
/// inherited it from state that is gone (the scaler failpoint that parked
/// every resume in the process, then process-wide parked counters); each
/// keeps it until a loop run shows it needs no timing isolation.
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
