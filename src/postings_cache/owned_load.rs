//! The OWNED load's unwind boundary: a panicking load is a failed load.
//!
//! Only the spawned loader (`PostingsCache::spawn_load`) holds a
//! single-flight marker, so only its scan needs a boundary: `finish_load`
//! must run whatever the scan does, or the marker outlives its task, and
//! every later read of the key then spins on the dead channel, loads
//! uncached, and its prefetch stays disabled. A request-task load holds
//! no marker; its task is its boundary and it is not wrapped.

use std::sync::Arc;

use futures_util::FutureExt;
use slatedb::Db;

use super::{PostingsCache, load_runs};
use crate::crypto::{RouteHash, RoutingKeyHash, SegmentHash};
use crate::postings::ValidatedRuns;

#[expect(
    clippy::too_many_arguments,
    reason = "load_owned; the owned load takes the read's resolved parts exactly as the loader received them; a request struct would exist for this single call site"
)]
pub(super) async fn load_owned(
    cache: &Arc<PostingsCache>,
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    kh: RoutingKeyHash,
    start_bucket: u64,
    target_offset: u64,
) -> anyhow::Result<(ValidatedRuns, u64, u64, bool)> {
    let scan = load_runs(cache, part, route, inc, kh, start_bucket, target_offset);
    #[cfg(test)]
    let scan = cache.scripted_panic_before(scan);
    match std::panic::AssertUnwindSafe(scan).catch_unwind().await {
        Ok(loaded) => loaded,
        Err(_payload) => {
            tracing::warn!("postings load panicked; its waiters load directly");
            Err(anyhow::anyhow!("postings load panicked"))
        }
    }
}

#[cfg(test)]
impl PostingsCache {
    /// Test-only panic injection for the OWNED load, armed per cache
    /// instance (a registry failpoint keys by stream name, which a postings
    /// load never sees). One-shot: the arm is consumed before it fires.
    pub(crate) fn panic_next_owned_load(&self) {
        self.panic_next_owned_load
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    async fn scripted_panic_before<T>(&self, scan: impl std::future::Future<Output = T>) -> T {
        let armed = self
            .panic_next_owned_load
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        assert!(!armed, "scripted postings load panic");
        scan.await
    }
}
