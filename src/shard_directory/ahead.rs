//! The directory's ahead door (F5 shape C, GATE-DECISION-f5-pause-log-reads-2026-11-17, addendum
//! "part 2 shape"): a split's child, registered ahead of its publish, starts its open through the
//! gate's existing door with zero patience. The gate mints the incarnation, owns the task, its
//! deadline and its reaper, as for any open; the fenced opener holds the engine until the topology
//! names the child a leaf (`topology::ahead`, `fence::open_held`), so nothing is in the serving map
//! before the install. Only a split calls this door: `open_or_wait` still refuses a prefix that is
//! no leaf, and a prefix that is no leaf and has no registration is still never opened (#251 R3-d).

use std::time::Duration;

impl super::ShardDirectory {
    /// Registers the split's child `prefix` with its log `path` and starts its open, where a request
    /// would open it after the install: never while the instance awaits its fleet view (edge
    /// #263), and with a ring published only when the split carries a birth for this instance
    /// (`born_here`). The registration, which the split holds until its publish, or none.
    pub(crate) async fn open_ahead(
        &self,
        prefix: &str,
        path: String,
        born_here: bool,
    ) -> Option<crate::topology::ahead::Registered> {
        let ownership = &self.inner.ownership;
        if ownership.awaits_view() || (self.ring_published() && !born_here) {
            return None;
        }
        let (owner, child) = (ownership.clone(), prefix.to_string());
        let ours = Box::new(move || owner.foreign_owner(&child).is_none());
        let registered = self.inner.ahead.register(prefix, path, ours);
        drop(
            self.inner
                .gate
                .get_or_open_as(prefix, Duration::ZERO, false)
                .await,
        );
        Some(registered)
    }
}
