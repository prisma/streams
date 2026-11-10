//! A published operation's intent ends `Reclaimed` without its actor (TLA-045's
//! `ReclaimPublished`; edge #249-#251's amendment). An actor that dies, or whose end fails its
//! tries, between its publish and its terminal CAS leaves the intent live under a stored topology
//! that already names its operation, and no claimant ever runs for it: claims run at a source's
//! open, and a retired source is never opened again. Whoever reads such an intent next (a split's or
//! a merge's start, the heartbeat's intent watch) CASes it `Reclaimed` with the result `published`,
//! as the actor would have.
//!
//! This ends nothing early. The actor's own end follows its publish at once; its only other work
//! after the publish is deleting the split's pause checkpoint on the retired parent, which, like a
//! delete that fails, is left to the checkpoint's lifetime. Every effect of a terminal intent (the
//! lineage sweep of its sources, the heartbeat's reservations and shrink hold, the busy checks) is
//! therefore one the cell already takes at the publish. A published operation never rolls back (its publish follows
//! its `'T'`), so a racing claimant can only roll forward to the same end, and the CAS lets one of
//! them land.

use object_store::ObjectStore;

use super::intent::{self, Held, Intent, IntentError, State};
use super::model::Topology;
use super::{recover, store};

/// Whether `stored` has published the operation of the live `intent`: past the version it started
/// from, `last_op` naming it.
pub(crate) fn published(stored: &Topology, intent: &Intent) -> bool {
    !intent.is_terminal()
        && stored.version > intent.version_seen
        && stored
            .last_op
            .as_ref()
            .is_some_and(|op| op.op_id == intent.op_id)
}

/// The cell's intent (as [`intent::read`]), a live one whose operation the stored topology has
/// published ended `Reclaimed` first (see the module). A topology that cannot be read leaves the
/// intent as read; a CAS that lost to another writer reads the intent again.
pub(crate) async fn read(store: &dyn ObjectStore) -> Result<Option<Held>, IntentError> {
    let held = intent::read(store).await?;
    let Some(live) = held.as_ref().filter(|h| !h.intent.is_terminal()) else {
        return Ok(held);
    };
    let Ok((stored, _)) = store::read(store).await else {
        return Ok(held);
    };
    if !published(&stored, &live.intent) {
        return Ok(held);
    }
    let ended = recover::terminal(live, State::Reclaimed, "published");
    match intent::replace(store, live, ended).await {
        Ok(reclaimed) => Ok(Some(reclaimed)),
        Err(IntentError::Lost) => intent::read(store).await,
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests;
