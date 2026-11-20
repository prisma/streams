//! A request write its shard's split holds at the barrier (Addendum 25 of
//! GATE-DECISION-meas-full-forward-wedge-2026-11-19). A split's pause parks its parent's
//! committer: every operation queued behind the barrier waits there for as long as the pause
//! takes, and before this a request waiting so stayed active for all of it, so a pause under load
//! shed the cell's next writes to any leaf (split `1`'s 7 on an idle store). Such a request takes
//! a waiting place, as one waiting on a planned move or on an open does (`admission::waiting`,
//! §22 rev 2 K1-K2c): capped at the in-flight cap, held until it is answered, out of the write
//! gate only. The four request writes that reach the committer use it: an append
//! (`application::append::submit`), a create's initial content
//! (`application::creation::initialization`), a native queue operation
//! (`ShardEngine::try_submit_native_queue`) and a segment close (`close_segment_on_engine`, a
//! request only from the internal segment-close route). No read reaches the committer, so none
//! parks here. Outside a served request the place is never taken.

use tokio::sync::oneshot;

use crate::shard::ShardEngine;

/// The committer's answer on `rx` to an operation enqueued on `engine`. If the split's barrier
/// parks the committer before the answer comes, or holds it already, the served request takes a
/// waiting place first.
pub(crate) async fn answered<T>(
    mut rx: oneshot::Receiver<T>,
    engine: &ShardEngine,
) -> Result<T, oneshot::error::RecvError> {
    let gate = &engine.gc_state().gate;
    let mut parks = std::pin::pin!(gate.next_park());
    parks.as_mut().enable();
    if !gate.parked() {
        match futures_util::future::select(&mut rx, parks).await {
            futures_util::future::Either::Left((answer, _)) => return answer,
            futures_util::future::Either::Right(((), _)) => {}
        }
    }
    crate::admission::park::waits_on_owner();
    rx.await
}
