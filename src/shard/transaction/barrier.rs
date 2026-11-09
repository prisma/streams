//! The split's barrier (S4-DESIGN §5.2): it rides `CommitOp::Seal` as `SealCommit::Barrier`, and
//! takes effect **after the group that contains it**. The committer commits that group, makes it
//! durable and answers it, then answers the barrier ("parked") and parks: operations dequeued
//! after the barrier's group wait in the queue, never offered to a group, until the split releases
//! the barrier.
//! - **The hold** ([`HOLD`], provisional, S4-DESIGN §8): a barrier not released within it sets
//!   the gate's `splitting`, and the committer resumes only to answer every group it gathers
//!   `Splitting` (`503 shard_splitting`, `Retry-After: 1`), the held operations first, until
//!   the release. So nothing waits on a long pause past the hold.
//! - **`Resume`** (a rollback): the queue proceeds in order.
//! - **`Retired`** (the split published, its snapshot installed): every held operation, then
//!   every later one, is rerouted (`commit_and_park`): an append to the child engine this
//!   instance owns for its route, acked with the child's offset; any other is answered
//!   `Splitting`, which the append path re-resolves into the new owner's `409`.
//!
//! Built for tests until the split sends the first barrier (S4 step 11); until then the committer
//! runs exactly `commit_group`.
//! - **A barrier dropped unreleased** (its split died mid-pause) closes the engine: the next open
//!   runs the open-path fence (S4 step 9), which decides from the stored intent and `'T'`
//!   whether the parent serves again. A parked parent never resumes on its own.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future;
use tokio::sync::oneshot;

use crate::shard::seal::SealCommit;
use crate::shard::{CommitOp, ShardEngine};

/// How long a parked committer holds its queue before it answers it `Splitting`.
pub(crate) const HOLD: Duration = Duration::from_secs(2);

/// The barrier as the committer receives it.
pub(crate) struct BarrierReq {
    /// Answered once the barrier's group is durable and answered: the committer is parked.
    parked: oneshot::Sender<()>,
    release: oneshot::Receiver<Release>,
}

/// How a split releases a parked committer.
pub(crate) enum Release {
    /// The split rolled back: the held operations proceed in order.
    Resume,
    /// The split published: the parent is retired (its gate holds the reroute).
    Retired,
}

/// A parked committer, held by its split. Dropped without a release, it closes the engine.
pub(crate) struct Barrier {
    engine: Arc<ShardEngine>,
    release: Option<oneshot::Sender<Release>>,
}

/// Why a barrier did not park its committer.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BarrierError {
    /// The engine closed before its committer parked.
    Closed,
}

impl Barrier {
    /// Release the committer: the held operations proceed in order.
    pub(crate) fn resume(mut self) {
        let Some(release) = self.release.take() else {
            return;
        };
        // Cleared first: a park past its hold sees the release after it set the flag.
        let gate = &self.engine.gc_state().gate;
        gate.set_splitting(false);
        gate.set_parked(false);
        // A committer past its hold, or one that exited (its engine closed), receives none.
        drop(release.send(Release::Resume));
    }
}

impl Barrier {
    /// The split published: from now on each append goes to the child engine `reroute` names
    /// for its route, and every other operation is answered `Splitting` (see the module).
    pub(crate) fn retire(mut self, reroute: crate::topology::gate::Reroute) {
        let Some(release) = self.release.take() else {
            return;
        };
        self.engine.gc_state().gate.set_retired(reroute);
        drop(release.send(Release::Retired));
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        if self.release.take().is_some() {
            tracing::error!(shard = %self.engine.prefix, "a split dropped its barrier unreleased; closing");
            self.engine.begin_close();
        }
    }
}

impl ShardEngine {
    /// Park this engine's committer after the group its barrier joins (see the module).
    pub(crate) async fn barrier(self: &Arc<Self>) -> Result<Barrier, BarrierError> {
        let (parked, parked_rx) = oneshot::channel();
        let (release_tx, release) = oneshot::channel();
        let commit = SealCommit::Barrier(BarrierReq { parked, release });
        self.tx
            .send(CommitOp::Seal(Box::new(commit)))
            .await
            .map_err(|_| BarrierError::Closed)?;
        parked_rx.await.map_err(|_| BarrierError::Closed)?;
        Ok(Barrier {
            engine: self.clone(),
            release: Some(release_tx),
        })
    }
}

/// One commit group, then the park if the group carried a barrier.
pub(super) async fn commit_and_park(engine: &ShardEngine, mut ops: Vec<CommitOp>) {
    let barrier = take_barrier(&mut ops);
    engine.commit_group(ops).await;
    if let Some(req) = barrier {
        park(engine, req).await;
    }
}

/// The group's barrier, taken out of it: a barrier stages nothing. A second one in the same group
/// stays, and `stage_seal` drops it: its split sees `Closed` (one split per engine at a time).
fn take_barrier(ops: &mut Vec<CommitOp>) -> Option<BarrierReq> {
    let at = ops.iter().position(
        |op| matches!(op, CommitOp::Seal(commit) if matches!(**commit, SealCommit::Barrier(_))),
    )?;
    match ops.remove(at) {
        CommitOp::Seal(commit) => match *commit {
            SealCommit::Barrier(req) => Some(req),
            _ => None,
        },
        _ => None,
    }
}

/// Answer the barrier and hold the queue until its split releases it, at most [`HOLD`].
async fn park(engine: &ShardEngine, req: BarrierReq) {
    let BarrierReq {
        parked,
        mut release,
    } = req;
    engine.gc_state().gate.set_parked(true);
    if engine.is_closed() || parked.send(()).is_err() {
        engine.gc_state().gate.set_parked(false);
        // Closed (the split sees `Closed`), or the split gave up before the park: nothing held.
        return;
    }
    let closed = Box::pin(engine.closed());
    // Closed, resumed, or dropped (the handle closed the engine): the park ends.
    let Err(_) = tokio::time::timeout(HOLD, future::select(closed, &mut release)).await else {
        return;
    };
    let gate = &engine.gc_state().gate;
    gate.set_splitting(true);
    if let Ok(Release::Resume) = release.try_recv() {
        // Released as the hold ended: `resume` cleared the flag before this set it.
        gate.set_splitting(false);
    }
}
