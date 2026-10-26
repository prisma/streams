//! Native queue operations into the shard committer (docs/QUEUES-DESIGN.md
//! §5): each operation is one `CommitOp::NativeQueue` the committer runs
//! in its commit group and answers on its own channel. A request's
//! operation is refused when the committer's queue is full; the shard's
//! own maintenance waits for room.
use tokio::sync::{mpsc, oneshot};

use super::{CommitOp, ShardEngine};
use crate::native_queue::{NqOp, NqRefusal, NqResult};

fn moved() -> NqRefusal {
    NqRefusal::Internal("shard fenced/moved; retry".into())
}

impl ShardEngine {
    /// Submit `op` for the queue `hash`, waiting for room in the
    /// committer's queue: the shard's own sweeps and collector, which hold
    /// no request.
    pub(crate) async fn submit_native_queue(&self, hash: [u8; 16], op: NqOp) -> NqResult {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CommitOp::NativeQueue { hash, op, resp: tx })
            .await
            .map_err(|_| moved())?;
        rx.await.map_err(|_| moved())?
    }

    /// Submit a request's `op` for the queue `hash`: refused at once,
    /// `NqRefusal::Busy`, when the committer's queue is full, as an append
    /// meets a full queue (`try_command`), so a request never waits for
    /// room holding its in-flight place and the slots appends need.
    pub(crate) async fn try_submit_native_queue(&self, hash: [u8; 16], op: NqOp) -> NqResult {
        if self.is_closed() {
            return Err(moved());
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(CommitOp::NativeQueue { hash, op, resp: tx })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => NqRefusal::Busy,
                mpsc::error::TrySendError::Closed(_) => moved(),
            })?;
        rx.await.map_err(|_| moved())?
    }

    /// Free places in the committer's queue now.
    #[cfg(test)]
    pub(crate) fn committer_slots_free(&self) -> usize {
        self.tx.capacity()
    }
}
