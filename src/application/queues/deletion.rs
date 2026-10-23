//! A deleting queue's cleanup (docs/QUEUES-DESIGN.md §8.2 "Queue delete"):
//! bounded committer steps delete every page and row of the queue, then the
//! queue's stream is deleted through the stream deletion path. It runs as
//! request maintenance (`RequestWork`), so no request owns a task; a step
//! that fails or a job that times out leaves the queue `deleting`, and the
//! next GET or DELETE of the queue submits the cleanup again.
use super::{QueueService, QueueTarget};
use crate::application::request_work::{Action, Key, Kind, WorkError};
use crate::native_queue::{NqOp, NqOut, NqRefusal};

/// Rows one cleanup step deletes.
const STEP_ROWS: usize = 512;

pub(crate) struct QueueDeletion {
    service: QueueService,
    stream: crate::tenant::TenantStreamRef,
    epoch: String,
}

impl QueueDeletion {
    pub(super) fn new(service: QueueService, desc: &crate::registry::StreamDesc) -> Self {
        Self {
            service,
            stream: desc.sref(),
            epoch: desc.stream_epoch.clone(),
        }
    }

    pub(super) fn key(&self) -> Key {
        Key {
            stream: self.stream.clone(),
            epoch: self.epoch.clone(),
            kind: Kind::QueueDelete,
        }
    }

    pub(super) fn into_action(self) -> Action {
        Action::QueueDelete(Box::new(self))
    }

    /// Step until nothing of the queue is left, then delete its stream.
    /// A queue already gone, or a stream of another incarnation, is done.
    pub(crate) async fn run(self) -> Result<(), WorkError> {
        let target = match self.service.target(&self.stream).await {
            Ok(t) if t.desc.stream_epoch == self.epoch => t,
            Ok(_) => return Ok(()),
            Err(f) if f.code == "queue_not_found" => return Ok(()),
            Err(f) => return Err(WorkError::Storage(f.message)),
        };
        self.drain(&target).await?;
        self.service
            .creation
            .delete(self.stream.clone(), &self.service.touch)
            .await
            .map_err(|e| WorkError::Storage(e.message))
    }

    async fn drain(&self, t: &QueueTarget) -> Result<(), WorkError> {
        loop {
            let step = t
                .engine
                .submit_native_queue(
                    t.hash,
                    NqOp::DeleteStep {
                        max_rows: STEP_ROWS,
                    },
                )
                .await;
            match step {
                Ok(NqOut::DeleteStepped { remaining: true }) => {}
                Ok(NqOut::DeleteStepped { remaining: false }) | Err(NqRefusal::NotFound) => {
                    return Ok(());
                }
                Ok(other) => return Err(WorkError::Storage(format!("unexpected {other:?}"))),
                Err(e) => return Err(WorkError::Storage(format!("{e:?}"))),
            }
        }
    }
}
