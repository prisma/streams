//! The admission charge a product append takes before it is enqueued
//! (SR2-4): its body bytes against the project's committer-queue budget
//! (`queued_append_bytes`), held by a guard until the append decides.
//! The project's one entry (`ProjectAdmission::queued_bytes`) carries the
//! count, which the memory-pressure estimate reads unweighted.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{ProjectAdmission, QuotaRefusal, QuotaRegistry};
use crate::project_policy::ProjectQuotas;
use crate::tenant::ProjectId;

/// Releases the queued-byte charge when the append is DECIDED (the
/// handler's await returns, success or failure).
pub(crate) struct QueuedBytesGuard {
    admission: Arc<ProjectAdmission>,
    bytes: u64,
}

impl Drop for QueuedBytesGuard {
    fn drop(&mut self) {
        self.admission
            .queued_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl QuotaRegistry {
    /// SR2-4: charge `bytes` to the project's committer-queue budget
    /// BEFORE the append is enqueued; the guard releases when the
    /// append DECIDES. 0 = not configured.
    pub(crate) fn charge_queued(
        &self,
        project: &ProjectId,
        quotas: &ProjectQuotas,
        bytes: u64,
    ) -> Result<Option<QueuedBytesGuard>, QuotaRefusal> {
        let Some(admission) = self.tracked(project) else {
            return Ok(None);
        };
        // Round-13.3 (field A1): queued bytes are the standing
        // committer-queue memory — charged UNCONDITIONALLY (the noisy
        // project held ~12 MB of ten-second queue the model could not
        // see); the configured ceiling stays the refusal line.
        let new = admission.queued_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if quotas.queued_append_bytes > 0 && new > quotas.queued_append_bytes {
            admission.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return Err(QuotaRefusal::QueuedBytes);
        }
        Ok(Some(QueuedBytesGuard { admission, bytes }))
    }
}
