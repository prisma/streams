//! TTL renewal has explicit admission/completion; retries keep one epoch/target.
use super::{CreationService, StreamDesc};
use crate::{
    application::request_work::{Action, Key, Kind, Ticket, WorkError},
    registry::{Mutation, Registry},
};
use std::sync::Arc;

pub(crate) struct TtlMutation {
    registry: Arc<Registry>,
    stream: crate::tenant::TenantStreamRef,
    epoch: String,
    ttl: u64,
    target: i64,
}
impl TtlMutation {
    pub(crate) async fn run(self) -> Result<(), WorkError> {
        if self.target <= crate::shard::now_ms() {
            return Err(WorkError::TimedOut);
        }
        self.registry
            .mutate_incarnation(&self.stream, &self.epoch, |current| {
                if current.deleted
                    || current.soft_deleted
                    || current.ttl_secs != Some(self.ttl)
                    || !current
                        .expires_at_ms
                        .is_some_and(|expires| expires < self.target)
                {
                    return Mutation::Decline(());
                }
                let mut next = current.to_persisted();
                next.expires_at_ms = Some(self.target);
                Mutation::Write(next, ())
            })
            .await
            .map_err(|error| WorkError::Storage(error.to_string()))?;
        Ok(())
    }
}
impl Drop for TtlMutation {
    fn drop(&mut self) {
        // Also revalidate after an ambiguous cancelled CAS.
        self.registry.invalidate(&self.stream);
    }
}
impl CreationService {
    pub(crate) fn touch_ttl(self: &Arc<Self>, desc: &StreamDesc) -> Result<Ticket, WorkError> {
        let (Some(ttl), Some(expires)) = (desc.ttl_secs, desc.expires_at_ms) else {
            return Ok(Ticket::complete());
        };
        let now = crate::shard::now_ms();
        let window = (ttl as i64).saturating_mul(1000);
        if desc.deleted
            || desc.soft_deleted
            || expires <= now
            || expires.saturating_sub(now) >= window - window / 4
        {
            return Ok(Ticket::complete());
        }
        let key = Key {
            stream: desc.sref(),
            epoch: desc.stream_epoch.clone(),
            kind: Kind::Ttl,
        };
        let action = Action::Ttl(TtlMutation {
            registry: self.registry.clone(),
            stream: key.stream.clone(),
            epoch: key.epoch.clone(),
            ttl,
            target: now.saturating_add(window),
        });
        self.runtime.request_work.submit(key, action)
    }
    pub(crate) async fn renew_ttl(self: &Arc<Self>, desc: &StreamDesc) -> Result<(), WorkError> {
        self.touch_ttl(desc)?.wait().await
    }
    #[cfg(test)]
    pub(crate) fn pending_ttl_for_tests(&self) -> usize {
        self.runtime.request_work.pending_ttl()
    }
}
