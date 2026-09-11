//! Fixture adapters for the HTTP surface: scenario entry points that keep
//! `AppState` out of the production application dependencies.
#![cfg(test)]
use super::*;

pub(crate) async fn release_fork_ref_for_test(
    state: &Arc<AppState>,
    source: crate::tenant::TenantStreamRef,
    fork_id: &str,
    epoch: &str,
) -> Result<bool, String> {
    state
        .creation_service()
        .release_fork_ref(source, fork_id, epoch)
        .await
}

pub(crate) async fn fence_segment_for_key(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    epoch: &str,
    key: &str,
    generation: u64,
) -> Result<bool, crate::application::lifecycle::SealError> {
    crate::application::lifecycle::fence_segment_for_key(
        &state.lifecycle_service(),
        sref,
        epoch,
        key,
        generation,
    )
    .await
}

#[expect(
    clippy::let_underscore_must_use,
    reason = "touch_ttl; the fixture touches the TTL of its own descriptor; a failed touch leaves nothing the scenario depends on"
)]
pub(crate) fn touch_ttl(state: &Arc<AppState>, desc: &StreamDesc) {
    let _ = state.creation_service().touch_ttl(desc);
}
impl AppState {
    pub(crate) async fn engine_for_scaler(&self, hash: &[u8; 16]) -> Option<Arc<ShardEngine>> {
        self.shards
            .resolve(hash, crate::shard_directory::Adoption::Internal)
            .await
            .ok()
    }
}
