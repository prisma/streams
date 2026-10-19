//! A superseded process's last read window. The processes of a deploy or a
//! restart share their instance's read spool, and the newer boot opening it
//! fences the older one's handle ("detected newer DB client"): from then on
//! the older process can spool nothing, and the window its stop owes the
//! ledger died with it. Its terminal round instead writes every batch it
//! still holds as one plain object beside the spool, which no fence refuses,
//! and the spool's holder moves it into the spool at its next sweep.
//!
//! Exactly once: a batch keeps its source boot and sequence, so a second
//! import (an import whose delete was lost) or a batch both spooled and
//! handed off (an ambiguous spool write) is deduplicated by the rollup's
//! per-boot sequence floor. The older process's spooled batches are older
//! than anything it hands off, and an import lands after them in the spool,
//! in sequence order, so the floor never passes a batch before it applies.

use super::ReadBatch;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;

type State = Arc<super::AppState>;

/// Where this instance's handed-off windows live: beside its read spool
/// (an empty prefix or instance adds no path segment).
fn handoff_dir(state: &State) -> Path {
    let prefix = state.config.cli.path_prefix.clone().unwrap_or_default();
    let instance = state.ownership.instance();
    Path::from(format!("{prefix}/telemetry/read-handoff/{instance}").as_str())
}

/// Hands off every read batch this process still holds, the active window
/// sealed: one object named by this boot. Answers how many batches it wrote;
/// on a refused write they stay in memory, as before.
pub(crate) async fn hand_off(state: &State) -> Result<usize, String> {
    let mut drain = state.billing.read_drain_all();
    if drain.batches.is_empty() {
        return Ok(0);
    }
    let boot = &state.runtime.identity.boot_id;
    let path = handoff_dir(state).join(format!("{boot}.json"));
    let body = serde_json::to_vec(&drain.batches).map_err(|e| e.to_string())?;
    let count = drain.batches.len();
    state
        .data_store
        .put(&path, PutPayload::from(body))
        .await
        .map_err(|e| format!("read handoff of {count} batches: {e}"))?;
    drain.accepted();
    Ok(count)
}

/// Moves every handed-off window into this process's read spool, then
/// deletes its object; every outbox sweep runs it. Without an open spool it
/// waits for one. A window that does not decode stays where it is, logged,
/// for an operator; a store fault leaves the rest for the next sweep.
pub(crate) async fn import(state: &State) {
    if let Err(e) = try_import(state).await {
        tracing::warn!("read handoff import deferred to the next sweep: {e}");
    }
}

async fn try_import(state: &State) -> Result<(), String> {
    if !state.billing.read_spool_open() {
        return Ok(());
    }
    let store: &Arc<dyn ObjectStore> = &state.data_store;
    let listed = store.list(Some(&handoff_dir(state)));
    let mut paths: Vec<Path> = futures_util::TryStreamExt::try_collect::<Vec<_>>(listed)
        .await
        .map_err(|e| format!("read handoff list: {e}"))?
        .into_iter()
        .map(|meta| meta.location)
        .collect();
    paths.sort();
    for path in paths {
        let body = store.get(&path).await.map_err(|e| e.to_string())?;
        let body = body.bytes().await.map_err(|e| e.to_string())?;
        let batches: Vec<ReadBatch> = match serde_json::from_slice(&body) {
            Ok(batches) => batches,
            Err(e) => {
                tracing::error!(
                    "read handoff {path} does not decode ({e}); its reads are NOT billed until an operator recovers it"
                );
                continue;
            }
        };
        state.billing.spool_handed_off(&batches).await?;
        store.delete(&path).await.map_err(|e| e.to_string())?;
        tracing::info!("read handoff {path}: {} batches spooled", batches.len());
    }
    Ok(())
}
