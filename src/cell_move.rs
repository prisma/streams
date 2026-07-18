//! Crash-recoverable, customer-key-free movement of one stream between cells.
//!
//! The global descriptor enters `Preparing` while the source cell remains
//! authoritative. Opening its shard fences the serving writer; a durable
//! per-storage-hash marker then prevents stale descriptor caches from
//! resurrecting the stream after cutover. Raw encrypted shard key ranges and
//! checkpointed encrypted history objects are copied into the target. The
//! descriptor CAS is the sole visibility point.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt};
use serde::Serialize;
use sha2::{Digest, Sha256};
use slatedb::admin::{Admin, AdminBuilder};
use slatedb::config::CheckpointOptions;
use slatedb::manifest::SsTableId;
use slatedb::{Db, WriteBatch};

use crate::registry::{CellMoveState, Registry, StorageHash, StreamDesc, shard_for_hash};

const MAX_MOVE_KEYS: u64 = 100_000_000;
const MAX_MOVE_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MAX_HISTORY_OBJECTS: usize = 100_000;
const MAX_FLEET_SNAPSHOT_BYTES: u64 = 1024 * 1024;
const MAX_FLEET_INSTANCES: usize = 64;
const FLEET_FRESH_MS: i64 = 10_000;

#[derive(Clone)]
pub struct CellStores {
    pub ops: Arc<dyn ObjectStore>,
    pub shard: Arc<dyn ObjectStore>,
    pub data: Arc<dyn ObjectStore>,
    /// Full `FLEET_PREFIX`, not the ordinary cell-local ops prefix.
    pub fleet: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CellMoveReport {
    pub operation_id: String,
    pub customer_id: String,
    pub stream: String,
    pub source_cell: String,
    pub target_cell: String,
    pub source_shard: String,
    pub target_shard: String,
    pub storage_hashes: usize,
    pub shard_keys: u64,
    pub shard_bytes: u64,
    pub history_databases: usize,
    pub history_objects: u64,
    pub source_capable_instances: usize,
    pub target_capable_instances: usize,
    pub already_completed: bool,
}

#[derive(serde::Deserialize)]
struct FleetProtocolSnapshot {
    version: u32,
    generated_at_ms: i64,
    heartbeats: Vec<FleetProtocolHeartbeat>,
}

#[derive(serde::Deserialize)]
struct FleetProtocolHeartbeat {
    instance: String,
    ts_ms: i64,
    #[serde(default)]
    cell_move_protocol: u32,
    draining: bool,
}

async fn verify_move_protocol(store: &Arc<dyn ObjectStore>, cell: &str) -> anyhow::Result<usize> {
    let result = store
        .get(&ObjPath::from("fleet.json"))
        .await
        .with_context(|| format!("load {cell} fleet capability snapshot"))?;
    anyhow::ensure!(
        result.meta.size <= MAX_FLEET_SNAPSHOT_BYTES,
        "{cell} fleet capability snapshot exceeds size bound"
    );
    let raw = result.bytes().await?;
    let snapshot: FleetProtocolSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("decode {cell} fleet capability snapshot"))?;
    let now = chrono::Utc::now().timestamp_millis();
    anyhow::ensure!(
        snapshot.version == 1
            && (1..=MAX_FLEET_INSTANCES).contains(&snapshot.heartbeats.len())
            && now
                .checked_sub(snapshot.generated_at_ms)
                .is_some_and(|age| (0..FLEET_FRESH_MS).contains(&age)),
        "{cell} fleet capability snapshot is malformed, empty, or stale"
    );
    let mut instances = HashSet::with_capacity(snapshot.heartbeats.len());
    for heartbeat in &snapshot.heartbeats {
        anyhow::ensure!(
            !heartbeat.instance.is_empty()
                && heartbeat.instance.len() <= 128
                && heartbeat
                    .instance
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                && instances.insert(heartbeat.instance.as_str())
                && now
                    .checked_sub(heartbeat.ts_ms)
                    .is_some_and(|age| (0..FLEET_FRESH_MS).contains(&age))
                && !heartbeat.draining,
            "{cell} fleet capability membership is malformed, stale, or draining"
        );
        anyhow::ensure!(
            heartbeat.cell_move_protocol >= crate::cell_move_fence::PROTOCOL_VERSION,
            "{cell} instance {} lacks cell-move protocol {} (mixed-version moves are forbidden)",
            heartbeat.instance,
            crate::cell_move_fence::PROTOCOL_VERSION
        );
    }
    Ok(snapshot.heartbeats.len())
}

#[derive(Default)]
struct ShardCopyReport {
    keys: u64,
    bytes: u64,
    absorbed: u64,
}

fn storage_hashes(descriptor: &StreamDesc) -> anyhow::Result<Vec<StorageHash>> {
    if descriptor.is_per_key() {
        anyhow::ensure!(
            (1..=256).contains(&descriptor.segment_count)
                && descriptor.segment_count.is_power_of_two(),
            "per-key cell move has an invalid segment count"
        );
        Ok((0..descriptor.segment_count)
            .map(|ordinal| descriptor.segment_hash(ordinal))
            .collect())
    } else {
        anyhow::ensure!(
            descriptor.ordering.is_none() && descriptor.segment_count == 0,
            "cell move has an unsupported ordering descriptor"
        );
        Ok(vec![descriptor.storage_hash()])
    }
}

fn hash_successor(mut hash: StorageHash) -> Option<StorageHash> {
    for byte in hash.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(hash);
        }
        *byte = 0;
    }
    None
}

fn hash_bounds(hash: &StorageHash) -> (Bound<Bytes>, Bound<Bytes>) {
    (
        Bound::Included(Bytes::copy_from_slice(hash)),
        hash_successor(*hash)
            .map(|next| Bound::Excluded(Bytes::copy_from_slice(&next)))
            .unwrap_or(Bound::Unbounded),
    )
}

fn digest_row(hasher: &mut Sha256, key: &[u8], value: &[u8]) {
    hasher.update((key.len() as u64).to_be_bytes());
    hasher.update(key);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn tail_absorbed(value: &[u8]) -> Option<u64> {
    if value.len() < 44 || !matches!(value[0], 2 | 3) {
        return None;
    }
    let sequence_at = if value[0] == 3 { 42 } else { 41 };
    let sequence_len =
        u16::from_le_bytes(value.get(sequence_at..sequence_at + 2)?.try_into().ok()?) as usize;
    (value.len() == sequence_at + 2 + sequence_len)
        .then(|| u64::from_le_bytes(value[25..33].try_into().expect("checked tail slice")))
}

async fn write_batch(db: &Db, batch: WriteBatch) -> anyhow::Result<()> {
    db.write_with_options(
        batch,
        &slatedb::config::WriteOptions {
            await_durable: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn clear_hash(db: &Db, hash: &StorageHash) -> anyhow::Result<()> {
    let mut iterator = db.scan(hash_bounds(hash)).await?;
    let mut batch = WriteBatch::new();
    let mut pending = 0usize;
    let mut count = 0u64;
    while let Some(row) = iterator.next().await? {
        batch.delete(row.key);
        pending += 1;
        count = count.saturating_add(1);
        anyhow::ensure!(
            count <= MAX_MOVE_KEYS,
            "target stream key range exceeds move bound"
        );
        if pending == 4_096 {
            write_batch(db, batch).await?;
            batch = WriteBatch::new();
            pending = 0;
        }
    }
    if pending > 0 {
        write_batch(db, batch).await?;
    }
    Ok(())
}

async fn copy_hash(
    source: &Db,
    target: &Db,
    hash: &StorageHash,
) -> anyhow::Result<ShardCopyReport> {
    clear_hash(target, hash).await?;
    let source_fence = crate::cell_move_fence::key(hash);
    let mut tail_key = hash.to_vec();
    tail_key.push(b't');
    let mut iterator = source.scan(hash_bounds(hash)).await?;
    let mut batch = WriteBatch::new();
    let mut pending = 0usize;
    let mut report = ShardCopyReport::default();
    let mut source_digest = Sha256::new();
    while let Some(row) = iterator.next().await? {
        if row.key.as_ref() == source_fence.as_slice() {
            continue;
        }
        report.keys = report.keys.saturating_add(1);
        report.bytes = report
            .bytes
            .saturating_add(row.key.len() as u64)
            .saturating_add(row.value.len() as u64);
        anyhow::ensure!(
            report.keys <= MAX_MOVE_KEYS && report.bytes <= MAX_MOVE_BYTES,
            "source stream key range exceeds move bound"
        );
        if row.key.as_ref() == tail_key.as_slice() {
            report.absorbed = tail_absorbed(&row.value)
                .ok_or_else(|| anyhow::anyhow!("source stream tail is corrupt during move"))?;
        }
        digest_row(&mut source_digest, &row.key, &row.value);
        batch.put(row.key, row.value);
        pending += 1;
        if pending == 4_096 {
            write_batch(target, batch).await?;
            batch = WriteBatch::new();
            pending = 0;
        }
    }
    if pending > 0 {
        write_batch(target, batch).await?;
    }

    let mut target_iterator = target.scan(hash_bounds(hash)).await?;
    let mut target_keys = 0u64;
    let mut target_bytes = 0u64;
    let mut target_digest = Sha256::new();
    while let Some(row) = target_iterator.next().await? {
        target_keys = target_keys.saturating_add(1);
        target_bytes = target_bytes
            .saturating_add(row.key.len() as u64)
            .saturating_add(row.value.len() as u64);
        digest_row(&mut target_digest, &row.key, &row.value);
    }
    anyhow::ensure!(
        target_keys == report.keys
            && target_bytes == report.bytes
            && target_digest.finalize() == source_digest.finalize(),
        "target shard key range does not match its fenced source"
    );
    Ok(report)
}

async fn clear_database(store: Arc<dyn ObjectStore>, path: &str) -> anyhow::Result<()> {
    let mut deleted = 0usize;
    for namespace in ["manifest", "wal", "compacted", "compactions"] {
        let prefix = ObjPath::from(format!("{path}/{namespace}"));
        let mut listing = store.list(Some(&prefix));
        while let Some(meta) = listing.try_next().await? {
            deleted += 1;
            anyhow::ensure!(
                deleted <= MAX_HISTORY_OBJECTS.saturating_mul(4),
                "target history database exceeds cleanup bound"
            );
            store.delete(&meta.location).await?;
        }
    }
    Ok(())
}

async fn copy_object(
    source: Arc<dyn ObjectStore>,
    target: Arc<dyn ObjectStore>,
    source_path: &ObjPath,
    target_path: &ObjPath,
    operation_id: &str,
    max_object_bytes: u64,
) -> anyhow::Result<()> {
    let meta = source.head(source_path).await?;
    anyhow::ensure!(
        meta.size > 0 && meta.size <= max_object_bytes,
        "cell move object is outside the configured size bound: {source_path}"
    );
    let source_etag = meta
        .e_tag
        .ok_or_else(|| anyhow::anyhow!("cell move object has no ETag: {source_path}"))?;
    let get = source
        .get_opts(
            source_path,
            GetOptions {
                if_match: Some(source_etag),
                ..Default::default()
            },
        )
        .await?;
    let temp = ObjPath::from(format!(
        "_cell_move_tmp/{operation_id}/{}",
        crate::crypto::hex(&Sha256::digest(target_path.as_ref().as_bytes()))
    ));
    let copied = crate::backup::copy_stream(get.into_stream(), target.clone(), &temp).await;
    let (size, _, _) = match copied {
        Ok(result) => result,
        Err(error) => {
            let _ = target.delete(&temp).await;
            return Err(error);
        }
    };
    if size != meta.size {
        let _ = target.delete(&temp).await;
        anyhow::bail!("cell move object changed size during copy: {source_path}");
    }
    let promoted = target.copy(&temp, target_path).await;
    let cleanup = target.delete(&temp).await;
    promoted?;
    cleanup?;
    Ok(())
}

fn referenced_compacted_paths(
    path: &str,
    manifest: &slatedb::manifest::VersionedManifest,
) -> anyhow::Result<Vec<ObjPath>> {
    anyhow::ensure!(
        manifest.external_dbs().is_empty(),
        "cross-cell history move does not accept external database references"
    );
    let mut objects = HashSet::new();
    for view in manifest
        .l0()
        .iter()
        .chain(manifest.compacted().iter().flat_map(|run| &run.sst_views))
        .chain(
            manifest
                .segments()
                .iter()
                .flat_map(|segment| segment.l0().iter()),
        )
        .chain(
            manifest
                .segments()
                .iter()
                .flat_map(|segment| segment.compacted().iter())
                .flat_map(|run| &run.sst_views),
        )
    {
        let SsTableId::Compacted(id) = view.sst.id else {
            anyhow::bail!("history manifest references a WAL as compacted data");
        };
        objects.insert(ObjPath::from(format!("{path}/compacted/{id}.sst")));
        anyhow::ensure!(
            objects.len() <= MAX_HISTORY_OBJECTS,
            "history manifest exceeds the move object bound"
        );
    }
    let mut objects: Vec<_> = objects.into_iter().collect();
    objects.sort();
    Ok(objects)
}

async fn copy_history_database(
    source: &CellStores,
    target: &CellStores,
    database_path: &str,
    operation_id: &str,
    max_object_bytes: u64,
) -> anyhow::Result<Option<u64>> {
    clear_database(target.data.clone(), database_path).await?;
    let admin = AdminBuilder::new(database_path.to_string(), source.data.clone()).build();
    if admin.read_manifest(None).await?.is_none() {
        return Ok(None);
    }
    let checkpoint = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime: Some(Duration::from_secs(3_600)),
            name: Some(format!("cell-move-{operation_id}")),
            ..Default::default()
        })
        .await?;
    let copied = copy_pinned_history(
        source,
        target,
        database_path,
        operation_id,
        max_object_bytes,
        &admin,
        checkpoint.manifest_id,
    )
    .await;
    let released = admin.delete_checkpoint(checkpoint.id).await;
    match (copied, released) {
        (Ok(objects), Ok(())) => Ok(Some(objects)),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn copy_pinned_history(
    source: &CellStores,
    target: &CellStores,
    database_path: &str,
    operation_id: &str,
    max_object_bytes: u64,
    admin: &Admin,
    manifest_id: u64,
) -> anyhow::Result<u64> {
    let manifest = admin
        .read_manifest(Some(manifest_id))
        .await?
        .ok_or_else(|| anyhow::anyhow!("cell move history checkpoint manifest disappeared"))?;
    anyhow::ensure!(
        manifest
            .replay_after_wal_id()
            .checked_add(1)
            .is_some_and(|first| first == manifest.next_wal_sst_id()),
        "encrypted history checkpoint unexpectedly contains a live WAL"
    );
    let objects = referenced_compacted_paths(database_path, &manifest)?;
    for path in &objects {
        copy_object(
            source.data.clone(),
            target.data.clone(),
            path,
            path,
            operation_id,
            max_object_bytes,
        )
        .await?;
        let baseline = crate::primary_scrub::history_baseline_path(path.as_ref());
        copy_object(
            source.ops.clone(),
            target.ops.clone(),
            &baseline,
            &baseline,
            operation_id,
            16 * 1024,
        )
        .await
        .with_context(|| format!("copy writer-verified baseline for {path}"))?;
    }
    let manifest_path = ObjPath::from(format!(
        "{database_path}/manifest/{manifest_id:020}.manifest"
    ));
    copy_object(
        source.data.clone(),
        target.data.clone(),
        &manifest_path,
        &manifest_path,
        operation_id,
        max_object_bytes,
    )
    .await?;
    anyhow::ensure!(
        AdminBuilder::new(database_path.to_string(), target.data.clone())
            .build()
            .read_manifest(Some(manifest_id))
            .await?
            .is_some(),
        "target history checkpoint manifest is unreadable"
    );
    Ok(objects.len() as u64 + 1)
}

/// Execute or resume one cross-cell stream move. `max_object_bytes` applies
/// to every copied SlateDB history object and should match the primary scrub
/// bound. A completed descriptor is returned idempotently without touching
/// its now-authoritative target data.
#[allow(clippy::too_many_arguments)]
pub async fn move_stream(
    registry: &Registry,
    customer_id: &str,
    stream: &str,
    source_cell: &str,
    target_cell: &str,
    operation_id: &str,
    source: &CellStores,
    target: &CellStores,
    max_object_bytes: u64,
) -> anyhow::Result<CellMoveReport> {
    anyhow::ensure!(
        (1024 * 1024..=1024 * 1024 * 1024).contains(&max_object_bytes),
        "cell move object bound must be between 1 MiB and 1 GiB"
    );
    // A lost-response retry after the placement CAS must stay resolvable even
    // if either fleet is asleep or unavailable. This is the only path allowed
    // to bypass fresh protocol evidence because it performs no physical or
    // registry mutation.
    if let Some(descriptor) = registry.get(customer_id, stream).await?
        && descriptor.cell_move.as_ref().is_some_and(|movement| {
            movement.state == CellMoveState::Completed
                && movement.operation_id == operation_id
                && movement.source_cell == source_cell
                && movement.target_cell == target_cell
        })
    {
        let hashes = storage_hashes(&descriptor)?;
        return Ok(CellMoveReport {
            operation_id: operation_id.to_string(),
            customer_id: customer_id.to_string(),
            stream: stream.to_string(),
            source_cell: source_cell.to_string(),
            target_cell: target_cell.to_string(),
            source_shard: String::new(),
            target_shard: String::new(),
            storage_hashes: hashes.len(),
            shard_keys: 0,
            shard_bytes: 0,
            history_databases: hashes.len(),
            history_objects: 0,
            source_capable_instances: 0,
            target_capable_instances: 0,
            already_completed: true,
        });
    }

    // This is an online protocol, not an operator assertion. Every member in
    // both fresh aggregate views must advertise the fence behavior. An old
    // aggregator drops the field while republishing and therefore also fails
    // closed. Operators must freeze deploy/scale changes for the move window.
    let source_capable_instances = verify_move_protocol(&source.fleet, source_cell).await?;
    let target_capable_instances = verify_move_protocol(&target.fleet, target_cell).await?;

    let descriptor = registry
        .begin_cell_move(customer_id, stream, source_cell, target_cell, operation_id)
        .await
        .context("begin cell move")?;
    let movement = descriptor
        .cell_move
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cell move descriptor lost its transition"))?;
    let hashes = storage_hashes(&descriptor)?;
    anyhow::ensure!(!hashes.is_empty(), "cell move has no storage hashes");
    if movement.state == CellMoveState::Completed {
        return Ok(CellMoveReport {
            operation_id: operation_id.to_string(),
            customer_id: customer_id.to_string(),
            stream: stream.to_string(),
            source_cell: source_cell.to_string(),
            target_cell: target_cell.to_string(),
            source_shard: String::new(),
            target_shard: String::new(),
            storage_hashes: hashes.len(),
            shard_keys: 0,
            shard_bytes: 0,
            history_databases: hashes.len(),
            history_objects: 0,
            source_capable_instances,
            target_capable_instances,
            already_completed: true,
        });
    }
    let source_topology = crate::registry::load_topology(&source.ops).await?;
    let target_topology = crate::registry::load_topology(&target.ops).await?;
    let source_prefix = shard_for_hash(&source_topology.shards, &descriptor.routing_hash());
    let target_prefix = shard_for_hash(&target_topology.shards, &descriptor.routing_hash());
    let source_path = source_topology.db_path(&source_prefix);
    let target_path = target_topology.db_path(&target_prefix);
    let mut report = CellMoveReport {
        operation_id: operation_id.to_string(),
        customer_id: customer_id.to_string(),
        stream: stream.to_string(),
        source_cell: source_cell.to_string(),
        target_cell: target_cell.to_string(),
        source_shard: source_path.clone(),
        target_shard: target_path.clone(),
        storage_hashes: hashes.len(),
        shard_keys: 0,
        shard_bytes: 0,
        history_databases: hashes.len(),
        history_objects: 0,
        source_capable_instances,
        target_capable_instances,
        already_completed: false,
    };

    let source_db = Db::builder(source_path, source.shard.clone())
        .build()
        .await
        .context("fence source shard writer")?;
    let fence =
        crate::cell_move_fence::encode(operation_id, target_cell).map_err(anyhow::Error::msg)?;
    let mut fence_batch = WriteBatch::new();
    for hash in &hashes {
        fence_batch.put(crate::cell_move_fence::key(hash), fence.clone());
    }
    if let Err(error) = write_batch(&source_db, fence_batch).await {
        let _ = source_db.close().await;
        return Err(error.context("install durable source stream fence"));
    }

    let target_db = match Db::builder(target_path, target.shard.clone()).build().await {
        Ok(db) => db,
        Err(error) => {
            let _ = source_db.close().await;
            return Err(error.into());
        }
    };
    let mut absorbed = Vec::with_capacity(hashes.len());
    let shard_copy = async {
        for hash in &hashes {
            let copied = copy_hash(&source_db, &target_db, hash).await?;
            report.shard_keys = report.shard_keys.saturating_add(copied.keys);
            report.shard_bytes = report.shard_bytes.saturating_add(copied.bytes);
            absorbed.push(copied.absorbed);
        }
        anyhow::Ok(())
    }
    .await;
    let target_close = target_db.close().await;
    if let Err(error) = shard_copy {
        let _ = source_db.close().await;
        return Err(error);
    }
    if let Err(error) = target_close {
        let _ = source_db.close().await;
        return Err(error.into());
    }

    for (hash, absorbed) in hashes.iter().zip(absorbed) {
        match copy_history_database(
            source,
            target,
            &crate::registry::history_db_path(hash),
            operation_id,
            max_object_bytes,
        )
        .await
        {
            Ok(Some(objects)) => {
                report.history_objects = report.history_objects.saturating_add(objects)
            }
            Ok(None) => anyhow::ensure!(
                absorbed == 0,
                "source history is absent below an absorbed shard frontier"
            ),
            Err(error) => {
                let _ = source_db.close().await;
                return Err(error);
            }
        }
    }

    // Recheck after the potentially long history copy. If a rolling deploy or
    // scale event introduced an incompatible member, leave Preparing + the
    // durable source fence in place and require an idempotent retry. Rewriting
    // the fence also proves this source DB still owns the latest writer epoch
    // immediately before the descriptor visibility CAS.
    report.source_capable_instances = match verify_move_protocol(&source.fleet, source_cell).await {
        Ok(count) => count,
        Err(error) => {
            let _ = source_db.close().await;
            return Err(error);
        }
    };
    report.target_capable_instances = match verify_move_protocol(&target.fleet, target_cell).await {
        Ok(count) => count,
        Err(error) => {
            let _ = source_db.close().await;
            return Err(error);
        }
    };
    let mut final_fence_batch = WriteBatch::new();
    for hash in &hashes {
        final_fence_batch.put(crate::cell_move_fence::key(hash), fence.clone());
    }
    if let Err(error) = write_batch(&source_db, final_fence_batch).await {
        let _ = source_db.close().await;
        return Err(error.context("revalidate durable source stream fence before cutover"));
    }

    let completion = registry
        .complete_cell_move(customer_id, stream, operation_id)
        .await
        .context("publish target cell placement");
    let source_close = source_db.close().await;
    completion?;
    source_close?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    async fn put_json(store: &Arc<dyn ObjectStore>, path: &str, value: serde_json::Value) {
        store
            .put(
                &ObjPath::from(path),
                PutPayload::from(serde_json::to_vec(&value).unwrap()),
            )
            .await
            .unwrap();
    }

    async fn put_capable_fleet(store: &Arc<dyn ObjectStore>, instance: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        put_json(
            store,
            "fleet.json",
            serde_json::json!({
                "version": 1,
                "generated_at_ms": now,
                "heartbeats": [{
                    "instance": instance,
                    "ts_ms": now,
                    "cell_move_protocol": crate::cell_move_fence::PROTOCOL_VERSION,
                    "draining": false
                }]
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn raw_encrypted_key_range_moves_once_and_leaves_a_source_fence() {
        let registry_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_json(
            &registry_store,
            crate::cells::CELLS_PATH,
            serde_json::json!({
                "version": 1,
                "generation": 1,
                "cells": [
                    {"cell_id":"cell-a","region":"a","ops_prefix":"cells/cell-a","weight":1,"state":"active"},
                    {"cell_id":"cell-b","region":"b","ops_prefix":"cells/cell-b","weight":1,"state":"active"}
                ]
            }),
        )
        .await;
        let registry = Registry::new(registry_store.clone());
        registry
            .get_or_create_customer_cell_affinity("customer-a", "cell-a")
            .await
            .unwrap();
        put_json(
            &registry_store,
            crate::registry::descriptor_path_for("customer-a", "orders").as_ref(),
            serde_json::json!({
                "customer_id": "customer-a",
                "cell": "cell-a",
                "name": "orders",
                "stream_epoch": "01010101010101010101010101010101",
                "key_fingerprint": "opaque",
                "created_ms": 1
            }),
        )
        .await;
        let descriptor = registry.get("customer-a", "orders").await.unwrap().unwrap();
        let hash = descriptor.storage_hash();

        let source = CellStores {
            ops: Arc::new(InMemory::new()),
            shard: Arc::new(InMemory::new()),
            data: Arc::new(InMemory::new()),
            fleet: Arc::new(InMemory::new()),
        };
        let target = CellStores {
            ops: Arc::new(InMemory::new()),
            shard: Arc::new(InMemory::new()),
            data: Arc::new(InMemory::new()),
            fleet: Arc::new(InMemory::new()),
        };
        put_capable_fleet(&source.fleet, "streams-1").await;
        put_capable_fleet(&target.fleet, "streams-1").await;
        let topology = serde_json::json!({
            "version": 1,
            "storage_format": 2,
            "shards": [""],
            "shard_paths": {}
        });
        put_json(&source.ops, "topology.json", topology.clone()).await;
        put_json(&target.ops, "topology.json", topology).await;

        let source_db = Db::builder("shards/root", source.shard.clone())
            .build()
            .await
            .unwrap();
        let mut tail_key = hash.to_vec();
        tail_key.push(b't');
        let mut record_key = hash.to_vec();
        record_key.push(b'r');
        record_key.extend_from_slice(&0u64.to_be_bytes());
        let mut tail = vec![3];
        tail.extend_from_slice(&1u64.to_le_bytes());
        tail.extend_from_slice(&1i64.to_le_bytes());
        tail.extend_from_slice(&10u64.to_le_bytes());
        tail.extend_from_slice(&0u64.to_le_bytes());
        tail.extend_from_slice(&0u64.to_le_bytes());
        tail.push(0);
        tail.extend_from_slice(&0u16.to_le_bytes());
        source_db.put(&tail_key, &tail).await.unwrap();
        source_db.put(&record_key, b"ciphertext").await.unwrap();
        let mut unrelated = [7u8; 32].to_vec();
        unrelated.push(b't');
        source_db.put(&unrelated, b"other-stream").await.unwrap();
        source_db.close().await.unwrap();

        let operation = "ab".repeat(16);
        registry
            .begin_cell_move("customer-a", "orders", "cell-a", "cell-b", &operation)
            .await
            .unwrap();
        let fenced = Db::builder("shards/root", source.shard.clone())
            .build()
            .await
            .unwrap();
        fenced
            .put(
                crate::cell_move_fence::key(&hash),
                crate::cell_move_fence::encode(&operation, "cell-b").unwrap(),
            )
            .await
            .unwrap();
        fenced.close().await.unwrap();
        let partial_target = Db::builder("shards/root", target.shard.clone())
            .build()
            .await
            .unwrap();
        partial_target
            .put(&tail_key, b"interrupted-partial-copy")
            .await
            .unwrap();
        partial_target.close().await.unwrap();

        let report = move_stream(
            &registry,
            "customer-a",
            "orders",
            "cell-a",
            "cell-b",
            &operation,
            &source,
            &target,
            1024 * 1024,
        )
        .await
        .unwrap();
        assert_eq!(report.shard_keys, 2);
        assert!(!report.already_completed);
        let completed = registry.get("customer-a", "orders").await.unwrap().unwrap();
        assert_eq!(completed.cell, "cell-b");

        let target_db = Db::builder("shards/root", target.shard.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(
            target_db.get(&tail_key).await.unwrap().unwrap().as_ref(),
            tail.as_slice()
        );
        assert_eq!(
            target_db.get(&record_key).await.unwrap().unwrap().as_ref(),
            b"ciphertext"
        );
        assert!(target_db.get(&unrelated).await.unwrap().is_none());
        assert!(
            target_db
                .get(crate::cell_move_fence::key(&hash))
                .await
                .unwrap()
                .is_none()
        );
        target_db.close().await.unwrap();

        let fenced_source = Db::builder("shards/root", source.shard.clone())
            .build()
            .await
            .unwrap();
        assert!(
            fenced_source
                .get(crate::cell_move_fence::key(&hash))
                .await
                .unwrap()
                .is_some()
        );
        fenced_source.close().await.unwrap();

        let retry = move_stream(
            &registry,
            "customer-a",
            "orders",
            "cell-a",
            "cell-b",
            &operation,
            &source,
            &target,
            1024 * 1024,
        )
        .await
        .unwrap();
        assert!(retry.already_completed);
    }

    #[tokio::test]
    async fn mixed_version_fleet_is_rejected_before_registry_transition() {
        let registry_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_json(
            &registry_store,
            crate::cells::CELLS_PATH,
            serde_json::json!({
                "version": 1,
                "generation": 1,
                "cells": [
                    {"cell_id":"cell-a","region":"a","ops_prefix":"cells/cell-a","weight":1,"state":"active"},
                    {"cell_id":"cell-b","region":"b","ops_prefix":"cells/cell-b","weight":1,"state":"active"}
                ]
            }),
        )
        .await;
        let registry = Registry::new(registry_store.clone());
        registry
            .get_or_create_customer_cell_affinity("customer-a", "cell-a")
            .await
            .unwrap();
        put_json(
            &registry_store,
            crate::registry::descriptor_path_for("customer-a", "orders").as_ref(),
            serde_json::json!({
                "customer_id": "customer-a",
                "cell": "cell-a",
                "name": "orders",
                "stream_epoch": "01010101010101010101010101010101",
                "key_fingerprint": "opaque",
                "created_ms": 1
            }),
        )
        .await;
        let source = CellStores {
            ops: Arc::new(InMemory::new()),
            shard: Arc::new(InMemory::new()),
            data: Arc::new(InMemory::new()),
            fleet: Arc::new(InMemory::new()),
        };
        let target = CellStores {
            ops: Arc::new(InMemory::new()),
            shard: Arc::new(InMemory::new()),
            data: Arc::new(InMemory::new()),
            fleet: Arc::new(InMemory::new()),
        };
        let now = chrono::Utc::now().timestamp_millis();
        put_json(
            &source.fleet,
            "fleet.json",
            serde_json::json!({
                "version": 1,
                "generated_at_ms": now,
                "heartbeats": [{
                    "instance": "streams-1",
                    "ts_ms": now,
                    "draining": false
                }]
            }),
        )
        .await;
        put_capable_fleet(&target.fleet, "streams-1").await;

        let error = move_stream(
            &registry,
            "customer-a",
            "orders",
            "cell-a",
            "cell-b",
            &"cd".repeat(16),
            &source,
            &target,
            1024 * 1024,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("lacks cell-move protocol"));
        assert!(
            registry
                .get("customer-a", "orders")
                .await
                .unwrap()
                .unwrap()
                .cell_move
                .is_none()
        );
    }
}
