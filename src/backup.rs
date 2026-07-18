//! Provider-independent object-store snapshots and restore.
//!
//! A snapshot conditionally reads the exact ETag returned by LIST, copies
//! every ciphertext/control object under a unique immutable prefix, writes a
//! checksummed inventory record per object, then publishes `_complete.json`
//! last. Partial or corrupt snapshots are never restorable.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, PutResult,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SNAPSHOT_FORMAT_VERSION: u32 = 1;
const COPY_PART_BYTES: usize = 8 * 1024 * 1024;
const MAX_INVENTORY_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct BackupSource {
    pub role: &'static str,
    pub store: Arc<dyn ObjectStore>,
}

pub struct BackupStatus {
    healthy: AtomicBool,
}

impl BackupStatus {
    pub fn ready(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotReport {
    pub format_version: u32,
    pub snapshot_id: String,
    pub started_ms: i64,
    pub completed_ms: i64,
    pub objects: u64,
    pub bytes: u64,
    pub roles: Vec<String>,
    /// XOR of SHA-256(inventory JSON) for a bounded-memory, order-independent
    /// proof that the complete inventory is present and unchanged.
    pub inventory_checksum: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct InventoryRecord {
    role: String,
    source_path: String,
    size: u64,
    sha256: String,
    backup_etag: String,
}

pub fn start(
    sources: Vec<BackupSource>,
    destination: Arc<dyn ObjectStore>,
    interval: Duration,
) -> Arc<BackupStatus> {
    // A configured backup is not healthy until at least one marker-last
    // snapshot has actually completed.
    let status = Arc::new(BackupStatus {
        healthy: AtomicBool::new(false),
    });
    let actor_status = status.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval.max(Duration::from_secs(60)));
        loop {
            tick.tick().await;
            match snapshot_once(&sources, destination.clone()).await {
                Ok(report) => {
                    actor_status.healthy.store(true, Ordering::Release);
                    tracing::info!(
                        snapshot = %report.snapshot_id,
                        objects = report.objects,
                        bytes = report.bytes,
                        "backup snapshot complete"
                    );
                }
                Err(error) => {
                    actor_status.healthy.store(false, Ordering::Release);
                    tracing::error!("backup snapshot failed: {error:#}");
                }
            }
        }
    });
    status
}

pub async fn snapshot_once(
    sources: &[BackupSource],
    destination: Arc<dyn ObjectStore>,
) -> anyhow::Result<SnapshotReport> {
    let started_ms = now_ms();
    let snapshot_id = format!("{:020}-{:032x}", started_ms.max(0), rand::random::<u128>());
    let mut objects = 0u64;
    let mut bytes = 0u64;
    let mut roles = Vec::new();
    let mut inventory_checksum = [0u8; 32];

    for source in sources {
        validate_role(source.role)?;
        roles.push(source.role.to_string());
        let mut listing = source.store.list(None);
        while let Some(meta) = listing.try_next().await? {
            let etag = meta
                .e_tag
                .clone()
                .ok_or_else(|| anyhow::anyhow!("object {} has no ETag", meta.location))?;
            let get = source
                .store
                .get_opts(
                    &meta.location,
                    GetOptions {
                        if_match: Some(etag),
                        ..Default::default()
                    },
                )
                .await?;
            let target = ObjPath::from(format!(
                "snapshots/{}/objects/{}/{}",
                snapshot_id, source.role, meta.location
            ));
            let (copied, digest, put) =
                copy_stream(get.into_stream(), destination.clone(), &target).await?;
            anyhow::ensure!(
                copied == meta.size,
                "object {} changed size during snapshot",
                meta.location
            );
            let record = InventoryRecord {
                role: source.role.to_string(),
                source_path: meta.location.to_string(),
                size: copied,
                sha256: hex_encode(&digest),
                backup_etag: put
                    .e_tag
                    .ok_or_else(|| anyhow::anyhow!("backup object {} has no ETag", target))?,
            };
            let inventory = serde_json::to_vec(&record)?;
            anyhow::ensure!(
                inventory.len() <= MAX_INVENTORY_BYTES,
                "inventory record too large"
            );
            xor_digest(&mut inventory_checksum, Sha256::digest(&inventory).into());
            let inventory_path = inventory_path(&snapshot_id, source.role, &record.source_path);
            destination
                .put_opts(
                    &inventory_path,
                    PutPayload::from(Bytes::from(inventory)),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            objects += 1;
            bytes += copied;
        }
    }

    let report = SnapshotReport {
        format_version: SNAPSHOT_FORMAT_VERSION,
        snapshot_id: snapshot_id.clone(),
        started_ms,
        completed_ms: now_ms(),
        objects,
        bytes,
        roles,
        inventory_checksum: hex_encode(&inventory_checksum),
    };
    let marker = marker_path(&snapshot_id);
    destination
        .put_opts(
            &marker,
            PutPayload::from(Bytes::from(serde_json::to_vec(&report)?)),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;
    // This mutable convenience pointer is never the authority: restore still
    // requires and validates the immutable marker named by this report.
    destination
        .put(
            &ObjPath::from("latest.json"),
            PutPayload::from(Bytes::from(serde_json::to_vec(&report)?)),
        )
        .await?;
    Ok(report)
}

pub async fn latest_snapshot_id(backup: Arc<dyn ObjectStore>) -> anyhow::Result<String> {
    let encoded = backup
        .get(&ObjPath::from("latest.json"))
        .await?
        .bytes()
        .await?;
    anyhow::ensure!(
        encoded.len() <= MAX_INVENTORY_BYTES,
        "latest pointer too large"
    );
    let report: SnapshotReport = serde_json::from_slice(&encoded)?;
    validate_snapshot_id(&report.snapshot_id)?;
    Ok(report.snapshot_id)
}

/// Validate and restore a complete snapshot into empty role stores.
///
/// Each object is copied through a unique temporary key, checked against the
/// immutable inventory, then atomically promoted. A failure cannot expose
/// corrupt bytes at the final source path. Operators must keep the targets
/// offline until this function succeeds.
pub async fn restore_snapshot(
    backup: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    targets: &HashMap<String, Arc<dyn ObjectStore>>,
) -> anyhow::Result<u64> {
    validate_snapshot_id(snapshot_id)?;
    let marker = marker_path(snapshot_id);
    let marker_bytes = backup.get(&marker).await?.bytes().await?;
    let report: SnapshotReport = serde_json::from_slice(&marker_bytes)?;
    anyhow::ensure!(
        report.format_version == SNAPSHOT_FORMAT_VERSION,
        "unsupported snapshot format {}",
        report.format_version
    );
    anyhow::ensure!(
        report.snapshot_id == snapshot_id,
        "snapshot marker id mismatch"
    );

    // Restore is deliberately fail-closed rather than merging a snapshot with
    // live state. Do all emptiness checks before writing any role.
    for role in &report.roles {
        let target = targets
            .get(role)
            .ok_or_else(|| anyhow::anyhow!("no restore target for role {role}"))?;
        anyhow::ensure!(
            target.list(None).try_next().await?.is_none(),
            "restore target for role {role} is not empty"
        );
    }

    let prefix = ObjPath::from(format!("snapshots/{snapshot_id}/inventory"));
    let mut listing = backup.list(Some(&prefix));
    let mut objects = 0u64;
    let mut bytes = 0u64;
    let mut inventory_checksum = [0u8; 32];
    while let Some(meta) = listing.try_next().await? {
        anyhow::ensure!(
            meta.size <= MAX_INVENTORY_BYTES as u64,
            "oversized inventory {}",
            meta.location
        );
        let encoded = backup.get(&meta.location).await?.bytes().await?;
        xor_digest(&mut inventory_checksum, Sha256::digest(&encoded).into());
        let record: InventoryRecord = serde_json::from_slice(&encoded)?;
        validate_role(&record.role)?;
        anyhow::ensure!(
            report.roles.contains(&record.role),
            "inventory references undeclared role {}",
            record.role
        );
        anyhow::ensure!(
            inventory_path(snapshot_id, &record.role, &record.source_path) == meta.location,
            "inventory path mismatch"
        );
        objects += 1;
        bytes = bytes
            .checked_add(record.size)
            .ok_or_else(|| anyhow::anyhow!("snapshot byte count overflow"))?;
    }
    anyhow::ensure!(
        objects == report.objects,
        "snapshot inventory count mismatch"
    );
    anyhow::ensure!(
        bytes == report.bytes,
        "snapshot inventory byte count mismatch"
    );
    anyhow::ensure!(
        hex_encode(&inventory_checksum) == report.inventory_checksum,
        "snapshot inventory checksum mismatch"
    );

    // Re-list instead of retaining every record: snapshot object count is not
    // allowed to become restore-process memory usage.
    let mut listing = backup.list(Some(&prefix));
    let mut restored = 0u64;
    while let Some(meta) = listing.try_next().await? {
        anyhow::ensure!(
            meta.size <= MAX_INVENTORY_BYTES as u64,
            "oversized inventory {}",
            meta.location
        );
        let encoded = backup.get(&meta.location).await?.bytes().await?;
        let record: InventoryRecord = serde_json::from_slice(&encoded)?;
        anyhow::ensure!(
            inventory_path(snapshot_id, &record.role, &record.source_path) == meta.location,
            "inventory path mismatch"
        );
        let target = targets
            .get(&record.role)
            .ok_or_else(|| anyhow::anyhow!("no restore target for role {}", record.role))?;
        let source = ObjPath::from(format!(
            "snapshots/{snapshot_id}/objects/{}/{}",
            record.role, record.source_path
        ));
        let get = backup
            .get_opts(
                &source,
                GetOptions {
                    if_match: Some(record.backup_etag.clone()),
                    ..Default::default()
                },
            )
            .await?;
        let temp = ObjPath::from(format!(
            "_restore_tmp/{snapshot_id}/{}",
            hex_encode(&Sha256::digest(format!(
                "{}\0{}",
                record.role, record.source_path
            )))
        ));
        let (copied, digest, _) = copy_stream(get.into_stream(), target.clone(), &temp).await?;
        if copied != record.size || hex_encode(&digest) != record.sha256 {
            let _ = target.delete(&temp).await;
            anyhow::bail!(
                "backup object failed integrity check: {}",
                record.source_path
            );
        }
        let final_path = ObjPath::from(record.source_path.as_str());
        // Targets were proven empty before any write and must remain offline;
        // object-store COPY atomically publishes the verified temporary body.
        let promoted = target.copy(&temp, &final_path).await;
        let cleanup = target.delete(&temp).await;
        promoted?;
        cleanup?;
        restored += 1;
    }
    anyhow::ensure!(restored == objects, "snapshot changed during restore");
    Ok(restored)
}

async fn copy_stream(
    mut stream: futures_util::stream::BoxStream<'static, object_store::Result<Bytes>>,
    destination: Arc<dyn ObjectStore>,
    target: &ObjPath,
) -> anyhow::Result<(u64, [u8; 32], PutResult)> {
    let mut buffer = BytesMut::with_capacity(COPY_PART_BYTES * 2);
    let mut upload = None;
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("object size overflow"))?;
        hasher.update(&chunk);
        buffer.extend_from_slice(&chunk);
        while buffer.len() >= COPY_PART_BYTES {
            let uploader = match upload.as_mut() {
                Some(uploader) => uploader,
                None => upload.insert(destination.put_multipart(target).await?),
            };
            uploader
                .put_part(PutPayload::from(buffer.split_to(COPY_PART_BYTES).freeze()))
                .await?;
        }
    }

    let put = if let Some(mut uploader) = upload {
        if !buffer.is_empty() {
            uploader.put_part(PutPayload::from(buffer.freeze())).await?;
        }
        uploader.complete().await?
    } else {
        destination
            .put_opts(
                target,
                PutPayload::from(buffer.freeze()),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?
    };
    Ok((size, hasher.finalize().into(), put))
}

fn marker_path(snapshot_id: &str) -> ObjPath {
    ObjPath::from(format!("snapshots/{snapshot_id}/_complete.json"))
}

fn inventory_path(snapshot_id: &str, role: &str, source_path: &str) -> ObjPath {
    let path_hash = Sha256::digest(format!("{role}\0{source_path}"));
    ObjPath::from(format!(
        "snapshots/{snapshot_id}/inventory/{role}/{}.json",
        hex_encode(&path_hash)
    ))
}

fn validate_role(role: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !role.is_empty()
            && role
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid backup role"
    );
    Ok(())
}

fn validate_snapshot_id(snapshot_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !snapshot_id.is_empty()
            && snapshot_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid snapshot id"
    );
    Ok(())
}

fn xor_digest(accumulator: &mut [u8; 32], digest: [u8; 32]) {
    for (target, source) in accumulator.iter_mut().zip(digest) {
        *target ^= source;
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn complete_snapshot_restores_exact_objects() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(
                &ObjPath::from("shards/a/manifest/1"),
                PutPayload::from_static(b"manifest"),
            )
            .await
            .unwrap();
        source
            .put(
                &ObjPath::from("shards/a/wal/1.sst"),
                PutPayload::from_static(b"ciphertext"),
            )
            .await
            .unwrap();
        let report = snapshot_once(
            &[BackupSource {
                role: "shard",
                store: source,
            }],
            backup.clone(),
        )
        .await
        .unwrap();
        let restored: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let targets = HashMap::from([("shard".to_string(), restored.clone())]);

        assert_eq!(
            restore_snapshot(backup, &report.snapshot_id, &targets)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            restored
                .get(&ObjPath::from("shards/a/wal/1.sst"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"ciphertext")
        );
    }

    #[tokio::test]
    async fn restore_rejects_missing_inventory() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(&ObjPath::from("one"), PutPayload::from_static(b"1"))
            .await
            .unwrap();
        source
            .put(&ObjPath::from("two"), PutPayload::from_static(b"2"))
            .await
            .unwrap();
        let report = snapshot_once(
            &[BackupSource {
                role: "ops",
                store: source,
            }],
            backup.clone(),
        )
        .await
        .unwrap();
        let prefix = ObjPath::from(format!("snapshots/{}/inventory", report.snapshot_id));
        let victim = backup
            .list(Some(&prefix))
            .try_next()
            .await
            .unwrap()
            .unwrap();
        backup.delete(&victim.location).await.unwrap();
        let target: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        let error = restore_snapshot(
            backup,
            &report.snapshot_id,
            &HashMap::from([("ops".to_string(), target)]),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("inventory count mismatch"));
    }

    #[tokio::test]
    async fn restore_requires_empty_target() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(&ObjPath::from("one"), PutPayload::from_static(b"1"))
            .await
            .unwrap();
        let report = snapshot_once(
            &[BackupSource {
                role: "ops",
                store: source,
            }],
            backup.clone(),
        )
        .await
        .unwrap();
        let target: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        target
            .put(&ObjPath::from("live"), PutPayload::from_static(b"data"))
            .await
            .unwrap();

        let error = restore_snapshot(
            backup,
            &report.snapshot_id,
            &HashMap::from([("ops".to_string(), target)]),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("not empty"));
    }
}
