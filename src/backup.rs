//! Provider-independent object-store snapshots and restore.
//!
//! Live SlateDB databases are pinned with expiring checkpoints while a
//! snapshot is taken. Source ETags feed a durable incremental index and
//! immutable SHA-256 blobs, so unchanged ciphertext/control objects are not
//! copied again. A checksummed inventory is published before `_complete.json`;
//! partial or corrupt snapshots are never restorable.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
use slatedb::admin::{Admin, AdminBuilder};
use slatedb::config::CheckpointOptions;

const SNAPSHOT_FORMAT_VERSION: u32 = 2;
const LEGACY_SNAPSHOT_FORMAT_VERSION: u32 = 1;
const COPY_PART_BYTES: usize = 8 * 1024 * 1024;
const MAX_INVENTORY_BYTES: usize = 16 * 1024;
const MAX_TOPOLOGY_BYTES: usize = 4 * 1024 * 1024;
const MAX_PINNED_SHARDS: usize = 16_384;
const MAX_PINNED_HISTORY_DBS: usize = 100_000;
const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;
const MAX_SNAPSHOT_GENERATIONS: usize = 100_000;
const SCRUB_STATE_FORMAT_VERSION: u32 = 1;
const MAX_SCRUB_STATE_BYTES: usize = 4 * 1024;

#[derive(Clone)]
pub struct BackupSource {
    pub role: &'static str,
    pub store: Arc<dyn ObjectStore>,
}

#[derive(Clone)]
pub struct BackupPins {
    pub topology_store: Arc<dyn ObjectStore>,
    pub shard_store: Arc<dyn ObjectStore>,
    pub data_store: Arc<dyn ObjectStore>,
    pub lifetime: Duration,
}

pub struct BackupConfig {
    pub sources: Vec<BackupSource>,
    pub destination: Arc<dyn ObjectStore>,
    pub interval: Duration,
    pub retention: Duration,
    pub scrub_interval: Duration,
    pub scrub_objects_per_interval: usize,
    pub pins: Option<BackupPins>,
}

pub struct BackupStatus {
    snapshot_healthy: AtomicBool,
    scrub_healthy: AtomicBool,
}

impl BackupStatus {
    pub fn ready(&self) -> bool {
        self.snapshot_healthy.load(Ordering::Acquire) && self.scrub_healthy.load(Ordering::Acquire)
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
    #[serde(default)]
    pub copied_objects: u64,
    #[serde(default)]
    pub copied_bytes: u64,
    #[serde(default)]
    pub reused_objects: u64,
    #[serde(default)]
    pub pinned_shards: u64,
    #[serde(default)]
    pub pinned_history_dbs: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct InventoryRecord {
    role: String,
    source_path: String,
    size: u64,
    sha256: String,
    backup_etag: String,
    /// Format 2 stores immutable content once and points every snapshot at it.
    /// Absent for format-1 snapshots, whose object path was snapshot-local.
    #[serde(default)]
    blob_path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceIndex {
    role: String,
    source_path: String,
    source_etag: String,
    size: u64,
    sha256: String,
    backup_etag: String,
    blob_path: String,
    snapshot_id: String,
    referenced_ms: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct BlobReference {
    sha256: String,
    blob_path: String,
    snapshot_id: String,
    referenced_ms: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ScrubState {
    format_version: u32,
    /// Last successfully verified reference in a provider-independent,
    /// lexicographically ordered sweep. Persisting this prevents process
    /// restarts from starving the high end of a large recovery corpus.
    cursor: Option<String>,
    updated_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct BackupTopology {
    version: u64,
    storage_format: u32,
    shards: Vec<String>,
    #[serde(default)]
    shard_paths: HashMap<String, String>,
}

impl BackupTopology {
    fn db_path(&self, prefix: &str) -> String {
        self.shard_paths.get(prefix).cloned().unwrap_or_else(|| {
            if prefix.is_empty() {
                "shards/root".to_string()
            } else {
                format!("shards/{prefix}")
            }
        })
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.version > 0, "backup topology version is invalid");
        anyhow::ensure!(self.storage_format == 2, "unsupported live storage format");
        anyhow::ensure!(
            !self.shards.is_empty() && self.shards.len() <= MAX_PINNED_SHARDS,
            "backup topology shard count is out of range"
        );
        let mut unique = HashSet::with_capacity(self.shards.len());
        for prefix in &self.shards {
            anyhow::ensure!(
                prefix.len() <= 128 && prefix.bytes().all(|byte| matches!(byte, b'0' | b'1')),
                "backup topology has invalid shard prefix"
            );
            anyhow::ensure!(unique.insert(prefix), "backup topology has duplicate shard");
            let path = self.db_path(prefix);
            anyhow::ensure!(path.len() <= 1_024, "backup shard path is too long");
            ObjPath::parse(path)?;
        }
        Ok(())
    }
}

struct CheckpointLease {
    admin: Admin,
    id: uuid::Uuid,
    path: String,
    manifest_id: u64,
}

struct PinnedBackupState {
    topology: BackupTopology,
    /// Exact manifest version to expose for every live DB and recursively
    /// referenced external DB. Newer manifests may exist while the copy runs,
    /// but are intentionally absent from this recovery point.
    /// `None` means this live shard had never been initialized at the recovery
    /// point. Any objects that appear under that DB path later are excluded.
    shard_manifests: HashMap<String, Option<PinnedDbManifest>>,
    /// Every initialized history DB is pinned after its shard checkpoint. The
    /// history state may therefore contain harmless future rows, but always
    /// contains the durable absorbed prefix named by the shard point.
    history_manifests: HashMap<String, Option<PinnedDbManifest>>,
}

#[derive(Clone)]
struct PinnedDbManifest {
    manifest_id: u64,
    allowed_manifest_ids: HashSet<u64>,
    replay_after_wal_id: u64,
    next_wal_sst_id: u64,
    compactions_id: Option<u64>,
}

pub fn start(config: BackupConfig) -> Arc<BackupStatus> {
    // A configured backup is not healthy until at least one marker-last
    // snapshot has actually completed.
    let status = Arc::new(BackupStatus {
        snapshot_healthy: AtomicBool::new(false),
        scrub_healthy: AtomicBool::new(true),
    });
    let actor_status = status.clone();
    tokio::spawn(async move {
        let mut snapshot_tick = tokio::time::interval(config.interval.max(Duration::from_secs(60)));
        let mut scrub_tick =
            tokio::time::interval(config.scrub_interval.max(Duration::from_secs(10)));
        loop {
            tokio::select! {
                _ = snapshot_tick.tick() => {
                    let result = snapshot_once_with_pins(
                        &config.sources,
                        config.destination.clone(),
                        config.pins.as_ref(),
                    ).await;
                    match result {
                        Ok(report) => {
                            match prune_once(config.destination.clone(), config.retention).await {
                                Ok(pruned) => {
                                    actor_status.snapshot_healthy.store(true, Ordering::Release);
                                    tracing::info!(
                                        snapshot = %report.snapshot_id,
                                        objects = report.objects,
                                        bytes = report.bytes,
                                        copied_objects = report.copied_objects,
                                        copied_bytes = report.copied_bytes,
                                        reused_objects = report.reused_objects,
                                        pinned_shards = report.pinned_shards,
                                        pinned_history_dbs = report.pinned_history_dbs,
                                        pruned,
                                        "incremental backup snapshot complete"
                                    );
                                }
                                Err(error) => {
                                    actor_status.snapshot_healthy.store(false, Ordering::Release);
                                    tracing::error!("backup retention failed: {error:#}");
                                }
                            }
                        }
                        Err(error) => {
                            actor_status.snapshot_healthy.store(false, Ordering::Release);
                            tracing::error!("backup snapshot failed: {error:#}");
                        }
                    }
                }
                _ = scrub_tick.tick() => {
                    match scrub_blob_batch(
                        config.destination.clone(),
                        config.scrub_objects_per_interval.max(1),
                    ).await {
                        Ok(checked) => {
                            actor_status.scrub_healthy.store(true, Ordering::Release);
                            tracing::info!(checked, "backup content scrub batch complete");
                        }
                        Err(error) => {
                            actor_status.scrub_healthy.store(false, Ordering::Release);
                            tracing::error!("backup content scrub failed: {error:#}");
                        }
                    }
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
    snapshot_once_with_pins(sources, destination, None).await
}

async fn snapshot_once_with_pins(
    sources: &[BackupSource],
    destination: Arc<dyn ObjectStore>,
    pins: Option<&BackupPins>,
) -> anyhow::Result<SnapshotReport> {
    let started_ms = now_ms();
    let snapshot_id = format!("{:020}-{:032x}", started_ms.max(0), rand::random::<u128>());
    let (pinned_state, leases) = match pins {
        Some(pins) => {
            let (state, leases) = acquire_checkpoint_leases(pins, &snapshot_id).await?;
            (Some(state), leases)
        }
        None => (None, Vec::new()),
    };
    let result = snapshot_once_inner(
        sources,
        destination,
        pins,
        pinned_state.as_ref(),
        started_ms,
        &snapshot_id,
    )
    .await;
    let release = release_checkpoint_leases(leases).await;
    match (result, release) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), _) => Err(error),
        (Ok(report), Err(error)) => {
            // Leases are born with a finite lifetime, so failed eager cleanup
            // cannot pin primary storage forever or invalidate a complete copy.
            tracing::warn!("backup checkpoint cleanup deferred to expiry: {error:#}");
            Ok(report)
        }
    }
}

async fn snapshot_once_inner(
    sources: &[BackupSource],
    destination: Arc<dyn ObjectStore>,
    pins: Option<&BackupPins>,
    pinned_state: Option<&PinnedBackupState>,
    started_ms: i64,
    snapshot_id: &str,
) -> anyhow::Result<SnapshotReport> {
    let mut objects = 0u64;
    let mut bytes = 0u64;
    let mut copied_objects = 0u64;
    let mut copied_bytes = 0u64;
    let mut reused_objects = 0u64;
    let mut roles = Vec::new();
    let mut inventory_checksum = [0u8; 32];

    for source in sources {
        validate_role(source.role)?;
        roles.push(source.role.to_string());
        let mut listing = source.store.list(None);
        while let Some(meta) = listing.try_next().await? {
            if matches!(source.role, "shard" | "data")
                && pinned_state
                    .is_some_and(|state| object_is_outside_recovery_point(&meta.location, state))
            {
                continue;
            }
            let source_etag = meta
                .e_tag
                .clone()
                .ok_or_else(|| anyhow::anyhow!("object {} has no ETag", meta.location))?;
            let record = match reusable_record(
                destination.clone(),
                source.role,
                &meta.location,
                &source_etag,
                meta.size,
            )
            .await?
            {
                Some(record) => {
                    reused_objects = reused_objects.saturating_add(1);
                    record
                }
                None => {
                    let record = copy_incremental_object(
                        source,
                        destination.clone(),
                        snapshot_id,
                        &meta.location,
                        &source_etag,
                        meta.size,
                    )
                    .await?;
                    copied_objects = copied_objects.saturating_add(1);
                    copied_bytes = copied_bytes.saturating_add(record.size);
                    record
                }
            };
            write_source_index(destination.clone(), &record, &source_etag, snapshot_id).await?;
            touch_blob_reference(destination.clone(), &record, snapshot_id).await?;
            let inventory = serde_json::to_vec(&record)?;
            anyhow::ensure!(
                inventory.len() <= MAX_INVENTORY_BYTES,
                "inventory record too large"
            );
            xor_digest(&mut inventory_checksum, Sha256::digest(&inventory).into());
            let inventory_path = inventory_path(snapshot_id, source.role, &record.source_path);
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
            bytes = bytes
                .checked_add(record.size)
                .ok_or_else(|| anyhow::anyhow!("snapshot byte count overflow"))?;
        }
    }

    if let (Some(pins), Some(state)) = (pins, pinned_state) {
        verify_pinned_state(pins, state).await?;
    }

    let report = SnapshotReport {
        format_version: SNAPSHOT_FORMAT_VERSION,
        snapshot_id: snapshot_id.to_string(),
        started_ms,
        completed_ms: now_ms(),
        objects,
        bytes,
        roles,
        inventory_checksum: hex_encode(&inventory_checksum),
        copied_objects,
        copied_bytes,
        reused_objects,
        pinned_shards: pinned_state.map_or(0, |state| state.topology.shards.len() as u64),
        pinned_history_dbs: pinned_state.map_or(0, |state| {
            state
                .history_manifests
                .values()
                .filter(|manifest| manifest.is_some())
                .count() as u64
        }),
    };
    let marker = marker_path(snapshot_id);
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

async fn read_backup_topology(store: &Arc<dyn ObjectStore>) -> anyhow::Result<BackupTopology> {
    let encoded = store
        .get(&ObjPath::from("topology.json"))
        .await?
        .bytes()
        .await?;
    anyhow::ensure!(
        encoded.len() <= MAX_TOPOLOGY_BYTES,
        "backup topology object is too large"
    );
    let topology: BackupTopology = serde_json::from_slice(&encoded)?;
    topology.validate()?;
    Ok(topology)
}

async fn acquire_checkpoint_leases(
    pins: &BackupPins,
    snapshot_id: &str,
) -> anyhow::Result<(PinnedBackupState, Vec<CheckpointLease>)> {
    let topology = read_backup_topology(&pins.topology_store).await?;
    let mut shard_leases = Vec::with_capacity(topology.shards.len());
    let mut shard_absent = Vec::new();
    for prefix in &topology.shards {
        let path = topology.db_path(prefix);
        if let Err(error) = acquire_db_checkpoint(
            pins.shard_store.clone(),
            path,
            pins.lifetime,
            snapshot_id,
            &mut shard_leases,
            &mut shard_absent,
        )
        .await
        {
            let _ = release_checkpoint_leases(shard_leases).await;
            return Err(error);
        }
    }
    if let Err(error) = verify_pinned_topology(pins, &topology).await {
        let _ = release_checkpoint_leases(shard_leases).await;
        return Err(error);
    }
    let (history_leases, history_absent) =
        match acquire_history_checkpoints(pins, snapshot_id).await {
            Ok(result) => result,
            Err(error) => {
                let _ = release_checkpoint_leases(shard_leases).await;
                return Err(error);
            }
        };
    let shard_manifests =
        match collect_pinned_manifests(pins.shard_store.clone(), &shard_leases, shard_absent).await
        {
            Ok(manifests) => manifests,
            Err(error) => {
                let _ = release_checkpoint_leases(shard_leases).await;
                let _ = release_checkpoint_leases(history_leases).await;
                return Err(error);
            }
        };
    let history_manifests =
        match collect_pinned_manifests(pins.data_store.clone(), &history_leases, history_absent)
            .await
        {
            Ok(manifests) => manifests,
            Err(error) => {
                let _ = release_checkpoint_leases(shard_leases).await;
                let _ = release_checkpoint_leases(history_leases).await;
                return Err(error);
            }
        };
    let mut leases = shard_leases;
    leases.extend(history_leases);
    Ok((
        PinnedBackupState {
            topology,
            shard_manifests,
            history_manifests,
        },
        leases,
    ))
}

async fn acquire_db_checkpoint(
    store: Arc<dyn ObjectStore>,
    path: String,
    lifetime: Duration,
    snapshot_id: &str,
    leases: &mut Vec<CheckpointLease>,
    absent: &mut Vec<String>,
) -> anyhow::Result<()> {
    let admin = AdminBuilder::new(path.clone(), store).build();
    if admin.read_manifest(None).await?.is_none() {
        absent.push(path);
        return Ok(());
    }
    let options = CheckpointOptions {
        lifetime: Some(lifetime.max(Duration::from_secs(60))),
        name: Some(format!("streams-backup-{snapshot_id}")),
        ..Default::default()
    };
    let checkpoint = admin.create_detached_checkpoint(&options).await?;
    leases.push(CheckpointLease {
        admin,
        id: checkpoint.id,
        path,
        manifest_id: checkpoint.manifest_id,
    });
    Ok(())
}

async fn acquire_history_checkpoints(
    pins: &BackupPins,
    snapshot_id: &str,
) -> anyhow::Result<(Vec<CheckpointLease>, Vec<String>)> {
    let mut paths = HashSet::new();
    let mut listing = pins.topology_store.list(Some(&ObjPath::from("registry")));
    while let Some(meta) = listing.try_next().await? {
        if !meta.location.as_ref().ends_with(".json")
            || !meta.location.as_ref().contains("/by-name/")
        {
            continue;
        }
        anyhow::ensure!(
            meta.size <= MAX_DESCRIPTOR_BYTES as u64,
            "registry descriptor is too large for recovery"
        );
        let encoded = pins
            .topology_store
            .get(&meta.location)
            .await?
            .bytes()
            .await?;
        let descriptor: crate::registry::StreamDesc = serde_json::from_slice(&encoded)?;
        anyhow::ensure!(
            crate::registry::descriptor_path_for(descriptor.owner(), &descriptor.name)
                == meta.location,
            "registry descriptor identity does not match its recovery path"
        );
        validate_recovery_descriptor(&descriptor)?;
        if descriptor.deleted {
            continue;
        }
        if descriptor.is_per_key() {
            for ordinal in 0..descriptor.segment_count {
                paths.insert(recovery_history_db_path(&descriptor.segment_hash(ordinal)));
            }
        } else {
            paths.insert(recovery_history_db_path(&descriptor.storage_hash()));
        }
        anyhow::ensure!(
            paths.len() <= MAX_PINNED_HISTORY_DBS,
            "active history database count exceeds the recovery cell bound"
        );
    }

    let mut paths: Vec<_> = paths.into_iter().collect();
    paths.sort();
    let mut leases = Vec::new();
    let mut absent = Vec::new();
    for path in paths {
        if let Err(error) = acquire_db_checkpoint(
            pins.data_store.clone(),
            path,
            pins.lifetime,
            snapshot_id,
            &mut leases,
            &mut absent,
        )
        .await
        {
            let _ = release_checkpoint_leases(leases).await;
            return Err(error);
        }
    }
    Ok((leases, absent))
}

fn validate_recovery_descriptor(descriptor: &crate::registry::StreamDesc) -> anyhow::Result<()> {
    anyhow::ensure!(
        !descriptor.owner().is_empty()
            && descriptor.owner().len() <= 1_024
            && !descriptor.name.is_empty()
            && descriptor.name.len() <= 1_024
            && descriptor.epoch_bytes().is_some(),
        "registry descriptor identity is invalid for recovery"
    );
    if descriptor.is_per_key() {
        anyhow::ensure!(
            (1..=256).contains(&descriptor.segment_count)
                && descriptor.segment_count.is_power_of_two(),
            "registry descriptor has invalid history segments"
        );
    } else {
        anyhow::ensure!(
            descriptor.ordering.is_none() && descriptor.segment_count == 0,
            "registry descriptor has unsupported history ordering"
        );
    }
    Ok(())
}

fn recovery_history_db_path(hash: &crate::registry::StorageHash) -> String {
    format!("streams/{}", crate::crypto::hex(hash))
}

async fn verify_pinned_topology(
    pins: &BackupPins,
    expected: &BackupTopology,
) -> anyhow::Result<()> {
    let current = read_backup_topology(&pins.topology_store).await?;
    anyhow::ensure!(
        current == *expected,
        "topology changed while the backup shard set was pinned"
    );
    Ok(())
}

async fn verify_pinned_state(pins: &BackupPins, state: &PinnedBackupState) -> anyhow::Result<()> {
    verify_pinned_topology(pins, &state.topology).await?;
    verify_manifest_set(&pins.shard_store, &state.shard_manifests).await?;
    verify_manifest_set(&pins.data_store, &state.history_manifests).await
}

async fn verify_manifest_set(
    store: &Arc<dyn ObjectStore>,
    manifests: &HashMap<String, Option<PinnedDbManifest>>,
) -> anyhow::Result<()> {
    for (path, pinned) in manifests {
        let Some(pinned) = pinned else {
            continue;
        };
        anyhow::ensure!(
            AdminBuilder::new(path.clone(), store.clone())
                .build()
                .read_manifest(Some(pinned.manifest_id))
                .await?
                .is_some(),
            "pinned manifest disappeared while snapshotting: {path}/{}",
            pinned.manifest_id
        );
    }
    Ok(())
}

async fn collect_pinned_manifests(
    store: Arc<dyn ObjectStore>,
    leases: &[CheckpointLease],
    absent: Vec<String>,
) -> anyhow::Result<HashMap<String, Option<PinnedDbManifest>>> {
    let mut pending = VecDeque::new();
    for lease in leases {
        pending.push_back((lease.path.clone(), lease.manifest_id));
    }
    let mut manifests: HashMap<String, Option<PinnedDbManifest>> =
        absent.into_iter().map(|path| (path, None)).collect();
    while let Some((path, manifest_id)) = pending.pop_front() {
        if let Some(existing) = manifests.get(&path) {
            anyhow::ensure!(
                existing
                    .as_ref()
                    .is_some_and(|existing| existing.manifest_id == manifest_id),
                "recovery point references conflicting manifests for {path}"
            );
            continue;
        }
        anyhow::ensure!(
            manifests.len() < MAX_PINNED_SHARDS.saturating_mul(8),
            "pinned external database graph exceeds safety bound"
        );
        let admin = AdminBuilder::new(path.clone(), store.clone()).build();
        let manifest = admin
            .read_manifest(Some(manifest_id))
            .await?
            .ok_or_else(|| anyhow::anyhow!("pinned manifest is missing: {path}/{manifest_id}"))?;
        let compactions_id = compatible_compactions_id(&admin, manifest.compactor_epoch()).await?;
        let mut allowed_manifest_ids = HashSet::from([manifest_id]);
        allowed_manifest_ids.extend(
            manifest
                .checkpoints()
                .iter()
                .map(|checkpoint| checkpoint.manifest_id),
        );
        anyhow::ensure!(
            allowed_manifest_ids.len() <= MAX_SNAPSHOT_GENERATIONS,
            "checkpoint manifest set exceeds recovery safety bound"
        );
        for external in manifest.external_dbs() {
            let checkpoint_id = external.final_checkpoint_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "external database has no final checkpoint: {}",
                    external.path
                )
            })?;
            let external_admin = AdminBuilder::new(external.path.clone(), store.clone()).build();
            let external_manifest = external_admin.read_manifest(None).await?.ok_or_else(|| {
                anyhow::anyhow!("external database is missing: {}", external.path)
            })?;
            external_manifest
                .checkpoints()
                .iter()
                .find(|checkpoint| checkpoint.id == checkpoint_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "external final checkpoint is missing: {}/{}",
                        external.path,
                        checkpoint_id
                    )
                })?;
            // Keep the checkpoint-bearing manifest, not merely the older data
            // manifest it references. Clone-open validation must observe the
            // final checkpoint after restore.
            pending.push_back((external.path.clone(), external_manifest.id()));
        }
        manifests.insert(
            path,
            Some(PinnedDbManifest {
                manifest_id,
                allowed_manifest_ids,
                replay_after_wal_id: manifest.replay_after_wal_id(),
                next_wal_sst_id: manifest.next_wal_sst_id(),
                compactions_id,
            }),
        );
    }
    Ok(manifests)
}

async fn compatible_compactions_id(admin: &Admin, epoch: u64) -> anyhow::Result<Option<u64>> {
    let Some(latest) = admin.read_compactions(None).await? else {
        return Ok(None);
    };
    if latest.compactor_epoch() <= epoch {
        return Ok(Some(latest.id()));
    }
    let versions = admin.list_compactions(..).await?;
    anyhow::ensure!(
        versions.len() <= MAX_SNAPSHOT_GENERATIONS,
        "compactions history exceeds recovery safety bound"
    );
    Ok(versions
        .into_iter()
        .rev()
        .find(|version| version.compactor_epoch() <= epoch)
        .map(|version| version.id()))
}

async fn release_checkpoint_leases(leases: Vec<CheckpointLease>) -> anyhow::Result<()> {
    let mut first_error = None;
    for lease in leases {
        if let Err(error) = lease.admin.delete_checkpoint(lease.id).await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

async fn reusable_record(
    destination: Arc<dyn ObjectStore>,
    role: &str,
    source_path: &ObjPath,
    source_etag: &str,
    size: u64,
) -> anyhow::Result<Option<InventoryRecord>> {
    let path = source_index_path(role, source_path.as_ref());
    let encoded = match destination.get(&path).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        encoded.len() <= MAX_INVENTORY_BYTES,
        "source index is too large"
    );
    let index: SourceIndex = serde_json::from_slice(&encoded)?;
    if index.role != role
        || index.source_path != source_path.as_ref()
        || index.source_etag != source_etag
        || index.size != size
        || !valid_sha256(&index.sha256)
        || index.blob_path != blob_path_for_sha(&index.sha256).as_ref()
    {
        return Ok(None);
    }
    let blob = ObjPath::parse(&index.blob_path)?;
    let meta = match destination.head(&blob).await {
        Ok(meta) if meta.size == size => meta,
        Ok(_) | Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let backup_etag = meta
        .e_tag
        .ok_or_else(|| anyhow::anyhow!("backup blob {blob} has no ETag"))?;
    Ok(Some(InventoryRecord {
        role: role.to_string(),
        source_path: source_path.to_string(),
        size,
        sha256: index.sha256,
        backup_etag,
        blob_path: Some(index.blob_path),
    }))
}

async fn copy_incremental_object(
    source: &BackupSource,
    destination: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    source_path: &ObjPath,
    source_etag: &str,
    expected_size: u64,
) -> anyhow::Result<InventoryRecord> {
    let get = source
        .store
        .get_opts(
            source_path,
            GetOptions {
                if_match: Some(source_etag.to_string()),
                ..Default::default()
            },
        )
        .await?;
    let staging = staging_path(snapshot_id, source.role, source_path.as_ref());
    let copied = copy_stream(get.into_stream(), destination.clone(), &staging).await;
    let (size, digest, _) = match copied {
        Ok(result) => result,
        Err(error) => {
            let _ = destination.delete(&staging).await;
            return Err(error);
        }
    };
    if size != expected_size {
        let _ = destination.delete(&staging).await;
        anyhow::bail!("object {source_path} changed size during snapshot");
    }
    let sha256 = hex_encode(&digest);
    let blob = blob_path_for_sha(&sha256);
    let existing_is_valid = match destination.head(&blob).await {
        Ok(meta) if meta.size == size => verify_blob_digest(destination.clone(), &blob, &sha256)
            .await
            .is_ok(),
        Ok(_) | Err(object_store::Error::NotFound { .. }) => false,
        Err(error) => {
            let _ = destination.delete(&staging).await;
            return Err(error.into());
        }
    };
    if !existing_is_valid && let Err(error) = destination.copy(&staging, &blob).await {
        let _ = destination.delete(&staging).await;
        return Err(error.into());
    }
    destination.delete(&staging).await?;
    let meta = destination.head(&blob).await?;
    anyhow::ensure!(meta.size == size, "backup blob has the wrong size");
    let backup_etag = meta
        .e_tag
        .ok_or_else(|| anyhow::anyhow!("backup blob {blob} has no ETag"))?;
    Ok(InventoryRecord {
        role: source.role.to_string(),
        source_path: source_path.to_string(),
        size,
        sha256,
        backup_etag,
        blob_path: Some(blob.to_string()),
    })
}

async fn write_source_index(
    destination: Arc<dyn ObjectStore>,
    record: &InventoryRecord,
    source_etag: &str,
    snapshot_id: &str,
) -> anyhow::Result<()> {
    let index = SourceIndex {
        role: record.role.clone(),
        source_path: record.source_path.clone(),
        source_etag: source_etag.to_string(),
        size: record.size,
        sha256: record.sha256.clone(),
        backup_etag: record.backup_etag.clone(),
        blob_path: record
            .blob_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("incremental record has no blob path"))?,
        snapshot_id: snapshot_id.to_string(),
        referenced_ms: now_ms(),
    };
    let encoded = serde_json::to_vec(&index)?;
    anyhow::ensure!(
        encoded.len() <= MAX_INVENTORY_BYTES,
        "source index too large"
    );
    destination
        .put(
            &source_index_path(&record.role, &record.source_path),
            PutPayload::from(Bytes::from(encoded)),
        )
        .await?;
    Ok(())
}

async fn touch_blob_reference(
    destination: Arc<dyn ObjectStore>,
    record: &InventoryRecord,
    snapshot_id: &str,
) -> anyhow::Result<()> {
    let reference = BlobReference {
        sha256: record.sha256.clone(),
        blob_path: record
            .blob_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("incremental record has no blob path"))?,
        snapshot_id: snapshot_id.to_string(),
        referenced_ms: now_ms(),
    };
    destination
        .put(
            &blob_reference_path(&record.sha256),
            PutPayload::from(Bytes::from(serde_json::to_vec(&reference)?)),
        )
        .await?;
    Ok(())
}

async fn verify_blob_digest(
    destination: Arc<dyn ObjectStore>,
    blob: &ObjPath,
    expected_sha256: &str,
) -> anyhow::Result<u64> {
    let mut stream = destination.get(blob).await?.into_stream();
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("backup blob size overflow"))?;
        hasher.update(&chunk);
    }
    anyhow::ensure!(
        hex_encode(&hasher.finalize()) == expected_sha256,
        "backup blob digest mismatch: {blob}"
    );
    Ok(size)
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
        matches!(
            report.format_version,
            LEGACY_SNAPSHOT_FORMAT_VERSION | SNAPSHOT_FORMAT_VERSION
        ),
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
        validate_inventory_record(&record, report.format_version)?;
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
        validate_inventory_record(&record, report.format_version)?;
        anyhow::ensure!(
            inventory_path(snapshot_id, &record.role, &record.source_path) == meta.location,
            "inventory path mismatch"
        );
        let target = targets
            .get(&record.role)
            .ok_or_else(|| anyhow::anyhow!("no restore target for role {}", record.role))?;
        let source = match &record.blob_path {
            Some(path) => ObjPath::parse(path)?,
            None => ObjPath::from(format!(
                "snapshots/{snapshot_id}/objects/{}/{}",
                record.role, record.source_path
            )),
        };
        // Format 2 is content-addressed and verified by SHA-256 below. Avoid
        // binding old inventories to a provider ETag if an operator repairs a
        // corrupt blob in place with the exact expected content.
        let get = if report.format_version == LEGACY_SNAPSHOT_FORMAT_VERSION {
            backup
                .get_opts(
                    &source,
                    GetOptions {
                        if_match: Some(record.backup_etag.clone()),
                        ..Default::default()
                    },
                )
                .await?
        } else {
            backup.get(&source).await?
        };
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

pub async fn prune_once(
    destination: Arc<dyn ObjectStore>,
    retention: Duration,
) -> anyhow::Result<u64> {
    anyhow::ensure!(!retention.is_zero(), "backup retention must be positive");
    let cutoff = now_ms().saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX));
    let snapshots_prefix = ObjPath::from("snapshots");
    let mut generations = HashSet::new();
    let mut completed = HashMap::new();
    let mut listing = destination.list(Some(&snapshots_prefix));
    while let Some(meta) = listing.try_next().await? {
        let Some(snapshot_id) = generation_from_path(&meta.location, "snapshots") else {
            continue;
        };
        validate_snapshot_id(&snapshot_id)?;
        generations.insert(snapshot_id.clone());
        anyhow::ensure!(
            generations.len() <= MAX_SNAPSHOT_GENERATIONS,
            "backup snapshot generation count exceeds safety bound"
        );
        if meta.location == marker_path(&snapshot_id) {
            anyhow::ensure!(
                meta.size <= MAX_INVENTORY_BYTES as u64,
                "snapshot marker is too large"
            );
            let encoded = destination.get(&meta.location).await?.bytes().await?;
            let report: SnapshotReport = serde_json::from_slice(&encoded)?;
            anyhow::ensure!(
                report.snapshot_id == snapshot_id,
                "snapshot marker id mismatch during retention"
            );
            completed.insert(snapshot_id, report.completed_ms);
        }
    }
    let staging_prefix = ObjPath::from("staging");
    let mut listing = destination.list(Some(&staging_prefix));
    while let Some(meta) = listing.try_next().await? {
        if let Some(snapshot_id) = generation_from_path(&meta.location, "staging") {
            validate_snapshot_id(&snapshot_id)?;
            generations.insert(snapshot_id);
            anyhow::ensure!(
                generations.len() <= MAX_SNAPSHOT_GENERATIONS,
                "backup snapshot generation count exceeds safety bound"
            );
        }
    }

    let mut delete_generations = HashSet::new();
    for snapshot_id in generations {
        let expired = completed
            .get(&snapshot_id)
            .copied()
            .or_else(|| snapshot_started_ms(&snapshot_id))
            .is_some_and(|timestamp| timestamp < cutoff);
        if expired {
            delete_generations.insert(snapshot_id);
        }
    }

    let mut pruned = 0u64;
    let mut listing = destination.list(Some(&snapshots_prefix));
    while let Some(meta) = listing.try_next().await? {
        if generation_from_path(&meta.location, "snapshots")
            .is_some_and(|id| delete_generations.contains(&id))
        {
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(1);
        }
    }
    let mut listing = destination.list(Some(&staging_prefix));
    while let Some(meta) = listing.try_next().await? {
        if generation_from_path(&meta.location, "staging")
            .is_some_and(|id| delete_generations.contains(&id))
        {
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(1);
        }
    }

    let refs_prefix = ObjPath::from("blob-refs");
    let mut listing = destination.list(Some(&refs_prefix));
    while let Some(meta) = listing.try_next().await? {
        anyhow::ensure!(
            meta.size <= MAX_INVENTORY_BYTES as u64,
            "blob reference is too large"
        );
        let encoded = destination.get(&meta.location).await?.bytes().await?;
        let reference: BlobReference = serde_json::from_slice(&encoded)?;
        anyhow::ensure!(
            valid_sha256(&reference.sha256)
                && reference.blob_path == blob_path_for_sha(&reference.sha256).as_ref()
                && meta.location == blob_reference_path(&reference.sha256),
            "malformed blob reference"
        );
        if reference.referenced_ms < cutoff && delete_generations.contains(&reference.snapshot_id) {
            match destination
                .delete(&ObjPath::parse(&reference.blob_path)?)
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(2);
        }
    }

    let indexes_prefix = ObjPath::from("source-index");
    let mut listing = destination.list(Some(&indexes_prefix));
    while let Some(meta) = listing.try_next().await? {
        anyhow::ensure!(
            meta.size <= MAX_INVENTORY_BYTES as u64,
            "source index is too large"
        );
        let encoded = destination.get(&meta.location).await?.bytes().await?;
        let index: SourceIndex = serde_json::from_slice(&encoded)?;
        if index.referenced_ms < cutoff && delete_generations.contains(&index.snapshot_id) {
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(1);
        }
    }
    Ok(pruned)
}

async fn scrub_blob_batch(destination: Arc<dyn ObjectStore>, limit: usize) -> anyhow::Result<u64> {
    // Walk references rather than blobs so deletion of a required blob is a
    // scrub failure rather than silently disappearing from the scan set. Do
    // not rely on provider listing order: retain the bounded lexicographically
    // smallest set after the durable cursor while scanning the full prefix.
    let prefix = ObjPath::from("blob-refs");
    let mut cursor = read_scrub_cursor(destination.clone()).await?;
    let mut wrapped = false;
    loop {
        let mut candidates = BTreeMap::new();
        let mut listing = destination.list(Some(&prefix));
        while let Some(meta) = listing.try_next().await? {
            if cursor
                .as_ref()
                .is_some_and(|offset| meta.location <= *offset)
            {
                continue;
            }
            candidates.insert(meta.location, meta.size);
            if candidates.len() > limit {
                candidates.pop_last();
            }
        }
        if candidates.is_empty() && cursor.is_some() && !wrapped {
            // Completed a full sweep. Wrap once so references inserted below
            // the previous cursor are included in the next cycle.
            cursor = None;
            wrapped = true;
            continue;
        }

        let mut checked = 0u64;
        for (location, size) in candidates {
            anyhow::ensure!(
                size <= MAX_INVENTORY_BYTES as u64,
                "blob reference is too large"
            );
            let encoded = destination.get(&location).await?.bytes().await?;
            let reference: BlobReference = serde_json::from_slice(&encoded)?;
            anyhow::ensure!(
                valid_sha256(&reference.sha256)
                    && reference.blob_path == blob_path_for_sha(&reference.sha256).as_ref()
                    && location == blob_reference_path(&reference.sha256),
                "malformed blob reference"
            );
            verify_blob_digest(
                destination.clone(),
                &ObjPath::parse(&reference.blob_path)?,
                &reference.sha256,
            )
            .await?;
            cursor = Some(location);
            checked = checked.saturating_add(1);
        }
        write_scrub_cursor(destination, cursor.as_ref()).await?;
        return Ok(checked);
    }
}

async fn read_scrub_cursor(destination: Arc<dyn ObjectStore>) -> anyhow::Result<Option<ObjPath>> {
    let encoded = match destination.get(&scrub_state_path()).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        encoded.len() <= MAX_SCRUB_STATE_BYTES,
        "backup scrub state is too large"
    );
    let state: ScrubState = serde_json::from_slice(&encoded)?;
    anyhow::ensure!(
        state.format_version == SCRUB_STATE_FORMAT_VERSION,
        "unsupported backup scrub state format"
    );
    state
        .cursor
        .map(ObjPath::parse)
        .transpose()
        .map_err(Into::into)
}

async fn write_scrub_cursor(
    destination: Arc<dyn ObjectStore>,
    cursor: Option<&ObjPath>,
) -> anyhow::Result<()> {
    let state = ScrubState {
        format_version: SCRUB_STATE_FORMAT_VERSION,
        cursor: cursor.map(ToString::to_string),
        updated_ms: now_ms(),
    };
    let encoded = serde_json::to_vec(&state)?;
    anyhow::ensure!(
        encoded.len() <= MAX_SCRUB_STATE_BYTES,
        "backup scrub state is too large"
    );
    destination
        .put(&scrub_state_path(), PutPayload::from(Bytes::from(encoded)))
        .await?;
    Ok(())
}

fn validate_inventory_record(record: &InventoryRecord, format_version: u32) -> anyhow::Result<()> {
    anyhow::ensure!(valid_sha256(&record.sha256), "invalid inventory digest");
    anyhow::ensure!(!record.backup_etag.is_empty(), "inventory ETag is empty");
    ObjPath::parse(&record.source_path)?;
    match (format_version, &record.blob_path) {
        (SNAPSHOT_FORMAT_VERSION, Some(path)) => {
            anyhow::ensure!(
                path == blob_path_for_sha(&record.sha256).as_ref(),
                "inventory blob path does not match its digest"
            );
        }
        (LEGACY_SNAPSHOT_FORMAT_VERSION, None) => {}
        _ => anyhow::bail!("inventory layout does not match snapshot format"),
    }
    Ok(())
}

fn source_index_path(role: &str, source_path: &str) -> ObjPath {
    let path_hash = Sha256::digest(format!("{role}\0{source_path}"));
    ObjPath::from(format!(
        "source-index/{role}/{}.json",
        hex_encode(&path_hash)
    ))
}

fn staging_path(snapshot_id: &str, role: &str, source_path: &str) -> ObjPath {
    let path_hash = Sha256::digest(format!("{role}\0{source_path}"));
    ObjPath::from(format!(
        "staging/{snapshot_id}/{role}/{}",
        hex_encode(&path_hash)
    ))
}

fn blob_path_for_sha(sha256: &str) -> ObjPath {
    ObjPath::from(format!("blobs/sha256/{}/{sha256}", &sha256[..2]))
}

fn blob_reference_path(sha256: &str) -> ObjPath {
    ObjPath::from(format!("blob-refs/{}/{sha256}.json", &sha256[..2]))
}

fn scrub_state_path() -> ObjPath {
    ObjPath::from("scrub-state.json")
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn object_is_outside_recovery_point(path: &ObjPath, state: &PinnedBackupState) -> bool {
    if let Some((db_path, file)) = path.as_ref().rsplit_once("/manifest/")
        && let Some(expected) = pinned_db(state, db_path)
    {
        let Some(expected) = expected else {
            return true;
        };
        let Some(id) = file
            .strip_suffix(".manifest")
            .filter(|id| id.len() == 20 && id.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            // A malformed object inside a pinned manifest namespace must never
            // be silently selected by SlateDB's latest-manifest discovery.
            return true;
        };
        return !expected.allowed_manifest_ids.contains(&id);
    }
    if let Some((db_path, file)) = path.as_ref().rsplit_once("/wal/")
        && let Some(expected) = pinned_db(state, db_path)
    {
        let Some(expected) = expected else {
            return true;
        };
        let Some(id) = file
            .strip_suffix(".sst")
            .filter(|id| id.len() == 20 && id.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            return true;
        };
        return id <= expected.replay_after_wal_id || id >= expected.next_wal_sst_id;
    }
    if let Some((db_path, file)) = path.as_ref().rsplit_once("/compactions/")
        && let Some(expected) = pinned_db(state, db_path)
    {
        let Some(expected) = expected else {
            return true;
        };
        let Some(id) = file
            .strip_suffix(".compactions")
            .filter(|id| id.len() == 20 && id.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            return true;
        };
        return Some(id) != expected.compactions_id;
    }
    if let Some((db_path, _)) = path.as_ref().rsplit_once("/compacted/")
        && pinned_db(state, db_path).is_some_and(Option::is_none)
    {
        return true;
    }
    false
}

fn pinned_db<'a>(state: &'a PinnedBackupState, path: &str) -> Option<&'a Option<PinnedDbManifest>> {
    state
        .shard_manifests
        .get(path)
        .or_else(|| state.history_manifests.get(path))
}

fn generation_from_path(path: &ObjPath, root: &str) -> Option<String> {
    path.as_ref()
        .strip_prefix(root)?
        .strip_prefix('/')?
        .split('/')
        .next()
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

fn snapshot_started_ms(snapshot_id: &str) -> Option<i64> {
    snapshot_id.split('-').next()?.parse().ok()
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
    async fn unchanged_objects_reuse_content_blobs_and_old_points_prune_safely() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(&ObjPath::from("one"), PutPayload::from_static(b"same"))
            .await
            .unwrap();
        source
            .put(&ObjPath::from("two"), PutPayload::from_static(b"other"))
            .await
            .unwrap();
        let sources = [BackupSource {
            role: "ops",
            store: source,
        }];
        let mut first = snapshot_once(&sources, backup.clone()).await.unwrap();
        let second = snapshot_once(&sources, backup.clone()).await.unwrap();
        assert_eq!(first.copied_objects, 2);
        assert_eq!(second.copied_objects, 0);
        assert_eq!(second.reused_objects, 2);
        assert_eq!(
            backup
                .list(Some(&ObjPath::from("blobs/sha256")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            2
        );

        first.completed_ms = 1;
        backup
            .put(
                &marker_path(&first.snapshot_id),
                PutPayload::from(Bytes::from(serde_json::to_vec(&first).unwrap())),
            )
            .await
            .unwrap();
        assert!(
            prune_once(backup.clone(), Duration::from_secs(24 * 60 * 60))
                .await
                .unwrap()
                > 0
        );
        assert!(
            matches!(
                backup.head(&marker_path(&first.snapshot_id)).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "expired recovery point was retained"
        );
        backup
            .head(&marker_path(&second.snapshot_id))
            .await
            .unwrap();
        assert_eq!(
            backup
                .list(Some(&ObjPath::from("blobs/sha256")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            2,
            "a blob referenced by the retained point was collected"
        );
    }

    #[tokio::test]
    async fn content_scrub_detects_blob_corruption() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(
                &ObjPath::from("one"),
                PutPayload::from_static(b"ciphertext"),
            )
            .await
            .unwrap();
        snapshot_once(
            &[BackupSource {
                role: "data",
                store: source,
            }],
            backup.clone(),
        )
        .await
        .unwrap();
        let blob = backup
            .list(Some(&ObjPath::from("blobs/sha256")))
            .try_next()
            .await
            .unwrap()
            .unwrap();
        backup
            .put(&blob.location, PutPayload::from_static(b"corruption"))
            .await
            .unwrap();
        let error = scrub_blob_batch(backup, 1).await.unwrap_err();
        assert!(error.to_string().contains("digest mismatch"));
    }

    #[tokio::test]
    async fn content_scrub_persists_progress_and_wraps() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        for (path, value) in [("one", b"a"), ("two", b"b"), ("three", b"c")] {
            source
                .put(&ObjPath::from(path), PutPayload::from_static(value))
                .await
                .unwrap();
        }
        snapshot_once(
            &[BackupSource {
                role: "data",
                store: source,
            }],
            backup.clone(),
        )
        .await
        .unwrap();
        let mut references = backup
            .list(Some(&ObjPath::from("blob-refs")))
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        references.sort();

        for expected in references.iter().chain(references.iter().take(1)) {
            assert_eq!(scrub_blob_batch(backup.clone(), 1).await.unwrap(), 1);
            let encoded = backup
                .get(&scrub_state_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let state: ScrubState = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(state.cursor.as_deref(), Some(expected.as_ref()));
        }
    }

    #[tokio::test]
    async fn live_shards_are_checkpoint_pinned_until_marker_publication() {
        let ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        ops.put(
            &ObjPath::from("topology.json"),
            PutPayload::from_static(
                br#"{"version":1,"storage_format":2,"shards":[""],"shard_paths":{}}"#,
            ),
        )
        .await
        .unwrap();
        let db = slatedb::Db::open("shards/root", shards.clone())
            .await
            .unwrap();
        db.put(b"key", b"durable").await.unwrap();
        db.close().await.unwrap();

        let report = snapshot_once_with_pins(
            &[
                BackupSource {
                    role: "ops",
                    store: ops.clone(),
                },
                BackupSource {
                    role: "shard",
                    store: shards.clone(),
                },
            ],
            backup,
            Some(&BackupPins {
                topology_store: ops,
                shard_store: shards.clone(),
                data_store: shards.clone(),
                lifetime: Duration::from_secs(60),
            }),
        )
        .await
        .unwrap();
        assert_eq!(report.pinned_shards, 1);
        assert!(
            AdminBuilder::new("shards/root", shards)
                .build()
                .list_checkpoints(None)
                .await
                .unwrap()
                .is_empty(),
            "successful backup left a checkpoint lease behind"
        );
    }

    #[tokio::test]
    async fn restore_selects_the_pinned_manifest_not_a_later_writer_manifest() {
        let ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        ops.put(
            &ObjPath::from("topology.json"),
            PutPayload::from_static(
                br#"{"version":1,"storage_format":2,"shards":[""],"shard_paths":{}}"#,
            ),
        )
        .await
        .unwrap();
        let db = slatedb::Db::open("shards/root", shards.clone())
            .await
            .unwrap();
        db.put(b"before", b"included").await.unwrap();
        db.close().await.unwrap();

        let pins = BackupPins {
            topology_store: ops.clone(),
            shard_store: shards.clone(),
            data_store: shards.clone(),
            lifetime: Duration::from_secs(60),
        };
        let snapshot_id = "00000000000000000042-pinned";
        let (state, leases) = acquire_checkpoint_leases(&pins, snapshot_id).await.unwrap();

        let db = slatedb::Db::open("shards/root", shards.clone())
            .await
            .unwrap();
        db.put(b"after", b"excluded").await.unwrap();
        db.close().await.unwrap();

        let report = snapshot_once_inner(
            &[
                BackupSource {
                    role: "ops",
                    store: ops,
                },
                BackupSource {
                    role: "shard",
                    store: shards,
                },
            ],
            backup.clone(),
            Some(&pins),
            Some(&state),
            42,
            snapshot_id,
        )
        .await
        .unwrap();
        release_checkpoint_leases(leases).await.unwrap();

        let restored_ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let restored_shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        restore_snapshot(
            backup,
            &report.snapshot_id,
            &HashMap::from([
                ("ops".to_string(), restored_ops),
                ("shard".to_string(), restored_shards.clone()),
            ]),
        )
        .await
        .unwrap();
        let restored = slatedb::Db::open("shards/root", restored_shards)
            .await
            .unwrap();
        assert_eq!(
            restored.get(b"before").await.unwrap(),
            Some(Bytes::from_static(b"included"))
        );
        assert_eq!(restored.get(b"after").await.unwrap(), None);
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_point_pins_history_after_the_absorbed_shard_cut() {
        let ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let data: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        ops.put(
            &ObjPath::from("topology.json"),
            PutPayload::from_static(
                br#"{"version":1,"storage_format":2,"shards":[""],"shard_paths":{}}"#,
            ),
        )
        .await
        .unwrap();
        let descriptor: crate::registry::StreamDesc = serde_json::from_value(serde_json::json!({
            "customer_id": "customer-a",
            "name": "history",
            "stream_epoch": "00000000000000000000000000000001",
            "key_fingerprint": "test",
            "created_ms": 1
        }))
        .unwrap();
        ops.put(
            &crate::registry::descriptor_path_for(descriptor.owner(), &descriptor.name),
            PutPayload::from(Bytes::from(serde_json::to_vec(&descriptor).unwrap())),
        )
        .await
        .unwrap();
        let history_path = recovery_history_db_path(&descriptor.storage_hash());
        let history = slatedb::Db::open(history_path.as_str(), data.clone())
            .await
            .unwrap();
        history.put(b"before", b"included").await.unwrap();
        history.close().await.unwrap();

        let pins = BackupPins {
            topology_store: ops.clone(),
            shard_store: shards.clone(),
            data_store: data.clone(),
            lifetime: Duration::from_secs(60),
        };
        let snapshot_id = "00000000000000000043-history";
        let (state, leases) = acquire_checkpoint_leases(&pins, snapshot_id).await.unwrap();
        let history = slatedb::Db::open(history_path.as_str(), data.clone())
            .await
            .unwrap();
        history.put(b"after", b"excluded").await.unwrap();
        history.close().await.unwrap();

        let report = snapshot_once_inner(
            &[
                BackupSource {
                    role: "ops",
                    store: ops,
                },
                BackupSource {
                    role: "shard",
                    store: shards,
                },
                BackupSource {
                    role: "data",
                    store: data,
                },
            ],
            backup.clone(),
            Some(&pins),
            Some(&state),
            43,
            snapshot_id,
        )
        .await
        .unwrap();
        release_checkpoint_leases(leases).await.unwrap();
        assert_eq!(report.pinned_history_dbs, 1);

        let restored_ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let restored_shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let restored_data: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        restore_snapshot(
            backup,
            &report.snapshot_id,
            &HashMap::from([
                ("ops".to_string(), restored_ops),
                ("shard".to_string(), restored_shards),
                ("data".to_string(), restored_data.clone()),
            ]),
        )
        .await
        .unwrap();
        let restored = slatedb::Db::open(history_path, restored_data)
            .await
            .unwrap();
        assert_eq!(
            restored.get(b"before").await.unwrap(),
            Some(Bytes::from_static(b"included"))
        );
        assert_eq!(restored.get(b"after").await.unwrap(), None);
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_point_closes_and_restores_external_clone_ancestry() {
        let ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let parent = slatedb::Db::open("shards/parent", shards.clone())
            .await
            .unwrap();
        parent.put(b"inherited", b"value").await.unwrap();
        parent.close().await.unwrap();
        let parent_admin = AdminBuilder::new("shards/parent", shards.clone()).build();
        let checkpoint = parent_admin
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();
        AdminBuilder::new("shards/child", shards.clone())
            .build()
            .create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint(
                "shards/parent",
                checkpoint.id,
            ))
            .build()
            .await
            .unwrap();
        parent_admin.delete_checkpoint(checkpoint.id).await.unwrap();
        ops.put(
            &ObjPath::from("topology.json"),
            PutPayload::from_static(
                br#"{"version":2,"storage_format":2,"shards":[""],"shard_paths":{"":"shards/child"}}"#,
            ),
        )
        .await
        .unwrap();

        let report = snapshot_once_with_pins(
            &[
                BackupSource {
                    role: "ops",
                    store: ops.clone(),
                },
                BackupSource {
                    role: "shard",
                    store: shards.clone(),
                },
            ],
            backup.clone(),
            Some(&BackupPins {
                topology_store: ops,
                shard_store: shards.clone(),
                data_store: shards,
                lifetime: Duration::from_secs(60),
            }),
        )
        .await
        .unwrap();
        let restored_ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let restored_shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        restore_snapshot(
            backup,
            &report.snapshot_id,
            &HashMap::from([
                ("ops".to_string(), restored_ops),
                ("shard".to_string(), restored_shards.clone()),
            ]),
        )
        .await
        .unwrap();
        let restored = slatedb::Db::open("shards/child", restored_shards)
            .await
            .unwrap();
        assert_eq!(
            restored.get(b"inherited").await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_format_snapshot_still_restores() {
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let snapshot_id = "00000000000000000001-legacy";
        let old_path = ObjPath::from(format!("snapshots/{snapshot_id}/objects/ops/one"));
        let put = backup
            .put(&old_path, PutPayload::from_static(b"legacy"))
            .await
            .unwrap();
        let record = InventoryRecord {
            role: "ops".to_string(),
            source_path: "one".to_string(),
            size: 6,
            sha256: hex_encode(&Sha256::digest(b"legacy")),
            backup_etag: put.e_tag.unwrap(),
            blob_path: None,
        };
        let inventory = serde_json::to_vec(&record).unwrap();
        backup
            .put(
                &inventory_path(snapshot_id, "ops", "one"),
                PutPayload::from(Bytes::from(inventory.clone())),
            )
            .await
            .unwrap();
        let report = SnapshotReport {
            format_version: LEGACY_SNAPSHOT_FORMAT_VERSION,
            snapshot_id: snapshot_id.to_string(),
            started_ms: 1,
            completed_ms: 2,
            objects: 1,
            bytes: 6,
            roles: vec!["ops".to_string()],
            inventory_checksum: hex_encode(&Sha256::digest(&inventory)),
            copied_objects: 0,
            copied_bytes: 0,
            reused_objects: 0,
            pinned_shards: 0,
            pinned_history_dbs: 0,
        };
        backup
            .put(
                &marker_path(snapshot_id),
                PutPayload::from(Bytes::from(serde_json::to_vec(&report).unwrap())),
            )
            .await
            .unwrap();
        let target: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        assert_eq!(
            restore_snapshot(
                backup,
                snapshot_id,
                &HashMap::from([("ops".to_string(), target.clone())]),
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            target
                .get(&ObjPath::from("one"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"legacy")
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
