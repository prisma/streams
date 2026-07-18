//! Provider-independent object-store snapshots and restore.
//!
//! Live SlateDB databases are pinned with expiring checkpoints while a
//! snapshot is taken. Source ETags feed a durable incremental index and
//! immutable SHA-256 blobs, so unchanged ciphertext/control objects are not
//! copied again. A checksummed inventory is published before `_complete.json`;
//! partial or corrupt snapshots are never restorable.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, PutResult,
    UpdateVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::admin::{Admin, AdminBuilder};
use slatedb::config::CheckpointOptions;

const SNAPSHOT_FORMAT_VERSION: u32 = 3;
const CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION: u32 = 2;
const LEGACY_SNAPSHOT_FORMAT_VERSION: u32 = 1;
const COPY_PART_BYTES: usize = 8 * 1024 * 1024;
const MAX_INVENTORY_BYTES: usize = 16 * 1024;
const MAX_TOPOLOGY_BYTES: usize = 4 * 1024 * 1024;
const MAX_PINNED_SHARDS: usize = 16_384;
const MAX_SNAPSHOT_GENERATIONS: usize = 100_000;
const SCRUB_STATE_FORMAT_VERSION: u32 = 1;
const GC_INTENT_FORMAT_VERSION: u32 = 1;
const RETENTION_CLOCK_FORMAT_VERSION: u32 = 1;
const MAX_SCRUB_STATE_BYTES: usize = 4 * 1024;
const COORDINATOR_FORMAT_VERSION: u32 = 2;
const COORDINATOR_LEASE_DURATION: Duration = Duration::from_secs(6);
const COORDINATOR_RENEW_MS: u64 = 2_000;
const MAX_COORDINATOR_DOCUMENT_BYTES: usize = 16 * 1024;

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

#[derive(Clone)]
pub struct BackupCoordinator {
    pub store: Arc<dyn ObjectStore>,
    pub owner: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupWriteFormat {
    V2,
    V3,
}

impl BackupWriteFormat {
    fn version(self) -> u32 {
        match self {
            Self::V2 => CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION,
            Self::V3 => SNAPSHOT_FORMAT_VERSION,
        }
    }

    fn content_epoch(self, coordinator_epoch: u64) -> u64 {
        match self {
            Self::V2 => 0,
            Self::V3 => coordinator_epoch,
        }
    }
}

impl TryFrom<u32> for BackupWriteFormat {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION => Ok(Self::V2),
            SNAPSHOT_FORMAT_VERSION => Ok(Self::V3),
            _ => anyhow::bail!("BACKUP_WRITE_FORMAT must be 2 or 3"),
        }
    }
}

pub struct BackupConfig {
    pub sources: Vec<BackupSource>,
    pub destination: Arc<dyn ObjectStore>,
    pub interval: Duration,
    pub retention: Duration,
    pub scrub_interval: Duration,
    pub scrub_objects_per_interval: usize,
    pub primary_scrub_interval: Duration,
    pub primary_scrub_objects_per_interval: usize,
    pub primary_scrub_max_object_bytes: u64,
    pub pins: Option<BackupPins>,
    pub coordinator: Option<BackupCoordinator>,
    pub write_format: BackupWriteFormat,
}

pub struct BackupStatus {
    snapshot_healthy: AtomicBool,
    scrub_healthy: AtomicBool,
    primary_scrub_healthy: AtomicBool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackupHealth {
    pub snapshot: bool,
    pub recovery_scrub: bool,
    pub primary_scrub: bool,
}

impl BackupStatus {
    pub fn ready(&self) -> bool {
        self.snapshot_healthy.load(Ordering::Acquire)
            && self.scrub_healthy.load(Ordering::Acquire)
            && self.primary_scrub_healthy.load(Ordering::Acquire)
    }

    pub fn health(&self) -> BackupHealth {
        BackupHealth {
            snapshot: self.snapshot_healthy.load(Ordering::Acquire),
            recovery_scrub: self.scrub_healthy.load(Ordering::Acquire),
            primary_scrub: self.primary_scrub_healthy.load(Ordering::Acquire),
        }
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
    #[serde(default)]
    pub coordinator_epoch: u64,
    #[serde(default)]
    pub coordinator_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    #[serde(default)]
    coordinator_epoch: u64,
    #[serde(default)]
    coordinator_sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct BlobReference {
    sha256: String,
    blob_path: String,
    snapshot_id: String,
    referenced_ms: i64,
    #[serde(default)]
    coordinator_epoch: u64,
    #[serde(default)]
    coordinator_sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ScrubState {
    format_version: u32,
    /// Last successfully verified reference in a provider-independent,
    /// lexicographically ordered sweep. Persisting this prevents process
    /// restarts from starving the high end of a large recovery corpus.
    cursor: Option<String>,
    updated_ms: i64,
    #[serde(default)]
    coordinator_epoch: u64,
    #[serde(default)]
    coordinator_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GcIntent {
    format_version: u32,
    snapshot_id: String,
    coordinator_epoch: u64,
    created_ms: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct RetentionClockProbe {
    format_version: u32,
    token: String,
    coordinator_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CoordinatorLease {
    format_version: u32,
    owner: String,
    token: String,
    epoch: u64,
    renewal_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyCoordinatorLease {
    format_version: u32,
    owner: String,
    token: String,
    epoch: u64,
    lease_until_ms: i64,
}

enum DecodedCoordinatorLease {
    Current(CoordinatorLease),
    Legacy(LegacyCoordinatorLease),
}

struct LeaseObservation {
    identity: String,
    token: Option<String>,
    epoch: u64,
    renewal_sequence: u64,
    /// The first renewal sequence observed after startup, takeover, or an
    /// interval in which the previous lease version became stale. Health
    /// published before this sequence cannot prove that the current owner is
    /// alive, even when its wall-clock timestamp looks recent.
    confirmed_since_sequence: Option<u64>,
    first_seen: Instant,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CoordinatorHealth {
    format_version: u32,
    lease_epoch: u64,
    #[serde(default)]
    lease_renewal_sequence: u64,
    sequence: u64,
    generated_ms: i64,
    latest_completed_ms: i64,
    last_scrub_ms: i64,
    snapshot_healthy: bool,
    scrub_healthy: bool,
    #[serde(default = "max_u64")]
    snapshot_age_ms: u64,
    #[serde(default = "max_u64")]
    scrub_age_ms: u64,
    #[serde(default)]
    last_primary_scrub_ms: i64,
    #[serde(default)]
    primary_scrub_healthy: bool,
    #[serde(default = "max_u64")]
    primary_scrub_age_ms: u64,
}

const fn max_u64() -> u64 {
    u64::MAX
}

struct CoordinatorState {
    config: BackupCoordinator,
    token: String,
    owned: AtomicBool,
    epoch: AtomicU64,
    renewal_sequence: AtomicU64,
    last_renewed: Mutex<Option<Instant>>,
    lease_observation: Mutex<Option<LeaseObservation>>,
    mutation_sequence: AtomicU64,
}

#[derive(Clone)]
struct PublicationFence {
    state: Arc<CoordinatorState>,
    epoch: u64,
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
    /// WALs acknowledged after the pinned manifest watermark are not covered
    /// by SlateDB's detached checkpoint. Copy them to immutable backup content
    /// as soon as each database cut is observed, before source WAL GC can race
    /// the slower fleet-wide inventory walk.
    protected_wals: Vec<ProtectedWal>,
}

struct SnapshotContext<'a> {
    pins: Option<&'a BackupPins>,
    pinned_state: Option<&'a PinnedBackupState>,
    started_ms: i64,
    snapshot_id: &'a str,
    fence: Option<&'a PublicationFence>,
    write_format: BackupWriteFormat,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
}

#[derive(Clone)]
struct PinnedDbManifest {
    manifest_id: u64,
    allowed_manifest_ids: HashSet<u64>,
    /// Exact WAL objects visible at the shard cut, including acknowledged
    /// WALs whose IDs have not yet reached the remotely persisted manifest.
    /// SlateDB replays these on normal open by listing above the replay
    /// watermark. ETags make the extended cut immutable across the copy.
    wal_etags: HashMap<u64, String>,
    /// WAL IDs at or above this value were visible in object storage but had
    /// not yet reached the pinned remote manifest. They are protected eagerly
    /// in the recovery provider rather than relying on source WAL retention.
    first_unmanifested_wal_id: u64,
    compactions_id: Option<u64>,
}

struct ProtectedWal {
    source_etag: String,
    record: InventoryRecord,
    reused: bool,
}

#[derive(Default)]
struct SnapshotProgress {
    objects: u64,
    bytes: u64,
    copied_objects: u64,
    copied_bytes: u64,
    reused_objects: u64,
    inventory_checksum: [u8; 32],
}

impl CoordinatorState {
    fn fence(self: &Arc<Self>) -> Option<PublicationFence> {
        let epoch = self.epoch.load(Ordering::Acquire);
        (self.owned.load(Ordering::Acquire) && epoch > 0 && self.local_lease_is_fresh()).then(
            || PublicationFence {
                state: self.clone(),
                epoch,
            },
        )
    }

    fn local_lease_is_fresh(&self) -> bool {
        self.last_renewed
            .lock()
            .expect("coordinator renewal lock poisoned")
            .is_some_and(|renewed| renewed.elapsed() < COORDINATOR_LEASE_DURATION)
    }

    fn note_renewed(&self, renewal_sequence: u64) {
        self.renewal_sequence
            .store(renewal_sequence, Ordering::Release);
        *self
            .last_renewed
            .lock()
            .expect("coordinator renewal lock poisoned") = Some(Instant::now());
        *self
            .lease_observation
            .lock()
            .expect("coordinator observation lock poisoned") = None;
    }

    fn clear_renewal(&self) {
        *self
            .last_renewed
            .lock()
            .expect("coordinator renewal lock poisoned") = None;
        self.renewal_sequence.store(0, Ordering::Release);
    }

    /// Returns true only after the exact remote lease version has remained
    /// unchanged for one complete local monotonic lease interval.
    fn remote_lease_is_stale(&self, identity: String, current: Option<(&str, u64, u64)>) -> bool {
        let mut observation = self
            .lease_observation
            .lock()
            .expect("coordinator observation lock poisoned");
        match observation.as_mut() {
            Some(current) if current.identity == identity => {
                current.first_seen.elapsed() >= COORDINATOR_LEASE_DURATION
            }
            _ => {
                let confirmed_since_sequence = match (observation.as_ref(), current) {
                    (Some(previous), Some((token, epoch, renewal_sequence)))
                        if previous.token.as_deref() == Some(token)
                            && previous.epoch == epoch
                            && renewal_sequence > previous.renewal_sequence =>
                    {
                        if previous.first_seen.elapsed() < COORDINATOR_LEASE_DURATION {
                            previous.confirmed_since_sequence.or(Some(renewal_sequence))
                        } else {
                            Some(renewal_sequence)
                        }
                    }
                    _ => None,
                };
                *observation = Some(LeaseObservation {
                    identity,
                    token: current.map(|(token, _, _)| token.to_string()),
                    epoch: current.map_or(0, |(_, epoch, _)| epoch),
                    renewal_sequence: current.map_or(0, |(_, _, sequence)| sequence),
                    confirmed_since_sequence,
                    first_seen: Instant::now(),
                });
                false
            }
        }
    }

    fn remote_lease_confirmation(&self, identity: &str) -> Option<u64> {
        self.lease_observation
            .lock()
            .expect("coordinator observation lock poisoned")
            .as_ref()
            .and_then(|observed| {
                (observed.identity == identity
                    && observed.first_seen.elapsed() < COORDINATOR_LEASE_DURATION)
                    .then_some(observed.confirmed_since_sequence)
                    .flatten()
            })
    }
}

impl PublicationFence {
    fn check_local(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state.owned.load(Ordering::Acquire)
                && self.state.epoch.load(Ordering::Acquire) == self.epoch
                && self.state.local_lease_is_fresh(),
            "backup coordinator lease was lost"
        );
        Ok(())
    }

    fn next_mutation_order(&self) -> anyhow::Result<(u64, u64)> {
        self.check_local()?;
        let previous = self
            .state
            .mutation_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| anyhow::anyhow!("backup mutation sequence exhausted"))?;
        Ok((self.epoch, previous + 1))
    }

    async fn verify_remote(&self) -> anyhow::Result<()> {
        self.check_local()?;
        let encoded = self
            .state
            .config
            .store
            .get(&coordinator_lease_path())
            .await?
            .bytes()
            .await?;
        anyhow::ensure!(
            encoded.len() <= MAX_COORDINATOR_DOCUMENT_BYTES,
            "backup coordinator lease is too large"
        );
        let DecodedCoordinatorLease::Current(lease) = decode_coordinator_lease(&encoded)? else {
            anyhow::bail!("backup coordinator still uses the legacy lease protocol");
        };
        anyhow::ensure!(
            coordinator_lease_is_valid(&lease)
                && lease.owner == self.state.config.owner
                && lease.token == self.state.token
                && lease.epoch == self.epoch,
            "backup coordinator lease was lost"
        );
        Ok(())
    }
}

fn valid_coordinator_owner(owner: &str) -> bool {
    !owner.is_empty()
        && owner.len() <= 128
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn coordinator_lease_is_valid(lease: &CoordinatorLease) -> bool {
    lease.format_version == COORDINATOR_FORMAT_VERSION
        && valid_coordinator_owner(&lease.owner)
        && lease.token.len() == 32
        && lease.token.bytes().all(|byte| byte.is_ascii_hexdigit())
        && lease.epoch > 0
        && lease.renewal_sequence > 0
}

fn legacy_coordinator_lease_is_valid(lease: &LegacyCoordinatorLease) -> bool {
    lease.format_version == 1
        && valid_coordinator_owner(&lease.owner)
        && lease.token.len() == 32
        && lease.token.bytes().all(|byte| byte.is_ascii_hexdigit())
        && lease.epoch > 0
        && lease.lease_until_ms > 0
}

fn decode_coordinator_lease(encoded: &[u8]) -> anyhow::Result<DecodedCoordinatorLease> {
    #[derive(Deserialize)]
    struct Header {
        format_version: u32,
    }
    match serde_json::from_slice::<Header>(encoded)?.format_version {
        COORDINATOR_FORMAT_VERSION => {
            let lease: CoordinatorLease = serde_json::from_slice(encoded)?;
            anyhow::ensure!(
                coordinator_lease_is_valid(&lease),
                "malformed backup coordinator lease"
            );
            Ok(DecodedCoordinatorLease::Current(lease))
        }
        1 => {
            let lease: LegacyCoordinatorLease = serde_json::from_slice(encoded)?;
            anyhow::ensure!(
                legacy_coordinator_lease_is_valid(&lease),
                "malformed legacy backup coordinator lease"
            );
            Ok(DecodedCoordinatorLease::Legacy(lease))
        }
        _ => anyhow::bail!("unsupported backup coordinator lease protocol"),
    }
}

fn coordinator_lease_identity(
    lease: &DecodedCoordinatorLease,
    encoded: &[u8],
    e_tag: Option<&str>,
    version: Option<&str>,
) -> String {
    let (protocol, token, epoch, sequence) = match lease {
        DecodedCoordinatorLease::Current(lease) => (
            lease.format_version,
            lease.token.as_str(),
            lease.epoch,
            lease.renewal_sequence,
        ),
        DecodedCoordinatorLease::Legacy(lease) => {
            (lease.format_version, lease.token.as_str(), lease.epoch, 0)
        }
    };
    let content_sha256 = hex_encode(&Sha256::digest(encoded));
    format!(
        "{protocol}:{token}:{epoch}:{sequence}:{content_sha256}:{}:{}",
        e_tag.unwrap_or(""),
        version.unwrap_or("")
    )
}

fn coordinator_token() -> String {
    let mut token = [0u8; 16];
    use rand::RngCore;
    rand::rng().fill_bytes(&mut token);
    crate::crypto::hex(&token)
}

async fn claim_coordinator(state: &CoordinatorState) -> anyhow::Result<Option<CoordinatorLease>> {
    anyhow::ensure!(
        valid_coordinator_owner(&state.config.owner),
        "invalid backup coordinator owner"
    );
    let path = coordinator_lease_path();
    for _ in 0..5 {
        match state.config.store.get(&path).await {
            Ok(result) => {
                let meta = result.meta.clone();
                let version = UpdateVersion {
                    e_tag: meta.e_tag.clone(),
                    version: meta.version.clone(),
                };
                let encoded = result.bytes().await?;
                anyhow::ensure!(
                    encoded.len() <= MAX_COORDINATOR_DOCUMENT_BYTES,
                    "backup coordinator lease is too large"
                );
                let current = decode_coordinator_lease(&encoded)?;
                let identity = coordinator_lease_identity(
                    &current,
                    &encoded,
                    meta.e_tag.as_deref(),
                    meta.version.as_deref(),
                );
                let (epoch, renewal_sequence) = match &current {
                    DecodedCoordinatorLease::Current(current) if current.token == state.token => {
                        anyhow::ensure!(
                            current.owner == state.config.owner,
                            "backup coordinator token changed owner"
                        );
                        (
                            current.epoch,
                            current.renewal_sequence.checked_add(1).ok_or_else(|| {
                                anyhow::anyhow!("backup coordinator renewal sequence exhausted")
                            })?,
                        )
                    }
                    DecodedCoordinatorLease::Current(current) => {
                        if !state.remote_lease_is_stale(
                            identity,
                            Some((&current.token, current.epoch, current.renewal_sequence)),
                        ) {
                            return Ok(None);
                        }
                        (
                            current.epoch.checked_add(1).ok_or_else(|| {
                                anyhow::anyhow!("backup coordinator epoch exhausted")
                            })?,
                            1,
                        )
                    }
                    DecodedCoordinatorLease::Legacy(current) => {
                        if !state.remote_lease_is_stale(identity, None) {
                            return Ok(None);
                        }
                        (
                            current.epoch.checked_add(1).ok_or_else(|| {
                                anyhow::anyhow!("backup coordinator epoch exhausted")
                            })?,
                            1,
                        )
                    }
                };
                let next = CoordinatorLease {
                    format_version: COORDINATOR_FORMAT_VERSION,
                    owner: state.config.owner.clone(),
                    token: state.token.clone(),
                    epoch,
                    renewal_sequence,
                };
                match state
                    .config
                    .store
                    .put_opts(
                        &path,
                        PutPayload::from(Bytes::from(serde_json::to_vec(&next)?)),
                        PutOptions::from(PutMode::Update(version)),
                    )
                    .await
                {
                    Ok(_) => {
                        state.note_renewed(next.renewal_sequence);
                        return Ok(Some(next));
                    }
                    Err(object_store::Error::Precondition { .. }) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                let lease = CoordinatorLease {
                    format_version: COORDINATOR_FORMAT_VERSION,
                    owner: state.config.owner.clone(),
                    token: state.token.clone(),
                    epoch: 1,
                    renewal_sequence: 1,
                };
                match state
                    .config
                    .store
                    .put_opts(
                        &path,
                        PutPayload::from(Bytes::from(serde_json::to_vec(&lease)?)),
                        PutOptions::from(PutMode::Create),
                    )
                    .await
                {
                    Ok(_) => {
                        state.note_renewed(lease.renewal_sequence);
                        return Ok(Some(lease));
                    }
                    Err(object_store::Error::AlreadyExists { .. }) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("backup coordinator lease CAS retries exhausted")
}

fn start_coordinator(config: BackupCoordinator) -> Arc<CoordinatorState> {
    let state = Arc::new(CoordinatorState {
        config,
        token: coordinator_token(),
        owned: AtomicBool::new(false),
        epoch: AtomicU64::new(0),
        renewal_sequence: AtomicU64::new(0),
        last_renewed: Mutex::new(None),
        lease_observation: Mutex::new(None),
        mutation_sequence: AtomicU64::new(0),
    });
    let renew = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(COORDINATOR_RENEW_MS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match claim_coordinator(&renew).await {
                Ok(Some(lease)) => {
                    renew.epoch.store(lease.epoch, Ordering::Release);
                    renew.owned.store(true, Ordering::Release);
                }
                Ok(None) => {
                    renew.owned.store(false, Ordering::Release);
                    renew.clear_renewal();
                }
                Err(error) => {
                    renew.owned.store(false, Ordering::Release);
                    renew.clear_renewal();
                    tracing::error!("backup coordinator lease failed: {error:#}");
                }
            }
        }
    });
    state
}

fn leadership_fence(
    coordinator: Option<&Arc<CoordinatorState>>,
) -> Option<Option<PublicationFence>> {
    match coordinator {
        Some(coordinator) => coordinator.fence().map(Some),
        None => Some(None),
    }
}

async fn publish_coordinator_health(
    fence: &PublicationFence,
    health: &CoordinatorHealth,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        health.format_version == COORDINATOR_FORMAT_VERSION
            && health.lease_epoch == fence.epoch
            && health.lease_renewal_sequence > 0
            && health.sequence > 0
            && health.generated_ms > 0,
        "invalid backup coordinator health"
    );
    fence.verify_remote().await?;
    let path = coordinator_health_path();
    let encoded = serde_json::to_vec(health)?;
    anyhow::ensure!(
        encoded.len() <= MAX_COORDINATOR_DOCUMENT_BYTES,
        "backup coordinator health is too large"
    );
    for _ in 0..5 {
        let mode = match fence.state.config.store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let current_bytes = result.bytes().await?;
                anyhow::ensure!(
                    current_bytes.len() <= MAX_COORDINATOR_DOCUMENT_BYTES,
                    "backup coordinator health is too large"
                );
                let current: CoordinatorHealth = serde_json::from_slice(&current_bytes)?;
                let current_order = (current.lease_epoch, current.sequence);
                let next_order = (health.lease_epoch, health.sequence);
                anyhow::ensure!(
                    current_order <= next_order,
                    "backup coordinator health publication was fenced"
                );
                if current_order == next_order {
                    anyhow::ensure!(
                        current.format_version == health.format_version
                            && current.generated_ms == health.generated_ms
                            && current.latest_completed_ms == health.latest_completed_ms
                            && current.last_scrub_ms == health.last_scrub_ms
                            && current.snapshot_healthy == health.snapshot_healthy
                            && current.scrub_healthy == health.scrub_healthy
                            && current.last_primary_scrub_ms == health.last_primary_scrub_ms
                            && current.primary_scrub_healthy == health.primary_scrub_healthy
                            && current.lease_renewal_sequence == health.lease_renewal_sequence
                            && current.snapshot_age_ms == health.snapshot_age_ms
                            && current.scrub_age_ms == health.scrub_age_ms
                            && current.primary_scrub_age_ms == health.primary_scrub_age_ms,
                        "conflicting backup coordinator health sequence"
                    );
                    return Ok(());
                }
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        fence.verify_remote().await?;
        match fence
            .state
            .config
            .store
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded.clone())),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("backup coordinator health CAS retries exhausted")
}

async fn load_coordinator_health(
    coordinator: &CoordinatorState,
    snapshot_interval: Duration,
    scrub_interval: Duration,
    primary_scrub_interval: Duration,
) -> anyhow::Result<CoordinatorHealth> {
    let lease_result = coordinator
        .config
        .store
        .get(&coordinator_lease_path())
        .await?;
    let lease_meta = lease_result.meta.clone();
    let lease_bytes = lease_result.bytes().await?;
    anyhow::ensure!(
        lease_bytes.len() <= MAX_COORDINATOR_DOCUMENT_BYTES,
        "backup coordinator lease is too large"
    );
    let decoded_lease = decode_coordinator_lease(&lease_bytes)?;
    let DecodedCoordinatorLease::Current(lease) = &decoded_lease else {
        anyhow::bail!("backup coordinator still uses the legacy lease protocol");
    };
    let lease_identity = coordinator_lease_identity(
        &decoded_lease,
        &lease_bytes,
        lease_meta.e_tag.as_deref(),
        lease_meta.version.as_deref(),
    );
    let local_leader = coordinator.owned.load(Ordering::Acquire)
        && lease.token == coordinator.token
        && lease.epoch == coordinator.epoch.load(Ordering::Acquire)
        && coordinator.local_lease_is_fresh();
    let confirmed_since_sequence = if local_leader {
        Some(lease.renewal_sequence)
    } else {
        coordinator.remote_lease_confirmation(&lease_identity)
    };
    anyhow::ensure!(
        coordinator_lease_is_valid(lease) && confirmed_since_sequence.is_some(),
        "backup coordinator lease has not proven a recent renewal"
    );
    let encoded = coordinator
        .config
        .store
        .get(&coordinator_health_path())
        .await?
        .bytes()
        .await?;
    anyhow::ensure!(
        encoded.len() <= MAX_COORDINATOR_DOCUMENT_BYTES,
        "backup coordinator health is too large"
    );
    let mut health: CoordinatorHealth = serde_json::from_slice(&encoded)?;
    let confirmed_since_sequence = confirmed_since_sequence.expect("checked above");
    anyhow::ensure!(
        health.format_version == COORDINATOR_FORMAT_VERSION
            && health.lease_epoch == lease.epoch
            && health.lease_renewal_sequence >= confirmed_since_sequence
            && health.lease_renewal_sequence <= lease.renewal_sequence
            && health.sequence > 0
            && health.generated_ms > 0
            && (!health.snapshot_healthy || health.latest_completed_ms > 0)
            && (!health.scrub_healthy || health.last_scrub_ms > 0)
            && (!health.primary_scrub_healthy || health.last_primary_scrub_ms > 0),
        "backup coordinator health is malformed or from another epoch"
    );
    // A publication carrying sequence S happened after lease renewal S. Every
    // subsequent live lease version is observed for less than the full lease
    // interval. Using one complete interval per version (including S) is a
    // conservative, clock-independent upper bound on publication age.
    let renewal_versions = lease
        .renewal_sequence
        .saturating_sub(health.lease_renewal_sequence)
        .saturating_add(1);
    let publication_age_upper_ms =
        duration_ms_u64(COORDINATOR_LEASE_DURATION).saturating_mul(renewal_versions);
    let snapshot_interval = snapshot_interval.max(Duration::from_secs(60));
    let scrub_interval = scrub_interval.max(Duration::from_secs(10));
    let primary_scrub_interval = primary_scrub_interval.max(Duration::from_secs(10));
    let snapshot_budget =
        duration_ms_u64(snapshot_interval.saturating_mul(2)).saturating_add(60_000);
    let scrub_budget = duration_ms_u64(scrub_interval.saturating_mul(3)).saturating_add(10_000);
    let primary_scrub_budget =
        duration_ms_u64(primary_scrub_interval.saturating_mul(3)).saturating_add(10_000);
    let report_budget = scrub_budget.max(primary_scrub_budget).max(duration_ms_u64(
        COORDINATOR_LEASE_DURATION.saturating_mul(2),
    ));
    health.snapshot_healthy &= health
        .snapshot_age_ms
        .saturating_add(publication_age_upper_ms)
        <= snapshot_budget;
    health.scrub_healthy &=
        health.scrub_age_ms.saturating_add(publication_age_upper_ms) <= scrub_budget;
    health.primary_scrub_healthy &= health
        .primary_scrub_age_ms
        .saturating_add(publication_age_upper_ms)
        <= primary_scrub_budget;
    anyhow::ensure!(
        publication_age_upper_ms <= report_budget,
        "backup coordinator health is stale"
    );
    Ok(health)
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn monotonic_age_ms(at: Option<Instant>) -> u64 {
    at.map_or(u64::MAX, |at| duration_ms_u64(at.elapsed()))
}

pub fn start(config: BackupConfig) -> Arc<BackupStatus> {
    // A configured backup is not healthy until at least one marker-last
    // snapshot has actually completed.
    let status = Arc::new(BackupStatus {
        snapshot_healthy: AtomicBool::new(false),
        scrub_healthy: AtomicBool::new(false),
        primary_scrub_healthy: AtomicBool::new(config.pins.is_none()),
    });
    let coordinator = config.coordinator.clone().map(start_coordinator);
    let actor_status = status.clone();
    tokio::spawn(async move {
        let mut snapshot_tick = tokio::time::interval(config.interval.max(Duration::from_secs(60)));
        let mut scrub_tick =
            tokio::time::interval(config.scrub_interval.max(Duration::from_secs(10)));
        let mut primary_scrub_tick =
            tokio::time::interval(config.primary_scrub_interval.max(Duration::from_secs(10)));
        let mut coordinator_tick = tokio::time::interval(Duration::from_secs(1));
        let mut active_epoch = 0u64;
        let mut snapshot_sequence = 0u64;
        let mut health_sequence = 0u64;
        let mut latest_completed_ms = 0i64;
        let mut last_scrub_ms = 0i64;
        let mut last_primary_scrub_ms = 0i64;
        let mut latest_completed_at = None;
        let mut last_scrub_at = None;
        let mut last_primary_scrub_at = None;
        let mut was_leader = coordinator.is_none();
        loop {
            tokio::select! {
                _ = snapshot_tick.tick() => {
                    let Some(fence) = leadership_fence(coordinator.as_ref()) else {
                        continue;
                    };
                    let epoch = fence.as_ref().map_or(0, |fence| fence.epoch);
                    if epoch != active_epoch {
                        active_epoch = epoch;
                        snapshot_sequence = 0;
                        health_sequence = 0;
                        latest_completed_ms = 0;
                        last_scrub_ms = 0;
                        last_primary_scrub_ms = 0;
                        last_scrub_at = None;
                        last_primary_scrub_at = None;
                        actor_status.snapshot_healthy.store(false, Ordering::Release);
                        actor_status.scrub_healthy.store(false, Ordering::Release);
                        actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                    }
                    snapshot_sequence = snapshot_sequence.saturating_add(1).max(1);
                    let result = snapshot_once_with_pins_fenced(
                        &config.sources,
                        config.destination.clone(),
                        config.pins.as_ref(),
                        fence.as_ref(),
                        config.write_format,
                        epoch,
                        snapshot_sequence,
                    ).await;
                    match result {
                        Ok(report) => {
                            match prune_once_fenced(
                                config.destination.clone(),
                                config.retention,
                                fence.as_ref(),
                                config.write_format == BackupWriteFormat::V3,
                            ).await {
                                Ok(pruned) => {
                                    let primary_healthy = actor_status
                                        .primary_scrub_healthy
                                        .load(Ordering::Acquire);
                                    actor_status
                                        .snapshot_healthy
                                        .store(primary_healthy, Ordering::Release);
                                    if primary_healthy {
                                        latest_completed_ms = report.completed_ms;
                                        latest_completed_at = Some(Instant::now());
                                    } else {
                                        latest_completed_at = None;
                                    }
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
                                    latest_completed_at = None;
                                    tracing::error!("backup retention failed: {error:#}");
                                }
                            }
                        }
                        Err(error) => {
                            actor_status.snapshot_healthy.store(false, Ordering::Release);
                            latest_completed_at = None;
                            tracing::error!("backup snapshot failed: {error:#}");
                        }
                    }
                    if let Some(fence) = fence.as_ref() {
                        health_sequence = health_sequence.saturating_add(1).max(1);
                        let health = CoordinatorHealth {
                            format_version: COORDINATOR_FORMAT_VERSION,
                            lease_epoch: fence.epoch,
                            lease_renewal_sequence: fence.state.renewal_sequence.load(Ordering::Acquire),
                            sequence: health_sequence,
                            generated_ms: now_ms(),
                            latest_completed_ms,
                            last_scrub_ms,
                            snapshot_healthy: actor_status.snapshot_healthy.load(Ordering::Acquire),
                            scrub_healthy: actor_status.scrub_healthy.load(Ordering::Acquire),
                            snapshot_age_ms: monotonic_age_ms(latest_completed_at),
                            scrub_age_ms: monotonic_age_ms(last_scrub_at),
                            last_primary_scrub_ms,
                            primary_scrub_healthy: actor_status.primary_scrub_healthy.load(Ordering::Acquire),
                            primary_scrub_age_ms: monotonic_age_ms(last_primary_scrub_at),
                        };
                        if let Err(error) = publish_coordinator_health(fence, &health).await {
                            actor_status.snapshot_healthy.store(false, Ordering::Release);
                            actor_status.scrub_healthy.store(false, Ordering::Release);
                            actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                            tracing::error!("backup coordinator health publication failed: {error:#}");
                        }
                    }
                }
                _ = scrub_tick.tick() => {
                    let Some(fence) = leadership_fence(coordinator.as_ref()) else {
                        continue;
                    };
                    let epoch = fence.as_ref().map_or(0, |fence| fence.epoch);
                    if epoch != active_epoch {
                        active_epoch = epoch;
                        snapshot_sequence = 0;
                        health_sequence = 0;
                        latest_completed_ms = 0;
                        last_scrub_ms = 0;
                        last_primary_scrub_ms = 0;
                        latest_completed_at = None;
                        last_primary_scrub_at = None;
                        actor_status.snapshot_healthy.store(false, Ordering::Release);
                        actor_status.scrub_healthy.store(false, Ordering::Release);
                        actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                    }
                    match scrub_blob_batch(
                        config.destination.clone(),
                        config.scrub_objects_per_interval.max(1),
                        fence.as_ref(),
                    ).await {
                        Ok(checked) => {
                            actor_status.scrub_healthy.store(true, Ordering::Release);
                            last_scrub_ms = now_ms();
                            last_scrub_at = Some(Instant::now());
                            tracing::info!(checked, "backup content scrub batch complete");
                        }
                        Err(error) => {
                            actor_status.scrub_healthy.store(false, Ordering::Release);
                            last_scrub_at = None;
                            tracing::error!("backup content scrub failed: {error:#}");
                        }
                    }
                    if let Some(fence) = fence.as_ref() {
                        health_sequence = health_sequence.saturating_add(1).max(1);
                        let health = CoordinatorHealth {
                            format_version: COORDINATOR_FORMAT_VERSION,
                            lease_epoch: fence.epoch,
                            lease_renewal_sequence: fence.state.renewal_sequence.load(Ordering::Acquire),
                            sequence: health_sequence,
                            generated_ms: now_ms(),
                            latest_completed_ms,
                            last_scrub_ms,
                            snapshot_healthy: actor_status.snapshot_healthy.load(Ordering::Acquire),
                            scrub_healthy: actor_status.scrub_healthy.load(Ordering::Acquire),
                            snapshot_age_ms: monotonic_age_ms(latest_completed_at),
                            scrub_age_ms: monotonic_age_ms(last_scrub_at),
                            last_primary_scrub_ms,
                            primary_scrub_healthy: actor_status.primary_scrub_healthy.load(Ordering::Acquire),
                            primary_scrub_age_ms: monotonic_age_ms(last_primary_scrub_at),
                        };
                        if let Err(error) = publish_coordinator_health(fence, &health).await {
                            actor_status.snapshot_healthy.store(false, Ordering::Release);
                            actor_status.scrub_healthy.store(false, Ordering::Release);
                            actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                            tracing::error!("backup coordinator health publication failed: {error:#}");
                        }
                    }
                }
                _ = primary_scrub_tick.tick() => {
                    let Some(fence) = leadership_fence(coordinator.as_ref()) else {
                        continue;
                    };
                    let epoch = fence.as_ref().map_or(0, |fence| fence.epoch);
                    if epoch != active_epoch {
                        active_epoch = epoch;
                        snapshot_sequence = 0;
                        health_sequence = 0;
                        latest_completed_ms = 0;
                        last_scrub_ms = 0;
                        last_primary_scrub_ms = 0;
                        latest_completed_at = None;
                        last_scrub_at = None;
                        actor_status.snapshot_healthy.store(false, Ordering::Release);
                        actor_status.scrub_healthy.store(false, Ordering::Release);
                        actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                    }
                    let result = match config.pins.as_ref() {
                        Some(pins) => {
                            let order = match fence.as_ref() {
                                Some(fence) => fence.next_mutation_order(),
                                None => Ok((0, 0)),
                            };
                            match order {
                                Ok((coordinator_epoch, coordinator_sequence)) => {
                                    crate::primary_scrub::scrub_batch(
                                        &crate::primary_scrub::PrimaryScrubConfig {
                                            topology_store: pins.topology_store.clone(),
                                            shard_store: pins.shard_store.clone(),
                                            data_store: pins.data_store.clone(),
                                            max_object_bytes: config.primary_scrub_max_object_bytes,
                                        },
                                        config.primary_scrub_objects_per_interval.max(1),
                                        coordinator_epoch,
                                        coordinator_sequence,
                                    ).await
                                }
                                Err(error) => Err(error),
                            }
                        }
                        None => Ok(crate::primary_scrub::PrimaryScrubReport {
                            checked: 0,
                            completed_sweep: true,
                        }),
                    };
                    match result {
                        Ok(report) => {
                            let was_healthy = actor_status
                                .primary_scrub_healthy
                                .load(Ordering::Acquire);
                            if report.completed_sweep {
                                actor_status
                                    .primary_scrub_healthy
                                    .store(true, Ordering::Release);
                                if !was_healthy {
                                    snapshot_tick.reset_immediately();
                                }
                            }
                            last_primary_scrub_ms = now_ms();
                            last_primary_scrub_at = Some(Instant::now());
                            tracing::info!(
                                checked = report.checked,
                                completed_sweep = report.completed_sweep,
                                "primary SlateDB integrity scrub batch complete"
                            );
                        }
                        Err(error) => {
                            actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                            actor_status.snapshot_healthy.store(false, Ordering::Release);
                            latest_completed_ms = 0;
                            latest_completed_at = None;
                            last_primary_scrub_at = None;
                            tracing::error!("primary SlateDB integrity scrub failed: {error:#}");
                        }
                    }
                    if let Some(fence) = fence.as_ref() {
                        health_sequence = health_sequence.saturating_add(1).max(1);
                        let health = CoordinatorHealth {
                            format_version: COORDINATOR_FORMAT_VERSION,
                            lease_epoch: fence.epoch,
                            lease_renewal_sequence: fence.state.renewal_sequence.load(Ordering::Acquire),
                            sequence: health_sequence,
                            generated_ms: now_ms(),
                            latest_completed_ms,
                            last_scrub_ms,
                            snapshot_healthy: actor_status.snapshot_healthy.load(Ordering::Acquire),
                            scrub_healthy: actor_status.scrub_healthy.load(Ordering::Acquire),
                            snapshot_age_ms: monotonic_age_ms(latest_completed_at),
                            scrub_age_ms: monotonic_age_ms(last_scrub_at),
                            last_primary_scrub_ms,
                            primary_scrub_healthy: actor_status.primary_scrub_healthy.load(Ordering::Acquire),
                            primary_scrub_age_ms: monotonic_age_ms(last_primary_scrub_at),
                        };
                        if let Err(error) = publish_coordinator_health(fence, &health).await {
                            actor_status.snapshot_healthy.store(false, Ordering::Release);
                            actor_status.scrub_healthy.store(false, Ordering::Release);
                            actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                            tracing::error!("backup coordinator health publication failed: {error:#}");
                        }
                    }
                }
                _ = coordinator_tick.tick(), if coordinator.is_some() => {
                    let is_leader = coordinator
                        .as_ref()
                        .and_then(|coordinator| coordinator.fence())
                        .is_some();
                    if is_leader && !was_leader {
                        snapshot_tick.reset_immediately();
                        scrub_tick.reset_immediately();
                        primary_scrub_tick.reset_immediately();
                    } else if !is_leader {
                        match load_coordinator_health(
                            coordinator.as_ref().expect("coordinator exists"),
                            config.interval,
                            config.scrub_interval,
                            config.primary_scrub_interval,
                        ).await {
                            Ok(health) => {
                                actor_status.snapshot_healthy.store(health.snapshot_healthy, Ordering::Release);
                                actor_status.scrub_healthy.store(health.scrub_healthy, Ordering::Release);
                                actor_status.primary_scrub_healthy.store(health.primary_scrub_healthy, Ordering::Release);
                            }
                            Err(error) => {
                                actor_status.snapshot_healthy.store(false, Ordering::Release);
                                actor_status.scrub_healthy.store(false, Ordering::Release);
                                actor_status.primary_scrub_healthy.store(false, Ordering::Release);
                                tracing::warn!("backup coordinator health unavailable: {error:#}");
                            }
                        }
                    }
                    was_leader = is_leader;
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
    snapshot_once_with_pins_fenced(
        sources,
        destination,
        None,
        None,
        BackupWriteFormat::V2,
        0,
        0,
    )
    .await
}

#[cfg(test)]
async fn snapshot_once_with_pins(
    sources: &[BackupSource],
    destination: Arc<dyn ObjectStore>,
    pins: Option<&BackupPins>,
) -> anyhow::Result<SnapshotReport> {
    snapshot_once_with_pins_fenced(
        sources,
        destination,
        pins,
        None,
        BackupWriteFormat::V2,
        0,
        0,
    )
    .await
}

async fn snapshot_once_with_pins_fenced(
    sources: &[BackupSource],
    destination: Arc<dyn ObjectStore>,
    pins: Option<&BackupPins>,
    fence: Option<&PublicationFence>,
    write_format: BackupWriteFormat,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
) -> anyhow::Result<SnapshotReport> {
    if let Some(fence) = fence {
        fence.check_local()?;
        anyhow::ensure!(
            coordinator_epoch == fence.epoch && coordinator_sequence > 0,
            "backup publication order does not match its coordinator lease"
        );
    }
    let started_ms = now_ms();
    anyhow::ensure!(
        write_format != BackupWriteFormat::V3 || coordinator_epoch > 0,
        "format-3 backup requires a coordinator epoch"
    );
    let snapshot_id = if write_format == BackupWriteFormat::V2 {
        format!("{:020}-{:032x}", started_ms.max(0), rand::random::<u128>())
    } else {
        format!(
            "{:020}-e{:020}-{:032x}",
            started_ms.max(0),
            coordinator_epoch,
            rand::random::<u128>()
        )
    };
    let (pinned_state, leases) = match pins {
        Some(pins) => {
            let (state, leases) = acquire_checkpoint_leases(
                pins,
                destination.clone(),
                &snapshot_id,
                write_format.content_epoch(coordinator_epoch),
            )
            .await?;
            (Some(state), leases)
        }
        None => (None, Vec::new()),
    };
    let result = snapshot_once_inner(
        sources,
        destination,
        SnapshotContext {
            pins,
            pinned_state: pinned_state.as_ref(),
            started_ms,
            snapshot_id: &snapshot_id,
            fence,
            write_format,
            coordinator_epoch,
            coordinator_sequence,
        },
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
    context: SnapshotContext<'_>,
) -> anyhow::Result<SnapshotReport> {
    let SnapshotContext {
        pins,
        pinned_state,
        started_ms,
        snapshot_id,
        fence,
        write_format,
        coordinator_epoch,
        coordinator_sequence,
    } = context;
    let mut progress = SnapshotProgress::default();
    let mut roles = Vec::with_capacity(sources.len());
    for source in sources {
        validate_role(source.role)?;
        roles.push(source.role.to_string());
    }
    let content_epoch = write_format.content_epoch(coordinator_epoch);
    let mut protected_paths = HashSet::new();

    if let Some(state) = pinned_state {
        for protected in &state.protected_wals {
            anyhow::ensure!(
                sources
                    .iter()
                    .any(|source| source.role == protected.record.role),
                "protected WAL has no matching snapshot source role: {}",
                protected.record.role
            );
            anyhow::ensure!(
                protected_paths.insert((
                    protected.record.role.clone(),
                    protected.record.source_path.clone(),
                )),
                "duplicate protected WAL in recovery cut: {}/{}",
                protected.record.role,
                protected.record.source_path
            );
            publish_inventory_record(
                destination.clone(),
                snapshot_id,
                protected.record.clone(),
                &protected.source_etag,
                protected.reused,
                content_epoch,
                coordinator_epoch,
                coordinator_sequence,
                fence,
                &mut progress,
            )
            .await?;
        }
    }

    for source in sources {
        if let Some(fence) = fence {
            fence.check_local()?;
        }
        let mut listing = source.store.list(None);
        while let Some(meta) = listing.try_next().await? {
            if matches!(
                meta.location.as_ref(),
                "backup/coordinator-lease.json" | "backup/health.json"
            ) {
                continue;
            }
            if let Some(fence) = fence {
                fence.check_local()?;
            }
            if protected_paths
                .contains(&(source.role.to_string(), meta.location.as_ref().to_string()))
            {
                continue;
            }
            if matches!(source.role, "shard" | "data")
                && pinned_state
                    .is_some_and(|state| object_is_outside_recovery_point(&meta.location, state))
            {
                continue;
            }
            if matches!(source.role, "shard" | "data")
                && let Some(expected_etag) =
                    pinned_state.and_then(|state| pinned_wal_etag(&meta.location, state))
            {
                anyhow::ensure!(
                    meta.e_tag.as_deref() == Some(expected_etag),
                    "pinned WAL changed while snapshotting: {}",
                    meta.location
                );
            }
            let source_etag = meta
                .e_tag
                .clone()
                .ok_or_else(|| anyhow::anyhow!("object {} has no ETag", meta.location))?;
            let (record, reused) = match reusable_record(
                destination.clone(),
                source.role,
                &meta.location,
                &source_etag,
                meta.size,
                snapshot_id,
                content_epoch,
            )
            .await?
            {
                Some(record) => (record, true),
                None => {
                    let record = copy_incremental_object(
                        source,
                        destination.clone(),
                        snapshot_id,
                        &meta.location,
                        &source_etag,
                        meta.size,
                        content_epoch,
                    )
                    .await?;
                    (record, false)
                }
            };
            publish_inventory_record(
                destination.clone(),
                snapshot_id,
                record,
                &source_etag,
                reused,
                content_epoch,
                coordinator_epoch,
                coordinator_sequence,
                fence,
                &mut progress,
            )
            .await?;
        }
    }

    if let (Some(pins), Some(state)) = (pins, pinned_state) {
        verify_pinned_state(pins, state).await?;
    }
    if let Some(fence) = fence {
        fence.verify_remote().await?;
    }

    let report = SnapshotReport {
        format_version: write_format.version(),
        snapshot_id: snapshot_id.to_string(),
        started_ms,
        completed_ms: now_ms(),
        objects: progress.objects,
        bytes: progress.bytes,
        roles,
        inventory_checksum: hex_encode(&progress.inventory_checksum),
        copied_objects: progress.copied_objects,
        copied_bytes: progress.copied_bytes,
        reused_objects: progress.reused_objects,
        pinned_shards: pinned_state.map_or(0, |state| state.topology.shards.len() as u64),
        pinned_history_dbs: pinned_state.map_or(0, |state| {
            state
                .history_manifests
                .values()
                .filter(|manifest| manifest.is_some())
                .count() as u64
        }),
        coordinator_epoch,
        coordinator_sequence,
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
    // requires and validates the immutable marker named by this report. A
    // coordinated actor CAS-orders it by lease epoch/sequence so a delayed
    // old leader cannot regress the pointer after takeover.
    if let Some(fence) = fence {
        fence.verify_remote().await?;
        publish_latest(destination, &report).await?;
    } else {
        destination
            .put(
                &ObjPath::from("latest.json"),
                PutPayload::from(Bytes::from(serde_json::to_vec(&report)?)),
            )
            .await?;
    }
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn publish_inventory_record(
    destination: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    record: InventoryRecord,
    source_etag: &str,
    reused: bool,
    content_epoch: u64,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
    fence: Option<&PublicationFence>,
    progress: &mut SnapshotProgress,
) -> anyhow::Result<()> {
    if let Some(fence) = fence {
        fence.check_local()?;
    }
    write_source_index(
        destination.clone(),
        &record,
        source_etag,
        snapshot_id,
        coordinator_epoch,
        coordinator_sequence,
    )
    .await?;
    touch_blob_reference(
        destination.clone(),
        &record,
        snapshot_id,
        content_epoch,
        coordinator_epoch,
        coordinator_sequence,
    )
    .await?;
    let inventory = serde_json::to_vec(&record)?;
    anyhow::ensure!(
        inventory.len() <= MAX_INVENTORY_BYTES,
        "inventory record too large"
    );
    xor_digest(
        &mut progress.inventory_checksum,
        Sha256::digest(&inventory).into(),
    );
    let inventory_path = inventory_path(snapshot_id, &record.role, &record.source_path);
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
    progress.objects = progress.objects.saturating_add(1);
    progress.bytes = progress
        .bytes
        .checked_add(record.size)
        .ok_or_else(|| anyhow::anyhow!("snapshot byte count overflow"))?;
    if reused {
        progress.reused_objects = progress.reused_objects.saturating_add(1);
    } else {
        progress.copied_objects = progress.copied_objects.saturating_add(1);
        progress.copied_bytes = progress.copied_bytes.saturating_add(record.size);
    }
    Ok(())
}

async fn publish_latest(
    destination: Arc<dyn ObjectStore>,
    report: &SnapshotReport,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        report.coordinator_epoch > 0 && report.coordinator_sequence > 0,
        "coordinated latest pointer has no publication order"
    );
    validate_snapshot_layout(report)?;
    let path = ObjPath::from("latest.json");
    let encoded = serde_json::to_vec(report)?;
    for _ in 0..5 {
        let mode = match destination.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let current_bytes = result.bytes().await?;
                anyhow::ensure!(
                    current_bytes.len() <= MAX_INVENTORY_BYTES,
                    "latest pointer is too large"
                );
                let current: SnapshotReport = serde_json::from_slice(&current_bytes)?;
                validate_snapshot_id(&current.snapshot_id)?;
                let current_order = (current.coordinator_epoch, current.coordinator_sequence);
                let next_order = (report.coordinator_epoch, report.coordinator_sequence);
                anyhow::ensure!(
                    current_order <= next_order,
                    "backup latest pointer publication was fenced"
                );
                if current_order == next_order {
                    anyhow::ensure!(
                        current.snapshot_id == report.snapshot_id,
                        "conflicting backup latest pointer sequence"
                    );
                    return Ok(());
                }
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        match destination
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded.clone())),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("backup latest pointer CAS retries exhausted")
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
    destination: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    content_epoch: u64,
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
    // Capture the shard WAL cut before pinning history. History must be at
    // least as new as every shard-side absorbed frontier included by the cut.
    let (shard_manifests, mut protected_wals) = match collect_pinned_manifests(
        BackupSource {
            role: "shard",
            store: pins.shard_store.clone(),
        },
        destination.clone(),
        snapshot_id,
        content_epoch,
        &shard_leases,
        shard_absent,
    )
    .await
    {
        Ok(manifests) => manifests,
        Err(error) => {
            let _ = release_checkpoint_leases(shard_leases).await;
            return Err(error);
        }
    };
    let (history_leases, history_absent) =
        match acquire_history_checkpoints(pins, snapshot_id).await {
            Ok(result) => result,
            Err(error) => {
                let _ = release_checkpoint_leases(shard_leases).await;
                return Err(error);
            }
        };
    let (history_manifests, history_protected_wals) = match collect_pinned_manifests(
        BackupSource {
            role: "data",
            store: pins.data_store.clone(),
        },
        destination,
        snapshot_id,
        content_epoch,
        &history_leases,
        history_absent,
    )
    .await
    {
        Ok(manifests) => manifests,
        Err(error) => {
            let _ = release_checkpoint_leases(shard_leases).await;
            let _ = release_checkpoint_leases(history_leases).await;
            return Err(error);
        }
    };
    protected_wals.extend(history_protected_wals);
    let mut leases = shard_leases;
    leases.extend(history_leases);
    Ok((
        PinnedBackupState {
            topology,
            shard_manifests,
            history_manifests,
            protected_wals,
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
    let paths = crate::registry::active_history_db_paths(&pins.topology_store).await?;
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

#[cfg(test)]
fn recovery_history_db_path(hash: &crate::registry::StorageHash) -> String {
    crate::registry::history_db_path(hash)
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
        for (id, expected_etag) in &pinned.wal_etags {
            if *id >= pinned.first_unmanifested_wal_id {
                // These WALs are already immutable in the recovery provider.
                // They were not referenced by the detached checkpoint, so
                // source WAL GC is allowed to remove them after protection.
                continue;
            }
            let wal = ObjPath::from(format!("{path}/wal/{id:020}.sst"));
            let meta = store.head(&wal).await?;
            anyhow::ensure!(
                meta.e_tag.as_deref() == Some(expected_etag),
                "pinned WAL changed while snapshotting: {wal}"
            );
        }
    }
    Ok(())
}

fn wal_id_for_path(root: &str, path: &ObjPath) -> Option<u64> {
    let suffix = path
        .as_ref()
        .strip_prefix(&format!("{root}/wal/"))?
        .strip_suffix(".sst")?;
    (suffix.len() == 20 && suffix.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| suffix.parse().ok())
        .flatten()
}

async fn capture_pinned_wals(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    replay_after_wal_id: u64,
    next_wal_sst_id: u64,
) -> anyhow::Result<HashMap<u64, String>> {
    let first = replay_after_wal_id
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("pinned WAL replay watermark exhausted: {path}"))?;
    anyhow::ensure!(
        next_wal_sst_id >= first,
        "pinned manifest has invalid WAL watermarks: {path}"
    );
    let mut wal_etags = HashMap::new();
    let prefix = ObjPath::from(format!("{path}/wal"));
    let mut listing = store.list(Some(&prefix));
    while let Some(meta) = listing.try_next().await? {
        let id = wal_id_for_path(path, &meta.location).ok_or_else(|| {
            anyhow::anyhow!(
                "malformed object in pinned WAL namespace: {}",
                meta.location
            )
        })?;
        if id < first {
            continue;
        }
        let etag = meta
            .e_tag
            .ok_or_else(|| anyhow::anyhow!("pinned WAL has no ETag: {}", meta.location))?;
        anyhow::ensure!(
            wal_etags.insert(id, etag).is_none(),
            "duplicate pinned WAL id: {path}/{id}"
        );
        anyhow::ensure!(
            wal_etags.len() <= MAX_SNAPSHOT_GENERATIONS,
            "pinned WAL set exceeds the database safety bound: {path}"
        );
    }
    if let Some(last) = wal_etags.keys().copied().max() {
        let count = last
            .checked_sub(first)
            .and_then(|count| count.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| anyhow::anyhow!("pinned WAL range overflow: {path}"))?;
        anyhow::ensure!(
            count <= MAX_SNAPSHOT_GENERATIONS && count == wal_etags.len(),
            "pinned WAL ids are not contiguous: {path}"
        );
        for id in first..=last {
            anyhow::ensure!(
                wal_etags.contains_key(&id),
                "pinned WAL range has a gap: {path}/{id}"
            );
        }
    }
    for id in first..next_wal_sst_id {
        anyhow::ensure!(
            wal_etags.contains_key(&id),
            "manifest-referenced WAL is missing: {path}/{id}"
        );
    }
    Ok(wal_etags)
}

async fn collect_pinned_manifests(
    source: BackupSource,
    destination: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    content_epoch: u64,
    leases: &[CheckpointLease],
    absent: Vec<String>,
) -> anyhow::Result<(HashMap<String, Option<PinnedDbManifest>>, Vec<ProtectedWal>)> {
    let store = source.store.clone();
    let mut pending = VecDeque::new();
    for lease in leases {
        pending.push_back((lease.path.clone(), lease.manifest_id));
    }
    let mut manifests: HashMap<String, Option<PinnedDbManifest>> =
        absent.into_iter().map(|path| (path, None)).collect();
    let mut protected_wals = Vec::new();
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
        let wal_etags = capture_pinned_wals(
            &store,
            &path,
            manifest.replay_after_wal_id(),
            manifest.next_wal_sst_id(),
        )
        .await?;
        for (&id, source_etag) in wal_etags
            .iter()
            .filter(|(id, _)| **id >= manifest.next_wal_sst_id())
        {
            let source_path = ObjPath::from(format!("{path}/wal/{id:020}.sst"));
            let meta = store.head(&source_path).await?;
            anyhow::ensure!(
                meta.e_tag.as_deref() == Some(source_etag),
                "pre-manifest WAL changed before recovery protection: {source_path}"
            );
            let (record, reused) = match reusable_record(
                destination.clone(),
                source.role,
                &source_path,
                source_etag,
                meta.size,
                snapshot_id,
                content_epoch,
            )
            .await?
            {
                Some(record) => (record, true),
                None => (
                    copy_incremental_object(
                        &source,
                        destination.clone(),
                        snapshot_id,
                        &source_path,
                        source_etag,
                        meta.size,
                        content_epoch,
                    )
                    .await?,
                    false,
                ),
            };
            protected_wals.push(ProtectedWal {
                source_etag: source_etag.clone(),
                record,
                reused,
            });
        }
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
                wal_etags,
                first_unmanifested_wal_id: manifest.next_wal_sst_id(),
                compactions_id,
            }),
        );
    }
    Ok((manifests, protected_wals))
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
    snapshot_id: &str,
    coordinator_epoch: u64,
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
        || !valid_index_blob_layout(&index)
    {
        return Ok(None);
    }
    let indexed_blob = ObjPath::parse(&index.blob_path)?;
    let desired_blob = blob_path_for_epoch(&index.sha256, coordinator_epoch);
    let meta = match destination.head(&indexed_blob).await {
        Ok(meta) if meta.size == size => meta,
        Ok(_) | Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let backup_etag = if indexed_blob == desired_blob {
        meta.e_tag
            .ok_or_else(|| anyhow::anyhow!("backup blob {indexed_blob} has no ETag"))?
    } else {
        // A new coordinator epoch never shares a deletable content path with
        // its predecessor. Re-home a still-valid immutable blob through a
        // checksummed staging object; if the old epoch is being collected at
        // the same time, fall back to copying the primary object instead.
        let get = match destination.get(&indexed_blob).await {
            Ok(get) => get,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let staging = staging_path(snapshot_id, role, source_path.as_ref());
        let copied = copy_stream(get.into_stream(), destination.clone(), &staging).await;
        let (copied_size, digest, _) = match copied {
            Ok(result) => result,
            Err(_) => {
                let _ = destination.delete(&staging).await;
                return Ok(None);
            }
        };
        if copied_size != size || hex_encode(&digest) != index.sha256 {
            let _ = destination.delete(&staging).await;
            return Ok(None);
        }
        if let Err(error) = destination.copy(&staging, &desired_blob).await {
            let _ = destination.delete(&staging).await;
            return Err(error.into());
        }
        destination.delete(&staging).await?;
        let promoted = destination.head(&desired_blob).await?;
        anyhow::ensure!(
            promoted.size == size,
            "re-homed backup blob has the wrong size"
        );
        promoted
            .e_tag
            .ok_or_else(|| anyhow::anyhow!("backup blob {desired_blob} has no ETag"))?
    };
    Ok(Some(InventoryRecord {
        role: role.to_string(),
        source_path: source_path.to_string(),
        size,
        sha256: index.sha256,
        backup_etag,
        blob_path: Some(desired_blob.to_string()),
    }))
}

async fn copy_incremental_object(
    source: &BackupSource,
    destination: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    source_path: &ObjPath,
    source_etag: &str,
    expected_size: u64,
    coordinator_epoch: u64,
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
    let blob = blob_path_for_epoch(&sha256, coordinator_epoch);
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
    coordinator_epoch: u64,
    coordinator_sequence: u64,
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
        coordinator_epoch,
        coordinator_sequence,
    };
    let encoded = serde_json::to_vec(&index)?;
    anyhow::ensure!(
        encoded.len() <= MAX_INVENTORY_BYTES,
        "source index too large"
    );
    let path = source_index_path(&record.role, &record.source_path);
    if coordinator_epoch == 0 {
        destination
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await?;
        return Ok(());
    }
    for _ in 0..5 {
        let mode = match destination.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let current: SourceIndex = serde_json::from_slice(&result.bytes().await?)?;
                let current_order = (current.coordinator_epoch, current.coordinator_sequence);
                let next_order = (coordinator_epoch, coordinator_sequence);
                anyhow::ensure!(
                    current_order <= next_order,
                    "backup source-index update was fenced"
                );
                if current_order == next_order {
                    anyhow::ensure!(
                        current.snapshot_id == snapshot_id
                            && current.role == record.role
                            && current.source_path == record.source_path,
                        "conflicting backup source-index sequence"
                    );
                    return Ok(());
                }
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        match destination
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded.clone())),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("backup source-index CAS retries exhausted")
}

async fn touch_blob_reference(
    destination: Arc<dyn ObjectStore>,
    record: &InventoryRecord,
    snapshot_id: &str,
    content_epoch: u64,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
) -> anyhow::Result<()> {
    let reference = BlobReference {
        sha256: record.sha256.clone(),
        blob_path: record
            .blob_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("incremental record has no blob path"))?,
        snapshot_id: snapshot_id.to_string(),
        referenced_ms: now_ms(),
        coordinator_epoch,
        coordinator_sequence,
    };
    let path = blob_reference_path_for_epoch(&record.sha256, content_epoch);
    let encoded = serde_json::to_vec(&reference)?;
    if coordinator_epoch == 0 {
        destination
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await?;
        return Ok(());
    }
    for _ in 0..5 {
        let mode = match destination.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let current: BlobReference = serde_json::from_slice(&result.bytes().await?)?;
                let current_order = (current.coordinator_epoch, current.coordinator_sequence);
                let next_order = (coordinator_epoch, coordinator_sequence);
                anyhow::ensure!(
                    current_order <= next_order,
                    "backup blob-reference update was fenced"
                );
                if current_order == next_order {
                    anyhow::ensure!(
                        current.snapshot_id == snapshot_id && current.sha256 == record.sha256,
                        "conflicting backup blob-reference sequence"
                    );
                    return Ok(());
                }
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        match destination
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded.clone())),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("backup blob-reference CAS retries exhausted")
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
    validate_snapshot_layout(&report)?;
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
            LEGACY_SNAPSHOT_FORMAT_VERSION
                | CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION
                | SNAPSHOT_FORMAT_VERSION
        ),
        "unsupported snapshot format {}",
        report.format_version
    );
    anyhow::ensure!(
        report.snapshot_id == snapshot_id,
        "snapshot marker id mismatch"
    );
    validate_snapshot_layout(&report)?;

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
        validate_inventory_record(&record, &report)?;
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
        validate_inventory_record(&record, &report)?;
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
        // Formats 2 and 3 are content-addressed and verified by SHA-256 below. Avoid
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

async fn ensure_gc_intent(
    destination: Arc<dyn ObjectStore>,
    snapshot_id: &str,
    coordinator_epoch: u64,
) -> anyhow::Result<()> {
    validate_snapshot_id(snapshot_id)?;
    let intent = GcIntent {
        format_version: GC_INTENT_FORMAT_VERSION,
        snapshot_id: snapshot_id.to_string(),
        coordinator_epoch,
        created_ms: now_ms(),
    };
    let path = gc_intent_path(snapshot_id);
    let encoded = serde_json::to_vec(&intent)?;
    match destination
        .put_opts(
            &path,
            PutPayload::from(Bytes::from(encoded)),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = destination.get(&path).await?.bytes().await?;
            anyhow::ensure!(
                existing.len() <= MAX_INVENTORY_BYTES,
                "backup GC intent is too large"
            );
            let existing: GcIntent = serde_json::from_slice(&existing)?;
            anyhow::ensure!(
                existing.format_version == GC_INTENT_FORMAT_VERSION
                    && existing.snapshot_id == snapshot_id
                    && existing.coordinator_epoch == coordinator_epoch
                    && existing.created_ms > 0,
                "conflicting backup GC intent"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

/// Obtain "now" from the recovery provider itself. Comparing provider object
/// timestamps with a producer wall clock would let a skewed process expire the
/// entire recovery corpus. The random CAS payload also prevents a stale read
/// from being mistaken for the just-written probe.
async fn retention_provider_now_ms(
    destination: Arc<dyn ObjectStore>,
    fence: Option<&PublicationFence>,
) -> anyhow::Result<i64> {
    let path = retention_clock_path();
    for _ in 0..5 {
        if let Some(fence) = fence {
            fence.verify_remote().await?;
        }
        let mode = match destination.head(&path).await {
            Ok(meta) => PutMode::Update(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            }),
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        let probe = RetentionClockProbe {
            format_version: RETENTION_CLOCK_FORMAT_VERSION,
            token: coordinator_token(),
            coordinator_epoch: fence.map_or(0, |fence| fence.epoch),
        };
        let encoded = serde_json::to_vec(&probe)?;
        match destination
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded)),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
        if let Some(fence) = fence {
            fence.verify_remote().await?;
        }
        let result = destination.get(&path).await?;
        let meta = result.meta.clone();
        anyhow::ensure!(
            meta.size <= MAX_COORDINATOR_DOCUMENT_BYTES as u64,
            "backup retention clock probe is too large"
        );
        let observed: RetentionClockProbe = serde_json::from_slice(&result.bytes().await?)?;
        if observed.token != probe.token {
            continue;
        }
        anyhow::ensure!(
            observed.format_version == RETENTION_CLOCK_FORMAT_VERSION
                && observed.coordinator_epoch == probe.coordinator_epoch,
            "malformed backup retention clock probe"
        );
        let provider_now_ms = meta.last_modified.timestamp_millis();
        anyhow::ensure!(
            provider_now_ms > 0,
            "backup provider returned an invalid object timestamp"
        );
        return Ok(provider_now_ms);
    }
    anyhow::bail!("backup retention clock probe CAS retries exhausted")
}

pub async fn prune_once(
    destination: Arc<dyn ObjectStore>,
    retention: Duration,
) -> anyhow::Result<u64> {
    prune_once_fenced(destination, retention, None, true).await
}

async fn prune_once_fenced(
    destination: Arc<dyn ObjectStore>,
    retention: Duration,
    fence: Option<&PublicationFence>,
    allow_legacy_content_gc: bool,
) -> anyhow::Result<u64> {
    if let Some(fence) = fence {
        fence.verify_remote().await?;
    }
    anyhow::ensure!(!retention.is_zero(), "backup retention must be positive");
    let provider_now_ms = retention_provider_now_ms(destination.clone(), fence).await?;
    let cutoff =
        provider_now_ms.saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX));
    let snapshots_prefix = ObjPath::from("snapshots");
    let mut generations = HashSet::new();
    let mut generation_latest_modified = HashMap::new();
    let mut completed = HashMap::new();
    let mut listing = destination.list(Some(&snapshots_prefix));
    while let Some(meta) = listing.try_next().await? {
        let Some(snapshot_id) = generation_from_path(&meta.location, "snapshots") else {
            continue;
        };
        validate_snapshot_id(&snapshot_id)?;
        let modified_ms = meta.last_modified.timestamp_millis();
        anyhow::ensure!(
            modified_ms > 0,
            "backup provider returned an invalid snapshot timestamp"
        );
        generations.insert(snapshot_id.clone());
        generation_latest_modified
            .entry(snapshot_id.clone())
            .and_modify(|timestamp: &mut i64| {
                *timestamp = (*timestamp).max(modified_ms);
            })
            .or_insert(modified_ms);
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
            validate_snapshot_layout(&report)?;
            completed.insert(snapshot_id, (modified_ms, report.coordinator_epoch));
        }
    }
    let staging_prefix = ObjPath::from("staging");
    let mut listing = destination.list(Some(&staging_prefix));
    while let Some(meta) = listing.try_next().await? {
        if let Some(snapshot_id) = generation_from_path(&meta.location, "staging") {
            validate_snapshot_id(&snapshot_id)?;
            let modified_ms = meta.last_modified.timestamp_millis();
            anyhow::ensure!(
                modified_ms > 0,
                "backup provider returned an invalid staging timestamp"
            );
            generations.insert(snapshot_id.clone());
            generation_latest_modified
                .entry(snapshot_id)
                .and_modify(|timestamp: &mut i64| {
                    *timestamp = (*timestamp).max(modified_ms);
                })
                .or_insert(modified_ms);
            anyhow::ensure!(
                generations.len() <= MAX_SNAPSHOT_GENERATIONS,
                "backup snapshot generation count exceeds safety bound"
            );
        }
    }
    let gc_prefix = ObjPath::from("gc-intents");
    let mut pending_gc = HashMap::new();
    let mut listing = destination.list(Some(&gc_prefix));
    while let Some(meta) = listing.try_next().await? {
        anyhow::ensure!(
            meta.size <= MAX_INVENTORY_BYTES as u64,
            "backup GC intent is too large"
        );
        let Some(snapshot_id) = generation_from_path(&meta.location, "gc-intents") else {
            anyhow::bail!("malformed backup GC intent path");
        };
        validate_snapshot_id(&snapshot_id)?;
        anyhow::ensure!(
            meta.location == gc_intent_path(&snapshot_id),
            "malformed backup GC intent path"
        );
        let encoded = destination.get(&meta.location).await?.bytes().await?;
        let intent: GcIntent = serde_json::from_slice(&encoded)?;
        anyhow::ensure!(
            intent.format_version == GC_INTENT_FORMAT_VERSION
                && intent.snapshot_id == snapshot_id
                && intent.created_ms > 0,
            "malformed backup GC intent"
        );
        generations.insert(snapshot_id.clone());
        pending_gc.insert(snapshot_id, intent.coordinator_epoch);
        anyhow::ensure!(
            generations.len() <= MAX_SNAPSHOT_GENERATIONS,
            "backup snapshot generation count exceeds safety bound"
        );
    }

    let newest_completed = completed
        .iter()
        .max_by(|(left_id, (left_time, _)), (right_id, (right_time, _))| {
            left_time
                .cmp(right_time)
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(snapshot_id, _)| snapshot_id.clone());
    let latest_completed = match destination.get(&ObjPath::from("latest.json")).await {
        Ok(result) => {
            let encoded = result.bytes().await?;
            anyhow::ensure!(
                encoded.len() <= MAX_INVENTORY_BYTES,
                "latest pointer is too large"
            );
            let report: SnapshotReport = serde_json::from_slice(&encoded)?;
            validate_snapshot_id(&report.snapshot_id)?;
            validate_snapshot_layout(&report)?;
            completed
                .contains_key(&report.snapshot_id)
                .then_some(report.snapshot_id)
        }
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => return Err(error.into()),
    };
    let mut delete_generations = HashSet::new();
    for snapshot_id in generations {
        let expired = completed
            .get(&snapshot_id)
            .map(|(marker_modified_ms, _)| *marker_modified_ms)
            .or_else(|| generation_latest_modified.get(&snapshot_id).copied())
            .is_some_and(|timestamp| timestamp < cutoff);
        let generation_epoch = completed
            .get(&snapshot_id)
            .map(|(_, epoch)| *epoch)
            .or_else(|| snapshot_coordinator_epoch(&snapshot_id))
            .unwrap_or(0);
        let fenced = fence.is_some_and(|fence| generation_epoch > fence.epoch);
        if expired
            && !fenced
            && newest_completed.as_ref() != Some(&snapshot_id)
            && latest_completed.as_ref() != Some(&snapshot_id)
        {
            delete_generations.insert(snapshot_id);
        }
    }
    for (snapshot_id, generation_epoch) in &pending_gc {
        if fence.is_none_or(|fence| *generation_epoch <= fence.epoch) {
            delete_generations.insert(snapshot_id.clone());
        }
    }

    let mut pruned = 0u64;
    for snapshot_id in &delete_generations {
        let generation_epoch = completed
            .get(snapshot_id)
            .map(|(_, epoch)| *epoch)
            .or_else(|| pending_gc.get(snapshot_id).copied())
            .or_else(|| snapshot_coordinator_epoch(snapshot_id))
            .unwrap_or(0);
        if let Some(fence) = fence {
            fence.check_local()?;
        }
        ensure_gc_intent(destination.clone(), snapshot_id, generation_epoch).await?;
    }
    // The complete marker is restore authority. Remove it only after the
    // resumable intent is durable, before any referenced content is deleted.
    for snapshot_id in &delete_generations {
        if let Some(fence) = fence {
            fence.check_local()?;
        }
        match destination.delete(&marker_path(snapshot_id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }

    for refs_prefix in ["blob-refs", "formats/3/blob-refs"] {
        let refs_prefix = ObjPath::from(refs_prefix);
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
                    && valid_blob_reference_layout(&reference, &meta.location),
                "malformed blob reference"
            );
            if fence.is_some_and(|fence| reference.coordinator_epoch > fence.epoch) {
                continue;
            }
            if fence.is_some()
                && !allow_legacy_content_gc
                && reference.blob_path == blob_path_for_sha(&reference.sha256).as_ref()
            {
                // During the read-first format-2 migration wave, retain shared
                // legacy blobs. An old epoch may still be paused inside an
                // unconditional delete. Format 3 can safely collect them only
                // after it has re-homed all live references.
                continue;
            }
            if delete_generations.contains(&reference.snapshot_id) {
                if let Some(fence) = fence {
                    fence.check_local()?;
                }
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
        anyhow::ensure!(
            valid_sha256(&index.sha256) && valid_index_blob_layout(&index),
            "malformed backup source index"
        );
        if fence.is_some_and(|fence| index.coordinator_epoch > fence.epoch) {
            continue;
        }
        if delete_generations.contains(&index.snapshot_id) {
            if let Some(fence) = fence {
                fence.check_local()?;
            }
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(1);
        }
    }
    // Content and mutable indexes are now gone or retained by a newer point.
    // Remove residual inventory/staging metadata, then the intent last. A
    // crash before the final delete simply resumes this generation next pass.
    let mut listing = destination.list(Some(&snapshots_prefix));
    while let Some(meta) = listing.try_next().await? {
        if generation_from_path(&meta.location, "snapshots")
            .is_some_and(|id| delete_generations.contains(&id))
        {
            if let Some(fence) = fence {
                fence.check_local()?;
            }
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(1);
        }
    }
    let mut listing = destination.list(Some(&staging_prefix));
    while let Some(meta) = listing.try_next().await? {
        if generation_from_path(&meta.location, "staging")
            .is_some_and(|id| delete_generations.contains(&id))
        {
            if let Some(fence) = fence {
                fence.check_local()?;
            }
            destination.delete(&meta.location).await?;
            pruned = pruned.saturating_add(1);
        }
    }
    for snapshot_id in &delete_generations {
        if let Some(fence) = fence {
            fence.check_local()?;
        }
        destination.delete(&gc_intent_path(snapshot_id)).await?;
        pruned = pruned.saturating_add(1);
    }
    Ok(pruned)
}

async fn scrub_blob_batch(
    destination: Arc<dyn ObjectStore>,
    limit: usize,
    fence: Option<&PublicationFence>,
) -> anyhow::Result<u64> {
    if let Some(fence) = fence {
        fence.check_local()?;
    }
    // Walk references rather than blobs so deletion of a required blob is a
    // scrub failure rather than silently disappearing from the scan set. Do
    // not rely on provider listing order: retain the bounded lexicographically
    // smallest set after the durable cursor while scanning the full prefix.
    let mut cursor = read_scrub_cursor(destination.clone()).await?;
    let mut wrapped = false;
    loop {
        let mut candidates = BTreeMap::new();
        for prefix in ["blob-refs", "formats/3/blob-refs"] {
            let prefix = ObjPath::from(prefix);
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
            if let Some(fence) = fence {
                fence.check_local()?;
            }
            anyhow::ensure!(
                size <= MAX_INVENTORY_BYTES as u64,
                "blob reference is too large"
            );
            let encoded = destination.get(&location).await?.bytes().await?;
            let reference: BlobReference = serde_json::from_slice(&encoded)?;
            anyhow::ensure!(
                valid_sha256(&reference.sha256)
                    && valid_blob_reference_layout(&reference, &location),
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
        if let Some(fence) = fence {
            fence.verify_remote().await?;
        }
        write_scrub_cursor(destination, cursor.as_ref(), fence).await?;
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
    fence: Option<&PublicationFence>,
) -> anyhow::Result<()> {
    let (coordinator_epoch, coordinator_sequence) = match fence {
        Some(fence) => fence.next_mutation_order()?,
        None => (0, 0),
    };
    let state = ScrubState {
        format_version: SCRUB_STATE_FORMAT_VERSION,
        cursor: cursor.map(ToString::to_string),
        updated_ms: now_ms(),
        coordinator_epoch,
        coordinator_sequence,
    };
    let encoded = serde_json::to_vec(&state)?;
    anyhow::ensure!(
        encoded.len() <= MAX_SCRUB_STATE_BYTES,
        "backup scrub state is too large"
    );
    let path = scrub_state_path();
    if coordinator_epoch == 0 {
        destination
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await?;
        return Ok(());
    }
    for _ in 0..5 {
        let mode = match destination.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let current: ScrubState = serde_json::from_slice(&result.bytes().await?)?;
                let current_order = (current.coordinator_epoch, current.coordinator_sequence);
                let next_order = (coordinator_epoch, coordinator_sequence);
                anyhow::ensure!(
                    current_order < next_order,
                    "backup scrub cursor update was fenced"
                );
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        match destination
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded.clone())),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("backup scrub cursor CAS retries exhausted")
}

fn validate_snapshot_layout(report: &SnapshotReport) -> anyhow::Result<()> {
    match report.format_version {
        SNAPSHOT_FORMAT_VERSION => {
            anyhow::ensure!(
                report.coordinator_epoch > 0
                    && report.coordinator_sequence > 0
                    && snapshot_coordinator_epoch(&report.snapshot_id)
                        == Some(report.coordinator_epoch),
                "format-3 snapshot is not bound to its coordinator epoch"
            );
        }
        CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION | LEGACY_SNAPSHOT_FORMAT_VERSION => {
            anyhow::ensure!(
                snapshot_coordinator_epoch(&report.snapshot_id).is_none(),
                "legacy snapshot id unexpectedly carries a coordinator epoch"
            );
        }
        _ => anyhow::bail!("unsupported snapshot format {}", report.format_version),
    }
    Ok(())
}

fn validate_inventory_record(
    record: &InventoryRecord,
    report: &SnapshotReport,
) -> anyhow::Result<()> {
    anyhow::ensure!(valid_sha256(&record.sha256), "invalid inventory digest");
    anyhow::ensure!(!record.backup_etag.is_empty(), "inventory ETag is empty");
    ObjPath::parse(&record.source_path)?;
    match (report.format_version, &record.blob_path) {
        (SNAPSHOT_FORMAT_VERSION, Some(path)) => {
            anyhow::ensure!(
                path == blob_path_for_epoch(&record.sha256, report.coordinator_epoch).as_ref(),
                "format-3 inventory blob path is outside its coordinator epoch"
            );
        }
        (CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION, Some(path)) => {
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

fn blob_path_for_epoch(sha256: &str, coordinator_epoch: u64) -> ObjPath {
    if coordinator_epoch == 0 {
        blob_path_for_sha(sha256)
    } else {
        ObjPath::from(format!(
            "formats/3/blobs/epochs/{coordinator_epoch:020}/sha256/{}/{sha256}",
            &sha256[..2]
        ))
    }
}

fn blob_reference_path(sha256: &str) -> ObjPath {
    ObjPath::from(format!("blob-refs/{}/{sha256}.json", &sha256[..2]))
}

fn blob_reference_path_for_epoch(sha256: &str, coordinator_epoch: u64) -> ObjPath {
    if coordinator_epoch == 0 {
        blob_reference_path(sha256)
    } else {
        ObjPath::from(format!(
            "formats/3/blob-refs/epochs/{coordinator_epoch:020}/{}/{sha256}.json",
            &sha256[..2]
        ))
    }
}

fn valid_index_blob_layout(index: &SourceIndex) -> bool {
    index.blob_path == blob_path_for_sha(&index.sha256).as_ref()
        || (index.coordinator_epoch > 0
            && index.blob_path
                == blob_path_for_epoch(&index.sha256, index.coordinator_epoch).as_ref())
}

fn valid_blob_reference_layout(reference: &BlobReference, location: &ObjPath) -> bool {
    let legacy = reference.blob_path == blob_path_for_sha(&reference.sha256).as_ref()
        && *location == blob_reference_path(&reference.sha256);
    let epoch = reference.coordinator_epoch > 0
        && reference.blob_path
            == blob_path_for_epoch(&reference.sha256, reference.coordinator_epoch).as_ref()
        && *location
            == blob_reference_path_for_epoch(&reference.sha256, reference.coordinator_epoch);
    legacy || epoch
}

fn scrub_state_path() -> ObjPath {
    ObjPath::from("scrub-state.json")
}

fn coordinator_lease_path() -> ObjPath {
    ObjPath::from("backup/coordinator-lease.json")
}

fn coordinator_health_path() -> ObjPath {
    ObjPath::from("backup/health.json")
}

fn retention_clock_path() -> ObjPath {
    ObjPath::from("retention/provider-clock.json")
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
        return !expected.wal_etags.contains_key(&id);
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

fn pinned_wal_etag<'a>(path: &ObjPath, state: &'a PinnedBackupState) -> Option<&'a str> {
    let (db_path, _) = path.as_ref().rsplit_once("/wal/")?;
    let expected = pinned_db(state, db_path)?.as_ref()?;
    let id = wal_id_for_path(db_path, path)?;
    expected.wal_etags.get(&id).map(String::as_str)
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

fn snapshot_coordinator_epoch(snapshot_id: &str) -> Option<u64> {
    let encoded = snapshot_id.split('-').nth(1)?.strip_prefix('e')?;
    (encoded.len() == 20 && encoded.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| encoded.parse().ok())
        .flatten()
}

fn marker_path(snapshot_id: &str) -> ObjPath {
    ObjPath::from(format!("snapshots/{snapshot_id}/_complete.json"))
}

fn gc_intent_path(snapshot_id: &str) -> ObjPath {
    ObjPath::from(format!("gc-intents/{snapshot_id}/intent.json"))
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

pub(crate) fn wall_time_ms() -> i64 {
    now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_coordinator(
        store: Arc<dyn ObjectStore>,
        owner: &str,
        token_byte: char,
    ) -> Arc<CoordinatorState> {
        Arc::new(CoordinatorState {
            config: BackupCoordinator {
                store,
                owner: owner.to_string(),
            },
            token: token_byte.to_string().repeat(32),
            owned: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            renewal_sequence: AtomicU64::new(0),
            last_renewed: Mutex::new(None),
            lease_observation: Mutex::new(None),
            mutation_sequence: AtomicU64::new(0),
        })
    }

    async fn activate_coordinator(state: &Arc<CoordinatorState>) -> CoordinatorLease {
        let lease = claim_coordinator(state).await.unwrap().unwrap();
        state.epoch.store(lease.epoch, Ordering::Release);
        state.owned.store(true, Ordering::Release);
        lease
    }

    async fn take_over_coordinator(state: &Arc<CoordinatorState>) -> CoordinatorLease {
        assert!(claim_coordinator(state).await.unwrap().is_none());
        state
            .lease_observation
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .first_seen = Instant::now() - COORDINATOR_LEASE_DURATION;
        activate_coordinator(state).await
    }

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
        let first = snapshot_once(&sources, backup.clone()).await.unwrap();
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

        ensure_gc_intent(backup.clone(), &first.snapshot_id, first.coordinator_epoch)
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
    async fn retention_uses_provider_age_not_untrusted_report_time() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(&ObjPath::from("one"), PutPayload::from_static(b"durable"))
            .await
            .unwrap();
        let sources = [BackupSource {
            role: "ops",
            store: source,
        }];
        let mut forged_old = snapshot_once(&sources, backup.clone()).await.unwrap();
        forged_old.completed_ms = 1;
        backup
            .put(
                &marker_path(&forged_old.snapshot_id),
                PutPayload::from(Bytes::from(serde_json::to_vec(&forged_old).unwrap())),
            )
            .await
            .unwrap();
        let newest = snapshot_once(&sources, backup.clone()).await.unwrap();

        assert_eq!(
            prune_once(backup.clone(), Duration::from_secs(24 * 60 * 60))
                .await
                .unwrap(),
            0
        );
        backup
            .head(&marker_path(&forged_old.snapshot_id))
            .await
            .unwrap();
        backup
            .head(&marker_path(&newest.snapshot_id))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn takeover_rehomes_blobs_so_delayed_old_epoch_delete_is_harmless() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let coordination: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(
                &ObjPath::from("one"),
                PutPayload::from_static(b"durable ciphertext"),
            )
            .await
            .unwrap();
        let sources = [BackupSource {
            role: "data",
            store: source,
        }];

        let first = test_coordinator(coordination.clone(), "streams-1", 'a');
        let first_lease = activate_coordinator(&first).await;
        let first_fence = first.fence().unwrap();
        let first_report = snapshot_once_with_pins_fenced(
            &sources,
            backup.clone(),
            None,
            Some(&first_fence),
            BackupWriteFormat::V3,
            first_lease.epoch,
            1,
        )
        .await
        .unwrap();
        assert_eq!(first_report.format_version, SNAPSHOT_FORMAT_VERSION);

        let second = test_coordinator(coordination, "streams-2", 'b');
        let second_lease = take_over_coordinator(&second).await;
        let second_fence = second.fence().unwrap();
        let second_report = snapshot_once_with_pins_fenced(
            &sources,
            backup.clone(),
            None,
            Some(&second_fence),
            BackupWriteFormat::V3,
            second_lease.epoch,
            1,
        )
        .await
        .unwrap();
        assert_eq!(second_report.reused_objects, 1);

        let digest = hex_encode(&Sha256::digest(b"durable ciphertext"));
        let first_blob = blob_path_for_epoch(&digest, first_lease.epoch);
        let second_blob = blob_path_for_epoch(&digest, second_lease.epoch);
        assert_ne!(first_blob, second_blob);
        backup.head(&first_blob).await.unwrap();
        backup.head(&second_blob).await.unwrap();
        backup
            .head(&blob_reference_path_for_epoch(&digest, second_lease.epoch))
            .await
            .unwrap();
        assert!(
            backup
                .list(Some(&ObjPath::from("blob-refs")))
                .try_next()
                .await
                .unwrap()
                .is_none(),
            "a format-2 binary would discover a format-3 reference"
        );
        assert_eq!(
            scrub_blob_batch(backup.clone(), 1, Some(&second_fence))
                .await
                .unwrap(),
            1
        );

        // Model an epoch-1 provider DELETE that was admitted before takeover
        // and only completed after epoch 2 published. It cannot name or damage
        // epoch-2 content.
        backup.delete(&first_blob).await.unwrap();
        let restored: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        restore_snapshot(
            backup,
            &second_report.snapshot_id,
            &HashMap::from([("data".to_string(), restored.clone())]),
        )
        .await
        .unwrap();
        assert_eq!(
            restored
                .get(&ObjPath::from("one"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"durable ciphertext")
        );
    }

    #[tokio::test]
    async fn coordinated_format2_wave_retains_shared_content_until_flip() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let coordination: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(&ObjPath::from("one"), PutPayload::from_static(b"legacy-v2"))
            .await
            .unwrap();
        let coordinator = test_coordinator(coordination, "read-first", 'c');
        let lease = activate_coordinator(&coordinator).await;
        let fence = coordinator.fence().unwrap();
        let report = snapshot_once_with_pins_fenced(
            &[BackupSource {
                role: "ops",
                store: source,
            }],
            backup.clone(),
            None,
            Some(&fence),
            BackupWriteFormat::V2,
            lease.epoch,
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            report.format_version,
            CONTENT_ADDRESSED_SNAPSHOT_FORMAT_VERSION
        );
        let digest = hex_encode(&Sha256::digest(b"legacy-v2"));
        let reference_path = blob_reference_path(&digest);
        ensure_gc_intent(
            backup.clone(),
            &report.snapshot_id,
            report.coordinator_epoch,
        )
        .await
        .unwrap();

        prune_once_fenced(
            backup.clone(),
            Duration::from_secs(24 * 60 * 60),
            Some(&fence),
            false,
        )
        .await
        .unwrap();
        backup.head(&blob_path_for_sha(&digest)).await.unwrap();
        backup.head(&reference_path).await.unwrap();
    }

    #[tokio::test]
    async fn retention_resumes_from_intent_after_all_point_metadata_is_lost() {
        let source: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source
            .put(
                &ObjPath::from("one"),
                PutPayload::from_static(b"collect-me"),
            )
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
        let digest = hex_encode(&Sha256::digest(b"collect-me"));
        let reference_path = blob_reference_path(&digest);
        let index_path = source_index_path("ops", "one");
        ensure_gc_intent(backup.clone(), &report.snapshot_id, 0)
            .await
            .unwrap();
        let snapshot_prefix = ObjPath::from(format!("snapshots/{}", report.snapshot_id));
        let objects = backup
            .list(Some(&snapshot_prefix))
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        for object in objects {
            backup.delete(&object).await.unwrap();
        }

        prune_once(backup.clone(), Duration::from_secs(24 * 60 * 60))
            .await
            .unwrap();
        for path in [
            blob_path_for_sha(&digest),
            reference_path,
            index_path,
            gc_intent_path(&report.snapshot_id),
        ] {
            assert!(matches!(
                backup.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
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
        let error = scrub_blob_batch(backup, 1, None).await.unwrap_err();
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
            assert_eq!(scrub_blob_batch(backup.clone(), 1, None).await.unwrap(), 1);
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
    async fn coordinator_takeover_fences_delayed_publications() {
        let coordination: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let first = test_coordinator(coordination.clone(), "streams-1", 'a');
        let first_lease = activate_coordinator(&first).await;
        let first_fence = first.fence().unwrap();
        first_fence.verify_remote().await.unwrap();

        let report = |epoch, sequence, label: &str| SnapshotReport {
            format_version: SNAPSHOT_FORMAT_VERSION,
            snapshot_id: format!("00000000000000000001-e{epoch:020}-{label}"),
            started_ms: now_ms(),
            completed_ms: now_ms(),
            objects: 0,
            bytes: 0,
            roles: Vec::new(),
            inventory_checksum: hex_encode(&[0; 32]),
            copied_objects: 0,
            copied_bytes: 0,
            reused_objects: 0,
            pinned_shards: 0,
            pinned_history_dbs: 0,
            coordinator_epoch: epoch,
            coordinator_sequence: sequence,
        };
        publish_latest(destination.clone(), &report(first_lease.epoch, 1, "first"))
            .await
            .unwrap();

        let second = test_coordinator(coordination.clone(), "streams-2", 'b');
        let second_lease = take_over_coordinator(&second).await;
        assert_eq!(second_lease.epoch, first_lease.epoch + 1);
        let second_fence = second.fence().unwrap();
        publish_latest(
            destination.clone(),
            &report(second_lease.epoch, 1, "second"),
        )
        .await
        .unwrap();
        let delayed = publish_latest(
            destination.clone(),
            &report(first_lease.epoch, u64::MAX, "delayed"),
        )
        .await
        .unwrap_err();
        assert!(delayed.to_string().contains("fenced"));
        assert!(first_fence.verify_remote().await.is_err());

        let health = CoordinatorHealth {
            format_version: COORDINATOR_FORMAT_VERSION,
            lease_epoch: second_lease.epoch,
            lease_renewal_sequence: second_lease.renewal_sequence,
            sequence: 1,
            generated_ms: i64::MAX,
            latest_completed_ms: i64::MAX,
            last_scrub_ms: i64::MAX,
            snapshot_healthy: true,
            scrub_healthy: true,
            snapshot_age_ms: 0,
            scrub_age_ms: 0,
            last_primary_scrub_ms: i64::MAX,
            primary_scrub_healthy: true,
            primary_scrub_age_ms: 0,
        };
        publish_coordinator_health(&second_fence, &health)
            .await
            .unwrap();
        let observed = load_coordinator_health(
            &second,
            Duration::from_secs(60),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert!(
            observed.snapshot_healthy && observed.scrub_healthy && observed.primary_scrub_healthy
        );
        assert!(
            publish_coordinator_health(
                &first_fence,
                &CoordinatorHealth {
                    lease_epoch: first_lease.epoch,
                    sequence: u64::MAX,
                    ..health
                },
            )
            .await
            .is_err()
        );
        assert_eq!(
            latest_snapshot_id(destination).await.unwrap(),
            format!("00000000000000000001-e{:020}-second", second_lease.epoch)
        );
    }

    #[tokio::test]
    async fn coordinator_takeover_uses_monotonic_observation_not_wall_clock() {
        let coordination: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let legacy = LegacyCoordinatorLease {
            format_version: 1,
            owner: "legacy-fast-clock".to_string(),
            token: "d".repeat(32),
            epoch: 41,
            lease_until_ms: i64::MAX,
        };
        coordination
            .put(
                &coordinator_lease_path(),
                PutPayload::from(Bytes::from(serde_json::to_vec(&legacy).unwrap())),
            )
            .await
            .unwrap();

        let current = test_coordinator(coordination.clone(), "current", 'e');
        let current_lease = take_over_coordinator(&current).await;
        assert_eq!(current_lease.epoch, 42);
        assert_eq!(current_lease.renewal_sequence, 1);
        let encoded = coordination
            .get(&coordinator_lease_path())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&encoded).contains("lease_until_ms"));

        let contender = test_coordinator(coordination, "contender", 'f');
        assert!(claim_coordinator(&contender).await.unwrap().is_none());
        contender
            .lease_observation
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .first_seen = Instant::now() - COORDINATOR_LEASE_DURATION;
        let renewed = claim_coordinator(&current).await.unwrap().unwrap();
        assert_eq!(renewed.epoch, current_lease.epoch);
        assert_eq!(renewed.renewal_sequence, 2);
        assert!(claim_coordinator(&contender).await.unwrap().is_none());

        *current.last_renewed.lock().unwrap() = Some(Instant::now() - COORDINATOR_LEASE_DURATION);
        assert!(current.fence().is_none());
    }

    #[tokio::test]
    async fn follower_requires_a_recent_renewal_and_health_from_after_a_pause() {
        let coordination: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let leader = test_coordinator(coordination.clone(), "leader", 'a');
        let first = activate_coordinator(&leader).await;
        let fence = leader.fence().unwrap();
        let follower = test_coordinator(coordination, "follower", 'b');
        let health = |lease_renewal_sequence, sequence| CoordinatorHealth {
            format_version: COORDINATOR_FORMAT_VERSION,
            lease_epoch: first.epoch,
            lease_renewal_sequence,
            sequence,
            generated_ms: i64::MAX,
            latest_completed_ms: i64::MAX,
            last_scrub_ms: i64::MAX,
            snapshot_healthy: true,
            scrub_healthy: true,
            snapshot_age_ms: 0,
            scrub_age_ms: 0,
            last_primary_scrub_ms: i64::MAX,
            primary_scrub_healthy: true,
            primary_scrub_age_ms: 0,
        };

        publish_coordinator_health(&fence, &health(first.renewal_sequence, 1))
            .await
            .unwrap();
        assert!(claim_coordinator(&follower).await.unwrap().is_none());
        assert!(
            load_coordinator_health(
                &follower,
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("has not proven a recent renewal")
        );

        let second = claim_coordinator(&leader).await.unwrap().unwrap();
        assert!(claim_coordinator(&follower).await.unwrap().is_none());
        assert!(
            load_coordinator_health(
                &follower,
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .is_err(),
            "health published before the observed renewal must remain fenced"
        );
        publish_coordinator_health(&fence, &health(second.renewal_sequence, 2))
            .await
            .unwrap();
        assert!(
            load_coordinator_health(
                &follower,
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .unwrap()
            .snapshot_healthy
        );

        follower
            .lease_observation
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .first_seen = Instant::now() - COORDINATOR_LEASE_DURATION;
        assert!(
            load_coordinator_health(
                &follower,
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .is_err(),
            "an unchanged lease must stop carrying health after the monotonic timeout"
        );

        let third = claim_coordinator(&leader).await.unwrap().unwrap();
        assert!(claim_coordinator(&follower).await.unwrap().is_none());
        assert!(
            load_coordinator_health(
                &follower,
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .is_err(),
            "a post-pause renewal must require a post-pause health publication"
        );
        publish_coordinator_health(&fence, &health(third.renewal_sequence, 3))
            .await
            .unwrap();
        assert!(
            load_coordinator_health(
                &follower,
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .unwrap()
            .snapshot_healthy
        );
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
    async fn recovery_point_includes_durable_wal_before_manifest_advance() {
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
        let db = slatedb::Db::builder("shards/root", shards.clone())
            .with_settings(slatedb::config::Settings {
                // AwaitDurable still writes a WAL, but no timer advances the
                // remote manifest before the detached checkpoint is taken.
                flush_interval: Some(Duration::from_millis(20)),
                garbage_collector_options: None,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            db.put(b"pre-manifest", b"acknowledged"),
        )
        .await
        .expect("durable pre-manifest write timed out")
        .unwrap();
        let before = AdminBuilder::new("shards/root", shards.clone())
            .build()
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap();
        let wal_objects = shards
            .list(Some(&ObjPath::from("shards/root/wal")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(wal_objects.iter().any(|meta| {
            wal_id_for_path("shards/root", &meta.location)
                .is_some_and(|id| id >= before.next_wal_sst_id())
        }));

        let pins = BackupPins {
            topology_store: ops.clone(),
            shard_store: shards.clone(),
            data_store: shards.clone(),
            lifetime: Duration::from_secs(60),
        };
        let snapshot_id = "00000000000000000044-pre-manifest";
        let (state, leases) = tokio::time::timeout(
            Duration::from_secs(5),
            acquire_checkpoint_leases(
                &pins,
                backup.clone(),
                snapshot_id,
                BackupWriteFormat::V2.content_epoch(0),
            ),
        )
        .await
        .expect("pre-manifest WAL protection timed out")
        .unwrap();
        assert!(!state.protected_wals.is_empty());
        tokio::time::timeout(Duration::from_secs(5), db.close())
            .await
            .expect("source close timed out")
            .unwrap();
        for protected in &state.protected_wals {
            shards
                .delete(&ObjPath::from(protected.record.source_path.clone()))
                .await
                .unwrap();
        }

        let report = tokio::time::timeout(
            Duration::from_secs(5),
            snapshot_once_inner(
                &[
                    BackupSource {
                        role: "ops",
                        store: ops,
                    },
                    BackupSource {
                        role: "shard",
                        store: shards.clone(),
                    },
                ],
                backup.clone(),
                SnapshotContext {
                    pins: Some(&pins),
                    pinned_state: Some(&state),
                    started_ms: 44,
                    snapshot_id,
                    fence: None,
                    write_format: BackupWriteFormat::V2,
                    coordinator_epoch: 0,
                    coordinator_sequence: 0,
                },
            ),
        )
        .await
        .expect("pre-manifest WAL inventory timed out")
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
            restored.get(b"pre-manifest").await.unwrap(),
            Some(Bytes::from_static(b"acknowledged"))
        );
        restored.close().await.unwrap();
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
        let (state, leases) = acquire_checkpoint_leases(&pins, backup.clone(), snapshot_id, 0)
            .await
            .unwrap();

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
            SnapshotContext {
                pins: Some(&pins),
                pinned_state: Some(&state),
                started_ms: 42,
                snapshot_id,
                fence: None,
                write_format: BackupWriteFormat::V2,
                coordinator_epoch: 0,
                coordinator_sequence: 0,
            },
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
        let (state, leases) = acquire_checkpoint_leases(&pins, backup.clone(), snapshot_id, 0)
            .await
            .unwrap();
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
            SnapshotContext {
                pins: Some(&pins),
                pinned_state: Some(&state),
                started_ms: 43,
                snapshot_id,
                fence: None,
                write_format: BackupWriteFormat::V2,
                coordinator_epoch: 0,
                coordinator_sequence: 0,
            },
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
            coordinator_epoch: 0,
            coordinator_sequence: 0,
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
