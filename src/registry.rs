//! Ops-bucket control plane: stream registry (CAS'd JSON descriptors, D18/D21)
//! and the dynamic shard topology (D3, §3.2).

use std::collections::{HashMap, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};

use crate::crypto::{hex, stream_hash};

pub const MAX_FORK_CHILDREN: usize = 10_000;
pub const MAX_ACTIVE_HISTORY_DBS: usize = 100_000;
const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;
const MAX_FORK_CHAIN_DEPTH: usize = 1_024;
/// Physical shard-log identity. The stable routing hash is first so a
/// topology prefix is also an exact SlateDB projection range; the second
/// half isolates incarnations (or per-key segments) without changing route.
pub type StorageHash = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDesc {
    /// Tenant isolation owner. Empty only for descriptors written by the
    /// pre-multitenant pilot; those are visible solely to `__legacy__`.
    #[serde(default)]
    pub customer_id: String,
    /// Globally authoritative placement. Empty only for legacy single-cell
    /// descriptors created before cells were enabled. Managed moves change it
    /// only through the fenced `cell_move` state machine below.
    #[serde(default)]
    pub cell: String,
    /// Bounded, restartable placement transition. A completed record is kept
    /// in the descriptor until the next move so retries and audits can resolve
    /// a lost final response without a second data copy.
    #[serde(default)]
    pub cell_move: Option<CellMove>,
    pub name: String,
    /// 16-byte hex; minted per creation, bound into HKDF (V9 mandate).
    pub stream_epoch: String,
    /// One-way key fingerprint; wrong-key requests are rejected with 403.
    pub key_fingerprint: String,
    pub created_ms: i64,
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
    #[serde(default)]
    pub deleted: bool,
    /// Profile kind; None = "generic".
    #[serde(default)]
    pub profile: Option<String>,
    /// Configured content type (create-time config; appends must match).
    #[serde(default = "default_content_type")]
    pub content_type: String,
    /// Raw TTL seconds as configured (config-compare + HEAD reporting).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    /// Ordering contract: None/"total" = single totally ordered sequence
    /// (default; unchanged semantics); "per-key" = segmented per-routing-key
    /// order (PER-KEY-ORDERING.md).
    #[serde(default)]
    pub ordering: Option<String>,
    /// Segment count for per-key streams (v1: static, power of two).
    #[serde(default)]
    pub segment_count: u32,
    /// Queue profile: deliveries before a message is settled to the $dlq
    /// routing-key view (default 5).
    #[serde(default)]
    pub queue_max_deliveries: Option<u32>,
    /// Provisioned stream-level append request and byte limits. `None` exists
    /// only for descriptors created before stream admission was introduced;
    /// those resolve through the deployment defaults.
    #[serde(default)]
    pub append_requests_per_second: Option<u64>,
    #[serde(default)]
    pub append_request_burst: Option<u64>,
    #[serde(default)]
    pub write_bytes_per_second: Option<u64>,
    #[serde(default)]
    pub write_burst_bytes: Option<u64>,
    /// Relative service share among this customer's streams at the shard
    /// committer. Tenant scheduling remains the outer fairness boundary.
    #[serde(default)]
    pub commit_weight: Option<u16>,
    /// Fingerprint of the touch capability token (state-protocol streams):
    /// authorizes /touch/* without granting payload decryption.
    #[serde(default)]
    pub touch_token_fingerprint: Option<String>,
    /// Pinned touch templates (state-protocol): the stream's query families,
    /// declared at creation, durable, loaded when the journal opens. There
    /// is no dynamic template state to lose on restarts or moves.
    #[serde(default)]
    pub touch_templates: Vec<PinnedTemplate>,
    /// Wait-URL signing key (hex, state-protocol): lets the origin verify
    /// the `sig` capability in collapsible wait URLs. Scoped strictly below
    /// the touch token (observation-forging at worst, never decryption).
    #[serde(default)]
    pub touch_sig_key: Option<String>,
    /// Hash of the canonical create-time initial append (body and/or close).
    /// Retries submit it through a reserved durable producer id, while a
    /// different body can never be mistaken for the same create operation.
    #[serde(default)]
    pub initial_request_hash: Option<String>,
    /// Fork creation identity. The inherited prefix is materialized into this
    /// incarnation, but these fields preserve idempotent config comparison and
    /// lifecycle/reference bookkeeping.
    #[serde(default)]
    pub forked_from: Option<String>,
    #[serde(default)]
    pub fork_source_epoch: Option<String>,
    #[serde(default)]
    pub fork_offset: Option<String>,
    #[serde(default)]
    pub fork_sub_offset: Option<u64>,
    /// Idempotent reverse references for soft-delete lifecycle. Materialized
    /// children do not need the source bytes, but the protocol keeps a deleted
    /// source address reserved until its last child is gone.
    #[serde(default)]
    pub fork_children: Vec<String>,
    #[serde(default)]
    pub fork_reference_registered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinnedTemplate {
    pub entity: String,
    pub fields: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellMoveState {
    Preparing,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CellMove {
    pub version: u32,
    pub operation_id: String,
    pub source_cell: String,
    pub target_cell: String,
    pub state: CellMoveState,
    pub started_ms: i64,
    #[serde(default)]
    pub completed_ms: Option<i64>,
    /// Set only after provider-clock retention and an exact target recovery
    /// proof allow the old physical copy to be reclaimed. The source shard
    /// fence itself remains permanent.
    #[serde(default)]
    pub source_cleaned_ms: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CustomerCellAffinity {
    pub version: u32,
    pub cells: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CellMigrationReport {
    pub scanned: usize,
    pub pending_placements: usize,
    pub pending_indices: usize,
    pub migrated_placements: usize,
    pub repaired_indices: usize,
}

/// Immutable, cell-local projection of the globally CAS'd stream descriptor.
/// It is published before the descriptor and revalidated against that source
/// of truth when enumerated, so a failed/racing create can leave only a safe
/// orphan while a live descriptor can never be absent from its cell index.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CellStreamIndex {
    version: u32,
    customer_id: String,
    name: String,
    cell: String,
}

pub struct CellRecoveryEntry {
    pub registry_objects: Vec<ObjectMeta>,
    pub history_db_paths: Vec<String>,
}

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

impl StreamDesc {
    pub fn owner(&self) -> &str {
        if self.customer_id.is_empty() {
            "__legacy__"
        } else {
            &self.customer_id
        }
    }

    pub fn epoch_bytes(&self) -> Option<[u8; 16]> {
        crate::crypto::unhex(&self.stream_epoch)?.try_into().ok()
    }

    pub fn cell_move_in_progress(&self) -> bool {
        self.cell_move
            .as_ref()
            .is_some_and(|movement| movement.state == CellMoveState::Preparing)
    }

    /// Storage identity: derived from (name, stream_epoch) so a recreated
    /// stream gets a fresh keyspace — full delete/recreate isolation.
    pub fn storage_hash(&self) -> StorageHash {
        let incarnation = if self.customer_id.is_empty() {
            // Storage compatibility for pilot descriptors created before
            // customer identity became part of the descriptor.
            crate::crypto::stream_hash(&format!("{}\u{0}inc\u{0}{}", self.name, self.stream_epoch))
        } else {
            crate::crypto::stream_hash(&format!(
                "{}\u{0}{}\u{0}inc\u{0}{}",
                self.customer_id, self.name, self.stream_epoch
            ))
        };
        composite_storage_hash(self.routing_hash(), incarnation)
    }

    pub fn routing_hash(&self) -> [u8; 16] {
        if self.customer_id.is_empty() {
            crate::crypto::stream_hash(&self.name)
        } else {
            crate::crypto::stream_hash(&format!("{}\u{0}{}", self.customer_id, self.name))
        }
    }

    pub fn is_json(&self) -> bool {
        media_type(&self.content_type) == "application/json"
    }

    pub fn is_per_key(&self) -> bool {
        self.ordering.as_deref() == Some("per-key")
    }

    /// Sub-stream identity of one segment of a per-key stream.
    pub fn segment_hash(&self, ordinal: u32) -> StorageHash {
        let identity = if self.customer_id.is_empty() {
            self.name.clone()
        } else {
            format!("{}\u{0}{}", self.customer_id, self.name)
        };
        let segment = crate::crypto::stream_hash(&format!(
            "{}\u{0}seg\u{0}{}\u{0}{}",
            identity, ordinal, self.stream_epoch
        ));
        composite_storage_hash(self.routing_hash(), segment)
    }

    /// Routing key -> segment ordinal (top bits of SHA-256(rk)).
    pub fn segment_for(&self, routing_key: &str) -> u32 {
        let n = self.segment_count.max(1);
        if n == 1 {
            return 0;
        }
        let h = crate::crypto::stream_hash(routing_key);
        let top = u32::from_be_bytes([h[0], h[1], h[2], h[3]]);
        top >> (32 - n.trailing_zeros())
    }
}

pub fn history_db_path(hash: &StorageHash) -> String {
    format!("streams/{}", hex(hash))
}

/// Enumerate the exact active history databases named by durable registry
/// descriptors. Backup and primary integrity actors share this fail-closed
/// implementation so they cannot silently protect different data sets.
pub async fn active_history_db_paths(store: &Arc<dyn ObjectStore>) -> anyhow::Result<Vec<String>> {
    active_history_db_paths_for_cell(store, None).await
}

/// Enumerate active history databases for one cell without scanning the
/// global descriptor namespace. `None` is the legacy single-cell mode.
pub async fn active_history_db_paths_for_cell(
    store: &Arc<dyn ObjectStore>,
    cell_id: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    use futures_util::TryStreamExt;

    let mut paths = std::collections::HashSet::new();
    let prefix = match cell_id {
        Some(cell_id) => {
            anyhow::ensure!(
                crate::cells::valid_cell_id(cell_id),
                "invalid recovery cell id"
            );
            cell_stream_index_prefix(cell_id)
        }
        None => ObjPath::from("registry"),
    };
    let mut listing = store.list(Some(&prefix));
    let mut indexed_streams = 0usize;
    while let Some(meta) = listing.try_next().await? {
        if !meta.location.as_ref().ends_with(".json") {
            continue;
        }
        anyhow::ensure!(
            meta.size <= MAX_DESCRIPTOR_BYTES as u64,
            "registry recovery record is too large"
        );
        let encoded = store.get(&meta.location).await?.bytes().await?;
        let descriptor = if let Some(cell_id) = cell_id {
            let index: CellStreamIndex = serde_json::from_slice(&encoded)?;
            anyhow::ensure!(
                index.version == 1
                    && index.cell == cell_id
                    && !index.customer_id.is_empty()
                    && index.customer_id != "__legacy__"
                    && index.customer_id.len() <= 1_024
                    && !index.name.is_empty()
                    && index.name.len() <= 1_024
                    && cell_stream_index_path(&index.customer_id, &index.name, &index.cell)
                        == meta.location,
                "cell stream index identity is invalid for recovery"
            );
            indexed_streams += 1;
            anyhow::ensure!(
                indexed_streams <= MAX_ACTIVE_HISTORY_DBS,
                "stream index count exceeds the recovery cell bound"
            );
            let descriptor_path = descriptor_path_for(&index.customer_id, &index.name);
            let result = match store.get(&descriptor_path).await {
                Ok(result) => result,
                // A create writes the index first. A missing descriptor is a
                // safe orphan left by a crash or a lost global create race.
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(error.into()),
            };
            anyhow::ensure!(
                result.meta.size <= MAX_DESCRIPTOR_BYTES as u64,
                "registry descriptor is too large for recovery"
            );
            let raw = result.bytes().await?;
            let descriptor: StreamDesc = serde_json::from_slice(&raw)?;
            anyhow::ensure!(
                descriptor.owner() == index.customer_id && descriptor.name == index.name,
                "registry descriptor identity does not match its cell index"
            );
            // A losing placement race, or an explicit future move, leaves the
            // old immutable marker behind. Only the authoritative owner acts.
            if descriptor.cell != cell_id {
                continue;
            }
            descriptor
        } else {
            if !meta.location.as_ref().contains("/by-name/")
                || meta.location.as_ref().contains("/by-cell/")
            {
                continue;
            }
            serde_json::from_slice(&encoded)?
        };
        if cell_id.is_none() {
            anyhow::ensure!(
                descriptor_path_for(descriptor.owner(), &descriptor.name) == meta.location,
                "registry descriptor identity does not match its recovery path"
            );
        }
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
        if descriptor.deleted {
            continue;
        }
        if descriptor.is_per_key() {
            for ordinal in 0..descriptor.segment_count {
                paths.insert(history_db_path(&descriptor.segment_hash(ordinal)));
            }
        } else {
            paths.insert(history_db_path(&descriptor.storage_hash()));
        }
        anyhow::ensure!(
            paths.len() <= MAX_ACTIVE_HISTORY_DBS,
            "active history database count exceeds the recovery cell bound"
        );
    }
    let mut paths: Vec<_> = paths.into_iter().collect();
    paths.sort();
    Ok(paths)
}

fn composite_storage_hash(routing: [u8; 16], incarnation: [u8; 16]) -> StorageHash {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&routing);
    out[16..].copy_from_slice(&incarnation);
    out
}

/// Media type with parameters stripped, lowercased.
pub fn media_type(ct: &str) -> String {
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

pub struct Registry {
    store: Arc<dyn ObjectStore>,
    cache: Mutex<RegistryCache>,
    cache_ttl: Duration,
    limits_cache: Mutex<LimitsCache>,
    limits_cache_ttl: Duration,
}

const DEFAULT_CACHE_CAPACITY: usize = 10_000;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CustomerLimits {
    pub version: u64,
    #[serde(default)]
    pub max_inflight: Option<usize>,
    #[serde(default)]
    pub max_live_connections: Option<usize>,
    #[serde(default)]
    pub write_bytes_per_second: Option<u64>,
    #[serde(default)]
    pub write_burst_bytes: Option<u64>,
    #[serde(default)]
    pub append_requests_per_second: Option<u64>,
    #[serde(default)]
    pub append_request_burst: Option<u64>,
    #[serde(default)]
    pub read_requests_per_second: Option<u64>,
    #[serde(default)]
    pub read_request_burst: Option<u64>,
    #[serde(default)]
    pub read_bytes_per_second: Option<u64>,
    #[serde(default)]
    pub read_burst_bytes: Option<u64>,
    #[serde(default)]
    pub queue_receives_per_second: Option<u64>,
    #[serde(default)]
    pub queue_receive_burst: Option<u64>,
    #[serde(default)]
    pub streams_count: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct StreamQuotaLease {
    customer_id: String,
    owner: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StreamQuotaLeaseDocument {
    version: u64,
    owner: String,
    lease_until_ms: i64,
}

struct CachedLimits {
    value: CustomerLimits,
    inserted_at: Instant,
    generation: u64,
}

struct LimitsCache {
    entries: HashMap<String, CachedLimits>,
    order: VecDeque<(String, u64)>,
    next_generation: u64,
    capacity: usize,
}

impl LimitsCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            capacity: capacity.max(1),
        }
    }

    fn get(&self, customer_id: &str, ttl: Duration) -> Option<CustomerLimits> {
        self.entries
            .get(customer_id)
            .filter(|entry| entry.inserted_at.elapsed() < ttl)
            .map(|entry| entry.value.clone())
    }

    fn insert(&mut self, customer_id: String, value: CustomerLimits) {
        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        self.entries.insert(
            customer_id.clone(),
            CachedLimits {
                value,
                inserted_at: Instant::now(),
                generation,
            },
        );
        self.order.push_back((customer_id, generation));
        while self.entries.len() > self.capacity {
            let Some((candidate, queued_generation)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.generation == queued_generation)
            {
                self.entries.remove(&candidate);
            }
        }
        if self.order.len() > self.capacity.saturating_mul(4) {
            self.order.retain(|(customer, generation)| {
                self.entries
                    .get(customer)
                    .is_some_and(|entry| entry.generation == *generation)
            });
        }
    }
}

struct CachedDesc {
    value: Option<StreamDesc>,
    inserted_at: Instant,
    generation: u64,
}

/// A bounded FIFO cache. The generation stored in the order queue prevents
/// an old queue entry from evicting a newer value for the same stream.
/// Registry lookups are attacker-influenced, so an unbounded HashMap here is
/// a process-memory denial of service even when every lookup is a miss.
struct RegistryCache {
    entries: HashMap<String, CachedDesc>,
    order: VecDeque<(String, u64)>,
    next_generation: u64,
    capacity: usize,
}

impl RegistryCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, name: &str, ttl: Duration) -> Option<Option<StreamDesc>> {
        let entry = self.entries.get(name)?;
        if entry.inserted_at.elapsed() < ttl {
            return Some(entry.value.clone());
        }
        self.entries.remove(name);
        None
    }

    fn insert(&mut self, name: String, value: Option<StreamDesc>) {
        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        self.entries.insert(
            name.clone(),
            CachedDesc {
                value,
                inserted_at: Instant::now(),
                generation,
            },
        );
        self.order.push_back((name, generation));
        while self.entries.len() > self.capacity {
            let Some((candidate, queued_generation)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.generation == queued_generation)
            {
                self.entries.remove(&candidate);
            }
        }
        // Repeated updates of a small key set can otherwise grow the stale
        // order queue forever even though the value map is bounded.
        if self.order.len() > self.capacity.saturating_mul(4) {
            self.order.retain(|(name, generation)| {
                self.entries
                    .get(name)
                    .is_some_and(|entry| entry.generation == *generation)
            });
        }
    }

    fn remove(&mut self, name: &str) {
        self.entries.remove(name);
    }
}

fn desc_path(customer_id: &str, name: &str) -> ObjPath {
    // Hash-keyed path: names are arbitrary UTF-8; the descriptor carries the
    // real name. Two hex chars of fan-out keep prefixes listable.
    let h = hex(&stream_hash(name));
    if customer_id == "__legacy__" {
        ObjPath::from(format!("registry/by-name/{}/{}.json", &h[..2], h))
    } else {
        let customer_hash = hex(&stream_hash(customer_id));
        ObjPath::from(format!(
            "registry/by-customer/{customer_hash}/by-name/{}/{}.json",
            &h[..2],
            h
        ))
    }
}

pub fn cell_stream_index_prefix(cell_id: &str) -> ObjPath {
    // Include the next fixed component so an S3 byte-prefix list for `c-a`
    // can never include sibling `c-aa`.
    ObjPath::from(format!("registry/by-cell/{cell_id}/by-customer"))
}

/// Resolve one immutable cell marker into its authoritative recovery closure.
/// Missing descriptors and markers superseded by another cell are safe
/// orphans. Every returned object was fetched and identity-validated.
pub async fn cell_recovery_entry(
    store: &Arc<dyn ObjectStore>,
    cell_id: &str,
    marker_meta: &ObjectMeta,
) -> anyhow::Result<Option<CellRecoveryEntry>> {
    anyhow::ensure!(
        crate::cells::valid_cell_id(cell_id),
        "invalid recovery cell id"
    );
    anyhow::ensure!(
        marker_meta.location.as_ref().ends_with(".json") && marker_meta.size <= 16 * 1024,
        "cell stream index is invalid for recovery"
    );
    let marker_result = store.get(&marker_meta.location).await?;
    anyhow::ensure!(
        marker_result.meta.size <= 16 * 1024,
        "cell stream index is too large for recovery"
    );
    let marker_object = marker_result.meta.clone();
    let marker_raw = marker_result.bytes().await?;
    let index: CellStreamIndex = serde_json::from_slice(&marker_raw)?;
    anyhow::ensure!(
        index.version == 1
            && index.cell == cell_id
            && !index.customer_id.is_empty()
            && index.customer_id != "__legacy__"
            && index.customer_id.len() <= 1_024
            && !index.name.is_empty()
            && index.name.len() <= 1_024
            && cell_stream_index_path(&index.customer_id, &index.name, &index.cell)
                == marker_meta.location,
        "cell stream index identity is invalid for recovery"
    );
    let descriptor_path = descriptor_path_for(&index.customer_id, &index.name);
    let descriptor_result = match store.get(&descriptor_path).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        descriptor_result.meta.size <= MAX_DESCRIPTOR_BYTES as u64,
        "registry descriptor is too large for recovery"
    );
    let descriptor_object = descriptor_result.meta.clone();
    let descriptor_raw = descriptor_result.bytes().await?;
    let descriptor: StreamDesc = serde_json::from_slice(&descriptor_raw)?;
    validate_descriptor_scope(&descriptor, &index.customer_id, &index.name)?;
    if descriptor.cell != cell_id {
        return Ok(None);
    }
    anyhow::ensure!(
        descriptor.epoch_bytes().is_some(),
        "registry descriptor epoch is invalid for recovery"
    );
    let affinity_path = customer_cell_affinity_path(&index.customer_id);
    let affinity_result = store.get(&affinity_path).await?;
    anyhow::ensure!(
        affinity_result.meta.size <= 16 * 1024,
        "customer cell affinity is too large for recovery"
    );
    let affinity_object = affinity_result.meta.clone();
    let affinity_raw = affinity_result.bytes().await?;
    let affinity: CustomerCellAffinity = serde_json::from_slice(&affinity_raw)?;
    validate_customer_cell_affinity(&affinity)?;
    anyhow::ensure!(
        affinity.cells.iter().any(|cell| cell == cell_id),
        "customer affinity omits its authoritative stream cell"
    );

    let mut history_db_paths = Vec::new();
    if descriptor.is_per_key() {
        anyhow::ensure!(
            (1..=256).contains(&descriptor.segment_count)
                && descriptor.segment_count.is_power_of_two(),
            "registry descriptor has invalid history segments"
        );
        if !descriptor.deleted {
            history_db_paths.extend(
                (0..descriptor.segment_count)
                    .map(|ordinal| history_db_path(&descriptor.segment_hash(ordinal))),
            );
        }
    } else {
        anyhow::ensure!(
            descriptor.ordering.is_none() && descriptor.segment_count == 0,
            "registry descriptor has unsupported history ordering"
        );
        if !descriptor.deleted {
            history_db_paths.push(history_db_path(&descriptor.storage_hash()));
        }
    }
    Ok(Some(CellRecoveryEntry {
        registry_objects: vec![marker_object, descriptor_object, affinity_object],
        history_db_paths,
    }))
}

fn cell_stream_index_path(customer_id: &str, name: &str, cell_id: &str) -> ObjPath {
    let customer_hash = hex(&stream_hash(customer_id));
    let name_hash = hex(&stream_hash(name));
    ObjPath::from(format!(
        "registry/by-cell/{cell_id}/by-customer/{customer_hash}/by-name/{}/{}.json",
        &name_hash[..2],
        name_hash
    ))
}

/// Canonical descriptor location used by recovery enumeration to reject a
/// malformed body whose claimed tenant/name does not match its durable key.
pub(crate) fn descriptor_path_for(customer_id: &str, name: &str) -> ObjPath {
    desc_path(customer_id, name)
}

fn cache_key(customer_id: &str, name: &str) -> String {
    format!("{customer_id}\u{0}{name}")
}

fn customer_limits_path(customer_id: &str) -> ObjPath {
    ObjPath::from(format!(
        "customers/{}/limits.json",
        hex(&stream_hash(customer_id))
    ))
}

fn stream_quota_lease_path(customer_id: &str) -> ObjPath {
    ObjPath::from(format!(
        "customers/{}/stream-quota-lease.json",
        hex(&stream_hash(customer_id))
    ))
}

fn customer_cell_affinity_path(customer_id: &str) -> ObjPath {
    ObjPath::from(format!(
        "customers/{}/cell-affinity.json",
        hex(&stream_hash(customer_id))
    ))
}

fn validate_customer_cell_affinity(
    affinity: &CustomerCellAffinity,
) -> Result<(), object_store::Error> {
    if affinity.version != 1 || affinity.cells.is_empty() {
        return Err(registry_error("invalid customer cell affinity"));
    }
    crate::cells::validate_customer_affinity(&affinity.cells)
        .map_err(|_| registry_error("invalid customer cell affinity"))
}

fn validate_descriptor_scope(
    descriptor: &StreamDesc,
    customer_id: &str,
    name: &str,
) -> Result<(), object_store::Error> {
    if descriptor.owner() != customer_id
        || descriptor.name != name
        || (!descriptor.cell.is_empty() && !crate::cells::valid_cell_id(&descriptor.cell))
    {
        return Err(registry_error(
            "stream descriptor identity does not match its registry path",
        ));
    }
    if descriptor
        .append_requests_per_second
        .is_some_and(|value| value > 1_000_000_000)
        || descriptor
            .append_request_burst
            .is_some_and(|value| value == 0 || value > 1_000_000_000)
        || descriptor
            .write_bytes_per_second
            .is_some_and(|value| value > 1 << 50)
        || descriptor
            .write_burst_bytes
            .is_some_and(|value| value == 0 || value > 1 << 50)
        || descriptor
            .commit_weight
            .is_some_and(|value| !(1..=100).contains(&value))
    {
        return Err(registry_error(
            "stream descriptor has invalid admission limits",
        ));
    }
    if let Some(movement) = &descriptor.cell_move {
        let valid_operation = movement.operation_id.len() == 32
            && movement
                .operation_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        let valid_cells = crate::cells::valid_cell_id(&movement.source_cell)
            && crate::cells::valid_cell_id(&movement.target_cell)
            && movement.source_cell != movement.target_cell;
        let valid_state = match movement.state {
            CellMoveState::Preparing => {
                descriptor.cell == movement.source_cell
                    && movement.completed_ms.is_none()
                    && movement.source_cleaned_ms.is_none()
            }
            CellMoveState::Completed => {
                descriptor.cell == movement.target_cell
                    && movement
                        .completed_ms
                        .is_some_and(|completed| completed >= movement.started_ms)
                    && movement.source_cleaned_ms.is_none_or(|cleaned| {
                        movement
                            .completed_ms
                            .is_some_and(|completed| cleaned >= completed)
                    })
            }
        };
        if movement.version != 1
            || !valid_operation
            || !valid_cells
            || movement.started_ms <= 0
            || !valid_state
        {
            return Err(registry_error(
                "stream descriptor has invalid cell move state",
            ));
        }
    }
    Ok(())
}

impl Registry {
    pub fn new(store: Arc<dyn ObjectStore>) -> Registry {
        Self::with_cache_capacity(store, DEFAULT_CACHE_CAPACITY)
    }

    fn with_cache_capacity(store: Arc<dyn ObjectStore>, cache_capacity: usize) -> Registry {
        Registry {
            store,
            cache: Mutex::new(RegistryCache::new(cache_capacity)),
            cache_ttl: Duration::from_secs(5),
            limits_cache: Mutex::new(LimitsCache::new(cache_capacity)),
            limits_cache_ttl: Duration::from_secs(60),
        }
    }

    pub async fn customer_limits(
        &self,
        customer_id: &str,
    ) -> Result<CustomerLimits, object_store::Error> {
        if let Some(limits) = self
            .limits_cache
            .lock()
            .unwrap()
            .get(customer_id, self.limits_cache_ttl)
        {
            return Ok(limits);
        }
        let limits = match self.store.get(&customer_limits_path(customer_id)).await {
            Ok(result) => {
                let raw = result.bytes().await?;
                let limits = parse_json::<CustomerLimits>(&raw, "customer limits")?;
                if limits.version != 1
                    || limits.max_inflight.is_some_and(|value| value > 1_000_000)
                    || limits
                        .max_live_connections
                        .is_some_and(|value| value > 1_000_000)
                    || limits
                        .write_bytes_per_second
                        .is_some_and(|value| value > 1 << 50)
                    || limits
                        .write_burst_bytes
                        .is_some_and(|value| value == 0 || value > 1 << 50)
                    || limits
                        .append_requests_per_second
                        .is_some_and(|value| value > 1_000_000_000)
                    || limits
                        .append_request_burst
                        .is_some_and(|value| value == 0 || value > 1_000_000_000)
                    || limits
                        .read_requests_per_second
                        .is_some_and(|value| value > 1_000_000_000)
                    || limits
                        .read_request_burst
                        .is_some_and(|value| value == 0 || value > 1_000_000_000)
                    || limits
                        .read_bytes_per_second
                        .is_some_and(|value| value > 1 << 50)
                    || limits
                        .read_burst_bytes
                        .is_some_and(|value| value == 0 || value > 1 << 50)
                    || limits
                        .queue_receives_per_second
                        .is_some_and(|value| value > 1_000_000_000)
                    || limits
                        .queue_receive_burst
                        .is_some_and(|value| value == 0 || value > 1_000_000_000)
                    || limits.streams_count.is_some_and(|value| value > 10_000_000)
                {
                    return Err(registry_error("invalid customer limits"));
                }
                limits
            }
            Err(object_store::Error::NotFound { .. }) => CustomerLimits {
                version: 1,
                ..CustomerLimits::default()
            },
            Err(error) => return Err(error),
        };
        self.limits_cache
            .lock()
            .unwrap()
            .insert(customer_id.to_string(), limits.clone());
        Ok(limits)
    }

    /// Return the durable at-most-four-cell affinity for a customer, creating
    /// the initial one-cell assignment with a create-only CAS. Concurrent
    /// stream creates may propose different cells, but every caller observes
    /// the same winning document before publishing a stream descriptor.
    pub async fn get_or_create_customer_cell_affinity(
        &self,
        customer_id: &str,
        proposed_cell: &str,
    ) -> Result<CustomerCellAffinity, object_store::Error> {
        if customer_id.is_empty()
            || customer_id.len() > 256
            || !crate::cells::valid_cell_id(proposed_cell)
        {
            return Err(registry_error("invalid customer cell placement request"));
        }
        let path = customer_cell_affinity_path(customer_id);
        for _ in 0..5 {
            match self.store.get(&path).await {
                Ok(result) => {
                    if result.meta.size > 16 * 1024 {
                        return Err(registry_error("customer cell affinity is too large"));
                    }
                    let raw = result.bytes().await?;
                    let affinity =
                        parse_json::<CustomerCellAffinity>(&raw, "customer cell affinity")?;
                    validate_customer_cell_affinity(&affinity)?;
                    return Ok(affinity);
                }
                Err(object_store::Error::NotFound { .. }) => {
                    let affinity = CustomerCellAffinity {
                        version: 1,
                        cells: vec![proposed_cell.to_string()],
                    };
                    match self
                        .store
                        .put_opts(
                            &path,
                            PutPayload::from(
                                serde_json::to_vec(&affinity).expect("cell affinity json"),
                            ),
                            PutOptions::from(PutMode::Create),
                        )
                        .await
                    {
                        Ok(_) => return Ok(affinity),
                        Err(object_store::Error::AlreadyExists { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("customer cell affinity create raced"))
    }

    async fn ensure_customer_cell_affinity_contains(
        &self,
        customer_id: &str,
        source_cell: &str,
        target_cell: &str,
    ) -> Result<CustomerCellAffinity, object_store::Error> {
        let path = customer_cell_affinity_path(customer_id);
        for _ in 0..10 {
            let result = self.store.get(&path).await?;
            if result.meta.size > 16 * 1024 {
                return Err(registry_error("customer cell affinity is too large"));
            }
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let raw = result.bytes().await?;
            let mut affinity = parse_json::<CustomerCellAffinity>(&raw, "customer cell affinity")?;
            validate_customer_cell_affinity(&affinity)?;
            if !affinity.cells.iter().any(|cell| cell == source_cell) {
                return Err(registry_error(
                    "customer affinity omits the stream source cell",
                ));
            }
            if affinity.cells.iter().any(|cell| cell == target_cell) {
                return Ok(affinity);
            }
            affinity.cells.push(target_cell.to_string());
            affinity.cells.sort();
            validate_customer_cell_affinity(&affinity)?;
            let encoded = serde_json::to_vec(&affinity).expect("customer cell affinity json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(encoded),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await
            {
                Ok(_) => return Ok(affinity),
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error(
            "customer cell affinity CAS retries exhausted",
        ))
    }

    /// Begin or resume one stream's cross-cell movement. The source remains
    /// authoritative while `Preparing`; the data mover must install its
    /// durable source-shard fence before copying and calling `complete_cell_move`.
    pub async fn begin_cell_move(
        &self,
        customer_id: &str,
        name: &str,
        expected_source_cell: &str,
        target_cell: &str,
        operation_id: &str,
    ) -> Result<StreamDesc, object_store::Error> {
        if customer_id.is_empty()
            || customer_id == "__legacy__"
            || !crate::cells::valid_cell_id(expected_source_cell)
            || !crate::cells::valid_cell_id(target_cell)
            || expected_source_cell == target_cell
            || operation_id.len() != 32
            || !operation_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(registry_error("invalid cell move request"));
        }
        if let Some(current) = self.get(customer_id, name).await?
            && current.cell_move.as_ref().is_some_and(|movement| {
                movement.state == CellMoveState::Completed
                    && movement.operation_id == operation_id
                    && movement.source_cell == expected_source_cell
                    && movement.target_cell == target_cell
            })
        {
            return Ok(current);
        }
        let directory = crate::cells::load(&self.store).await?;
        if directory.get(expected_source_cell).is_none()
            || !directory.get(target_cell).is_some_and(|cell| {
                cell.state == crate::cells::CellState::Active && cell.weight > 0
            })
        {
            return Err(registry_error(
                "cell move source or placement-eligible target is absent",
            ));
        }
        self.ensure_customer_cell_affinity_contains(customer_id, expected_source_cell, target_cell)
            .await?;

        let path = desc_path(customer_id, name);
        for _ in 0..10 {
            let result = self.store.get(&path).await?;
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            if result.meta.size > MAX_DESCRIPTOR_BYTES as u64 {
                return Err(registry_error("stream descriptor is too large"));
            }
            let raw = result.bytes().await?;
            let mut descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            if descriptor.cell_move.as_ref().is_some_and(|movement| {
                movement.operation_id == operation_id
                    && movement.source_cell == expected_source_cell
                    && movement.target_cell == target_cell
            }) {
                return Ok(descriptor);
            }
            if descriptor
                .cell_move
                .as_ref()
                .is_some_and(|movement| movement.state == CellMoveState::Preparing)
            {
                return Err(registry_error(
                    "stream already has a different cell move in progress",
                ));
            }
            if descriptor.cell_move.as_ref().is_some_and(|movement| {
                movement.state == CellMoveState::Completed && movement.source_cleaned_ms.is_none()
            }) {
                return Err(registry_error(
                    "prior cell move source retention cleanup is still pending",
                ));
            }
            if descriptor.deleted || descriptor.cell != expected_source_cell {
                return Err(registry_error(
                    "stream is deleted or no longer belongs to the expected source cell",
                ));
            }
            let started_ms = chrono::Utc::now().timestamp_millis();
            descriptor.cell_move = Some(CellMove {
                version: 1,
                operation_id: operation_id.to_string(),
                source_cell: expected_source_cell.to_string(),
                target_cell: target_cell.to_string(),
                state: CellMoveState::Preparing,
                started_ms,
                completed_ms: None,
                source_cleaned_ms: None,
            });
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            let mut target_descriptor = descriptor.clone();
            target_descriptor.cell = target_cell.to_string();
            target_descriptor.cell_move = None;
            self.ensure_cell_stream_index(&target_descriptor).await?;
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(
                        serde_json::to_vec(&descriptor).expect("stream descriptor json"),
                    ),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(customer_id, name);
                    return Ok(descriptor);
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("begin cell move CAS retries exhausted"))
    }

    /// Linearize a fully copied stream onto its target cell. A retry after a
    /// lost response resolves the retained completed operation id instead of
    /// initiating another placement mutation.
    pub async fn complete_cell_move(
        &self,
        customer_id: &str,
        name: &str,
        operation_id: &str,
    ) -> Result<StreamDesc, object_store::Error> {
        let path = desc_path(customer_id, name);
        for _ in 0..10 {
            let result = self.store.get(&path).await?;
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            if result.meta.size > MAX_DESCRIPTOR_BYTES as u64 {
                return Err(registry_error("stream descriptor is too large"));
            }
            let raw = result.bytes().await?;
            let mut descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            let Some(movement) = descriptor.cell_move.as_ref() else {
                return Err(registry_error("stream has no cell move to complete"));
            };
            if movement.operation_id != operation_id {
                return Err(registry_error("cell move operation id does not match"));
            }
            if movement.state == CellMoveState::Completed {
                return Ok(descriptor);
            }
            let target = movement.target_cell.clone();
            let mut completed = movement.clone();
            descriptor.cell = target;
            completed.state = CellMoveState::Completed;
            completed.completed_ms = Some(chrono::Utc::now().timestamp_millis());
            descriptor.cell_move = Some(completed);
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            self.ensure_cell_stream_index(&descriptor).await?;
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(
                        serde_json::to_vec(&descriptor).expect("stream descriptor json"),
                    ),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(customer_id, name);
                    return Ok(descriptor);
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("complete cell move CAS retries exhausted"))
    }

    /// Prove, entirely on the registry provider's clock, that a completed
    /// move descriptor has remained unchanged for the required rollback
    /// window. The returned bytes are the exact authoritative object body and
    /// are subsequently matched against the target recovery point.
    #[allow(clippy::too_many_arguments)]
    pub async fn completed_cell_move_retention_proof(
        &self,
        customer_id: &str,
        name: &str,
        source_cell: &str,
        target_cell: &str,
        operation_id: &str,
        minimum_retention: Duration,
    ) -> Result<(StreamDesc, Vec<u8>), object_store::Error> {
        let path = desc_path(customer_id, name);
        let result = self.store.get(&path).await?;
        if result.meta.size > MAX_DESCRIPTOR_BYTES as u64 {
            return Err(registry_error("stream descriptor is too large"));
        }
        let descriptor_meta = result.meta.clone();
        let raw = result.bytes().await?;
        let descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
        validate_descriptor_scope(&descriptor, customer_id, name)?;
        let Some(movement) = descriptor.cell_move.as_ref() else {
            return Err(registry_error("stream has no retained cell move"));
        };
        if movement.state != CellMoveState::Completed
            || movement.operation_id != operation_id
            || movement.source_cell != source_cell
            || movement.target_cell != target_cell
            || descriptor.cell != target_cell
        {
            return Err(registry_error(
                "stream does not match the completed cell move cleanup request",
            ));
        }
        if movement.source_cleaned_ms.is_some() {
            return Ok((descriptor, raw.to_vec()));
        }
        if !descriptor.fork_children.is_empty() {
            return Err(registry_error(
                "cell move source cleanup waits for all fork children to be released",
            ));
        }

        let probe = ObjPath::from(format!(
            "_cell_move_cleanup_clock/{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        self.store
            .put_opts(
                &probe,
                PutPayload::from(operation_id.to_string()),
                PutOptions::from(PutMode::Create),
            )
            .await?;
        let probe_meta = match self.store.head(&probe).await {
            Ok(meta) => meta,
            Err(error) => {
                let _ = self.store.delete(&probe).await;
                return Err(error);
            }
        };
        self.store.delete(&probe).await?;
        let age = probe_meta
            .last_modified
            .signed_duration_since(descriptor_meta.last_modified)
            .to_std()
            .map_err(|_| registry_error("registry provider clock regressed"))?;
        if age < minimum_retention {
            return Err(registry_error(
                "completed cell move has not passed its provider-clock rollback window",
            ));
        }
        Ok((descriptor, raw.to_vec()))
    }

    /// Retain the move identity while recording that its old physical copy is
    /// gone. A crash before this CAS simply makes cleanup retry its idempotent
    /// exact-range/object deletion.
    pub async fn complete_cell_move_source_cleanup(
        &self,
        customer_id: &str,
        name: &str,
        operation_id: &str,
    ) -> Result<StreamDesc, object_store::Error> {
        let path = desc_path(customer_id, name);
        for _ in 0..10 {
            let result = self.store.get(&path).await?;
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            if result.meta.size > MAX_DESCRIPTOR_BYTES as u64 {
                return Err(registry_error("stream descriptor is too large"));
            }
            let raw = result.bytes().await?;
            let mut descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            let Some(movement) = descriptor.cell_move.as_mut() else {
                return Err(registry_error("stream has no retained cell move"));
            };
            if movement.state != CellMoveState::Completed || movement.operation_id != operation_id {
                return Err(registry_error(
                    "cell move cleanup operation id does not match",
                ));
            }
            if movement.source_cleaned_ms.is_some() {
                return Ok(descriptor);
            }
            movement.source_cleaned_ms = Some(chrono::Utc::now().timestamp_millis());
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(
                        serde_json::to_vec(&descriptor).expect("stream descriptor json"),
                    ),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(customer_id, name);
                    return Ok(descriptor);
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error(
            "complete cell move source cleanup CAS retries exhausted",
        ))
    }

    async fn ensure_cell_stream_index(
        &self,
        descriptor: &StreamDesc,
    ) -> Result<(), object_store::Error> {
        if descriptor.cell.is_empty() {
            return Ok(());
        }
        let index = CellStreamIndex {
            version: 1,
            customer_id: descriptor.owner().to_string(),
            name: descriptor.name.clone(),
            cell: descriptor.cell.clone(),
        };
        if index.customer_id == "__legacy__"
            || index.customer_id.is_empty()
            || index.customer_id.len() > 1_024
            || index.name.is_empty()
            || index.name.len() > 1_024
            || !crate::cells::valid_cell_id(&index.cell)
        {
            return Err(registry_error("invalid cell stream index identity"));
        }
        let path = cell_stream_index_path(&index.customer_id, &index.name, &index.cell);
        let body = PutPayload::from(serde_json::to_vec(&index).expect("cell stream index json"));
        match self
            .store
            .put_opts(&path, body, PutOptions::from(PutMode::Create))
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let result = self.store.get(&path).await?;
                if result.meta.size > 16 * 1024 {
                    return Err(registry_error("cell stream index is too large"));
                }
                let raw = result.bytes().await?;
                let current = parse_json::<CellStreamIndex>(&raw, "cell stream index")?;
                if current != index {
                    return Err(registry_error("cell stream index identity mismatch"));
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn cell_stream_index_exists(
        &self,
        descriptor: &StreamDesc,
    ) -> Result<bool, object_store::Error> {
        if descriptor.cell.is_empty() {
            return Ok(false);
        }
        let expected = CellStreamIndex {
            version: 1,
            customer_id: descriptor.owner().to_string(),
            name: descriptor.name.clone(),
            cell: descriptor.cell.clone(),
        };
        let path = cell_stream_index_path(&expected.customer_id, &expected.name, &expected.cell);
        match self.store.get(&path).await {
            Ok(result) => {
                if result.meta.size > 16 * 1024 {
                    return Err(registry_error("cell stream index is too large"));
                }
                let raw = result.bytes().await?;
                let current = parse_json::<CellStreamIndex>(&raw, "cell stream index")?;
                if current != expected {
                    return Err(registry_error("cell stream index identity mismatch"));
                }
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn assign_descriptor_cell(
        &self,
        customer_id: &str,
        name: &str,
        cell_id: &str,
    ) -> Result<bool, object_store::Error> {
        let path = desc_path(customer_id, name);
        for _ in 0..10 {
            let result = self.store.get(&path).await?;
            let etag = result.meta.e_tag.clone();
            if result.meta.size > MAX_DESCRIPTOR_BYTES as u64 {
                return Err(registry_error("stream descriptor is too large"));
            }
            let raw = result.bytes().await?;
            let mut descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&descriptor, customer_id, name)?;
            if !descriptor.cell.is_empty() && descriptor.cell != cell_id {
                return Err(registry_error(
                    "stream descriptor is already assigned to a different cell",
                ));
            }
            if descriptor.cell == cell_id {
                self.ensure_cell_stream_index(&descriptor).await?;
                return Ok(false);
            }
            descriptor.cell = cell_id.to_string();
            self.ensure_cell_stream_index(&descriptor).await?;
            let body = serde_json::to_vec(&descriptor).expect("stream descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(customer_id, name);
                    return Ok(true);
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("descriptor placement CAS retries exhausted"))
    }

    /// Audit or perform the only supported no-placement migration: every
    /// descriptor is assigned to the sole active cell before a second cell is
    /// admitted. Serving must be quiesced by the caller while `apply` is true
    /// so a concurrent create cannot land behind the strongly-consistent list.
    pub async fn migrate_single_cell_descriptors(
        &self,
        cell_id: &str,
        max_descriptors: usize,
        apply: bool,
    ) -> anyhow::Result<CellMigrationReport> {
        use futures_util::TryStreamExt;

        anyhow::ensure!(
            crate::cells::valid_cell_id(cell_id),
            "invalid migration cell id"
        );
        anyhow::ensure!(
            (1..=10_000_000).contains(&max_descriptors),
            "max descriptors must be between 1 and 10000000"
        );
        let directory = crate::cells::load(&self.store).await?;
        anyhow::ensure!(
            directory.cells.len() == 1
                && directory.cells[0].cell_id == cell_id
                && directory.cells[0].state == crate::cells::CellState::Active
                && directory.cells[0].weight > 0,
            "placement migration requires cells.json to contain exactly the target active cell"
        );

        let mut report = CellMigrationReport::default();
        let mut listing = self.store.list(Some(&ObjPath::from("registry")));
        while let Some(meta) = listing.try_next().await? {
            let location = meta.location.as_ref();
            if !location.ends_with(".json")
                || !location.contains("/by-name/")
                || location.contains("/by-cell/")
            {
                continue;
            }
            report.scanned += 1;
            anyhow::ensure!(
                report.scanned <= max_descriptors,
                "descriptor count exceeds the explicit migration bound"
            );
            anyhow::ensure!(
                meta.size <= MAX_DESCRIPTOR_BYTES as u64,
                "stream descriptor is too large"
            );
            let raw = self.store.get(&meta.location).await?.bytes().await?;
            let descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&descriptor, descriptor.owner(), &descriptor.name)?;
            anyhow::ensure!(
                descriptor_path_for(descriptor.owner(), &descriptor.name) == meta.location,
                "stream descriptor identity does not match its registry path"
            );
            anyhow::ensure!(
                descriptor.owner() != "__legacy__",
                "legacy pilot descriptors have no tenant identity and cannot enter managed cells"
            );
            anyhow::ensure!(
                descriptor.cell.is_empty() || descriptor.cell == cell_id,
                "descriptor is assigned outside the sole migration cell"
            );

            if descriptor.cell.is_empty() {
                report.pending_placements += 1;
                if !apply {
                    continue;
                }
                let affinity = self
                    .get_or_create_customer_cell_affinity(descriptor.owner(), cell_id)
                    .await?;
                anyhow::ensure!(
                    affinity.cells == [cell_id],
                    "customer affinity is not confined to the sole migration cell"
                );
                if self
                    .assign_descriptor_cell(descriptor.owner(), &descriptor.name, cell_id)
                    .await?
                {
                    report.migrated_placements += 1;
                }
            } else if !self.cell_stream_index_exists(&descriptor).await? {
                report.pending_indices += 1;
                if apply {
                    self.ensure_cell_stream_index(&descriptor).await?;
                    report.repaired_indices += 1;
                }
            }
        }
        Ok(report)
    }

    /// Serialize the count-and-create decision for one customer. The lease is
    /// deliberately short-lived and reverified immediately before a
    /// descriptor CAS; a canceled request can reduce create availability for
    /// at most 30 seconds but cannot permanently strand the account.
    pub async fn acquire_stream_quota_lease(
        &self,
        customer_id: &str,
    ) -> Result<StreamQuotaLease, object_store::Error> {
        const LEASE_MS: i64 = 30_000;
        let mut random = [0u8; 16];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut random);
        let owner = hex(&random);
        let path = stream_quota_lease_path(customer_id);
        for _ in 0..120 {
            let now = chrono::Utc::now().timestamp_millis();
            let desired = StreamQuotaLeaseDocument {
                version: 1,
                owner: owner.clone(),
                lease_until_ms: now.saturating_add(LEASE_MS),
            };
            let body = PutPayload::from(serde_json::to_vec(&desired).expect("quota lease json"));
            match self.store.get(&path).await {
                Ok(result) => {
                    let etag = result.meta.e_tag.clone();
                    let raw = result.bytes().await?;
                    let current =
                        parse_json::<StreamQuotaLeaseDocument>(&raw, "stream quota lease")?;
                    if current.version != 1
                        || current.owner.len() > 64
                        || current.lease_until_ms > now.saturating_add(300_000)
                    {
                        return Err(registry_error("invalid stream quota lease"));
                    }
                    if !current.owner.is_empty() && current.lease_until_ms > now {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    match self
                        .store
                        .put_opts(
                            &path,
                            body,
                            PutOptions::from(PutMode::Update(UpdateVersion {
                                e_tag: etag,
                                version: None,
                            })),
                        )
                        .await
                    {
                        Ok(_) => {
                            return Ok(StreamQuotaLease {
                                customer_id: customer_id.to_string(),
                                owner,
                            });
                        }
                        Err(object_store::Error::Precondition { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
                Err(object_store::Error::NotFound { .. }) => {
                    match self
                        .store
                        .put_opts(&path, body, PutOptions::from(PutMode::Create))
                        .await
                    {
                        Ok(_) => {
                            return Ok(StreamQuotaLease {
                                customer_id: customer_id.to_string(),
                                owner,
                            });
                        }
                        Err(object_store::Error::AlreadyExists { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("stream quota lease is busy"))
    }

    pub async fn verify_stream_quota_lease(
        &self,
        lease: &StreamQuotaLease,
    ) -> Result<(), object_store::Error> {
        const LEASE_MS: i64 = 30_000;
        let path = stream_quota_lease_path(&lease.customer_id);
        for _ in 0..5 {
            let result = self.store.get(&path).await?;
            let etag = result.meta.e_tag.clone();
            let raw = result.bytes().await?;
            let mut current = parse_json::<StreamQuotaLeaseDocument>(&raw, "stream quota lease")?;
            let now = chrono::Utc::now().timestamp_millis();
            if current.version != 1 || current.owner != lease.owner || current.lease_until_ms <= now
            {
                return Err(registry_error("stream quota lease was lost"));
            }
            current.lease_until_ms = now.saturating_add(LEASE_MS);
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(
                        serde_json::to_vec(&current).expect("stream quota lease json"),
                    ),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("stream quota lease verification raced"))
    }

    pub async fn release_stream_quota_lease(
        &self,
        lease: &StreamQuotaLease,
    ) -> Result<(), object_store::Error> {
        let path = stream_quota_lease_path(&lease.customer_id);
        for _ in 0..5 {
            let result = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(()),
                Err(error) => return Err(error),
            };
            let etag = result.meta.e_tag.clone();
            let raw = result.bytes().await?;
            let mut current = parse_json::<StreamQuotaLeaseDocument>(&raw, "stream quota lease")?;
            if current.owner != lease.owner {
                return Ok(());
            }
            current.owner.clear();
            current.lease_until_ms = 0;
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(
                        serde_json::to_vec(&current).expect("stream quota lease json"),
                    ),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub async fn get(
        &self,
        customer_id: &str,
        name: &str,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        let key = cache_key(customer_id, name);
        if let Some(desc) = self.cache.lock().unwrap().get(&key, self.cache_ttl) {
            return Ok(desc);
        }
        let fetched = match self.store.get(&desc_path(customer_id, name)).await {
            Ok(r) => {
                let raw = r.bytes().await?;
                let descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
                validate_descriptor_scope(&descriptor, customer_id, name)?;
                Some(descriptor)
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => return Err(e),
        };
        // A create may publish through another cell. There is no cross-process
        // invalidation channel, so caching absence would turn a successful
        // global descriptor CAS into transient wrong-cell 404s. Positive
        // descriptors are placement-stable outside an operator move. During a
        // move, a stale source cache is stopped by the durable shard fence and
        // a stale target cache may replay/503 only until this short TTL;
        // authenticated miss traffic is bounded by tenant admission.
        if fetched.is_some() {
            self.cache.lock().unwrap().insert(key, fetched.clone());
        }
        Ok(fetched)
    }

    /// Create a descriptor; on a lost CAS race, return the winner's.
    pub async fn create(
        &self,
        desc: StreamDesc,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        let customer_id = desc.owner().to_string();
        validate_descriptor_scope(&desc, &customer_id, &desc.name)?;
        self.ensure_cell_stream_index(&desc).await?;
        let raw = serde_json::to_vec(&desc).expect("desc json");
        let key = cache_key(&customer_id, &desc.name);
        match self
            .store
            .put_opts(
                &desc_path(&customer_id, &desc.name),
                PutPayload::from(raw),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => {
                self.cache.lock().unwrap().insert(key, Some(desc.clone()));
                Ok((true, desc))
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.invalidate(&customer_id, &desc.name);
                let existing = self.get(&customer_id, &desc.name).await?.ok_or_else(|| {
                    object_store::Error::NotFound {
                        path: desc.name.clone(),
                        source: "raced create then missing".into(),
                    }
                })?;
                Ok((false, existing))
            }
            Err(e) => Err(e),
        }
    }

    /// Replace exactly one observed dead descriptor with a fresh incarnation.
    /// A concurrent recreator can win, in which case its descriptor is
    /// returned with `created=false`; it is never overwritten. Comparing the
    /// epoch is what makes delete/recreate a linearizable identity change.
    pub async fn recreate(
        &self,
        customer_id: &str,
        name: &str,
        expected_epoch: &str,
        fresh: StreamDesc,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        validate_descriptor_scope(&fresh, customer_id, name)?;
        self.ensure_cell_stream_index(&fresh).await?;
        let path = desc_path(customer_id, name);
        for _ in 0..5 {
            let got = match self.store.get(&path).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(object_store::Error::NotFound {
                        path: name.to_string(),
                        source: "recreate on missing descriptor".into(),
                    });
                }
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let current = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&current, customer_id, name)?;
            if current.cell_move_in_progress() {
                return Err(registry_error("stream cell move is in progress"));
            }
            if current.stream_epoch != expected_epoch {
                self.cache
                    .lock()
                    .unwrap()
                    .insert(cache_key(customer_id, name), Some(current.clone()));
                return Ok((false, current));
            }
            if current.cell != fresh.cell {
                self.cache
                    .lock()
                    .unwrap()
                    .insert(cache_key(customer_id, name), Some(current.clone()));
                return Ok((false, current));
            }
            if current.cell_move.as_ref().is_some_and(|movement| {
                movement.state == CellMoveState::Completed && movement.source_cleaned_ms.is_none()
            }) {
                return Err(registry_error(
                    "cell-moved stream cannot be recreated before source cleanup",
                ));
            }
            let body = serde_json::to_vec(&fresh).map_err(|e| object_store::Error::Generic {
                store: "registry",
                source: Box::new(e),
            })?;
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, name), Some(fresh.clone()));
                    return Ok((true, fresh));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        // Resolve an exhausted race to the winner instead of letting a
        // transient CAS storm turn into an identity overwrite on retry.
        self.invalidate(customer_id, name);
        self.get(customer_id, name)
            .await?
            .map(|d| (false, d))
            .ok_or_else(|| object_store::Error::NotFound {
                path: name.to_string(),
                source: "recreate race ended with missing descriptor".into(),
            })
    }

    /// CAS-update the descriptor (delete = tombstone).
    pub async fn update<F: Fn(&mut StreamDesc)>(
        &self,
        customer_id: &str,
        name: &str,
        apply: F,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let got = match self.store.get(&desc_path(customer_id, name)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut desc = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&desc, customer_id, name)?;
            if desc.cell_move_in_progress() {
                return Err(registry_error("stream cell move is in progress"));
            }
            apply(&mut desc);
            validate_descriptor_scope(&desc, customer_id, name)?;
            let body = serde_json::to_vec(&desc).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(customer_id, name),
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(customer_id, name);
                    return Ok(Some(desc));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "descriptor CAS retries exhausted".into(),
        })
    }

    /// Renew a sliding TTL with a descriptor CAS. Reads and writes on TTL
    /// streams are intentionally durable control-plane mutations; HEAD and
    /// fixed Stream-Expires-At streams do not call this path.
    pub async fn renew_ttl(
        &self,
        customer_id: &str,
        name: &str,
        expected_epoch: &str,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut desc = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&desc, customer_id, name)?;
            if desc.cell_move_in_progress() {
                return Err(registry_error("stream cell move is in progress"));
            }
            let now = chrono::Utc::now().timestamp_millis();
            let Some(ttl_secs) = desc.ttl_secs else {
                return Ok(Some(desc));
            };
            if desc.stream_epoch != expected_epoch
                || desc.deleted
                || desc.expires_at_ms.is_some_and(|expires| expires <= now)
            {
                return Ok(Some(desc));
            }
            let ttl_ms = i64::try_from(ttl_secs)
                .ok()
                .and_then(|ttl| ttl.checked_mul(1000))
                .and_then(|ttl| now.checked_add(ttl))
                .ok_or_else(|| registry_error("TTL expiry overflow"))?;
            desc.expires_at_ms = Some(ttl_ms);
            let body = serde_json::to_vec(&desc).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, name), Some(desc.clone()));
                    return Ok(Some(desc));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "TTL renewal CAS retries exhausted".into(),
        })
    }

    pub async fn add_fork_child(
        &self,
        customer_id: &str,
        source_name: &str,
        expected_source_epoch: &str,
        child_name: &str,
    ) -> Result<bool, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, source_name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut source = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&source, customer_id, source_name)?;
            if source.cell_move_in_progress() {
                return Err(registry_error("stream cell move is in progress"));
            }
            if source.stream_epoch != expected_source_epoch {
                return Ok(false);
            }
            if source.fork_children.iter().any(|child| child == child_name) {
                return Ok(true);
            }
            if source.deleted {
                return Ok(false);
            }
            if source.fork_children.len() >= MAX_FORK_CHILDREN {
                return Err(registry_error("fork child limit reached"));
            }
            source.fork_children.push(child_name.to_string());
            let body = serde_json::to_vec(&source).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, source_name), Some(source));
                    return Ok(true);
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("fork reference CAS retries exhausted"))
    }

    pub async fn remove_fork_child(
        &self,
        customer_id: &str,
        source_name: &str,
        expected_source_epoch: &str,
        child_name: &str,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, source_name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut source = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&source, customer_id, source_name)?;
            if source.cell_move_in_progress() {
                return Err(registry_error("stream cell move is in progress"));
            }
            if source.stream_epoch != expected_source_epoch {
                return Ok(None);
            }
            let before = source.fork_children.len();
            source.fork_children.retain(|child| child != child_name);
            if source.fork_children.len() == before {
                return Ok(Some(source));
            }
            let body = serde_json::to_vec(&source).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, source_name), Some(source.clone()));
                    return Ok(Some(source));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error(
            "fork reference removal CAS retries exhausted",
        ))
    }

    /// Release a dead leaf's reverse reference and recursively release any
    /// dead ancestors that become childless. An intermediate fork must retain
    /// its own parent reference while it still has a child; otherwise deleting
    /// the middle of a chain makes the root appear fully collected too early.
    pub async fn release_fork_chain(
        &self,
        customer_id: &str,
        leaf: &StreamDesc,
    ) -> Result<(), object_store::Error> {
        if !leaf.fork_children.is_empty() || !leaf.fork_reference_registered {
            return Ok(());
        }

        let mut child_name = leaf.name.clone();
        let mut parent_name = leaf.forked_from.clone();
        let mut parent_epoch = leaf.fork_source_epoch.clone();
        for _ in 0..MAX_FORK_CHAIN_DEPTH {
            let (Some(name), Some(epoch)) = (parent_name.as_deref(), parent_epoch.as_deref())
            else {
                return Ok(());
            };
            let Some(parent) = self
                .remove_fork_child(customer_id, name, epoch, &child_name)
                .await?
            else {
                return Ok(());
            };

            let expired = parent
                .expires_at_ms
                .is_some_and(|expires| expires <= chrono::Utc::now().timestamp_millis());
            if (!parent.deleted && !expired) || !parent.fork_children.is_empty() {
                return Ok(());
            }
            child_name = parent.name.clone();
            parent_name = parent.forked_from.clone();
            parent_epoch = parent.fork_source_epoch.clone();
            if !parent.fork_reference_registered {
                return Ok(());
            }
        }
        Err(registry_error("fork chain exceeds maximum depth"))
    }

    /// Tombstone only the incarnation the caller actually observed.
    pub async fn mark_deleted(
        &self,
        customer_id: &str,
        name: &str,
        expected_epoch: &str,
        expected_cell: &str,
    ) -> Result<Option<(bool, StreamDesc)>, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut desc = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&desc, customer_id, name)?;
            if desc.cell_move_in_progress() {
                return Err(registry_error("stream cell move is in progress"));
            }
            if desc.stream_epoch != expected_epoch || desc.cell != expected_cell || desc.deleted {
                return Ok(Some((false, desc)));
            }
            desc.deleted = true;
            let body = serde_json::to_vec(&desc).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, name), Some(desc.clone()));
                    return Ok(Some((true, desc)));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("delete CAS retries exhausted"))
    }

    pub fn invalidate(&self, customer_id: &str, name: &str) {
        self.cache
            .lock()
            .unwrap()
            .remove(&cache_key(customer_id, name));
    }

    pub async fn list(
        &self,
        customer_id: &str,
        limit: usize,
    ) -> Result<Vec<StreamDesc>, object_store::Error> {
        use futures_util::TryStreamExt;
        let prefix = if customer_id == "__legacy__" {
            ObjPath::from("registry/by-name")
        } else {
            ObjPath::from(format!(
                "registry/by-customer/{}/by-name",
                hex(&stream_hash(customer_id))
            ))
        };
        let mut out = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await? {
            if out.len() >= limit {
                break;
            }
            let result = self.store.get(&meta.location).await?;
            let raw = result.bytes().await?;
            let d = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            if d.owner() != customer_id {
                return Err(registry_error(
                    "stream descriptor owner does not match listing prefix",
                ));
            }
            if !d.deleted {
                out.push(d);
            }
        }
        Ok(out)
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(
    raw: &[u8],
    kind: &'static str,
) -> Result<T, object_store::Error> {
    serde_json::from_slice(raw).map_err(|e| object_store::Error::Generic {
        store: "registry",
        source: format!("corrupt {kind}: {e}").into(),
    })
}

// ---- shard topology ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub version: u64,
    /// Physical shard-key layout. v2 is
    /// `[routing_hash16][incarnation_or_segment16][kind][suffix]`.
    #[serde(default)]
    pub storage_format: u32,
    /// Complete binary prefix code over the stream-hash bit space. "" = one
    /// shard covering everything.
    pub shards: Vec<String>,
    /// Generation-specific DB paths for online split children. Unmapped
    /// shards use the canonical `shards/<prefix>` path.
    #[serde(default)]
    pub shard_paths: HashMap<String, String>,
}

impl Topology {
    pub fn db_path(&self, prefix: &str) -> String {
        self.shard_paths.get(prefix).cloned().unwrap_or_else(|| {
            if prefix.is_empty() {
                "shards/root".to_string()
            } else {
                format!("shards/{prefix}")
            }
        })
    }
}

const TOPOLOGY_PATH: &str = "topology.json";

pub async fn load_topology(store: &Arc<dyn ObjectStore>) -> Result<Topology, object_store::Error> {
    let result = store.get(&ObjPath::from(TOPOLOGY_PATH)).await?;
    let raw = result.bytes().await?;
    let topology = parse_json::<Topology>(&raw, "topology")?;
    validate_topology(&topology)?;
    Ok(topology)
}

/// Lexicographic SlateDB projection bounds for a routing-bit prefix. Physical
/// keys begin with the full 16-byte routing hash, so the range remains exact
/// even though incarnation bytes and record suffixes follow it.
pub fn shard_projection_bounds(
    prefix: &str,
) -> Result<(Bound<Bytes>, Bound<Bytes>), object_store::Error> {
    if prefix.len() > 128 || !prefix.bytes().all(|bit| bit == b'0' || bit == b'1') {
        return Err(registry_error("invalid shard projection prefix"));
    }
    if prefix.is_empty() {
        return Ok((Bound::Unbounded, Bound::Unbounded));
    }
    let mut value = 0u128;
    for bit in prefix.bytes() {
        value = (value << 1) | u128::from(bit == b'1');
    }
    let shift = 128 - prefix.len();
    let lower = value << shift;
    let upper = if prefix.bytes().all(|bit| bit == b'1') {
        None
    } else {
        Some((value + 1) << shift)
    };
    Ok((
        Bound::Included(Bytes::copy_from_slice(&lower.to_be_bytes())),
        upper
            .map(|upper| Bound::Excluded(Bytes::copy_from_slice(&upper.to_be_bytes())))
            .unwrap_or(Bound::Unbounded),
    ))
}

/// Publish `parent -> parent0,parent1` with one topology CAS. The caller must
/// have created and durably verified both projected child DBs first; keeping
/// the data-plane work outside this function makes the final visibility step
/// a single linearization point.
pub async fn cas_publish_topology_split(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
    expected_version: u64,
) -> Result<Topology, object_store::Error> {
    cas_publish_topology_split_with_paths(store, parent, expected_version, None).await
}

pub async fn cas_publish_topology_split_with_paths(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
    expected_version: u64,
    child_paths: Option<(&str, &str)>,
) -> Result<Topology, object_store::Error> {
    let path = ObjPath::from(TOPOLOGY_PATH);
    let result = store.get(&path).await?;
    let etag = result.meta.e_tag.clone();
    let raw = result.bytes().await?;
    let mut topology = parse_json::<Topology>(&raw, "topology")?;
    validate_topology(&topology)?;
    if topology.version != expected_version {
        return Err(registry_error(
            "topology version changed before split publish",
        ));
    }
    let Some(index) = topology.shards.iter().position(|prefix| prefix == parent) else {
        return Err(registry_error("split parent is not in the live topology"));
    };
    if parent.len() >= 128 {
        return Err(registry_error("maximum shard-prefix depth reached"));
    }
    topology.shards.remove(index);
    topology.shard_paths.remove(parent);
    topology.shards.push(format!("{parent}0"));
    topology.shards.push(format!("{parent}1"));
    if let Some((zero_path, one_path)) = child_paths {
        topology
            .shard_paths
            .insert(format!("{parent}0"), zero_path.to_string());
        topology
            .shard_paths
            .insert(format!("{parent}1"), one_path.to_string());
    }
    topology.shards.sort();
    topology.version = topology
        .version
        .checked_add(1)
        .ok_or_else(|| registry_error("topology version overflow"))?;
    validate_topology(&topology)?;
    let body = serde_json::to_vec(&topology).expect("topology json");
    store
        .put_opts(
            &path,
            PutPayload::from(body),
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: etag,
                version: None,
            })),
        )
        .await?;
    Ok(topology)
}

/// Publish `parent0,parent1 -> parent` with one topology CAS. The caller must
/// have quiesced both children behind durable per-shard fences and verified a
/// union clone at `parent_path` before making it visible.
pub async fn cas_publish_topology_merge_with_path(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
    expected_version: u64,
    parent_path: &str,
) -> Result<Topology, object_store::Error> {
    let path = ObjPath::from(TOPOLOGY_PATH);
    let result = store.get(&path).await?;
    let etag = result.meta.e_tag.clone();
    let raw = result.bytes().await?;
    let mut topology = parse_json::<Topology>(&raw, "topology")?;
    validate_topology(&topology)?;
    if topology.version != expected_version {
        return Err(registry_error(
            "topology version changed before merge publish",
        ));
    }
    if parent.len() >= 128 || !parent.bytes().all(|bit| bit == b'0' || bit == b'1') {
        return Err(registry_error("invalid merge parent prefix"));
    }
    let zero = format!("{parent}0");
    let one = format!("{parent}1");
    if topology.shards.contains(&parent.to_string()) {
        return Err(registry_error("merge parent is already live"));
    }
    if !topology.shards.contains(&zero) || !topology.shards.contains(&one) {
        return Err(registry_error("merge children are not both live siblings"));
    }
    ObjPath::parse(parent_path).map_err(|_| registry_error("invalid merged shard object path"))?;

    topology
        .shards
        .retain(|prefix| prefix != &zero && prefix != &one);
    topology.shard_paths.remove(&zero);
    topology.shard_paths.remove(&one);
    topology.shards.push(parent.to_string());
    topology
        .shard_paths
        .insert(parent.to_string(), parent_path.to_string());
    topology.shards.sort();
    topology.version = topology
        .version
        .checked_add(1)
        .ok_or_else(|| registry_error("topology version overflow"))?;
    validate_topology(&topology)?;
    let body = serde_json::to_vec(&topology).expect("topology json");
    store
        .put_opts(
            &path,
            PutPayload::from(body),
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: etag,
                version: None,
            })),
        )
        .await?;
    Ok(topology)
}

pub async fn load_or_init_topology(
    store: &Arc<dyn ObjectStore>,
    initial_shards: usize,
) -> Result<Topology, object_store::Error> {
    let path = ObjPath::from(TOPOLOGY_PATH);
    match store.get(&path).await {
        Ok(r) => {
            let raw = r.bytes().await?;
            let topology = parse_json::<Topology>(&raw, "topology")?;
            validate_topology(&topology)?;
            return Ok(topology);
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    let initial_shards = initial_shards.max(1);
    if !initial_shards.is_power_of_two() {
        return Err(registry_error("initial shards must be a power of two"));
    }
    let bits = initial_shards.trailing_zeros() as usize;
    let shards: Vec<String> = if bits == 0 {
        vec![String::new()]
    } else {
        (0..initial_shards)
            .map(|i| format!("{:0width$b}", i, width = bits))
            .collect()
    };
    let topo = Topology {
        version: 1,
        storage_format: 2,
        shards,
        shard_paths: HashMap::new(),
    };
    let raw = serde_json::to_vec(&topo).expect("topology json");
    match store
        .put_opts(
            &path,
            PutPayload::from(raw),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(topo),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let r = store.get(&path).await?;
            let raw = r.bytes().await?;
            let topology = parse_json::<Topology>(&raw, "topology")?;
            validate_topology(&topology)?;
            Ok(topology)
        }
        Err(e) => Err(e),
    }
}

/// Longest-prefix match of the stream hash's leading bits against the shard
/// set. `shards` must form a complete prefix code.
pub fn shard_for_hash(shards: &[String], hash: &[u8; 16]) -> String {
    shards
        .iter()
        .filter(|prefix| shard_prefix_matches(prefix, hash))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

/// Does `hash` fall inside the shard identified by bit-prefix `prefix`?
pub fn shard_prefix_matches(prefix: &str, hash: &[u8; 16]) -> bool {
    if prefix.len() > hash.len() * 8 {
        return false;
    }
    prefix.bytes().enumerate().all(|(index, expected)| {
        let actual = b'0' + ((hash[index / 8] >> (7 - index % 8)) & 1);
        expected == actual
    })
}

#[derive(Default)]
struct PrefixNode {
    terminal: bool,
    zero: Option<Box<PrefixNode>>,
    one: Option<Box<PrefixNode>>,
}

fn validate_topology(topology: &Topology) -> Result<(), object_store::Error> {
    if topology.version == 0 {
        return Err(registry_error("topology version must be positive"));
    }
    if topology.storage_format != 2 {
        return Err(registry_error(
            "unsupported storage format; an explicit offline migration is required",
        ));
    }
    if topology.shards.is_empty() {
        return Err(registry_error("topology must contain at least one shard"));
    }
    for (prefix, path) in &topology.shard_paths {
        if !topology.shards.contains(prefix)
            || path.len() > 512
            || !path.starts_with("shards/")
            || path.contains("//")
            || path.split('/').any(|component| component == "..")
        {
            return Err(registry_error("invalid topology shard path mapping"));
        }
        ObjPath::parse(path).map_err(|_| registry_error("invalid topology shard object path"))?;
    }
    let mut root = PrefixNode::default();
    for prefix in &topology.shards {
        if prefix.len() > 128 || !prefix.bytes().all(|b| b == b'0' || b == b'1') {
            return Err(registry_error(
                "topology shard prefixes must be binary and at most 128 bits",
            ));
        }
        let mut node = &mut root;
        for bit in prefix.bytes() {
            if node.terminal {
                return Err(registry_error(
                    "topology contains overlapping shard prefixes",
                ));
            }
            node = if bit == b'0' {
                node.zero.get_or_insert_with(Default::default)
            } else {
                node.one.get_or_insert_with(Default::default)
            };
        }
        if node.terminal || node.zero.is_some() || node.one.is_some() {
            return Err(registry_error(
                "topology contains duplicate or overlapping shard prefixes",
            ));
        }
        node.terminal = true;
    }

    fn complete(node: &PrefixNode) -> bool {
        if node.terminal {
            return node.zero.is_none() && node.one.is_none();
        }
        match (&node.zero, &node.one) {
            (Some(zero), Some(one)) => complete(zero) && complete(one),
            _ => false,
        }
    }
    if !complete(&root) {
        return Err(registry_error(
            "topology shard prefixes do not cover the hash space",
        ));
    }
    Ok(())
}

fn registry_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "registry",
        source: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn descriptor(name: &str, epoch: &str) -> StreamDesc {
        StreamDesc {
            customer_id: "__legacy__".to_string(),
            cell: String::new(),
            cell_move: None,
            name: name.to_string(),
            stream_epoch: epoch.to_string(),
            key_fingerprint: format!("fingerprint-{epoch}"),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            profile: None,
            content_type: "application/octet-stream".to_string(),
            ttl_secs: None,
            ordering: None,
            segment_count: 0,
            queue_max_deliveries: None,
            append_requests_per_second: None,
            append_request_burst: None,
            write_bytes_per_second: None,
            write_burst_bytes: None,
            commit_weight: None,
            touch_token_fingerprint: None,
            touch_templates: Vec::new(),
            touch_sig_key: None,
            initial_request_hash: None,
            forked_from: None,
            fork_source_epoch: None,
            fork_offset: None,
            fork_sub_offset: None,
            fork_children: Vec::new(),
            fork_reference_registered: false,
        }
    }

    #[test]
    fn cache_is_bounded_and_stale_order_entries_do_not_evict_new_values() {
        let mut cache = RegistryCache::new(2);
        cache.insert("a".into(), Some(descriptor("a", "old")));
        cache.insert("a".into(), Some(descriptor("a", "new")));
        cache.insert("b".into(), Some(descriptor("b", "b")));
        assert_eq!(cache.entries.len(), 2);
        cache.insert("c".into(), Some(descriptor("c", "c")));
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.get("a", Duration::from_secs(60)).is_none());
        assert!(cache.get("b", Duration::from_secs(60)).is_some());
        assert!(cache.get("c", Duration::from_secs(60)).is_some());
    }

    #[tokio::test]
    async fn concurrent_recreate_has_exactly_one_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let seed = Registry::new(store.clone());
        let mut dead = descriptor("stream", "dead");
        dead.deleted = true;
        assert!(seed.create(dead).await.unwrap().0);

        // Separate Registry instances model concurrent servers with
        // independent caches racing on the same control-plane object.
        let left = Registry::new(store.clone());
        let right = Registry::new(store.clone());
        let (a, b) = tokio::join!(
            left.recreate("__legacy__", "stream", "dead", descriptor("stream", "left")),
            right.recreate(
                "__legacy__",
                "stream",
                "dead",
                descriptor("stream", "right")
            ),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert_ne!(a.0, b.0, "exactly one CAS may create the incarnation");
        assert_eq!(
            a.1.stream_epoch, b.1.stream_epoch,
            "loser must observe winner"
        );

        seed.invalidate("__legacy__", "stream");
        let stored = seed.get("__legacy__", "stream").await.unwrap().unwrap();
        assert_eq!(stored.stream_epoch, a.1.stream_epoch);
    }

    #[tokio::test]
    async fn corrupt_descriptor_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &desc_path("__legacy__", "bad"),
                PutPayload::from_static(b"not json"),
            )
            .await
            .unwrap();
        let registry = Registry::new(store);
        assert!(registry.get("__legacy__", "bad").await.is_err());
    }

    #[tokio::test]
    async fn concurrent_customer_cell_affinity_has_one_bounded_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let left = Registry::new(store.clone());
        let right = Registry::new(store);
        let (left, right) = tokio::join!(
            left.get_or_create_customer_cell_affinity("customer", "c-left"),
            right.get_or_create_customer_cell_affinity("customer", "c-right"),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        assert_eq!(left.cells.len(), 1);
        assert!(matches!(left.cells[0].as_str(), "c-left" | "c-right"));
    }

    #[tokio::test]
    async fn a_cross_cell_create_is_visible_after_an_observed_miss() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let observing_cell = Registry::new(store.clone());
        let creating_cell = Registry::new(store);
        assert!(
            observing_cell
                .get("customer-a", "orders")
                .await
                .unwrap()
                .is_none()
        );
        let mut desc = descriptor("orders", "epoch");
        desc.customer_id = "customer-a".into();
        assert!(creating_cell.create(desc).await.unwrap().0);
        assert!(
            observing_cell
                .get("customer-a", "orders")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn invalid_descriptor_cell_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut desc = descriptor("bad-cell", "epoch");
        desc.cell = "../other".to_string();
        store
            .put(
                &desc_path("__legacy__", "bad-cell"),
                PutPayload::from(serde_json::to_vec(&desc).unwrap()),
            )
            .await
            .unwrap();
        let registry = Registry::new(store);
        assert!(registry.get("__legacy__", "bad-cell").await.is_err());
    }

    #[tokio::test]
    async fn cell_index_bounds_recovery_enumeration_to_the_authoritative_cell() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Registry::new(store.clone());
        let mut desc = descriptor("orders", &"01".repeat(16));
        desc.customer_id = "customer-a".into();
        desc.cell = "cell-a".into();
        assert!(registry.create(desc.clone()).await.unwrap().0);
        let mut prefix_neighbor = descriptor("neighbor", &"04".repeat(16));
        prefix_neighbor.customer_id = "customer-b".into();
        prefix_neighbor.cell = "cell-aa".into();
        assert!(registry.create(prefix_neighbor).await.unwrap().0);

        assert_eq!(
            active_history_db_paths_for_cell(&store, Some("cell-a"))
                .await
                .unwrap(),
            vec![history_db_path(&desc.storage_hash())]
        );
        assert!(
            active_history_db_paths_for_cell(&store, Some("cell-b"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cell_move_descriptor_is_restartable_and_changes_recovery_authority_once() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &ObjPath::from(crate::cells::CELLS_PATH),
                PutPayload::from_static(
                    br#"{"version":1,"generation":2,"cells":[{"cell_id":"cell-a","region":"a","ops_prefix":"cells/cell-a","weight":1,"state":"active"},{"cell_id":"cell-b","region":"b","ops_prefix":"cells/cell-b","weight":1,"state":"active"}]}"#,
                ),
            )
            .await
            .unwrap();
        let registry = Registry::new(store.clone());
        registry
            .get_or_create_customer_cell_affinity("customer-a", "cell-a")
            .await
            .unwrap();
        let mut desc = descriptor("orders", &"05".repeat(16));
        desc.customer_id = "customer-a".into();
        desc.cell = "cell-a".into();
        assert!(registry.create(desc.clone()).await.unwrap().0);

        let operation = "ab".repeat(16);
        let preparing = registry
            .begin_cell_move("customer-a", "orders", "cell-a", "cell-b", &operation)
            .await
            .unwrap();
        assert!(preparing.cell_move_in_progress());
        assert_eq!(preparing.cell, "cell-a");
        assert_eq!(
            active_history_db_paths_for_cell(&store, Some("cell-a"))
                .await
                .unwrap(),
            vec![history_db_path(&desc.storage_hash())]
        );
        assert!(
            active_history_db_paths_for_cell(&store, Some("cell-b"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            registry
                .update("customer-a", "orders", |_| {})
                .await
                .is_err(),
            "ordinary descriptor mutations must not cross the move CAS"
        );

        let completed = registry
            .complete_cell_move("customer-a", "orders", &operation)
            .await
            .unwrap();
        assert_eq!(completed.cell, "cell-b");
        assert_eq!(
            completed.cell_move.as_ref().unwrap().state,
            CellMoveState::Completed
        );
        let stale_delete = registry
            .mark_deleted("customer-a", "orders", &desc.stream_epoch, "cell-a")
            .await
            .unwrap()
            .unwrap();
        assert!(!stale_delete.0);
        assert_eq!(stale_delete.1.cell, "cell-b");
        assert!(!stale_delete.1.deleted);
        let mut stale_recreate = descriptor("orders", &"06".repeat(16));
        stale_recreate.customer_id = "customer-a".into();
        stale_recreate.cell = "cell-a".into();
        let stale_recreate = registry
            .recreate("customer-a", "orders", &desc.stream_epoch, stale_recreate)
            .await
            .unwrap();
        assert!(!stale_recreate.0);
        assert_eq!(stale_recreate.1.cell, "cell-b");
        assert!(
            active_history_db_paths_for_cell(&store, Some("cell-a"))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            active_history_db_paths_for_cell(&store, Some("cell-b"))
                .await
                .unwrap(),
            vec![history_db_path(&desc.storage_hash())]
        );
        assert!(
            registry
                .begin_cell_move("customer-a", "orders", "cell-b", "cell-a", &"cd".repeat(16),)
                .await
                .is_err(),
            "a second move must not erase pending source-cleanup identity"
        );
        let (retained, authoritative) = registry
            .completed_cell_move_retention_proof(
                "customer-a",
                "orders",
                "cell-a",
                "cell-b",
                &operation,
                Duration::ZERO,
            )
            .await
            .unwrap();
        assert_eq!(retained.cell, "cell-b");
        assert_eq!(
            serde_json::from_slice::<StreamDesc>(&authoritative)
                .unwrap()
                .cell,
            "cell-b"
        );
        let cleaned = registry
            .complete_cell_move_source_cleanup("customer-a", "orders", &operation)
            .await
            .unwrap();
        assert!(
            cleaned
                .cell_move
                .as_ref()
                .unwrap()
                .source_cleaned_ms
                .is_some()
        );
        store
            .put(
                &ObjPath::from(crate::cells::CELLS_PATH),
                PutPayload::from_static(
                    br#"{"version":1,"generation":3,"cells":[{"cell_id":"cell-b","region":"b","ops_prefix":"cells/cell-b","weight":1,"state":"active"}]}"#,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            registry
                .begin_cell_move("customer-a", "orders", "cell-a", "cell-b", &operation,)
                .await
                .unwrap()
                .cell,
            "cell-b",
            "a retry after the final response was lost must resolve completion"
        );
    }

    #[tokio::test]
    async fn orphan_and_losing_cell_indices_are_safe() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let orphan = CellStreamIndex {
            version: 1,
            customer_id: "customer-a".into(),
            name: "orphan".into(),
            cell: "cell-a".into(),
        };
        store
            .put(
                &cell_stream_index_path("customer-a", "orphan", "cell-a"),
                PutPayload::from(serde_json::to_vec(&orphan).unwrap()),
            )
            .await
            .unwrap();

        let registry = Registry::new(store.clone());
        let mut winner = descriptor("orders", &"02".repeat(16));
        winner.customer_id = "customer-a".into();
        winner.cell = "cell-b".into();
        assert!(registry.create(winner).await.unwrap().0);
        let loser = CellStreamIndex {
            version: 1,
            customer_id: "customer-a".into(),
            name: "orders".into(),
            cell: "cell-a".into(),
        };
        store
            .put(
                &cell_stream_index_path("customer-a", "orders", "cell-a"),
                PutPayload::from(serde_json::to_vec(&loser).unwrap()),
            )
            .await
            .unwrap();

        assert!(
            active_history_db_paths_for_cell(&store, Some("cell-a"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn single_cell_migration_is_audited_idempotent_and_recovery_complete() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &ObjPath::from(crate::cells::CELLS_PATH),
                PutPayload::from_static(
                    br#"{"version":1,"generation":1,"cells":[{"cell_id":"cell-a","region":"test","ops_prefix":"cells/cell-a","weight":1,"state":"active"}]}"#,
                ),
            )
            .await
            .unwrap();
        let registry = Registry::new(store.clone());
        let mut desc = descriptor("orders", &"03".repeat(16));
        desc.customer_id = "customer-a".into();
        assert!(registry.create(desc.clone()).await.unwrap().0);

        let audit = registry
            .migrate_single_cell_descriptors("cell-a", 10, false)
            .await
            .unwrap();
        assert_eq!(audit.scanned, 1);
        assert_eq!(audit.pending_placements, 1);
        assert!(
            active_history_db_paths_for_cell(&store, Some("cell-a"))
                .await
                .unwrap()
                .is_empty()
        );

        let applied = registry
            .migrate_single_cell_descriptors("cell-a", 10, true)
            .await
            .unwrap();
        assert_eq!(applied.migrated_placements, 1);
        let post = registry
            .migrate_single_cell_descriptors("cell-a", 10, false)
            .await
            .unwrap();
        assert_eq!(post.pending_placements, 0);
        assert_eq!(post.pending_indices, 0);
        assert_eq!(
            active_history_db_paths_for_cell(&store, Some("cell-a"))
                .await
                .unwrap(),
            vec![history_db_path(&desc.storage_hash())]
        );
    }

    #[tokio::test]
    async fn customers_can_use_the_same_name_without_visibility_or_storage_collisions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Registry::new(store);
        let mut left = descriptor("orders", "left");
        left.customer_id = "customer-a".into();
        let mut right = descriptor("orders", "right");
        right.customer_id = "customer-b".into();
        assert!(registry.create(left.clone()).await.unwrap().0);
        assert!(registry.create(right.clone()).await.unwrap().0);

        assert_eq!(
            registry
                .get("customer-a", "orders")
                .await
                .unwrap()
                .unwrap()
                .stream_epoch,
            "left"
        );
        assert_eq!(
            registry
                .get("customer-b", "orders")
                .await
                .unwrap()
                .unwrap()
                .stream_epoch,
            "right"
        );
        assert_eq!(registry.list("customer-a", 10).await.unwrap().len(), 1);
        assert_eq!(registry.list("customer-b", 10).await.unwrap().len(), 1);
        assert_ne!(left.storage_hash(), right.storage_hash());
        assert_ne!(left.routing_hash(), right.routing_hash());
    }

    #[tokio::test]
    async fn deleting_a_fork_chain_retains_then_cascades_ancestor_references() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Registry::new(store);
        let root = descriptor("root", "root-epoch");
        let mut middle = descriptor("middle", "middle-epoch");
        middle.forked_from = Some("root".into());
        middle.fork_source_epoch = Some("root-epoch".into());
        middle.fork_reference_registered = true;
        let mut leaf = descriptor("leaf", "leaf-epoch");
        leaf.forked_from = Some("middle".into());
        leaf.fork_source_epoch = Some("middle-epoch".into());
        leaf.fork_reference_registered = true;
        for desc in [root, middle, leaf] {
            assert!(registry.create(desc).await.unwrap().0);
        }
        assert!(
            registry
                .add_fork_child("__legacy__", "root", "root-epoch", "middle")
                .await
                .unwrap()
        );
        assert!(
            registry
                .add_fork_child("__legacy__", "middle", "middle-epoch", "leaf")
                .await
                .unwrap()
        );

        registry
            .mark_deleted("__legacy__", "root", "root-epoch", "")
            .await
            .unwrap();
        let middle = registry
            .mark_deleted("__legacy__", "middle", "middle-epoch", "")
            .await
            .unwrap()
            .unwrap()
            .1;
        registry
            .release_fork_chain("__legacy__", &middle)
            .await
            .unwrap();
        assert_eq!(
            registry
                .get("__legacy__", "root")
                .await
                .unwrap()
                .unwrap()
                .fork_children,
            vec!["middle"]
        );

        let leaf = registry
            .mark_deleted("__legacy__", "leaf", "leaf-epoch", "")
            .await
            .unwrap()
            .unwrap()
            .1;
        registry
            .release_fork_chain("__legacy__", &leaf)
            .await
            .unwrap();
        assert!(
            registry
                .get("__legacy__", "middle")
                .await
                .unwrap()
                .unwrap()
                .fork_children
                .is_empty()
        );
        assert!(
            registry
                .get("__legacy__", "root")
                .await
                .unwrap()
                .unwrap()
                .fork_children
                .is_empty()
        );
    }

    #[tokio::test]
    async fn invalid_initial_shard_count_is_an_error_not_a_panic() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(load_or_init_topology(&store, 3).await.is_err());
    }

    #[test]
    fn topology_must_be_a_complete_non_overlapping_prefix_code() {
        assert!(
            validate_topology(&Topology {
                version: 1,
                storage_format: 2,
                shards: vec!["0".into(), "10".into(), "11".into()],
                shard_paths: HashMap::new(),
            })
            .is_ok()
        );
        assert!(
            validate_topology(&Topology {
                version: 1,
                storage_format: 2,
                shards: vec!["0".into(), "10".into()],
                shard_paths: HashMap::new(),
            })
            .is_err()
        );
        assert!(
            validate_topology(&Topology {
                version: 1,
                storage_format: 2,
                shards: vec!["0".into(), "00".into(), "1".into()],
                shard_paths: HashMap::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn prefix_matching_uses_the_full_128_bit_hash() {
        let mut hash = [0u8; 16];
        hash[15] = 1;
        let matching = format!("{}1", "0".repeat(127));
        assert!(shard_prefix_matches(&matching, &hash));
        assert_eq!(
            shard_for_hash(&[format!("{}0", "0".repeat(127)), matching.clone()], &hash),
            matching
        );
    }

    #[test]
    fn physical_keys_share_the_stable_topology_prefix_but_isolate_incarnations() {
        let first = descriptor("orders", "epoch-a");
        let second = descriptor("orders", "epoch-b");
        let route = first.routing_hash();
        let first_storage = first.storage_hash();
        let second_storage = second.storage_hash();

        assert_eq!(&first_storage[..16], &route);
        assert_eq!(&second_storage[..16], &route);
        assert_ne!(&first_storage[16..], &second_storage[16..]);
        for prefix in ["0", "1", "0101", "10101010"] {
            assert_eq!(
                shard_prefix_matches(prefix, &route),
                shard_prefix_matches(prefix, first_storage[..16].try_into().unwrap())
            );
        }
    }

    #[test]
    fn projection_bounds_are_exact_for_non_byte_aligned_prefixes() {
        let (lower, upper) = shard_projection_bounds("101").unwrap();
        let Bound::Included(lower) = lower else {
            panic!("lower bound must be included");
        };
        let Bound::Excluded(upper) = upper else {
            panic!("upper bound must be excluded");
        };
        assert_eq!(lower.as_ref(), &(5u128 << 125).to_be_bytes());
        assert_eq!(upper.as_ref(), &(6u128 << 125).to_be_bytes());

        let (_, upper) = shard_projection_bounds("111").unwrap();
        assert!(matches!(upper, Bound::Unbounded));
    }

    #[tokio::test]
    async fn topology_split_publish_is_one_versioned_cas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let initial = load_or_init_topology(&store, 1).await.unwrap();
        let split = cas_publish_topology_split(&store, "", initial.version)
            .await
            .unwrap();
        assert_eq!(split.version, initial.version + 1);
        assert_eq!(split.shards, vec!["0", "1"]);
        assert!(
            cas_publish_topology_split(&store, "0", initial.version)
                .await
                .is_err()
        );
        assert_eq!(load_topology(&store).await.unwrap().shards, vec!["0", "1"]);
    }

    #[tokio::test]
    async fn topology_merge_publish_is_one_versioned_cas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let initial = load_or_init_topology(&store, 1).await.unwrap();
        let split = cas_publish_topology_split(&store, "", initial.version)
            .await
            .unwrap();
        let merged_path = "shards/merges/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/root";
        let merged = cas_publish_topology_merge_with_path(&store, "", split.version, merged_path)
            .await
            .unwrap();
        assert_eq!(merged.version, split.version + 1);
        assert_eq!(merged.shards, vec![""]);
        assert_eq!(merged.db_path(""), merged_path);
        assert!(
            cas_publish_topology_merge_with_path(&store, "", split.version, merged_path)
                .await
                .is_err()
        );
        assert_eq!(
            load_topology(&store).await.unwrap().db_path(""),
            merged_path
        );
    }

    #[tokio::test]
    async fn customer_limits_are_durable_bounded_and_fail_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &customer_limits_path("customer-a"),
                PutPayload::from_static(
                    br#"{"version":1,"max_inflight":3,"max_live_connections":2,"write_bytes_per_second":100,"write_burst_bytes":200,"append_requests_per_second":10,"append_request_burst":20,"read_requests_per_second":30,"read_request_burst":40,"read_bytes_per_second":300,"read_burst_bytes":400,"queue_receives_per_second":50,"queue_receive_burst":60,"streams_count":2}"#,
                ),
            )
            .await
            .unwrap();
        let registry = Registry::with_cache_capacity(store.clone(), 2);
        let limits = registry.customer_limits("customer-a").await.unwrap();
        assert_eq!(limits.max_inflight, Some(3));
        assert_eq!(limits.max_live_connections, Some(2));
        assert_eq!(limits.append_request_burst, Some(20));
        assert_eq!(limits.read_bytes_per_second, Some(300));
        assert_eq!(limits.queue_receive_burst, Some(60));
        assert_eq!(limits.streams_count, Some(2));

        let corrupt = Registry::with_cache_capacity(store.clone(), 2);
        store
            .put(
                &customer_limits_path("customer-b"),
                PutPayload::from_static(b"not-json"),
            )
            .await
            .unwrap();
        assert!(corrupt.customer_limits("customer-b").await.is_err());

        store
            .put(
                &customer_limits_path("customer-c"),
                PutPayload::from_static(
                    br#"{"version":1,"read_bytes_per_second":1,"read_burst_bytes":0}"#,
                ),
            )
            .await
            .unwrap();
        assert!(
            Registry::with_cache_capacity(store, 2)
                .customer_limits("customer-c")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn stream_quota_lease_is_owned_verified_and_reusable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Registry::new(store);
        let first = registry
            .acquire_stream_quota_lease("customer-a")
            .await
            .unwrap();
        registry.verify_stream_quota_lease(&first).await.unwrap();
        registry.release_stream_quota_lease(&first).await.unwrap();
        let second = registry
            .acquire_stream_quota_lease("customer-a")
            .await
            .unwrap();
        assert_ne!(first.owner, second.owner);
        registry.verify_stream_quota_lease(&second).await.unwrap();
    }
}
