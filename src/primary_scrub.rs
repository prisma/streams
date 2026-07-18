//! Bounded logical integrity scrubbing for live SlateDB databases.
//!
//! The recovery corpus has its own SHA-256 scrub. This actor validates the
//! primary authority instead: latest manifests must decode, referenced shard
//! SST/WAL blocks pass SlateDB's logical readers, and customer-key encrypted
//! history bytes match immutable digests established by the keyed writer. A
//! durable cursor prevents failover from starving the high end of a cell.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use futures_util::stream::BoxStream;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::admin::AdminBuilder;
use slatedb::manifest::{SsTableHandle, SsTableId, SsTableView};
use slatedb::{BlockTransformer, SstReader, WalReader};

const STATE_FORMAT_VERSION: u32 = 1;
const MAX_STATE_BYTES: usize = 16 * 1024;
const MAX_DATABASES: usize = 116_384;
const MAX_REFERENCED_OBJECTS_PER_DB: usize = 100_000;
const HISTORY_BASELINE_FORMAT_VERSION: u32 = 1;
const MAX_HISTORY_BASELINE_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct PrimaryScrubConfig {
    pub cell_id: Option<String>,
    pub topology_store: Arc<dyn ObjectStore>,
    pub registry_store: Arc<dyn ObjectStore>,
    pub shard_store: Arc<dyn ObjectStore>,
    pub data_store: Arc<dyn ObjectStore>,
    pub max_object_bytes: u64,
}

#[derive(Debug)]
pub struct PrimaryScrubReport {
    pub checked: u64,
    pub completed_sweep: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Cursor {
    database: Option<String>,
    unit: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableState {
    format_version: u32,
    cursor: Cursor,
    updated_ms: i64,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
}

#[derive(Clone)]
struct DatabaseTarget {
    key: String,
    path: String,
    store: Arc<dyn ObjectStore>,
    verification: ObjectVerification,
}

#[derive(Clone, Copy)]
enum ObjectVerification {
    Logical,
    WriterBaseline,
}

enum Work {
    Manifest,
    Compacted {
        root: String,
        store: Arc<dyn ObjectStore>,
        handle: SsTableHandle,
    },
    Wal {
        root: String,
        store: Arc<dyn ObjectStore>,
        id: u64,
    },
    HistoryObject {
        source_path: String,
        store: Arc<dyn ObjectStore>,
        logical: Option<(String, SsTableHandle)>,
    },
}

struct WorkUnit {
    key: String,
    work: Work,
}

#[derive(Debug, Deserialize, Serialize)]
struct HistoryBaseline {
    format_version: u32,
    source_path: String,
    size: u64,
    sha256: String,
    #[serde(default)]
    source_etag: Option<String>,
    /// The keyed writer reopened and logically decoded this object before
    /// making its absorbed frontier authoritative. Pre-publication baselines
    /// are already sufficient for keyless byte-integrity checks, but this
    /// monotonic bit preserves the stronger writer validation without an
    /// O(history) rescan on every absorb.
    #[serde(default)]
    logical_verified: bool,
    created_ms: i64,
}

/// History-only object-store wrapper that establishes the immutable digest
/// before a compacted SST becomes visible. The wrapper is constructed only by
/// the keyed history writer, after its block transformer has encoded the
/// payload. Publishing the baseline first makes every crash point safe:
/// baseline-only and baseline+unreferenced-SST are harmless orphans, while a
/// manifest can never reference an SST that predates its baseline.
#[derive(Debug)]
pub struct HistoryIntegrityStore {
    inner: Arc<dyn ObjectStore>,
    baseline_store: Arc<dyn ObjectStore>,
    compacted_prefix: String,
    max_object_bytes: u64,
}

impl HistoryIntegrityStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        baseline_store: Arc<dyn ObjectStore>,
        database_path: &str,
        max_object_bytes: u64,
    ) -> Self {
        Self {
            inner,
            baseline_store,
            compacted_prefix: format!("{database_path}/compacted/"),
            max_object_bytes,
        }
    }

    fn is_history_sst(&self, path: &ObjPath) -> bool {
        path.as_ref().starts_with(&self.compacted_prefix)
            && path.as_ref().ends_with(".sst")
            && !path.as_ref()[self.compacted_prefix.len()..].contains('/')
    }
}

impl std::fmt::Display for HistoryIntegrityStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HistoryIntegrityStore({})", self.inner)
    }
}

fn history_integrity_store_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "history-integrity",
        source: message.into().into(),
    }
}

#[async_trait]
impl ObjectStore for HistoryIntegrityStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.is_history_sst(location) {
            prepare_history_payload_baseline(
                self.baseline_store.clone(),
                location.as_ref(),
                &payload,
                self.max_object_bytes,
            )
            .await
            .map_err(|error| history_integrity_store_error(error.to_string()))?;
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.is_history_sst(location) {
            return Err(object_store::Error::NotSupported {
                source: "history SST multipart uploads cannot publish an atomic integrity baseline"
                    .into(),
            });
        }
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        if self.is_history_sst(to) {
            return Err(object_store::Error::NotSupported {
                source: "history SST copies cannot bypass the keyed integrity writer".into(),
            });
        }
        self.inner.copy_opts(from, to, options).await
    }
}

pub async fn scrub_batch(
    config: &PrimaryScrubConfig,
    limit: usize,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
) -> anyhow::Result<PrimaryScrubReport> {
    anyhow::ensure!(limit > 0, "primary scrub batch must be positive");
    anyhow::ensure!(
        (coordinator_epoch == 0 && coordinator_sequence == 0)
            || (coordinator_epoch > 0 && coordinator_sequence > 0),
        "primary scrub publication has an incomplete coordinator order"
    );
    anyhow::ensure!(
        config.max_object_bytes > 0,
        "primary scrub object bound must be positive"
    );
    let targets = database_targets(config).await?;
    let mut cursor = read_cursor(config.topology_store.clone()).await?;
    let mut checked = 0u64;
    let mut completed_sweep = false;

    if targets.is_empty() {
        cursor = Cursor::default();
        completed_sweep = true;
    } else {
        let mut index = cursor.database.as_ref().map_or(0, |database| {
            targets.partition_point(|target| target.key.as_str() < database.as_str())
        });
        let mut steps = 0usize;
        let max_steps = targets.len().saturating_add(limit).max(1);
        while checked < limit as u64 && steps < max_steps {
            steps += 1;
            if index >= targets.len() {
                cursor = Cursor::default();
                completed_sweep = true;
                break;
            }
            let target = &targets[index];
            let units = database_work(target, config.max_object_bytes).await?;
            let same_database = cursor.database.as_deref() == Some(target.key.as_str());
            let unit_index = if same_database {
                cursor.unit.as_ref().map_or(0, |unit| {
                    units.partition_point(|candidate| candidate.key.as_str() <= unit.as_str())
                })
            } else {
                0
            };
            if let Some(unit) = units.get(unit_index) {
                if let Err(error) = verify_work(unit, config).await {
                    // SlateDB may retire an SST between manifest discovery and
                    // verification. Only suppress that race when a fresh
                    // manifest no longer references the exact work unit.
                    let still_referenced = database_work(target, config.max_object_bytes)
                        .await?
                        .iter()
                        .any(|candidate| candidate.key == unit.key);
                    if still_referenced {
                        return Err(anyhow::anyhow!(
                            "primary scrub failed for {} / {}: {error:#}",
                            target.key,
                            unit.key
                        ));
                    }
                }
                cursor.database = Some(target.key.clone());
                cursor.unit = Some(unit.key.clone());
                checked = checked.saturating_add(1);
                continue;
            }
            index += 1;
            if index >= targets.len() {
                cursor = Cursor::default();
                completed_sweep = true;
                break;
            }
            cursor.database = targets.get(index).map(|next| next.key.clone());
            cursor.unit = None;
        }
    }

    publish_cursor(
        config.topology_store.clone(),
        cursor,
        coordinator_epoch,
        coordinator_sequence,
    )
    .await?;
    Ok(PrimaryScrubReport {
        checked,
        completed_sweep,
    })
}

async fn database_targets(config: &PrimaryScrubConfig) -> anyhow::Result<Vec<DatabaseTarget>> {
    let topology = crate::registry::load_topology(&config.topology_store).await?;
    let mut targets = BTreeMap::new();
    for prefix in &topology.shards {
        let path = topology.db_path(prefix);
        targets.insert(
            format!("shard:{path}"),
            DatabaseTarget {
                key: format!("shard:{path}"),
                path,
                store: config.shard_store.clone(),
                verification: ObjectVerification::Logical,
            },
        );
    }
    for path in crate::registry::active_history_db_paths_for_cell(
        &config.registry_store,
        config.cell_id.as_deref(),
    )
    .await?
    {
        targets.insert(
            format!("data:{path}"),
            DatabaseTarget {
                key: format!("data:{path}"),
                path,
                store: config.data_store.clone(),
                verification: ObjectVerification::WriterBaseline,
            },
        );
    }
    anyhow::ensure!(
        targets.len() <= MAX_DATABASES,
        "primary scrub database count exceeds the cell bound"
    );
    Ok(targets.into_values().collect())
}

async fn database_work(
    target: &DatabaseTarget,
    max_object_bytes: u64,
) -> anyhow::Result<Vec<WorkUnit>> {
    let manifest_prefix = ObjPath::from(format!("{}/manifest", target.path));
    let mut listing = target.store.list(Some(&manifest_prefix));
    let mut latest = None;
    let mut manifest_count = 0usize;
    while let Some(meta) = listing.try_next().await? {
        let Some(id) = manifest_id(&target.path, &meta.location) else {
            continue;
        };
        manifest_count = manifest_count.saturating_add(1);
        anyhow::ensure!(
            manifest_count <= MAX_REFERENCED_OBJECTS_PER_DB,
            "primary manifest count exceeds the database bound"
        );
        if latest.as_ref().is_none_or(|(latest_id, _)| id > *latest_id) {
            latest = Some((id, meta.size));
        }
    }
    let Some((manifest_id, manifest_size)) = latest else {
        return Ok(vec![WorkUnit {
            key: "0-manifest-absent".to_string(),
            work: Work::Manifest,
        }]);
    };
    anyhow::ensure!(
        manifest_size > 0 && manifest_size <= max_object_bytes,
        "primary manifest size is outside the scrub bound"
    );
    let admin = AdminBuilder::new(target.path.clone(), target.store.clone()).build();
    let manifest = admin
        .read_manifest(Some(manifest_id))
        .await?
        .ok_or_else(|| anyhow::anyhow!("primary manifest disappeared during scrub"))?;
    anyhow::ensure!(
        manifest.initialized(),
        "primary scrub found uninitialized database"
    );
    let mut units = vec![WorkUnit {
        key: format!("0-manifest-{:020}", manifest.id()),
        work: Work::Manifest,
    }];
    let mut external = HashMap::new();
    for database in manifest.external_dbs() {
        anyhow::ensure!(
            !database.path.is_empty() && database.path.len() <= 1_024,
            "primary scrub found invalid external database path"
        );
        ObjPath::parse(&database.path)?;
        for id in &database.sst_ids {
            anyhow::ensure!(
                external.insert(*id, database.path.clone()).is_none(),
                "primary scrub found duplicate external SST ownership"
            );
        }
    }
    let mut seen = HashSet::new();
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
        add_compacted_unit(target, view, &external, &mut seen, &mut units)?;
    }
    let first_wal = manifest
        .replay_after_wal_id()
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("primary scrub WAL watermark exhausted"))?;
    let next_wal = manifest.next_wal_sst_id();
    let wal_count = next_wal
        .checked_sub(first_wal)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| anyhow::anyhow!("primary scrub found invalid WAL watermarks"))?;
    anyhow::ensure!(
        wal_count <= MAX_REFERENCED_OBJECTS_PER_DB,
        "primary scrub WAL reference count exceeds the database bound"
    );
    anyhow::ensure!(
        units.len() <= MAX_REFERENCED_OBJECTS_PER_DB + 1,
        "primary scrub compacted SST count exceeds the database bound"
    );
    let mut wal_ids: BTreeSet<u64> = (first_wal..next_wal).collect();
    anyhow::ensure!(
        units.len().saturating_add(wal_ids.len()) <= MAX_REFERENCED_OBJECTS_PER_DB + 1,
        "primary live SST/WAL count exceeds the database bound"
    );
    let wal_prefix = ObjPath::from(format!("{}/wal", target.path));
    let mut listing = target.store.list(Some(&wal_prefix));
    while let Some(meta) = listing.try_next().await? {
        let Some(id) = wal_id(&target.path, &meta.location) else {
            continue;
        };
        if id >= first_wal {
            wal_ids.insert(id);
            anyhow::ensure!(
                units.len().saturating_add(wal_ids.len()) <= MAX_REFERENCED_OBJECTS_PER_DB + 1,
                "primary live SST/WAL count exceeds the database bound"
            );
        }
    }
    for id in wal_ids {
        let work = match target.verification {
            ObjectVerification::Logical => Work::Wal {
                root: target.path.clone(),
                store: target.store.clone(),
                id,
            },
            ObjectVerification::WriterBaseline => Work::HistoryObject {
                source_path: format!("{}/wal/{id:020}.sst", target.path),
                store: target.store.clone(),
                logical: None,
            },
        };
        units.push(WorkUnit {
            key: format!("2-wal-{id:020}"),
            work,
        });
    }
    anyhow::ensure!(
        units.len() <= MAX_REFERENCED_OBJECTS_PER_DB + 1,
        "primary scrub SST reference count exceeds the database bound"
    );
    units.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(units)
}

fn manifest_id(root: &str, path: &ObjPath) -> Option<u64> {
    let suffix = path
        .as_ref()
        .strip_prefix(&format!("{root}/manifest/"))?
        .strip_suffix(".manifest")?;
    (suffix.len() == 20 && suffix.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| suffix.parse().ok())
        .flatten()
}

fn wal_id(root: &str, path: &ObjPath) -> Option<u64> {
    let suffix = path
        .as_ref()
        .strip_prefix(&format!("{root}/wal/"))?
        .strip_suffix(".sst")?;
    (suffix.len() == 20 && suffix.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| suffix.parse().ok())
        .flatten()
}

fn add_compacted_unit(
    target: &DatabaseTarget,
    view: &SsTableView,
    external: &HashMap<SsTableId, String>,
    seen: &mut HashSet<(String, SsTableId)>,
    units: &mut Vec<WorkUnit>,
) -> anyhow::Result<()> {
    let SsTableId::Compacted(id) = view.sst.id else {
        anyhow::bail!("primary manifest tree references a WAL as compacted data");
    };
    let root = external
        .get(&view.sst.id)
        .cloned()
        .unwrap_or_else(|| target.path.clone());
    if seen.insert((root.clone(), view.sst.id)) {
        let work = match target.verification {
            ObjectVerification::Logical => Work::Compacted {
                root: root.clone(),
                store: target.store.clone(),
                handle: view.sst.clone(),
            },
            ObjectVerification::WriterBaseline => Work::HistoryObject {
                source_path: format!("{root}/compacted/{id}.sst"),
                store: target.store.clone(),
                logical: Some((root.clone(), view.sst.clone())),
            },
        };
        units.push(WorkUnit {
            key: format!("1-compacted-{root}-{id}"),
            work,
        });
    }
    Ok(())
}

async fn verify_work(unit: &WorkUnit, config: &PrimaryScrubConfig) -> anyhow::Result<()> {
    match &unit.work {
        Work::Manifest => Ok(()),
        Work::Compacted {
            root,
            store,
            handle,
        } => verify_compacted(root, store.clone(), handle, config.max_object_bytes, None).await,
        Work::Wal { root, store, id } => {
            let file = WalReader::new(root.clone(), store.clone()).get(*id);
            let metadata = file.metadata().await?;
            anyhow::ensure!(
                metadata.metadata.size <= config.max_object_bytes,
                "primary WAL SST size exceeds the scrub bound"
            );
            let mut iterator = file.iterator().await?;
            while iterator.next().await?.is_some() {}
            Ok(())
        }
        Work::HistoryObject {
            source_path, store, ..
        } => {
            verify_history_baseline(
                source_path,
                store.clone(),
                config.topology_store.clone(),
                config.max_object_bytes,
            )
            .await
        }
    }
}

async fn verify_compacted(
    root: &str,
    store: Arc<dyn ObjectStore>,
    handle: &SsTableHandle,
    max_object_bytes: u64,
    transformer: Option<Arc<dyn BlockTransformer>>,
) -> anyhow::Result<()> {
    let reader = SstReader::new(root, store, None, transformer);
    let file = reader.open_with_handle(handle.clone())?;
    let metadata = file.metadata().await?;
    anyhow::ensure!(
        metadata.metadata.size <= max_object_bytes
            && metadata.metadata.size >= handle.estimate_size(),
        "primary compacted SST size is outside the scrub bound"
    );
    let index = file.index().await?;
    anyhow::ensure!(!index.is_empty(), "primary SST index is empty");
    for pair in index.windows(2) {
        anyhow::ensure!(
            pair[0].0 < pair[1].0,
            "primary SST index offsets are out of order"
        );
    }
    let stats = file.stats().await?;
    let mut last_key: Option<Bytes> = None;
    let mut row_count = 0u64;
    for (block, (_, expected_first)) in index.iter().enumerate() {
        let rows = file.read_block(block).await?;
        let first = rows
            .first()
            .ok_or_else(|| anyhow::anyhow!("primary SST contains an empty data block"))?;
        anyhow::ensure!(
            first.key >= *expected_first,
            "primary SST separator exceeds its data block"
        );
        for row in rows {
            anyhow::ensure!(
                last_key.as_ref().is_none_or(|key| key <= &row.key),
                "primary compacted SST keys are out of order"
            );
            last_key = Some(row.key);
            row_count = row_count.saturating_add(1);
        }
    }
    if let Some(stats) = stats {
        anyhow::ensure!(
            stats.num_rows() == row_count,
            "primary SST statistics do not match decoded rows"
        );
    }
    Ok(())
}

/// Establish a create-only digest before the keyed history writer publishes a
/// compacted SST. This is called from `HistoryIntegrityStore::put_opts` on the
/// exact transformed payload, before the underlying object PUT. An existing
/// baseline must describe identical bytes, so retries are idempotent while an
/// attempted immutable-path rewrite fails before changing primary data.
async fn prepare_history_payload_baseline(
    baseline_store: Arc<dyn ObjectStore>,
    source_path: &str,
    payload: &PutPayload,
    max_object_bytes: u64,
) -> anyhow::Result<()> {
    let size = payload.content_length() as u64;
    anyhow::ensure!(
        size > 0 && size <= max_object_bytes,
        "history SST size is outside the integrity bound"
    );
    let mut digest = Sha256::new();
    for chunk in payload {
        digest.update(chunk);
    }
    create_history_baseline(
        baseline_store,
        HistoryBaseline {
            format_version: HISTORY_BASELINE_FORMAT_VERSION,
            source_path: source_path.to_string(),
            size,
            sha256: crate::crypto::hex(&digest.finalize()),
            // The object is intentionally not visible yet. Its digest is the
            // immutable identity; a later scrub validates the stored body.
            source_etag: None,
            logical_verified: false,
            created_ms: crate::backup::wall_time_ms(),
        },
    )
    .await
}

/// Reconcile immutable whole-object digests for newly written customer-key
/// encrypted history SSTs. The writer still has the customer key here, so a
/// newly discovered legacy/unwrapped object is logically decoded before its
/// create-only baseline is trusted. Normal writes already have their exact
/// baseline from the pre-publication wrapper above.
pub async fn record_history_baselines(
    baseline_store: Arc<dyn ObjectStore>,
    data_store: Arc<dyn ObjectStore>,
    database_path: &str,
    max_object_bytes: u64,
    transformer: Arc<dyn BlockTransformer>,
) -> anyhow::Result<u64> {
    let target = DatabaseTarget {
        key: format!("data:{database_path}"),
        path: database_path.to_string(),
        store: data_store,
        verification: ObjectVerification::WriterBaseline,
    };
    let units = database_work(&target, max_object_bytes).await?;
    let mut recorded = 0u64;
    for unit in units {
        let Work::HistoryObject {
            source_path,
            store,
            logical,
        } = unit.work
        else {
            continue;
        };
        let Some((root, handle)) = logical else {
            anyhow::bail!("encrypted history database unexpectedly contains a live WAL");
        };
        let (size, source_etag, sha256) =
            object_digest(store.clone(), &source_path, max_object_bytes).await?;
        match read_history_baseline(baseline_store.clone(), &source_path).await? {
            Some(existing) => {
                validate_history_baseline(&existing, &source_path, size, &sha256)?;
                // Baselines written before this field existed always carried
                // the post-PUT ETag and were established only after logical
                // decoding, so that is the backward-compatible proof.
                if !existing.logical_verified && existing.source_etag.is_none() {
                    verify_compacted(
                        &root,
                        store,
                        &handle,
                        max_object_bytes,
                        Some(transformer.clone()),
                    )
                    .await?;
                    finalize_history_baseline(
                        baseline_store.clone(),
                        &source_path,
                        size,
                        &sha256,
                        source_etag,
                    )
                    .await?;
                }
            }
            None => {
                verify_compacted(
                    &root,
                    store,
                    &handle,
                    max_object_bytes,
                    Some(transformer.clone()),
                )
                .await?;
                create_history_baseline(
                    baseline_store.clone(),
                    HistoryBaseline {
                        format_version: HISTORY_BASELINE_FORMAT_VERSION,
                        source_path,
                        size,
                        sha256,
                        source_etag,
                        logical_verified: true,
                        created_ms: crate::backup::wall_time_ms(),
                    },
                )
                .await?;
                recorded = recorded.saturating_add(1);
            }
        }
    }
    Ok(recorded)
}

async fn verify_history_baseline(
    source_path: &str,
    source_store: Arc<dyn ObjectStore>,
    baseline_store: Arc<dyn ObjectStore>,
    max_object_bytes: u64,
) -> anyhow::Result<()> {
    let baseline = read_history_baseline(baseline_store, source_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("encrypted history SST has no writer-verified baseline"))?;
    let (size, _, sha256) = object_digest(source_store, source_path, max_object_bytes).await?;
    validate_history_baseline(&baseline, source_path, size, &sha256)
}

async fn object_digest(
    store: Arc<dyn ObjectStore>,
    source_path: &str,
    max_object_bytes: u64,
) -> anyhow::Result<(u64, Option<String>, String)> {
    let path = ObjPath::parse(source_path)?;
    let result = store.get(&path).await?;
    anyhow::ensure!(
        result.meta.size > 0 && result.meta.size <= max_object_bytes,
        "history SST size is outside the scrub bound"
    );
    let size = result.meta.size;
    let source_etag = result.meta.e_tag.clone();
    let encoded = result.bytes().await?;
    anyhow::ensure!(
        encoded.len() as u64 == size,
        "history SST body length does not match metadata"
    );
    Ok((
        size,
        source_etag,
        crate::crypto::hex(&Sha256::digest(&encoded)),
    ))
}

async fn read_history_baseline(
    store: Arc<dyn ObjectStore>,
    source_path: &str,
) -> anyhow::Result<Option<HistoryBaseline>> {
    let encoded = match store.get(&history_baseline_path(source_path)).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        encoded.len() <= MAX_HISTORY_BASELINE_BYTES,
        "history integrity baseline is too large"
    );
    Ok(Some(serde_json::from_slice(&encoded)?))
}

async fn create_history_baseline(
    store: Arc<dyn ObjectStore>,
    baseline: HistoryBaseline,
) -> anyhow::Result<()> {
    validate_history_baseline(
        &baseline,
        &baseline.source_path,
        baseline.size,
        &baseline.sha256,
    )?;
    anyhow::ensure!(baseline.created_ms > 0, "history baseline has no timestamp");
    let path = history_baseline_path(&baseline.source_path);
    let encoded = serde_json::to_vec(&baseline)?;
    anyhow::ensure!(
        encoded.len() <= MAX_HISTORY_BASELINE_BYTES,
        "history integrity baseline is too large"
    );
    match store
        .put_opts(
            &path,
            PutPayload::from(Bytes::from(encoded)),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = read_history_baseline(store, &baseline.source_path)
                .await?
                .ok_or_else(|| anyhow::anyhow!("history baseline disappeared after conflict"))?;
            validate_history_baseline(
                &existing,
                &baseline.source_path,
                baseline.size,
                &baseline.sha256,
            )
        }
        Err(error) => Err(error.into()),
    }
}

async fn finalize_history_baseline(
    store: Arc<dyn ObjectStore>,
    source_path: &str,
    size: u64,
    sha256: &str,
    source_etag: Option<String>,
) -> anyhow::Result<()> {
    let path = history_baseline_path(source_path);
    for _ in 0..5 {
        let result = store.get(&path).await?;
        anyhow::ensure!(
            result.meta.size <= MAX_HISTORY_BASELINE_BYTES as u64,
            "history integrity baseline is too large"
        );
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let raw = result.bytes().await?;
        let mut baseline: HistoryBaseline = serde_json::from_slice(&raw)?;
        validate_history_baseline(&baseline, source_path, size, sha256)?;
        if baseline.logical_verified || baseline.source_etag.is_some() {
            return Ok(());
        }
        baseline.logical_verified = true;
        baseline.source_etag = source_etag.clone();
        let encoded = serde_json::to_vec(&baseline)?;
        anyhow::ensure!(
            encoded.len() <= MAX_HISTORY_BASELINE_BYTES,
            "history integrity baseline is too large"
        );
        match store
            .put_opts(
                &path,
                PutPayload::from(Bytes::from(encoded)),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!("history baseline finalization CAS retries exhausted")
}

fn validate_history_baseline(
    baseline: &HistoryBaseline,
    source_path: &str,
    size: u64,
    sha256: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        baseline.format_version == HISTORY_BASELINE_FORMAT_VERSION
            && baseline.source_path == source_path
            && baseline.size == size
            && baseline.sha256 == sha256
            && baseline.created_ms > 0
            && baseline
                .source_etag
                .as_ref()
                .is_none_or(|etag| !etag.is_empty() && etag.len() <= 1_024)
            && baseline.sha256.len() == 64
            && baseline
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "encrypted history SST does not match its writer-verified baseline"
    );
    Ok(())
}

pub(crate) fn history_baseline_path(source_path: &str) -> ObjPath {
    let digest = Sha256::digest(format!("history\0{source_path}"));
    ObjPath::from(format!(
        "integrity/history/{}.json",
        crate::crypto::hex(&digest)
    ))
}

async fn read_cursor(store: Arc<dyn ObjectStore>) -> anyhow::Result<Cursor> {
    let encoded = match store.get(&state_path()).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(Cursor::default()),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        encoded.len() <= MAX_STATE_BYTES,
        "primary scrub state is too large"
    );
    let state: DurableState = serde_json::from_slice(&encoded)?;
    anyhow::ensure!(
        state.format_version == STATE_FORMAT_VERSION
            && state.updated_ms > 0
            && ((state.coordinator_epoch == 0 && state.coordinator_sequence == 0)
                || (state.coordinator_epoch > 0 && state.coordinator_sequence > 0)),
        "unsupported or malformed primary scrub state"
    );
    validate_cursor(&state.cursor)?;
    Ok(state.cursor)
}

async fn publish_cursor(
    store: Arc<dyn ObjectStore>,
    cursor: Cursor,
    coordinator_epoch: u64,
    coordinator_sequence: u64,
) -> anyhow::Result<()> {
    validate_cursor(&cursor)?;
    let state = DurableState {
        format_version: STATE_FORMAT_VERSION,
        cursor,
        updated_ms: crate::backup::wall_time_ms(),
        coordinator_epoch,
        coordinator_sequence,
    };
    let encoded = serde_json::to_vec(&state)?;
    anyhow::ensure!(
        encoded.len() <= MAX_STATE_BYTES,
        "primary scrub state is too large"
    );
    let path = state_path();
    if coordinator_epoch == 0 {
        store
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await?;
        return Ok(());
    }
    for _ in 0..5 {
        let mode = match store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let current_bytes = result.bytes().await?;
                anyhow::ensure!(
                    current_bytes.len() <= MAX_STATE_BYTES,
                    "primary scrub state is too large"
                );
                let current: DurableState = serde_json::from_slice(&current_bytes)?;
                let current_order = (current.coordinator_epoch, current.coordinator_sequence);
                let next_order = (coordinator_epoch, coordinator_sequence);
                anyhow::ensure!(
                    current_order < next_order,
                    "primary scrub cursor publication was fenced"
                );
                PutMode::Update(version)
            }
            Err(object_store::Error::NotFound { .. }) => PutMode::Create,
            Err(error) => return Err(error.into()),
        };
        match store
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
    anyhow::bail!("primary scrub cursor CAS retries exhausted")
}

fn validate_cursor(cursor: &Cursor) -> anyhow::Result<()> {
    anyhow::ensure!(
        cursor.unit.is_none() || cursor.database.is_some(),
        "primary scrub cursor unit has no database"
    );
    for value in [&cursor.database, &cursor.unit].into_iter().flatten() {
        anyhow::ensure!(
            !value.is_empty() && value.len() <= 4_096 && !value.contains('\0'),
            "primary scrub cursor is malformed"
        );
    }
    Ok(())
}

fn state_path() -> ObjPath {
    ObjPath::from("integrity/primary-scrub.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{FlushOptions, FlushType};

    struct XorTransformer(u8);

    #[async_trait::async_trait]
    impl BlockTransformer for XorTransformer {
        async fn encode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
            Ok(Bytes::from(
                data.iter().map(|byte| byte ^ self.0).collect::<Vec<_>>(),
            ))
        }

        async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
            self.encode(data).await
        }
    }

    async fn populated_config() -> PrimaryScrubConfig {
        let ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shards: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let data: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
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
        db.put(b"alpha", b"durable-value").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();
        PrimaryScrubConfig {
            cell_id: None,
            topology_store: ops.clone(),
            registry_store: ops,
            shard_store: shards,
            data_store: data,
            max_object_bytes: 16 * 1024 * 1024,
        }
    }

    async fn compacted_work(config: &PrimaryScrubConfig) -> WorkUnit {
        let target = database_targets(config).await.unwrap().remove(0);
        database_work(&target, config.max_object_bytes)
            .await
            .unwrap()
            .into_iter()
            .find(|unit| matches!(unit.work, Work::Compacted { .. }))
            .expect("flushed database has a compacted SST")
    }

    #[tokio::test]
    async fn durable_cursor_progresses_and_rejects_stale_coordinator() {
        let config = populated_config().await;
        let first_report = scrub_batch(&config, 1, 7, 1).await.unwrap();
        assert_eq!(first_report.checked, 1);
        assert!(!first_report.completed_sweep);
        let first: DurableState = serde_json::from_slice(
            &config
                .topology_store
                .get(&state_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(first.cursor.database.is_some());
        assert!(
            first
                .cursor
                .unit
                .as_deref()
                .is_some_and(|unit| unit.starts_with("0-manifest-"))
        );

        let second_report = scrub_batch(&config, 1, 7, 2).await.unwrap();
        assert_eq!(second_report.checked, 1);
        assert!(!second_report.completed_sweep);
        let second: DurableState = serde_json::from_slice(
            &config
                .topology_store
                .get(&state_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_ne!(first.cursor.unit, second.cursor.unit);
        assert_eq!(second.coordinator_sequence, 2);
        let completed = scrub_batch(&config, 1, 7, 3).await.unwrap();
        assert_eq!(completed.checked, 0);
        assert!(completed.completed_sweep);
        assert!(scrub_batch(&config, 1, 7, 2).await.is_err());
        assert!(scrub_batch(&config, 1, 6, u64::MAX).await.is_err());
    }

    #[tokio::test]
    async fn logical_reader_detects_corrupt_and_missing_referenced_sst() {
        let config = populated_config().await;
        let unit = compacted_work(&config).await;
        verify_work(&unit, &config).await.unwrap();
        let (root, id) = match &unit.work {
            Work::Compacted { root, handle, .. } => {
                let SsTableId::Compacted(id) = handle.id else {
                    panic!("compacted work has a WAL id")
                };
                (root, id)
            }
            _ => unreachable!(),
        };
        let path = ObjPath::from(format!("{root}/compacted/{id}.sst"));
        let mut encoded = config
            .shard_store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        assert!(!encoded.is_empty());
        encoded[0] ^= 0xff;
        config
            .shard_store
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await
            .unwrap();
        assert!(verify_work(&unit, &config).await.is_err());

        config.shard_store.delete(&path).await.unwrap();
        assert!(verify_work(&unit, &config).await.is_err());
    }

    #[tokio::test]
    async fn latest_manifest_is_bounded_and_logically_decoded() {
        let config = populated_config().await;
        let target = database_targets(&config).await.unwrap().remove(0);
        assert!(database_work(&target, 1).await.is_err());

        let prefix = ObjPath::from("shards/root/manifest");
        let mut listing = config.shard_store.list(Some(&prefix));
        let mut latest = None;
        while let Some(meta) = listing.try_next().await.unwrap() {
            if let Some(id) = manifest_id("shards/root", &meta.location) {
                if latest
                    .as_ref()
                    .is_none_or(|(latest_id, _): &(u64, ObjPath)| id > *latest_id)
                {
                    latest = Some((id, meta.location));
                }
            }
        }
        let (_, path) = latest.expect("database has a manifest");
        let mut encoded = config
            .shard_store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        encoded[0] ^= 0xff;
        config
            .shard_store
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await
            .unwrap();
        assert!(
            database_work(&target, config.max_object_bytes)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn logical_reader_detects_same_size_wal_corruption() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::Db::open("wal-db", store.clone()).await.unwrap();
        db.put(b"key", b"wal-value").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();
        let mut id = None;
        for file in WalReader::new("wal-db", store.clone())
            .list(..)
            .await
            .unwrap()
        {
            if file.metadata().await.unwrap().metadata.size > 0 {
                id = Some(file.id);
                break;
            }
        }
        let id = id.expect("WAL flush created a non-fencing file");
        let target = DatabaseTarget {
            key: "shard:wal-db".to_string(),
            path: "wal-db".to_string(),
            store: store.clone(),
            verification: ObjectVerification::Logical,
        };
        assert!(
            database_work(&target, 16 * 1024 * 1024)
                .await
                .unwrap()
                .iter()
                .any(|unit| matches!(&unit.work, Work::Wal { id: found, .. } if *found == id)),
            "latest manifest must enumerate its replayable WAL"
        );
        let unit = WorkUnit {
            key: format!("2-wal-{id:020}"),
            work: Work::Wal {
                root: "wal-db".to_string(),
                store: store.clone(),
                id,
            },
        };
        let config = PrimaryScrubConfig {
            cell_id: None,
            topology_store: Arc::new(object_store::memory::InMemory::new()),
            registry_store: Arc::new(object_store::memory::InMemory::new()),
            shard_store: store.clone(),
            data_store: Arc::new(object_store::memory::InMemory::new()),
            max_object_bytes: 16 * 1024 * 1024,
        };
        verify_work(&unit, &config).await.unwrap();
        let path = ObjPath::from(format!("wal-db/wal/{id:020}.sst"));
        let mut encoded = store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        encoded[0] ^= 0xff;
        store
            .put(&path, PutPayload::from(Bytes::from(encoded)))
            .await
            .unwrap();
        assert!(verify_work(&unit, &config).await.is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn history_writer_baseline_precedes_and_fences_immutable_sst_put() {
        let integrity: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let data: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let writer =
            HistoryIntegrityStore::new(data.clone(), integrity.clone(), "streams/history-a", 1024);
        let path = ObjPath::from("streams/history-a/compacted/one.sst");
        writer
            .put(&path, PutPayload::from_static(b"encrypted-sst"))
            .await
            .unwrap();
        verify_history_baseline(path.as_ref(), data.clone(), integrity.clone(), 1024)
            .await
            .unwrap();

        let error = writer
            .put(&path, PutPayload::from_static(b"changed-sst"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("writer-verified baseline"));
        assert_eq!(
            data.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"encrypted-sst")
        );
    }

    #[tokio::test]
    async fn encrypted_history_uses_writer_verified_immutable_digest() {
        let ops: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let data: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let transformer: Arc<dyn BlockTransformer> = Arc::new(XorTransformer(0x5a));
        let path = "streams/encrypted-history";
        let db = slatedb::Db::builder(path, data.clone())
            .with_block_transformer(transformer.clone())
            .build()
            .await
            .unwrap();
        db.put(b"key", b"encrypted-value").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        assert_eq!(
            record_history_baselines(
                ops.clone(),
                data.clone(),
                path,
                16 * 1024 * 1024,
                transformer,
            )
            .await
            .unwrap(),
            1
        );
        let target = DatabaseTarget {
            key: format!("data:{path}"),
            path: path.to_string(),
            store: data.clone(),
            verification: ObjectVerification::WriterBaseline,
        };
        let unit = database_work(&target, 16 * 1024 * 1024)
            .await
            .unwrap()
            .into_iter()
            .find(|unit| matches!(unit.work, Work::HistoryObject { .. }))
            .unwrap();
        let config = PrimaryScrubConfig {
            cell_id: None,
            topology_store: ops.clone(),
            registry_store: ops,
            shard_store: Arc::new(object_store::memory::InMemory::new()),
            data_store: data.clone(),
            max_object_bytes: 16 * 1024 * 1024,
        };
        verify_work(&unit, &config).await.unwrap();
        let source_path = match &unit.work {
            Work::HistoryObject { source_path, .. } => ObjPath::from(source_path.as_str()),
            _ => unreachable!(),
        };
        let mut encoded = data
            .get(&source_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        encoded[0] ^= 0xff;
        data.put(&source_path, PutPayload::from(Bytes::from(encoded)))
            .await
            .unwrap();
        assert!(verify_work(&unit, &config).await.is_err());
    }
}
