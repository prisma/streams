//! Crash-recoverable online shard split actor.
//!
//! A CAS-created intent is the distributed exclusion point. The ring owner
//! closes the parent through ShardEngine's durability barrier, creates two
//! generation-specific projection clones, and publishes both paths with one
//! topology CAS. Every phase is derived from durable objects, so a new owner
//! can resume after process loss without reopening the parent to traffic.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use slatedb::admin::AdminBuilder;
use slatedb::{CloneSourceSpec, DbReader};

use crate::http::{AppState, ring_pick};
use crate::registry::{
    Topology, cas_publish_topology_split_with_paths, load_topology, shard_projection_bounds,
};
use crate::shard::now_ms;

const INTENT_VERSION: u32 = 1;
// A dead owner is reclaimable inside the COMPUTE-SPEC 15 s crash RTO. Live
// operations renew every three seconds; a different owner always waits for
// expiry, so clock/heartbeat observations are not used as a fencing oracle.
const LEASE_MS: i64 = 12_000;
const LEASE_RENEW_MS: u64 = 3_000;
const MAX_INTENTS: usize = 1_536;

/// Release-mode crash injection used only by the split recovery matrix. The
/// deliberately awkward TEST name prevents this from looking like an
/// operator feature; production manifests must never set it.
fn test_crash_after(phase: &str) {
    if std::env::var("STREAMS_TEST_SPLIT_CRASH_AFTER").as_deref() == Ok(phase) {
        tracing::error!(phase, "test crash after durable split transition");
        std::process::abort();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AutoSplitConfig {
    pub single_shard_write_ceiling_bytes_per_sec: u64,
    pub sustain: Duration,
    pub gc_retention: Duration,
    pub gc_interval: Duration,
}

#[derive(Clone, Copy)]
struct HotSample {
    total_bytes: u64,
    sampled_at: Instant,
    above_since: Option<Instant>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SplitIntent {
    version: u32,
    operation_id: String,
    parent: String,
    parent_path: String,
    zero_path: String,
    one_path: String,
    created_ms: i64,
    lease_owner: String,
    lease_until_ms: i64,
    #[serde(default)]
    abandoned_generations: Vec<AbandonedGeneration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AbandonedGeneration {
    operation_id: String,
    abandoned_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct GcCandidate {
    version: u32,
    operation_id: String,
    abandoned_ms: i64,
}

fn intent_path(parent: &str) -> ObjPath {
    ObjPath::from(format!(
        "split-intents/{}.json",
        if parent.is_empty() { "root" } else { parent }
    ))
}

fn new_operation(parent: &str) -> (String, String, String) {
    let mut operation = [0u8; 16];
    use rand::RngCore;
    rand::rng().fill_bytes(&mut operation);
    let operation_id = crate::crypto::hex(&operation);
    let base = format!("shards/splits/{operation_id}/");
    (
        operation_id,
        format!("{base}{parent}0"),
        format!("{base}{parent}1"),
    )
}

fn validate_intent(intent: &SplitIntent) -> Result<(), String> {
    if intent.version != INTENT_VERSION
        || intent.operation_id.len() != 32
        || !intent
            .operation_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || intent.parent.len() >= 128
        || !intent.parent.bytes().all(|bit| bit == b'0' || bit == b'1')
        || intent.lease_owner.len() > 256
        || intent.parent_path.len() > 512
        || intent.parent_path.contains("//")
        || intent
            .parent_path
            .split('/')
            .any(|component| component == "..")
        || intent.abandoned_generations.len() > 64
        || intent.abandoned_generations.iter().any(|generation| {
            generation.operation_id.len() != 32
                || !generation
                    .operation_id
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                || generation.abandoned_ms <= 0
        })
    {
        return Err("malformed split intent".to_string());
    }
    let base = format!("shards/splits/{}/", intent.operation_id);
    if intent.zero_path != format!("{base}{}0", intent.parent)
        || intent.one_path != format!("{base}{}1", intent.parent)
        || !intent.parent_path.starts_with("shards/")
    {
        return Err("split intent paths do not match its operation".to_string());
    }
    ObjPath::parse(&intent.parent_path)
        .map_err(|_| "malformed split intent parent path".to_string())?;
    Ok(())
}

async fn read_intent(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
) -> Result<Option<(SplitIntent, Option<String>)>, String> {
    match store.get(&intent_path(parent)).await {
        Ok(result) => {
            let etag = result.meta.e_tag.clone();
            let raw = result.bytes().await.map_err(|error| error.to_string())?;
            let intent: SplitIntent =
                serde_json::from_slice(&raw).map_err(|_| "malformed split intent".to_string())?;
            validate_intent(&intent)?;
            Ok(Some((intent, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

async fn create_intent(state: &AppState, parent: &str) -> Result<SplitIntent, String> {
    let topology = load_topology(&state.ops_store)
        .await
        .map_err(|error| error.to_string())?;
    if !topology.shards.iter().any(|prefix| prefix == parent) {
        return Err("split parent is not live".to_string());
    }
    let (operation_id, zero_path, one_path) = new_operation(parent);
    let intent = SplitIntent {
        version: INTENT_VERSION,
        operation_id,
        parent: parent.to_string(),
        parent_path: topology.db_path(parent),
        zero_path,
        one_path,
        created_ms: now_ms(),
        lease_owner: state.instance_name.clone(),
        lease_until_ms: now_ms() + LEASE_MS,
        abandoned_generations: Vec::new(),
    };
    validate_intent(&intent)?;
    let result = state
        .shard_store
        .put_opts(
            &intent_path(parent),
            PutPayload::from(serde_json::to_vec(&intent).expect("intent json")),
            PutOptions::from(PutMode::Create),
        )
        .await;
    match result {
        Ok(_) => Ok(intent),
        Err(object_store::Error::AlreadyExists { .. }) => read_intent(&state.shard_store, parent)
            .await?
            .map(|(intent, _)| intent)
            .ok_or_else(|| "split intent raced then disappeared".to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn claim_intent(state: &AppState, parent: &str) -> Result<Option<SplitIntent>, String> {
    for _ in 0..5 {
        let Some((mut intent, etag)) = read_intent(&state.shard_store, parent).await? else {
            return Ok(None);
        };
        if intent.lease_owner != state.instance_name {
            if intent.lease_until_ms > now_ms() {
                return Ok(None);
            }
            let topology = load_topology(&state.ops_store)
                .await
                .map_err(|error| error.to_string())?;
            if topology.shards.iter().any(|prefix| prefix == parent) {
                // Never reuse an abandoned attempt's paths: the old process
                // or an object-store request may still be completing writes
                // after lease expiry. If topology already published this
                // operation, retain it and let reconcile remove the intent.
                if intent.abandoned_generations.len() >= 64 {
                    return Err("split takeover generation bound exceeded".to_string());
                }
                intent.abandoned_generations.push(AbandonedGeneration {
                    operation_id: intent.operation_id.clone(),
                    abandoned_ms: now_ms(),
                });
                let (operation_id, zero_path, one_path) = new_operation(parent);
                intent.operation_id = operation_id;
                intent.zero_path = zero_path;
                intent.one_path = one_path;
            }
        }
        intent.lease_owner = state.instance_name.clone();
        intent.lease_until_ms = now_ms() + LEASE_MS;
        let result = state
            .shard_store
            .put_opts(
                &intent_path(parent),
                PutPayload::from(serde_json::to_vec(&intent).expect("intent json")),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: etag,
                    version: None,
                })),
            )
            .await;
        match result {
            Ok(_) => {
                persist_gc_candidates(state, &intent).await?;
                return Ok(Some(intent));
            }
            Err(object_store::Error::Precondition { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("split intent lease CAS retries exhausted".to_string())
}

fn gc_candidate_path(operation_id: &str) -> ObjPath {
    ObjPath::from(format!("split-gc-candidates/{operation_id}.json"))
}

async fn persist_gc_candidates(state: &AppState, intent: &SplitIntent) -> Result<(), String> {
    for abandoned in &intent.abandoned_generations {
        let candidate = GcCandidate {
            version: 1,
            operation_id: abandoned.operation_id.to_ascii_lowercase(),
            abandoned_ms: abandoned.abandoned_ms,
        };
        match state
            .shard_store
            .put_opts(
                &gc_candidate_path(&candidate.operation_id),
                PutPayload::from(serde_json::to_vec(&candidate).expect("GC candidate json")),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

async fn renew_intent(state: &AppState, parent: &str, operation_id: &str) -> Result<(), String> {
    for _ in 0..5 {
        let Some((mut intent, etag)) = read_intent(&state.shard_store, parent).await? else {
            return Err("split intent disappeared while leased".to_string());
        };
        if intent.operation_id != operation_id || intent.lease_owner != state.instance_name {
            return Err("split intent lease was lost".to_string());
        }
        intent.lease_until_ms = now_ms() + LEASE_MS;
        match state
            .shard_store
            .put_opts(
                &intent_path(parent),
                PutPayload::from(serde_json::to_vec(&intent).expect("intent json")),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: etag,
                    version: None,
                })),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("split intent renewal CAS retries exhausted".to_string())
}

async fn verify_intent_owner(
    state: &AppState,
    parent: &str,
    operation_id: &str,
) -> Result<(), String> {
    let Some((intent, _)) = read_intent(&state.shard_store, parent).await? else {
        return Err("split intent disappeared before topology publish".to_string());
    };
    if intent.operation_id != operation_id
        || intent.lease_owner != state.instance_name
        || intent.lease_until_ms <= now_ms()
    {
        return Err("split intent is not owned before topology publish".to_string());
    }
    Ok(())
}

struct SplitWorkerGuard {
    state: Arc<AppState>,
    parent: String,
}

struct LeaseRenewer(tokio::task::JoinHandle<()>);

impl Drop for LeaseRenewer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Drop for SplitWorkerGuard {
    fn drop(&mut self) {
        self.state
            .split_workers
            .lock()
            .unwrap()
            .remove(&self.parent);
    }
}

fn acquire_worker(state: &Arc<AppState>, parent: &str) -> Option<SplitWorkerGuard> {
    if !state
        .split_workers
        .lock()
        .unwrap()
        .insert(parent.to_string())
    {
        return None;
    }
    Some(SplitWorkerGuard {
        state: state.clone(),
        parent: parent.to_string(),
    })
}

fn is_ring_owner(state: &AppState, parent: &str) -> bool {
    let active = state.ring_active.read().unwrap();
    active.is_empty()
        || active
            .get(ring_pick(parent, &active))
            .is_some_and(|owner| owner == &state.instance_name)
}

async fn path_has_objects(store: &Arc<dyn ObjectStore>, path: &str) -> Result<bool, String> {
    match store.list(Some(&ObjPath::from(path))).next().await {
        Some(Ok(_)) => Ok(true),
        Some(Err(error)) => Err(error.to_string()),
        None => Ok(false),
    }
}

async fn child_is_valid(store: &Arc<dyn ObjectStore>, path: &str) -> bool {
    match DbReader::builder(path, store.clone()).build().await {
        Ok(reader) => {
            reader.close().await.ok();
            true
        }
        Err(_) => false,
    }
}

async fn published_children_are_valid(state: &AppState, topology: &Topology, parent: &str) -> bool {
    let zero = format!("{parent}0");
    let one = format!("{parent}1");
    topology.shards.contains(&zero)
        && topology.shards.contains(&one)
        && child_is_valid(&state.shard_store, &topology.db_path(&zero)).await
        && child_is_valid(&state.shard_store, &topology.db_path(&one)).await
}

async fn clear_partial_child(store: &Arc<dyn ObjectStore>, path: &str) -> Result<(), String> {
    let mut list = store.list(Some(&ObjPath::from(path)));
    let mut objects = Vec::new();
    while let Some(item) = list.next().await {
        objects.push(item.map_err(|error| error.to_string())?.location);
        if objects.len() > 100_000 {
            return Err("partial split child cleanup exceeds object bound".to_string());
        }
    }
    for object in objects {
        store
            .delete(&object)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn ensure_child(
    store: &Arc<dyn ObjectStore>,
    parent_path: &str,
    child_prefix: &str,
    child_path: &str,
) -> Result<(), String> {
    if path_has_objects(store, child_path).await? {
        if child_is_valid(store, child_path).await {
            return Ok(());
        }
        clear_partial_child(store, child_path).await?;
    }
    let mut source = CloneSourceSpec::new(parent_path);
    source.projection_range =
        Some(shard_projection_bounds(child_prefix).map_err(|error| error.to_string())?);
    AdminBuilder::new(child_path, store.clone())
        .build()
        .create_clone_builder_from_source(source)
        .build()
        .await
        .map_err(|error| error.to_string())?;
    if !child_is_valid(store, child_path).await {
        return Err("projected split child failed reopen verification".to_string());
    }
    Ok(())
}

async fn open_and_quiesce_parent(
    state: &Arc<AppState>,
    intent: &SplitIntent,
) -> Result<(), String> {
    let cached = state.shards.read().unwrap().get(&intent.parent).cloned();
    let engine = if let Some(engine) = cached.filter(|engine| !engine.is_closed()) {
        engine
    } else {
        state.shards.write().unwrap().remove(&intent.parent);
        let engine = (state.opener.open)(intent.parent.clone(), intent.parent_path.clone())
            .await
            .map_err(|error| error.to_string())?;
        state
            .shards
            .write()
            .unwrap()
            .insert(intent.parent.clone(), engine.clone());
        engine
    };
    engine.quiesce_for_split().await
}

fn install_topology(state: &AppState, topology: Topology) {
    let live: std::collections::HashSet<&str> =
        topology.shards.iter().map(String::as_str).collect();
    *state.topology.write().unwrap() = topology.clone();
    state
        .topology_version
        .store(topology.version, Ordering::Release);
    let retired: Vec<_> = {
        let mut shards = state.shards.write().unwrap();
        let retired_prefixes: Vec<_> = shards
            .keys()
            .filter(|prefix| !live.contains(prefix.as_str()))
            .cloned()
            .collect();
        retired_prefixes
            .into_iter()
            .filter_map(|prefix| shards.remove(&prefix))
            .collect()
    };
    for engine in retired {
        engine.retire();
    }
}

async fn reconcile_inner(
    state: Arc<AppState>,
    intent: SplitIntent,
    lease_lost: Arc<AtomicBool>,
) -> Result<(), String> {
    let current = load_topology(&state.ops_store)
        .await
        .map_err(|error| error.to_string())?;
    let zero = format!("{}0", intent.parent);
    let one = format!("{}1", intent.parent);
    if !current.shards.iter().any(|prefix| prefix == &intent.parent) {
        // A previous claimant may have won the topology CAS with a different
        // generation after our lease expired. Accept only a fully reopenable
        // pair; this also resolves a lost successful CAS response.
        if published_children_are_valid(&state, &current, &intent.parent).await {
            install_topology(&state, current);
            state
                .shard_store
                .delete(&intent_path(&intent.parent))
                .await
                .map_err(|error| error.to_string())?;
            test_crash_after("intent_deleted");
            state
                .splitting_prefixes
                .write()
                .unwrap()
                .remove(&intent.parent);
            return Ok(());
        }
        return Err("split parent disappeared into an unrelated topology".to_string());
    }

    open_and_quiesce_parent(&state, &intent).await?;
    test_crash_after("parent_quiesced");
    if lease_lost.load(Ordering::Acquire) {
        return Err("split intent lease renewal failed".to_string());
    }
    renew_intent(&state, &intent.parent, &intent.operation_id).await?;
    ensure_child(
        &state.shard_store,
        &intent.parent_path,
        &zero,
        &intent.zero_path,
    )
    .await?;
    test_crash_after("zero_child_ready");
    if lease_lost.load(Ordering::Acquire) {
        return Err("split intent lease renewal failed".to_string());
    }
    renew_intent(&state, &intent.parent, &intent.operation_id).await?;
    ensure_child(
        &state.shard_store,
        &intent.parent_path,
        &one,
        &intent.one_path,
    )
    .await?;
    test_crash_after("one_child_ready");
    if lease_lost.load(Ordering::Acquire) {
        return Err("split intent lease renewal failed".to_string());
    }
    renew_intent(&state, &intent.parent, &intent.operation_id).await?;
    verify_intent_owner(&state, &intent.parent, &intent.operation_id).await?;

    let mut published = None;
    for _ in 0..5 {
        let topology = load_topology(&state.ops_store)
            .await
            .map_err(|error| error.to_string())?;
        if !topology.shards.contains(&intent.parent) {
            if published_children_are_valid(&state, &topology, &intent.parent).await {
                published = Some(topology);
                break;
            }
            return Err("split parent disappeared into invalid children".to_string());
        }
        renew_intent(&state, &intent.parent, &intent.operation_id).await?;
        verify_intent_owner(&state, &intent.parent, &intent.operation_id).await?;
        match cas_publish_topology_split_with_paths(
            &state.ops_store,
            &intent.parent,
            topology.version,
            Some((&intent.zero_path, &intent.one_path)),
        )
        .await
        {
            Ok(topology) => {
                published = Some(topology);
                break;
            }
            Err(object_store::Error::Precondition { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    let topology = published.ok_or_else(|| "topology split CAS retries exhausted".to_string())?;
    test_crash_after("topology_published");
    install_topology(&state, topology);
    state
        .shard_store
        .delete(&intent_path(&intent.parent))
        .await
        .map_err(|error| error.to_string())?;
    test_crash_after("intent_deleted");
    state
        .splitting_prefixes
        .write()
        .unwrap()
        .remove(&intent.parent);
    Ok(())
}

async fn reconcile(state: Arc<AppState>, intent: SplitIntent) -> Result<(), String> {
    let lease_lost = Arc::new(AtomicBool::new(false));
    let renew_state = state.clone();
    let renew_parent = intent.parent.clone();
    let renew_operation = intent.operation_id.clone();
    let renew_lost = lease_lost.clone();
    let _renewer = LeaseRenewer(tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(LEASE_RENEW_MS)).await;
            if let Err(error) = renew_intent(&renew_state, &renew_parent, &renew_operation).await {
                tracing::warn!(parent = %renew_parent, "split lease renewal stopped: {error}");
                renew_lost.store(true, Ordering::Release);
                break;
            }
        }
    }));
    reconcile_inner(state, intent, lease_lost).await
}

async fn list_intents(store: &Arc<dyn ObjectStore>) -> Result<Vec<SplitIntent>, String> {
    let mut list = store.list(Some(&ObjPath::from("split-intents")));
    let mut intents = Vec::new();
    while let Some(item) = list.next().await {
        let meta = item.map_err(|error| error.to_string())?;
        if !meta.location.as_ref().ends_with(".json") {
            continue;
        }
        let result = store
            .get(&meta.location)
            .await
            .map_err(|error| error.to_string())?;
        let raw = result.bytes().await.map_err(|error| error.to_string())?;
        let intent: SplitIntent =
            serde_json::from_slice(&raw).map_err(|_| "malformed split intent".to_string())?;
        validate_intent(&intent)?;
        intents.push(intent);
        if intents.len() > MAX_INTENTS {
            return Err("split intent count exceeds topology bound".to_string());
        }
    }
    Ok(intents)
}

fn split_operation(path: &str) -> Option<&str> {
    let operation = path.strip_prefix("shards/splits/")?.split('/').next()?;
    (operation.len() == 32 && operation.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(operation)
}

fn referenced_split_operations(
    topology: &Topology,
    intents: &[SplitIntent],
) -> std::collections::HashSet<String> {
    let mut referenced = std::collections::HashSet::new();
    let paths = topology
        .shard_paths
        .values()
        .map(String::as_str)
        .chain(intents.iter().flat_map(|intent| {
            [
                intent.parent_path.as_str(),
                intent.zero_path.as_str(),
                intent.one_path.as_str(),
            ]
        }));
    for path in paths {
        if let Some(operation) = split_operation(path) {
            referenced.insert(operation.to_ascii_lowercase());
        }
    }
    referenced.extend(intents.iter().flat_map(|intent| {
        intent
            .abandoned_generations
            .iter()
            .map(|generation| generation.operation_id.to_ascii_lowercase())
    }));
    referenced
}

async fn gc_abandoned_generations(state: &AppState, retention: Duration) -> Result<usize, String> {
    const MAX_OBJECTS_PER_RUN: usize = 100_000;
    let topology = load_topology(&state.ops_store)
        .await
        .map_err(|error| error.to_string())?;
    let intents = list_intents(&state.shard_store).await?;
    let referenced = referenced_split_operations(&topology, &intents);
    let cutoff_ms =
        now_ms().saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX));
    let mut candidates = Vec::new();
    let mut candidate_list = state
        .shard_store
        .list(Some(&ObjPath::from("split-gc-candidates")));
    while let Some(item) = candidate_list.next().await {
        let meta = item.map_err(|error| error.to_string())?;
        if candidates.len() >= MAX_OBJECTS_PER_RUN {
            return Err("split GC candidate bound exceeded".to_string());
        }
        let result = state
            .shard_store
            .get(&meta.location)
            .await
            .map_err(|error| error.to_string())?;
        let raw = result.bytes().await.map_err(|error| error.to_string())?;
        let candidate: GcCandidate =
            serde_json::from_slice(&raw).map_err(|_| "malformed split GC candidate".to_string())?;
        if candidate.version != 1
            || candidate.operation_id.len() != 32
            || !candidate
                .operation_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || candidate.abandoned_ms <= 0
            || meta.location != gc_candidate_path(&candidate.operation_id)
        {
            return Err("malformed split GC candidate".to_string());
        }
        candidates.push((candidate, meta.location));
    }

    let mut listed = 0usize;
    let mut deleted = 0usize;
    for (candidate, marker_path) in candidates {
        let operation = candidate.operation_id.to_ascii_lowercase();
        if candidate.abandoned_ms > cutoff_ms || referenced.contains(&operation) {
            continue;
        }
        // Close the scan/delete race with a fresh reference snapshot. The
        // actor publishes topology before deleting its intent, so a live or
        // publishable generation is always present in at least one set.
        let topology = load_topology(&state.ops_store)
            .await
            .map_err(|error| error.to_string())?;
        let intents = list_intents(&state.shard_store).await?;
        if referenced_split_operations(&topology, &intents).contains(&operation) {
            continue;
        }
        let mut objects = Vec::new();
        let mut list = state
            .shard_store
            .list(Some(&ObjPath::from(format!("shards/splits/{operation}"))));
        while let Some(item) = list.next().await {
            listed = listed.saturating_add(1);
            if listed > MAX_OBJECTS_PER_RUN {
                return Err("split generation GC object bound exceeded".to_string());
            }
            objects.push(item.map_err(|error| error.to_string())?.location);
        }
        for object in objects {
            state
                .shard_store
                .delete(&object)
                .await
                .map_err(|error| error.to_string())?;
            deleted = deleted.saturating_add(1);
        }
        state
            .shard_store
            .delete(&marker_path)
            .await
            .map_err(|error| error.to_string())?;
        tracing::info!(operation, "deleted abandoned split generation");
    }
    Ok(deleted)
}

pub async fn initialize(state: &Arc<AppState>) -> Result<(), String> {
    let intents = list_intents(&state.shard_store).await?;
    let mut splitting = state.splitting_prefixes.write().unwrap();
    splitting.extend(intents.into_iter().map(|intent| intent.parent));
    Ok(())
}

pub fn start(state: Arc<AppState>, auto: AutoSplitConfig) {
    tokio::spawn(async move {
        let mut hot_samples: std::collections::HashMap<String, HotSample> =
            std::collections::HashMap::new();
        let mut next_gc = Instant::now() + auto.gc_interval;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let intents = match list_intents(&state.shard_store).await {
                Ok(intents) => intents,
                Err(error) => {
                    tracing::error!("split intent scan failed: {error}");
                    state.split_ready.store(false, Ordering::Release);
                    continue;
                }
            };
            {
                let topology = state.topology.read().unwrap().clone();
                let mut splitting = state.splitting_prefixes.write().unwrap();
                splitting.clear();
                splitting.extend(
                    intents
                        .iter()
                        .filter(|intent| topology.shards.contains(&intent.parent))
                        .map(|intent| intent.parent.clone()),
                );
            }
            state.split_ready.store(true, Ordering::Release);
            for intent in intents {
                // The original owner is allowed to finish during a ring
                // resize. Otherwise only the current owner may take over.
                let owns_lease =
                    intent.lease_owner == state.instance_name && intent.lease_until_ms > now_ms();
                if !owns_lease && !is_ring_owner(&state, &intent.parent) {
                    continue;
                }
                let Some(_worker) = acquire_worker(&state, &intent.parent) else {
                    continue;
                };
                match claim_intent(&state, &intent.parent).await {
                    Ok(Some(claimed)) => {
                        if let Err(error) = reconcile(state.clone(), claimed).await {
                            tracing::error!(parent = %intent.parent, "split reconcile failed: {error}");
                            state.split_ready.store(false, Ordering::Release);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(parent = %intent.parent, "split claim failed: {error}");
                        state.split_ready.store(false, Ordering::Release);
                    }
                }
            }

            if Instant::now() >= next_gc {
                match gc_abandoned_generations(&state, auto.gc_retention).await {
                    Ok(deleted) if deleted > 0 => {
                        tracing::info!(deleted, "split generation GC completed");
                    }
                    Ok(_) => {}
                    Err(error) => tracing::error!("split generation GC failed: {error}"),
                }
                next_gc = Instant::now() + auto.gc_interval;
            }

            let ceiling = auto.single_shard_write_ceiling_bytes_per_sec;
            if ceiling == 0 {
                continue;
            }
            let threshold = ceiling.saturating_mul(3).div_ceil(5).max(1);
            let topology = state.topology.read().unwrap().clone();
            let now = Instant::now();
            let counters: Vec<(String, u64)> = state
                .shards
                .read()
                .unwrap()
                .iter()
                .filter(|(prefix, _)| topology.shards.contains(*prefix))
                .map(|(prefix, engine)| {
                    (
                        prefix.clone(),
                        engine.stats_appended_bytes.load(Ordering::Relaxed),
                    )
                })
                .collect();
            hot_samples.retain(|prefix, _| topology.shards.contains(prefix));
            let mut trigger = None;
            for (prefix, total_bytes) in counters {
                let Some(previous) = hot_samples.get(&prefix).copied() else {
                    hot_samples.insert(
                        prefix,
                        HotSample {
                            total_bytes,
                            sampled_at: now,
                            above_since: None,
                        },
                    );
                    continue;
                };
                let elapsed = now.duration_since(previous.sampled_at);
                let delta = total_bytes.checked_sub(previous.total_bytes);
                let rate = delta
                    .filter(|_| !elapsed.is_zero())
                    .map(|bytes| bytes as f64 / elapsed.as_secs_f64())
                    .unwrap_or(0.0);
                let above_since = if rate >= threshold as f64 {
                    previous.above_since.or(Some(now))
                } else {
                    None
                };
                hot_samples.insert(
                    prefix.clone(),
                    HotSample {
                        total_bytes,
                        sampled_at: now,
                        above_since,
                    },
                );
                if above_since.is_some_and(|since| now.duration_since(since) >= auto.sustain)
                    && !state.splitting_prefixes.read().unwrap().contains(&prefix)
                    && is_ring_owner(&state, &prefix)
                {
                    trigger = Some((prefix, rate));
                    break;
                }
            }
            if let Some((prefix, rate)) = trigger {
                tracing::info!(
                    shard = %prefix,
                    observed_bytes_per_sec = rate,
                    threshold_bytes_per_sec = threshold,
                    "automatic sustained-load shard split triggered"
                );
                if let Err(error) = request(state.clone(), prefix.clone()).await {
                    tracing::error!(shard = %prefix, "automatic shard split failed: {error}");
                    state.split_ready.store(false, Ordering::Release);
                }
                hot_samples.remove(&prefix);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_generation_references_cover_topology_and_every_intent_path() {
        let topology = Topology {
            version: 1,
            storage_format: 2,
            shards: vec!["0".to_string(), "1".to_string()],
            shard_paths: [(
                "0".to_string(),
                "shards/splits/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0".to_string(),
            )]
            .into_iter()
            .collect(),
        };
        let intent = SplitIntent {
            version: INTENT_VERSION,
            operation_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            parent: "1".to_string(),
            parent_path: "shards/splits/cccccccccccccccccccccccccccccccc/1".to_string(),
            zero_path: "shards/splits/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/10".to_string(),
            one_path: "shards/splits/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/11".to_string(),
            created_ms: 1,
            lease_owner: "owner".to_string(),
            lease_until_ms: 2,
            abandoned_generations: vec![AbandonedGeneration {
                operation_id: "dddddddddddddddddddddddddddddddd".to_string(),
                abandoned_ms: 1,
            }],
        };
        let referenced = referenced_split_operations(&topology, &[intent]);
        assert_eq!(referenced.len(), 4);
        assert!(referenced.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(referenced.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        assert!(referenced.contains("cccccccccccccccccccccccccccccccc"));
        assert!(referenced.contains("dddddddddddddddddddddddddddddddd"));
        assert!(split_operation("shards/splits/not-hex/0").is_none());
    }
}

pub async fn request(state: Arc<AppState>, parent: String) -> Result<Topology, String> {
    if parent.len() >= 128 || !parent.bytes().all(|bit| bit == b'0' || bit == b'1') {
        return Err("parent must be a binary prefix shorter than 128 bits".to_string());
    }
    if !is_ring_owner(&state, &parent) {
        return Err("request must be sent to the current shard owner".to_string());
    }
    let Some(_worker) = acquire_worker(&state, &parent) else {
        return Err("split operation is already running".to_string());
    };
    create_intent(&state, &parent).await?;
    test_crash_after("intent_created");
    state
        .splitting_prefixes
        .write()
        .unwrap()
        .insert(parent.clone());
    let claimed = claim_intent(&state, &parent)
        .await?
        .ok_or_else(|| "split intent is leased by another instance".to_string())?;
    test_crash_after("intent_claimed");
    reconcile(state.clone(), claimed).await?;
    let topology = state.topology.read().unwrap().clone();
    Ok(topology)
}
