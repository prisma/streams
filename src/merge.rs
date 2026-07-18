//! Crash-recoverable online sibling merge actor.
//!
//! A parent-scoped intent coordinates the operation. Before either child is
//! quiesced, the actor CAS-creates a fence in the same per-shard namespace
//! used by split intents. Every writer checks that namespace after remote
//! durability and before ACK, so a stale child owner can add at most
//! unacknowledged data after the merge snapshot. SlateDB then creates a
//! manifest-union clone and one topology CAS makes the parent visible.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use slatedb::admin::AdminBuilder;
use slatedb::{CloneSourceSpec, DbReader};

use crate::http::AppState;
use crate::reconfiguration::{
    FenceDocument, decode_fence, fence_path, merge_fence, released_fence, validate_merge_fence,
};
use crate::registry::{
    Topology, cas_publish_topology_merge_with_path, load_topology, shard_projection_bounds,
};
use crate::shard::now_ms;
use crate::split::{install_topology, is_ring_owner};

const INTENT_VERSION: u32 = 1;
const LEASE_MS: i64 = 12_000;
const LEASE_RENEW_MS: u64 = 3_000;
const MAX_INTENTS: usize = 768;

#[derive(Clone, Copy, Debug)]
pub struct MergeConfig {
    pub gc_retention: Duration,
    pub gc_interval: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MergeIntent {
    version: u32,
    #[serde(default = "active_status")]
    status: String,
    operation_id: String,
    parent: String,
    zero: String,
    one: String,
    zero_path: String,
    one_path: String,
    target_path: String,
    created_ms: i64,
    lease_owner: String,
    lease_until_ms: i64,
    #[serde(default)]
    abandoned_generations: Vec<AbandonedGeneration>,
}

fn active_status() -> String {
    "active".to_string()
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

fn test_crash_after(phase: &str) {
    if std::env::var("STREAMS_TEST_MERGE_CRASH_AFTER").as_deref() == Ok(phase) {
        tracing::error!(phase, "test crash after durable merge transition");
        std::process::abort();
    }
}

fn parent_name(parent: &str) -> &str {
    if parent.is_empty() { "root" } else { parent }
}

fn intent_path(parent: &str) -> ObjPath {
    ObjPath::from(format!("merge-intents/{}.json", parent_name(parent)))
}

fn gc_candidate_path(operation_id: &str) -> ObjPath {
    ObjPath::from(format!("merge-gc-candidates/{operation_id}.json"))
}

fn target_path(operation_id: &str, parent: &str) -> String {
    format!("shards/merges/{operation_id}/{}", parent_name(parent))
}

fn new_operation(parent: &str) -> (String, String) {
    let mut operation = [0u8; 16];
    use rand::RngCore;
    rand::rng().fill_bytes(&mut operation);
    let operation_id = crate::crypto::hex(&operation);
    let path = target_path(&operation_id, parent);
    (operation_id, path)
}

fn valid_operation(operation: &str) -> bool {
    operation.len() == 32 && operation.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_db_path(path: &str) -> bool {
    path.len() <= 512
        && path.starts_with("shards/")
        && !path.contains("//")
        && !path.split('/').any(|component| component == "..")
        && ObjPath::parse(path).is_ok()
}

fn validate_intent(intent: &MergeIntent) -> Result<(), String> {
    if intent.version != INTENT_VERSION
        || (intent.status != "active" && intent.status != "released")
        || !valid_operation(&intent.operation_id)
        || intent.parent.len() >= 128
        || !intent.parent.bytes().all(|bit| bit == b'0' || bit == b'1')
        || intent.zero != format!("{}0", intent.parent)
        || intent.one != format!("{}1", intent.parent)
        || !valid_db_path(&intent.zero_path)
        || !valid_db_path(&intent.one_path)
        || intent.target_path != target_path(&intent.operation_id, &intent.parent)
        || intent.created_ms <= 0
        || intent.lease_owner.len() > 256
        || intent.abandoned_generations.len() > 64
        || intent.abandoned_generations.iter().any(|generation| {
            !valid_operation(&generation.operation_id) || generation.abandoned_ms <= 0
        })
    {
        return Err("malformed merge intent".to_string());
    }
    Ok(())
}

async fn read_intent(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
) -> Result<Option<(MergeIntent, Option<String>)>, String> {
    match store.get(&intent_path(parent)).await {
        Ok(result) => {
            let etag = result.meta.e_tag.clone();
            let raw = result.bytes().await.map_err(|error| error.to_string())?;
            let intent: MergeIntent =
                serde_json::from_slice(&raw).map_err(|_| "malformed merge intent".to_string())?;
            validate_intent(&intent)?;
            Ok(Some((intent, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn topology_has_children(topology: &Topology, intent: &MergeIntent) -> bool {
    topology.shards.contains(&intent.zero) && topology.shards.contains(&intent.one)
}

fn topology_has_requested_children(topology: &Topology, parent: &str) -> bool {
    topology.shards.contains(&format!("{parent}0"))
        && topology.shards.contains(&format!("{parent}1"))
        && !topology.shards.contains(&parent.to_string())
}

async fn create_intent(state: &AppState, parent: &str) -> Result<MergeIntent, String> {
    let topology = load_topology(&state.ops_store)
        .await
        .map_err(|error| error.to_string())?;
    if !topology_has_requested_children(&topology, parent) {
        return Err("merge requires two live sibling shards".to_string());
    }
    let zero = format!("{parent}0");
    let one = format!("{parent}1");
    let (operation_id, target_path) = new_operation(parent);
    let intent = MergeIntent {
        version: INTENT_VERSION,
        status: active_status(),
        operation_id,
        parent: parent.to_string(),
        zero_path: topology.db_path(&zero),
        one_path: topology.db_path(&one),
        zero,
        one,
        target_path,
        created_ms: now_ms(),
        lease_owner: state.instance_name.clone(),
        lease_until_ms: now_ms() + LEASE_MS,
        abandoned_generations: Vec::new(),
    };
    validate_intent(&intent)?;
    let path = intent_path(parent);
    for _ in 0..5 {
        match state
            .shard_store
            .put_opts(
                &path,
                PutPayload::from(serde_json::to_vec(&intent).expect("merge intent json")),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => return Ok(intent),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let Some((current, etag)) = read_intent(&state.shard_store, parent).await? else {
                    continue;
                };
                if current.status == "active" {
                    return Ok(current);
                }
                match state
                    .shard_store
                    .put_opts(
                        &path,
                        PutPayload::from(serde_json::to_vec(&intent).expect("merge intent json")),
                        PutOptions::from(PutMode::Update(UpdateVersion {
                            e_tag: etag,
                            version: None,
                        })),
                    )
                    .await
                {
                    Ok(_) => return Ok(intent),
                    Err(object_store::Error::Precondition { .. }) => continue,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("merge intent tombstone CAS retries exhausted".to_string())
}

async fn persist_gc_candidates(state: &AppState, intent: &MergeIntent) -> Result<(), String> {
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
                PutPayload::from(serde_json::to_vec(&candidate).expect("merge GC candidate json")),
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

async fn claim_intent(state: &AppState, parent: &str) -> Result<Option<MergeIntent>, String> {
    for _ in 0..5 {
        let Some((mut intent, etag)) = read_intent(&state.shard_store, parent).await? else {
            return Ok(None);
        };
        if intent.status == "released" {
            return Ok(None);
        }
        if intent.lease_owner != state.instance_name {
            if intent.lease_until_ms > now_ms() {
                return Ok(None);
            }
            let topology = load_topology(&state.ops_store)
                .await
                .map_err(|error| error.to_string())?;
            if topology_has_children(&topology, &intent) {
                if intent.abandoned_generations.len() >= 64 {
                    return Err("merge takeover generation bound exceeded".to_string());
                }
                intent.abandoned_generations.push(AbandonedGeneration {
                    operation_id: intent.operation_id.clone(),
                    abandoned_ms: now_ms(),
                });
                let (operation_id, path) = new_operation(parent);
                intent.operation_id = operation_id;
                intent.target_path = path;
            }
        }
        intent.lease_owner = state.instance_name.clone();
        intent.lease_until_ms = now_ms() + LEASE_MS;
        match state
            .shard_store
            .put_opts(
                &intent_path(parent),
                PutPayload::from(serde_json::to_vec(&intent).expect("merge intent json")),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: etag,
                    version: None,
                })),
            )
            .await
        {
            Ok(_) => {
                persist_gc_candidates(state, &intent).await?;
                return Ok(Some(intent));
            }
            Err(object_store::Error::Precondition { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("merge intent lease CAS retries exhausted".to_string())
}

async fn renew_intent(state: &AppState, parent: &str, operation: &str) -> Result<(), String> {
    for _ in 0..5 {
        let Some((mut intent, etag)) = read_intent(&state.shard_store, parent).await? else {
            return Err("merge intent disappeared while leased".to_string());
        };
        if intent.status != "active"
            || intent.operation_id != operation
            || intent.lease_owner != state.instance_name
        {
            return Err("merge intent lease was lost".to_string());
        }
        intent.lease_until_ms = now_ms() + LEASE_MS;
        match state
            .shard_store
            .put_opts(
                &intent_path(parent),
                PutPayload::from(serde_json::to_vec(&intent).expect("merge intent json")),
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
    Err("merge intent renewal CAS retries exhausted".to_string())
}

async fn verify_intent_owner(state: &AppState, intent: &MergeIntent) -> Result<(), String> {
    let Some((current, _)) = read_intent(&state.shard_store, &intent.parent).await? else {
        return Err("merge intent disappeared before topology publish".to_string());
    };
    if current.status != "active"
        || current.operation_id != intent.operation_id
        || current.lease_owner != state.instance_name
        || current.lease_until_ms <= now_ms()
    {
        return Err("merge intent is not owned before topology publish".to_string());
    }
    Ok(())
}

async fn release_intent(state: &AppState, intent: &MergeIntent) -> Result<(), String> {
    for _ in 0..5 {
        let Some((mut current, etag)) = read_intent(&state.shard_store, &intent.parent).await?
        else {
            // Backward-compatible recovery for an operation completed by a
            // pre-tombstone binary.
            return Ok(());
        };
        if current.status == "released" && current.operation_id == intent.operation_id {
            return Ok(());
        }
        if current.status != "active"
            || current.operation_id != intent.operation_id
            || current.lease_owner != state.instance_name
            || current.lease_until_ms <= now_ms()
        {
            return Err("merge intent ownership changed before release".to_string());
        }
        current.status = "released".to_string();
        current.lease_until_ms = 0;
        match state
            .shard_store
            .put_opts(
                &intent_path(&intent.parent),
                PutPayload::from(serde_json::to_vec(&current).expect("merge intent json")),
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
    Err("merge intent release CAS retries exhausted".to_string())
}

fn operation_is_known(intent: &MergeIntent, operation: &str) -> bool {
    intent.operation_id.eq_ignore_ascii_case(operation)
        || intent
            .abandoned_generations
            .iter()
            .any(|generation| generation.operation_id.eq_ignore_ascii_case(operation))
}

async fn ensure_fence(state: &AppState, intent: &MergeIntent, child: &str) -> Result<(), String> {
    let path = fence_path(child);
    for _ in 0..8 {
        match state.shard_store.get(&path).await {
            Ok(result) => {
                let etag = result.meta.e_tag.clone();
                let raw = result.bytes().await.map_err(|error| error.to_string())?;
                let existing = match decode_fence(&raw)? {
                    FenceDocument::Split => {
                        return Err(format!("child {child} is being split"));
                    }
                    FenceDocument::Merge(existing) => existing,
                    FenceDocument::ReleasedSplit(existing) => {
                        if existing.parent != child {
                            return Err(
                                "released split fence path does not match its parent".to_string()
                            );
                        }
                        let replacement = merge_fence(&intent.operation_id, &intent.parent, child);
                        match state
                            .shard_store
                            .put_opts(
                                &path,
                                PutPayload::from(
                                    serde_json::to_vec(&replacement).expect("merge fence json"),
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
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                    FenceDocument::Released(existing) => {
                        if existing.child != child {
                            return Err("released fence path does not match its child".to_string());
                        }
                        let replacement = merge_fence(&intent.operation_id, &intent.parent, child);
                        match state
                            .shard_store
                            .put_opts(
                                &path,
                                PutPayload::from(
                                    serde_json::to_vec(&replacement).expect("merge fence json"),
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
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                };
                if existing.child != child || existing.parent != intent.parent {
                    return Err(format!("child {child} has another merge fence"));
                }
                if existing.operation_id == intent.operation_id {
                    return Ok(());
                }
                if !operation_is_known(intent, &existing.operation_id) {
                    return Err(format!("child {child} has an unrelated merge fence"));
                }
                let replacement = merge_fence(&intent.operation_id, &intent.parent, child);
                match state
                    .shard_store
                    .put_opts(
                        &path,
                        PutPayload::from(
                            serde_json::to_vec(&replacement).expect("merge fence json"),
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
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                let fence = merge_fence(&intent.operation_id, &intent.parent, child);
                validate_merge_fence(&fence)?;
                match state
                    .shard_store
                    .put_opts(
                        &path,
                        PutPayload::from(serde_json::to_vec(&fence).expect("merge fence json")),
                        PutOptions::from(PutMode::Create),
                    )
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(object_store::Error::AlreadyExists { .. }) => continue,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Err(format!(
        "merge fence CAS retries exhausted for child {child}"
    ))
}

async fn release_fence(state: &AppState, intent: &MergeIntent, child: &str) -> Result<(), String> {
    release_fence_inner(state, intent, child, true).await
}

async fn release_owned_fence(
    state: &AppState,
    intent: &MergeIntent,
    child: &str,
) -> Result<(), String> {
    release_fence_inner(state, intent, child, false).await
}

async fn release_fence_inner(
    state: &AppState,
    intent: &MergeIntent,
    child: &str,
    strict: bool,
) -> Result<(), String> {
    let path = fence_path(child);
    for _ in 0..5 {
        match state.shard_store.get(&path).await {
            Ok(result) => {
                let etag = result.meta.e_tag.clone();
                let raw = result.bytes().await.map_err(|error| error.to_string())?;
                let fence = match decode_fence(&raw)? {
                    FenceDocument::Merge(fence) => fence,
                    FenceDocument::Released(released) => {
                        if released.operation_id == intent.operation_id
                            && released.parent == intent.parent
                            && released.child == child
                        {
                            return Ok(());
                        }
                        if strict {
                            return Err(format!(
                                "refusing to release unrelated tombstone for child {child}"
                            ));
                        }
                        return Ok(());
                    }
                    FenceDocument::Split => {
                        if strict {
                            return Err(format!(
                                "refusing to release split fence for child {child}"
                            ));
                        }
                        return Ok(());
                    }
                    FenceDocument::ReleasedSplit(_) => {
                        if strict {
                            return Err(format!(
                                "refusing to release split tombstone for child {child}"
                            ));
                        }
                        return Ok(());
                    }
                };
                if fence.operation_id != intent.operation_id
                    || fence.parent != intent.parent
                    || fence.child != child
                {
                    if strict {
                        return Err(format!(
                            "refusing to release unrelated fence for child {child}"
                        ));
                    }
                    return Ok(());
                }
                let released = released_fence(&fence, now_ms());
                match state
                    .shard_store
                    .put_opts(
                        &path,
                        PutPayload::from(
                            serde_json::to_vec(&released).expect("released fence json"),
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
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        }
    }
    Err(format!(
        "fence release CAS retries exhausted for child {child}"
    ))
}

async fn abort_unpublished(state: &AppState, intent: &MergeIntent) -> Result<(), String> {
    verify_intent_owner(state, intent).await?;
    release_owned_fence(state, intent, &intent.zero).await?;
    release_owned_fence(state, intent, &intent.one).await?;
    verify_intent_owner(state, intent).await?;
    release_intent(state, intent).await?;
    let mut blocked = state.splitting_prefixes.write().unwrap();
    blocked.remove(&intent.zero);
    blocked.remove(&intent.one);
    Ok(())
}

async fn path_has_objects(store: &Arc<dyn ObjectStore>, path: &str) -> Result<bool, String> {
    match store.list(Some(&ObjPath::from(path))).next().await {
        Some(Ok(_)) => Ok(true),
        Some(Err(error)) => Err(error.to_string()),
        None => Ok(false),
    }
}

async fn db_is_valid(store: &Arc<dyn ObjectStore>, path: &str) -> bool {
    match DbReader::builder(path, store.clone()).build().await {
        Ok(reader) => {
            reader.close().await.ok();
            true
        }
        Err(_) => false,
    }
}

async fn open_and_quiesce_child(
    state: &Arc<AppState>,
    child: &str,
    path: &str,
) -> Result<(), String> {
    let cached = state.shards.read().unwrap().get(child).cloned();
    let engine = if let Some(engine) = cached.filter(|engine| !engine.is_closed()) {
        engine
    } else {
        state.shards.write().unwrap().remove(child);
        let engine = (state.opener.open)(child.to_string(), path.to_string())
            .await
            .map_err(|error| error.to_string())?;
        state
            .shards
            .write()
            .unwrap()
            .insert(child.to_string(), engine.clone());
        engine
    };
    engine.quiesce_for_union().await
}

async fn ensure_target(state: &AppState, intent: &MergeIntent) -> Result<(), String> {
    if path_has_objects(&state.shard_store, &intent.target_path).await?
        && db_is_valid(&state.shard_store, &intent.target_path).await
    {
        return Ok(());
    }
    let mut zero = CloneSourceSpec::new(intent.zero_path.as_str());
    zero.projection_range =
        Some(shard_projection_bounds(&intent.zero).map_err(|error| error.to_string())?);
    let mut one = CloneSourceSpec::new(intent.one_path.as_str());
    one.projection_range =
        Some(shard_projection_bounds(&intent.one).map_err(|error| error.to_string())?);
    AdminBuilder::new(intent.target_path.as_str(), state.shard_store.clone())
        .build()
        .create_clone_builder_from_source(zero)
        .with_source(one)
        .build()
        .await
        .map_err(|error| error.to_string())?;
    if !db_is_valid(&state.shard_store, &intent.target_path).await {
        return Err("merged union clone failed reopen verification".to_string());
    }
    Ok(())
}

fn merge_operation(path: &str) -> Option<&str> {
    let operation = path.strip_prefix("shards/merges/")?.split('/').next()?;
    valid_operation(operation).then_some(operation)
}

async fn published_parent_is_valid(
    state: &AppState,
    topology: &Topology,
    intent: &MergeIntent,
) -> bool {
    if !topology.shards.contains(&intent.parent)
        || topology.shards.contains(&intent.zero)
        || topology.shards.contains(&intent.one)
    {
        return false;
    }
    let path = topology.db_path(&intent.parent);
    merge_operation(&path).is_some_and(|operation| operation_is_known(intent, operation))
        && db_is_valid(&state.shard_store, &path).await
}

async fn finish_published(
    state: &AppState,
    intent: &MergeIntent,
    topology: Topology,
) -> Result<(), String> {
    install_topology(state, topology);
    // The parent prefix was retired by the earlier split and may still have
    // an anti-flap timestamp. This topology version points it at a verified
    // new generation, so the old-path holdoff must not create a 3 s outage.
    state.open_lock.lock().await.remove(&intent.parent);
    verify_intent_owner(state, intent).await?;
    release_fence(state, intent, &intent.zero).await?;
    release_fence(state, intent, &intent.one).await?;
    release_intent(state, intent).await?;
    test_crash_after("intent_deleted");
    let mut blocked = state.splitting_prefixes.write().unwrap();
    blocked.remove(&intent.zero);
    blocked.remove(&intent.one);
    Ok(())
}

async fn reconcile_inner(
    state: Arc<AppState>,
    intent: MergeIntent,
    lease_lost: Arc<AtomicBool>,
) -> Result<(), String> {
    let current = load_topology(&state.ops_store)
        .await
        .map_err(|error| error.to_string())?;
    if !topology_has_children(&current, &intent) {
        if published_parent_is_valid(&state, &current, &intent).await {
            return finish_published(&state, &intent, current).await;
        }
        abort_unpublished(&state, &intent).await?;
        return Err("merge aborted because its sibling topology changed".to_string());
    }

    if let Err(error) = ensure_fence(&state, &intent, &intent.zero).await {
        if error.contains("being split") || error.contains("another merge fence") {
            abort_unpublished(&state, &intent).await?;
            return Err(format!("merge aborted: {error}"));
        }
        return Err(error);
    }
    if let Err(error) = ensure_fence(&state, &intent, &intent.one).await {
        if error.contains("being split") || error.contains("another merge fence") {
            abort_unpublished(&state, &intent).await?;
            return Err(format!("merge aborted: {error}"));
        }
        return Err(error);
    }
    test_crash_after("fences_ready");
    {
        let mut blocked = state.splitting_prefixes.write().unwrap();
        blocked.insert(intent.zero.clone());
        blocked.insert(intent.one.clone());
    }
    verify_intent_owner(&state, &intent).await?;
    open_and_quiesce_child(&state, &intent.zero, &intent.zero_path).await?;
    test_crash_after("zero_quiesced");
    if lease_lost.load(Ordering::Acquire) {
        return Err("merge intent lease renewal failed".to_string());
    }
    renew_intent(&state, &intent.parent, &intent.operation_id).await?;
    open_and_quiesce_child(&state, &intent.one, &intent.one_path).await?;
    test_crash_after("one_quiesced");
    if lease_lost.load(Ordering::Acquire) {
        return Err("merge intent lease renewal failed".to_string());
    }
    renew_intent(&state, &intent.parent, &intent.operation_id).await?;
    ensure_target(&state, &intent).await?;
    test_crash_after("target_ready");
    if lease_lost.load(Ordering::Acquire) {
        return Err("merge intent lease renewal failed".to_string());
    }
    verify_intent_owner(&state, &intent).await?;

    let mut published = None;
    for _ in 0..5 {
        let topology = load_topology(&state.ops_store)
            .await
            .map_err(|error| error.to_string())?;
        if !topology_has_children(&topology, &intent) {
            if published_parent_is_valid(&state, &topology, &intent).await {
                published = Some(topology);
                break;
            }
            return Err("merge children disappeared before publish".to_string());
        }
        renew_intent(&state, &intent.parent, &intent.operation_id).await?;
        verify_intent_owner(&state, &intent).await?;
        match cas_publish_topology_merge_with_path(
            &state.ops_store,
            &intent.parent,
            topology.version,
            &intent.target_path,
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
    let topology = published.ok_or_else(|| "topology merge CAS retries exhausted".to_string())?;
    test_crash_after("topology_published");
    finish_published(&state, &intent, topology).await
}

struct LeaseRenewer(tokio::task::JoinHandle<()>);

impl Drop for LeaseRenewer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn reconcile(state: Arc<AppState>, intent: MergeIntent) -> Result<(), String> {
    let lease_lost = Arc::new(AtomicBool::new(false));
    let renew_state = state.clone();
    let renew_parent = intent.parent.clone();
    let renew_operation = intent.operation_id.clone();
    let renew_lost = lease_lost.clone();
    let _renewer = LeaseRenewer(tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(LEASE_RENEW_MS)).await;
            if let Err(error) = renew_intent(&renew_state, &renew_parent, &renew_operation).await {
                tracing::warn!(parent = %renew_parent, "merge lease renewal stopped: {error}");
                renew_lost.store(true, Ordering::Release);
                break;
            }
        }
    }));
    reconcile_inner(state, intent, lease_lost).await
}

async fn list_intents(store: &Arc<dyn ObjectStore>) -> Result<Vec<MergeIntent>, String> {
    let mut list = store.list(Some(&ObjPath::from("merge-intents")));
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
        let intent: MergeIntent =
            serde_json::from_slice(&raw).map_err(|_| "malformed merge intent".to_string())?;
        validate_intent(&intent)?;
        if meta.location != intent_path(&intent.parent) {
            return Err("merge intent path does not match its parent".to_string());
        }
        if intent.status == "released" {
            continue;
        }
        intents.push(intent);
        if intents.len() > MAX_INTENTS {
            return Err("merge intent count exceeds topology bound".to_string());
        }
    }
    Ok(intents)
}

fn referenced_merge_operations(topology: &Topology, intents: &[MergeIntent]) -> HashSet<String> {
    let mut referenced = HashSet::new();
    for path in topology
        .shard_paths
        .values()
        .map(String::as_str)
        .chain(intents.iter().map(|intent| intent.target_path.as_str()))
    {
        if let Some(operation) = merge_operation(path) {
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
    let referenced = referenced_merge_operations(&topology, &intents);
    let cutoff_ms =
        now_ms().saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX));
    let mut candidates = Vec::new();
    let mut list = state
        .shard_store
        .list(Some(&ObjPath::from("merge-gc-candidates")));
    while let Some(item) = list.next().await {
        let meta = item.map_err(|error| error.to_string())?;
        if candidates.len() >= MAX_OBJECTS_PER_RUN {
            return Err("merge GC candidate bound exceeded".to_string());
        }
        let result = state
            .shard_store
            .get(&meta.location)
            .await
            .map_err(|error| error.to_string())?;
        let raw = result.bytes().await.map_err(|error| error.to_string())?;
        let candidate: GcCandidate =
            serde_json::from_slice(&raw).map_err(|_| "malformed merge GC candidate".to_string())?;
        if candidate.version != 1
            || !valid_operation(&candidate.operation_id)
            || candidate.abandoned_ms <= 0
            || meta.location != gc_candidate_path(&candidate.operation_id)
        {
            return Err("malformed merge GC candidate".to_string());
        }
        candidates.push((candidate, meta.location));
    }

    let mut listed = 0usize;
    let mut deleted = 0usize;
    for (candidate, marker) in candidates {
        let operation = candidate.operation_id.to_ascii_lowercase();
        if candidate.abandoned_ms > cutoff_ms || referenced.contains(&operation) {
            continue;
        }
        let topology = load_topology(&state.ops_store)
            .await
            .map_err(|error| error.to_string())?;
        let intents = list_intents(&state.shard_store).await?;
        if referenced_merge_operations(&topology, &intents).contains(&operation) {
            continue;
        }
        let mut objects = Vec::new();
        let mut generation = state
            .shard_store
            .list(Some(&ObjPath::from(format!("shards/merges/{operation}"))));
        while let Some(item) = generation.next().await {
            listed = listed.saturating_add(1);
            if listed > MAX_OBJECTS_PER_RUN {
                return Err("merge generation GC object bound exceeded".to_string());
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
            .delete(&marker)
            .await
            .map_err(|error| error.to_string())?;
        tracing::info!(operation, "deleted abandoned merge generation");
    }
    Ok(deleted)
}

struct WorkerGuard {
    state: Arc<AppState>,
    key: String,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.state.split_workers.lock().unwrap().remove(&self.key);
    }
}

fn acquire_worker(state: &Arc<AppState>, parent: &str) -> Option<WorkerGuard> {
    let key = format!("merge:{parent}");
    if !state.split_workers.lock().unwrap().insert(key.clone()) {
        return None;
    }
    Some(WorkerGuard {
        state: state.clone(),
        key,
    })
}

pub async fn initialize(state: &Arc<AppState>) -> Result<(), String> {
    list_intents(&state.shard_store).await.map(|_| ())
}

pub fn start(state: Arc<AppState>, config: MergeConfig) {
    tokio::spawn(async move {
        let mut next_gc = std::time::Instant::now() + config.gc_interval;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let intents = match list_intents(&state.shard_store).await {
                Ok(intents) => intents,
                Err(error) => {
                    tracing::error!("merge intent scan failed: {error}");
                    state.merge_ready.store(false, Ordering::Release);
                    continue;
                }
            };
            state.merge_ready.store(true, Ordering::Release);
            for intent in intents {
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
                            if error.starts_with("merge aborted") {
                                tracing::warn!(parent = %intent.parent, "{error}");
                            } else {
                                tracing::error!(parent = %intent.parent, "merge reconcile failed: {error}");
                                state.merge_ready.store(false, Ordering::Release);
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(parent = %intent.parent, "merge claim failed: {error}");
                        state.merge_ready.store(false, Ordering::Release);
                    }
                }
            }
            if std::time::Instant::now() >= next_gc {
                match gc_abandoned_generations(&state, config.gc_retention).await {
                    Ok(deleted) if deleted > 0 => {
                        tracing::info!(deleted, "merge generation GC completed")
                    }
                    Ok(_) => {}
                    Err(error) => tracing::error!("merge generation GC failed: {error}"),
                }
                next_gc = std::time::Instant::now() + config.gc_interval;
            }
        }
    });
}

pub async fn request(state: Arc<AppState>, parent: String) -> Result<Topology, String> {
    if parent.len() >= 128 || !parent.bytes().all(|bit| bit == b'0' || bit == b'1') {
        return Err("parent must be a binary prefix shorter than 128 bits".to_string());
    }
    if !is_ring_owner(&state, &parent) {
        return Err("request must be sent to the current merge coordinator".to_string());
    }
    let Some(_worker) = acquire_worker(&state, &parent) else {
        return Err("merge operation is already running".to_string());
    };
    create_intent(&state, &parent).await?;
    test_crash_after("intent_created");
    let claimed = claim_intent(&state, &parent)
        .await?
        .ok_or_else(|| "merge intent is leased by another instance".to_string())?;
    reconcile(state.clone(), claimed).await?;
    Ok(state.topology.read().unwrap().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_generation_references_cover_topology_and_intents() {
        let topology = Topology {
            version: 2,
            storage_format: 2,
            shards: vec![String::new()],
            shard_paths: [(
                String::new(),
                "shards/merges/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/root".to_string(),
            )]
            .into_iter()
            .collect(),
        };
        let intent = MergeIntent {
            version: INTENT_VERSION,
            status: active_status(),
            operation_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            parent: String::new(),
            zero: "0".to_string(),
            one: "1".to_string(),
            zero_path: "shards/0".to_string(),
            one_path: "shards/1".to_string(),
            target_path: "shards/merges/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/root".to_string(),
            created_ms: 1,
            lease_owner: "owner".to_string(),
            lease_until_ms: 2,
            abandoned_generations: vec![AbandonedGeneration {
                operation_id: "cccccccccccccccccccccccccccccccc".to_string(),
                abandoned_ms: 1,
            }],
        };
        let referenced = referenced_merge_operations(&topology, &[intent]);
        assert_eq!(referenced.len(), 3);
        assert!(referenced.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(referenced.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        assert!(referenced.contains("cccccccccccccccccccccccccccccccc"));
    }
}
