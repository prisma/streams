//! `GET /operator/topology.json` (S4-DESIGN §11, edge #251): the topology this instance routes by
//! (its version, and each leaf's home, owner, birth and state) and the cell's intent, whose state is
//! derived from §6.1's durable witnesses, the newest first:
//!
//! | state | witness |
//! |---|---|
//! | `Reclaimed`, `RolledBack` | the intent's own terminal state |
//! | `Published` | the stored topology past `version_seen`, its `last_op` the operation |
//! | `SourcesRetired` | the parent's `'T'` (read only where the parent is open) |
//! | `ChildrenReady` | both children's manifests initialized |
//! | `Quiesced` | the pause's `topo-<op_id>` checkpoint on the parent |
//! | `Preparing` | none of the above |
//!
//! `never_leaf_paths` (S4-DESIGN §15, the never-leaf path contract) counts the homes of the stored
//! topology below its `next_home` that were never published and that no live intent names as a
//! child: an abandoned attempt's homes, whose paths S6's sweeper deletes (null when the stored
//! topology is unreadable).

use std::sync::Arc;

use object_store::ObjectStore;
use serde_json::{Value, json};

use super::intent::{self, Intent, State};
use super::model::{Snapshot, born_home_path};
use super::{fence, recover, store};
use crate::ownership::OwnershipService;
use crate::shard_directory::ShardDirectory;

/// The route's body (see the module).
pub(crate) async fn topology(
    dir: &ShardDirectory,
    ownership: &OwnershipService,
    store: &Arc<dyn ObjectStore>,
) -> Value {
    let snapshot = dir.prefixes();
    let read = intent::read(&**store).await;
    let live = match &read {
        Ok(Some(held)) if !held.intent.is_terminal() => Some(&held.intent),
        _ => None,
    };
    let shards: Vec<Value> = snapshot
        .shards
        .leaves
        .iter()
        .map(|(prefix, home)| {
            let born = snapshot.shards.homes.get(home).map(|h| h.born_version);
            let source = live
                .is_some_and(|intent| intent.sources.iter().any(|(p, h)| p == prefix && h == home));
            json!({
                "prefix": prefix,
                "home": home,
                "owner": ownership.effective_owner(prefix),
                "born_version": born,
                "state": if source { "splitting" } else { "serving" },
            })
        })
        .collect();
    let never_leaf = never_leaf_paths(store, live).await;
    let intent = match read {
        Ok(None) => Value::Null,
        Ok(Some(held)) => {
            let state = derived(dir, store, &snapshot, &held.intent).await;
            let i = held.intent;
            json!({
                "kind": i.kind,
                "op_id": i.op_id,
                "sources": i.sources,
                "children": i.children,
                "state": state,
                "started_ms": i.started_ms,
                "lease_until_ms": i.lease_until_ms,
                "outcome": i.outcome,
            })
        }
        Err(error) => json!({"error": "intent_unreadable", "message": error.to_string()}),
    };
    json!({
        "version": snapshot.shards.version,
        "shards": shards,
        "intent": intent,
        "never_leaf_paths": never_leaf,
    })
}

/// The stored topology's never-leaf homes (see the module), with `live` the live intent if any.
async fn never_leaf_paths(store: &Arc<dyn ObjectStore>, live: Option<&Intent>) -> Option<usize> {
    let (stored, _) = store::read(&**store).await.ok()?;
    let named = |h: &u32| live.is_some_and(|intent| intent.children.contains(h));
    let never = (0..stored.shards.next_home)
        .filter(|h| !stored.shards.homes.contains_key(h) && !named(h))
        .count();
    Some(never)
}

/// The intent's state from its witnesses (see the module).
async fn derived(
    dir: &ShardDirectory,
    store: &Arc<dyn ObjectStore>,
    snapshot: &Snapshot,
    intent: &Intent,
) -> &'static str {
    match intent.state {
        State::Reclaimed => return "Reclaimed",
        State::RolledBack => return "RolledBack",
        State::Preparing => {}
    }
    if let Ok((stored, _)) = store::read(&**store).await {
        let ours = stored
            .last_op
            .as_ref()
            .is_some_and(|op| op.op_id == intent.op_id);
        if ours && stored.version > intent.version_seen {
            return "Published";
        }
    }
    let Some((prefix, home)) = intent.sources.first() else {
        return "Preparing";
    };
    if let Some(parent) = dir.open(prefix)
        && matches!(parent.db.get(fence::retired_key()).await, Ok(Some(_)))
    {
        return "SourcesRetired";
    }
    let children = recover::children_of(intent);
    let mut ready = !children.is_empty();
    for (child, at) in &children {
        let path = born_home_path(child, *at);
        let admin = slatedb::admin::AdminBuilder::new(path.as_str(), store.clone()).build();
        let initialized = admin.read_manifest(None).await;
        ready &= matches!(initialized, Ok(Some(manifest)) if manifest.initialized());
    }
    if ready {
        return "ChildrenReady";
    }
    let Some(log) = snapshot.shards.homes.get(home).map(|h| h.path.clone()) else {
        return "Preparing";
    };
    let admin = slatedb::admin::AdminBuilder::new(log.as_str(), store.clone()).build();
    let name = format!("topo-{}", intent.op_id);
    match admin.list_checkpoints(Some(&name)).await {
        Ok(taken) if taken.iter().any(|c| c.name.as_deref() == Some(&*name)) => "Quiesced",
        _ => "Preparing",
    }
}
