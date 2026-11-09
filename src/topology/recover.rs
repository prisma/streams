//! The recovery of a split whose actor stopped (S4-DESIGN §4.3, §6.4). Only an opener of the
//! parent claims the lapsed intent, after its own open fenced the actor's writer (`fence`), so no
//! `'T'` can land after the opener read the log. Then it
//!
//! - **rolls back** when the log holds no `'T'`: each child path gets the takeover GC (its clone's
//!   final checkpoints stripped from its sources, then its files), the pause's `topo-<op_id>`
//!   checkpoint is deleted, then each child's reference and `-d` lists under every root of the
//!   parent's lineage, and the intent ends `RolledBack` last, so a rollback cut short is the next
//!   claimant's to redo. A stale actor's clone that lands after it is S6's never-leaf sweeper's
//!   (N1): its home is below `next_home`, was never published, and no live intent names it;
//! - **rolls forward** from `'T'`: the topology `'T'` names is published (a CAS idempotent on
//!   `last_op`), the pause's checkpoint is deleted, and a claimed intent ends `Reclaimed`.
//!
//! The split's own rollback runs the same deletions.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::fence::Retired;
use super::intent::{self, Held, Intent, IntentError, State};
use super::model::{ShardSet, born_home_path};
use super::store::{self, WriteError};
use crate::shard::ShardEngine;
use crate::spill::lineage;

/// How many times a roll forward re-reads a topology that moved under its CAS.
const PUBLISH_TRIES: usize = 3;

/// Claim `held`, lapsed, for the opener `owner`: the lease's owner and its next epoch, by CAS. A
/// claimant that lost the race gets `Lost`.
pub(crate) async fn claim(
    store: &dyn ObjectStore,
    held: &Held,
    owner: &str,
) -> Result<Held, IntentError> {
    let mut next = held.intent.clone();
    next.lease_owner = owner.to_string();
    next.lease_epoch = next.lease_epoch.saturating_add(1);
    intent::replace(store, held, next).await
}

/// `held`'s intent, ended in `state` with `result`.
pub(crate) fn terminal(held: &Held, state: State, result: &str) -> Intent {
    let mut ended = held.intent.clone();
    ended.state = state;
    ended.outcome = Some(intent::Outcome {
        result: result.to_string(),
        reason: result.to_string(),
    });
    ended
}

/// `home` and every home it descends from in `shards`, each once, nearest first: the roots a
/// child of `home` may hold objects under.
pub(crate) fn lineage_of(shards: &ShardSet, home: u32) -> Vec<u32> {
    let mut roots = vec![home];
    let mut at = 0;
    while let Some(&h) = roots.get(at) {
        let parents = shards.homes.get(&h).map(|home| home.parents.clone());
        for parent in parents.unwrap_or_default() {
            if !roots.contains(&parent) {
                roots.push(parent);
            }
        }
        at += 1;
    }
    roots
}

/// The children of the split `intent` names: each half of its source's prefix and its home.
pub(crate) fn children_of(intent: &Intent) -> Vec<(String, u32)> {
    let Some((prefix, _)) = intent.sources.first() else {
        return Vec::new();
    };
    ["0", "1"]
        .into_iter()
        .zip(&intent.children)
        .map(|(half, home)| (format!("{prefix}{half}"), *home))
        .collect()
}

/// Roll back the split `held` names, before `'T'` (see the module): `roots` are the parent's
/// lineage (`lineage_of`). The intent's terminal CAS is the result; the deletions before it are
/// best effort, each left to the never-leaf sweeper or the checkpoint's lifetime if it fails.
pub(crate) async fn roll_back(
    parent: &ShardEngine,
    roots: &[u32],
    held: &Held,
    why: &str,
) -> Result<Held, IntentError> {
    clear(parent, roots, &held.intent).await;
    let ended = terminal(held, State::RolledBack, why);
    intent::replace(&**parent.data_store(), held, ended).await
}

/// The deletions of a rollback of the split `intent` names (see the module), before its terminal
/// CAS: each child path's takeover GC, the pause's checkpoint, then the lists under `roots`.
pub(crate) async fn clear(parent: &ShardEngine, roots: &[u32], intent: &Intent) {
    let store = parent.data_store();
    let children = children_of(intent);
    for (prefix, home) in &children {
        delete_child(store, &born_home_path(prefix, *home)).await;
    }
    drop_checkpoint(parent, &intent.op_id).await;
    for (_, home) in &children {
        delete_lists(&**store, parent, *home, roots).await;
    }
}

/// Roll forward from `retired`, the parent's `'T'` (see the module); `claimed` is the intent this
/// opener claimed, if a live one named the parent.
pub(crate) async fn roll_forward(
    parent: &ShardEngine,
    retired: &Retired,
    claimed: Option<&Held>,
) -> Result<(), String> {
    let store = parent.data_store();
    publish(&**store, &parent.prefix, retired).await?;
    drop_checkpoint(parent, &retired.op_id).await;
    if let Some(held) = claimed.filter(|held| held.intent.op_id == retired.op_id) {
        let ended = terminal(held, State::Reclaimed, "published");
        intent::replace(&**store, held, ended)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Publish the split of `prefix` that `retired` names (S4-DESIGN §6.2 step 4.1, §6.4): done when
/// the stored topology is at or past `retired.version` (one intent per cell, and `'T'` precedes
/// its publish, so that version is this operation's), else the split CASed over the version
/// before it, re-read when it moved. Any other version is stuck, for an operator.
async fn publish(store: &dyn ObjectStore, prefix: &str, retired: &Retired) -> Result<(), String> {
    let half = |h: &str| {
        let child = format!("{prefix}{h}");
        let named = retired.children.iter().find(|(p, _)| *p == child);
        named.map(|(_, home)| *home)
    };
    let (Some(low), Some(high)) = (half("0"), half("1")) else {
        return Err(format!("'T' names no children of {prefix:?}"));
    };
    for _ in 0..PUBLISH_TRIES {
        let (topology, etag) = store::read(store).await.map_err(|e| format!("{e:?}"))?;
        if topology.version >= retired.version {
            return Ok(());
        }
        if topology.version.saturating_add(1) != retired.version {
            return Err(format!(
                "stuck: the topology is at {} and 'T' at {}",
                topology.version, retired.version
            ));
        }
        let next = topology.split(prefix, [low, high], &retired.op_id)?;
        match store::replace(store, &etag, &next).await {
            Ok(_) => return Ok(()),
            Err(WriteError::Moved) => {}
            Err(WriteError::Store(e)) => return Err(e.to_string()),
        }
    }
    Err(format!("the topology moved {PUBLISH_TRIES} times"))
}

/// The takeover GC of a child's `path` (S4-DESIGN §6.4): its clone's final checkpoints stripped
/// from its sources, then its files (SlateDB's `delete_db`). A path whose manifest does not decode,
/// or that holds files but no manifest, has the files it listed deleted one by one; one that holds
/// none is left untouched.
pub(crate) async fn delete_child(store: &Arc<dyn ObjectStore>, path: &str) {
    use futures_util::TryStreamExt;
    let prefix = object_store::path::Path::from(path);
    let listed: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .unwrap_or_default();
    if listed.is_empty() {
        return;
    }
    let admin = slatedb::admin::AdminBuilder::new(path, store.clone()).build();
    if admin.delete_db(true).await.is_ok() {
        return;
    }
    for meta in listed {
        drop(store.delete(&meta.location).await);
    }
}

/// Delete the pause's checkpoint of operation `op_id` from the parent, if it was taken: the children
/// were cloned from it (or never will be). A failed delete is left to the checkpoint's lifetime.
pub(crate) async fn drop_checkpoint(parent: &ShardEngine, op_id: &str) {
    let path = parent.log_path();
    let admin =
        slatedb::admin::AdminBuilder::new(path.as_str(), parent.data_store().clone()).build();
    let name = format!("topo-{op_id}");
    let Ok(taken) = admin.list_checkpoints(Some(&name)).await else {
        return;
    };
    for checkpoint in taken
        .into_iter()
        .filter(|c| c.name.as_deref() == Some(&*name))
    {
        drop(admin.delete_checkpoint(checkpoint.id).await);
    }
}

/// Delete `holder`'s reference and supplementary lists under each of the `roots`.
pub(crate) async fn delete_lists(
    store: &dyn ObjectStore,
    parent: &ShardEngine,
    holder: u32,
    roots: &[u32],
) {
    for root in roots {
        let root = parent.object_root(*root);
        for path in [
            lineage::reference_list_path(&root, holder),
            lineage::supplementary_list_path(&root, holder),
        ] {
            drop(store.delete(&object_store::path::Path::from(path)).await);
        }
    }
}

// The merge's claimant (S6 §7.4), dispatched from the fence for a merge's intent.
pub(crate) mod merge;
#[cfg(test)]
mod tests;
