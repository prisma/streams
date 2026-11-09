//! The split (S4-DESIGN §6): one shard log becomes two, each a projected clone of the parent's
//! checkpoint seeded with the sentinel rows its range needs (`topology::sentinel`), published by a
//! CAS of the topology.
//!
//! The pieces the steps share:
//! - [`key_bounds`]: a child's range of keys in the log, which its clone projects.
//! - [`seed_rows`]: what a child is seeded with: its routed rows, its recomputed `'R'` and `'O'` rows
//!   and a fresh `'Y'` naming its home.
//! - [`run_entries`]: the parent's run-index entries (one GET per object its `'I'` rows name), which
//!   the children's counts include.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::config::{CheckpointOptions, CheckpointScope};

use super::sentinel::{Counts, Row};
use super::{fence, intent, sentinel, store};
use crate::keyspace::range::Range;
use crate::keyspace::{SENTINEL, tag};
use crate::shard::ShardEngine;
use crate::shard_directory::{Adoption, RetirementReason, ShardDirectory};
use crate::spill::directory::DirEntry;
use crate::spill::{gc, lineage, runindex};

/// The pause's quiesce deadline: the gate exclusive within it, or the split rolls back.
const QUIESCE: Duration = Duration::from_secs(5);
/// How long the retired parent's held operations may take to drain to the children.
const DRAIN: Duration = Duration::from_secs(2);
/// The pause's checkpoint lifetime (deleted at the end; it expires if the split dies).
const CHECKPOINT_LIFETIME: Duration = Duration::from_secs(3600);

/// The keys of the child of `prefix`: from the first key of its entity range to the first key past
/// it, never reaching the sentinel (whose rows a child gets by seed, not by projection).
pub(crate) fn key_bounds(prefix: &str) -> (Bound<Bytes>, Bound<Bytes>) {
    let range = crate::keyspace::range::Range::of(prefix);
    let len = u32::try_from(prefix.len()).unwrap_or(128).min(128);
    let bits = u128::from_str_radix(prefix, 2).unwrap_or(0);
    let lo = if len == 0 { 0 } else { bits << (128 - len) };
    let width = if len == 0 {
        None
    } else {
        1u128.checked_shl(128 - len)
    };
    let hi = width.and_then(|w| lo.checked_add(w));
    debug_assert!(len == 0 || range.holds_route(&lo.to_be_bytes()));
    let start = if lo == 0 {
        Bound::Unbounded
    } else {
        Bound::Included(Bytes::copy_from_slice(&lo.to_be_bytes()))
    };
    let end = hi.map_or(SENTINEL, u128::to_be_bytes);
    (start, Bound::Excluded(Bytes::copy_from_slice(&end)))
}

/// A child's seed: its routed rows, its recomputed `'R'` and `'O'` rows, and a fresh `'Y'` naming
/// `home`, one row per key, in key order.
pub(crate) fn seed_rows(routed: &[Row], counts: &Counts, home: u32) -> Vec<Row> {
    let mut seed: BTreeMap<Vec<u8>, Vec<u8>> = routed.iter().cloned().collect();
    for (id, r) in &counts.refcounts {
        seed.insert(gc::refcount_key(*id), gc::encode_refcount(r));
    }
    for (id, o) in &counts.orphans {
        seed.insert(runindex::orphan_key(*id), runindex::encode_orphans(*o));
    }
    let (key, value) = crate::shard::open_index::identity_row(home);
    seed.insert(key, value);
    seed.into_iter().collect()
}

/// The `(hash, entry)` pairs of the run-index objects the source's `'I'` rows name, each object read
/// whole from `root_of(its home)` and checked against its pointer row.
pub(crate) async fn run_entries(
    store: &dyn ObjectStore,
    root_of: impl Fn(u32) -> String,
    rows: &[Row],
) -> Result<Vec<([u8; 16], DirEntry)>, String> {
    let mut out = Vec::new();
    for (key, value) in rows {
        if key.get(SENTINEL.len()) != Some(&tag::POINTER) {
            continue;
        }
        let id = runindex::parse_sentinel_key(tag::POINTER, key)
            .ok_or_else(|| "a corrupt 'I' key".to_string())?;
        let pointer = runindex::decode_pointer(id, value)
            .ok_or_else(|| format!("a corrupt 'I' row of {id:?}"))?;
        let path = runindex::run_index_path(&root_of(id.home), id);
        let got = store
            .get(&object_store::path::Path::from(path.as_str()))
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        let body = got.bytes().await.map_err(|e| format!("{path}: {e}"))?;
        if body.len() as u64 != pointer.len {
            return Err(format!("{path}: length differs from its 'I' row"));
        }
        let (_, runs) = runindex::format::decode_object(&body)
            .ok_or_else(|| format!("{path}: does not decode"))?;
        for (footer, entries) in runs {
            out.extend(entries.into_iter().map(|e| (footer.hash, e)));
        }
    }
    Ok(out)
}

/// Why a split did not happen (the route's refusal codes, S4-DESIGN §6.2 step 0.1).
#[derive(Debug)]
pub(crate) enum Refused {
    /// `topology_busy`: a live intent holds the cell.
    Busy,
    /// `not_splittable`: the prefix is at the routing width, or its log holds `'T'`.
    NotSplittable(String),
    /// `unknown_prefix`: no leaf of the installed topology.
    UnknownPrefix,
    /// The split started and rolled back before `'T'`.
    RolledBack(String),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("topology_busy: a live intent holds the cell"),
            Self::NotSplittable(why) => write!(f, "not_splittable: {why}"),
            Self::UnknownPrefix => f.write_str("unknown_prefix"),
            Self::RolledBack(why) => write!(f, "rolled back: {why}"),
        }
    }
}

/// What a split published: the topology version, and each child's prefix, home and engine (opened
/// here, this instance owning both halves of the parent it held).
pub(crate) struct Published {
    pub version: u64,
    pub children: Vec<(String, u32, Arc<ShardEngine>)>,
}

/// The operation's parts every publishing step reads.
struct Op<'a> {
    dir: &'a ShardDirectory,
    store: &'a dyn ObjectStore,
    parent: &'a Arc<ShardEngine>,
    children: &'a [Child],
    op_id: &'a str,
}

/// One child: its prefix, home and range.
struct Child {
    prefix: String,
    home: u32,
    range: Range,
}

/// Split `parent`'s shard into its two halves (S4-DESIGN §6.2), as operation `op_id` of
/// `owner`. Before `'T'` any failure rolls back; from `'T'` the split only rolls forward.
pub(crate) async fn split(
    dir: &ShardDirectory,
    parent: &Arc<ShardEngine>,
    op_id: &str,
    owner: &str,
    clock: &dyn crate::runtime::Clock,
) -> Result<Published, Refused> {
    let store = parent.data_store().clone();
    let home = refuse(dir, parent).await?;
    let (version, children, held) = begin(&*store, parent, home, op_id, owner).await?;
    let gate = &parent.gc_state().gate;
    let prepared = prepare(&*store, parent, &children).await;
    let hold = match prepared {
        Ok(()) => gate.exclusive(QUIESCE).await,
        Err(_) => None,
    };
    let barrier = match hold {
        Some(_) => parent.barrier().await.ok(),
        None => None,
    };
    if let Some(point) = gate.take_pause() {
        // Tests only: a test that went away is no reason to stop the split.
        if point.reached.send(()).is_ok() {
            drop(point.go.await);
        }
    }
    let copied = match (&prepared, &barrier) {
        (Ok(()), Some(_)) => copy(&*store, parent, &children, op_id).await,
        (Err(e), _) => Err(e.clone()),
        (_, None) => Err("the quiesce did not complete within its deadline".to_string()),
    };
    if let Err(why) = copied {
        if let Some(barrier) = barrier {
            barrier.resume();
        }
        drop(hold);
        return Err(roll_back(&*store, parent, &children, &held, why).await);
    }
    let retired = fence::Retired {
        version: version + 1,
        op_id: op_id.to_string(),
        children: children
            .iter()
            .map(|c| (c.prefix.clone(), c.home))
            .collect(),
    };
    let row = serde_json::to_vec(&retired).unwrap_or_default();
    let written = parent.db.put(fence::retired_key(), row).await;
    let flushed = match written {
        Ok(_) => parent.db.flush().await,
        Err(e) => Err(e),
    };
    if let Err(e) = flushed {
        let barrier = barrier.expect("held");
        barrier.resume();
        drop(hold);
        return Err(roll_back(&*store, parent, &children, &held, format!("'T': {e}")).await);
    }
    let retire = |reroute| {
        if let Some(barrier) = barrier {
            barrier.retire(reroute);
        }
    };
    let now = clock.monotonic();
    let op = Op {
        dir,
        store: &*store,
        parent,
        children: &children,
        op_id,
    };
    let published = publish(&op, now, retire).await;
    drop(hold);
    let ended = held_terminal(&held, intent::State::Reclaimed, "published");
    drop(intent::replace(&*store, &held, ended).await);
    published.map_err(Refused::RolledBack)
}

/// The refusals (S4-DESIGN §6.2 step 0.1): the parent's home, or why it cannot split now.
async fn refuse(dir: &ShardDirectory, parent: &ShardEngine) -> Result<u32, Refused> {
    let snapshot = dir.prefixes();
    let home = *snapshot
        .shards
        .leaves
        .get(&parent.prefix)
        .ok_or(Refused::UnknownPrefix)?;
    if parent.prefix.len() >= crate::keyspace::ROUTE_PREFIX_BITS {
        return Err(Refused::NotSplittable("at the routing width".to_string()));
    }
    match parent.db.get(fence::retired_key()).await {
        Ok(None) => {}
        Ok(Some(_)) => return Err(Refused::NotSplittable("a retired parent".to_string())),
        Err(e) => return Err(Refused::RolledBack(format!("'T': {e}"))),
    }
    match intent::read(&**parent.data_store()).await {
        Ok(Some(held)) if !held.intent.is_terminal() => Err(Refused::Busy),
        Ok(_) => Ok(home),
        Err(e) => Err(Refused::RolledBack(e.to_string())),
    }
}

/// The home CAS, then the intent (S4-DESIGN §6.2 step 0.2): the version split from, the children
/// and the intent written.
async fn begin(
    store: &dyn ObjectStore,
    parent: &ShardEngine,
    home: u32,
    op_id: &str,
    owner: &str,
) -> Result<(u64, Vec<Child>, intent::Held), Refused> {
    let failed = |e: store::WriteError| Refused::RolledBack(format!("{e:?}"));
    let (topology, etag) = store::read(store).await.map_err(failed)?;
    let (reserved, homes) = topology.reserve_homes();
    store::replace(store, &etag, &reserved)
        .await
        .map_err(failed)?;
    let children: Vec<Child> = ["0", "1"]
        .into_iter()
        .zip(homes)
        .map(|(half, home)| {
            let prefix = format!("{}{half}", parent.prefix);
            Child {
                range: Range::of(&prefix),
                prefix,
                home,
            }
        })
        .collect();
    let preparing = intent::Intent {
        kind: "split".to_string(),
        op_id: op_id.to_string(),
        sources: vec![(parent.prefix.clone(), home)],
        children: homes.to_vec(),
        version_seen: topology.version,
        state: intent::State::Preparing,
        lease_owner: owner.to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    let held = intent::create(store, &preparing)
        .await
        .map_err(|e| match e {
            intent::IntentError::Busy(_) => Refused::Busy,
            other => Refused::RolledBack(other.to_string()),
        })?;
    Ok((topology.version, children, held))
}

/// Preparing (S4-DESIGN §6.2 steps 0.3 to 0.5), unpaused: the parent folds what it can, its
/// collection stands down and the touched-queue mark starts; the plan is made from its sentinel
/// and its lists are PUT.
async fn prepare(
    store: &dyn ObjectStore,
    parent: &Arc<ShardEngine>,
    children: &[Child],
) -> Result<(), String> {
    for _ in 0..8 {
        match parent.fold_once().await {
            Ok(report) if report.more => {}
            _ => break,
        }
    }
    let gate = &parent.gc_state().gate;
    gate.set_collection_off(true);
    gate.start_marking();
    let rows = sentinel_rows(parent).await?;
    let plan = plan(store, parent, children, &rows).await?;
    for (child, (_, counts)) in children.iter().zip(&plan) {
        put_lists(store, parent, child, &rows, counts).await?;
    }
    Ok(())
}

/// Quiesced (S4-DESIGN §6.2 steps 1.3 to 2.5): the WAL flushed and a checkpoint of everything, the
/// plan made again from the quiesced sentinel and its sums checked, then each child cloned from the
/// checkpoint into its range and seeded.
async fn copy(
    store: &dyn ObjectStore,
    parent: &Arc<ShardEngine>,
    children: &[Child],
    op_id: &str,
) -> Result<(), String> {
    parent.db.flush().await.map_err(|e| format!("flush: {e}"))?;
    let options = CheckpointOptions {
        lifetime: Some(CHECKPOINT_LIFETIME),
        source: None,
        name: Some(format!("topo-{op_id}")),
    };
    let checkpoint = parent
        .db
        .create_checkpoint(CheckpointScope::All, &options)
        .await
        .map_err(|e| format!("checkpoint: {e}"))?;
    let rows = sentinel_rows(parent).await?;
    let plan = plan(store, parent, children, &rows).await?;
    let counts: Vec<Counts> = plan.iter().map(|(_, c)| c.clone()).collect();
    sentinel::check_sums(&rows, &counts).map_err(|e| e.to_string())?;
    for (child, (routed, counts)) in children.iter().zip(&plan) {
        put_lists(store, parent, child, &rows, counts).await?;
        let seed = seed_rows(routed, counts, child.home);
        clone_child(parent, &checkpoint, child, seed).await?;
    }
    Ok(())
}

/// Each child's routed rows and counts from the parent's sentinel `rows`.
async fn plan(
    store: &dyn ObjectStore,
    parent: &ShardEngine,
    children: &[Child],
    rows: &[Row],
) -> Result<Vec<(Vec<Row>, Counts)>, String> {
    let ranges: Vec<Range> = children.iter().map(|c| c.range).collect();
    let routed = sentinel::route(rows, &ranges).map_err(|e| e.to_string())?;
    let run = run_entries(store, |home| parent.object_root(home), rows).await?;
    let mut out = Vec::with_capacity(children.len());
    for (routed, range) in routed.into_iter().zip(ranges) {
        let counts = sentinel::child_counts(rows, &run, range).map_err(|e| e.to_string())?;
        out.push((routed, counts));
    }
    Ok(out)
}

/// The parent's sentinel rows, read now.
async fn sentinel_rows(parent: &ShardEngine) -> Result<Vec<Row>, String> {
    let mut scan = parent
        .db
        .scan_prefix(&SENTINEL[..], ..)
        .await
        .map_err(|e| format!("sentinel scan: {e}"))?;
    let mut rows = Vec::new();
    while let Some(kv) = scan
        .next()
        .await
        .map_err(|e| format!("sentinel scan: {e}"))?
    {
        rows.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    Ok(rows)
}

/// `child`'s reference lists, one under each ancestor home's root it holds objects of.
async fn put_lists(
    store: &dyn ObjectStore,
    parent: &ShardEngine,
    child: &Child,
    rows: &[Row],
    counts: &Counts,
) -> Result<(), String> {
    let lists = sentinel::child_lists(rows, counts).map_err(|e| e.to_string())?;
    for (ancestor, entries) in lists {
        let path = lineage::reference_list_path(&parent.object_root(ancestor), child.home);
        store
            .put(
                &object_store::path::Path::from(path.as_str()),
                lineage::encode(&entries).into(),
            )
            .await
            .map_err(|e| format!("{path}: {e}"))?;
    }
    Ok(())
}

/// The fallback arm (S4-DESIGN §6.2 step 2.5): `child`'s log cloned from the parent's checkpoint into
/// its range, then opened, seeded (its `'Y'` last) and flushed, and closed.
async fn clone_child(
    parent: &ShardEngine,
    checkpoint: &slatedb::CheckpointCreateResult,
    child: &Child,
    seed: Vec<Row>,
) -> Result<(), String> {
    let path = super::model::born_home_path(&child.prefix, child.home);
    let source = slatedb::CloneSourceSpec::with_checkpoint(parent.log_path(), checkpoint.id)
        .with_projection_range(key_bounds(&child.prefix));
    slatedb::admin::AdminBuilder::new(path.as_str(), parent.data_store().clone())
        .build()
        .create_clone_builder_from_source(source)
        .build()
        .await
        .map_err(|e| format!("clone {path}: {e}"))?;
    let db = crate::keyspace::db_builder(path.as_str(), parent.data_store().clone())
        .build()
        .await
        .map_err(|e| format!("open {path}: {e}"))?;
    let (identity, _) = crate::shard::open_index::identity_row(child.home);
    let (last, first): (Vec<Row>, Vec<Row>) = seed.into_iter().partition(|(k, _)| *k == identity);
    for batch in [first, last] {
        let mut write = slatedb::WriteBatch::new();
        for (key, value) in batch {
            write.put(key, value);
        }
        db.write(write)
            .await
            .map_err(|e| format!("seed {path}: {e}"))?;
        db.flush().await.map_err(|e| format!("seed {path}: {e}"))?;
    }
    db.close().await.map_err(|e| format!("close {path}: {e}"))
}

/// Published (S4-DESIGN §6.2 step 4): the topology CAS and its install, the children opened here,
/// the barrier retired to them, and once the parent's held operations drained, the parent retired.
async fn publish(
    op: &Op<'_>,
    now: crate::runtime::MonotonicNow,
    retire: impl FnOnce(super::gate::Reroute),
) -> Result<Published, String> {
    let Op {
        dir,
        store,
        parent,
        children,
        op_id,
    } = *op;
    let failed = |e: store::WriteError| format!("{e:?}");
    let (topology, etag) = store::read(store).await.map_err(failed)?;
    let homes = [children[0].home, children[1].home];
    let next = topology.split(&parent.prefix, homes, op_id)?;
    let etag = store::replace(store, &etag, &next).await.map_err(failed)?;
    let mut shards = next.shards.clone();
    shards.version = next.version;
    dir.install(shards, Some(etag), now);
    let mut opened = Vec::with_capacity(children.len());
    for child in children {
        let mut route = [0u8; 16];
        let bits = u128::from_str_radix(&child.prefix, 2).unwrap_or(0);
        route.copy_from_slice(&(bits << (128 - child.prefix.len())).to_be_bytes());
        let engine = dir
            .resolve(&route, Adoption::Internal)
            .await
            .map_err(|e| format!("open {}: {e:?}", child.prefix))?;
        opened.push((child.prefix.clone(), child.home, engine));
    }
    let engines: Vec<(Range, Arc<ShardEngine>)> = children
        .iter()
        .zip(&opened)
        .map(|(c, (_, _, e))| (c.range, e.clone()))
        .collect();
    let reroute: super::gate::Reroute = Arc::new(move |route: &[u8; 16]| {
        engines
            .iter()
            .find(|(r, _)| r.holds_route(route))
            .map(|(_, e)| e.clone())
    });
    retire(reroute);
    let drained = tokio::time::Instant::now() + DRAIN;
    while parent.queued_ops() > 0 && tokio::time::Instant::now() < drained {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    dir.retire(&parent.prefix, RetirementReason::Split, |_, _| true);
    Ok(Published {
        version: next.version,
        children: opened,
    })
}

/// Roll back before `'T'` (S4-DESIGN §6.4's first rows): collection resumes, the touched mark is
/// dropped, the lists are deleted and the intent ends `RolledBack`. The children's paths are left for
/// S6's never-leaf sweeper.
async fn roll_back(
    store: &dyn ObjectStore,
    parent: &ShardEngine,
    children: &[Child],
    held: &intent::Held,
    why: String,
) -> Refused {
    let gate = &parent.gc_state().gate;
    gate.set_collection_off(false);
    drop(gate.take_touched());
    if let Ok(rows) = sentinel_rows(parent).await {
        for child in children {
            let Ok(counts) = sentinel::child_counts(&rows, &[], child.range) else {
                continue;
            };
            let Ok(lists) = sentinel::child_lists(&rows, &counts) else {
                continue;
            };
            for ancestor in lists.keys() {
                let path = lineage::reference_list_path(&parent.object_root(*ancestor), child.home);
                drop(store.delete(&object_store::path::Path::from(path)).await);
            }
        }
    }
    let ended = held_terminal(held, intent::State::RolledBack, &why);
    drop(intent::replace(store, held, ended).await);
    Refused::RolledBack(why)
}

/// `held`'s intent, ended in `state` with `result`.
fn held_terminal(held: &intent::Held, state: intent::State, result: &str) -> intent::Intent {
    let mut ended = held.intent.clone();
    ended.state = state;
    ended.outcome = Some(intent::Outcome {
        result: result.to_string(),
        reason: result.to_string(),
    });
    ended
}

#[cfg(test)]
mod tests;
