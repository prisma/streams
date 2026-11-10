//! The split (S4-DESIGN §6): one shard log becomes two, each a projected clone of the parent's
//! checkpoint seeded at its initialization with the sentinel rows its range needs
//! (`topology::sentinel`; C4), published by a CAS of the topology.
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

use super::actor::Actor;
use super::sentinel::{Counts, Row};
use super::{fence, intent, recover, sentinel, store};
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
        let (_, runs) = runindex::whole::decode_object(&body)
            .ok_or_else(|| format!("{path}: does not decode"))?;
        for (footer, entries) in runs {
            out.extend(entries.into_iter().map(|e| (footer.hash, e)));
        }
    }
    Ok(out)
}

/// Why a split did not happen (the route's refusal codes, S4-DESIGN §6.2 step 0.1).
#[derive(Debug, Clone)]
pub(crate) enum Refused {
    /// `topology_busy`: a live intent holds the cell (`intent`), or the fleet has not settled
    /// (`fleet`). An override is no refusal of its own (#251 amendment R3).
    Busy(&'static str),
    /// `not_ring_owner`: another instance (named) holds the parent.
    NotOwner(String),
    /// `instance_full`: at its peak (the parent and both children) the split would have this
    /// instance hold `resident` shards, past its `max` (`SHARDS_PER_INSTANCE_MAX`, S4-DESIGN §10).
    InstanceFull { resident: usize, max: u32 },
    /// `topology_unavailable`: no topology loop runs here, or the parent could not be opened.
    Unavailable(String),
    /// `not_splittable`: the prefix is at the routing width, or its log holds `'T'`.
    NotSplittable(String),
    /// `unknown_prefix`: no leaf of the installed topology.
    UnknownPrefix,
    /// The split started and rolled back before `'T'`.
    RolledBack(String),
    /// The split wrote `'T'` but its publish failed: it only rolls forward, so its intent stays
    /// live for the next opener of the parent to claim and roll forward from `'T'` (S4-DESIGN §6.4).
    Unfinished(String),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(reason) => write!(f, "topology_busy: {reason}"),
            Self::NotOwner(owner) => write!(f, "not_ring_owner: {owner} holds the parent"),
            Self::InstanceFull { resident, max } => {
                write!(
                    f,
                    "instance_full: {resident} resident shards at the split's peak, past {max}"
                )
            }
            Self::Unavailable(why) => write!(f, "topology_unavailable: {why}"),
            Self::NotSplittable(why) => write!(f, "not_splittable: {why}"),
            Self::UnknownPrefix => f.write_str("unknown_prefix"),
            Self::RolledBack(why) => write!(f, "rolled back: {why}"),
            Self::Unfinished(why) => {
                write!(
                    f,
                    "past 'T', unpublished: {why}; the next open of the parent rolls it forward"
                )
            }
        }
    }
}

/// What a split published: the topology version, and each child's prefix, home and engine (opened
/// here, this instance owning both halves of the parent it held).
pub(crate) struct Published {
    pub version: u64,
    pub children: Vec<(String, u32, Arc<ShardEngine>)>,
    /// The pause: from the barrier's park to its release to the children (S4-DESIGN §9).
    pub pause: Duration,
}

/// The operation's parts every step reads.
struct Op<'a> {
    dir: &'a ShardDirectory,
    store: &'a dyn ObjectStore,
    parent: &'a Arc<ShardEngine>,
    children: &'a [Child],
    op_id: &'a str,
    /// The topology version the split started from.
    version: u64,
}

/// One child: its prefix, home and range.
struct Child {
    prefix: String,
    home: u32,
    range: Range,
}

/// A split that started (S4-DESIGN §6.2 steps 0.1 and 0.2): its refusals passed, its homes are
/// reserved and its intent written. The rest of it runs in `finish`, as its own task.
pub(crate) struct Started {
    dir: ShardDirectory,
    parent: Arc<ShardEngine>,
    op_id: String,
    version: u64,
    children: Vec<Child>,
    held: intent::Held,
}

/// Start the split of `parent`'s shard into its two halves as operation `op_id` of `owner`: the
/// refusals, the home CAS and the intent.
pub(crate) async fn start(
    dir: &ShardDirectory,
    parent: &Arc<ShardEngine>,
    op_id: &str,
    owner: &str,
) -> Result<Started, Refused> {
    let store = parent.data_store().clone();
    let home = refuse(dir, parent).await?;
    let (version, children, held) = begin(&*store, parent, home, op_id, owner).await?;
    Ok(Started {
        dir: dir.clone(),
        parent: parent.clone(),
        op_id: op_id.to_string(),
        version,
        children,
        held,
    })
}

/// Split `parent`'s shard, start to finish (`start`, then `Started::finish`).
#[cfg(test)]
pub(crate) async fn split(
    dir: &ShardDirectory,
    parent: &Arc<ShardEngine>,
    op_id: &str,
    owner: &str,
    clock: &dyn crate::runtime::Clock,
) -> Result<Published, Refused> {
    start(dir, parent, op_id, owner).await?.finish(clock).await
}

impl Started {
    /// The operation's id.
    pub(crate) fn op_id(&self) -> &str {
        &self.op_id
    }

    /// The topology version the split started from.
    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    /// The rest of the split (S4-DESIGN §6.2 from step 0.3). Before `'T'` any failure, and a lost
    /// lease, rolls back; from `'T'` the split only rolls forward. The actor renews its intent while
    /// the steps run, and ends it once they are done.
    pub(crate) async fn finish(
        self,
        clock: &dyn crate::runtime::Clock,
    ) -> Result<Published, Refused> {
        let store = self.parent.data_store().clone();
        let actor = Actor::new(&*store, clock, self.held);
        let op = Op {
            dir: &self.dir,
            store: &*store,
            parent: &self.parent,
            children: &self.children,
            op_id: &self.op_id,
            version: self.version,
        };
        let steps = std::pin::pin!(run(&op, &actor));
        let renewals = std::pin::pin!(actor.renew());
        let ran = match futures_util::future::select(steps, renewals).await {
            futures_util::future::Either::Left((ran, _)) => ran,
            futures_util::future::Either::Right((never, _)) => match never {},
        };
        match &ran {
            Ok(_) => {
                actor.end(intent::State::Reclaimed, "published").await;
                recover::drop_checkpoint(&self.parent, &self.op_id).await;
            }
            Err(Refused::RolledBack(why)) => actor.end(intent::State::RolledBack, why).await,
            // Past 'T' (Unfinished) the split only rolls forward: its intent stays live, and
            // lapses for the next opener of the parent to claim and roll forward from 'T'.
            Err(_) => {}
        }
        ran
    }
}

/// The split's steps from Preparing to its publish (S4-DESIGN §6.2), the actor's ownership checked
/// at each step before `'T'` and around the clones; a failure before `'T'` is rolled back here.
async fn run(op: &Op<'_>, actor: &Actor<'_>) -> Result<Published, Refused> {
    let parent = op.parent;
    let gate = &parent.gc_state().gate;
    let prepared = prepare(op.store, parent, op.children).await;
    let prepared = match prepared {
        Ok(put) => actor.still_owned("the pause").await.map(|()| put),
        Err(e) => Err(e),
    };
    let hold = match prepared {
        Ok(_) => gate.exclusive(QUIESCE).await,
        Err(_) => None,
    };
    let barrier = match hold {
        Some(_) => parent.barrier().await.ok(),
        None => None,
    };
    #[cfg(test)]
    if let Some(point) = gate.take_pause() {
        // Tests only: a test that went away is no reason to stop the split.
        if point.reached.send(()).is_ok() {
            drop(point.go.await);
        }
    }
    let paused_at = std::time::Instant::now();
    let copied = match (&prepared, &barrier) {
        (Ok(put), Some(_)) => copy(op, actor, put).await,
        (Err(e), _) => Err(e.clone()),
        (_, None) => Err("the quiesce did not complete within its deadline".to_string()),
    };
    let retired = match copied {
        Ok(()) => write_retired(op, actor).await,
        Err(e) => Err(e),
    };
    let barrier = match (retired, barrier) {
        (Ok(()), Some(barrier)) => barrier,
        (retired, barrier) => {
            if let Some(barrier) = barrier {
                barrier.resume();
            }
            drop(hold);
            let why = retired
                .err()
                .unwrap_or_else(|| "the barrier was lost".to_string());
            return Err(roll_back(op.dir, parent, &actor.held(), why).await);
        }
    };
    let released = std::sync::OnceLock::new();
    let retire = |reroute| {
        released.get_or_init(|| paused_at.elapsed());
        barrier.retire(reroute);
    };
    let published = publish(op, actor.clock().monotonic(), retire)
        .await
        .map(|mut published| {
            published.pause = released.get().copied().unwrap_or_default();
            published
        });
    drop(hold);
    published.map_err(Refused::Unfinished)
}

/// The parent's `'T'` (S4-DESIGN §6.2 step 3.1), written and flushed while the actor still owns its
/// intent: from it the split only rolls forward.
async fn write_retired(op: &Op<'_>, actor: &Actor<'_>) -> Result<(), String> {
    actor.still_owned("'T'").await?;
    let retired = fence::Retired {
        version: op.version + 1,
        op_id: op.op_id.to_string(),
        children: op
            .children
            .iter()
            .map(|c| (c.prefix.clone(), c.home))
            .collect(),
    };
    let row = serde_json::to_vec(&retired).unwrap_or_default();
    op.parent
        .db
        .put(fence::retired_key(), row)
        .await
        .map_err(|e| format!("'T': {e}"))?;
    op.parent.db.flush().await.map_err(|e| format!("'T': {e}"))
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
    match super::reclaim::read(&**parent.data_store()).await {
        Ok(Some(held)) if !held.intent.is_terminal() => Err(Refused::Busy("intent")),
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
            intent::IntentError::Busy(_) => Refused::Busy("intent"),
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
) -> Result<Vec<Lists>, String> {
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
    let mut put = Vec::with_capacity(children.len());
    for (child, (_, counts)) in children.iter().zip(&plan) {
        let lists = sentinel::child_lists(&rows, counts).map_err(|e| e.to_string())?;
        put_lists(store, parent, child, &lists, lineage::reference_list_path).await?;
        put.push(lists);
    }
    Ok(put)
}

/// Quiesced (S4-DESIGN §6.2 steps 1.3 to 2.5): the WAL flushed and a checkpoint of everything, the
/// plan made again from the quiesced sentinel and its sums checked, then each child cloned from the
/// checkpoint into its range and seeded.
async fn copy(op: &Op<'_>, actor: &Actor<'_>, prepared: &[Lists]) -> Result<(), String> {
    let (store, parent, children, op_id) = (op.store, op.parent, op.children, op.op_id);
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
    for ((child, (_, counts)), put) in children.iter().zip(&plan).zip(prepared) {
        let now = sentinel::child_lists(&rows, counts).map_err(|e| e.to_string())?;
        let (first, more) = pause_lists(put, supplements(put, &now));
        put_lists(store, parent, child, &first, lineage::reference_list_path).await?;
        put_lists(
            store,
            parent,
            child,
            &more,
            lineage::supplementary_list_path,
        )
        .await?;
    }
    // A clone CASes its final checkpoint onto the parent's manifest, which no writer epoch fences
    // (S4-REVIEW-rev2 N1): ownership is checked right before and right after them.
    actor.still_owned("the clones").await?;
    let clones = children.iter().zip(&plan).map(|(child, (routed, counts))| {
        let seed = seed_rows(routed, counts, child.home);
        clone_child(parent, &checkpoint, child, seed)
    });
    for cloned in futures_util::future::join_all(clones).await {
        cloned?;
    }
    actor.still_owned("'T', after the clones").await
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
    let released = sentinel::Released::of(rows).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(children.len());
    for (routed, range) in routed.into_iter().zip(ranges) {
        let counts =
            sentinel::child_counts(rows, &run, range, &released).map_err(|e| e.to_string())?;
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

/// A child's lists: per ancestor home, the objects of that home it holds (`sentinel::child_lists`).
type Lists = BTreeMap<u32, Vec<sentinel::ListEntry>>;

/// Per ancestor home, the holdings `now` lists that `put` lacks (S4-DESIGN §7.3): what the pause
/// lists (`pause_lists`). A home with none has no list from the pause.
fn supplements(put: &Lists, now: &Lists) -> Lists {
    let mut added = Lists::new();
    for (home, entries) in now {
        let had = put.get(home).map_or(&[][..], Vec::as_slice);
        let new: Vec<_> = entries
            .iter()
            .filter(|entry| had.binary_search(entry).is_err())
            .copied()
            .collect();
        if !new.is_empty() {
            added.insert(*home, new);
        }
    }
    added
}

/// The pause's lists from `added` (`supplements`), split by whether Preparing listed the home:
/// the homes it did not (the child's first holding of the home landed after Preparing's plan) get
/// their reference list now, the others a `-d` list beside theirs. S6's readers (the open-time
/// detector, the lineage sweep's `held_by`) require a reference list for every ancestor home a
/// child holds, and the parent is a live leaf until the publish, so no sweep has yet decided on
/// one's absence.
fn pause_lists(put: &Lists, added: Lists) -> (Lists, Lists) {
    added
        .into_iter()
        .partition(|(home, _)| !put.contains_key(home))
}

/// `child`'s lists, one under each ancestor home's root in `lists`, at `path_of(root, child.home)`:
/// its reference lists in Preparing, its supplementary (`-d`) lists in the pause.
async fn put_lists(
    store: &dyn ObjectStore,
    parent: &ShardEngine,
    child: &Child,
    lists: &Lists,
    path_of: fn(&str, u32) -> String,
) -> Result<(), String> {
    for (ancestor, entries) in lists {
        let path = path_of(&parent.object_root(*ancestor), child.home);
        store
            .put(
                &object_store::path::Path::from(path.as_str()),
                lineage::encode(entries).into(),
            )
            .await
            .map_err(|e| format!("{path}: {e}"))?;
    }
    Ok(())
}

/// C4 (S4-DESIGN §6.2 step 2.4, the fork's `CloneBuilder::with_initial_rows`): `child`'s log is
/// cloned from the parent's checkpoint into its range and holds its seed (its `'Y'` included) as its
/// own L0 table from the moment it is initialized, so the child needs no open to be complete: a
/// clone that fails before it is initialized has no `'Y'` and cannot be opened.
async fn clone_child(
    parent: &ShardEngine,
    checkpoint: &slatedb::CheckpointCreateResult,
    child: &Child,
    seed: Vec<Row>,
) -> Result<(), String> {
    let path = super::model::born_home_path(&child.prefix, child.home);
    let source = slatedb::CloneSourceSpec::with_checkpoint(parent.log_path(), checkpoint.id)
        .with_projection_range(key_bounds(&child.prefix));
    let rows = seed
        .into_iter()
        .map(|(key, value)| (Bytes::from(key), Bytes::from(value)))
        .collect();
    slatedb::admin::AdminBuilder::new(path.as_str(), parent.data_store().clone())
        .build()
        .create_clone_builder_from_source(source)
        .with_initial_rows(rows)
        .build()
        .await
        .map_err(|e| format!("clone {path}: {e}"))
}

/// Published (S4-DESIGN §6.2 step 4): the topology CAS and its install, then at once the barrier
/// retired to the children (each held append waits for its child's open), the children opened here,
/// and once the parent's held operations drained, the parent retired.
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
        ..
    } = *op;
    let failed = |e: store::WriteError| format!("{e:?}");
    let (topology, etag) = store::read(store).await.map_err(failed)?;
    let homes = [children[0].home, children[1].home];
    let next = topology.split(&parent.prefix, homes, op_id)?;
    let etag = store::replace(store, &etag, &next).await.map_err(failed)?;
    let mut shards = next.shards.clone();
    shards.version = next.version;
    dir.install(shards, Some(etag), now);
    let ranges: Vec<Range> = children.iter().map(|c| c.range).collect();
    let resolver = dir.clone();
    let reroute: super::gate::Reroute = Arc::new(move |route: [u8; 16]| {
        let (dir, ours) = (
            resolver.clone(),
            ranges.iter().any(|r| r.holds_route(&route)),
        );
        Box::pin(async move {
            if !ours {
                return None;
            }
            dir.resolve(&route, Adoption::Internal).await.ok()
        })
    });
    retire(reroute);
    let opens = children.iter().map(|child| async move {
        let bits = u128::from_str_radix(&child.prefix, 2).unwrap_or(0);
        let route = (bits << (128 - child.prefix.len())).to_be_bytes();
        let engine = dir
            .resolve(&route, Adoption::Internal)
            .await
            .map_err(|e| format!("open {}: {e:?}", child.prefix))?;
        Ok::<_, String>((child.prefix.clone(), child.home, engine))
    });
    // The split is published: a child that fails to open here opens on its next request, and the
    // split goes on to retire its parent.
    let mut opened = Vec::with_capacity(children.len());
    for open in futures_util::future::join_all(opens).await {
        match open {
            Ok(child) => opened.push(child),
            Err(error) => tracing::warn!(op_id, "a published split's child did not open: {error}"),
        }
    }
    let drained = tokio::time::Instant::now() + DRAIN;
    while parent.queued_ops() > 0 && tokio::time::Instant::now() < drained {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    dir.retire(&parent.prefix, RetirementReason::Split, |_, _| true);
    Ok(Published {
        version: next.version,
        children: opened,
        pause: Duration::ZERO,
    })
}

/// Roll back before `'T'` (S4-DESIGN §6.4's first rows): collection resumes and the touched mark is
/// dropped, then the claimant's deletions run on the split's own intent (`recover::clear`); the
/// actor ends the intent once its renewals stop.
async fn roll_back(
    dir: &ShardDirectory,
    parent: &ShardEngine,
    held: &intent::Held,
    why: String,
) -> Refused {
    let gate = &parent.gc_state().gate;
    gate.set_collection_off(false);
    drop(gate.take_touched());
    let shards = dir.prefixes().shards.clone();
    let roots = shards
        .leaves
        .get(&parent.prefix)
        .map(|home| recover::lineage_of(&shards, *home))
        .unwrap_or_default();
    recover::clear(parent, &roots, &held.intent).await;
    Refused::RolledBack(why)
}

#[cfg(test)]
mod tests;
