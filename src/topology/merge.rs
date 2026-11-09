//! The merge (S6-DESIGN rev 6 §7): two sibling leaves become one, a union clone of both sources'
//! checkpoints, each projected onto its own range, seeded with the merged sentinel rows
//! (`spill::gc::merge`) and published by a CAS of the topology. The shape is S4's split
//! (`topology::split`), run backwards: one intent naming both sources, collection off on both,
//! both gates exclusive, both barriers, `'T'` into both, and the sources closed only after the
//! barrier's `Retired` release re-routes their held operations to `M` (S4-DESIGN §14.1).
//!
//! `M`'s reference lists are written under every ancestor root it holds objects of (the sources'
//! own homes included: their objects become ancestor objects), before the CAS that makes it a leaf
//! (L5). Before `'T'` any failure rolls back; from `'T'` the merge only rolls forward.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::config::{CheckpointOptions, CheckpointScope};

use super::model::{Home, LastOp, Topology, born_home_path};
use super::sentinel::{Counts, Row};
use super::split::key_bounds;
use super::{fence, intent, sentinel, store};
use crate::keyspace::SENTINEL;
use crate::keyspace::range::Range;
use crate::shard::ShardEngine;
use crate::shard_directory::{Adoption, RetirementReason, ShardDirectory};
use crate::spill::gc::merge::{Rows, merged_sentinel};
use crate::spill::{gc, lineage, runindex};

/// The pause's quiesce deadline: both gates exclusive within it, or the merge rolls back.
const QUIESCE: Duration = Duration::from_secs(5);
/// How long the retired sources' held operations may take to drain to `M`.
const DRAIN: Duration = Duration::from_secs(2);
/// How long `M`'s open may be held off while its prefix's previous incarnation closes.
const OPEN_WAIT: Duration = Duration::from_secs(5);
/// The pause's checkpoint lifetime (it expires if the merge dies).
const CHECKPOINT_LIFETIME: Duration = Duration::from_secs(3600);

/// Why a merge did not happen.
#[derive(Debug)]
pub(crate) enum Refused {
    /// A live intent holds the cell.
    Busy,
    /// The two are not sibling leaves of the installed topology, or one holds `'T'`.
    NotMergeable(String),
    /// The merge started and rolled back before `'T'`.
    RolledBack(String),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("topology_busy: a live intent holds the cell"),
            Self::NotMergeable(why) => write!(f, "not_mergeable: {why}"),
            Self::RolledBack(why) => write!(f, "rolled back: {why}"),
        }
    }
}

/// What a merge published: the topology version, and `M`'s prefix, home and engine.
pub(crate) struct Published {
    pub version: u64,
    pub merged: (String, u32, Arc<ShardEngine>),
}

/// The two sources, in prefix order (`…0`, `…1`), with their homes, and the merged leaf's.
struct Pair {
    sources: [(Arc<ShardEngine>, u32); 2],
    prefix: String,
    home: u32,
}

impl Topology {
    /// One fresh home for a merged leaf: `next_home`, with `next_home` past it.
    pub(crate) fn reserve_home(&self) -> (Self, u32) {
        let mut next = self.clone();
        let home = self.shards.next_home;
        next.shards.next_home = home.saturating_add(1);
        (next, home)
    }

    /// The topology once `op_id` merged the sibling leaves under `prefix` into `home` (S6 §7.2
    /// step 4): the next version; `prefix` a leaf at `home`, born at it, whose parents are the two
    /// sources' homes, both retired at it; and `last_op` names the operation. Refused unless both
    /// halves are leaves and `home` is fresh below `next_home`.
    pub(crate) fn merged(&self, prefix: &str, home: u32, op_id: &str) -> Result<Self, String> {
        let halves = [format!("{prefix}0"), format!("{prefix}1")];
        let mut parents = Vec::with_capacity(2);
        for half in &halves {
            let leaf = self
                .shards
                .leaves
                .get(half)
                .ok_or_else(|| format!("{half:?} is no leaf"))?;
            parents.push(*leaf);
        }
        if home >= self.shards.next_home || self.shards.homes.contains_key(&home) {
            return Err(format!("{home} is not a fresh reserved home"));
        }
        let mut next = self.clone();
        let version = self.version + 1;
        next.version = version;
        next.shards.version = version;
        let mut sources = Vec::with_capacity(2);
        for (half, parent) in halves.iter().zip(&parents) {
            next.shards.leaves.remove(half);
            if let Some(source) = next.shards.homes.get_mut(parent) {
                source.retired_version = Some(version);
            }
            sources.push((half.clone(), *parent));
        }
        next.shards.leaves.insert(prefix.to_string(), home);
        let born = Home {
            path: born_home_path(prefix, home),
            prefix: prefix.to_string(),
            parents,
            born_version: version,
            retired_version: None,
        };
        next.shards.homes.insert(home, born);
        next.shards.shards = next.shards.leaves.keys().cloned().collect();
        next.last_op = Some(LastOp {
            op_id: op_id.to_string(),
            kind: "merge".to_string(),
            sources,
            children: vec![home],
        });
        Ok(next)
    }
}

/// The parts every publishing step reads.
struct Op<'a> {
    dir: &'a ShardDirectory,
    store: &'a dyn ObjectStore,
    pair: &'a Pair,
    op_id: &'a str,
}

/// Merge the sibling leaves `sources` (`…0`, then `…1`) into their parent prefix (S6 §7.2), as
/// operation `op_id` of `owner`.
pub(crate) async fn merge(
    dir: &ShardDirectory,
    sources: [&Arc<ShardEngine>; 2],
    op_id: &str,
    owner: &str,
    clock: &dyn crate::runtime::Clock,
) -> Result<Published, Refused> {
    let [left, right] = sources;
    let store = left.data_store().clone();
    let (prefix, homes) = refuse(dir, left, right).await?;
    let (version, home, held) = begin(&*store, &prefix, homes, op_id, owner).await?;
    let pair = Pair {
        sources: [(left.clone(), homes[0]), (right.clone(), homes[1])],
        prefix,
        home,
    };
    let prepared = prepare(dir, &*store, &pair).await;
    let holds = match &prepared {
        Ok(()) => {
            let first = left.gc_state().gate.exclusive(QUIESCE).await;
            let second = match first {
                Some(_) => right.gc_state().gate.exclusive(QUIESCE).await,
                None => None,
            };
            first.zip(second)
        }
        Err(_) => None,
    };
    let barriers = match holds {
        Some(_) => match (left.barrier().await, right.barrier().await) {
            (Ok(a), Ok(b)) => Some((a, b)),
            (Ok(a), Err(_)) | (Err(_), Ok(a)) => {
                a.resume();
                None
            }
            (Err(_), Err(_)) => None,
        },
        None => None,
    };
    let copied = match (&prepared, &barriers) {
        (Ok(()), Some(_)) => copy(dir, &*store, &pair, op_id).await,
        (Err(e), _) => Err(e.clone()),
        (_, None) => Err("the quiesce did not complete within its deadline".to_string()),
    };
    if let Err(why) = copied {
        if let Some((a, b)) = barriers {
            a.resume();
            b.resume();
        }
        drop(holds);
        return Err(roll_back(dir, &*store, &pair, &held, why).await);
    }
    if let Err(why) = retire_sources(&pair, version, op_id).await {
        if let Some((a, b)) = barriers {
            a.resume();
            b.resume();
        }
        drop(holds);
        return Err(roll_back(dir, &*store, &pair, &held, why).await);
    }
    let retire = |reroute: super::gate::Reroute| {
        if let Some((a, b)) = barriers {
            a.retire(reroute.clone());
            b.retire(reroute);
        }
    };
    let op = Op {
        dir,
        store: &*store,
        pair: &pair,
        op_id,
    };
    let published = publish(&op, clock.monotonic(), retire).await;
    drop(holds);
    let ended = terminal(&held, intent::State::Reclaimed, "published");
    drop(intent::replace(&*store, &held, ended).await);
    published.map_err(Refused::RolledBack)
}

/// The refusals: the parent prefix and the two sources' homes, or why they cannot merge now.
async fn refuse(
    dir: &ShardDirectory,
    left: &ShardEngine,
    right: &ShardEngine,
) -> Result<(String, [u32; 2]), Refused> {
    let not = |why: &str| Refused::NotMergeable(why.to_string());
    let prefix = left
        .prefix
        .strip_suffix('0')
        .filter(|p| right.prefix.strip_suffix('1') == Some(*p))
        .ok_or_else(|| not("the two are not sibling halves"))?
        .to_string();
    let snapshot = dir.prefixes();
    let home_of = |p: &str| snapshot.shards.leaves.get(p).copied();
    let homes = [
        home_of(&left.prefix).ok_or_else(|| not("the left half is no leaf"))?,
        home_of(&right.prefix).ok_or_else(|| not("the right half is no leaf"))?,
    ];
    for source in [left, right] {
        match source.db.get(fence::retired_key()).await {
            Ok(None) => {}
            Ok(Some(_)) => return Err(not("a retired source")),
            Err(e) => return Err(Refused::RolledBack(format!("'T': {e}"))),
        }
    }
    match intent::read(&**left.data_store()).await {
        Ok(Some(held)) if !held.intent.is_terminal() => Err(Refused::Busy),
        Ok(_) => Ok((prefix, homes)),
        Err(e) => Err(Refused::RolledBack(e.to_string())),
    }
}

/// The home CAS, then the intent naming both sources and `M`: the version merged from, `M`'s home
/// and the intent written.
async fn begin(
    store: &dyn ObjectStore,
    prefix: &str,
    homes: [u32; 2],
    op_id: &str,
    owner: &str,
) -> Result<(u64, u32, intent::Held), Refused> {
    let failed = |e: store::WriteError| Refused::RolledBack(format!("{e:?}"));
    let (topology, etag) = store::read(store).await.map_err(failed)?;
    let (reserved, home) = topology.reserve_home();
    store::replace(store, &etag, &reserved)
        .await
        .map_err(failed)?;
    let preparing = intent::Intent {
        kind: "merge".to_string(),
        op_id: op_id.to_string(),
        sources: vec![
            (format!("{prefix}0"), homes[0]),
            (format!("{prefix}1"), homes[1]),
        ],
        children: vec![home],
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
    Ok((topology.version, home, held))
}

/// Preparing, unpaused: each source folds what it can, its collection stands down and its
/// touched-queue mark starts; `M`'s lists are made from the merged sentinel and PUT.
async fn prepare(dir: &ShardDirectory, store: &dyn ObjectStore, pair: &Pair) -> Result<(), String> {
    for (source, _) in &pair.sources {
        for _ in 0..8 {
            match source.fold_once().await {
                Ok(report) if report.more => {}
                _ => break,
            }
        }
        let gate = &source.gc_state().gate;
        gate.set_collection_off(true);
        gate.start_marking();
    }
    let merged = merged_rows(pair).await?;
    put_lists(dir, store, pair, &merged).await
}

/// `M`'s sentinel rows: the merge rules over both sources' sentinel rows, read now.
async fn merged_rows(pair: &Pair) -> Result<Vec<Row>, String> {
    let left = sentinel_rows(&pair.sources[0].0).await?;
    let right = sentinel_rows(&pair.sources[1].0).await?;
    let merged = merged_sentinel(&left, &right).map_err(|e| format!("merge rules: {e:?}"))?;
    Ok(merged.into_iter().collect())
}

/// A source's sentinel rows, read now.
async fn sentinel_rows(source: &ShardEngine) -> Result<Rows, String> {
    let mut scan = source
        .db
        .scan_prefix(&SENTINEL[..], ..)
        .await
        .map_err(|e| format!("sentinel scan: {e}"))?;
    let mut rows = Rows::new();
    while let Some(kv) = scan
        .next()
        .await
        .map_err(|e| format!("sentinel scan: {e}"))?
    {
        rows.insert(kv.key.to_vec(), kv.value.to_vec());
    }
    Ok(rows)
}

/// `M`'s counts, from its merged `'R'` and `'O'` rows.
fn counts_of(merged: &[Row]) -> Result<Counts, String> {
    let mut counts = Counts::default();
    for (key, value) in merged {
        match key.get(SENTINEL.len()) {
            Some(&crate::keyspace::tag::REFCOUNT) => {
                let id = runindex::parse_sentinel_key(crate::keyspace::tag::REFCOUNT, key)
                    .ok_or("a corrupt 'R' key")?;
                let r = gc::decode_refcount(value).ok_or("a corrupt 'R' row")?;
                counts.refcounts.push((id, r));
            }
            Some(&crate::keyspace::tag::ORPHAN) => {
                let id = runindex::parse_sentinel_key(crate::keyspace::tag::ORPHAN, key)
                    .ok_or("a corrupt 'O' key")?;
                let o = runindex::decode_orphans(value).ok_or("a corrupt 'O' row")?;
                counts.orphans.push((id, o));
            }
            _ => {}
        }
    }
    Ok(counts)
}

/// `M`'s reference lists, one under each ancestor home's root it holds objects of.
async fn put_lists(
    dir: &ShardDirectory,
    store: &dyn ObjectStore,
    pair: &Pair,
    merged: &[Row],
) -> Result<(), String> {
    let counts = counts_of(merged)?;
    let lists = sentinel::child_lists(merged, &counts).map_err(|e| e.to_string())?;
    let snapshot = dir.prefixes();
    for (ancestor, entries) in lists {
        let root = snapshot
            .shards
            .homes
            .get(&ancestor)
            .map(|h| h.path.clone())
            .ok_or_else(|| format!("home {ancestor} is not in the topology"))?;
        let path = lineage::reference_list_path(&root, pair.home);
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

/// Quiesced: both WALs flushed and checkpointed, the lists written again from the quiesced
/// sentinels, then `M` cloned as the union of both checkpoints, each projected onto its range,
/// and seeded (its `'Y'` last).
async fn copy(
    dir: &ShardDirectory,
    store: &dyn ObjectStore,
    pair: &Pair,
    op_id: &str,
) -> Result<(), String> {
    let mut checkpoints = Vec::with_capacity(2);
    for (source, _) in &pair.sources {
        source.db.flush().await.map_err(|e| format!("flush: {e}"))?;
        let options = CheckpointOptions {
            lifetime: Some(CHECKPOINT_LIFETIME),
            source: None,
            name: Some(format!("topo-{op_id}")),
        };
        let checkpoint = source
            .db
            .create_checkpoint(CheckpointScope::All, &options)
            .await
            .map_err(|e| format!("checkpoint: {e}"))?;
        checkpoints.push(checkpoint);
    }
    let merged = merged_rows(pair).await?;
    put_lists(dir, store, pair, &merged).await?;
    clone_merged(pair, &checkpoints, merged).await
}

/// `M`'s log cloned from both checkpoints, each source projected onto its own range, and seeded
/// with its sentinel rows in the same step (C4's seeded clone, `with_initial_rows`): `M` is
/// initialized together with its rows, its fresh `'Y'` among them.
async fn clone_merged(
    pair: &Pair,
    checkpoints: &[slatedb::CheckpointCreateResult],
    seed: Vec<Row>,
) -> Result<(), String> {
    let path = born_home_path(&pair.prefix, pair.home);
    let mut sources = pair
        .sources
        .iter()
        .zip(checkpoints)
        .map(|((source, _), checkpoint)| {
            slatedb::CloneSourceSpec::with_checkpoint(source.log_path(), checkpoint.id)
                .with_projection_range(key_bounds(&source.prefix))
        });
    let (Some(first), Some(second)) = (sources.next(), sources.next()) else {
        return Err("a merge has two sources".to_string());
    };
    let (identity, value) = crate::shard::open_index::identity_row(pair.home);
    let mut rows: BTreeMap<Vec<u8>, Vec<u8>> = seed.into_iter().collect();
    rows.insert(identity, value);
    let rows = rows
        .into_iter()
        .map(|(key, value)| (bytes::Bytes::from(key), bytes::Bytes::from(value)))
        .collect();
    let store = pair.sources[0].0.data_store().clone();
    slatedb::admin::AdminBuilder::new(path.as_str(), store)
        .build()
        .create_clone_builder_from_source(first)
        .with_source(second)
        .with_initial_rows(rows)
        .build()
        .await
        .map_err(|e| format!("clone {path}: {e}"))
}

/// SourcesRetired: one `'T'` into each source, naming `M`, and flushed. The sources stay open,
/// their barriers held, until the release.
async fn retire_sources(pair: &Pair, version: u64, op_id: &str) -> Result<(), String> {
    let retired = fence::Retired {
        version: version + 1,
        op_id: op_id.to_string(),
        children: vec![(pair.prefix.clone(), pair.home)],
    };
    let row = serde_json::to_vec(&retired).map_err(|e| format!("'T': {e}"))?;
    for (source, _) in &pair.sources {
        source
            .db
            .put(fence::retired_key(), row.clone())
            .await
            .map_err(|e| format!("'T': {e}"))?;
        source.db.flush().await.map_err(|e| format!("'T': {e}"))?;
    }
    Ok(())
}

/// Published: the topology CAS and its install, `M` opened here, both barriers retired to it, and
/// once the sources' held operations drained, the sources retired (after the release, so a held
/// operation is re-routed, never drained as `shard_moving`).
async fn publish(
    op: &Op<'_>,
    now: crate::runtime::MonotonicNow,
    retire: impl FnOnce(super::gate::Reroute),
) -> Result<Published, String> {
    let Op {
        dir,
        store,
        pair,
        op_id,
    } = *op;
    let failed = |e: store::WriteError| format!("{e:?}");
    let (topology, etag) = store::read(store).await.map_err(failed)?;
    let next = topology.merged(&pair.prefix, pair.home, op_id)?;
    let etag = store::replace(store, &etag, &next).await.map_err(failed)?;
    let mut shards = next.shards.clone();
    shards.version = next.version;
    dir.install(shards, Some(etag), now);
    // The release first: each held operation waits for `M`'s open, then goes to it.
    let range = Range::of(&pair.prefix);
    let (resolver, merged_prefix) = (dir.clone(), pair.prefix.clone());
    let reroute: super::gate::Reroute = Arc::new(move |route: [u8; 16]| {
        let (dir, prefix, ours) = (
            resolver.clone(),
            merged_prefix.clone(),
            range.holds_route(&route),
        );
        Box::pin(async move {
            if !ours {
                return None;
            }
            open_merged(&dir, &route, &prefix).await.ok()
        })
    });
    retire(reroute);
    let bits = u128::from_str_radix(&pair.prefix, 2).unwrap_or(0);
    let shift = 128 - u32::try_from(pair.prefix.len()).unwrap_or(0).min(128);
    let route = bits.checked_shl(shift).unwrap_or(0).to_be_bytes();
    let engine = open_merged(dir, &route, &pair.prefix).await?;
    let drained = tokio::time::Instant::now() + DRAIN;
    while pair.sources.iter().any(|(s, _)| s.queued_ops() > 0)
        && tokio::time::Instant::now() < drained
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for (source, _) in &pair.sources {
        // A newer topology no longer has the source's prefix as a leaf (the variant's meaning).
        dir.retire(&source.prefix, RetirementReason::Split, |_, _| true);
    }
    Ok(Published {
        version: next.version,
        merged: (pair.prefix.clone(), pair.home, engine),
    })
}

/// `M` opened here. Its prefix is the one the split retired, so the directory may still be
/// closing that incarnation: an open it holds off (`Opening`) is retried within `OPEN_WAIT`.
async fn open_merged(
    dir: &ShardDirectory,
    route: &[u8; 16],
    prefix: &str,
) -> Result<Arc<ShardEngine>, String> {
    let deadline = tokio::time::Instant::now() + OPEN_WAIT;
    loop {
        match dir.resolve(route, Adoption::Internal).await {
            Ok(engine) => return Ok(engine),
            Err(crate::shard_directory::ResolveError::Opening { .. })
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(format!("open {prefix}: {e:?}")),
        }
    }
}

/// Roll back before `'T'`: collection resumes on both sources, their touched marks are dropped,
/// `M`'s lists are deleted and the intent ends `RolledBack`. `M`'s path, if a clone began, is left
/// for the never-leaf sweeper.
async fn roll_back(
    dir: &ShardDirectory,
    store: &dyn ObjectStore,
    pair: &Pair,
    held: &intent::Held,
    why: String,
) -> Refused {
    for (source, _) in &pair.sources {
        let gate = &source.gc_state().gate;
        gate.set_collection_off(false);
        drop(gate.take_touched());
    }
    if let Ok(merged) = merged_rows(pair).await {
        let lists = counts_of(&merged)
            .ok()
            .and_then(|counts| sentinel::child_lists(&merged, &counts).ok())
            .unwrap_or_default();
        let snapshot = dir.prefixes();
        let roots: BTreeMap<u32, String> = snapshot
            .shards
            .homes
            .iter()
            .map(|(h, home)| (*h, home.path.clone()))
            .collect();
        for ancestor in lists.keys() {
            if let Some(root) = roots.get(ancestor) {
                let path = lineage::reference_list_path(root, pair.home);
                drop(store.delete(&object_store::path::Path::from(path)).await);
            }
        }
    }
    let ended = terminal(held, intent::State::RolledBack, &why);
    drop(intent::replace(store, held, ended).await);
    Refused::RolledBack(why)
}

/// What a claimant of a lapsed merge intent found, once its own opens had fenced **both** sources
/// (S6 §7.4, L8): a claim that fenced one source could decide while the other's previous writer
/// still acks writes `M`'s older checkpoint lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Evidence {
    /// Whether each source's log holds `'T'` (this merge's).
    pub(crate) retired: [bool; 2],
    /// Whether a `topo-<op_id>` checkpoint, or any file under `M`'s path, exists.
    pub(crate) copy_began: bool,
    /// Whether `M`'s log is complete (its seeded clone finished: `'Y'` present).
    pub(crate) merged_complete: bool,
    /// Whether the stored topology already names this operation as its `last_op`.
    pub(crate) published: bool,
}

/// The claimant's decision for a lapsed merge intent (S6 §7.4's table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Recovery {
    /// `'T'` in neither source, nothing copied: delete `M`'s lists, end the intent `RolledBack`.
    RollBack,
    /// `'T'` in neither source, a copy begun: also delete `M`'s clone (its manifest's
    /// `external_dbs` checkpoints, its files) and both `topo-` checkpoints, then roll back.
    RollBackWithTakeoverGc,
    /// `'T'` in one or both sources and `M` complete: write the missing `'T'`, publish (idempotent
    /// on `last_op`), release any hold with `Retired`, then close both, and end `Reclaimed`.
    RollForward,
    /// The topology already names the operation: end the intent `Reclaimed`.
    Reclaim,
    /// `'T'` written but `M` not complete cannot arise (`'T'` follows `M`'s seeded clone); the claim
    /// stops and pages rather than guess.
    Inconsistent,
}

/// S6 §7.4's table over `evidence`.
pub(crate) fn recovery(evidence: Evidence) -> Recovery {
    let any_retired = evidence.retired.iter().any(|r| *r);
    match (evidence.published, any_retired, evidence.merged_complete) {
        (true, _, _) => Recovery::Reclaim,
        (false, true, true) => Recovery::RollForward,
        (false, true, false) => Recovery::Inconsistent,
        (false, false, _) if evidence.copy_began => Recovery::RollBackWithTakeoverGc,
        (false, false, _) => Recovery::RollBack,
    }
}

/// `held`'s intent, ended in `state` with `result`.
fn terminal(held: &intent::Held, state: intent::State, result: &str) -> intent::Intent {
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
