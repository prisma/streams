//! The claimant of a lapsed merge (S6-DESIGN rev 6 §7.4, L8), dispatched from the open-path fence
//! (`topology::fence`) for an intent of kind `merge`. A merge has two sources, and the claimant
//! fences **both** before it decides: the engine opening is one, and the other's log gets a
//! maintenance open here (never served), whose writer-epoch bump fences its previous writer. A
//! claim that fenced one source could decide while the other's previous writer still acked writes
//! that `M`'s older checkpoint lacks (`nc-claim-fences-one-source`).
//!
//! The claim (a CAS raising the lease epoch) comes first, right after the opener's own open and
//! before the other source's maintenance open (TLA-048 option (b)). Then, as S4's split recovery
//! does: the evidence (each source's `'T'`, whether a copy began, whether `M` is complete, whether
//! the topology already names the operation), and S6 §7.4's table (`recovery`), acted on with
//! `topology::recover`'s helpers.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::super::fence::{self, Retired};
use super::super::intent::{self, Held, State};
use super::super::model::{Home, LastOp, Snapshot, Topology, born_home_path};
use super::super::store::{self, WriteError};
use crate::shard::ShardEngine;

/// How many times a roll forward re-reads a topology that moved under its CAS.
const PUBLISH_TRIES: usize = 3;

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

/// What a claimant of a lapsed merge intent found, once its own opens had fenced **both** sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Evidence {
    /// Whether each source's log holds this merge's `'T'`.
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
    /// on `last_op`), drop both checkpoints, and end `Reclaimed`.
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

/// Whether the fence's recovery is a merge's: its live intent's kind, or a `'T'` naming one child.
pub(crate) fn is_merge(retired: Option<&Retired>, live: Option<&Held>) -> bool {
    live.is_some_and(|held| held.intent.kind == "merge")
        || retired.is_some_and(|t| t.children.len() == 1)
}

/// One source of the merge, as the claimant holds it: the engine opening, or its log opened for
/// maintenance (never served).
enum Source<'a> {
    Opening(&'a ShardEngine),
    Fenced { db: slatedb::Db, path: String },
}

impl Source<'_> {
    fn db(&self) -> &slatedb::Db {
        match self {
            Self::Opening(engine) => &engine.db,
            Self::Fenced { db, .. } => db,
        }
    }

    /// The source's log path.
    fn path(&self) -> String {
        match self {
            Self::Opening(engine) => engine.log_path(),
            Self::Fenced { path, .. } => path.clone(),
        }
    }
}

/// The merge's identity: its operation, its two sources `(prefix, home)` in prefix order, and the
/// merged leaf's prefix and home.
struct Merge {
    op_id: String,
    sources: [(String, u32); 2],
    prefix: String,
    home: u32,
}

/// The merge a lapsed `live` intent names, or (with none) the one the opening source's `'T'`
/// names, its sources' homes read from `snapshot`.
fn merge_of(snapshot: &Snapshot, retired: Option<&Retired>, live: Option<&Held>) -> Option<Merge> {
    if let Some(held) = live {
        let intent = &held.intent;
        let [a, b] = <[(String, u32); 2]>::try_from(intent.sources.clone()).ok()?;
        let prefix = a.0.strip_suffix('0')?.to_string();
        let home = *intent.children.first()?;
        return Some(Merge {
            op_id: intent.op_id.clone(),
            sources: [a, b],
            prefix,
            home,
        });
    }
    let retired = retired?;
    let (prefix, home) = retired.children.first()?.clone();
    let half = |h: &str| {
        let p = format!("{prefix}{h}");
        let home = snapshot
            .shards
            .homes
            .iter()
            .find(|(_, x)| x.prefix == p)
            .map(|(h, _)| *h)?;
        Some((p, home))
    };
    Some(Merge {
        op_id: retired.op_id.clone(),
        sources: [half("0")?, half("1")?],
        prefix,
        home,
    })
}

/// Recover the lapsed merge the opening `engine` is a source of (see the module).
pub(crate) async fn recover(
    engine: &ShardEngine,
    snapshot: &Snapshot,
    retired: Option<Retired>,
    live: Option<Held>,
) -> Result<(), String> {
    let merge = merge_of(snapshot, retired.as_ref(), live.as_ref())
        .ok_or_else(|| "the merge's sources cannot be read".to_string())?;
    let store = engine.data_store().clone();
    let claimed = claim(engine, &store, live.as_ref()).await?;
    let mut sources = Vec::with_capacity(2);
    for (prefix, home) in &merge.sources {
        if *prefix == engine.prefix {
            sources.push(Source::Opening(engine));
            continue;
        }
        let path = snapshot
            .shards
            .homes
            .get(home)
            .map(|h| h.path.clone())
            .ok_or_else(|| format!("home {home} is not in the topology"))?;
        let db = crate::keyspace::db_builder(path.as_str(), store.clone())
            .build()
            .await
            .map_err(|e| format!("fence {path}: {e}"))?;
        sources.push(Source::Fenced { db, path });
    }
    let acted = decide_and_act(&store, snapshot, &merge, &sources, claimed.as_ref()).await;
    // Every path closes the maintenance open before the verdict: the fence re-reads after a
    // recovery, and maintenance engines must not pile up across its rechecks.
    for source in sources {
        if let Source::Fenced { db, .. } = source {
            drop(db.close().await);
        }
    }
    acted
}

/// The claim of a lapsed `live` intent by the opening `engine` (a CAS raising its lease epoch),
/// made before the other source's maintenance open (TLA-048 option (b), claim-first): a reopen of
/// that source then holds on the fresh claim instead of finding the intent lapsed and fencing the
/// claimant's writer, and a claimant whose CAS lost fences nothing. With no live intent (a `'T'`
/// alone) there is nothing to claim.
async fn claim(
    engine: &ShardEngine,
    store: &Arc<dyn ObjectStore>,
    live: Option<&Held>,
) -> Result<Option<Held>, String> {
    let Some(held) = live else {
        return Ok(None);
    };
    let owner = format!("opener:{}@{}", engine.prefix, engine.writer_epoch);
    super::claim(&**store, held, &owner)
        .await
        .map(Some)
        .map_err(|e| e.to_string())
}

/// With the intent claimed and both sources fenced: the evidence, and the act.
async fn decide_and_act(
    store: &Arc<dyn ObjectStore>,
    snapshot: &Snapshot,
    merge: &Merge,
    sources: &[Source<'_>],
    claimed: Option<&Held>,
) -> Result<(), String> {
    let claimant = Claimant {
        store,
        snapshot,
        merge,
        sources,
        claimed,
    };
    let (evidence, t) = decide(store, merge, sources).await?;
    act(&claimant, evidence, t).await
}

/// The evidence, and the `'T'` row one source holds (to copy into the other on a roll forward).
async fn decide(
    store: &Arc<dyn ObjectStore>,
    merge: &Merge,
    sources: &[Source<'_>],
) -> Result<(Evidence, Option<Retired>), String> {
    let mut retired = [false; 2];
    let mut row = None;
    let mut checkpoints = false;
    for (i, source) in sources.iter().enumerate() {
        if let Some(raw) = source
            .db()
            .get(fence::retired_key())
            .await
            .map_err(|e| format!("'T': {e}"))?
        {
            let t: Retired = serde_json::from_slice(&raw).map_err(|e| format!("'T': {e}"))?;
            retired[i] = t.op_id == merge.op_id;
            if retired[i] {
                row = Some(t);
            }
        }
        checkpoints |= has_checkpoint(store, source, &merge.op_id).await;
    }
    // A published merge is reclaimed whatever `M` holds (`recovery`), so it is read first and
    // `M` is not looked at again once it may serve.
    let published = store::read(&**store)
        .await
        .map(|(topology, _)| topology.last_op.is_some_and(|op| op.op_id == merge.op_id))
        .unwrap_or(false);
    let path = born_home_path(&merge.prefix, merge.home);
    let files = has_files(store, &path).await;
    let merged_complete = !published && files && merged_is_complete(store, &path, merge.home).await;
    let evidence = Evidence {
        retired,
        copy_began: checkpoints || files,
        merged_complete,
        published,
    };
    Ok((evidence, row))
}

/// What the claimant acts with: the store, the snapshot, the merge, both fenced sources and the
/// intent it claimed.
struct Claimant<'a> {
    store: &'a Arc<dyn ObjectStore>,
    snapshot: &'a Snapshot,
    merge: &'a Merge,
    sources: &'a [Source<'a>],
    claimed: Option<&'a Held>,
}

/// Act on S6 §7.4's decision for `evidence`.
async fn act(
    claimant: &Claimant<'_>,
    evidence: Evidence,
    retired: Option<Retired>,
) -> Result<(), String> {
    let Claimant {
        store,
        snapshot,
        merge,
        sources,
        claimed,
    } = *claimant;
    let end = |state: State, result: &'static str| async move {
        if let Some(held) = claimed {
            let ended = super::terminal(held, state, result);
            intent::replace(&**store, held, ended)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok::<(), String>(())
    };
    match recovery(evidence) {
        Recovery::Reclaim => end(State::Reclaimed, "published").await,
        Recovery::RollForward => {
            let t = retired.ok_or("'T' was read but not kept")?;
            for (source, done) in sources.iter().zip(evidence.retired) {
                if !done {
                    let row = serde_json::to_vec(&t).map_err(|e| format!("'T': {e}"))?;
                    source
                        .db()
                        .put(fence::retired_key(), row)
                        .await
                        .map_err(|e| format!("'T': {e}"))?;
                    source.db().flush().await.map_err(|e| format!("'T': {e}"))?;
                }
            }
            publish(&**store, merge).await?;
            drop_checkpoints(store, sources, &merge.op_id).await;
            end(State::Reclaimed, "published").await
        }
        Recovery::RollBack | Recovery::RollBackWithTakeoverGc => {
            super::delete_child(store, &born_home_path(&merge.prefix, merge.home)).await;
            drop_checkpoints(store, sources, &merge.op_id).await;
            delete_merged_lists(&**store, snapshot, merge).await;
            end(State::RolledBack, "rolled_back: the actor's lease lapsed").await
        }
        Recovery::Inconsistent => Err(format!(
            "merge {}: 'T' in a source but M at {} is not complete",
            merge.op_id,
            born_home_path(&merge.prefix, merge.home)
        )),
    }
}

/// Delete `M`'s reference and supplementary lists under every root of either source's lineage.
async fn delete_merged_lists(store: &dyn ObjectStore, snapshot: &Snapshot, merge: &Merge) {
    let mut roots: Vec<u32> = Vec::new();
    for (_, home) in &merge.sources {
        for root in super::lineage_of(&snapshot.shards, *home) {
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
    }
    for root in roots {
        let Some(at) = snapshot.shards.homes.get(&root) else {
            continue;
        };
        for path in [
            crate::spill::lineage::reference_list_path(&at.path, merge.home),
            crate::spill::lineage::supplementary_list_path(&at.path, merge.home),
        ] {
            drop(store.delete(&object_store::path::Path::from(path)).await);
        }
    }
}

/// Publish the merge (idempotent: done once the stored topology names it).
async fn publish(store: &dyn ObjectStore, merge: &Merge) -> Result<(), String> {
    for _ in 0..PUBLISH_TRIES {
        let (topology, etag) = store::read(store).await.map_err(|e| format!("{e:?}"))?;
        if topology
            .last_op
            .as_ref()
            .is_some_and(|op| op.op_id == merge.op_id)
        {
            return Ok(());
        }
        let next = topology.merged(&merge.prefix, merge.home, &merge.op_id)?;
        match store::replace(store, &etag, &next).await {
            Ok(_) => return Ok(()),
            Err(WriteError::Moved) => {}
            Err(WriteError::Store(e)) => return Err(e.to_string()),
        }
    }
    Err(format!("the topology moved {PUBLISH_TRIES} times"))
}

/// Whether the source's log holds the merge's `topo-<op_id>` checkpoint.
async fn has_checkpoint(store: &Arc<dyn ObjectStore>, source: &Source<'_>, op_id: &str) -> bool {
    let path = source.path();
    let admin = slatedb::admin::AdminBuilder::new(path.as_str(), store.clone()).build();
    let name = format!("topo-{op_id}");
    admin
        .list_checkpoints(Some(&name))
        .await
        .is_ok_and(|taken| taken.iter().any(|c| c.name.as_deref() == Some(&*name)))
}

/// Delete the merge's `topo-<op_id>` checkpoint from every source.
async fn drop_checkpoints(store: &Arc<dyn ObjectStore>, sources: &[Source<'_>], op_id: &str) {
    for source in sources {
        let path = source.path();
        let admin = slatedb::admin::AdminBuilder::new(path.as_str(), store.clone()).build();
        let name = format!("topo-{op_id}");
        let Ok(taken) = admin.list_checkpoints(Some(&name)).await else {
            continue;
        };
        for checkpoint in taken
            .into_iter()
            .filter(|c| c.name.as_deref() == Some(&*name))
        {
            drop(admin.delete_checkpoint(checkpoint.id).await);
        }
    }
}

/// Whether anything lies under `path`.
async fn has_files(store: &Arc<dyn ObjectStore>, path: &str) -> bool {
    use futures_util::TryStreamExt;
    let at = object_store::path::Path::from(path);
    let mut listing = store.list(Some(&at));
    listing.try_next().await.ok().flatten().is_some()
}

/// Whether `M`'s log at `path` is complete: its fresh `'Y'` naming `home` is there.
pub(crate) async fn merged_is_complete(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    home: u32,
) -> bool {
    // A reader that writes nothing: a writer's open would bump `M`'s epoch and fence an `M`
    // that already serves (a source's opener rolling forward from `'T'` alone after the publish).
    let reader = slatedb::DbReader::builder(path, store.clone())
        .with_merge_operator(crate::spill::pending::merge_operator())
        .with_reader_mode(slatedb::DbReaderMode::FollowLatest)
        .build()
        .await;
    let Ok(reader) = reader else {
        return false;
    };
    let (key, value) = crate::shard::open_index::identity_row(home);
    let complete = reader
        .get(&key)
        .await
        .ok()
        .flatten()
        .is_some_and(|v| v.as_ref() == value.as_slice());
    drop(reader.close().await);
    complete
}
