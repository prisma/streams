//! The lineage sweeper's cell loop (S6-DESIGN rev 6 §4 steps 1-2, L6). Once a
//! period, for every retired home of the topology snapshot, one current leaf
//! sweeps the home's root (`ShardEngine::sweep_lineage`). Each process
//! decides from its own snapshot and sweeps only through engines it has open,
//! so the leaf's owner does it.
//!
//! - **When.** Only once this process has held the snapshot's version for
//!   `LINEAGE_GRACE` by its own monotonic clock (`Snapshot::installed_at`). That
//!   grace outlasts every read of a retired incarnation (ASM-HSEG-GRACE). No time
//!   from another node measures it.
//! - **Holders.** The leaves of the version with a reference list under the
//!   home's root, from one LIST of `lineage/refs/`. A list of a home that is not
//!   a leaf belongs to an abandoned attempt or a retired leaf, and is ignored.
//! - **Who.** The holder with the smallest home, or the leaf with the smallest
//!   home when no leaf holds anything there. Two sweepers at once are safe
//!   (every step is idempotent and each decides from one snapshot).
//!
//! - **The live intent.** A home that the cell's unresolved operation names as a
//!   source is never swept. A child it names counts as a holder: its reference
//!   list was written before the operation's CAS, and if the operation publishes,
//!   it is a leaf holding exactly that.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use object_store::ObjectStore;

use crate::runtime::{Clock, MonotonicNow};
use crate::shard_directory::ShardDirectory;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};
use crate::topology::model::Snapshot;

/// How long this process holds a version before it sweeps under it.
pub(crate) const LINEAGE_GRACE: Duration = Duration::from_secs(3600);
/// How often the loop looks (the orphan sweep's cadence).
const PERIOD: Duration = Duration::from_secs(3600);

/// One sweep due: the retired home, its root, the leaf (by prefix) that
/// sweeps it, and the holders whose evidence it reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Job {
    pub(crate) home: u32,
    pub(crate) root: String,
    pub(crate) by: String,
    pub(crate) holders: Vec<u32>,
}

/// What the cell's unresolved topology intent names (empty when there is none,
/// or it is terminal).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Live {
    pub(crate) sources: BTreeSet<u32>,
    pub(crate) children: BTreeSet<u32>,
}

/// Whether `snap`'s version was held here for the grace at `now`.
pub(crate) fn aged(snap: &Snapshot, now: MonotonicNow) -> bool {
    snap.installed_at
        .is_some_and(|at| now.since(at) >= LINEAGE_GRACE)
}

/// The sweeps due under `snap` and the live intent `live` (see the module),
/// given per retired home the homes `listed` with a reference list under its
/// root. Nothing before the version has aged.
pub(crate) fn plan(
    snap: &Snapshot,
    live: &Live,
    listed: &BTreeMap<u32, BTreeSet<u32>>,
    now: MonotonicNow,
) -> Vec<Job> {
    if !aged(snap, now) {
        return Vec::new();
    }
    let leaves: BTreeMap<u32, &String> = snap.shards.leaves.iter().map(|(p, h)| (*h, p)).collect();
    let Some((&first, _)) = leaves.first_key_value() else {
        return Vec::new();
    };
    let mut jobs = Vec::new();
    for (&home, entry) in &snap.shards.homes {
        let swept = entry.retired_version.is_some() && !leaves.contains_key(&home);
        if !swept || live.sources.contains(&home) {
            continue;
        }
        let holders: Vec<u32> = listed
            .get(&home)
            .into_iter()
            .flatten()
            .filter(|h| leaves.contains_key(h) || live.children.contains(h))
            .copied()
            .collect();
        let by = holders
            .iter()
            .find(|h| leaves.contains_key(h))
            .copied()
            .unwrap_or(first);
        let Some(prefix) = leaves.get(&by) else {
            continue;
        };
        jobs.push(Job {
            home,
            root: entry.path.clone(),
            by: (*prefix).clone(),
            holders,
        });
    }
    jobs
}

/// The homes with a reference list under `root` (`<root>/lineage/refs/<h>`).
async fn listed_under(store: &dyn ObjectStore, root: &str) -> object_store::Result<BTreeSet<u32>> {
    let prefix = object_store::path::Path::from(format!("{root}/lineage/refs/"));
    let mut listing = store.list(Some(&prefix));
    let mut homes = BTreeSet::new();
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        if let Some(home) = meta.location.filename().and_then(|n| n.parse::<u32>().ok()) {
            homes.insert(home);
        }
    }
    Ok(homes)
}

/// What the cell's unresolved topology intent names. A terminal intent, or none,
/// names nothing. One that does not decode fails the pass closed.
async fn live_intent(store: &dyn ObjectStore) -> Result<Live, String> {
    let held = crate::topology::intent::read(store)
        .await
        .map_err(|e| e.to_string())?;
    let Some(held) = held.filter(|h| !h.intent.is_terminal()) else {
        return Ok(Live::default());
    };
    Ok(Live {
        sources: held.intent.sources.iter().map(|(_, home)| *home).collect(),
        children: held.intent.children.iter().copied().collect(),
    })
}

/// One pass at `now`: the sweeps this process ran (through engines it has
/// open) and the objects they deleted.
pub(crate) async fn once(
    shards: &ShardDirectory,
    store: &dyn ObjectStore,
    now: MonotonicNow,
) -> Result<(usize, u64), String> {
    let snap = shards.prefixes();
    if !aged(&snap, now) {
        return Ok((0, 0));
    }
    let mut listed = BTreeMap::new();
    for (&home, entry) in &snap.shards.homes {
        if entry.retired_version.is_some() {
            let homes = listed_under(store, &entry.path)
                .await
                .map_err(|e| e.to_string())?;
            listed.insert(home, homes);
        }
    }
    let (mut ran, mut deleted) = (0, 0);
    let live = live_intent(store).await?;
    for job in plan(&snap, &live, &listed, now) {
        let Some(engine) = shards.open(&job.by) else {
            continue;
        };
        let report = engine
            .sweep_lineage(&job.root, &job.holders)
            .await
            .map_err(|e| format!("home {}: {e}", job.home))?;
        ran += 1;
        deleted += report.deleted;
    }
    Ok((ran, deleted))
}

/// Start the loop: one pass each period.
pub(crate) fn start(
    shards: ShardDirectory,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    tasks: &TaskSupervisor,
) {
    if let Err(rejected) = tasks.spawn("lineage-sweep", Policy::Noncritical, move |cancel| {
        run(shards, store, clock, cancel)
    }) {
        tracing::debug!(
            ?rejected,
            "runtime stopping: the lineage sweep was not started"
        );
    }
}

async fn run(
    shards: ShardDirectory,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    cancel: Cancellation,
) -> TaskResult {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(PERIOD) => {}
        }
        match once(&shards, &*store, clock.monotonic()).await {
            Ok((0, _)) => {}
            Ok((ran, deleted)) => tracing::info!(ran, deleted, "lineage sweep"),
            Err(e) => tracing::warn!("lineage sweep stopped: {e}"),
        }
    }
}

#[cfg(test)]
mod tests;
