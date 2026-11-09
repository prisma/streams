//! The open-path fence (S4-DESIGN §6.3; S6-REVIEW 1, findings 1, 8, 18): every engine open, after
//! SlateDB's open fenced the previous writer, reads the shard log's retired-parent row `'T'` and
//! the cell's topology intent, and decides whether the engine may serve.
//!
//! | `'T'` | intent naming this incarnation as a source | snapshot vs `'T'` | verdict |
//! |---|---|---|---|
//! | no | none, or terminal | — | serve |
//! | no | live | — | hold |
//! | no | lapsed | — | roll back (the claimant), then serve: with the claim, S4 step 11 |
//! | yes | live | snapshot < `T.version` | hold (the actor is between `'T'` and publish) |
//! | yes | lapsed, terminal or absent | snapshot < `T.version` | roll forward (the claimant) |
//! | yes | any | snapshot ≥ `T.version` | retired: the parent never serves again |
//!
//! **A held open waits inside the open** ([`fenced`]): it re-reads the fence every [`RECHECK`] with
//! its collection passes off, and the engine is published only once it may serve. A request meanwhile
//! meets the open gate's retryable `503 shard_opening`, reads included. Past [`FENCE_WAIT`] the open
//! fails and the gate holds the shard off. Until the claimant's recovery lands (S4 step 11), every
//! verdict but `Serve` holds; a store failure on either read holds too (fail closed).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::shard::ShardEngine;
use crate::topology::model::Snapshot;

/// How often a held open re-reads the fence.
pub(crate) const RECHECK: Duration = Duration::from_secs(1);
/// How long an open waits on the fence before it fails (provisional, far inside the open deadline).
pub(crate) const FENCE_WAIT: Duration = Duration::from_secs(60);

/// The retired-parent row's sentinel tag (`keyspace::SENTINEL_TAGS`).
const RETIRED_TAG: u8 = b'T';

/// The retired-parent row `'T'`: written by a split's parent just before its topology is published
/// (S4 step 11). From it the operation only rolls forward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Retired {
    /// The topology version the split publishes.
    pub version: u64,
    pub op_id: String,
    /// The children, `(prefix, home)`.
    pub children: Vec<(String, u32)>,
}

/// The `'T'` row's key.
pub(crate) fn retired_key() -> Vec<u8> {
    let mut key = crate::keyspace::SENTINEL.to_vec();
    key.push(RETIRED_TAG);
    key
}

/// Whether the cell's intent names this incarnation as a source. A lapsed one (`intent::Lapse`)
/// comes with the claim (S4 step 11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// No intent, a terminal one, or one that names other sources.
    Unnamed,
    Live,
}

/// The fence's verdict on an open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Serve,
    Hold,
    RollForward,
    Retired,
}

/// The table (see the module): `retired` is `'T'`'s version, if the log holds one.
pub(crate) fn decide(retired: Option<u64>, source: Source, snapshot: u64) -> Verdict {
    match retired {
        Some(version) if snapshot >= version => Verdict::Retired,
        _ if source == Source::Live => Verdict::Hold,
        None => Verdict::Serve,
        Some(_) => Verdict::RollForward,
    }
}

/// The verdict on `engine` under `snapshot`: one point read of `'T'`, one GET of the intent.
pub(crate) async fn check(engine: &ShardEngine, snapshot: &Snapshot) -> Result<Verdict, String> {
    let retired = match engine.db.get(retired_key()).await {
        Ok(Some(raw)) => {
            let row: Retired = serde_json::from_slice(&raw).map_err(|e| format!("'T': {e}"))?;
            Some(row.version)
        }
        Ok(None) => None,
        Err(e) => return Err(format!("'T': {e}")),
    };
    let held = crate::topology::intent::read(&**engine.data_store())
        .await
        .map_err(|e| e.to_string())?;
    let home = snapshot.shards.leaves.get(&engine.prefix).copied();
    let named = held.is_some_and(|held| {
        let intent = held.intent;
        !intent.is_terminal()
            && intent
                .sources
                .iter()
                .any(|(prefix, h)| *prefix == engine.prefix && Some(*h) == home)
    });
    let source = if named { Source::Live } else { Source::Unnamed };
    Ok(decide(retired, source, snapshot.shards.version))
}

/// The directory's topology as the fence reads it at each check.
pub(crate) type SnapshotFn = Arc<dyn Fn() -> Option<Arc<Snapshot>> + Send + Sync>;

/// `open`, fenced: it opens the log at the path the topology gives the prefix's home (its static
/// path for a prefix that is no leaf), and the engine it opens is returned only once the fence lets
/// it serve (see the module).
pub(crate) fn fenced(
    open: crate::sharddir::LogOpener,
    snapshot: SnapshotFn,
) -> crate::sharddir::OpenFn {
    let open = Arc::new(open.0);
    Box::new(move |prefix, incarnation| {
        let (open, snapshot) = (open.clone(), snapshot.clone());
        Box::pin(async move {
            let path = log_path(snapshot().as_deref(), &prefix);
            let engine = open(prefix, path.clone(), incarnation).await?;
            engine.place(path, snapshot.clone());
            hold_until_served(&engine, &snapshot).await?;
            Ok(engine)
        })
    })
}

/// The path of `prefix`'s log: its leaf home's in `snapshot`, else its static path.
pub(crate) fn log_path(snapshot: Option<&Snapshot>, prefix: &str) -> String {
    snapshot
        .and_then(|s| s.shards.homes.get(s.shards.leaves.get(prefix)?))
        .map_or_else(
            || crate::sharddir::shard_db_path(prefix),
            |home| home.path.clone(),
        )
}

/// Re-read the fence every [`RECHECK`] until it lets `engine` serve, its collection passes off
/// meanwhile; past [`FENCE_WAIT`], close it and fail the open.
async fn hold_until_served(engine: &ShardEngine, snapshot: &SnapshotFn) -> anyhow::Result<()> {
    let gate = &engine.gc_state().gate;
    let started = Instant::now();
    loop {
        let verdict = match snapshot() {
            Some(snapshot) => check(engine, &snapshot).await.unwrap_or_else(|error| {
                tracing::warn!(shard = %engine.prefix, "the open's fence could not read: {error}");
                Verdict::Hold
            }),
            // The directory is gone: no open is served from it.
            None => Verdict::Hold,
        };
        if verdict == Verdict::Serve {
            gate.set_collection_off(false);
            return Ok(());
        }
        gate.set_collection_off(true);
        if started.elapsed() >= FENCE_WAIT {
            engine.begin_close();
            anyhow::bail!("held by a split ({verdict:?}) for {FENCE_WAIT:?}");
        }
        tracing::info!(shard = %engine.prefix, ?verdict, "a split holds the open");
        tokio::time::sleep(RECHECK).await;
    }
}

#[cfg(test)]
mod tests;
