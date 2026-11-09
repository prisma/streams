//! The open-path fence (S4-DESIGN §6.3; S6-REVIEW 1, findings 1, 8, 18): every engine open, after
//! SlateDB's open fenced the previous writer, reads the shard log's retired-parent row `'T'` and
//! the cell's topology intent, and decides whether the engine may serve.
//!
//! | `'T'` | intent naming this incarnation as a source | snapshot vs `'T'` | verdict |
//! |---|---|---|---|
//! | no | none, or terminal | — | serve |
//! | no | live | — | hold |
//! | no | lapsed | — | roll back (the claimant), then serve |
//! | yes | live | snapshot < `T.version` | hold (the actor is between `'T'` and publish) |
//! | yes | lapsed, terminal or absent | snapshot < `T.version` | roll forward (the claimant) |
//! | yes | any | snapshot ≥ `T.version` | retired: the parent never serves again |
//!
//! **A held open waits inside the open** ([`fenced`]): it re-reads the fence every [`RECHECK`] with
//! its collection passes off, and the engine is published only once it may serve. A request meanwhile
//! meets the open gate's retryable `503 shard_opening`, reads included. Past [`FENCE_WAIT`] the open
//! fails and the gate holds the shard off. A store failure on either read holds (fail closed).
//!
//! **The opener is the claimant** (S4-DESIGN §4.3): its open fenced the actor's writer before the
//! first read, so no `'T'` lands after it. A live intent is lapsed once its ETag stayed unchanged
//! across [`intent::LEASE`] + [`intent::SKEW`] of this open's own observations (`intent::Lapse`, on
//! its monotonic clock); the opener then claims it by CAS and rolls the split back or forward
//! (`recover`), and reads the fence again. A claim another claimant won, or a recovery that failed,
//! holds and reads again. With `'T'` and no live intent it rolls forward without a claim: the
//! publish is idempotent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::runtime::Clock;
use crate::shard::ShardEngine;
use crate::topology::intent::{self, Held, Lapse};
use crate::topology::model::Snapshot;
use crate::topology::recover;

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

/// Whether the cell's intent names this incarnation as a source, and whether it lapsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// No intent, a terminal one, or one that names other sources.
    Unnamed,
    Live,
    /// Live, its ETag unchanged across a lapse of this open's observations.
    Lapsed,
}

/// The fence's verdict on an open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Serve,
    Hold,
    RollBack,
    RollForward,
    Retired,
}

/// The table (see the module): `retired` is `'T'`'s version, if the log holds one.
pub(crate) fn decide(retired: Option<u64>, source: Source, snapshot: u64) -> Verdict {
    match (retired, source) {
        (Some(version), _) if snapshot >= version => Verdict::Retired,
        (_, Source::Live) => Verdict::Hold,
        (None, Source::Lapsed) => Verdict::RollBack,
        (None, Source::Unnamed) => Verdict::Serve,
        (Some(_), _) => Verdict::RollForward,
    }
}

/// What one read of the fence found: the log's `'T'`, and the live intent naming this incarnation
/// as a source.
struct Found {
    retired: Option<Retired>,
    live: Option<Held>,
}

/// One read of the fence on `engine` under `snapshot`: one point read of `'T'`, one GET of the
/// intent.
async fn read(engine: &ShardEngine, snapshot: &Snapshot) -> Result<Found, String> {
    let retired = match engine.db.get(retired_key()).await {
        Ok(Some(raw)) => Some(serde_json::from_slice(&raw).map_err(|e| format!("'T': {e}"))?),
        Ok(None) => None,
        Err(e) => return Err(format!("'T': {e}")),
    };
    let held = intent::read(&**engine.data_store())
        .await
        .map_err(|e| e.to_string())?;
    let home = snapshot.shards.leaves.get(&engine.prefix).copied();
    let live = held.filter(|held| {
        let intent = &held.intent;
        !intent.is_terminal()
            && intent
                .sources
                .iter()
                .any(|(prefix, h)| *prefix == engine.prefix && Some(*h) == home)
    });
    Ok(Found { retired, live })
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
/// meanwhile, and recover a lapsed split as its claimant (see the module); past [`FENCE_WAIT`],
/// close it and fail the open.
pub(crate) async fn hold_until_served(
    engine: &ShardEngine,
    snapshot: &SnapshotFn,
) -> anyhow::Result<()> {
    let gate = &engine.gc_state().gate;
    let started = Instant::now();
    let clock = crate::runtime::SystemClock::default();
    let mut lapse: Option<Lapse> = None;
    loop {
        let verdict = match snapshot() {
            Some(snapshot) => match read(engine, &snapshot).await {
                Ok(found) => {
                    let now = clock.monotonic();
                    let verdict = decide_and_recover(engine, &snapshot, found, &mut lapse, now);
                    verdict.await
                }
                Err(error) => {
                    tracing::warn!(shard = %engine.prefix, "the open's fence could not read: {error}");
                    Verdict::Hold
                }
            },
            // The directory is gone: no open is served from it.
            None => Verdict::Hold,
        };
        if verdict == Verdict::Serve {
            // S6 §3.3 (L5): a shard holding an ancestor's rows without its reference list there
            // must not serve, and nothing heals it, so the open fails at once.
            match engine.lineage_refusal().await {
                Ok(None) => {
                    gate.set_collection_off(false);
                    return Ok(());
                }
                Ok(Some(reason)) => {
                    engine.begin_close();
                    tracing::error!(shard = %engine.prefix, "lineage_list_missing: {reason}");
                    anyhow::bail!("lineage_list_missing: {reason}");
                }
                Err(error) => {
                    tracing::warn!(shard = %engine.prefix, "the open's lineage check could not read: {error}");
                }
            }
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

/// The verdict on what the fence `found`, `lapse` the open's observation of the live intent; a
/// roll back or forward is run here, as the claimant (see the module), and the fence holds to read
/// again.
async fn decide_and_recover(
    engine: &ShardEngine,
    snapshot: &Snapshot,
    found: Found,
    lapse: &mut Option<Lapse>,
    now: crate::runtime::MonotonicNow,
) -> Verdict {
    let source = match &found.live {
        None => Source::Unnamed,
        Some(held) => {
            let observed = lapse.get_or_insert_with(|| Lapse::first(&held.etag, now));
            match observed.observe(&held.etag, now) {
                Some(_) => Source::Lapsed,
                None => Source::Live,
            }
        }
    };
    let verdict = decide(
        found.retired.as_ref().map(|t| t.version),
        source,
        snapshot.shards.version,
    );
    let recovered = match verdict {
        Verdict::RollBack | Verdict::RollForward => {
            recover(engine, snapshot, found.retired, found.live).await
        }
        Verdict::Serve | Verdict::Hold | Verdict::Retired => return verdict,
    };
    match recovered {
        Ok(()) => tracing::info!(shard = %engine.prefix, ?verdict, "the opener recovered a split"),
        Err(error) => {
            tracing::warn!(shard = %engine.prefix, ?verdict, "the opener's recovery failed: {error}");
        }
    }
    Verdict::Hold
}

/// The claimant's recovery (`recover`): `live` (lapsed) claimed first, then rolled back without
/// `retired`, else forward from it.
async fn recover(
    engine: &ShardEngine,
    snapshot: &Snapshot,
    retired: Option<Retired>,
    live: Option<Held>,
) -> Result<(), String> {
    if recover::merge::is_merge(retired.as_ref(), live.as_ref()) {
        return recover::merge::recover(engine, snapshot, retired, live).await;
    }
    let store = engine.data_store();
    let owner = format!("opener:{}@{}", engine.prefix, engine.writer_epoch);
    let claimed = match &live {
        Some(held) => Some(
            recover::claim(&**store, held, &owner)
                .await
                .map_err(|e| e.to_string())?,
        ),
        None => None,
    };
    match (retired, claimed) {
        (Some(retired), claimed) => recover::roll_forward(engine, &retired, claimed.as_ref()).await,
        (None, Some(claimed)) => {
            let home = claimed.intent.sources.first().map_or(0, |(_, h)| *h);
            let roots = recover::lineage_of(&snapshot.shards, home);
            let why = format!("claimed by {owner}: the actor's lease lapsed");
            recover::roll_back(engine, &roots, &claimed, &why)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        }
        (None, None) => Ok(()),
    }
}

#[cfg(test)]
mod tests;
