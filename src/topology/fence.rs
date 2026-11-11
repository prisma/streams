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
//! **An open of a source is held before it opens** (TLA-048 option (b), change 1): while a live
//! intent names the prefix as a source and is fresh by the opener's own observations, the open
//! waits without opening the log, so its writer-epoch bump never fences a live actor or a
//! claimant's maintenance writer; it opens once the intent is absent, terminal or lapsed, and the
//! table below runs after the open as before. A claimant's maintenance open of its other source
//! opens the log directly and is never held here.
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

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use object_store::ObjectStore;

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
async fn read(engine: &ShardEngine, snapshot: &Snapshot) -> Result<Found, ReadFailure> {
    let retired = match engine.db.get(retired_key()).await {
        Ok(Some(raw)) => Some(
            serde_json::from_slice(&raw).map_err(|e| ReadFailure::Other(format!("'T': {e}")))?,
        ),
        Ok(None) => None,
        Err(e) if e.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) => {
            return Err(ReadFailure::Fenced);
        }
        Err(e) => return Err(ReadFailure::Other(format!("'T': {e}"))),
    };
    let held = intent::read(&**engine.data_store())
        .await
        .map_err(|e| ReadFailure::Other(e.to_string()))?;
    let live = held.filter(|held| names_source(&held.intent, &engine.prefix, snapshot));
    Ok(Found { retired, live })
}

/// Why a read of the fence failed: the engine's log was opened by a newer writer (a claimant's
/// maintenance open of a sibling mid-open, say), which no later read on this engine survives; or
/// anything else, which the fence holds on and reads again.
enum ReadFailure {
    Fenced,
    Other(String),
}

/// How a held open ended without failing: the engine may serve, or a newer writer fenced it
/// (`ReadFailure::Fenced`) and it was closed, so the open re-enters the hold before the open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldEnd {
    Served,
    Fenced,
}

/// Whether `intent` is live and names `prefix`, at its leaf home in `snapshot`, as a source.
fn names_source(intent: &intent::Intent, prefix: &str, snapshot: &Snapshot) -> bool {
    let home = snapshot.shards.leaves.get(prefix).copied();
    !intent.is_terminal()
        && intent
            .sources
            .iter()
            .any(|(p, h)| p == prefix && Some(*h) == home)
}

/// The directory's topology as the fence reads it at each check.
pub(crate) type SnapshotFn = Arc<dyn Fn() -> Option<Arc<Snapshot>> + Send + Sync>;

/// `open`, fenced: it opens the log at the path the topology gives the prefix's home (its static
/// path before any topology is known), and the engine it opens is returned only once the fence
/// lets it serve (see the module). A prefix that is no leaf of the topology is refused
/// `not_a_leaf` without opening anything (#251 amendment R3-d).
pub(crate) fn fenced(
    open: crate::sharddir::LogOpener,
    snapshot: SnapshotFn,
) -> crate::sharddir::OpenFn {
    let open = Arc::new(open.0);
    // The cell's data store, which holds the intent: every engine this opener opens carries it, so
    // it is known from the first open on, and every later open is held before it opens. Stated
    // limit: a process's first open is not held, so it can fence a fresh claimant or actor once
    // per process start (bootstrap::run, which builds the opener and holds the store, is frozen);
    // restarts are finite, so the liveness argument (TLA-048 option (b)) holds.
    let intents: Arc<OnceLock<Arc<dyn ObjectStore>>> = Arc::default();
    Box::new(move |prefix, incarnation| {
        let (open, snapshot, intents) = (open.clone(), snapshot.clone(), intents.clone());
        Box::pin(async move { open_held(&open, prefix, incarnation, &snapshot, &intents).await })
    })
}

/// The fenced open of `prefix` (see [`fenced`]): the hold before the open, the open, the hold after
/// it. An engine a newer writer fenced while it was held (the lead's option (a), 2026-11-10: a
/// claimant's maintenance open of a sibling mid-open) is closed, its termination awaited (so its
/// close notification, carrying this incarnation, lands before any replacement is published), and
/// the open re-enters the hold before the open, which waits on the claimant's fresh claim and
/// reopens once the intent ended or lapsed; all within twice [`FENCE_WAIT`].
async fn open_held(
    open: &crate::sharddir::OpenAt,
    prefix: String,
    incarnation: crate::sharddir::EngineIncarnation,
    snapshot: &SnapshotFn,
    intents: &OnceLock<Arc<dyn ObjectStore>>,
) -> anyhow::Result<Arc<ShardEngine>> {
    let started = Instant::now();
    loop {
        let lapse = match intents.get() {
            Some(store) => hold_before_open(&prefix, snapshot, &**store).await?,
            None => None,
        };
        // A prefix that is no leaf has no log of its own to open: its static path would be a new,
        // empty one (#251 amendment R3-d).
        let held = snapshot();
        if held
            .as_deref()
            .is_some_and(|held| !held.shards.leaves.contains_key(&prefix))
        {
            anyhow::bail!("not_a_leaf: {prefix}");
        }
        let path = log_path(held.as_deref(), &prefix);
        let engine = open(prefix.clone(), path.clone(), incarnation).await?;
        intents.get_or_init(|| engine.data_store().clone());
        engine.place(path, snapshot.clone());
        match hold_until_served(&engine, snapshot, lapse).await? {
            HoldEnd::Served => return Ok(engine),
            HoldEnd::Fenced => {
                drop(engine.shutdown_handle().wait(FENCE_WAIT).await);
                if started.elapsed() >= FENCE_WAIT * 2 {
                    anyhow::bail!("fenced while held, again past {:?}", FENCE_WAIT * 2);
                }
                tracing::info!(shard = %prefix, "a newer writer fenced the held open; it reopens");
            }
        }
    }
}

/// The hold before the open (TLA-048 option (b), change 1): while a live intent names `prefix` as
/// a source and is fresh by this opener's own observations (`intent::Lapse`), the open waits
/// without opening the log, so it fences no writer of a live actor or of a claimant (whose
/// maintenance open of its other source opens the log directly, never through here). It opens once
/// the intent is absent, terminal, names other sources, or lapsed, and hands the fence its
/// observation, so a lapsed intent is claimed at once. A read that fails holds (fail closed); past
/// [`FENCE_WAIT`] the open fails.
async fn hold_before_open(
    prefix: &str,
    snapshot: &SnapshotFn,
    store: &dyn ObjectStore,
) -> anyhow::Result<Option<Lapse>> {
    let started = Instant::now();
    let clock = crate::runtime::SystemClock::default();
    let mut lapse: Option<Lapse> = None;
    loop {
        let read = intent::read(store).await;
        match (snapshot(), read) {
            (Some(snapshot), Ok(held)) => {
                let Some(held) = held.filter(|h| names_source(&h.intent, prefix, &snapshot)) else {
                    return Ok(lapse);
                };
                let now = clock.monotonic();
                let observed = lapse.get_or_insert_with(|| Lapse::first(&held.etag, now));
                if observed.observe(&held.etag, now).is_some() {
                    return Ok(lapse);
                }
            }
            (None, _) => {}
            (Some(_), Err(error)) => {
                tracing::warn!(shard = %prefix, "the open's pre-open hold could not read: {error}");
            }
        }
        if started.elapsed() >= FENCE_WAIT {
            anyhow::bail!("held before its open by a live intent for {FENCE_WAIT:?}");
        }
        tracing::info!(shard = %prefix, "a live intent holds the open before it opens");
        tokio::time::sleep(RECHECK).await;
    }
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
/// close it and fail the open. `lapse` is the observation of the live intent the hold before the
/// open made, if any. A read a newer writer fenced closes the engine and answers [`HoldEnd::Fenced`].
pub(crate) async fn hold_until_served(
    engine: &ShardEngine,
    snapshot: &SnapshotFn,
    lapse: Option<Lapse>,
) -> anyhow::Result<HoldEnd> {
    let gate = &engine.gc_state().gate;
    let started = Instant::now();
    let clock = crate::runtime::SystemClock::default();
    let mut lapse = lapse;
    loop {
        let verdict = match snapshot() {
            Some(snapshot) => match read(engine, &snapshot).await {
                Ok(found) => {
                    let now = clock.monotonic();
                    let verdict = decide_and_recover(engine, &snapshot, found, &mut lapse, now);
                    verdict.await
                }
                Err(ReadFailure::Fenced) => {
                    engine.begin_close();
                    return Ok(HoldEnd::Fenced);
                }
                Err(ReadFailure::Other(error)) => {
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
                    return Ok(HoldEnd::Served);
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
