//! The cell's topology intent, `topology/intent.json` (S4-DESIGN §4): one per cell, leased,
//! never deleted.
//!
//! - **One live operation.** A split (S4) or a merge (S6) starts by writing the intent: a
//!   `Create` when there is none, or a CAS over a terminal one with a fresh `op_id`. A live one
//!   refuses the new operation (`409 topology_busy {reason: "intent"}` at the route).
//! - **Never deleted.** An operation ends by a CAS to a terminal state, `Reclaimed` or
//!   `RolledBack`, carrying the lease epoch it ran under: a stale actor's late write loses its
//!   CAS. The fence and the busy check treat a terminal intent as none. An operation the stored
//!   topology published whose actor never ended it is ended `Reclaimed` by its next reader
//!   (`reclaim`).
//! - **Leased.** The actor renews `lease_until_ms` by CAS every third of [`LEASE`] (3.3 s). A renewal that
//!   fails on the store is retried at the next renewal; only a CAS mismatch (a claimant), its
//!   parent `Db` reporting fenced, or its own monotonic clock passing [`LEASE`] + [`SKEW`] since
//!   its last successful renewal ends its ownership ([`Lease::owned`]).
//! - **A lapse is measured on the observer's own monotonic clock** ([`Lapse`]): the intent's
//!   ETag unchanged across at least [`LEASE`] + [`SKEW`] of the claimant's observations. No
//!   `lease_until_ms` is ever compared with a local wall clock.

use std::time::Duration;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store::{PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::runtime::MonotonicNow;

/// Where the cell's one intent lives.
const INTENT_PATH: &str = "topology/intent.json";
/// The lease an actor holds the intent under.
pub(crate) const LEASE: Duration = Duration::from_secs(10);
/// The clock skew a lapse allows for, on top of the lease.
pub(crate) const SKEW: Duration = Duration::from_secs(1);

/// The intent's state: `Preparing` while its operation runs, then one of the two terminals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum State {
    Preparing,
    Reclaimed,
    RolledBack,
}

/// How a terminal intent ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Outcome {
    /// `published` or `rolled_back`.
    pub result: String,
    pub reason: String,
}

/// The stored intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Intent {
    /// `split` or `merge`.
    pub kind: String,
    pub op_id: String,
    /// The source incarnations, `(prefix, home)`.
    pub sources: Vec<(String, u32)>,
    /// The child homes.
    pub children: Vec<u32>,
    /// The topology version the operation started from.
    pub version_seen: u64,
    pub state: State,
    /// The instance that holds the lease, and the epoch it holds it under (a claim raises it).
    pub lease_owner: String,
    pub lease_epoch: u64,
    /// For display, and as a floor: never compared with a local wall clock.
    pub lease_until_ms: i64,
    pub started_ms: i64,
    pub outcome: Option<Outcome>,
}

impl Intent {
    /// Whether the intent is terminal (`Reclaimed` or `RolledBack`): treated as none.
    pub(crate) fn is_terminal(&self) -> bool {
        self.state != State::Preparing
    }
}

/// The intent as read, with the ETag a CAS over it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Held {
    pub intent: Intent,
    pub etag: String,
}

/// Why a write of the intent did not land.
#[derive(Debug)]
pub(crate) enum IntentError {
    /// A live intent of another operation holds the cell.
    Busy(Box<Intent>),
    /// The ETag moved under the write: another writer (a claimant) won.
    Lost,
    /// The store failed; the write may be retried.
    Store(object_store::Error),
}

impl std::fmt::Display for IntentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(live) => write!(
                f,
                "a live {} intent {} holds the cell",
                live.kind, live.op_id
            ),
            Self::Lost => f.write_str("the intent changed under the write"),
            Self::Store(e) => write!(f, "intent store: {e}"),
        }
    }
}

fn path() -> ObjPath {
    ObjPath::from(INTENT_PATH)
}

fn encode(intent: &Intent) -> Result<PutPayload, IntentError> {
    serde_json::to_vec(intent)
        .map(PutPayload::from)
        .map_err(|e| {
            IntentError::Store(object_store::Error::Generic {
                store: "topology",
                source: Box::new(e),
            })
        })
}

/// A write's ETag, or a store error: a write that stores none cannot be CASed over again.
fn etag_of(result: object_store::PutResult) -> Result<String, IntentError> {
    result.e_tag.filter(|e| !e.is_empty()).ok_or_else(|| {
        IntentError::Store(object_store::Error::Generic {
            store: "topology",
            source: "the intent write returned no ETag".into(),
        })
    })
}

/// The cell's intent, if one was ever written. One that does not decode is an error: it
/// fails closed, as a corrupt topology does.
pub(crate) async fn read(store: &dyn ObjectStore) -> Result<Option<Held>, IntentError> {
    let got = match store.get(&path()).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(IntentError::Store(e)),
    };
    let etag = got.meta.e_tag.clone().unwrap_or_default();
    let raw = got.bytes().await.map_err(IntentError::Store)?;
    let intent = serde_json::from_slice(&raw).map_err(|e| {
        IntentError::Store(object_store::Error::Generic {
            store: "topology",
            source: format!("corrupt topology intent: {e}").into(),
        })
    })?;
    Ok(Some(Held { intent, etag }))
}

/// CAS `intent` over the object at `etag`. A missing ETag never becomes an unconditional PUT.
async fn cas(store: &dyn ObjectStore, etag: &str, intent: &Intent) -> Result<String, IntentError> {
    if etag.is_empty() {
        return Err(IntentError::Lost);
    }
    let mode = PutMode::Update(UpdateVersion {
        e_tag: Some(etag.to_string()),
        version: None,
    });
    match store
        .put_opts(&path(), encode(intent)?, PutOptions::from(mode))
        .await
    {
        Ok(put) => etag_of(put),
        Err(object_store::Error::Precondition { .. }) => Err(IntentError::Lost),
        Err(e) => Err(IntentError::Store(e)),
    }
}

/// Start an operation: write `intent` (`Preparing`) when the cell has no intent, or over a
/// terminal one. A live intent refuses it (`Busy`); a racing writer's win is `Lost`.
pub(crate) async fn create(store: &dyn ObjectStore, intent: &Intent) -> Result<Held, IntentError> {
    let created = store
        .put_opts(&path(), encode(intent)?, PutOptions::from(PutMode::Create))
        .await;
    let etag = match created {
        Ok(put) => etag_of(put)?,
        Err(object_store::Error::AlreadyExists { .. }) => {
            let Some(held) = read(store).await? else {
                return Err(IntentError::Lost);
            };
            if !held.intent.is_terminal() {
                return Err(IntentError::Busy(Box::new(held.intent)));
            }
            cas(store, &held.etag, intent).await?
        }
        Err(e) => return Err(IntentError::Store(e)),
    };
    Ok(Held {
        intent: intent.clone(),
        etag,
    })
}

/// Write `next` over `held` (a renewal, a claim or a terminal transition): the new ETag, or
/// `Lost` when the intent changed since `held` was read.
pub(crate) async fn replace(
    store: &dyn ObjectStore,
    held: &Held,
    next: Intent,
) -> Result<Held, IntentError> {
    let etag = cas(store, &held.etag, &next).await?;
    Ok(Held { intent: next, etag })
}

/// The fleet's reader of the cell's intent (S4-DESIGN §10.1): a GET only when the object's ETag
/// moved since the last read, and the live intent's `op_id` it last found.
#[derive(Debug, Default)]
pub(crate) struct Watch {
    etag: Option<String>,
    live: Option<Live>,
}

/// A live intent as the fleet reads it: its operation, its kind, its actor (the lease owner) and
/// the prefixes it names as its sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Live {
    pub op_id: String,
    pub kind: String,
    pub owner: String,
    pub sources: Vec<String>,
}

/// The `op_id` a watch reports when the intent cannot be read: live, so the fleet does not shrink
/// (no actor and no sources are known).
pub(crate) const UNREADABLE: &str = "unreadable";

impl Live {
    fn of(intent: Intent) -> Self {
        Self {
            op_id: intent.op_id,
            kind: intent.kind,
            owner: intent.lease_owner,
            sources: intent
                .sources
                .into_iter()
                .map(|(prefix, _)| prefix)
                .collect(),
        }
    }
}

impl Watch {
    /// The live intent, if the cell has one: none for an absent or terminal intent, or for one
    /// whose operation the stored topology published, which it ends `Reclaimed` (`reclaim`). A read
    /// that fails, or an intent that does not decode, reports one whose `op_id` is [`UNREADABLE`]
    /// (fail closed) and is read again next time.
    pub(crate) async fn live(&mut self, store: &dyn ObjectStore) -> Option<Live> {
        let opts = object_store::GetOptions {
            if_none_match: self.etag.clone(),
            ..Default::default()
        };
        let read = match store.get_opts(&path(), opts).await {
            Err(object_store::Error::NotModified { .. }) => {
                Ok((self.etag.take(), self.live.take()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((None, None)),
            Err(e) => Err(e.to_string()),
            Ok(got) => {
                let etag = got.meta.e_tag.clone();
                match got.bytes().await {
                    Ok(raw) => serde_json::from_slice::<Intent>(&raw)
                        .map(|intent| (etag, (!intent.is_terminal()).then(|| Live::of(intent))))
                        .map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                }
            }
        };
        (self.etag, self.live) = read.unwrap_or_else(|error| {
            tracing::warn!(%error, "the topology intent could not be read; the fleet holds its size");
            let unknown = Live {
                op_id: UNREADABLE.to_string(),
                kind: String::new(),
                owner: String::new(),
                sources: Vec::new(),
            };
            (None, Some(unknown))
        });
        self.reclaim_published(store).await;
        self.live.clone()
    }

    /// While a live intent is seen: if the stored topology published its operation, end it
    /// `Reclaimed` (`reclaim::read`), and none is live.
    async fn reclaim_published(&mut self, store: &dyn ObjectStore) {
        if self
            .live
            .as_ref()
            .is_none_or(|live| live.op_id == UNREADABLE)
        {
            return;
        }
        if let Ok(Some(held)) = super::reclaim::read(store).await
            && held.intent.is_terminal()
        {
            (self.etag, self.live) = (Some(held.etag), None);
        }
    }
}

/// The actor's hold on its lease: the instant of its last successful renewal, on its own
/// monotonic clock.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Lease {
    renewed: MonotonicNow,
}

impl Lease {
    /// A lease just written (the create, or a renewal that landed) at `now`.
    pub(crate) fn renewed_at(now: MonotonicNow) -> Self {
        Self { renewed: now }
    }

    /// Whether the actor still owns the intent at `now` by its own clock: less than
    /// [`LEASE`] + [`SKEW`] since its last successful renewal. A renewal that failed on the
    /// store does not move the instant and is retried; a CAS mismatch or a fenced parent ends
    /// ownership regardless.
    pub(crate) fn owned(&self, now: MonotonicNow) -> bool {
        elapsed(self.renewed, now) < LEASE + SKEW
    }
}

/// A claimant's view of whether an intent lapsed: the ETag it saw and when it first saw it,
/// on its own monotonic clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Lapse {
    etag: String,
    since: MonotonicNow,
}

impl Lapse {
    /// The first observation of the intent at `etag`, at `now`.
    pub(crate) fn first(etag: &str, now: MonotonicNow) -> Self {
        Self {
            etag: etag.to_string(),
            since: now,
        }
    }

    /// A later observation at `now`: `Some(lapsed_for)` once the ETag has stayed `etag` for at
    /// least [`LEASE`] + [`SKEW`] (how long it has been lapsed), `None` while it has not. An
    /// ETag that moved starts a new observation.
    pub(crate) fn observe(&mut self, etag: &str, now: MonotonicNow) -> Option<Duration> {
        if self.etag != etag {
            *self = Self::first(etag, now);
            return None;
        }
        elapsed(self.since, now).checked_sub(LEASE + SKEW)
    }
}

fn elapsed(from: MonotonicNow, to: MonotonicNow) -> Duration {
    Duration::from_millis(u64::try_from(to.millis() - from.millis()).unwrap_or(0))
}

#[cfg(test)]
mod tests;
