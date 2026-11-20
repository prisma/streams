//! The cell's topology operations on this instance (S4-DESIGN §11): one supervised loop takes each
//! split request and starts it (the refusals, the home reservation and the intent, answered to the
//! asker), then runs the rest of each started split as a supervised task of its own, so a split
//! outlives the request, or the scaler pass, that asked for it. The directory holds only the
//! requests' sender ([`Handle`]), never the supervisor (`crate::tasks`: a state that holds its
//! supervisor makes a runtime that failed to start immortal).

use std::sync::{Arc, OnceLock};

use tokio::sync::{mpsc, oneshot};

use super::split::{self, Refused};
use crate::runtime::Clock;
use crate::shard_directory::{Adoption, ResolveError, ShardDirectory};
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// How many split requests may wait for the loop.
const QUEUED: usize = 16;
/// How old the fleet's published count must be before a split starts (S4-DESIGN §10.1,
/// `SHARD_TOPOLOGY_COOLDOWN_SECS`): the topology waits for the fleet, never the reverse.
const FLEET_SETTLE_MS: i64 = 60_000;

/// A started split: its operation, and the topology version it splits from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Accepted {
    pub op_id: String,
    pub version: u64,
}

/// One request and where its answer goes: a split of a leaf, or a merge of a prefix's halves.
enum Request {
    Split {
        prefix: String,
        reply: oneshot::Sender<Result<Accepted, Refused>>,
    },
    Merge {
        prefix: String,
        reply: oneshot::Sender<Result<Accepted, super::merge::Refused>>,
    },
}

/// The way to this instance's topology loop, once it started (`start`).
#[derive(Clone, Default)]
pub(crate) struct Handle(Arc<OnceLock<mpsc::Sender<Request>>>);

impl Handle {
    /// Ask the loop to split the leaf `prefix`: `Accepted` once the split started (it then runs on),
    /// else why not. `Unavailable` when no loop runs here, or it stopped.
    pub(crate) async fn split(&self, prefix: String) -> Result<Accepted, Refused> {
        let stopped = || Refused::Unavailable("the topology loop is not running".to_string());
        let sender = self.0.get().ok_or_else(stopped)?;
        let (reply, answer) = oneshot::channel();
        sender
            .send(Request::Split { prefix, reply })
            .await
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// Ask the loop to merge `prefix`'s halves: `Accepted` once the merge started (it then runs on),
    /// else why not. `Unavailable` when no loop runs here, or it stopped.
    pub(crate) async fn merge(&self, prefix: String) -> Result<Accepted, super::merge::Refused> {
        use super::merge::Refused as M;
        let stopped = || M::Unavailable("the topology loop is not running".to_string());
        let sender = self.0.get().ok_or_else(stopped)?;
        let (reply, answer) = oneshot::channel();
        sender
            .send(Request::Merge { prefix, reply })
            .await
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }
}

/// What the loop starts splits with: the directory (whose resolve applies the ring and its
/// overrides), the fleet's documents, this instance's ownership view (its splits' children are born
/// into it) and the fleet's move cooldown, the clock, this instance's name (the actor's lease owner)
/// and the shards it may hold (`SHARDS_PER_INSTANCE_MAX`).
pub(crate) struct Parts {
    pub dir: ShardDirectory,
    pub fleet: crate::fleet::FleetRepository,
    /// This instance's ownership view, which a split's children are born into (`split::Birth`).
    pub ownership: crate::ownership::OwnershipService,
    pub move_cooldown_ms: i64,
    pub clock: Arc<dyn Clock>,
    pub owner: String,
    pub shards_max: u32,
}

/// Start this instance's topology loop under `tasks`, reached through the directory's handle. A
/// second start leaves the first loop.
#[expect(
    clippy::let_underscore_must_use,
    reason = "start; the supervisor rejects a spawn only while it is stopping, when no topology operation is owed; a rejected loop has nothing left to run"
)]
pub(crate) fn start(parts: Parts, tasks: &TaskSupervisor) {
    let (sender, requests) = mpsc::channel(QUEUED);
    if parts.dir.topology_ops().0.set(sender).is_err() {
        return;
    }
    let spawner = tasks.clone();
    let _ = tasks.spawn("topology-ops", Policy::Noncritical, move |cancel| {
        run(parts, requests, spawner, cancel)
    });
}

/// The loop: each request started in turn, each started split finished as its own task. A
/// cancellation drops a start where it stands, as the supervisor's grace would abort it
/// (Addendum 22): every state a start leaves has its owner, the directory's open task for an open
/// and a source's opener (`recover::claim`) for a written intent, and the operation itself runs as
/// its own task from its intent on.
async fn run(
    parts: Parts,
    mut requests: mpsc::Receiver<Request>,
    tasks: TaskSupervisor,
    cancel: Cancellation,
) -> TaskResult {
    loop {
        let request = tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            request = requests.recv() => request,
        };
        let Some(request) = request else {
            return TaskResult::Done;
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return TaskResult::Done,
            () = parts.handle(request, &tasks) => {}
        }
    }
}

impl Parts {
    /// One request started, and answered.
    async fn handle(&self, request: Request, tasks: &TaskSupervisor) {
        let (prefix, reply) = match request {
            Request::Split { prefix, reply } => (prefix, reply),
            Request::Merge { prefix, reply } => {
                // The asker may have gone away: the merge runs on regardless.
                drop(reply.send(self.start_merge(&prefix, tasks).await));
                return;
            }
        };
        let answer = match self.start(&prefix).await {
            Ok(started) => {
                let accepted = Accepted {
                    op_id: started.op_id().to_string(),
                    version: started.version(),
                };
                finish(started, self.clock.clone(), tasks);
                Ok(accepted)
            }
            Err(refused) => Err(refused),
        };
        // The asker may have gone away: the split runs on regardless.
        drop(reply.send(answer));
    }
}

/// Run the rest of `started` as a supervised task of its own.
#[expect(
    clippy::let_underscore_must_use,
    reason = "finish; the supervisor rejects a spawn only while it is stopping; a split it rejects stays live until an opener of its parent claims and rolls it back (S4-DESIGN §6.4)"
)]
fn finish(started: split::Started, clock: Arc<dyn Clock>, tasks: &TaskSupervisor) {
    let _ = tasks.spawn("topology-split", Policy::Noncritical, move |_| async move {
        let op_id = started.op_id().to_string();
        match started.finish(&*clock).await {
            Ok(published) => {
                let children: Vec<&str> = published.children.iter().map(|(p, _, _)| p.as_str()).collect();
                let pause_ms = published.pause.as_millis();
                tracing::info!(%op_id, version = published.version, ?children, pause_ms, "the split published");
            }
            Err(refused) => tracing::warn!(%op_id, "the split ended: {refused}"),
        }
        TaskResult::Done
    });
}

impl Parts {
    /// Start the split of leaf `prefix` (S4-DESIGN §6.2 step 0): refused for no leaf, a fleet that
    /// has not settled (`fleet_settled`: a move younger than its cooldown among them), another
    /// instance holding it (an override placing it elsewhere among them), or an instance the split
    /// would fill past `shards_max` at its peak (its open shards, the parent among them, plus two:
    /// the parent stays open beside both children until the publish, S4-DESIGN §10, #261); else the
    /// parent is opened here and the split started. An override placing the leaf here does not
    /// refuse it: count balance moves leaves by overrides, and a moved leaf must still split
    /// (#251 amendment R3, as S6's B4 for the merge).
    async fn start(&self, prefix: &str) -> Result<split::Started, Refused> {
        let snapshot = self.dir.prefixes();
        if !snapshot.shards.leaves.contains_key(prefix) {
            return Err(Refused::UnknownPrefix);
        }
        self.fleet_settled().await?;
        let parent = self
            .dir
            .resolve(&route_of(prefix), Adoption::Internal)
            .await
            .map_err(|error| match error {
                ResolveError::NotOwner { owner, .. } => Refused::NotOwner(owner),
                other => Refused::Unavailable(format!("{other:?}")),
            })?;
        room_for_split(self.dir.open_count(), self.shards_max)?;
        let op_id = format!(
            "split-{prefix}-v{}-{}",
            snapshot.shards.version,
            self.clock.now().ms()
        );
        let birth = split::Birth::new(
            self.fleet.clone(),
            self.ownership.clone(),
            self.owner.clone(),
        );
        let started = split::start(&self.dir, &parent, &op_id, &self.owner).await?;
        Ok(started.born_by(birth))
    }
}

impl Parts {
    /// Whether the fleet settled (S4-DESIGN §10.1): its published count at least
    /// [`FLEET_SETTLE_MS`] old and no override younger than the move cooldown, both by the wall
    /// clock the fleet's documents are stamped in; else `topology_busy {reason: "fleet"}`. A cell
    /// without the fleet has nothing to wait for; an unreadable document is `Unavailable`.
    async fn fleet_settled(&self) -> Result<(), Refused> {
        if !self.fleet.enabled() {
            return Ok(());
        }
        let unread = |e: anyhow::Error| Refused::Unavailable(format!("fleet: {e}"));
        let now = crate::shard::now_ms();
        let (desired, _) = self.fleet.read_desired_state().await.map_err(unread)?;
        let (overrides, _) = self.fleet.read_overrides().await.map_err(unread)?;
        let counted_at = desired.map(|d| d.computed_at_ms);
        let moved_at = overrides.entries.values().map(|entry| entry.ms);
        if settled(now, counted_at, moved_at, self.move_cooldown_ms) {
            Ok(())
        } else {
            Err(Refused::Busy("fleet"))
        }
    }
}

/// Room for a split on an instance holding `open` shards: the parent open beside both children,
/// at most `shards_max`; else `instance_full`.
fn room_for_split(open: usize, shards_max: u32) -> Result<(), Refused> {
    let resident = open.saturating_add(2);
    if resident > usize::try_from(shards_max).unwrap_or(usize::MAX) {
        return Err(Refused::InstanceFull {
            resident,
            max: shards_max,
        });
    }
    Ok(())
}

/// Whether the fleet settled at `now`: its count, published at `counted_at`, at least
/// [`FLEET_SETTLE_MS`] old, and no override, each stamped at its `moved_at`, younger than
/// `cooldown_ms`.
fn settled(
    now: i64,
    counted_at: Option<i64>,
    mut moved_at: impl Iterator<Item = i64>,
    cooldown_ms: i64,
) -> bool {
    let young_count = counted_at.is_some_and(|at| now.saturating_sub(at) < FLEET_SETTLE_MS);
    let young_move = moved_at.any(|at| now.saturating_sub(at) < cooldown_ms);
    !(young_count || young_move)
}

/// The first route hash of `prefix`'s range: its bits, then zeros.
pub(crate) fn route_of(prefix: &str) -> [u8; 16] {
    let bits = u128::from_str_radix(prefix, 2).unwrap_or(0);
    let width = u32::try_from(prefix.len()).unwrap_or(128);
    bits.checked_shl(128 - width.min(128))
        .unwrap_or(0)
        .to_be_bytes()
}

// The merge's requests (S6 §7): its refusals, its start and its supervised finish.
mod merge;
#[cfg(test)]
mod tests;
