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

/// A started split: its operation, and the topology version it splits from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Accepted {
    pub op_id: String,
    pub version: u64,
}

/// One split request and where its answer goes.
struct Request {
    prefix: String,
    reply: oneshot::Sender<Result<Accepted, Refused>>,
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
            .send(Request { prefix, reply })
            .await
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }
}

/// What the loop starts splits with: the directory, the ring's overrides, the clock, this
/// instance's name (the actor's lease owner) and the shards it may hold (`SHARDS_PER_INSTANCE_MAX`).
pub(crate) struct Parts {
    pub dir: ShardDirectory,
    pub ownership: crate::ownership::OwnershipService,
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

/// The loop: each request started in turn, each started split finished as its own task.
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
        let Some(Request { prefix, reply }) = request else {
            return TaskResult::Done;
        };
        let answer = match parts.start(&prefix).await {
            Ok(started) => {
                let accepted = Accepted {
                    op_id: started.op_id().to_string(),
                    version: started.version(),
                };
                finish(started, parts.clock.clone(), &tasks);
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
    /// Start the split of leaf `prefix` (S4-DESIGN §6.2 step 0): refused for no leaf, an override on
    /// it, another instance holding it, or an instance the split would fill past `shards_max` (its
    /// open shards, the parent among them, plus one: the parent retires and two children open,
    /// S4-DESIGN §10); else the parent is opened here and the split started.
    async fn start(&self, prefix: &str) -> Result<split::Started, Refused> {
        let snapshot = self.dir.prefixes();
        if !snapshot.shards.leaves.contains_key(prefix) {
            return Err(Refused::UnknownPrefix);
        }
        if self.ownership.overrides().contains_key(prefix) {
            return Err(Refused::Busy("override"));
        }
        let parent = self
            .dir
            .resolve(&route_of(prefix), Adoption::Internal)
            .await
            .map_err(|error| match error {
                ResolveError::NotOwner { owner, .. } => Refused::NotOwner(owner),
                other => Refused::Unavailable(format!("{other:?}")),
            })?;
        let resident = self.dir.open_count().saturating_add(1);
        if resident > usize::try_from(self.shards_max).unwrap_or(usize::MAX) {
            return Err(Refused::InstanceFull {
                resident,
                max: self.shards_max,
            });
        }
        let op_id = format!(
            "split-{prefix}-v{}-{}",
            snapshot.shards.version,
            self.clock.now().ms()
        );
        split::start(&self.dir, &parent, &op_id, &self.owner).await
    }
}

/// The first route hash of `prefix`'s range: its bits, then zeros.
fn route_of(prefix: &str) -> [u8; 16] {
    let bits = u128::from_str_radix(prefix, 2).unwrap_or(0);
    let width = u32::try_from(prefix.len()).unwrap_or(128);
    bits.checked_shl(128 - width.min(128))
        .unwrap_or(0)
        .to_be_bytes()
}

#[cfg(test)]
mod tests;
