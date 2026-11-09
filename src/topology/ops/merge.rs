//! A merge request on this instance's topology loop (S6-DESIGN rev 6 §7, edge change #251): its
//! refusals, its start (answered to the asker), and the rest run as a supervised task of its own,
//! as a split's is.

use std::sync::Arc;

use super::{Accepted, Parts, route_of};
use crate::runtime::Clock;
use crate::shard_directory::{Adoption, ResolveError};
use crate::tasks::{Policy, TaskResult, TaskSupervisor};
use crate::topology::merge::{self, Refused};
use crate::topology::split;

impl Parts {
    /// Start the merge of `prefix`'s halves (S6 §7.2 step 0) and finish it under `tasks`: refused for
    /// halves that are not both leaves, an override on either, a fleet that has not settled
    /// (`fleet_settled`, S4 §10.1), a half another instance holds, or an instance the merge would
    /// fill past `shards_max` (its open shards, both halves among them, plus `M`: the actor holds
    /// all three until the publish); else both are opened here and the merge started.
    pub(super) async fn start_merge(
        &self,
        prefix: &str,
        tasks: &TaskSupervisor,
    ) -> Result<Accepted, Refused> {
        let snapshot = self.dir.prefixes();
        let halves = [format!("{prefix}0"), format!("{prefix}1")];
        if !halves
            .iter()
            .all(|h| snapshot.shards.leaves.contains_key(h))
        {
            return Err(Refused::UnknownPrefix);
        }
        let overrides = self.ownership.overrides();
        if halves.iter().any(|h| overrides.contains_key(h)) {
            return Err(Refused::Busy("override"));
        }
        self.fleet_settled()
            .await
            .map_err(|refused| match refused {
                split::Refused::Busy(reason) => Refused::Busy(reason),
                other => Refused::Unavailable(other.to_string()),
            })?;
        let mut engines = Vec::with_capacity(2);
        for half in &halves {
            let engine = self
                .dir
                .resolve(&route_of(half), Adoption::Internal)
                .await
                .map_err(|error| match error {
                    ResolveError::NotOwner { owner, .. } => Refused::NotOwner(owner),
                    other => Refused::Unavailable(format!("{other:?}")),
                })?;
            engines.push(engine);
        }
        let resident = self.dir.open_count().saturating_add(1);
        if resident > usize::try_from(self.shards_max).unwrap_or(usize::MAX) {
            return Err(Refused::InstanceFull {
                resident,
                max: self.shards_max,
            });
        }
        let op_id = format!(
            "merge-{prefix}-v{}-{}",
            snapshot.shards.version,
            self.clock.now().ms()
        );
        let started =
            merge::start(&self.dir, [&engines[0], &engines[1]], &op_id, &self.owner).await?;
        let accepted = Accepted {
            op_id: started.op_id().to_string(),
            version: started.version(),
        };
        finish(started, self.clock.clone(), tasks);
        Ok(accepted)
    }
}

/// Run the rest of `started` as a supervised task of its own. The supervisor rejects a spawn only
/// while it is stopping; a merge it rejects stays live until an opener of a source claims and rolls
/// it back (S6 §7.4).
fn finish(started: merge::Started, clock: Arc<dyn Clock>, tasks: &TaskSupervisor) {
    let op_id = started.op_id().to_string();
    let spawned = tasks.spawn("topology-merge", Policy::Noncritical, move |_| async move {
        let op_id = started.op_id().to_string();
        match started.finish(&*clock).await {
            Ok(published) => {
                let (prefix, home, _) = &published.merged;
                tracing::info!(%op_id, version = published.version, %prefix, home, "the merge published");
            }
            Err(refused) => tracing::warn!(%op_id, "the merge ended: {refused}"),
        }
        TaskResult::Done
    });
    if spawned.is_err() {
        tracing::warn!(%op_id, "the merge was left to its sources' openers: the supervisor is stopping");
    }
}
