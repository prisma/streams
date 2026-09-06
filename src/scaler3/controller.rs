//! One bounded scaler turn. Volatile hints rotate fairly; a transition's
//! persisted `pending` intent is the recovery authority once phase A commits.
use crate::application::topology::{self, TopologyService};
use crate::tasks::Cancellation;
use crate::tenant::TenantStreamRef;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

const WORK_PER_PASS: usize = 16;
const DECISION_DEADLINE: Duration = Duration::from_secs(45);
pub(super) const PASS_DEADLINE: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) enum Decision {
    Split(TenantStreamRef, String, u32, u64),
    Merge(TenantStreamRef, String),
}
impl Decision {
    fn key(&self) -> (TenantStreamRef, String) {
        match self {
            Self::Split(name, epoch, ..) | Self::Merge(name, epoch) => {
                (name.clone(), epoch.clone())
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PassReport {
    pub attempted: usize,
    pub completed: usize,
    pub deferred: usize,
    pub cancelled: bool,
}

pub(crate) struct Controller {
    topology: TopologyService,
    ops: Arc<crate::ops::OpsService>,
    cooldown_secs: i64,
    pending: VecDeque<Decision>,
    queued: HashSet<(TenantStreamRef, String)>,
}
impl Controller {
    pub(crate) fn new(
        topology: TopologyService,
        ops: Arc<crate::ops::OpsService>,
        cooldown_secs: i64,
    ) -> Self {
        Self {
            topology,
            ops,
            cooldown_secs,
            pending: VecDeque::new(),
            queued: HashSet::new(),
        }
    }

    /// At most one pending hint per incarnation; stale or refused hints can be
    /// recomputed by the next evaluation. Never accumulate unbounded traffic.
    pub(crate) fn enqueue(&mut self, work: Decision) {
        if self.pending.len() < super::SKETCH_MAX && self.queued.insert(work.key()) {
            self.pending.push_back(work);
        }
    }

    pub(crate) async fn pass(
        &mut self,
        cancel: &Cancellation,
        deadline: tokio::time::Instant,
    ) -> PassReport {
        let mut report = PassReport::default();
        let count = self.pending.len().min(WORK_PER_PASS);
        for _ in 0..count {
            if cancel.is_cancelled() {
                report.cancelled = true;
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let Some(work) = self.pending.pop_front() else {
                break;
            };
            report.attempted += 1;
            // After phase A, cancelling the operation leaves descriptor intent
            // and idempotent parent closure intact. No destructive work runs
            // before that intent. Unknown completion stays queued for retry.
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => None,
                result = tokio::time::timeout_at(
                    deadline.min(tokio::time::Instant::now() + DECISION_DEADLINE),
                    self.execute(&work),
                ) => Some(result),
            };
            let Some(result) = result else {
                self.pending.push_front(work);
                report.cancelled = true;
                break;
            };
            match result {
                Ok(Some(completed)) => {
                    self.queued.remove(&work.key());
                    report.completed += usize::from(completed);
                }
                Ok(None) | Err(_) => self.pending.push_back(work),
            }
        }
        report.deferred = self.pending.len();
        report
    }

    /// Some(false) means a confirmed stale/inapplicable hint; None is debt.
    async fn execute(&self, work: &Decision) -> Option<bool> {
        let (name, epoch) = work.key();
        let desc = match self.topology.registry.get(&name).await {
            Ok(Some(desc)) if desc.stream_epoch == epoch => desc,
            Ok(_) => return Some(false),
            Err(_) => return None,
        };
        let completed = match work {
            Decision::Split(_, _, seg_id, split_at) => {
                let done = topology::execute_split_fenced(
                    &self.topology,
                    &name,
                    &epoch,
                    *seg_id,
                    *split_at,
                )
                .await;
                if done {
                    self.ops.emit(crate::ops::OpsEvent::new("split_committed", format!("split/{epoch}/{seg_id}"))
                        .stream(&name, &epoch)
                        .fields(serde_json::json!({"segId":seg_id,"splitAt":split_at,"projectId":name.project_id().as_str()})));
                }
                done
            }
            Decision::Merge(..) => {
                // Existing pending intent takes precedence over a new hint.
                if desc
                    .segments
                    .as_ref()
                    .is_some_and(|map| map.pending.is_some())
                {
                    topology::resume_fenced(&self.topology, &name, &epoch).await
                } else {
                    self.merge(&desc).await?
                }
            }
        };
        if completed {
            return Some(true);
        }
        self.topology.registry.invalidate(&name);
        match self.topology.registry.get(&name).await {
            Ok(Some(desc))
                if desc.stream_epoch == epoch
                    && desc
                        .segments
                        .as_ref()
                        .is_some_and(|map| map.pending.is_some()) =>
            {
                None
            }
            Ok(_) => Some(false),
            Err(_) => None,
        }
    }

    async fn merge(&self, desc: &crate::registry::StreamDesc) -> Option<bool> {
        let Some(map) = &desc.segments else {
            return Some(false);
        };
        let mut live: Vec<_> = map
            .segments
            .iter()
            .filter(|segment| segment.is_live())
            .collect();
        live.sort_by_key(|segment| segment.lo);
        let min_age = self.cooldown_secs.saturating_mul(1000);
        let now = self.topology.scaler.clock.now().ms();
        let Some(pair) = live.windows(2).find(|pair| {
            pair[0].hi == pair[1].lo
                && now - pair[0].created_ms >= min_age
                && now - pair[1].created_ms >= min_age
        }) else {
            return Some(false);
        };
        let (a, b) = (pair[0].seg_id, pair[1].seg_id);
        let done =
            topology::execute_merge_fenced(&self.topology, &desc.sref(), &desc.stream_epoch, a, b)
                .await;
        if done {
            self.ops.emit(
                crate::ops::OpsEvent::new(
                    "merge_committed",
                    format!("merge/{}/{a}/{b}", desc.stream_epoch),
                )
                .stream(&desc.sref(), &desc.stream_epoch)
                .fields(
                    serde_json::json!({"a":a,"b":b,"projectId":desc.sref().project_id().as_str()}),
                ),
            );
        }
        Some(done)
    }
}
