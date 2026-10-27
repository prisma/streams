//! The queue tree's L0 merges (DESIGN-6A draft 3, the A2 policy). The
//! size-tiered scheduler never compacts the queue tree
//! (`native_queue::compaction_guard`), so the collector merges its L0s once
//! they reach `L0_TRIGGER`, well under `l0_max_ssts`: every L0 view plus the
//! newest sorted runs no larger than twice what the merge already holds (so
//! runs stay geometric, about log2 of the tree over one batch), into the
//! oldest included run or a fresh one. Including the last run makes it the
//! last run, so tombstones drop then. The merger reads the in-memory
//! manifest every tick and touches the store only when a merge is due: one
//! compactor-state read and one submission; it does not poll the job, it
//! waits until the merged L0s leave its manifest.
use super::{Arc, QUEUE_TREE, ShardEngine, fresh_run_id, queue_tree, quiet};
use slatedb::compactor::{CompactorStateView, SourceId};

/// The queue tree's L0 count at which the merger merges them: half the
/// 32-L0 line, so a merge refused once (and retried after `MERGE_PATIENCE`)
/// still leaves the tree under it (spike A2: peaks 17-19 at 16; at 8 the
/// tree cost about twice as much).
const L0_TRIGGER: usize = 16;
/// How often the merger looks at the queue tree's L0 count.
const MERGE_TICK: std::time::Duration = std::time::Duration::from_secs(2);
/// How long a submitted merge may keep its L0s before the merger looks
/// again: a refused or failed submission (the coordinator validates it on
/// its own tick) is then retried, and one still running keeps the tree not
/// quiet, so nothing is submitted beside it. A 120 s patience let a refused
/// merge hold 26 more L0s (wave 1, n16, peak 42).
const MERGE_PATIENCE: std::time::Duration = std::time::Duration::from_secs(10);
/// How far above the next global run id a fresh merge destination sits, so
/// the size-tiered scheduler, which takes the next id for a stream-tree job
/// planned at the same moment, does not take the same one (the coordinator
/// refuses the second of two live jobs with one destination).
const FRESH_ID_GAP: u32 = 16;

/// A merge plan: sources, destination, and the L0 views it names.
pub(super) struct Merge {
    pub(super) sources: Vec<SourceId>,
    pub(super) destination: u32,
    pub(super) l0_views: Vec<String>,
    pub(super) runs: usize,
    pub(super) bytes: u64,
}

/// The merge of `view`'s queue tree: every L0 view, then newest runs while
/// each is at most twice the bytes gathered so far.
pub(super) fn plan(view: &CompactorStateView) -> Option<Merge> {
    let tree = queue_tree(view.manifest())?;
    if tree.l0().is_empty() {
        return None;
    }
    let mut sources: Vec<SourceId> = tree.l0().iter().map(|v| SourceId::SstView(v.id)).collect();
    let mut bytes: u64 = tree.l0().iter().map(|v| v.estimate_size()).sum();
    let mut lowest = None;
    let mut runs = 0;
    for run in tree.compacted() {
        let size = run.estimate_size();
        if size > bytes.saturating_mul(2) {
            break;
        }
        sources.push(SourceId::SortedRun(run.id));
        bytes = bytes.saturating_add(size);
        lowest = Some(run.id);
        runs += 1;
    }
    Some(Merge {
        sources,
        destination: lowest.unwrap_or_else(|| fresh_run_id(view).saturating_add(FRESH_ID_GAP)),
        l0_views: tree.l0().iter().map(|v| v.id.to_string()).collect(),
        runs,
        bytes,
    })
}

/// The merger's pending submission: the L0 views it named, and when.
struct Pending {
    l0_views: Vec<String>,
    at: std::time::Instant,
}

impl ShardEngine {
    /// Merge the queue tree's L0s whenever they reach the trigger, until the
    /// engine closes.
    pub(super) async fn queue_l0_merger_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(MERGE_TICK);
        let mut closed = self.close_tx.subscribe();
        let mut pending: Option<Pending> = None;
        while self.queue_tick(&mut interval, &mut closed).await {
            let manifest = self.db.manifest();
            let Some(tree) = queue_tree(&manifest) else {
                continue;
            };
            let live: Vec<String> = tree.l0().iter().map(|v| v.id.to_string()).collect();
            let waiting = pending.as_ref().is_some_and(|p| {
                p.l0_views.iter().any(|v| live.contains(v)) && p.at.elapsed() < MERGE_PATIENCE
            });
            if waiting {
                continue;
            }
            pending = None;
            if live.len() < L0_TRIGGER {
                continue;
            }
            match self.submit_l0_merge().await {
                Ok(Some(views)) => {
                    pending = Some(Pending {
                        l0_views: views,
                        at: std::time::Instant::now(),
                    })
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(shard = %self.prefix, "queue L0 merge deferred: {error}")
                }
            }
        }
    }

    /// Plan and submit one merge when the queue tree holds no live job;
    /// the L0 views it names, or `None` when it submitted nothing.
    async fn submit_l0_merge(&self) -> Result<Option<Vec<String>>, String> {
        let admin = self.queue_admin();
        let view = admin
            .read_compactor_state_view()
            .await
            .map_err(|e| e.to_string())?;
        if !quiet(&view) {
            return Ok(None);
        }
        let Some(merge) = plan(&view) else {
            return Ok(None);
        };
        let spec = slatedb::compactor::CompactionSpec::for_segment(
            bytes::Bytes::from_static(&QUEUE_TREE),
            merge.sources,
            merge.destination,
        );
        admin
            .submit_compaction(spec)
            .await
            .map_err(|e| format!("queue L0 merge refused: {e}"))?;
        tracing::info!(
            shard = %self.prefix,
            l0s = merge.l0_views.len(),
            runs = merge.runs,
            bytes = merge.bytes,
            destination = merge.destination,
            "queue L0 merge submitted"
        );
        Ok(Some(merge.l0_views))
    }
}
