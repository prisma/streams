//! The shard log's compaction scheduler (docs/QUEUES-DESIGN.md §8.2):
//! SlateDB's size-tiered scheduler, whose validation also refuses a
//! submitted spec that shares a sorted run or SST view with a live
//! compaction. At the pinned SlateDB only scheduler proposals are checked
//! against other jobs; the queue collector's spec (every L0 view and every
//! sorted run into the last run) enters through `Admin::submit_compaction`
//! unchecked, and beside a regular compaction into the last run the worker's
//! executor asserted and closed the shard log on every open (100x run of
//! 2026-10-24). The coordinator validates every submitted spec in one task,
//! so this check has no window between a read and a submission.
use std::collections::HashSet;
use std::sync::Arc;

use slatedb::compactor::{
    CompactionRequest, CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec,
    CompactionStatus, CompactorStateView, SizeTieredCompactionSchedulerSupplier, SourceId,
};

/// The shard log's database builder: `settings`' compactor options run under
/// the guarded scheduler, and every key is routed to its segment's tree by
/// the shard log's segment extractor (layout 6, `crate::keyspace`).
pub(crate) fn shard_log_builder(
    path: &str,
    store: Arc<dyn object_store::ObjectStore>,
    mut settings: slatedb::config::Settings,
) -> slatedb::DbBuilder<object_store::path::Path> {
    let compactor = settings.compactor_options.take().map(|options| {
        slatedb::CompactorBuilder::new(object_store::path::Path::from(path), store.clone())
            .with_options(options)
            .with_scheduler_supplier(Arc::new(GuardedSupplier))
    });
    let builder = crate::keyspace::db_builder(path, store).with_settings(settings);
    match compactor {
        Some(compactor) => builder.with_compactor_builder(compactor),
        None => builder,
    }
}

struct GuardedSupplier;

impl CompactionSchedulerSupplier for GuardedSupplier {
    fn compaction_scheduler(
        &self,
        options: &slatedb::config::CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        let inner = SizeTieredCompactionSchedulerSupplier::new().compaction_scheduler(options);
        Box::new(Guarded { inner })
    }
}

struct Guarded {
    inner: Box<dyn CompactionScheduler + Send + Sync>,
}

impl CompactionScheduler for Guarded {
    /// The size-tiered scheduler's proposals for the queue tree are
    /// dropped: that tree is compacted only by the queue collector (its L0
    /// merges and its debt passes, `shard::queue_maintenance`), so a mixed
    /// cell runs one stream of scheduler jobs, not one per tree (spike A2:
    /// the queue tree's marginal cost on a mixed cell fell from $3.95 to
    /// $0.25 per shard-month at -50% list).
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        self.inner
            .propose(state)
            .into_iter()
            .filter(|spec| spec.segment().as_ref() != crate::keyspace::QUEUE)
            .collect()
    }

    fn validate(
        &self,
        state: &CompactorStateView,
        spec: &CompactionSpec,
    ) -> Result<(), slatedb::Error> {
        self.inner.validate(state, spec)?;
        if live_conflict(state, spec) {
            tracing::warn!("compaction refused: it shares a run or table with a live job: {spec}");
            return Err(slatedb::Error::invalid(format!(
                "{spec} shares a run or table with a live compaction"
            )));
        }
        Ok(())
    }

    fn generate(
        &self,
        state: &CompactorStateView,
        request: &CompactionRequest,
    ) -> Result<Vec<CompactionSpec>, slatedb::Error> {
        self.inner.generate(state, request)
    }
}

/// Every sorted run and SST view a spec reads or writes. Sorted-run ids are
/// global across segments (RFC 0024: one counter; a fresh destination must
/// exceed every tree's runs) and views are ULIDs, so two specs in different
/// segments never share an id and the comparison needs no segment.
fn footprint(spec: &CompactionSpec) -> HashSet<SourceId> {
    let destination = spec.destination().map(SourceId::SortedRun);
    spec.sources().iter().copied().chain(destination).collect()
}

/// Whether `spec` shares a run or view with a claimed (scheduled or running)
/// compaction, or with one awaiting its commit. The coordinator also
/// validates a compaction it commits, which then sees itself awaiting its
/// commit under the same spec: that one is not a conflict.
fn live_conflict(state: &CompactorStateView, spec: &CompactionSpec) -> bool {
    let Some(file) = state.compactions() else {
        return false;
    };
    let candidate = footprint(spec);
    file.recent_compactions().any(|job| {
        let live = match job.status() {
            CompactionStatus::Scheduled | CompactionStatus::Running => true,
            CompactionStatus::Compacted => job.spec() != spec,
            _ => false,
        };
        live && !footprint(job.spec()).is_disjoint(&candidate)
    })
}
