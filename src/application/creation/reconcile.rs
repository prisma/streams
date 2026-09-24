//! Background settlement of retained fork-reference debt (TLA-019-F4).
//!
//! A child's tombstone keeps `parent_ref_pending` when its `DELETE` died
//! after the tombstone write, or when its release found the reference absent
//! on a live source: a creator between its pre-check and its install may
//! still land the reference and then die before its compensating check. The
//! `DELETE` may already have answered success, so the client has no reason to
//! repeat it. This actor owns that repair instead: it pages the fork-debt
//! index a bounded number of markers at a time and, for each debt-bearing
//! tombstone, runs exactly what a repeated `DELETE` runs (`repair_tombstone`).
//!
//! Safety against a live creator rests on the protocol the repeated `DELETE`
//! already relies on. The reconciler only ever releases a reference by the
//! id of an incarnation that is tombstoned or replaced, which can never serve
//! reads again, fenced to the source incarnation it was installed on; a new
//! child under the same name has a new epoch and so a different id. It
//! clears a debt only on a conclusive release. An absent reference on the
//! live source is inconclusive, so the marker stays and a creator's late
//! install is released on a later pass, or by the creator's own post-check
//! if it lives. It never installs anything.
//!
//! Restart safety: the index is durable and the cursor is not. A restarted
//! reconciler begins a new circle; every step is idempotent, and a marker is
//! dropped only after its debt was seen paid.
use super::deletion::{release_fork_ref, repair_tombstone};
use super::*;
use crate::registry::Lifecycle;
use crate::registry::fork_debt::ForkDebt;

/// Markers one pass examines. Each costs a descriptor read and at most one
/// repeated-delete repair, itself bounded by the 64-hop ancestor walk.
const PASS_LIMIT: usize = 64;

/// What one pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconcilePass {
    /// Debts seen paid; their markers are gone.
    pub(crate) settled: usize,
    /// Debts still owed: an inconclusive release or a failed step.
    pub(crate) pending: usize,
    /// Markers whose descriptor owes nothing yet: live, expiring or retained
    /// for its own forks.
    pub(crate) deferred: usize,
}

enum Verdict {
    Settled,
    Pending,
    Deferred,
}

impl CreationService {
    /// One bounded pass over the fork-debt index, continuing after `after`.
    /// Returns what it did and where the next pass continues: `None` starts
    /// a new circle.
    pub(crate) async fn reconcile_fork_debt(
        self: &Arc<Self>,
        after: Option<&str>,
    ) -> Result<(ReconcilePass, Option<String>), String> {
        let page = self
            .registry
            .fork_debt_page(after, PASS_LIMIT)
            .await
            .map_err(|error| error.to_string())?;
        let mut pass = ReconcilePass::default();
        for debt in &page.debts {
            match settle(self, debt).await {
                Ok(Verdict::Settled) => pass.settled += 1,
                Ok(Verdict::Deferred) => pass.deferred += 1,
                Ok(Verdict::Pending) => pass.pending += 1,
                Err(error) => {
                    // Revisited next circle; one failing marker must not
                    // stall the rest of the index.
                    tracing::warn!(stream = %debt.child(), "fork-debt reconcile: {error}");
                    pass.pending += 1;
                }
            }
        }
        let next = if page.exhausted {
            None
        } else {
            page.next_after
        };
        Ok((pass, next))
    }
}

async fn settle(state: &Arc<CreationService>, debt: &ForkDebt) -> Result<Verdict, String> {
    state.registry.invalidate(debt.child());
    let current = state
        .registry
        .get(debt.child())
        .await
        .map_err(|error| error.to_string())?;
    let paid = match current {
        Some(desc) if desc.stream_epoch == debt.child_epoch() => match desc.lifecycle() {
            Lifecycle::Deleted {
                parent_ref_pending: true,
            } => {
                // The repeated DELETE, exactly: pay this tombstone's debt,
                // then walk up the fork chain.
                repair_tombstone(state, &desc, true).await?;
                state.registry.invalidate(debt.child());
                !state
                    .registry
                    .get(debt.child())
                    .await
                    .map_err(|error| error.to_string())?
                    .is_some_and(|after| {
                        after.stream_epoch == debt.child_epoch() && after.parent_ref_pending
                    })
            }
            Lifecycle::Deleted {
                parent_ref_pending: false,
            } => true,
            // The delete that wrote the marker may still be before its
            // tombstone write, or the cascade has yet to tombstone a
            // retained source. Nothing is owed yet.
            Lifecycle::Active
            | Lifecycle::Initializing(_)
            | Lifecycle::Sealing
            | Lifecycle::Sealed
            | Lifecycle::RetainedForks => return Ok(Verdict::Deferred),
        },
        // The incarnation was replaced by a recreation of its name (or the
        // descriptor is gone): the tombstone that carried the debt no longer
        // exists, so the marker is its only record. Pay it from the marker.
        _ => {
            release_fork_ref(
                state,
                debt.source().clone(),
                debt.fork_id(),
                debt.source_epoch(),
            )
            .await?
        }
    };
    if !paid {
        return Ok(Verdict::Pending);
    }
    state
        .registry
        .settle_fork_debt(debt.child(), debt.child_epoch())
        .await
        .map_err(|error| error.to_string())?;
    Ok(Verdict::Settled)
}

/// The supervised reconciler: a circle over the index at start, then one
/// every `period` (`FORK_DEBT_SWEEP_SECS`). Each pass is bounded by
/// `PASS_LIMIT` markers; the passes of one circle run back to back so a
/// backlog drains at the index's pace, not one page per period. Every pass
/// observes cancellation, so shutdown drops at most the current marker's
/// idempotent step.
pub(crate) fn spawn_fork_debt_reconciler(
    service: Arc<CreationService>,
    tasks: &crate::tasks::TaskSupervisor,
    period: std::time::Duration,
) {
    if let Err(rejected) = tasks.spawn(
        "fork-debt-reconcile",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut after: Option<String> = None;
            loop {
                let cursor = after.clone();
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    outcome = service.reconcile_fork_debt(cursor.as_deref()) => match outcome {
                        Ok((pass, next)) => {
                            if pass != ReconcilePass::default() {
                                tracing::info!(
                                    settled = pass.settled,
                                    pending = pass.pending,
                                    deferred = pass.deferred,
                                    "fork-debt reconcile pass"
                                );
                            }
                            after = next;
                        }
                        // The page replays next pass.
                        Err(error) => tracing::warn!("fork-debt reconcile paused (index list): {error}"),
                    },
                }
                if after.is_some() {
                    continue;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = tokio::time::sleep(period) => {}
                }
            }
        },
    ) {
        // Only while stopping, when no settlement is owed.
        tracing::warn!("fork-debt-reconcile not spawned: {rejected:?}");
    }
}
