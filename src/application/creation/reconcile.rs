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
//!
//! Until the one-time backfill (`Registry::backfill_fork_debt`) records its
//! completion, each round also walks one catalog page to index tombstones
//! older than the index. Each completed circle publishes the pending count
//! and the oldest pending marker's write time to [`ForkDebtStatus`], which
//! the ops snapshot exports and the `fork_debt_stale` alert reads.
use super::deletion::{release_fork_ref, repair_tombstone};
use super::*;
use crate::registry::Lifecycle;
use crate::registry::fork_debt::ForkDebt;

/// Markers one pass examines. Each costs a descriptor read and at most one
/// repeated-delete repair, itself bounded by the 64-hop ancestor walk.
const PASS_LIMIT: usize = 64;
/// Descriptors one backfill step walks: the billing tombstone walk's page.
const BACKFILL_PAGE: usize = 256;
/// A pending debt older than this many circle periods raises
/// `fork_debt_stale`, as does a circle that has not completed for as long.
const STALE_CIRCLES: u32 = 3;

/// The reconciler's published state, one per runtime: what the last
/// completed circle left pending, and whether the backfill is done.
#[derive(Debug, Default)]
pub(crate) struct ForkDebtStatus {
    pending: std::sync::atomic::AtomicU64,
    deferred: std::sync::atomic::AtomicU64,
    /// Write time of the oldest marker the last circle left pending; 0 when
    /// it left none.
    oldest_pending_ms: std::sync::atomic::AtomicI64,
    /// When the last circle completed; 0 before the first.
    circle_ms: std::sync::atomic::AtomicI64,
    /// When this reconciler started; 0 when none runs in this runtime.
    started_ms: std::sync::atomic::AtomicI64,
    stale_after_ms: std::sync::atomic::AtomicU64,
    backfill_complete: std::sync::atomic::AtomicBool,
}

impl ForkDebtStatus {
    /// `gauges` with the reconciler's own added: the ops snapshot's
    /// assembly. Ages are measured now, on the wall clock (marker write
    /// times are the object store's), so they keep growing while the
    /// reconciler is stuck. A runtime with no reconciler adds nothing.
    // mt-lint: allow(name-keyed-map): metric name, not stream identity
    pub(crate) fn exported(
        &self,
        mut gauges: std::collections::BTreeMap<String, u64>,
    ) -> std::collections::BTreeMap<String, u64> {
        self.export(now_ms(), &mut gauges);
        gauges
    }

    // mt-lint: allow(name-keyed-map): metric name, not stream identity
    fn export(&self, now_ms: i64, gauges: &mut std::collections::BTreeMap<String, u64>) {
        use std::sync::atomic::Ordering::Relaxed;
        let started = self.started_ms.load(Relaxed);
        if started == 0 {
            return;
        }
        let age = |since: i64| u64::try_from(now_ms.saturating_sub(since)).unwrap_or(0);
        let oldest = self.oldest_pending_ms.load(Relaxed);
        let circle = self.circle_ms.load(Relaxed);
        for (name, value) in [
            ("fork_debt_pending", self.pending.load(Relaxed)),
            ("fork_debt_deferred", self.deferred.load(Relaxed)),
            (
                "fork_debt_oldest_pending_age_ms",
                if oldest == 0 { 0 } else { age(oldest) },
            ),
            (
                "fork_debt_circle_age_ms",
                age(if circle == 0 { started } else { circle }),
            ),
            (
                "fork_debt_stale_after_ms",
                self.stale_after_ms.load(Relaxed),
            ),
            (
                "fork_debt_backfill_complete",
                u64::from(self.backfill_complete.load(Relaxed)),
            ),
        ] {
            gauges.insert(name.into(), value);
        }
    }

    fn start(&self, now_ms: i64, period: std::time::Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let stale = period.saturating_mul(STALE_CIRCLES).as_millis();
        self.stale_after_ms
            .store(u64::try_from(stale).unwrap_or(u64::MAX), Relaxed);
        self.started_ms.store(now_ms, Relaxed);
    }

    fn publish(&self, now_ms: i64, circle: &ReconcilePass) {
        use std::sync::atomic::Ordering::Relaxed;
        self.pending.store(circle.pending as u64, Relaxed);
        self.deferred.store(circle.deferred as u64, Relaxed);
        self.oldest_pending_ms
            .store(circle.oldest_pending_ms.unwrap_or(0), Relaxed);
        self.circle_ms.store(now_ms, Relaxed);
    }
}

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
    /// Write time of the oldest marker left pending.
    pub(crate) oldest_pending_ms: Option<i64>,
}

impl ReconcilePass {
    fn pend(&mut self, debt: &ForkDebt) {
        self.pending += 1;
        let written = debt.written_ms();
        self.oldest_pending_ms = Some(self.oldest_pending_ms.map_or(written, |o| o.min(written)));
    }

    fn absorb(&mut self, pass: &ReconcilePass) {
        self.settled += pass.settled;
        self.pending += pass.pending;
        self.deferred += pass.deferred;
        self.oldest_pending_ms = match (self.oldest_pending_ms, pass.oldest_pending_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
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
                Ok(Verdict::Pending) => pass.pend(debt),
                Err(error) => {
                    // Revisited next circle; one failing marker must not
                    // stall the rest of the index.
                    tracing::warn!(stream = %debt.child(), "fork-debt reconcile: {error}");
                    pass.pend(debt);
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

/// The loop's own state: where the circle continues, what it has seen so
/// far, and whether the backfill is known to be done.
#[derive(Default)]
struct Round {
    after: Option<String>,
    circle: ReconcilePass,
    backfilled: bool,
}

/// One bounded round: a backfill step while the backfill is incomplete, then
/// one reconcile pass. Returns whether more work is due at once (the circle
/// or the backfill continues) rather than after the period.
async fn round(service: &Arc<CreationService>, state: &mut Round) -> bool {
    let status = &service.runtime.fork_debt;
    let mut backfill_continues = false;
    if !state.backfilled {
        match service.registry.backfill_fork_debt(BACKFILL_PAGE).await {
            Ok(crate::registry::fork_debt::BackfillStep::Complete) => {
                state.backfilled = true;
                status
                    .backfill_complete
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                tracing::info!("fork-debt backfill complete");
            }
            Ok(crate::registry::fork_debt::BackfillStep::Advanced { indexed }) => {
                if indexed > 0 {
                    tracing::info!(indexed, "fork-debt backfill indexed older tombstones");
                }
                backfill_continues = true;
            }
            // Retried next round, after the period.
            Err(error) => tracing::warn!("fork-debt backfill paused: {error}"),
        }
    }
    match service.reconcile_fork_debt(state.after.as_deref()).await {
        Ok((pass, next)) => {
            if pass.settled + pass.pending + pass.deferred > 0 {
                tracing::info!(
                    settled = pass.settled,
                    pending = pass.pending,
                    deferred = pass.deferred,
                    "fork-debt reconcile pass"
                );
            }
            state.circle.absorb(&pass);
            state.after = next;
            if state.after.is_none() {
                status.publish(now_ms(), &state.circle);
                state.circle = ReconcilePass::default();
            }
        }
        // The page replays next round.
        Err(error) => tracing::warn!("fork-debt reconcile paused (index list): {error}"),
    }
    backfill_continues || state.after.is_some()
}

/// The supervised reconciler: a circle over the index at start, then one
/// every `period` (`FORK_DEBT_SWEEP_SECS`). Each round is bounded by one
/// backfill page and `PASS_LIMIT` markers; the rounds of one circle, and of
/// the backfill, run back to back so a backlog drains at the index's pace,
/// not one page per period. Every round observes cancellation, so shutdown
/// drops at most the current marker's idempotent step.
pub(crate) fn spawn_fork_debt_reconciler(
    service: Arc<CreationService>,
    tasks: &crate::tasks::TaskSupervisor,
    period: std::time::Duration,
) {
    if let Err(rejected) = tasks.spawn(
        "fork-debt-reconcile",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            service.runtime.fork_debt.start(now_ms(), period);
            let mut state = Round::default();
            loop {
                let more = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    more = round(&service, &mut state) => more,
                };
                if more {
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
