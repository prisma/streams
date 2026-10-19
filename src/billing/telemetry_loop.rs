//! The telemetry loops, spawned under the runtime's supervisor: the read
//! spool's opener and ownership sweep, and the ledger drain cadence. The
//! drain loop also owns the one terminal round a graceful stop owes the
//! accuracy contract (OBSERVABILITY-BILLING §2.3, §7.4): the active read
//! window is sealed and drained before the loop reports itself finished,
//! inside one cadence and at most 5 s, so a wedged store still stops
//! cooperatively (R09) whatever the cadence (T9(b)).

use super::{drain_once, open_read_spool, sweep_owned_outboxes};
use std::sync::Arc;
use std::time::Duration;

/// The longest a graceful stop's terminal round may take: one cadence, and
/// never more than 5 s (the owner's decision of 2026-10-08, T9(b)), so the
/// supervisor's 10 s grace never aborts it, whatever `TELEMETRY_DRAIN_SECS`
/// says.
const TERMINAL_ROUND_MAX: Duration = Duration::from_secs(5);

/// The drainer task: every TELEMETRY_DRAIN_SECS (default 8), one drain
/// round. Errors log and retry — the durable outbox holds the truth.
pub(crate) fn spawn_telemetry(
    state: Arc<crate::http::AppState>,
    tasks: &crate::tasks::TaskSupervisor,
) {
    if state.billing.usage_key().is_none() {
        tracing::info!("telemetry pipeline off (USAGE_STREAM_KEY unset)");
        return;
    }
    // Open the durable read spool before the first drain (required
    // mode already opened it synchronously at startup); ownership
    // sweep runs at start and every OUTBOX_SWEEP_SECS.
    {
        let st = state.clone();
        if let Err(rejected) = tasks.spawn(
            "telemetry-outbox-sweep",
            crate::tasks::Policy::Critical,
            move |cancel| async move {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = async {
                        if let Err(e) = open_read_spool(&st).await {
                            tracing::error!("read spool open failed: {e}");
                        }
                        sweep_owned_outboxes(&st).await;
                    } => {}
                }
                let sweep_secs: u64 = st.config.billing.outbox_sweep_secs;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(sweep_secs)) => {}
                    }
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                        _ = sweep_owned_outboxes(&st) => {}
                    }
                }
            },
        ) {
            tracing::warn!("telemetry-outbox-sweep not spawned: {rejected:?}");
        }
    }
    let cadence = Duration::from_secs(state.config.billing.telemetry_drain_secs.max(1));
    let mut metrics = MetricsEmission {
        every: Duration::from_secs(state.config.billing.metrics_interval_secs),
        last: None,
        refused: None,
    };
    if let Err(rejected) = tasks.spawn(
        "telemetry-drain",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut tick = tokio::time::interval(cadence);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tick.tick() => {}
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = async {
                        match drain_once(&state).await {
                            Ok(_) => {
                                state.runtime.telemetry.drain_succeeded(state.runtime.clock.now());
                            }
                            Err(e) => tracing::warn!("usage drain: {e}"),
                        }
                        if let Err(e) = crate::ops::drain_ops_once(&state).await {
                            tracing::warn!("ops drain: {e}");
                        }
                        if let Err(e) = crate::audit::drain_audit_once(&state).await {
                            tracing::warn!("audit drain: {e}");
                        }
                        if let Err(e) = crate::fleet::drain_fleet_events(&state).await {
                            tracing::warn!("fleet event drain: {e}");
                        }
                        metrics.round(&state).await;
                    } => {}
                }
            }
            terminal_round(&state, cadence.min(TERMINAL_ROUND_MAX)).await;
            crate::tasks::TaskResult::Done
        },
    ) {
        tracing::warn!("telemetry-drain not spawned: {rejected:?}");
    }
}

/// The drain loop's `_ops_metrics` emission: a point every
/// `METRICS_INTERVAL_SECS` (default 15), and the newest point the ledger
/// refused, kept and sent again at the next drain round (the owner's
/// round-6 call 16: a shed while an instance drains lost the point). The
/// kept point and a new one go as one batch, oldest first; a refused batch
/// keeps only its newest point, so a refusal costs a point only when it
/// lasts past the next point. An append whose outcome was unknown may
/// have been written: sending it again stores the point twice, which the
/// rollup's raw tier keys by instance and time.
struct MetricsEmission {
    every: Duration,
    last: Option<crate::runtime::MonotonicNow>,
    refused: Option<crate::ops::OpsSnapshot>,
}

impl MetricsEmission {
    async fn round(&mut self, state: &Arc<crate::http::AppState>) {
        let now = state.runtime.clock.monotonic();
        let mut batch: Vec<crate::ops::OpsSnapshot> = self.refused.take().into_iter().collect();
        if self
            .last
            .is_none_or(|previous| now.since(previous) >= self.every)
        {
            self.last = Some(now);
            let point = Arc::new(crate::ops::collect_snapshot(state));
            // Cached for /operator/data.json before the append (#159), so
            // the operator surface keeps it while this append is shed.
            state.runtime.ops.remember(Arc::clone(&point));
            crate::ops::evaluate_alerts(state, &point).await;
            batch.push(Arc::unwrap_or_clone(point));
        }
        if batch.is_empty() {
            return;
        }
        if let Err(e) = crate::ops::append_metrics(state, &batch).await {
            tracing::warn!("ops metrics emit: {e}; the newest point is sent again next round");
            self.refused = batch.pop();
        }
    }
}

/// A graceful stop owes the ledger the window the cadence had not
/// reached yet (§7.4 "graceful stops flush all read usage"): seal it
/// whatever its age, then one ordinary round — spool first, so what the
/// store accepts is durable before the ledger is asked. `bound`, one
/// cadence and at most `TERMINAL_ROUND_MAX`, bounds the round: the
/// supervisor's grace belongs to the whole process, and a store that cannot
/// take one round inside it is the outage the spool already owns; the
/// `ReadDrain` guard requeues whatever a cut round still held, exactly as it
/// does for an interrupted ordinary round.
/// The handoff after a terminal round: one object write, so the round's 5 s
/// and this stay inside the supervisor's 10 s grace.
const HANDOFF_MAX: Duration = Duration::from_secs(2);

async fn terminal_round(state: &Arc<crate::http::AppState>, bound: Duration) {
    state.billing.seal_aged_reads(0);
    match tokio::time::timeout(bound, drain_once(state)).await {
        Ok(Ok(_)) => state
            .runtime
            .telemetry
            .drain_succeeded(state.runtime.clock.now()),
        Ok(Err(e)) => {
            tracing::warn!("usage drain at shutdown: {e}");
            hand_off(state).await;
        }
        Err(_elapsed) => tracing::warn!(
            "usage drain at shutdown did not finish inside {bound:?}; the spool keeps what the store accepted"
        ),
    }
}

/// What a refused terminal round could not spool (a newer boot fenced this
/// process's spool handle) goes to the spool's next holder. A round its
/// bound cut keeps its batches in memory, as an interrupted round does.
async fn hand_off(state: &Arc<crate::http::AppState>) {
    match tokio::time::timeout(HANDOFF_MAX, super::read_handoff::hand_off(state)).await {
        Ok(Ok(0)) => {}
        Ok(Ok(count)) => tracing::warn!("read usage at shutdown: {count} batches handed off"),
        Ok(Err(e)) => tracing::warn!("read usage at shutdown not handed off: {e}"),
        Err(_elapsed) => tracing::warn!("read usage handoff did not finish inside {HANDOFF_MAX:?}"),
    }
}
