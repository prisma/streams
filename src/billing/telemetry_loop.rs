//! The telemetry loops, spawned under the runtime's supervisor: the read
//! spool's opener and ownership sweep, and the ledger drain cadence. The
//! drain loop also owns the one terminal round a graceful stop owes the
//! accuracy contract (OBSERVABILITY-BILLING §2.3, §7.4): the active read
//! window is sealed and drained before the loop reports itself finished,
//! inside one cadence, so a wedged store still stops cooperatively (R09).

use super::{drain_once, open_read_spool, sweep_owned_outboxes};
use std::sync::Arc;
use std::time::Duration;

/// The drainer task: every TELEMETRY_DRAIN_SECS (default 2), one drain
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
    let metrics_secs: u64 = state.config.billing.metrics_interval_secs;
    if let Err(rejected) = tasks.spawn(
        "telemetry-drain",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut tick = tokio::time::interval(cadence);
            let mut last_metrics = None;
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
                        let now = state.runtime.clock.monotonic();
                        if last_metrics.is_none_or(|previous| {
                            now.since(previous) >= std::time::Duration::from_secs(metrics_secs)
                        }) {
                            last_metrics = Some(now);
                            if let Err(e) = crate::ops::emit_metrics_once(&state).await {
                                tracing::warn!("ops metrics emit: {e}");
                            }
                        }
                    } => {}
                }
            }
            terminal_round(&state, cadence).await;
            crate::tasks::TaskResult::Done
        },
    ) {
        tracing::warn!("telemetry-drain not spawned: {rejected:?}");
    }
}

/// A graceful stop owes the ledger the window the cadence had not
/// reached yet (§7.4 "graceful stops flush all read usage"): seal it
/// whatever its age, then one ordinary round — spool first, so what the
/// store accepts is durable before the ledger is asked. One cadence bounds
/// the round: the supervisor's grace belongs to the whole process, and a
/// store that cannot take one round inside one cadence is the outage the
/// spool already owns; the `ReadDrain` guard requeues whatever a cut round
/// still held, exactly as it does for an interrupted ordinary round.
async fn terminal_round(state: &Arc<crate::http::AppState>, cadence: Duration) {
    state.billing.seal_aged_reads(0);
    match tokio::time::timeout(cadence, drain_once(state)).await {
        Ok(Ok(_)) => state
            .runtime
            .telemetry
            .drain_succeeded(state.runtime.clock.now()),
        Ok(Err(e)) => tracing::warn!("usage drain at shutdown: {e}"),
        Err(_elapsed) => tracing::warn!(
            "usage drain at shutdown did not finish inside {cadence:?}; the spool keeps what the store accepted"
        ),
    }
}
