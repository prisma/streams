//! The telemetry loops, spawned under the runtime's supervisor: the read
//! spool's opener and ownership sweep, and the ledger drain cadence.

use super::{drain_once, open_read_spool, sweep_owned_outboxes};

/// The drainer task: every TELEMETRY_DRAIN_SECS (default 2), one drain
/// round. Errors log and retry — the durable outbox holds the truth.
pub(crate) fn spawn_telemetry(
    state: std::sync::Arc<crate::http::AppState>,
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
    let secs: u64 = state.config.billing.telemetry_drain_secs;
    let metrics_secs: u64 = state.config.billing.metrics_interval_secs;
    if let Err(rejected) = tasks.spawn(
        "telemetry-drain",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            let mut last_metrics = None;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = tick.tick() => {}
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
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
        },
    ) {
        tracing::warn!("telemetry-drain not spawned: {rejected:?}");
    }
}
