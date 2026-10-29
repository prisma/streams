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

/// How often the alert rules run (OBSERVABILITY-BILLING §13.2): every 15 s,
/// at the first drain round that reaches it (16 s at the default 8 s
/// cadence), whatever `METRICS_INTERVAL_SECS` says. A round that runs them
/// with no `_ops_metrics` point due takes a snapshot for them alone.
const ALERT_EVALUATION_INTERVAL: Duration = Duration::from_secs(15);

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
    let mut metrics = MetricsEmission::new(Duration::from_secs(
        state.config.billing.metrics_interval_secs,
    ));
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
/// `METRICS_INTERVAL_SECS` (default 60), and the newest point the ledger
/// refused, kept and sent again at the next drain round (the owner's
/// round-6 call 16: a shed while an instance drains lost the point). The
/// kept point and a new one go as one batch, oldest first; a refused batch
/// keeps only its newest point, so a refusal costs a point only when it
/// lasts past the next point. An append whose outcome was unknown may
/// have been written: sending it again stores the point twice, which the
/// rollup's raw tier keys by instance and time.
///
/// The alert rules run on every snapshot, and on one of their own every
/// `ALERT_EVALUATION_INTERVAL` between points; that snapshot is cached for
/// `/operator/data.json` as a point is, and not stored. Its collection
/// drained the peak gauges, so it hands them to the next snapshot.
struct MetricsEmission {
    every: Duration,
    /// When the last point was taken.
    last: Option<crate::runtime::MonotonicNow>,
    /// When the alert rules last ran.
    evaluated: Option<crate::runtime::MonotonicNow>,
    /// The newest snapshot taken for the alert rules alone since the last
    /// point, holding the peaks drained since then.
    unsent: Option<Arc<crate::ops::OpsSnapshot>>,
    refused: Option<crate::ops::OpsSnapshot>,
}

/// What a drain round takes a snapshot for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Due {
    /// A point for `_ops_metrics`; the alert rules read it too.
    Point,
    /// The alert rules alone.
    Alerts,
}

impl MetricsEmission {
    fn new(every: Duration) -> Self {
        Self {
            every,
            last: None,
            evaluated: None,
            unsent: None,
            refused: None,
        }
    }

    async fn round(&mut self, state: &Arc<crate::http::AppState>) {
        let mut batch: Vec<crate::ops::OpsSnapshot> = self.refused.take().into_iter().collect();
        let now = state.runtime.clock.monotonic();
        if let Some((snapshot, due)) = self.take(now, || crate::ops::collect_snapshot(state)) {
            // Cached for /operator/data.json before the append (#159), so
            // the operator surface keeps it while this append is shed.
            state.runtime.ops.remember(Arc::clone(&snapshot));
            crate::ops::evaluate_alerts(state, &snapshot).await;
            if due == Due::Point {
                batch.push(Arc::unwrap_or_clone(snapshot));
            }
        }
        if batch.is_empty() {
            return;
        }
        if let Err(e) = crate::ops::append_metrics(state, &batch).await {
            tracing::warn!("ops metrics emit: {e}; the newest point is sent again next round");
            self.refused = batch.pop();
        }
    }

    /// The round's snapshot, when a point or the alert rules are due,
    /// carrying the peaks of the unsent snapshots taken since the last
    /// point; one taken for the alert rules alone is kept as the unsent.
    fn take(
        &mut self,
        now: crate::runtime::MonotonicNow,
        collect: impl FnOnce() -> crate::ops::OpsSnapshot,
    ) -> Option<(Arc<crate::ops::OpsSnapshot>, Due)> {
        let due = self.due(now)?;
        let mut snapshot = collect();
        if let Some(unsent) = self.unsent.take() {
            snapshot.carry_peaks(&unsent);
        }
        let snapshot = Arc::new(snapshot);
        if due == Due::Alerts {
            self.unsent = Some(Arc::clone(&snapshot));
        }
        Some((snapshot, due))
    }

    /// What the round at `now` takes a snapshot for, if anything: a point
    /// once `every` has passed since the last, else the alert rules once
    /// `ALERT_EVALUATION_INTERVAL` has passed since they last ran.
    fn due(&mut self, now: crate::runtime::MonotonicNow) -> Option<Due> {
        let passed = |since: Option<crate::runtime::MonotonicNow>, every: Duration| {
            since.is_none_or(|previous| now.since(previous) >= every)
        };
        let due = if passed(self.last, self.every) {
            self.last = Some(now);
            Due::Point
        } else if passed(self.evaluated, ALERT_EVALUATION_INTERVAL) {
            Due::Alerts
        } else {
            return None;
        };
        self.evaluated = Some(now);
        Some(due)
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

#[cfg(test)]
mod emission_tests {
    use super::{Due, MetricsEmission};
    use crate::runtime::{Clock, ManualClock};
    use std::time::Duration;

    /// A collected snapshot with the peak RSS gauge and the plain RSS gauge.
    fn collected(rss_peak: u64, rss: u64) -> crate::ops::OpsSnapshot {
        let gauges = [("rss_peak_since_scrape_mb", rss_peak), ("rss_mb", rss)];
        crate::ops::OpsSnapshot {
            v: 1,
            ts_ms: 0,
            cell: String::new(),
            region: String::new(),
            instance: String::new(),
            role: "server".into(),
            counters: std::collections::BTreeMap::new(),
            gauges: gauges.map(|(k, v)| (k.to_string(), v)).into(),
        }
    }

    /// Ten minutes of drain rounds every `cadence` seconds with a point due
    /// every `metrics` seconds: the seconds of the points, and of every
    /// snapshot the alert rules read (the points' and their own).
    fn ten_minutes(metrics: u64, cadence: u64) -> (Vec<u64>, Vec<u64>) {
        let clock = ManualClock::at(0);
        let mut emission = MetricsEmission::new(Duration::from_secs(metrics));
        let (mut points, mut evaluated) = (Vec::new(), Vec::new());
        for at in (0..=600).step_by(usize::try_from(cadence).unwrap()) {
            let due = emission.take(clock.monotonic(), || collected(0, 0));
            clock.advance_monotonic(Duration::from_secs(cadence));
            let Some((_, due)) = due else { continue };
            evaluated.push(at);
            if due == Due::Point {
                points.push(at);
            }
        }
        (points, evaluated)
    }

    fn every(step: u64, last: u64) -> Vec<u64> {
        (0..=last).step_by(usize::try_from(step).unwrap()).collect()
    }

    /// At the defaults (a round every 8 s) the alert rules run every 16 s,
    /// 38 times in ten minutes, as they did when every 15 s snapshot was a
    /// point, and every fourth of those snapshots is a point, 64 s apart:
    /// 10 points. Rounds every 5 s land on both cadences exactly: a point
    /// every 60 s, the rules every 15 s.
    #[test]
    fn default_rounds_take_a_point_a_minute_and_run_the_alerts_every_16_s() {
        let defaults = crate::config::BillingConfig::default();
        let (points, evaluated) = ten_minutes(
            defaults.metrics_interval_secs,
            defaults.telemetry_drain_secs,
        );
        assert_eq!(points, every(64, 576), "a point every 64 s");
        assert_eq!(evaluated, every(16, 592), "the alert rules every 16 s");
        let (points, evaluated) = ten_minutes(defaults.metrics_interval_secs, 5);
        assert_eq!(points, every(60, 600), "a point once 60 s have passed");
        assert_eq!(evaluated, every(15, 600), "the rules once 15 s have passed");
    }

    /// One round 16 s after the last: what it took the snapshot for, and the
    /// snapshot's peak RSS and RSS gauges.
    fn round(
        emission: &mut MetricsEmission,
        clock: &ManualClock,
        snapshot: crate::ops::OpsSnapshot,
    ) -> (Due, [u64; 2]) {
        let (taken, due) = emission.take(clock.monotonic(), || snapshot).unwrap();
        clock.advance_monotonic(Duration::from_secs(16));
        let gauge = |name: &str| taken.gauges[name];
        let names = ["rss_peak_since_scrape_mb", "rss_mb"];
        (due, names.map(gauge))
    }

    /// A snapshot taken for the alert rules alone is not stored, but its
    /// collection drained the peak gauges: each later snapshot carries the
    /// largest of each since the last point, the next point included, and
    /// the snapshot after that point starts afresh. Other gauges are as
    /// collected.
    #[test]
    fn the_next_point_carries_the_peaks_the_alert_snapshots_drained() {
        let clock = ManualClock::at(0);
        let mut emission = MetricsEmission::new(Duration::from_secs(60));
        let mut rounds = Vec::new();
        for (rss_peak, rss) in [
            (100, 90),
            (700, 650),
            (300, 280),
            (200, 190),
            (150, 140),
            (120, 110),
        ] {
            rounds.push(round(&mut emission, &clock, collected(rss_peak, rss)));
        }
        assert_eq!(
            rounds,
            [
                (Due::Point, [100, 90]),
                (Due::Alerts, [700, 650]),
                (Due::Alerts, [700, 280]),
                (Due::Alerts, [700, 190]),
                (Due::Point, [700, 140]),
                (Due::Alerts, [120, 110]),
            ]
        );
    }
}
