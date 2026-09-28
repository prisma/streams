//! This instance's heartbeat, `fleet/<instance>.json`, published by its own
//! supervised task once per beat (item 40, the owner's decision).
//!
//! The heartbeat's stamp is the process's liveness: this task can still
//! publish. It used to be the first step of the fleet tick, so its period
//! was the whole tick: a slow coordination document stretched it, and a
//! held heartbeat PUT stopped the controller. The document also carries,
//! separately, how long ago that controller last completed a pass
//! (`Heartbeat::progress_age_ms`); the ring planner judges the two apart
//! (`planning::active_members`). A beat costs one PUT, and a slow or failed
//! PUT delays only the next beat.
//!
//! A beat also carries this runtime's instance-wide withdrawal
//! (`Heartbeat::withdrawn`), and a stopping runtime's last beat withdraws
//! it: its peers drop it at their next pass instead of waiting out its
//! liveness, while its fencing still holds for as long as it runs.
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{AppState, Heartbeat, cpu_time_secs, rss_bytes};
use crate::shard::now_ms;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// How often this instance publishes its heartbeat.
const PERIOD: Duration = Duration::from_secs(2);
/// Commits older than this no longer describe the durable-write cost; the
/// store's WAL-PUT summary covers the same window.
const ACK_WINDOW_MS: i64 = 15_000;
/// The last beat's bound. The supervisor joins its tasks within its own
/// grace (the ordered stop allows 10 s), so a store that no longer answers
/// delays the stop by this much at most.
const LAST_BEAT_DEADLINE: Duration = Duration::from_secs(3);

/// Start this runtime's heartbeat beside its fleet tick.
pub(super) fn start(state: Arc<AppState>, instance: String, tasks: &TaskSupervisor) {
    if let Err(rejected) = tasks.spawn("fleet-heartbeat", Policy::Critical, move |cancel| {
        run(state, instance, cancel)
    }) {
        tracing::debug!(
            ?rejected,
            "runtime stopping: the fleet heartbeat was not started"
        );
    }
}

/// Publish a beat every period until cancelled, then a last, withdrawing
/// one.
async fn run(state: Arc<AppState>, instance: String, cancel: Cancellation) -> TaskResult {
    let mut sampler =
        Sampler::starting(Instant::now(), state.admission.fleet_ops(), cpu_time_secs());
    let mut beats = tokio::time::interval(PERIOD);
    beats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The interval's first tick is immediate; the first beat waits a period
    // so its rates cover one.
    beats.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = beats.tick() => {}
        }
        sampler.observe(Instant::now(), state.admission.fleet_ops(), cpu_time_secs());
        let heartbeat = sampler.heartbeat(&state, &instance);
        let published = tokio::select! {
            _ = cancel.cancelled() => break,
            published = state.fleet.publish_heartbeat(&instance, &heartbeat) => published,
        };
        if let Err(error) = published {
            tracing::warn!(%error, "heartbeat put failed");
        }
    }
    let mut last = sampler.heartbeat(&state, &instance);
    last.withdrawn
        .get_or_insert_with(|| "runtime stopping".into());
    let published = tokio::time::timeout(
        LAST_BEAT_DEADLINE,
        state.fleet.publish_heartbeat(&instance, &last),
    );
    match published.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "last heartbeat put failed"),
        Err(_) => tracing::warn!("last heartbeat put timed out"),
    }
    TaskResult::Done
}

/// The smoothed request and CPU rates, and the previous reading, so each
/// beat's rates span exactly one beat.
#[derive(Debug)]
struct Sampler {
    at: Instant,
    ops: u64,
    cpu_secs: f64,
    rps: f64,
    cpu_pct: f64,
}

impl Sampler {
    fn starting(at: Instant, ops: u64, cpu_secs: f64) -> Self {
        Sampler {
            at,
            ops,
            cpu_secs,
            rps: 0.0,
            cpu_pct: 0.0,
        }
    }

    /// Fold the reading taken at `at` (the admitted-operation counter and
    /// the process CPU seconds) into the smoothed rates.
    fn observe(&mut self, at: Instant, ops: u64, cpu_secs: f64) {
        let secs = at.duration_since(self.at).as_secs_f64().max(0.001);
        self.rps = ewma(self.rps, ops.saturating_sub(self.ops) as f64 / secs);
        self.cpu_pct = ewma(
            self.cpu_pct,
            ((cpu_secs - self.cpu_secs) / secs * 100.0).max(0.0),
        );
        (self.at, self.ops, self.cpu_secs) = (at, ops, cpu_secs);
    }

    fn heartbeat(&self, state: &AppState, instance: &str) -> Heartbeat {
        let engines = state.shards.engines_by_prefix();
        let (_, wedge_max_ms) = worst_wedge(
            engines
                .iter()
                .map(|(prefix, engine)| (prefix.clone(), engine.wedge_ms())),
        );
        // An engine whose timing ring is poisoned may hold a half-recorded
        // wait, so its samples are left out.
        let waits: Vec<(i64, u32)> = engines
            .iter()
            .filter_map(|(_, engine)| engine.timings.lock().ok())
            .flat_map(|timings| {
                timings
                    .iter()
                    .map(|group| (group.ts_ms, group.durable_wait_us))
                    .collect::<Vec<_>>()
            })
            .collect();
        let (inflight, inflight_peak) = state.admission.swap_peak();
        let (wal_put_p50_ms, wal_put_p99_ms, out_inflight, out_inflight_peak) =
            crate::store_timing::heartbeat_summary();
        let ts_ms = now_ms();
        Heartbeat {
            instance: instance.to_string(),
            ts_ms,
            rps: tenths(self.rps),
            ack_p50_ms: recent_p50_ms(waits, ts_ms),
            cpu_pct: tenths(self.cpu_pct),
            inflight,
            inflight_peak,
            rss_mb: megabytes(rss_bytes()),
            wal_put_p50_ms,
            wal_put_p99_ms,
            out_inflight,
            out_inflight_peak,
            owned_shards: engines.into_iter().map(|(prefix, _)| prefix).collect(),
            draining: false,
            absorb_lag_max_secs: state.runtime.usage.absorb_lag_max(),
            wedge_max_ms,
            url: state.config.fleet.self_url.clone(),
            boot_id: state.runtime.identity.boot_id.clone(),
            progress_age_ms: state.fleet.standing().progress_age_ms(),
            withdrawn: state.withdrawal(),
        }
    }
}

impl AppState {
    /// Why this runtime takes no new ownership, instance-wide; `None` while
    /// it may. It is the readiness verdict: a supervisor that is stopping
    /// or lost a Critical loop, or a shard directory that reports a cell
    /// failure, never opened a shard, or could not close one.
    fn withdrawal(&self) -> Option<String> {
        self.tasks
            .unready_reason()
            .or_else(|| self.shards.unready_reason())
    }

    /// This runtime's own maintenance pressure as its rebalancer judges it
    /// at the tick: the most wedged shard it serves (prefix, ms) and its
    /// effective lag in seconds.
    pub(super) fn fleet_pressure(&self) -> (String, i64, u64) {
        let (wedge_prefix, wedge_max_ms) = worst_wedge(
            self.shards
                .engines_by_prefix()
                .into_iter()
                .map(|(prefix, engine)| (prefix, engine.wedge_ms())),
        );
        let lag = effective_lag_secs(self.runtime.usage.absorb_lag_max(), wedge_max_ms);
        (wedge_prefix, wedge_max_ms, lag)
    }
}

/// The most wedged shard of `wedges` (prefix, blocked commit write or stale
/// durability in ms), `("", 0)` when there is none.
fn worst_wedge(wedges: impl Iterator<Item = (String, i64)>) -> (String, i64) {
    wedges.max_by_key(|(_, wedge)| *wedge).unwrap_or_default()
}

/// A shard host's lag as the rebalancer judges it: the oldest unabsorbed
/// bytes or the wedge, whichever is longer. A backpressured shard sheds
/// appends before they commit, so a wedge never shows as absorb lag.
fn effective_lag_secs(absorb_lag_secs: u64, wedge_ms: i64) -> u64 {
    absorb_lag_secs.max(u64::try_from(wedge_ms / 1000).unwrap_or(0))
}

/// The median durable wait (ms, to a tenth) of the commits in `waits`
/// (commit time, wait in µs) recorded within `ACK_WINDOW_MS` of `now_ms`.
fn recent_p50_ms(waits: Vec<(i64, u32)>, now_ms: i64) -> f64 {
    let cutoff = now_ms - ACK_WINDOW_MS;
    let mut recent: Vec<u32> = waits
        .into_iter()
        .filter(|(ts_ms, _)| *ts_ms >= cutoff)
        .map(|(_, wait_us)| wait_us)
        .collect();
    recent.sort_unstable();
    recent
        .get(recent.len() / 2)
        .map_or(0.0, |wait_us| tenths(f64::from(*wait_us) / 1000.0))
}

/// The fleet's smoothing: a first reading is taken as is.
fn ewma(previous: f64, reading: f64) -> f64 {
    if previous == 0.0 {
        reading
    } else {
        previous * 0.6 + reading * 0.4
    }
}

fn megabytes(bytes: u64) -> f64 {
    tenths(bytes as f64 / 1_048_576.0)
}

fn tenths(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::{
        ACK_WINDOW_MS, Sampler, effective_lag_secs, ewma, megabytes, recent_p50_ms, tenths,
        worst_wedge,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn each_beat_rates_one_beat_and_smooths_after_the_first() {
        let start = Instant::now();
        let mut sampler = Sampler::starting(start, 1_000, 10.0);
        sampler.observe(start + Duration::from_secs(2), 1_100, 11.0);
        assert_eq!((sampler.rps, sampler.cpu_pct), (50.0, 50.0));
        sampler.observe(start + Duration::from_secs(4), 1_300, 11.0);
        assert_eq!((sampler.rps, sampler.cpu_pct), (70.0, 30.0));
    }

    #[test]
    fn the_worst_wedge_names_its_shard() {
        let wedges = [("a", 5), ("b", 9), ("c", 0)].map(|(prefix, ms)| (prefix.to_string(), ms));
        assert_eq!(worst_wedge(wedges.into_iter()), ("b".to_string(), 9));
        assert_eq!(worst_wedge(std::iter::empty()), (String::new(), 0));
    }

    #[test]
    fn a_wedge_counts_as_lag_in_whole_seconds() {
        assert_eq!(effective_lag_secs(3, 7_999), 7);
        assert_eq!(effective_lag_secs(9, 7_999), 9);
        assert_eq!(effective_lag_secs(3, -5_000), 3);
    }

    #[test]
    fn the_ack_median_covers_only_the_window() {
        let now = 100_000;
        // The stale sample and the window's edge each move the median.
        let waits = vec![
            (now - ACK_WINDOW_MS - 1, 900_000),
            (now - ACK_WINDOW_MS, 1_000),
            (now, 3_000),
            (now - 1, 5_000),
        ];
        assert_eq!(recent_p50_ms(waits, now), 3.0);
        let even = vec![(now, 1_000), (now, 2_000), (now, 3_000), (now, 4_000)];
        assert_eq!(recent_p50_ms(even, now), 3.0, "the upper median");
        assert_eq!(recent_p50_ms(Vec::new(), now), 0.0);
    }

    #[test]
    fn readings_are_published_to_a_tenth() {
        assert_eq!(ewma(0.0, 7.0), 7.0);
        assert_eq!(ewma(10.0, 20.0), 14.0);
        assert_eq!(megabytes(3 * 1_048_576 + 104_858), 3.1);
        assert_eq!(tenths(1.26), 1.3);
    }
}
