//! The fleet count's hysteresis (FLEET-HYSTERESIS.md, ruled in
//! FLEET-HYSTERESIS-REVIEW.md §2-§3): the in-flight requests the planner
//! reads, and when a planned count is published. The fleet tick
//! (`fleet::start`) folds its members' beats, has the gate hold the fold's
//! in-flight, plans (`planning::Demand::plan`), and asks the gate whether
//! this tick publishes. The gate is pure and takes the tick's clock, so a
//! replay drives it as the tick does.
use std::time::{Duration, Instant};

use super::Desired;
use super::planning::Loads;

/// A tick's clock, taken once a tick: the gate holds and decides on the
/// same instant.
#[derive(Clone, Copy, Debug)]
pub(super) struct Now {
    pub(super) at: Instant,
}

impl Now {
    pub(super) fn current() -> Self {
        Self { at: Instant::now() }
    }
}

/// The fleet count's hysteresis: the in-flight the planner reads, and when
/// a planned count is published.
#[derive(Debug)]
pub(super) struct Gate {
    scale_in: Duration,
    /// Since when, on this instance's clock, the shrink target has stayed
    /// below the published count.
    below_since: Option<Instant>,
}

impl Gate {
    pub(super) fn new(scale_in: Duration) -> Self {
        Self {
            scale_in,
            below_since: None,
        }
    }

    /// Hold the in-flight the tick plans with (`Loads::inflight`): the
    /// fold's sum, as the members report it.
    pub(super) fn hold(&mut self, _loads: &mut Loads, _cur: Option<&Desired>, _now: Now) {}

    /// The count this tick publishes, if any, from the plan's grow and
    /// shrink targets (`planning::Need`) and the published document: the
    /// first count at once; a grow at once; a shrink to the conservative
    /// target once it has stayed below the published count for
    /// `SCALE_IN_SECS` on this instance's clock.
    pub(super) fn decide(
        &mut self,
        plan: (u64, u64),
        cur: Option<&Desired>,
        now: Now,
    ) -> Option<u64> {
        let (need, shrink) = plan;
        let Some(cur) = cur else {
            return Some(need);
        };
        if need > cur.count {
            self.below_since = None;
            Some(need)
        } else if shrink < cur.count {
            let since = *self.below_since.get_or_insert(now.at);
            (now.at.duration_since(since) >= self.scale_in).then_some(shrink)
        } else {
            self.below_since = None;
            None
        }
    }

    /// The tick's publication was written: a shrink's wait starts again.
    pub(super) fn published(&mut self) {
        self.below_since = None;
    }
}

#[cfg(test)]
mod tests {
    use super::super::Desired;
    use super::super::planning::{Demand, Loads, Thresholds};
    use super::{Gate, Now};
    use std::time::{Duration, Instant};

    const FLEET_MAX: u64 = 8;
    const SHARDS: u64 = 16;
    /// The wall time the replay starts at.
    const EPOCH_MS: i64 = 1_000_000_000;

    /// FLEET-HYSTERESIS-REVIEW.md §7's per-tick trace: (through, in-flight
    /// summed over the members, cores in use), each row through its tick.
    /// A one-tick spike, a sustained load, a ten-tick spike over it (a
    /// shard move's refusals), a heavier sustained load, a long quiet.
    const TRACE: [(i64, f64, f64); 9] = [
        (0, 0.0, 0.2),
        (88, 20.0, 0.2),
        (90, 241.0, 0.4),
        (150, 36.0, 0.4),
        (300, 300.0, 0.5),
        (320, 700.0, 0.6),
        (398, 300.0, 0.6),
        (480, 650.0, 0.9),
        (1_100, 30.0, 0.3),
    ];

    fn reading(t: i64) -> (f64, f64) {
        let row = TRACE.iter().find(|row| t <= row.0).unwrap();
        (row.1, row.2)
    }

    /// The production thresholds, as `fleet::start_configured` derives
    /// them from the command line's defaults, at `FLEET_MAX` 8.
    fn thresholds() -> Thresholds {
        let cli = crate::config::CliArgs::deterministic();
        Thresholds {
            target_util: (cli.scale_out_cpu_pct as f64 / 100.0).clamp(0.05, 0.95),
            scale_in_util: (cli.scale_in_cpu_pct as f64 / 100.0).clamp(0.05, 0.90),
            edge_slots: cli.scale_edge_slots,
            latency_ms: cli.scale_latency_ms,
            latency_sustain: Duration::from_secs(cli.scale_lat_sustain_secs),
            edge_latency_ms: cli.scale_edge_latency_ms,
            hot_cpu_pct: cli.scale_out_cpu_pct as f64,
            cpu_sustain: Duration::from_secs(cli.scale_cpu_sustain_secs),
            max: FLEET_MAX,
            rss_shed_mb: 0,
            cache_capacity: crate::registry::cache::REGISTRY_CACHE_MAX as u64,
            tracker_capacity: crate::quota::MAX_TRACKED_PROJECTS as u64,
        }
    }

    /// Run the trace through the gate, the planner and the gate's decision
    /// every 2 s from 0 s to 1,100 s, as the fleet tick does, with the
    /// command line's `SCALE_IN_SECS`: one member doing real work (10 rps,
    /// no ack or store latency, no CPU hot), the members following the
    /// published count one tick later, every publication written. The
    /// publications, (second, count).
    fn replay() -> Vec<(i64, u64)> {
        let cli = crate::config::CliArgs::deterministic();
        let at = thresholds();
        let base = Instant::now();
        let mut gate = Gate::new(Duration::from_secs(cli.scale_in_secs));
        let mut demand = Demand::default();
        let mut published: Option<Desired> = None;
        let mut out = Vec::new();
        for t in (0..=1_100_i64).step_by(2) {
            let now = Now {
                at: base + Duration::from_secs(t.unsigned_abs()),
            };
            let (inflight, cores_used) = reading(t);
            let mut loads = Loads {
                cores_used,
                inflight,
                rps: 10.0,
                live: published.as_ref().map_or(1, |d| d.count),
                shards: SHARDS,
                ..Loads::default()
            };
            gate.hold(&mut loads, published.as_ref(), now);
            let plan = demand.plan(&loads, &at, now.at);
            if let Some(count) = gate.decide((plan.need, plan.shrink), published.as_ref(), now) {
                let epoch = published.as_ref().map_or(1, |d| d.epoch + 1);
                published = Some(Desired {
                    count,
                    reason: plan.reason,
                    epoch,
                    computed_at_ms: EPOCH_MS + t * 1_000,
                    pending_events: Vec::new(),
                });
                gate.published();
                out.push((t, count));
            }
        }
        out
    }

    /// The fleet tick's rule, as `fleet::start` applied it before it moved
    /// here: every spike the planner reads on one tick moves the count, and
    /// a shrink jumps to the conservative target a minute after the spike
    /// ends. This pins that the extraction changed no decision.
    #[test]
    fn the_trace_publishes_every_spike_and_every_trough() {
        assert_eq!(
            replay(),
            vec![(0, 1), (90, 3), (302, 7), (382, 5), (400, 7), (542, 1)]
        );
    }
}
