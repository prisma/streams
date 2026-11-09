//! The fleet count's hysteresis (FLEET-HYSTERESIS.md, ruled in
//! FLEET-HYSTERESIS-REVIEW.md §2-§3): the in-flight requests the planner
//! reads, and when a planned count is published. The fleet tick
//! (`fleet::start`) folds its members' beats, has the gate hold the fold's
//! in-flight, plans (`planning::Demand::plan`), and asks the gate whether
//! this tick publishes. The gate is pure and takes the tick's clock, so a
//! replay drives it as the tick does.
//!
//! Every rule that more than one instance applies is timed from the
//! published document (`Desired::computed_at_ms`), which every publisher
//! reads: in package 2's ramp four instances published thirteen counts on
//! their own stale views, three of them within five seconds.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::Desired;
use super::planning::Loads;

/// How long an in-flight reading must last before it can grow the fleet
/// (H1): longer than one shard move's refusals, shorter than a move's
/// cooldown.
const SCALE_OUT_SUSTAIN_MS: i64 = 30_000;
/// A grow publishes only over a published count at least this old (H5):
/// the shrink-to-grow cooldown, and one grow per window however many
/// instances read the same demand.
const GROW_FLOOR_MS: i64 = 30_000;

/// A tick's clock, taken once a tick: this host's monotonic instant, for
/// what one instance measures alone, and the wall time the fleet documents
/// are stamped in, for what every publisher must agree on.
#[derive(Clone, Copy, Debug)]
pub(super) struct Now {
    pub(super) at: Instant,
    pub(super) ms: i64,
}

impl Now {
    pub(super) fn current() -> Self {
        Self {
            at: Instant::now(),
            ms: crate::shard::now_ms(),
        }
    }
}

/// The fleet count's hysteresis: the sustained in-flight the planner reads,
/// and when a planned count is published.
#[derive(Debug)]
pub(super) struct Gate {
    scale_in: Duration,
    /// How long after a published change the in-flight is not read (H6):
    /// the ring is moving shards, and their refused writes are not load.
    settle_ms: i64,
    /// Since when, on this instance's clock, the shrink target has stayed
    /// below the published count.
    below_since: Option<Instant>,
    /// The wall time the window samples from (the last change plus the
    /// settle), its first sample since then, and the trailing window's
    /// samples (wall ms, in-flight).
    window_from_ms: Option<i64>,
    first_ms: Option<i64>,
    samples: VecDeque<(i64, f64)>,
}

impl Gate {
    pub(super) fn new(scale_in: Duration, settle_secs: u64) -> Self {
        Self {
            scale_in,
            settle_ms: i64::try_from(settle_secs.saturating_mul(1_000)).unwrap_or(i64::MAX),
            below_since: None,
            window_from_ms: None,
            first_ms: None,
            samples: VecDeque::new(),
        }
    }

    /// Hold the in-flight the tick plans with (`Loads::inflight`) at the
    /// least the members reported over the trailing window (H1), sampled
    /// only from the settle after the published count's change (H6): 0
    /// until a full window holds. Utilization, occupancy and the sustained
    /// dimensions read the fold as it is.
    pub(super) fn hold(&mut self, loads: &mut Loads, cur: Option<&Desired>, now: Now) {
        let from = cur.map(|d| d.computed_at_ms.saturating_add(self.settle_ms));
        if from != self.window_from_ms {
            self.window_from_ms = from;
            self.first_ms = None;
            self.samples.clear();
        }
        loads.inflight = if from.is_some_and(|from| now.ms < from) {
            0.0
        } else {
            self.sustained(loads.inflight, now.ms)
        };
    }

    /// The least in-flight over the trailing window, with this tick's
    /// reading in it; 0 while the window is not yet full.
    fn sustained(&mut self, inflight: f64, now_ms: i64) -> f64 {
        let start = now_ms.saturating_sub(SCALE_OUT_SUSTAIN_MS);
        let first = *self.first_ms.get_or_insert(now_ms);
        self.samples.push_back((now_ms, inflight));
        while self.samples.front().is_some_and(|sample| sample.0 < start) {
            self.samples.pop_front();
        }
        if first > start {
            return 0.0;
        }
        self.samples
            .iter()
            .map(|sample| sample.1)
            .fold(f64::INFINITY, f64::min)
    }

    /// The count this tick publishes, if any, from the plan's grow and
    /// shrink targets (`planning::Need`) and the published document: the
    /// first count at once; a grow once the document is `GROW_FLOOR_MS` old;
    /// a shrink to the conservative target once it has stayed below the
    /// published count for `SCALE_IN_SECS` on this instance's clock.
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
            (now.ms.saturating_sub(cur.computed_at_ms) >= GROW_FLOOR_MS).then_some(need)
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

    /// A published change restarts the window (H1, H6): what the members
    /// reported before it counts for nothing after it, so the planner reads
    /// 0 through the settle (60 s) and until a full window (30 s) holds
    /// after it, though 500 have been in flight all along.
    #[test]
    fn a_published_change_restarts_the_window_after_its_settle() {
        let base = Instant::now();
        let mut gate = Gate::new(Duration::from_secs(300), 60);
        let document = |at: i64| Desired {
            count: 2,
            reason: String::new(),
            epoch: 1,
            computed_at_ms: EPOCH_MS + at * 1_000,
            pending_events: Vec::new(),
        };
        let mut held = |cur: &Desired, t: i64| {
            let mut loads = Loads {
                inflight: 500.0,
                ..Loads::default()
            };
            let now = Now {
                at: base + Duration::from_secs(t.unsigned_abs()),
                ms: EPOCH_MS + t * 1_000,
            };
            gate.hold(&mut loads, Some(cur), now);
            (t, loads.inflight)
        };
        let first = document(0);
        let before: Vec<_> = (100..=200).step_by(2).map(|t| held(&first, t)).collect();
        assert_eq!(before.last(), Some(&(200, 500.0)), "a full window");
        let second = document(200);
        let after: Vec<_> = (202..=300).step_by(2).map(|t| held(&second, t)).collect();
        let read = after.iter().find(|(_, inflight)| *inflight > 0.0);
        assert_eq!(read, Some(&(290, 500.0)), "200 s + 60 s + 30 s");
    }

    /// A tick's clock `t` seconds into the test.
    fn at(base: Instant, t: i64) -> Now {
        Now {
            at: base + Duration::from_secs(t.unsigned_abs()),
            ms: EPOCH_MS + t * 1_000,
        }
    }

    /// A count of 4 published at the test's start.
    fn four() -> Desired {
        Desired {
            count: 4,
            reason: String::new(),
            epoch: 2,
            computed_at_ms: EPOCH_MS,
            pending_events: Vec::new(),
        }
    }

    /// A grow publishes over a document exactly 30 s old (H5), and not 2 s
    /// sooner.
    #[test]
    fn a_grow_publishes_over_a_document_30_s_old() {
        let base = Instant::now();
        let mut gate = Gate::new(Duration::from_secs(300), 60);
        let cur = four();
        assert_eq!(gate.decide((5, 4), Some(&cur), at(base, 28)), None);
        assert_eq!(gate.decide((5, 4), Some(&cur), at(base, 30)), Some(5));
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
    /// command line's `SCALE_IN_SECS` and the fleet's default move cooldown
    /// as the settle: one member doing real work (10 rps, no ack or store
    /// latency, no CPU hot), the members following the published count one
    /// tick later, every publication written. The publications, (second,
    /// count).
    fn replay() -> Vec<(i64, u64)> {
        let cli = crate::config::CliArgs::deterministic();
        let settle = crate::config::FleetConfig::default().rebalance_move_cooldown_secs;
        let at = thresholds();
        let base = Instant::now();
        let mut gate = Gate::new(Duration::from_secs(cli.scale_in_secs), settle);
        let mut demand = Demand::default();
        let mut published: Option<Desired> = None;
        let mut out = Vec::new();
        for t in (0..=1_100_i64).step_by(2) {
            let now = Now {
                at: base + Duration::from_secs(t.unsigned_abs()),
                ms: EPOCH_MS + t * 1_000,
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
                    computed_at_ms: now.ms,
                    pending_events: Vec::new(),
                });
                gate.published();
                out.push((t, count));
            }
        }
        out
    }

    /// The trace's spikes grow the fleet no more: the one-tick spike at
    /// 90 s and the ten-tick one at 302-320 s are shorter than the 30 s
    /// window, and the in-flight is not read for 60 s after each change.
    /// The sustained 300 in flight grows it to 3 once its window fills
    /// (182 s). Shrinks still jump to the conservative target a minute
    /// after the window goes blind on a change.
    #[test]
    fn only_in_flight_that_lasts_grows_the_fleet() {
        assert_eq!(
            replay(),
            vec![(0, 1), (182, 3), (244, 1), (334, 3), (396, 2), (542, 1)]
        );
    }
}
