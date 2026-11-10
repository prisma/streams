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
//! liveness, while its fencing still holds for as long as it runs. A beat
//! also says when this runtime drains, and which beat of each candidate its
//! current ownership view read, which a peer's planned drain waits on
//! (`fleet::drain`).
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::{AppState, cpu_time_secs, rss_bytes, standing};
use crate::shard::now_ms;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

mod shard_load;

/// How often this instance publishes its heartbeat.
pub(super) const PERIOD: Duration = Duration::from_secs(2);
/// Commits older than this no longer describe the durable-write cost; the
/// store's WAL-PUT summary covers the same window.
const ACK_WINDOW_MS: i64 = 15_000;
/// The last beat's bound. The supervisor joins its tasks within its own
/// grace (the ordered stop allows 10 s), so a store that no longer answers
/// delays the stop by this much at most.
const LAST_BEAT_DEADLINE: Duration = Duration::from_secs(3);

/// The heartbeat document, `fleet/<instance>.json`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Heartbeat {
    pub instance: String,
    /// When this beat was published (publisher clock). Its own supervised
    /// task publishes every beat, so its age is process liveness only
    /// (`fleet::heartbeat`).
    pub ts_ms: i64,
    pub rps: f64,
    /// p50 of commit durable-wait over the last 15 s across owned shards
    /// (ms). The latency dimension of the load vector (§4.2): rps alone
    /// deadlocks — a congested instance caps its own throughput signal.
    #[serde(default)]
    pub ack_p50_ms: f64,
    /// Measured process CPU (user+sys) over the last heartbeat interval,
    /// as % of one core. THE primary scaling signal: assumed-capacity
    /// constants go stale every time the engine changes speed (run 5
    /// scaled out at ~5 % utilization because SCALE_RPS_CAPACITY still
    /// described the pre-pacing engine). Utilization is workload- and
    /// version-independent.
    #[serde(default)]
    pub cpu_pct: f64,
    /// In-flight HTTP requests at heartbeat time / windowed peak since the
    /// last beat. Measures ADMITTED CONCURRENCY — the per-instance
    /// resource the platform edge actually bounds (runs 6–8: the fleet
    /// ceiling at 16–25 % CPU). Requests parked in a wait (long-polls,
    /// consumer pulls, watch waits) are not counted (shared cells M1).
    #[serde(default)]
    pub inflight: i64,
    #[serde(default)]
    pub inflight_peak: i64,
    /// Resident set size (MB). The 1 GB boxes die at ~RSS cap (the
    /// block-cache epidemic); scaling and alarms need to see it.
    #[serde(default)]
    pub rss_mb: f64,
    /// O14a: WAL PUT latency at the object_store client over the last 15 s
    /// (ms) — the durable-commit path's raw store cost, same window as
    /// ack_p50_ms so excursions correlate sample-for-sample.
    #[serde(default)]
    pub wal_put_p50_ms: u64,
    #[serde(default)]
    pub wal_put_p99_ms: u64,
    /// Outbound object-store ops in flight now / peak. The platform egress
    /// budget (~50 concurrent per instance) gates these; a pinned peak
    /// during an ack excursion is the egress-exhaustion signature.
    #[serde(default)]
    pub out_inflight: i64,
    #[serde(default)]
    pub out_inflight_peak: i64,
    /// Refusals more instances could relieve since the previous beat (item
    /// 6, #149): reads refused for read memory (429
    /// `container_memory_pressure`, 503 `read_memory_busy`), projects the
    /// tracker refused (503 `container_tracker_capacity`), and admission's
    /// in-flight, RSS and survival sheds. A quota, per-stream, wedged-shard
    /// or fair shed is not one: another instance would not relieve it. The
    /// resources the tiny-stream ramp exhausted while CPU stayed low.
    #[serde(default)]
    pub refusals: u64,
    /// Consecutive beats, this one included, that each counted a refusal.
    /// The publisher counts them, so a peer that reads one beat in three
    /// judges the streak whole (`planning::Demand`).
    #[serde(default)]
    pub refusal_beats: u32,
    /// Stream descriptors this instance's descriptor cache holds (of
    /// `REGISTRY_CACHE_MAX`): an instance resource the fleet scales on.
    #[serde(default)]
    pub cached_streams: u64,
    /// Stream handles resident across the owned shards, and in the fullest
    /// one (of `HANDLE_MAX_RESIDENT`, a per-shard limit): a shard's fill,
    /// which shard split (item 7) reads.
    #[serde(default)]
    pub resident_streams: u64,
    #[serde(default)]
    pub resident_streams_max_shard: u64,
    /// Active projects the quota tracker holds (of `MAX_TRACKED_PROJECTS`).
    #[serde(default)]
    pub tracked_projects: u64,
    /// The touch path's store-read latency over the last 15 s (ms): GETs
    /// of data objects, 0 below a sustained sample.
    #[serde(default)]
    pub store_read_p50_ms: u64,
    #[serde(default)]
    pub store_read_p99_ms: u64,
    /// How far the publisher's last desired count ran past the topology's
    /// shards: the split demand shard split (item 7) reads; 0 while the
    /// shards cover the fleet.
    #[serde(default)]
    pub split_demand: u64,
    /// Why the topology policy's split waits (`instance_full`: no room for both children
    /// beside the parent, or no other instance with room for one); absent while none waits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_refused: Option<String>,
    /// The shards the ring gives this instance, as its topology policy counts them.
    #[serde(default)]
    pub shards_resident: u32,
    /// The topology policy's latest decision as its segments (E6): `split prefix=… kind=…
    /// doubling=… fleet_axis=…`, `instance_full shards_per_instance=<n>/<max>`, or `hold=<name>`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub topology_decision: String,
    pub owned_shards: Vec<String>,
    /// The publisher is handing its ownership off in a planned drain: every
    /// ring drops it while it keeps beating (`fleet::drain`).
    pub draining: bool,
    /// The longest maintenance stall across owned shards (s): seconds since
    /// a shard's sealer last retired unsealed bytes while some are held
    /// (layout 6). The rebalance signal: sustained > REBALANCE_LAG_SECS
    /// means this host cannot keep up with its shards' internal machinery
    /// and one should move (SCALING.md §4).
    #[serde(default)]
    pub absorb_lag_max_secs: u64,
    /// Worst wedge across owned shards (ms): blocked commit write or
    /// stale durability. Complements absorb lag as the rebalance signal —
    /// a backpressured shard sheds appends BEFORE they commit, so lag
    /// (age of committed-but-unabsorbed bytes) never grows on a wedged
    /// instance (ladder p4b D3: zero-flow vacuous run).
    #[serde(default)]
    pub wedge_max_ms: i64,
    /// This instance's own base URL (SELF_URL env), published so peers
    /// can fan segment-scoped work out to the owner — cross-owner
    /// lineage reads and consumer sweeps need an address, and only the
    /// platform (not the instance's peers) knows it otherwise.
    #[serde(default)]
    pub url: String,
    /// The publishing runtime's boot identity: Compute reuses ordinal
    /// names across deploys, so the name alone cannot tell two processes
    /// apart.
    #[serde(default)]
    pub boot_id: String,
    /// How long before this beat the publisher's fleet tick last completed
    /// a pass (monotonic): controller progress, judged apart from liveness
    /// (`planning::active_members`). Absent from a version whose tick
    /// stamped the heartbeat itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_age_ms: Option<u64>,
    /// Why this instance takes no new ownership (instance-wide withdrawal:
    /// a lost Critical loop, a cell failure, or the runtime stopping);
    /// absent while it may. Every ring drops a withdrawn instance, its own
    /// included, while its heartbeat stays live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawn: Option<String>,
    /// This beat's sequence number within its boot.
    #[serde(default)]
    pub seq: u64,
    /// Which draining beat of each draining candidate the publisher's
    /// current ownership view read and leaves out. A peer's planned drain
    /// waits until every peer's view has read one of its draining beats and
    /// left it out (`fleet::drain`). It holds only live draining candidates,
    /// so it stays small whatever the fleet's size. Empty from an earlier
    /// version.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub viewed: standing::Viewed,
    /// The `op_id` of the cell's live topology intent as this instance last
    /// read it (`topology::intent::Watch`; `unreadable` when the read
    /// failed): while any member reports one the fleet does not shrink
    /// (S4-DESIGN §10.1). Absent with none, and from an earlier version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_intent: Option<String>,
    /// The shards this instance holds in reserve for its own live split
    /// (S4-DESIGN §4.4, §10): two, its children, from the intent's creation
    /// until it is terminal; the rebalancers count them as held. Zero
    /// otherwise, and from an earlier version.
    #[serde(default)]
    pub reserved_shards: u32,
    /// Each owned shard's load: the topology policy's split and merge lines (S5).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shard_loads: Vec<shard_load::ShardLoad>,
}

/// Start this runtime's heartbeat beside its fleet tick. It joins the fleet
/// first: the tick reads this instance's heartbeat beside the members it
/// names (`FleetRepository::join`).
pub(super) fn start(state: Arc<AppState>, instance: String, tasks: &TaskSupervisor) {
    state.fleet.join(&instance);
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
    let mut sampler = Sampler::starting(
        Instant::now(),
        state.admission.fleet_ops(),
        cpu_time_secs(),
        instance_refusals(&state),
    );
    let mut beats = tokio::time::interval(PERIOD);
    beats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The interval's first tick is immediate; the first beat waits a period
    // so its rates cover one. Meanwhile the drain marker every tick
    // revalidates is created if absent (`repository::drains`).
    beats.tick().await;
    tokio::select! {
        _ = cancel.cancelled() => return TaskResult::Done,
        () = state.fleet.create_drain_marker() => {}
    }
    let mut intent = crate::topology::intent::Watch::default();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = beats.tick() => {}
            () = state.fleet.standing().beat_requested() => {}
        }
        let live = tokio::select! {
            _ = cancel.cancelled() => break,
            live = intent.live(&*state.data_store) => live,
        };
        open_unopened_sources(&state, live.as_ref()).await;
        let sources = live.as_ref().map(|l| l.sources.clone()).unwrap_or_default();
        state.fleet.standing().set_topology_sources(sources);
        let reserved_shards = reserved_by(live.as_ref(), &instance);
        let topology_intent = live.map(|l| l.op_id);
        sampler.observe(
            Instant::now(),
            state.admission.fleet_ops(),
            cpu_time_secs(),
            instance_refusals(&state),
        );
        sampler.top.observe(&state.shards.engines_by_prefix());
        let heartbeat = Heartbeat {
            topology_intent,
            reserved_shards,
            ..sampler.heartbeat(&state, &instance)
        };
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

/// The shards `instance` holds in reserve for `live` (S4-DESIGN §4.4, §10): a split's two
/// children while the instance is its actor, none for a merge (it lowers the count) or another
/// instance's operation.
/// Open each source of the live intent `live` that this instance owns and does not hold open
/// (TLA-048's liveness): only a source's opener claims a lapsed intent, and a source no request
/// reaches is otherwise never opened again (a merge's halves are cold by the policy's rule; an
/// actor that died, or ended `Unfinished` and dropped its barriers, left them closed). The open's
/// fence holds while the intent's lease is fresh, so nothing is decided under a live actor, whose
/// sources are open on its own instance anyway; once the lease lapsed it claims the intent and
/// rolls it back or forward. A prefix that is no leaf here (a published split's parent) is left
/// to the intent's reclaim.
async fn open_unopened_sources(state: &AppState, live: Option<&crate::topology::intent::Live>) {
    let Some(live) = live.filter(|live| live.op_id != crate::topology::intent::UNREADABLE) else {
        return;
    };
    let leaves = state.shards.prefixes();
    for prefix in &live.sources {
        let unopened = leaves.shards.leaves.contains_key(prefix)
            && state.ownership.is_mine(prefix)
            && !state.shards.is_open(prefix);
        if unopened {
            let outcome = state
                .shards
                .open_or_wait(prefix, Duration::from_secs(1))
                .await;
            let ready = matches!(outcome, crate::sharddir::OpenOutcome::Ready(_));
            tracing::info!(shard = %prefix, op_id = %live.op_id, ready, "opening a live intent's source for its fence");
        }
    }
}

fn reserved_by(live: Option<&crate::topology::intent::Live>, instance: &str) -> u32 {
    let ours = live.is_some_and(|live| live.kind == "split" && live.owner == instance);
    if ours { 2 } else { 0 }
}

/// The smoothed request and CPU rates, the last beat's read-memory
/// refusals and their streak, and the previous reading, so each beat's
/// rates and refusals span exactly one beat.
#[derive(Debug)]
struct Sampler {
    at: Instant,
    ops: u64,
    cpu_secs: f64,
    rps: f64,
    cpu_pct: f64,
    /// The read-memory refusal counter at the previous reading.
    refusals: u64,
    /// Refusals since the previous reading.
    refused: u64,
    /// Consecutive readings that each counted a refusal.
    refusal_beats: u32,
    /// Each owned shard's streams' tails at the last reading (S5's hot entity).
    top: shard_load::TopStreams,
}

impl Sampler {
    fn starting(at: Instant, ops: u64, cpu_secs: f64, refusals: u64) -> Self {
        Sampler {
            at,
            ops,
            cpu_secs,
            rps: 0.0,
            cpu_pct: 0.0,
            refusals,
            refused: 0,
            refusal_beats: 0,
            top: shard_load::TopStreams::default(),
        }
    }

    /// Fold the reading taken at `at` (the admitted-operation counter, the
    /// process CPU seconds and the read-memory refusal counter) into the
    /// smoothed rates and the refusal streak.
    fn observe(&mut self, at: Instant, ops: u64, cpu_secs: f64, refusals: u64) {
        let secs = at.duration_since(self.at).as_secs_f64().max(0.001);
        self.rps = ewma(self.rps, ops.saturating_sub(self.ops) as f64 / secs);
        self.cpu_pct = ewma(
            self.cpu_pct,
            ((cpu_secs - self.cpu_secs) / secs * 100.0).max(0.0),
        );
        self.refused = refusals.saturating_sub(self.refusals);
        self.refusal_beats = if self.refused > 0 {
            self.refusal_beats.saturating_add(1)
        } else {
            0
        };
        (self.at, self.ops, self.cpu_secs, self.refusals) = (at, ops, cpu_secs, refusals);
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
        // Active requests only: parked waits never ask for servers (M1).
        let (inflight, inflight_peak) = state.admission.swap_active_peak();
        let (wal_put_p50_ms, wal_put_p99_ms, out_inflight, out_inflight_peak) =
            crate::store_timing::heartbeat_summary();
        let (store_read_p50_ms, store_read_p99_ms) = crate::store_timing::store_read_summary();
        let residents: Vec<u64> = engines
            .iter()
            .map(|(_, engine)| engine.resident_streams() as u64)
            .collect();
        let ts_ms = now_ms();
        // The sequence first: a beat numbered from the drain's first on
        // reads the drain that began before its number was taken.
        let seq = state.fleet.standing().next_beat();
        let handle_max = state.config.cli.handle_max_resident;
        let shard_loads =
            shard_load::of_engines(&engines, handle_max, ts_ms, ACK_WINDOW_MS, &self.top);
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
            refusals: self.refused,
            refusal_beats: self.refusal_beats,
            cached_streams: state.registry.cache_len() as u64,
            resident_streams: residents.iter().sum(),
            resident_streams_max_shard: residents.iter().copied().max().unwrap_or(0),
            tracked_projects: state.quotas.stats().0 as u64,
            store_read_p50_ms,
            store_read_p99_ms,
            split_demand: state.fleet.standing().split_demand(),
            split_refused: state.fleet.standing().split_refused().map(str::to_owned),
            shards_resident: state.fleet.standing().shards_resident(),
            topology_decision: state.fleet.standing().topology_decision(),
            owned_shards: engines.into_iter().map(|(prefix, _)| prefix).collect(),
            draining: state.fleet.standing().draining(),
            absorb_lag_max_secs: state.shards.stall_max_secs(),
            wedge_max_ms,
            url: state.config.fleet.self_url.clone(),
            boot_id: state.runtime.identity.boot_id.clone(),
            progress_age_ms: state.fleet.standing().progress_age_ms(),
            withdrawn: state.withdrawal(),
            seq,
            viewed: left_out(state),
            topology_intent: None,
            reserved_shards: 0,
            shard_loads,
        }
    }
}

/// The draining beats this runtime's published view read, less any whose
/// instance the view still holds (the ring's fallback can): read before the
/// ring, so a view published in between can only hold an entry back.
fn left_out(state: &AppState) -> standing::Viewed {
    let mut viewed = state.fleet.standing().viewed();
    let ring = state.ownership.ring_active();
    viewed.retain(|instance, _| !ring.contains(instance));
    viewed
}

impl AppState {
    /// Why this runtime takes no new ownership, instance-wide; `None` while
    /// it may. It is the readiness verdict: a supervisor that is stopping
    /// or lost a Critical loop, or a shard directory that reports a cell
    /// failure, never opened a shard, or could not close one.
    fn withdrawal(&self) -> Option<String> {
        let tasks = self.tasks.readiness_reason();
        if tasks.as_deref() == Some(crate::tasks::DRAINING) {
            // A cell failure says more than the drain that is under way.
            return self.shards.unready_reason().or(tasks);
        }
        tasks.or_else(|| self.shards.unready_reason())
    }

    /// This runtime's own maintenance pressure as its rebalancer judges it
    /// at the tick: the most wedged shard it serves (prefix, ms) and its
    /// effective lag in seconds.
    pub(crate) fn fleet_pressure(&self) -> (String, i64, u64) {
        // A source of the live topology intent is never a lag move's shard (S4-DESIGN §4.4).
        let sources = self.fleet.standing().topology_sources();
        let (wedge_prefix, wedge_max_ms) = worst_wedge(
            self.shards
                .engines_by_prefix()
                .into_iter()
                .filter(|(prefix, _)| !sources.contains(prefix))
                .map(|(prefix, engine)| (prefix, engine.wedge_ms())),
        );
        let lag = effective_lag_secs(self.shards.stall_max_secs(), wedge_max_ms);
        (wedge_prefix, wedge_max_ms, lag)
    }
}

/// The most wedged shard of `wedges` (prefix, blocked commit write or stale
/// durability in ms), `("", 0)` when there is none.
fn worst_wedge(wedges: impl Iterator<Item = (String, i64)>) -> (String, i64) {
    wedges.max_by_key(|(_, wedge)| *wedge).unwrap_or_default()
}

/// A shard host's lag as the rebalancer judges it: the longest sealer
/// stall or the wedge, whichever is longer. A backpressured shard sheds
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

/// The refusals more instances could relieve, since boot: admission's and
/// the project tracker's.
pub(crate) fn instance_refusals(state: &AppState) -> u64 {
    state
        .admission
        .instance_refusals()
        .saturating_add(state.quotas.tracker_refusals())
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
        let mut sampler = Sampler::starting(start, 1_000, 10.0, 0);
        sampler.observe(start + Duration::from_secs(2), 1_100, 11.0, 0);
        assert_eq!((sampler.rps, sampler.cpu_pct), (50.0, 50.0));
        sampler.observe(start + Duration::from_secs(4), 1_300, 11.0, 0);
        assert_eq!((sampler.rps, sampler.cpu_pct), (70.0, 30.0));
    }

    /// Each beat publishes the refusals since the previous one, and how many
    /// beats in a row each counted one: the streak a peer that reads one beat
    /// in three judges whole.
    #[test]
    fn each_beat_counts_its_refusals_and_their_streak() {
        let start = Instant::now();
        let mut sampler = Sampler::starting(start, 0, 0.0, 5);
        let mut beats = Vec::new();
        for (beat, refusals) in [(1, 5), (2, 7), (3, 8), (4, 12), (5, 12), (6, 13)] {
            sampler.observe(start + Duration::from_secs(2 * beat), 0, 0.0, refusals);
            beats.push((sampler.refused, sampler.refusal_beats));
        }
        assert_eq!(beats, [(0, 0), (2, 1), (1, 2), (4, 3), (0, 0), (1, 1)]);
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
