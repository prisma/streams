//! Pure fleet-view preparation. Required authority reads must all succeed
//! before the controller publishes the resulting ring and override snapshot.
use std::collections::HashMap;
use std::time::Duration;

/// The fleet tick's pacing (`fleet::start`): the sleep between passes, and
/// the deadline that abandons a pass whose store operations have not
/// finished (each bounded on its own, `repository::DOCUMENT_DEADLINE`).
const TICK_PERIOD_MS: u64 = 2_000;
const PASS_DEADLINE_MS: u64 = 45_000;
pub(super) const TICK_PERIOD: Duration = Duration::from_millis(TICK_PERIOD_MS);
pub(super) const PASS_DEADLINE: Duration = Duration::from_millis(PASS_DEADLINE_MS);

/// A peer keeps its place in the ring while its heartbeat is younger than
/// this on the reader's clock: fifteen beats of its own publisher
/// (`fleet::heartbeat`). This is process liveness only.
const RING_LIVENESS_MS: i64 = 30_000;

/// A candidate's controller must have completed a pass (published its
/// ownership view from a complete authority read) within this long of its
/// latest heartbeat. Derived from the budgets a pass may use, never tuned:
/// a pass publishes after its reads and may spend the rest of its deadline
/// on its moves, so two consecutive publications can lie two deadlines and
/// a period apart; one pass abandoned at its deadline (the next retries its
/// reads) adds a period and a deadline. The age is the publisher's own
/// monotonic measure, so neither host skew nor a stepped wall clock moves
/// the judgement.
const PROGRESS_DEADLINE_MS: u64 = 3 * PASS_DEADLINE_MS + 2 * TICK_PERIOD_MS;

/// What the ring planner reads from one published heartbeat: separate
/// facts, never inferred from traffic (an idle instance keeps its place).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Candidacy {
    /// The heartbeat's age on the reader's clock: is the process alive.
    age_ms: i64,
    /// How long before that heartbeat the publisher's fleet tick last
    /// completed a pass: is its controller progressing.
    progress_age_ms: u64,
    /// Whether it withdrew from new ownership, instance-wide.
    withdrawn: bool,
    /// Whether it is handing its ownership off in a planned drain.
    draining: bool,
    /// The shards it holds in reserve for its own live split (S4-DESIGN §4.4).
    reserved_shards: u32,
}

impl super::Heartbeat {
    /// This heartbeat as the ring planner judges it at `now_ms`. A version
    /// that publishes no progress stamped its heartbeat inside its tick, so
    /// there the stamp itself is the progress.
    pub(super) fn candidacy(&self, now_ms: i64) -> Candidacy {
        Candidacy {
            age_ms: now_ms - self.ts_ms,
            progress_age_ms: self.progress_age_ms.unwrap_or(0),
            withdrawn: self.withdrawn.is_some(),
            draining: self.draining,
            reserved_shards: self.reserved_shards,
        }
    }
}

impl Candidacy {
    /// The shards it holds in reserve for its own live split, which the rebalancers count as held.
    pub(super) fn reserved_shards(&self) -> u32 {
        self.reserved_shards
    }

    /// Whether a peer with this candidacy is a ring member: one that can
    /// take ownership over.
    pub(super) fn takes_ownership(&self) -> bool {
        member(false, Some(self))
    }

    /// Whether this heartbeat is live on the reader's clock.
    pub(super) fn live(&self) -> bool {
        self.age_ms < RING_LIVENESS_MS
    }

    /// Whether a peer's views keep being published: its heartbeat is live
    /// and its controller progresses. A drain waits for every such peer to
    /// read it, whatever the peer's own standing.
    pub(super) fn publishes_views(&self) -> bool {
        self.live() && self.progress_age_ms < PROGRESS_DEADLINE_MS
    }
}

/// Whether a ring of `count` keeps a member other than `instance`: another
/// of the first `count` ordinals takes ownership. Without one, every ring
/// falls back to the ordinal set, which holds `instance` again, so its
/// ownership cannot be handed off.
// mt-lint: allow(name-keyed-map): fleet instance -> its published candidacy
pub(super) fn another_member(
    count: u64,
    instance: &str,
    candidacies: &HashMap<String, Candidacy>,
) -> bool {
    (1..=count.max(1))
        .map(|index| format!("streams-{index}"))
        .any(|name| {
            name != instance
                && candidacies
                    .get(&name)
                    .is_some_and(Candidacy::takes_ownership)
        })
}

/// The ring's active members: the first `count` ordinal instances that have
/// neither withdrawn nor begun a planned drain, whose controller published a
/// completed pass within the progress deadline of their latest heartbeat
/// and, for a peer, whose heartbeat is still live.
/// This instance is running (its own tick is asking), so its liveness is not
/// judged, and it is kept when the listing missed its heartbeat. Every
/// instance judges every candidate, itself included, from the same
/// published documents, so their rings agree. An empty result falls back to
/// the unfiltered ordinal set (bootstrap: everyone asleep, the first request
/// must land).
// mt-lint: allow(name-keyed-map): fleet instance -> its published candidacy
pub(super) fn active_members(
    count: u64,
    instance: &str,
    candidacies: &HashMap<String, Candidacy>,
) -> Vec<String> {
    let ordinal: Vec<String> = (1..=count.max(1))
        .map(|index| format!("streams-{index}"))
        .collect();
    let active: Vec<String> = ordinal
        .iter()
        .filter(|name| member(name.as_str() == instance, candidacies.get(*name)))
        .cloned()
        .collect();
    if active.is_empty() { ordinal } else { active }
}

/// One ordinal candidate's ring membership (see `active_members`).
fn member(is_self: bool, candidacy: Option<&Candidacy>) -> bool {
    match candidacy {
        Some(candidacy) => {
            (is_self || candidacy.age_ms < RING_LIVENESS_MS)
                && candidacy.progress_age_ms < PROGRESS_DEADLINE_MS
                && !candidacy.withdrawn
                && !candidacy.draining
        }
        None => is_self,
    }
}

/// The published URL map's entries whose values are bare trusted origins;
/// every other entry is logged and dropped.
// mt-lint: allow(name-keyed-map): fleet instance -> validated peer base URL
pub(super) fn trusted_urls(
    map: HashMap<String, String>,
    policy: &crate::config::FleetConfig,
) -> HashMap<String, String> {
    map.into_iter()
        .filter(|(instance, url)| {
            let valid = super::valid_peer_url(url, policy);
            if !valid {
                tracing::warn!(%instance,"rejecting malformed peer URL from urls.json");
            }
            valid
        })
        .collect()
}

/// A member whose beats report refusals this many beats in a row counts as
/// hot (item 6, #149): the tiny-stream ramp refused reads and new projects
/// for minutes while every instance's CPU stayed under the cpu-hot line.
pub(super) const REFUSAL_BEATS_HOT: u32 = 3;
/// A member whose RSS is at this share of its shed line counts as hot (the
/// lead's ruling, 2026-11-03): an RSS sum would count every instance's idle
/// floor, so RSS is a per-member signal, like CPU.
pub(super) const RSS_HOT_SHARE: f64 = 0.8;

/// What the desired count is planned from: the tick's fold of its fresh,
/// non-draining peers' beats, the routers' edge latency, and the topology.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Loads {
    pub(super) cores_used: f64,
    pub(super) inflight: f64,
    pub(super) rps: f64,
    pub(super) live: u64,
    /// The highest latency (ack p50, or the touch path's store-read p50)
    /// and CPU of the members doing real work (≥ 5 rps).
    pub(super) max_loaded_p50: f64,
    pub(super) max_loaded_cpu: f64,
    pub(super) edge_p50: f64,
    /// The longest refusal streak any member reports, and the highest RSS
    /// (MB), whatever its rate.
    pub(super) max_refusal_beats: u32,
    pub(super) max_rss_mb: f64,
    /// The members' descriptor-cache entries and tracked projects, summed:
    /// the instance resources a fleet fills (occupancy).
    pub(super) cached_streams: u64,
    pub(super) tracked_projects: u64,
    /// The topology's shards: a fleet larger than them adds nothing.
    pub(super) shards: u64,
    /// A member reports a live topology intent: no shrink (S4-DESIGN §10.1).
    pub(super) topology_busy: bool,
    /// The most ring-resident shards of a member whose split waits for an instance (its
    /// topology policy's `instance_full`, S5 and Q4); `None` while no split waits.
    pub(super) instance_full: Option<u32>,
}

impl Loads {
    /// Fold one fresh, non-draining member's beat. The load-gated
    /// dimensions count only members doing real work (≥ 5 rps), so idle
    /// blips and cold starts (binary download, shard replay burn CPU) do not
    /// scale the fleet; refusals, RSS and occupancy count whatever the rate:
    /// a cell full of tiny streams fills at a low rate.
    pub(super) fn fold(&mut self, beat: &super::Heartbeat) {
        self.live += 1;
        self.rps += beat.rps;
        self.cores_used += beat.cpu_pct / 100.0;
        self.inflight += beat.inflight.max(0) as f64;
        self.max_refusal_beats = self.max_refusal_beats.max(beat.refusal_beats);
        self.max_rss_mb = self.max_rss_mb.max(beat.rss_mb);
        self.cached_streams = self.cached_streams.saturating_add(beat.cached_streams);
        self.tracked_projects = self.tracked_projects.saturating_add(beat.tracked_projects);
        self.topology_busy |= beat.topology_intent.is_some();
        if beat.split_refused.as_deref() == Some(crate::topology::policy_loop::INSTANCE_FULL) {
            self.instance_full = self.instance_full.max(Some(beat.shards_resident));
        }
        if beat.rps >= 5.0 {
            let store = beat.store_read_p50_ms as f64;
            self.max_loaded_p50 = self.max_loaded_p50.max(beat.ack_p50_ms).max(store);
            self.max_loaded_cpu = self.max_loaded_cpu.max(beat.cpu_pct);
        }
    }
}

/// The members' split reservations, as the fleet tick's candidacies carry them (S4-DESIGN §4.4).
pub(super) trait Reservations {
    /// The members of `active` that may receive a lag move: those holding no split reservation
    /// (the actor of a live split is about to hold two more shards).
    fn unreserved(&self, active: &[String]) -> Vec<String>;
    /// The shards `member` holds in reserve for its own live split.
    fn reserved_of(&self, member: &str) -> usize;
}

impl Reservations for HashMap<String, Candidacy> {
    fn unreserved(&self, active: &[String]) -> Vec<String> {
        let reserving = |m: &String| self.get(m).is_some_and(|c| c.reserved_shards > 0);
        active.iter().filter(|m| !reserving(m)).cloned().collect()
    }

    fn reserved_of(&self, member: &str) -> usize {
        let reserved = self.get(member).map_or(0, |c| c.reserved_shards);
        usize::try_from(reserved).unwrap_or(usize::MAX)
    }
}

/// The planning thresholds: `fleet::FleetCfg`'s, this instance's RSS shed
/// line, and the per-instance capacities every member of one build shares.
#[derive(Clone, Copy, Debug)]
pub(super) struct Thresholds {
    pub(super) target_util: f64,
    pub(super) scale_in_util: f64,
    pub(super) edge_slots: u64,
    pub(super) latency_ms: u64,
    pub(super) latency_sustain: Duration,
    pub(super) edge_latency_ms: u64,
    pub(super) hot_cpu_pct: f64,
    pub(super) cpu_sustain: Duration,
    pub(super) max: u64,
    /// `ADMIT_RSS_SHED_MB`; 0 = no shed line, no RSS signal.
    pub(super) rss_shed_mb: u64,
    /// `REGISTRY_CACHE_MAX` and `MAX_TRACKED_PROJECTS`.
    pub(super) cache_capacity: u64,
    pub(super) tracker_capacity: u64,
    /// `SHARDS_PER_INSTANCE_MAX` (Q4), for the reason a waiting split publishes.
    pub(super) shards_per_instance_max: u32,
}

/// A pass's plan: the count to grow to, the conservative count to shrink
/// to, how far the demand runs past the topology's shards, and the reason a
/// published count records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Need {
    pub(super) need: u64,
    pub(super) shrink: u64,
    /// The demand, within `FLEET_MAX`, past the shards: published on the
    /// heartbeat for shard split (item 7) to read; 0 while the shards cover
    /// it.
    pub(super) split_demand: u64,
    /// Each dimension's reading and what it wanted.
    pub(super) reason: String,
}

/// The desired-count planner's state across passes: when the sustained
/// dimensions began to breach.
#[derive(Debug, Default)]
pub(super) struct Demand {
    latency_since: Option<std::time::Instant>,
    hot_since: Option<std::time::Instant>,
}

impl Demand {
    /// The pass's plan at `now`, the maximum over the dimensions (§4.1/§4.2):
    /// - utilization (primary): ceil(cores in use / target_util), so scale-out
    ///   begins when the fleet nears 75 % of its maximum;
    /// - edge slots: in-flight requests against the per-instance admission
    ///   budget at target_util of it, before the edge queue forms;
    /// - latency: a sustained ack-p50 or store-read-p50 breach of a loaded
    ///   member wants one more (congestion that does not show as CPU, and a
    ///   slow or refused read is not an ack);
    /// - edge: a router-observed p50 breach wants one more;
    /// - hot (sustained): a loaded member at the cpu-hot line, one reporting
    ///   `REFUSAL_BEATS_HOT` beats of refusals in a row, or one whose RSS is
    ///   at `RSS_HOT_SHARE` of its shed line wants one more (item 6);
    /// - occupancy: the descriptor cache's and the tracker's fill over the
    ///   fleet, ceil(total / (target_util × capacity)) (item 6).
    ///
    /// The count is clamped to the topology's shards and to the fleet's
    /// maximum, and what the demand runs past the shards is the split
    /// demand. The shrink target uses the conservative divisor, and keeps
    /// the sustained dimensions, so a sustained refusal, RSS or latency
    /// breach blocks a shrink; edge congestion blocks it outright, since rps
    /// falls while clients queue at the edge, and so does a member's live
    /// topology intent (S4-DESIGN §10.1: a split or merge is never raced by a
    /// scale-in; scale-out is never blocked).
    pub(super) fn plan(&mut self, loads: &Loads, at: &Thresholds, now: std::time::Instant) -> Need {
        let live_plus_one = loads.live + 1;
        let need_util = ceil_count(loads.cores_used / at.target_util);
        let slots = |util: f64| {
            if at.edge_slots > 0 {
                ceil_count(loads.inflight / (util * at.edge_slots as f64).max(1.0))
            } else {
                0
            }
        };
        let latency = loads.max_loaded_p50 > at.latency_ms as f64;
        let need_latency = if sustained(&mut self.latency_since, latency, at.latency_sustain, now) {
            live_plus_one
        } else {
            0
        };
        let edge_hot = loads.edge_p50 > at.edge_latency_ms as f64 && loads.rps >= 5.0;
        let need_edge = if edge_hot { live_plus_one } else { 0 };
        let rss_hot =
            at.rss_shed_mb > 0 && loads.max_rss_mb >= RSS_HOT_SHARE * at.rss_shed_mb as f64;
        let hot = loads.max_loaded_cpu >= at.hot_cpu_pct
            || loads.max_refusal_beats >= REFUSAL_BEATS_HOT
            || rss_hot;
        let need_hot = if sustained(&mut self.hot_since, hot, at.cpu_sustain, now) {
            live_plus_one
        } else {
            0
        };
        let need_slots = slots(at.target_util);
        let need_occupancy = occupancy(loads, at, at.target_util);
        let need_instance = if loads.instance_full.is_some() {
            live_plus_one
        } else {
            0
        };
        let wanted = need_util
            .max(need_slots)
            .max(need_latency)
            .max(need_hot)
            .max(need_edge)
            .max(need_occupancy)
            .max(need_instance);
        let shards = if loads.shards == 0 {
            u64::MAX
        } else {
            loads.shards
        };
        // A split that waits for an instance wants the home of the child it adds: past the
        // clamp by that one shard (S4 §10.1 rule 4 otherwise stands).
        let waiting = need_instance.min(shards.saturating_add(1));
        let need = wanted.min(shards).max(waiting).clamp(1, at.max);
        let shrink = if edge_hot || loads.topology_busy {
            at.max // sentinel: never below the current count, so no shrink
        } else {
            ceil_count(loads.cores_used / at.scale_in_util)
                .max(slots(at.scale_in_util))
                .max(need_latency)
                .max(need_hot)
                .max(occupancy(loads, at, at.scale_in_util))
                .min(shards)
                .max(waiting)
                .clamp(1, at.max)
        };
        let split_demand = wanted.min(at.max).saturating_sub(shards);
        let l = loads;
        let mut reason = format!(
            "cores_used={:.2} util->{need_util} inflight={:.0} slots->{need_slots} hot_cpu={:.0}% refusal_beats={} rss_max={:.0}MB ({need_hot}) ack_p50={:.0}ms ({need_latency}) edge_p50={:.0}ms ({need_edge}) cached={} projects={} ({need_occupancy}) shards={} split->{split_demand} rps={:.0} live={}",
            l.cores_used,
            l.inflight,
            l.max_loaded_cpu,
            l.max_refusal_beats,
            l.max_rss_mb,
            l.max_loaded_p50,
            l.edge_p50,
            l.cached_streams,
            l.tracked_projects,
            l.shards,
            l.rps,
            l.live,
        );
        if let Some(resident) = l.instance_full {
            let max = at.shards_per_instance_max;
            reason.push_str(&format!(
                " shards_per_instance={resident}/{max} ({need_instance})"
            ));
        }
        Need {
            need,
            shrink,
            split_demand,
            reason,
        }
    }
}

/// The instances the fleet's filling resources want at `util` of each
/// instance's capacity: its descriptor-cache entries and its tracked
/// projects, each ceil(total / (util × capacity)), the larger.
fn occupancy(loads: &Loads, at: &Thresholds, util: f64) -> u64 {
    let over = |total: u64, capacity: u64| {
        if capacity == 0 {
            0
        } else {
            ceil_count(total as f64 / (util * capacity as f64))
        }
    };
    over(loads.cached_streams, at.cache_capacity)
        .max(over(loads.tracked_projects, at.tracker_capacity))
}

/// A desired count from a non-negative ratio: its ceiling.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "ceil_count; the desired counts are ceilings of non-negative ratios bounded by the fleet maximum, far below u64, and Rust float conversion saturates; checked conversions would only restate that bound"
)]
fn ceil_count(ratio: f64) -> u64 {
    ratio.ceil() as u64
}

/// Whether a breach (`breaching` now) has lasted `sustain`: `since` holds
/// when it began, and a pass that finds no breach clears it.
fn sustained(
    since: &mut Option<std::time::Instant>,
    breaching: bool,
    sustain: Duration,
    now: std::time::Instant,
) -> bool {
    if !breaching {
        *since = None;
        return false;
    }
    now.duration_since(*since.get_or_insert(now)) >= sustain
}

#[cfg(test)]
mod tests {
    use super::{
        Candidacy, PROGRESS_DEADLINE_MS, RING_LIVENESS_MS, active_members, another_member,
    };
    use std::collections::HashMap;

    const NOW: i64 = 1_000_000;

    /// `instance`'s candidacy from a published heartbeat `age_ms` old whose
    /// controller completed a pass `progress_age_ms` before it (`None`: a
    /// version that publishes no progress).
    fn beat(instance: &str, age_ms: i64, progress_age_ms: Option<u64>) -> (String, Candidacy) {
        let progress =
            progress_age_ms.map_or(String::new(), |age| format!(r#","progress_age_ms":{age}"#));
        let document = format!(
            r#"{{"instance":"{instance}","ts_ms":{},"rps":0.0,"owned_shards":[],"draining":false{progress}}}"#,
            NOW - age_ms
        );
        let heartbeat: crate::fleet::Heartbeat = serde_json::from_str(&document).unwrap();
        (instance.to_string(), heartbeat.candidacy(NOW))
    }

    fn ring(count: u64, beats: &[(String, Candidacy)]) -> Vec<String> {
        active_members(count, "streams-1", &beats.iter().cloned().collect())
    }

    #[test]
    fn a_live_peer_whose_controller_stopped_progressing_leaves_the_ring() {
        let progressing = [
            beat("streams-1", 0, Some(0)),
            beat("streams-2", 1_000, Some(PROGRESS_DEADLINE_MS - 1)),
        ];
        assert_eq!(ring(2, &progressing), ["streams-1", "streams-2"]);
        let stuck = [
            beat("streams-1", 0, Some(0)),
            beat("streams-2", 1_000, Some(PROGRESS_DEADLINE_MS)),
        ];
        assert_eq!(ring(2, &stuck), ["streams-1"]);
    }

    #[test]
    fn a_dark_peer_leaves_the_ring_whatever_its_last_progress() {
        let live = [
            beat("streams-1", 0, Some(0)),
            beat("streams-2", RING_LIVENESS_MS - 1, Some(0)),
        ];
        assert_eq!(ring(2, &live), ["streams-1", "streams-2"]);
        let dark = [
            beat("streams-1", 0, Some(0)),
            beat("streams-2", RING_LIVENESS_MS, Some(0)),
        ];
        assert_eq!(ring(2, &dark), ["streams-1"]);
        assert_eq!(ring(2, &[]), ["streams-1"], "an unlisted peer is not live");
    }

    #[test]
    fn this_instance_is_judged_on_its_progress_not_on_the_listing() {
        let stale_listing = [
            beat("streams-1", 2 * RING_LIVENESS_MS, Some(0)),
            beat("streams-2", 0, Some(0)),
        ];
        assert_eq!(ring(2, &stale_listing), ["streams-1", "streams-2"]);
        let unlisted = [beat("streams-2", 0, Some(0))];
        assert_eq!(ring(2, &unlisted), ["streams-1", "streams-2"]);
        let stuck = [
            beat("streams-1", 0, Some(PROGRESS_DEADLINE_MS)),
            beat("streams-2", 0, Some(0)),
        ];
        assert_eq!(
            ring(2, &stuck),
            ["streams-2"],
            "its peers judge it the same way"
        );
    }

    #[test]
    fn a_withdrawn_instance_leaves_every_ring_itself_included() {
        let withdrawn = |instance: &str| {
            let document = format!(
                r#"{{"instance":"{instance}","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":false,"withdrawn":"critical task terminated"}}"#
            );
            let heartbeat: crate::fleet::Heartbeat = serde_json::from_str(&document).unwrap();
            (instance.to_string(), heartbeat.candidacy(NOW))
        };
        let peer = [beat("streams-1", 0, Some(0)), withdrawn("streams-2")];
        assert_eq!(ring(2, &peer), ["streams-1"]);
        let this = [withdrawn("streams-1"), beat("streams-2", 0, Some(0))];
        assert_eq!(ring(2, &this), ["streams-2"]);
    }

    #[test]
    fn a_draining_instance_leaves_every_ring_itself_included() {
        let draining = |instance: &str| {
            let document = format!(
                r#"{{"instance":"{instance}","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":true}}"#
            );
            let heartbeat: crate::fleet::Heartbeat = serde_json::from_str(&document).unwrap();
            (instance.to_string(), heartbeat.candidacy(NOW))
        };
        let peer = [beat("streams-1", 0, Some(0)), draining("streams-2")];
        assert_eq!(ring(2, &peer), ["streams-1"]);
        assert!(!peer[1].1.takes_ownership());
        assert!(peer[0].1.takes_ownership());
        let this = [draining("streams-1"), beat("streams-2", 0, Some(0))];
        assert_eq!(ring(2, &this), ["streams-2"]);
    }

    #[test]
    fn a_handoff_needs_another_member_within_the_count() {
        let candidacies = |beats: &[(String, Candidacy)]| beats.iter().cloned().collect();
        let two = candidacies(&[beat("streams-1", 0, Some(0)), beat("streams-2", 0, Some(0))]);
        assert!(another_member(2, "streams-1", &two));
        assert!(
            !another_member(1, "streams-1", &two),
            "streams-2 is above the count"
        );
        assert!(another_member(1, "streams-2", &two));
        let stuck = candidacies(&[
            beat("streams-1", 0, Some(0)),
            beat("streams-2", 0, Some(PROGRESS_DEADLINE_MS)),
        ]);
        assert!(!another_member(2, "streams-1", &stuck));
        assert!(!another_member(2, "streams-1", &HashMap::new()));
    }

    #[test]
    fn a_live_progressing_peer_publishes_views_whatever_its_standing() {
        let peer = |extra: &str| {
            let document = format!(
                r#"{{"instance":"streams-2","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":false{extra}}}"#
            );
            let heartbeat: crate::fleet::Heartbeat = serde_json::from_str(&document).unwrap();
            heartbeat.candidacy(NOW)
        };
        assert!(peer("").publishes_views());
        assert!(peer(r#","withdrawn":"x""#).publishes_views());
        assert!(!peer(&format!(r#","progress_age_ms":{PROGRESS_DEADLINE_MS}"#)).publishes_views());
        assert!(beat("streams-2", RING_LIVENESS_MS - 1, Some(0)).1.live());
        assert!(!beat("streams-2", RING_LIVENESS_MS, Some(0)).1.live());
    }

    #[test]
    fn a_heartbeat_without_progress_is_its_own_progress() {
        let earlier_version = [
            beat("streams-1", 0, Some(0)),
            beat("streams-2", 1_000, None),
        ];
        assert_eq!(ring(2, &earlier_version), ["streams-1", "streams-2"]);
    }

    #[test]
    fn the_progress_deadline_is_three_pass_deadlines_and_two_periods() {
        assert_eq!(
            PROGRESS_DEADLINE_MS, 139_000,
            "the pilot's ring mirror (src/bin/pilot/lb.rs) repeats this value"
        );
    }

    #[test]
    fn a_ring_every_candidate_left_is_every_ordinal() {
        let none_eligible = [
            beat("streams-1", 0, Some(PROGRESS_DEADLINE_MS)),
            beat("streams-2", RING_LIVENESS_MS, Some(0)),
        ];
        assert_eq!(ring(2, &none_eligible), ["streams-1", "streams-2"]);
        assert_eq!(
            ring(0, &[]),
            ["streams-1"],
            "a ring has at least one ordinal"
        );
    }

    /// **The tick's moves honour a split's reservation** (S4-DESIGN §4.4). A member whose beat
    /// reserves two shards for its own split receives no lag move and counts the two as held in
    /// return-home's share; the others, and a member with no beat, reserve nothing.
    #[test]
    fn a_split_reservation_bars_a_lag_move_and_counts_as_held() {
        use super::Reservations;
        let reserving = |instance: &str, reserved: u32| {
            let document = format!(
                r#"{{"instance":"{instance}","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":false,"reserved_shards":{reserved}}}"#
            );
            let heartbeat: crate::fleet::Heartbeat = serde_json::from_str(&document).unwrap();
            (instance.to_string(), heartbeat.candidacy(NOW))
        };
        let candidacies: HashMap<String, Candidacy> =
            [reserving("streams-1", 0), reserving("streams-2", 2)]
                .into_iter()
                .collect();
        let active: Vec<String> = ["streams-1", "streams-2", "streams-3"]
            .map(str::to_string)
            .to_vec();
        assert_eq!(candidacies.unreserved(&active), ["streams-1", "streams-3"]);
        let held: Vec<usize> = active.iter().map(|m| candidacies.reserved_of(m)).collect();
        assert_eq!(held, [0, 2, 0]);
    }
}

#[cfg(test)]
mod waiting_tests;

#[cfg(test)]
mod demand_tests {
    use super::{Demand, Loads, Need, REFUSAL_BEATS_HOT, Thresholds};
    use std::time::{Duration, Instant};

    pub(super) fn thresholds() -> Thresholds {
        Thresholds {
            target_util: 0.75,
            scale_in_util: 0.5,
            edge_slots: 0,
            latency_ms: 250,
            latency_sustain: Duration::from_secs(20),
            edge_latency_ms: 400,
            hot_cpu_pct: 75.0,
            cpu_sustain: Duration::from_secs(20),
            max: 64,
            rss_shed_mb: 500,
            cache_capacity: 65_536,
            tracker_capacity: 32_768,
            shards_per_instance_max: 7,
        }
    }

    /// Four members at 30 % CPU: utilization alone wants two, and shrinks
    /// to three.
    pub(super) fn quiet(shards: u64) -> Loads {
        Loads {
            cores_used: 1.2,
            rps: 400.0,
            live: 4,
            max_loaded_cpu: 30.0,
            max_rss_mb: 200.0,
            shards,
            ..Loads::default()
        }
    }

    /// A plan's numbers: (need, shrink, split demand).
    pub(super) type Numbers = (u64, u64, u64);

    pub(super) fn n(plan: &Need) -> Numbers {
        (plan.need, plan.shrink, plan.split_demand)
    }

    /// `loads` planned at the start and once the sustain has passed.
    fn sustained(loads: &Loads) -> (Numbers, Numbers) {
        let at = thresholds();
        let start = Instant::now();
        let mut demand = Demand::default();
        let first = n(&demand.plan(loads, &at, start));
        (first, n(&demand.plan(loads, &at, start + at.cpu_sustain)))
    }

    /// A member refusing work on three beats in a row, or one whose RSS is
    /// at 80 % of its shed line, counts as hot like a cpu-hot one: once
    /// sustained the fleet wants one more than it has and will not shrink,
    /// though its CPU is at 30 %. Just under either line, nothing moves.
    #[test]
    fn refusals_or_rss_near_its_shed_line_count_as_hot_once_sustained() {
        let refusing = Loads {
            max_refusal_beats: REFUSAL_BEATS_HOT,
            ..quiet(64)
        };
        let full = Loads {
            max_rss_mb: 400.0,
            ..quiet(64)
        };
        for hot in [refusing, full] {
            assert_eq!(sustained(&hot), ((2, 3, 0), (5, 5, 0)));
        }
        let near = Loads {
            max_refusal_beats: REFUSAL_BEATS_HOT - 1,
            max_rss_mb: 399.0,
            ..quiet(64)
        };
        assert_eq!(sustained(&near), ((2, 3, 0), (2, 3, 0)));
        let unbounded = Thresholds {
            rss_shed_mb: 0,
            ..thresholds()
        };
        let mut demand = Demand::default();
        let start = Instant::now();
        demand.plan(&full, &unbounded, start);
        assert_eq!(
            n(&demand.plan(&full, &unbounded, start + unbounded.cpu_sustain)),
            (2, 3, 0)
        );
    }

    /// A fleet of idle members at 40 % of the shed line each is not full: RSS
    /// is judged per member, never summed.
    #[test]
    fn idle_members_rss_is_never_summed() {
        let idle = Loads {
            max_rss_mb: 200.0,
            live: 8,
            ..quiet(64)
        };
        assert_eq!(sustained(&idle).1, (2, 3, 0));
    }

    /// The fleet's descriptor-cache entries and tracked projects ask for
    /// ceil(total / (target × capacity)) whatever the rate, and shrink only
    /// below ceil(total / (scale-in × capacity)).
    #[test]
    fn filling_resources_ask_for_their_occupancy() {
        let caching = Loads {
            cached_streams: 180_000,
            rps: 0.0,
            ..quiet(64)
        };
        // 180,000 / (0.75 × 65,536) = 3.7; / (0.5 × 65,536) = 5.5.
        assert_eq!(sustained(&caching).0, (4, 6, 0));
        let tracking = Loads {
            tracked_projects: 100_000,
            ..quiet(64)
        };
        // 100,000 / (0.75 × 32,768) = 4.1; / (0.5 × 32,768) = 6.1.
        assert_eq!(sustained(&tracking).0, (5, 7, 0));
    }

    /// A loaded member whose touch-path store reads wait past the latency
    /// line, sustained, wants one more: a slow read is not an ack.
    #[test]
    fn a_sustained_store_wait_wants_one_more() {
        let mut slow = quiet(64);
        let mut beat: crate::fleet::Heartbeat = serde_json::from_str(
            r#"{"instance":"streams-2","ts_ms":0,"rps":40.0,"cpu_pct":30.0,"owned_shards":[],"draining":false,"store_read_p50_ms":300}"#,
        )
        .unwrap();
        slow.fold(&beat);
        assert_eq!(slow.max_loaded_p50, 300.0);
        assert_eq!(sustained(&slow), ((2, 3, 0), (6, 6, 0)));
        beat.rps = 1.0;
        let mut idle = quiet(64);
        idle.fold(&beat);
        assert_eq!(
            idle.max_loaded_p50, 0.0,
            "an idle member's reads are compaction's"
        );
    }

    /// A beat folds into the fleet's rate and in-flight sums exactly; a
    /// negative in-flight reading (a racing decrement) counts as none.
    #[test]
    fn the_fold_sums_each_members_rate_and_in_flight() {
        let beat = |rps: f64, inflight: i64| -> crate::fleet::Heartbeat {
            serde_json::from_str(&format!(
                r#"{{"instance":"streams-2","ts_ms":0,"rps":{rps},"cpu_pct":30.0,"inflight":{inflight},"owned_shards":[],"draining":false}}"#
            ))
            .unwrap()
        };
        let mut loads = Loads::default();
        for (rps, inflight) in [(40.0, 6), (2.0, 3), (3.0, -1)] {
            loads.fold(&beat(rps, inflight));
        }
        assert_eq!((loads.live, loads.rps, loads.inflight), (3, 45.0, 9.0));
    }

    /// In-flight requests ask for their share of the edge slots:
    /// ceil(in flight / (util × slots)), and a build with no slot budget has
    /// no such dimension.
    #[test]
    fn in_flight_asks_for_its_share_of_the_edge_slots() {
        let busy = Loads {
            inflight: 300.0,
            ..quiet(64)
        };
        let slotted = Thresholds {
            edge_slots: 64,
            ..thresholds()
        };
        let now = Instant::now();
        // 300 / (0.75 × 64) = 6.25; / (0.5 × 64) = 9.4.
        assert_eq!(n(&Demand::default().plan(&busy, &slotted, now)), (7, 10, 0));
        assert_eq!(
            n(&Demand::default().plan(&busy, &thresholds(), now)),
            (2, 3, 0)
        );
    }

    /// The latency line is breached only past it: a loaded p50 of exactly
    /// the line wants nothing, however long it lasts.
    #[test]
    fn the_latency_line_is_breached_only_past_it() {
        let on_the_line = Loads {
            max_loaded_p50: 250.0,
            ..quiet(64)
        };
        assert_eq!(sustained(&on_the_line), ((2, 3, 0), (2, 3, 0)));
        let past = Loads {
            max_loaded_p50: 251.0,
            ..quiet(64)
        };
        assert_eq!(sustained(&past), ((2, 3, 0), (5, 5, 0)));
    }

    /// The router's p50 past its line wants one more at once and blocks a
    /// shrink, at a rate of 5 rps or more; on the line, or at a lower rate
    /// (a blip), it does nothing.
    #[test]
    fn the_edge_line_is_breached_past_it_at_a_real_rate() {
        let at = thresholds();
        let now = Instant::now();
        let plan = |edge_p50: f64, rps: f64| {
            let loads = Loads {
                edge_p50,
                rps,
                ..quiet(64)
            };
            n(&Demand::default().plan(&loads, &at, now))
        };
        assert_eq!(plan(401.0, 5.0), (5, 64, 0));
        assert_eq!(plan(400.0, 5.0), (2, 3, 0));
        assert_eq!(plan(401.0, 4.9), (2, 3, 0));
    }

    /// **A live topology intent blocks a shrink, never a grow** (S4-DESIGN
    /// §10.1). Four quiet members want two and would shrink to three; with
    /// a fifth, idle member's beat naming a live intent, the shrink target is
    /// the sentinel that is never below the count (64), and the grow target
    /// stays two. The same beat naming none leaves the shrink at three.
    #[test]
    fn a_live_topology_intent_blocks_a_shrink_never_a_grow() {
        let beat = |intent: &str| -> crate::fleet::Heartbeat {
            serde_json::from_str(&format!(
                r#"{{"instance":"streams-5","ts_ms":0,"rps":0.0,"owned_shards":[],"draining":false{intent}}}"#
            ))
            .unwrap()
        };
        let plan = |intent: &str| {
            let mut loads = quiet(64);
            loads.fold(&beat(intent));
            n(&Demand::default().plan(&loads, &thresholds(), Instant::now()))
        };
        assert_eq!(plan(r#","topology_intent":"op-s""#), (2, 64, 0));
        assert_eq!(plan(""), (2, 3, 0));
    }

    /// A pass that finds no breach restarts the sustain.
    #[test]
    fn a_pass_without_the_breach_restarts_its_sustain() {
        let at = thresholds();
        let start = Instant::now();
        let mut demand = Demand::default();
        let hot = Loads {
            max_refusal_beats: 3,
            ..quiet(64)
        };
        demand.plan(&hot, &at, start);
        demand.plan(&quiet(64), &at, start + Duration::from_secs(10));
        let resumed = start + Duration::from_secs(11);
        assert_eq!(demand.plan(&hot, &at, resumed).need, 2);
        assert_eq!(demand.plan(&hot, &at, resumed + at.cpu_sustain).need, 5);
    }

    /// A fleet past its shards adds nothing: the count is clamped to the
    /// topology's shards, and what the demand runs past them, within the
    /// fleet's maximum, is the split demand.
    #[test]
    fn the_count_is_clamped_to_the_shards_and_the_rest_is_split_demand() {
        let hot = Loads {
            max_refusal_beats: 3,
            ..quiet(4)
        };
        assert_eq!(sustained(&hot).1, (4, 4, 1));
        let at = thresholds();
        let mut demand = Demand::default();
        let now = Instant::now();
        let busy = Loads {
            cores_used: 6.0,
            ..quiet(4)
        };
        assert_eq!(
            n(&demand.plan(&busy, &at, now)),
            (4, 4, 4),
            "8 wanted, 4 shards"
        );
        let one_shard = Loads {
            cached_streams: 10_000_000,
            shards: 1,
            ..quiet(1)
        };
        // 10M / 49,152 = 204 instances wanted; FLEET_MAX 64 bounds the split.
        assert_eq!(n(&demand.plan(&one_shard, &at, now)), (1, 1, 63));
        let unknown = Loads { shards: 0, ..busy };
        assert_eq!(
            n(&demand.plan(&unknown, &at, now)),
            (8, 12, 0),
            "no topology: no clamp"
        );
    }
}
