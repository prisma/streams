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
        }
    }
}

impl Candidacy {
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
}
