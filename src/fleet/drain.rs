//! This runtime's planned drain (item 40, the owner's decision). A requested
//! stop runs it before any loop is cancelled (`tasks::drain`), so the
//! runtime keeps its liveness and its fencing until its ownership has
//! moved; it never drops its heartbeat first and leaves its peers to
//! presume it dead.
//!
//! The drain publishes `draining` at once and keeps beating. Every ring, its
//! own included, then drops this instance: its tick yields what it holds,
//! those engines close, and the owners the ring names now open those shards,
//! fencing this writer. The drain is complete only when this runtime holds
//! nothing (no shard, no open in flight, no close unsettled or failed) and
//! every peer that takes ownership has published a view that read one of its
//! draining beats and leaves it out. Each peer echoes its view (`ring`) and
//! which draining beat of each candidate that view read (`viewed`); this
//! runtime compares those with its own boot and beat sequence, so no two
//! hosts' clocks are ever compared. A drain that runs out of budget names
//! what was still pending: a timeout is never reported as a handoff.
//!
//! A drain is begun only when every peer that takes ownership honours one:
//! a peer of an earlier version publishes no beat sequence and would keep
//! routing here, so with one in the fleet the runtime stops as it did before
//! drains existed.
use std::sync::Arc;
use std::time::Duration;

use super::standing::ViewedBeat;
use super::{AppState, Heartbeat, heartbeat, planning, repository};
use crate::shard::{EngineShutdown, now_ms};

/// The drain's bound, derived from what it waits for: its draining beat's
/// publication (one document deadline), a peer pass that reads it (a tick
/// period and a pass deadline), that peer's next beat (a beat period and a
/// document deadline), and the closes of the shards it yields (the ordered
/// stop's close grace, `SHARD_CLOSE_GRACE`): 79 s.
pub(crate) const BUDGET: Duration = repository::DOCUMENT_DEADLINE
    .saturating_add(planning::TICK_PERIOD)
    .saturating_add(planning::PASS_DEADLINE)
    .saturating_add(heartbeat::PERIOD)
    .saturating_add(repository::DOCUMENT_DEADLINE)
    .saturating_add(SHARD_CLOSE_GRACE);

/// The supervisor's bound on the drain: its budget, and a second in which to
/// record how it ended.
pub(super) const SUPERVISED: Duration = BUDGET.saturating_add(Duration::from_secs(1));

/// The close grace `bootstrap::run`'s ordered stop gives the shards.
const SHARD_CLOSE_GRACE: Duration = Duration::from_secs(10);

/// How often the drain looks again.
const POLL: Duration = Duration::from_millis(250);

/// How a planned drain ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    /// Every peer that takes ownership read the drain and leaves this
    /// instance out, and it holds nothing.
    HandedOff,
    /// No peer can take its ownership through a drain: none takes ownership
    /// (a fleet of one, every peer withdrawn or draining), or one is of an
    /// earlier version that would not honour it. Nothing was handed off.
    NoPeer,
    /// The budget ran out with this still pending.
    TimedOut { pending: Vec<String> },
}

/// The drain the supervisor runs before a requested stop: drains, then
/// records how it ended.
pub(super) async fn run(state: Arc<AppState>, instance: String) {
    tracing::info!(%instance, budget = ?BUDGET, "planned drain: handing off ownership");
    match drain(&state, &instance, BUDGET).await {
        DrainOutcome::HandedOff => tracing::info!(
            %instance,
            "planned drain complete: every peer excludes this instance, which holds no shard"
        ),
        DrainOutcome::NoPeer => tracing::warn!(
            %instance,
            "planned drain: no peer can take ownership through a drain; stopping without a handoff"
        ),
        DrainOutcome::TimedOut { pending } => tracing::warn!(
            %instance,
            ?pending,
            "planned drain timed out; stopping with ownership not handed off"
        ),
    }
}

/// Drains this runtime (`instance`), bounded by `budget`.
pub(crate) async fn drain(state: &AppState, instance: &str, budget: Duration) -> DrainOutcome {
    let deadline = tokio::time::Instant::now() + budget;
    let standing = state.fleet.standing();
    let mut closing: Vec<(String, EngineShutdown)> = Vec::new();
    loop {
        let pending = match heartbeats(state, deadline).await {
            Ok(beats) => {
                let takers = takers(&beats, instance, now_ms());
                if !drainable(&takers) {
                    return DrainOutcome::NoPeer;
                }
                if !standing.draining() {
                    standing.begin_draining();
                }
                track(state, &mut closing);
                let drained = ViewedBeat {
                    boot_id: state.runtime.identity.boot_id.clone(),
                    seq: standing.draining_from().unwrap_or(u64::MAX),
                };
                let mut pending = not_yet_excluded(&takers, instance, &drained);
                pending.extend(still_held(state, &closing).await);
                pending
            }
            Err(unreadable) => vec![unreadable],
        };
        if pending.is_empty() {
            return DrainOutcome::HandedOff;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return DrainOutcome::TimedOut { pending };
        }
        tokio::time::sleep_until(deadline.min(now + POLL)).await;
    }
}

/// The heartbeat set, read within the drain's deadline.
async fn heartbeats(
    state: &AppState,
    deadline: tokio::time::Instant,
) -> Result<Vec<Heartbeat>, String> {
    match tokio::time::timeout_at(deadline, state.fleet.peek_heartbeat_set()).await {
        Ok(Ok(beats)) => Ok(beats),
        Ok(Err(error)) => Err(format!("fleet heartbeats unreadable: {error}")),
        Err(_) => Err("fleet heartbeats not read within the drain's budget".into()),
    }
}

/// The peers of `instance` that take ownership as the ring judges them at
/// `now_ms`.
fn takers<'a>(beats: &'a [Heartbeat], instance: &str, now_ms: i64) -> Vec<&'a Heartbeat> {
    beats
        .iter()
        .filter(|beat| beat.instance != instance && beat.candidacy(now_ms).takes_ownership())
        .collect()
}

/// Whether these takers can take ownership through a drain: there is one,
/// and each publishes a beat sequence (an earlier version publishes none and
/// would keep routing here).
fn drainable(takers: &[&Heartbeat]) -> bool {
    !takers.is_empty() && takers.iter().all(|peer| peer.seq > 0)
}

/// Every engine this runtime holds now, added to those the drain waits on.
fn track(state: &AppState, closing: &mut Vec<(String, EngineShutdown)>) {
    for (prefix, engine) in state.shards.engines_by_prefix() {
        if !closing.iter().any(|(held, _)| *held == prefix) {
            closing.push((prefix, engine.shutdown_handle()));
        }
    }
}

/// Each peer whose published view has not read `instance`'s draining beat
/// `drained` (or a later one of the same boot), or still includes it: it
/// may still route or assign here.
fn not_yet_excluded(takers: &[&Heartbeat], instance: &str, drained: &ViewedBeat) -> Vec<String> {
    takers
        .iter()
        .filter_map(|peer| {
            let read = peer
                .viewed
                .get(instance)
                .is_some_and(|seen| seen.boot_id == drained.boot_id && seen.seq >= drained.seq);
            if !read {
                Some(format!(
                    "{} has not read this instance's drain",
                    peer.instance
                ))
            } else if peer.ring.iter().any(|member| member == instance) {
                Some(format!(
                    "{} still has this instance in its view",
                    peer.instance
                ))
            } else {
                None
            }
        })
        .collect()
}

/// Each shard this runtime still holds or is opening, and each engine it
/// held whose close has not settled or failed.
async fn still_held(state: &AppState, closing: &[(String, EngineShutdown)]) -> Vec<String> {
    let mut held: Vec<String> = state
        .shards
        .held_prefixes()
        .into_iter()
        .map(|prefix| format!("shard {prefix} still held"))
        .collect();
    let opening = state.shards.open_stats()["in_flight"].as_u64().unwrap_or(0);
    if opening > 0 {
        held.push(format!("{opening} shard opens in flight"));
    }
    for (prefix, engine) in closing {
        match engine.settle(Duration::ZERO).await {
            None => held.push(format!("shard {prefix} still closing")),
            Some(Err(error)) => held.push(format!("shard {prefix} close failed: {error}")),
            Some(Ok(())) => {}
        }
    }
    held
}

#[cfg(test)]
mod tests {
    use super::{BUDGET, SUPERVISED, ViewedBeat, drainable, not_yet_excluded, takers};
    use crate::fleet::Heartbeat;
    use std::time::Duration;

    const NOW: i64 = 1_000_000;

    fn beat(instance: &str, extra: &str) -> Heartbeat {
        let document = format!(
            r#"{{"instance":"{instance}","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":false,"seq":1{extra}}}"#
        );
        serde_json::from_str(&document).unwrap()
    }

    #[test]
    fn the_budget_is_what_the_drain_waits_for() {
        assert_eq!(BUDGET, Duration::from_secs(10 + 2 + 45 + 2 + 10 + 10));
        assert_eq!(SUPERVISED, BUDGET + Duration::from_secs(1));
    }

    #[test]
    fn only_peers_that_take_ownership_are_waited_for() {
        let beats = [
            beat("streams-1", ""),
            beat("streams-2", ""),
            beat("streams-3", r#","withdrawn":"critical task terminated""#),
            beat("streams-4", r#","progress_age_ms":999999999"#),
        ];
        let waited: Vec<&str> = takers(&beats, "streams-1", NOW)
            .iter()
            .map(|peer| peer.instance.as_str())
            .collect();
        assert_eq!(waited, ["streams-2"]);
        assert!(takers(&beats[..1], "streams-1", NOW).is_empty());
    }

    #[test]
    fn a_drain_needs_a_taker_and_every_taker_of_this_version() {
        let current = beat("streams-2", "");
        let earlier: Heartbeat = serde_json::from_str(&format!(
            r#"{{"instance":"streams-3","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":false}}"#
        ))
        .unwrap();
        assert!(drainable(&[&current]));
        assert!(!drainable(&[]));
        assert!(!drainable(&[&current, &earlier]));
    }

    #[test]
    fn a_peer_is_pending_until_its_view_read_this_boots_drain_and_leaves_it_out() {
        let viewed = |boot: &str, seq: u64, ring: &str| {
            format!(
                r#","viewed":{{"streams-1":{{"boot_id":"{boot}","seq":{seq}}}}},"ring":[{ring}]"#
            )
        };
        let beats = [
            beat("streams-2", &viewed("b", 4, r#""streams-2""#)),
            beat("streams-3", &viewed("b", 5, r#""streams-3""#)),
            beat("streams-4", &viewed("b", 9, r#""streams-1","streams-4""#)),
            beat("streams-5", &viewed("a", 9, r#""streams-5""#)),
            beat("streams-6", ""),
        ];
        let peers: Vec<&Heartbeat> = beats.iter().collect();
        let drained = ViewedBeat {
            boot_id: "b".into(),
            seq: 5,
        };
        assert_eq!(
            not_yet_excluded(&peers, "streams-1", &drained),
            [
                "streams-2 has not read this instance's drain",
                "streams-4 still has this instance in its view",
                "streams-5 has not read this instance's drain",
                "streams-6 has not read this instance's drain"
            ]
        );
    }
}
