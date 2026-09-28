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
//! nothing (no shard, no open still running, no close unsettled) and every
//! peer whose views keep being published has published one that read one of
//! its draining beats and leaves it out (`Heartbeat::viewed`). Completion
//! compares this runtime's own boot and beat sequence, never one host's
//! clock with another's; which peers are waited for follows the ring's own
//! liveness rule, on this host's clock. A drain that runs out of budget
//! names what was still pending: a timeout is never reported as a handoff.
//!
//! A drain is begun only when the ring would keep another member within the
//! desired count and every peer it waits for honours a drain: a peer of an
//! earlier version publishes no beat sequence and would keep routing here,
//! so with one in the fleet the runtime stops without a handoff.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::planning::Candidacy;
use super::standing::ViewedBeat;
use super::{AppState, Heartbeat, heartbeat, planning, repository};
use crate::shard::now_ms;

/// The drain's bound, derived from what it waits for, each at its own bound:
/// its draining beat (a heartbeat PUT already in flight, then its own: two
/// document deadlines); a peer's view that read it (a pass in flight when
/// the beat landed, a tick period, and the next pass up to its publication:
/// two pass deadlines and a period, the same reasoning as the progress
/// deadline); that view's echo (a beat in flight, a beat period, then its
/// own PUT: two document deadlines and a period); and the closes of the
/// shards it yields (the ordered stop's close grace): 144 s.
pub(crate) const BUDGET: Duration = repository::DOCUMENT_DEADLINE
    .saturating_add(repository::DOCUMENT_DEADLINE)
    .saturating_add(planning::PASS_DEADLINE)
    .saturating_add(planning::TICK_PERIOD)
    .saturating_add(planning::PASS_DEADLINE)
    .saturating_add(repository::DOCUMENT_DEADLINE)
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
    /// Every peer whose views keep being published read the drain and leaves
    /// this instance out, and it holds nothing.
    HandedOff,
    /// No handoff was possible: the ring keeps no other member within the
    /// desired count (a fleet of one; every other member withdrawn,
    /// draining or stuck; every peer gone while it drained), or a peer is of
    /// an earlier version and would not honour a drain.
    NoPeer,
    /// Every peer read the drain, but a close this runtime still owes has
    /// failed: the drain cannot finish, and waiting would not change that.
    Failed { failures: Vec<String> },
    /// The budget ran out with this still pending (before anything was
    /// announced, if the fleet could not be read within a document deadline).
    TimedOut { pending: Vec<String> },
}

/// The drain the supervisor runs before a requested stop: drains, reports how
/// it ended, and records it.
pub(super) async fn run(state: Arc<AppState>, instance: String) {
    tracing::info!(%instance, budget = ?BUDGET, "planned drain: handing off ownership");
    let outcome = drain(&state, &instance, BUDGET).await;
    let (warning, line) = report(&outcome);
    if warning {
        tracing::warn!(%instance, "{line}");
    } else {
        tracing::info!(%instance, "{line}");
    }
    state.fleet.standing().record_drain(outcome);
}

/// How an outcome is reported: whether it is a warning, and its line. Only a
/// handoff says the drain completed.
fn report(outcome: &DrainOutcome) -> (bool, String) {
    match outcome {
        DrainOutcome::HandedOff => (
            false,
            "planned drain complete: every peer excludes this instance, which holds no shard"
                .into(),
        ),
        DrainOutcome::NoPeer => (
            true,
            "planned drain: no peer can take ownership through a drain; stopping without a handoff"
                .into(),
        ),
        DrainOutcome::Failed { failures } => (
            true,
            format!(
                "planned drain failed; stopping with ownership not cleanly handed off: {}",
                failures.join("; ")
            ),
        ),
        DrainOutcome::TimedOut { pending } => (
            true,
            format!(
                "planned drain timed out; stopping with ownership not handed off: {}",
                pending.join("; ")
            ),
        ),
    }
}

/// Drains this runtime (`instance`), bounded by `budget`.
pub(crate) async fn drain(state: &AppState, instance: &str, budget: Duration) -> DrainOutcome {
    let started = tokio::time::Instant::now();
    let deadline = started + budget;
    let announce_by = started + repository::DOCUMENT_DEADLINE;
    let standing = state.fleet.standing();
    loop {
        let pending = match observe(state, deadline).await {
            Ok((beats, count)) => {
                let now = now_ms();
                let candidacies: HashMap<String, Candidacy> = beats
                    .iter()
                    .map(|beat| (beat.instance.clone(), beat.candidacy(now)))
                    .collect();
                let waited = waited(&beats, instance, now);
                if !planning::another_member(count, instance, &candidacies)
                    || waited.iter().any(|peer| peer.seq == 0)
                {
                    return DrainOutcome::NoPeer;
                }
                if !standing.draining() {
                    standing.begin_draining();
                }
                let drained = ViewedBeat {
                    boot_id: state.runtime.identity.boot_id.clone(),
                    seq: standing.draining_from().unwrap_or(u64::MAX),
                };
                let mut pending = not_yet_excluded(&waited, instance, &drained);
                let holdings = holdings(state).await;
                if pending.is_empty() && holdings.only_failures() {
                    return DrainOutcome::Failed {
                        failures: holdings.pending(),
                    };
                }
                pending.extend(holdings.pending());
                pending
            }
            Err(unreadable)
                if !standing.draining() && tokio::time::Instant::now() >= announce_by =>
            {
                return DrainOutcome::TimedOut {
                    pending: vec![unreadable],
                };
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
        tokio::time::sleep(POLL.min(deadline.saturating_duration_since(now))).await;
    }
}

/// The heartbeat set and the desired count, each read within the deadline.
async fn observe(
    state: &AppState,
    deadline: tokio::time::Instant,
) -> Result<(Vec<Heartbeat>, u64), String> {
    let beats = match tokio::time::timeout_at(deadline, state.fleet.peek_heartbeat_set()).await {
        Ok(Ok(beats)) => beats,
        Ok(Err(error)) => return Err(format!("fleet heartbeats unreadable: {error}")),
        Err(_) => return Err("fleet heartbeats not read within the drain's budget".into()),
    };
    match tokio::time::timeout_at(deadline, state.fleet.read_desired_state()).await {
        Ok(Ok((desired, _))) => Ok((beats, desired.map_or(1, |desired| desired.count))),
        Ok(Err(error)) => Err(format!("fleet desired count unreadable: {error}")),
        Err(_) => Err("fleet desired count not read within the drain's budget".into()),
    }
}

/// The peers of `instance` whose views keep being published, as the ring
/// judges liveness at `now_ms`: whatever their own standing (withdrawn,
/// draining, above the count), their views still route, so the drain waits
/// for each to read it.
fn waited<'a>(beats: &'a [Heartbeat], instance: &str, now_ms: i64) -> Vec<&'a Heartbeat> {
    beats
        .iter()
        .filter(|beat| beat.instance != instance && beat.candidacy(now_ms).publishes_views())
        .collect()
}

/// Each peer whose published view has not read `instance`'s draining beat
/// `drained` (or a later one of the same boot) and left it out: it may still
/// route or assign here.
fn not_yet_excluded(waited: &[&Heartbeat], instance: &str, drained: &ViewedBeat) -> Vec<String> {
    waited
        .iter()
        .filter(|peer| {
            !peer
                .viewed
                .get(instance)
                .is_some_and(|seen| seen.boot_id == drained.boot_id && seen.seq >= drained.seq)
        })
        .map(|peer| format!("{} has not read this instance's drain", peer.instance))
        .collect()
}

/// What this runtime still holds, as one observation: the shards it serves,
/// and, as its open gate sees them under one lock, its opens still running
/// (reaping ones included) and how each close it owes has settled.
struct Holdings {
    held: Vec<String>,
    opening: usize,
    closes: Vec<Settled>,
}

/// How a close has settled: `None` while it has not.
type Settled = Option<Result<(), String>>;

async fn holdings(state: &AppState) -> Holdings {
    let (closing, opening) = state.shards.pending_work();
    let mut closes = Vec::with_capacity(closing.len());
    for engine in closing {
        closes.push(engine.settle(Duration::ZERO).await);
    }
    Holdings {
        held: state.shards.held_prefixes(),
        opening,
        closes,
    }
}

impl Holdings {
    /// Each shard still held, the opens still running, the closes not yet
    /// settled, and each failed close.
    fn pending(&self) -> Vec<String> {
        let mut pending: Vec<String> = self
            .held
            .iter()
            .map(|prefix| format!("shard {prefix} still held"))
            .collect();
        if self.opening > 0 {
            pending.push(format!("{} shard opens still running", self.opening));
        }
        let unsettled = self
            .closes
            .iter()
            .filter(|settled| settled.is_none())
            .count();
        if unsettled > 0 {
            pending.push(format!("{unsettled} shard closes not settled"));
        }
        for settled in &self.closes {
            if let Some(Err(error)) = settled {
                pending.push(format!("a shard close failed: {error}"));
            }
        }
        pending
    }

    /// Whether what remains is only failed closes: final, so waiting longer
    /// cannot finish the drain.
    fn only_failures(&self) -> bool {
        self.held.is_empty()
            && self.opening == 0
            && self.closes.iter().all(Option::is_some)
            && self
                .closes
                .iter()
                .any(|settled| matches!(settled, Some(Err(_))))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BUDGET, DrainOutcome, Holdings, SUPERVISED, ViewedBeat, not_yet_excluded, report, waited,
    };
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
    fn the_budget_is_what_the_drain_waits_for_each_at_its_bound() {
        assert_eq!(
            BUDGET,
            Duration::from_secs((10 + 10) + (45 + 2 + 45) + (10 + 2 + 10) + 10)
        );
        assert_eq!(SUPERVISED, BUDGET + Duration::from_secs(1));
    }

    #[test]
    fn every_peer_whose_views_keep_coming_is_waited_for_whatever_its_standing() {
        let beats = [
            beat("streams-1", ""),
            beat("streams-2", ""),
            beat("streams-3", r#","withdrawn":"critical task terminated""#),
            beat("streams-4", r#","progress_age_ms":999999999"#),
            serde_json::from_str(&format!(
                r#"{{"instance":"streams-5","ts_ms":{NOW},"rps":0.0,"owned_shards":[],"draining":true,"seq":1}}"#
            ))
            .unwrap(),
        ];
        let names: Vec<&str> = waited(&beats, "streams-1", NOW)
            .iter()
            .map(|peer| peer.instance.as_str())
            .collect();
        assert_eq!(names, ["streams-2", "streams-3", "streams-5"]);
        assert!(
            waited(&beats, "streams-1", NOW + 30_000).is_empty(),
            "dark peers are not waited for"
        );
    }

    #[test]
    fn a_peer_is_pending_until_its_view_read_this_boots_drain_and_left_it_out() {
        let viewed = |boot: &str, seq: u64| {
            format!(r#","viewed":{{"streams-1":{{"boot_id":"{boot}","seq":{seq}}}}}"#)
        };
        let beats = [
            beat("streams-2", &viewed("b", 4)),
            beat("streams-3", &viewed("b", 5)),
            beat("streams-4", &viewed("b", 9)),
            beat("streams-5", &viewed("a", 9)),
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
                "streams-5 has not read this instance's drain",
                "streams-6 has not read this instance's drain"
            ]
        );
    }

    #[test]
    fn anything_still_held_opening_or_closing_is_pending_and_a_failure_is_final() {
        let busy = Holdings {
            held: vec!["p1".into()],
            opening: 2,
            closes: vec![None, Some(Err("fenced write failed".into())), Some(Ok(()))],
        };
        assert_eq!(
            busy.pending(),
            [
                "shard p1 still held",
                "2 shard opens still running",
                "1 shard closes not settled",
                "a shard close failed: fenced write failed"
            ]
        );
        assert!(!busy.only_failures());
        let failed = Holdings {
            held: Vec::new(),
            opening: 0,
            closes: vec![Some(Ok(())), Some(Err("x".into()))],
        };
        assert!(failed.only_failures());
        let settled = Holdings {
            held: Vec::new(),
            opening: 0,
            closes: vec![Some(Ok(()))],
        };
        assert!(settled.pending().is_empty());
        assert!(!settled.only_failures());
        for unfinished in [
            Holdings {
                held: vec!["p".into()],
                opening: 0,
                closes: vec![Some(Err("x".into()))],
            },
            Holdings {
                held: Vec::new(),
                opening: 1,
                closes: vec![Some(Err("x".into()))],
            },
            Holdings {
                held: Vec::new(),
                opening: 0,
                closes: vec![None, Some(Err("x".into()))],
            },
        ] {
            assert!(!unfinished.only_failures());
        }
    }

    #[test]
    fn only_a_handoff_is_reported_as_complete() {
        let (warning, line) = report(&DrainOutcome::HandedOff);
        assert!(!warning && line.contains("complete"));
        for outcome in [
            DrainOutcome::NoPeer,
            DrainOutcome::Failed {
                failures: vec!["a shard close failed: x".into()],
            },
            DrainOutcome::TimedOut {
                pending: vec![
                    "streams-2 has not read this instance's drain".into(),
                    "shard p still held".into(),
                ],
            },
        ] {
            let (warning, line) = report(&outcome);
            assert!(warning, "{line}");
            assert!(!line.contains("complete"), "{line}");
            let items = match &outcome {
                DrainOutcome::Failed { failures } => failures.clone(),
                DrainOutcome::TimedOut { pending } => pending.clone(),
                _ => Vec::new(),
            };
            assert!(
                items.iter().all(|item| line.contains(item.as_str())),
                "{line}"
            );
        }
        assert!(
            report(&DrainOutcome::TimedOut {
                pending: vec!["p".into()]
            })
            .1
            .contains("timed out")
        );
        assert!(report(&DrainOutcome::NoPeer).1.contains("no peer"));
        assert!(
            report(&DrainOutcome::Failed {
                failures: vec!["p".into()]
            })
            .1
            .contains("failed")
        );
    }
}
