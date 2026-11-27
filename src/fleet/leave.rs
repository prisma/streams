//! The leave (§22 step 6: GATE-DECISION-s22-addendum-2026-11-19 C9-C11; GATE-DECISION-s22-join-
//! 2026-11-20 Addendum 41 B, as Addendum 44 §1 Q4 and §2 Q7 and Addendum 60 amend it).
//!
//! A member **leaves** while the document lists it with its own boot id and its ordinal is beyond
//! the count its pass read, and no drain has begun (the drain moves its shards itself). It keeps
//! serving and pinning (C7: a leaver is a listed member) while it:
//! 1. hands off every leaf its view gives it, giver first, to a successor (the drain's choice,
//!    `drain::successors`: the document's ring within the count, holding a qualifying beat of the
//!    listed boot, less itself; C11), the resident ones through the runner's move and the cold
//!    ones as the runner moves a leaf with no engine (D6);
//! 2. seals its open gate once it holds nothing, reversibly (`sharddir::leave`);
//! 3. is released by its own tick's pass: one CAS removes its name (`join`'s `Write::Released`),
//!    and the view without it follows with no await. Not while no member within the count is
//!    listed (Addendum 44 Q4).
//!
//! The phase (`LEAVE_PHASE`) runs on this instance's monotonic clock over its leaving passes, and
//! not while it has no successor or holds a shard the live topology intent names (Addendum 41 B
//! item 3). Past it the leave falls back: the gate refuses opens whatever is held, each held shard
//! is warned and counted (`note_evacuated`, `leave_evacuated`) and taken by fence or by this
//! instance's own yield, and the pass releases. A count that returns before the release ends the
//! leave; the seal lifts only once nothing is held (Addendum 60 §1), at the next publication.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::record::{Candidates, within_count};
use super::{AppState, Overrides};
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// How long a leaver's phase runs before it falls back: eight moves at their bound, as the drain's
/// handoff phase (`drain::HANDOFF_PHASE`), 32 s.
pub(super) const LEAVE_PHASE: Duration = super::handoff::BOUND.saturating_mul(8);

/// How often the leave's task looks for a leaf to hand off and for the seal: the tick's period, so
/// a leaver reads the document no more often than its tick does (C16).
const LOOK: Duration = super::planning::TICK_PERIOD;

/// This boot's leave: whether it is leaving, its phase's clock, and what its fallback gave up.
#[derive(Debug, Default)]
pub(crate) struct Leave {
    phase: Mutex<Option<Phase>>,
    evacuated: AtomicU64,
    /// The moves this boot's looks asked the runner for, since boot.
    asked: AtomicU64,
}

/// A leave's phase: how long its clock has run, and the pass it was last advanced at.
#[derive(Debug, Clone, Copy)]
struct Phase {
    ran: Duration,
    at: tokio::time::Instant,
}

impl Leave {
    /// A pass's verdict: `None` when it is not leaving, else whether the phase's clock `runs`
    /// since the last leaving pass. Whether the phase is over. A pass that is not leaving ends the
    /// leave, so a later one starts its clock again.
    fn note_pass(&self, leaving: Option<bool>, now: tokio::time::Instant) -> bool {
        let Ok(mut phase) = self.phase.lock() else {
            return false;
        };
        let Some(runs) = leaving else {
            *phase = None;
            return false;
        };
        let Phase { ran, at } = phase.get_or_insert(Phase {
            ran: Duration::ZERO,
            at: now,
        });
        if runs {
            *ran = ran.saturating_add(now.saturating_duration_since(*at));
        }
        *at = now;
        *ran >= LEAVE_PHASE
    }

    /// Whether this boot is leaving, as its last pass found.
    pub(crate) fn leaving(&self) -> bool {
        self.phase.lock().is_ok_and(|phase| phase.is_some())
    }

    /// Tests: the shards a fallback gave up, since boot (`leave_evacuated`).
    #[cfg(test)]
    pub(crate) fn evacuated(&self) -> u64 {
        self.evacuated.load(Ordering::Relaxed)
    }

    /// Tests: the moves this boot's looks asked the runner for, since boot.
    #[cfg(test)]
    pub(crate) fn asked(&self) -> u64 {
        self.asked.load(Ordering::Relaxed)
    }
}

/// The pass's leave step, before it plans (`join::pass`): whether this instance is leaving and,
/// if so, whether its gate is sealed. Past the phase it falls back first, so the plan releases.
pub(super) fn step(
    state: &AppState,
    candidates: &Candidates,
    document: &Overrides,
) -> Option<bool> {
    let standing = state.fleet.standing();
    let listed = document.members.as_deref();
    let leaving = candidates.is_member(listed)
        && !within_count(candidates.me(), candidates.count())
        && !standing.drain_begun();
    let sources = standing.topology_sources();
    let held = state.shards.held_prefixes();
    let runs = !super::drain::successors(candidates, document).is_empty()
        && held.iter().all(|prefix| !sources.contains(prefix));
    let over = standing
        .leave()
        .note_pass(leaving.then_some(runs), tokio::time::Instant::now());
    if !leaving {
        return None;
    }
    if over && !state.shards.leave_sealed() {
        fall_back(state, &held);
    }
    Some(state.shards.leave_sealed())
}

/// The leave's fallback past its phase: opens are refused whatever is held, and each held shard is
/// warned and counted as given up, not handed off.
fn fall_back(state: &AppState, held: &[String]) {
    state.shards.stop_for_leave();
    tracing::warn!(?held, phase = ?LEAVE_PHASE, "leave: its phase ran out; releasing, the shards still held are taken by fence");
    state.shards.note_evacuated(held);
    let count = u64::try_from(held.len()).unwrap_or(u64::MAX);
    let leave = state.fleet.standing().leave();
    leave.evacuated.fetch_add(count, Ordering::Relaxed);
}

/// After a publication (`view::published`): lift the leave's seal once this instance is no
/// longer leaving (the count came back, or the view without it is published), and only once it
/// holds nothing (Addendum 60 §1). Publish, then lift: an open after a release is refused by
/// ownership, not by the gate.
pub(super) fn published(state: &AppState) {
    if state.shards.leave_sealed() && !state.fleet.standing().leave().leaving() {
        let lifted = state.shards.unseal_leave();
        tracing::info!(lifted, "leave: no longer leaving; the gate's leave seal");
    }
}

/// Start this runtime's leave task beside its handoff runner.
pub(super) fn start(state: Arc<AppState>, tasks: &TaskSupervisor) {
    if let Err(rejected) = tasks.spawn("fleet-leave", Policy::Noncritical, move |cancel| {
        run(state, cancel)
    }) {
        tracing::debug!(
            ?rejected,
            "runtime stopping: the fleet leave was not started"
        );
    }
}

/// A look every `LOOK` until cancelled.
async fn run(state: Arc<AppState>, cancel: Cancellation) -> TaskResult {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = tokio::time::sleep(LOOK) => {}
        }
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            () = look(&state) => {}
        }
    }
}

/// One look while leaving and no drain has begun: hand off every leaf the view gives this
/// instance, then seal once it holds nothing. With no successor, or the document unread, nothing
/// is moved and the next look tries again.
async fn look(state: &AppState) {
    let standing = state.fleet.standing();
    if !handing_off(state) {
        return;
    }
    let Ok((document, _)) = state.fleet.read_overrides().await else {
        tracing::warn!("leave: the overrides document was not read; no shard is handed off");
        return;
    };
    let successors = standing.successors(&document);
    if successors.is_empty() {
        return;
    }
    for prefix in owned(state) {
        // A drain begun mid-look moves the rest itself, and a count that came back ends the leave
        // (G5; Addendum 67 §2): no further leaf is asked for.
        if !handing_off(state) {
            return;
        }
        let to = successors[crate::ownership::ring_pick(&prefix, &successors)].clone();
        standing.leave().asked.fetch_add(1, Ordering::Relaxed);
        let change = super::handoff::Change::To(to);
        let outcome = super::handoff::departing_move(&state.fleet, &prefix, change).await;
        tracing::info!(shard = %prefix, ?outcome, "leave: handing a leaf off");
    }
    // Sealed only once the view gives it no leaf (each move publishes its document) and nothing is
    // held: only a stale view then routes here, answered `503 shard_closing` until it catches up.
    if owned(state).is_empty() && state.shards.seal_for_leave() {
        tracing::info!("leave: nothing held; the gate is sealed for the release");
    }
}

/// Whether a look hands off: this instance is leaving, no drain has begun (the drain moves its
/// shards itself, G5) and its gate is not sealed (it holds nothing).
fn handing_off(state: &AppState) -> bool {
    let standing = state.fleet.standing();
    let facts = (
        standing.leave().leaving(),
        standing.drain_begun(),
        state.shards.leave_sealed(),
    );
    hands_off(facts)
}

/// `handing_off` over its facts: (leaving, drain begun, sealed).
fn hands_off((leaving, drain_begun, sealed): (bool, bool, bool)) -> bool {
    leaving && !drain_begun && !sealed
}

/// The leaves this instance's view gives it.
fn owned(state: &AppState) -> Vec<String> {
    let me = state.config.cli.instance_name.as_str();
    let mine = |prefix: &&String| state.ownership.effective_owner(prefix).as_deref() == Some(me);
    state
        .shards
        .prefixes()
        .iter()
        .filter(mine)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
