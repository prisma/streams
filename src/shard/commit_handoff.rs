#![warn(clippy::wildcard_enum_match_arm)]
//! Terminal ownership of transaction completion, guarded by `in_flight`.
//!
//! Retirement, applied publication/registration, no-write attachment and
//! durable dispatch linearize under this ONE mutex. `closed` is only the
//! admission/wakeup mirror; queue emptiness never overrides terminal state.
//! A dispatcher that claimed a remotely durable group before retirement owns
//! its completion. No later caller can register or release a successful reply.
//!
//! Lock order: dispatch_gate (where needed), then this mutex, then local
//! maintenance/stream/pressure mirrors. Never hold this mutex across await,
//! encoding, storage I/O, notification or callbacks. Close never takes the
//! dispatch gate. Claimed effects and rejected senders run after unlocking.
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::{DurableEffects, InFlightGroup, ShardEngine};

#[derive(Default)]
pub(super) struct CommitHandoff {
    terminal: bool,
    pending: Vec<InFlightGroup>,
}

pub(super) enum Attachment {
    Pending,
    Durable,
    Retired,
}

impl CommitHandoff {
    pub(super) fn pending(&self) -> &[InFlightGroup] {
        &self.pending
    }

    /// The returned registration slot borrows the guard through ALL applied
    /// mirror updates and insertion. Retirement cannot split that operation.
    pub(super) fn publication(&mut self) -> Option<&mut Vec<InFlightGroup>> {
        (!self.terminal).then_some(&mut self.pending)
    }

    pub(super) fn attach(&mut self, effects: &mut DurableEffects) -> Attachment {
        if self.terminal {
            Attachment::Retired
        } else if let Some(last) = self.pending.last_mut() {
            last.effects.acks.append(&mut effects.acks);
            last.effects.queue_acks.append(&mut effects.queue_acks);
            last.effects.native_acks.append(&mut effects.native_acks);
            Attachment::Pending
        } else {
            // The committer serializes writes/overlays. With the dispatch
            // gate also held, an open empty queue means every earlier applied
            // group completed its remote-durable publications.
            Attachment::Durable
        }
    }

    pub(super) fn take_durable(&mut self, sequence: u64) -> Vec<InFlightGroup> {
        if self.terminal {
            return Vec::new();
        }
        let split = self.pending.partition_point(|group| group.seq <= sequence);
        self.pending.drain(..split).collect()
    }

    /// The oldest pending group a dispatch through `sequence` must take.
    fn first_durable(&self, sequence: u64) -> Option<u64> {
        self.pending
            .first()
            .filter(|group| group.seq <= sequence)
            .map(|group| group.seq)
    }

    pub(super) fn retire(&mut self) -> Option<Vec<InFlightGroup>> {
        if self.terminal {
            return None;
        }
        self.terminal = true;
        Some(std::mem::take(&mut self.pending))
    }
}

/// The dispatch watchdog's view of one stall episode: the oldest durable
/// group it found pending, since when, the dispatch attempts counted then,
/// and whether the episode was warned about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Watch {
    seq: u64,
    since: Instant,
    attempts: u64,
    warned: bool,
}

/// What the watchdog does after a look.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Quiet,
    Warn,
    Stalled,
}

/// The watchdog's look at durable dispatch: `due` is the oldest pending
/// group the durable sequence covers now while nothing holds the dispatch
/// gate (None otherwise), `attempts` the dispatches attempted so far. The
/// same group still due with no dispatch attempted since counts on from
/// when it was first seen; any progress (another group, a dispatch
/// attempted, the gate held, nothing due) starts the episode over. An
/// episode is warned about once at `dispatch_warn` and stalled at
/// `dispatch_stall`.
fn dispatch_look(
    seen: Option<Watch>,
    due: Option<u64>,
    attempts: u64,
    now: Instant,
    s: &crate::shard::seal::SealSettings,
) -> (Option<Watch>, Verdict) {
    let Some(seq) = due else {
        return (None, Verdict::Quiet);
    };
    let watch = match seen {
        Some(w) if w.seq == seq && w.attempts == attempts => w,
        _ => Watch {
            seq,
            since: now,
            attempts,
            warned: false,
        },
    };
    let waited = now.duration_since(watch.since);
    if waited >= s.dispatch_stall {
        (Some(watch), Verdict::Stalled)
    } else if waited >= s.dispatch_warn && !watch.warned {
        (
            Some(Watch {
                warned: true,
                ..watch
            }),
            Verdict::Warn,
        )
    } else {
        (Some(watch), Verdict::Quiet)
    }
}

impl ShardEngine {
    /// Durable dispatch through `durable_seq`, counted for the watchdog
    /// (the tests' park and skip hooks sit in front of it).
    pub(super) async fn dispatch_checked(&self, durable_seq: u64) -> u32 {
        #[cfg(test)]
        let skipped = self.layout.seal.before_dispatch().await.then_some(0);
        #[cfg(not(test))]
        let skipped = None;
        match skipped {
            Some(acked) => acked,
            None => {
                self.layout
                    .seal
                    .counters
                    .dispatches
                    .fetch_add(1, Ordering::Relaxed);
                self.dispatch_durable(durable_seq).await
            }
        }
    }

    /// The sealer loop's watchdog over durable dispatch: a group the shard
    /// log's durable sequence covers that stays pending with no dispatch
    /// at all (none attempted, the gate never seen held) is warned about
    /// at `dispatch_warn`, and at `dispatch_stall` the engine retires,
    /// so every waiter is answered (a bug: the acker and the pump dispatch
    /// every durable group within moments).
    pub(super) fn watch_dispatch(&self, seen: &mut Option<Watch>) {
        let sp = &self.layout.seal;
        let durable = self.db.status().durable_seq;
        let idle = self.dispatch_gate.try_lock().is_ok();
        let due = self
            .in_flight
            .lock()
            .ok()
            .and_then(|handoff| handoff.first_durable(durable))
            .filter(|_| idle);
        let attempts = sp.counters.dispatches.load(Ordering::Relaxed);
        let (look, verdict) = dispatch_look(*seen, due, attempts, Instant::now(), &sp.settings());
        *seen = look;
        match verdict {
            Verdict::Quiet => {}
            Verdict::Warn => {
                sp.counters
                    .dispatch_stall_warnings
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    shard = %self.prefix,
                    durable,
                    "a durable group has waited for dispatch with none attempted"
                );
            }
            Verdict::Stalled => {
                tracing::error!(
                    shard = %self.prefix,
                    durable,
                    "a durable group stayed pending with no dispatch; retiring the engine"
                );
                self.tasks.failed("dispatch");
                self.begin_close();
            }
        }
    }
}

#[cfg(test)]
mod loom_tests;
#[cfg(test)]
mod watchdog_tests;
