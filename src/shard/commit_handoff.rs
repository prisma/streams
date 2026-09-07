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
use super::{DurableEffects, InFlightGroup};

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

    pub(super) fn retire(&mut self) -> Option<Vec<InFlightGroup>> {
        if self.terminal {
            return None;
        }
        self.terminal = true;
        Some(std::mem::take(&mut self.pending))
    }
}
