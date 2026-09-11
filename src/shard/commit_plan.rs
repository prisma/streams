//! Typed commit decisions and publication effects. This module performs no I/O.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AppendFinish {
    Open,
    /// Append the final records and close in the same storage transaction.
    Close,
}

pub(crate) struct CloseReq {
    pub hash: [u8; 16],
    pub generation: Option<u64>,
    pub resp: oneshot::Sender<Result<AppendAck, AppendErr>>,
}

pub(crate) struct SealFenceReq {
    pub hash: [u8; 16],
    pub generation: u64,
    pub resp: oneshot::Sender<Result<AppendAck, AppendErr>>,
}

#[derive(Debug)]
pub(crate) enum EnqueueError {
    Full,
    Closed,
}

type ReplyEffect<T, E> = (oneshot::Sender<Result<T, E>>, Result<T, E>);
pub(super) type RingPublication = (Arc<StreamHandle>, Vec<(u64, Bytes)>);

/// Effects whose truth requires the group's REMOTE durability sequence.
/// No sender/tail/ring/touch escapes this plan before durable dispatch.
#[derive(Default)]
pub(super) struct DurableEffects {
    pub acks: Vec<ReplyEffect<AppendAck, AppendErr>>,
    pub queue_acks: Vec<ReplyEffect<crate::queue::QueueOut, String>>,
    pub tails: Vec<(Arc<StreamHandle>, TailFields)>,
    pub ring_pub: Vec<RingPublication>,
    pub signals: Vec<AbsorbSignal>,
    pub touches: Vec<TouchFeed>,
    pub usage: Vec<(Arc<crate::usage::Counters>, u64, u64)>,
}

impl DurableEffects {
    pub(super) fn reply(self) {
        for (reply, result) in self.acks {
            let _ = reply.send(result);
        }
        for (reply, result) in self.queue_acks {
            let _ = reply.send(result);
        }
    }
    pub fn reject(self, error: AppendErr) {
        let queue_error = match &error {
            AppendErr::Moved => "shard fenced/moved; retry".to_owned(),
            error => format!("{error:?}"),
        };
        for (reply, _) in self.acks {
            let _ = reply.send(Err(error.clone()));
        }
        for (reply, _) in self.queue_acks {
            let _ = reply.send(Err(queue_error.clone()));
        }
    }
}

pub(super) enum ProducerDecision {
    Accept((u64, u64)),
    Reply(Result<AppendAck, AppendErr>),
}

/// Preserve protocol order: stale epoch, duplicate/hash conflict, epoch
/// start, sequence gap, collection close. Returned replies are staged.
pub(super) fn decide_producer(
    request: &ProducerReq,
    current: Option<(u64, u64, u64, [u8; 16])>,
    tail: &TailFields,
    sealed: Option<SealedReject>,
) -> ProducerDecision {
    let reject = |error| ProducerDecision::Reply(Err(error));
    if let Some((epoch, seq, offset, request_hash)) = current {
        if request.epoch < epoch {
            return reject(AppendErr::ProducerStale {
                current_epoch: epoch,
            });
        }
        if request.epoch == epoch && request.seq <= seq {
            if request.seq == seq
                && request_hash != [0; 16]
                && request
                    .request_hash
                    .is_some_and(|hash| hash != request_hash)
            {
                return reject(AppendErr::ProducerSeqReused);
            }
            return ProducerDecision::Reply(Ok(AppendAck {
                last_offset: if request.seq == seq && offset != u64::MAX {
                    offset
                } else {
                    tail.next.wrapping_sub(1)
                },
                next_offset: tail.next,
                closed: tail.closed,
                producer: Some((epoch, seq)),
                duplicate: true,
            }));
        }
        if request.epoch > epoch && request.seq != 0 {
            return reject(AppendErr::ProducerEpochSeq);
        }
        if request.epoch == epoch && seq.checked_add(1).is_none_or(|next| request.seq > next) {
            return reject(AppendErr::ProducerGap {
                expected: seq.saturating_add(1),
                received: request.seq,
            });
        }
    } else if request.seq != 0 {
        return reject(AppendErr::ProducerGap {
            expected: 0,
            received: request.seq,
        });
    }
    if sealed.is_some() {
        return reject(AppendErr::Closed {
            next_offset: tail.next,
        });
    }
    ProducerDecision::Accept((request.epoch, request.seq))
}

pub(super) fn seal_authorized(generation: Option<u64>, closing: bool, fence: u64) -> bool {
    match generation {
        Some(g) => g >= fence,
        None => !closing || fence == 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r03_producer_decision_keeps_duplicate_before_close_and_new_epoch_fence() {
        let request = ProducerReq {
            id: "p".into(),
            epoch: 2,
            seq: 4,
            request_hash: Some([3; 16]),
        };
        let tail = TailFields {
            next: 30,
            closed: true,
            ..Default::default()
        };
        assert!(matches!(
            decide_producer(
                &request,
                Some((2, 4, 12, [3; 16])),
                &tail,
                Some(SealedReject::Sealed)
            ),
            ProducerDecision::Reply(Ok(AppendAck {
                last_offset: 12,
                duplicate: true,
                ..
            }))
        ));
        assert!(matches!(
            decide_producer(&request, Some((3, 4, 12, [3; 16])), &tail, None),
            ProducerDecision::Reply(Err(AppendErr::ProducerStale { .. }))
        ));
        assert!(matches!(
            decide_producer(&request, Some((2, 4, 12, [4; 16])), &tail, None),
            ProducerDecision::Reply(Err(AppendErr::ProducerSeqReused))
        ));
        assert!(!seal_authorized(None, true, 1));
        assert!(!seal_authorized(Some(1), true, 2));
        assert!(seal_authorized(Some(2), true, 2));
    }
}

/// A request from a deleted generation cannot inherit leases or cursor state
/// from a replacement. Bind legacy empty state, reset older state, preserve
/// the same generation; the actor stages any refusal in DurableEffects.
pub(super) enum ConsumerGeneration {
    Bind,
    Reset,
    Continue,
    Fenced { current: u64 },
}
pub(super) fn decide_consumer_generation(current: u64, requested: u64) -> ConsumerGeneration {
    if current == 0 {
        ConsumerGeneration::Bind
    } else if current > requested {
        ConsumerGeneration::Fenced { current }
    } else if current < requested {
        ConsumerGeneration::Reset
    } else {
        ConsumerGeneration::Continue
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UsageAckScope {
    ThroughVersion(u64),
    FinalRowsOnly,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BillingAckDecision {
    ClearDirty,
    RetainDirty,
}
pub(super) fn decide_billing_ack(current: Option<u64>, scope: UsageAckScope) -> BillingAckDecision {
    match scope {
        UsageAckScope::ThroughVersion(version)
            if current.is_none_or(|current| current <= version) =>
        {
            BillingAckDecision::ClearDirty
        }
        _ => BillingAckDecision::RetainDirty,
    }
}
