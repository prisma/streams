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
    /// Receipts of the absorbed advances this group retired. They settle
    /// when the group drops: after durable dispatch has published its
    /// tails, or with its refusal.
    pub receipts: Vec<SubmitReceipt>,
}

impl DurableEffects {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "DurableEffects::reply; a reply is a oneshot whose send fails only when the requester already went away; a handled result would only restate that nobody waits"
    )]
    pub(super) fn reply(self) {
        for (reply, result) in self.acks {
            let _ = reply.send(result);
        }
        for (reply, result) in self.queue_acks {
            let _ = reply.send(result);
        }
    }
    #[expect(
        clippy::let_underscore_must_use,
        reason = "DurableEffects::reject; a reply is a oneshot whose send fails only when the requester already went away; a handled result would only restate that nobody waits"
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "DurableEffects::reject; every waiting reply receives its own copy of the error, so the value is cloned per reply and the last clone is as cheap as a borrow; borrowing it would edit every fenced call site inside the committer and the engine close, whose blank-body mutants no bounded test can observe"
    )]
    pub(super) fn reject(self, error: AppendErr) {
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

#[cfg(kani)]
mod proofs;

/// What one absorbed advance copied: `len` stored frame bytes of the
/// stream's records, starting at offset `from` (the gather's `chunk_cost`
/// over the same rows the append path counted). The committer retires
/// them only from the stream's absorbed boundary, so every byte of
/// `[absorbed, next)` leaves the ledger once, provided `len` is the
/// stored size of `[from, upto)`. An absorber's copy also carries its
/// submission's receipt, which settles once the advance can no longer
/// land.
pub(crate) struct CopiedBytes {
    pub(super) from: u64,
    pub(super) len: u64,
    receipt: Option<SubmitReceipt>,
}

impl CopiedBytes {
    pub(crate) fn new(from: u64, len: u64) -> Self {
        Self {
            from,
            len,
            receipt: None,
        }
    }

    /// The same copy, carrying the receipt its submission was counted by.
    pub(crate) fn receipted(self, receipt: SubmitReceipt) -> Self {
        Self {
            receipt: Some(receipt),
            ..self
        }
    }

    /// The receipt, for the group that retired this copy to hold until
    /// its durable dispatch.
    pub(super) fn into_receipt(self) -> Option<SubmitReceipt> {
        self.receipt
    }
}

/// The settlement counters' atomic word: generic so the Loom model runs
/// these same transitions, a receipt's drop included, on instrumented
/// atomics.
pub(crate) trait SettlementWord: Default {
    fn load(&self, order: Ordering) -> u64;
    fn fetch_add(&self, value: u64, order: Ordering) -> u64;
    fn fetch_sub(&self, value: u64, order: Ordering) -> u64;
}

impl SettlementWord for AtomicU64 {
    fn load(&self, order: Ordering) -> u64 {
        AtomicU64::load(self, order)
    }
    fn fetch_add(&self, value: u64, order: Ordering) -> u64 {
        AtomicU64::fetch_add(self, value, order)
    }
    fn fetch_sub(&self, value: u64, order: Ordering) -> u64 {
        AtomicU64::fetch_sub(self, value, order)
    }
}

/// Stream buckets of an absorber's settlement counters: 1,024 words of
/// 8 bytes, 8 KiB per absorber (one per engine). Streams share a word only
/// when the ten bits `bucket` reads agree, and sharing only delays a
/// rollback, never permits one.
pub(crate) const SETTLEMENT_BUCKETS: usize = 1024;

/// The absorbed advances an absorber submitted that could still land,
/// counted per stream bucket. Each advance is counted when it is submitted
/// and settles when its receipt drops: at staging when the committer
/// drops the advance, with its group's refusal, or after durable dispatch
/// has published the group's tails. A bucket at zero therefore proves no
/// advance of its streams can still move a boundary, so a lane mark ahead
/// of the durable boundary is stranded, and one still in flight is not.
pub(crate) struct Submissions<W: SettlementWord = AtomicU64, const N: usize = SETTLEMENT_BUCKETS> {
    unsettled: [W; N],
}

impl<W: SettlementWord, const N: usize> Default for Submissions<W, N> {
    fn default() -> Self {
        Self {
            unsettled: std::array::from_fn(|_| W::default()),
        }
    }
}

impl<W: SettlementWord, const N: usize> Submissions<W, N> {
    /// A stream's counter: all of hash byte 14 and the low two bits of byte
    /// 15. The hash is a SHA-256 prefix independent of the placement route,
    /// so any ten of its bits are uniform; the tail is a convention.
    fn bucket(hash: &[u8; 16]) -> usize {
        usize::from(u16::from_le_bytes([hash[14], hash[15]])) % N
    }

    /// Count one advance of `hash` as submitted; its receipt settles it.
    pub(crate) fn submit(self: &Arc<Self>, hash: &[u8; 16]) -> SubmitReceipt<W, N> {
        let bucket = Self::bucket(hash);
        self.unsettled[bucket].fetch_add(1, Ordering::Relaxed);
        SubmitReceipt {
            submissions: Arc::clone(self),
            bucket,
        }
    }

    /// No advance of `hash`'s bucket can still land. Acquire pairs with
    /// the settling Release: a caller that sees zero also sees every
    /// durable boundary published before those receipts dropped.
    pub(crate) fn settled(&self, hash: &[u8; 16]) -> bool {
        self.unsettled[Self::bucket(hash)].load(Ordering::Acquire) == 0
    }
}

/// One submitted advance's claim on its stream bucket; dropping it settles
/// the advance.
pub(crate) struct SubmitReceipt<W: SettlementWord = AtomicU64, const N: usize = SETTLEMENT_BUCKETS>
{
    submissions: Arc<Submissions<W, N>>,
    bucket: usize,
}

impl<W: SettlementWord, const N: usize> Drop for SubmitReceipt<W, N> {
    fn drop(&mut self) {
        self.submissions.unsettled[self.bucket].fetch_sub(1, Ordering::Release);
    }
}

/// One stream's entry in a gather's batch: its hash, its new absorbed
/// boundary and the bytes the gather copied to reach it.
pub(crate) type AbsorbedAdvance = ([u8; 16], u64, CopiedBytes);

/// What retiring one advancing absorbed op did to the stream's tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AbsorbRetirement {
    /// It started at the boundary: the boundary moved to its `upto` and
    /// the ledger lost exactly its bytes.
    Exact,
    /// It started elsewhere, so part of it is retired already or a gap
    /// lies before it: nothing moved.
    Detached,
    /// It started at the boundary but claims more bytes than the ledger
    /// holds, which only a corrupt ledger can do: nothing moved.
    Diverged,
}

/// Retire an advancing absorbed op against the stream's tail. Only a copy
/// that starts exactly at the absorbed boundary moves it; the ledger is
/// checked, never clamped.
pub(super) fn retire_absorbed(
    tail: &mut TailFields,
    upto: u64,
    copied: &CopiedBytes,
) -> AbsorbRetirement {
    if copied.from != tail.absorbed {
        return AbsorbRetirement::Detached;
    }
    let Some(remaining) = tail.unabsorbed_bytes.checked_sub(copied.len) else {
        return AbsorbRetirement::Diverged;
    };
    tail.absorbed = upto.min(tail.next);
    tail.unabsorbed_bytes = remaining;
    AbsorbRetirement::Exact
}

#[cfg(test)]
mod loom_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// Release hold (the capacity run's one-off 500): an advance retires
    /// only when it starts at the boundary, and a claim larger than the
    /// ledger moves nothing.
    #[test]
    fn retire_absorbed_retires_only_a_copy_that_starts_at_the_boundary() {
        let start = TailFields {
            absorbed: 4,
            next: 8,
            unabsorbed_bytes: 100,
            ..Default::default()
        };
        let retire = |from, upto, len| {
            let mut tail = start.clone();
            let outcome = retire_absorbed(&mut tail, upto, &CopiedBytes::new(from, len));
            (outcome, tail.absorbed, tail.unabsorbed_bytes)
        };
        assert_eq!(retire(4, 6, 60), (AbsorbRetirement::Exact, 6, 40));
        assert_eq!(retire(2, 6, 60), (AbsorbRetirement::Detached, 4, 100));
        assert_eq!(retire(6, 8, 60), (AbsorbRetirement::Detached, 4, 100));
        assert_eq!(retire(4, 6, 101), (AbsorbRetirement::Diverged, 4, 100));
        assert_eq!(retire(4, 9, 100), (AbsorbRetirement::Exact, 8, 0));
    }

    /// A stream settles when the last receipt of its bucket drops; another
    /// bucket's receipts never hold it, and a stream sharing its bucket is
    /// held with it (a rollback waits, it never runs early).
    #[test]
    fn a_stream_settles_when_its_last_receipt_drops() {
        let stream = |last: [u8; 2], first: u8| {
            let mut hash = [first; 16];
            hash[14..].copy_from_slice(&last);
            hash
        };
        let (a, b) = (stream([1, 0], 0), stream([2, 0], 0));
        let (shares_a, wraps_to_a) = (stream([1, 0], 9), stream([1, 4], 0));
        let submissions = Arc::new(Submissions::<AtomicU64>::default());
        assert!(submissions.settled(&a) && submissions.settled(&b));
        let first = submissions.submit(&a);
        assert!(!submissions.settled(&a), "a submitted advance is settled");
        assert!(
            submissions.settled(&b),
            "another bucket's advance holds a stream"
        );
        assert!(!submissions.settled(&shares_a) && !submissions.settled(&wraps_to_a));
        let second = submissions.submit(&a);
        drop(first);
        assert!(
            !submissions.settled(&a),
            "an advance settled with an earlier one"
        );
        let other = submissions.submit(&b);
        drop(second);
        assert!(
            submissions.settled(&a),
            "the last receipt left its stream unsettled"
        );
        assert!(!submissions.settled(&b));
        let copy = CopiedBytes::new(0, 1).receipted(submissions.submit(&a));
        assert!(!submissions.settled(&a));
        let receipt = copy.into_receipt();
        assert!(receipt.is_some() && !submissions.settled(&a));
        drop((receipt, other));
        assert!(submissions.settled(&a) && submissions.settled(&b));
    }

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
