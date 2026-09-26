//! Kani proofs for the committer's producer and seal-fence decisions:
//! KANI-036 (stale and new-epoch admission), KANI-037 (duplicates, hash
//! conflicts and replay results), KANI-038 (sequence gaps at the numeric
//! boundary) and KANI-039 (the seal generation truth table). Every harness
//! calls the production function with full-width `u64` epochs, sequences,
//! offsets and fences, symbolic 16-byte hashes, both tail-closure flags and
//! every sealed state. The producer id is not an input of these decisions,
//! so it stays empty. KANI-046 (the absorbed boundary and the trim frontier)
//! calls `retire_absorbed` and `trim_target` on full-width frontiers.
use super::{
    AbsorbRetirement, CopiedBytes, ProducerDecision, decide_producer, retire_absorbed,
    seal_authorized, trim_target,
};
use crate::shard::{AppendAck, AppendErr, ProducerReq, SealedReject, TailFields};

fn any_request() -> ProducerReq {
    ProducerReq {
        id: String::new(),
        epoch: kani::any(),
        seq: kani::any(),
        request_hash: kani::any(),
    }
}

fn any_tail() -> TailFields {
    TailFields {
        next: kani::any(),
        closed: kani::any(),
        ..TailFields::default()
    }
}

fn any_sealed() -> Option<SealedReject> {
    match kani::any::<u8>() % 3 {
        0 => None,
        1 => Some(SealedReject::Sealing),
        _ => Some(SealedReject::Sealed),
    }
}

/// The reply a decision stages, or `None` for an admitted append.
fn reply(decision: ProducerDecision) -> Option<Result<AppendAck, AppendErr>> {
    match decision {
        ProducerDecision::Accept(_) => None,
        ProducerDecision::Reply(reply) => Some(reply),
    }
}

/// KANI-036: a lower epoch is refused before anything else is consulted; a
/// new epoch, and a producer with no remembered state, must begin at
/// sequence 0; an admitted request is echoed exactly.
#[kani::proof]
fn kani_036_stale_and_new_epoch_admission() {
    let request = any_request();
    let current: Option<(u64, u64, u64, [u8; 16])> = kani::any();
    let tail = any_tail();
    let sealed = any_sealed();
    let decision = decide_producer(&request, current, &tail, sealed);
    if let ProducerDecision::Accept(admitted) = &decision {
        assert!(
            *admitted == (request.epoch, request.seq),
            "an admitted request is echoed exactly"
        );
        assert!(sealed.is_none(), "a sealed collection admits nothing");
    }
    let reply = reply(decision);
    match current {
        Some((epoch, ..)) if request.epoch < epoch => assert!(
            matches!(reply, Some(Err(AppendErr::ProducerStale { current_epoch })) if current_epoch == epoch),
            "a lower epoch is stale, whatever the sequence, hashes or closure"
        ),
        Some((epoch, ..)) if request.epoch > epoch => {
            if request.seq != 0 {
                assert!(
                    matches!(reply, Some(Err(AppendErr::ProducerEpochSeq))),
                    "a new epoch must start at sequence 0"
                );
            } else {
                assert!(
                    matches!(reply, None | Some(Err(AppendErr::Closed { .. }))),
                    "a new epoch at sequence 0 is admitted unless the collection is closed"
                );
            }
        }
        None if request.seq != 0 => assert!(
            matches!(reply, Some(Err(AppendErr::ProducerGap { expected: 0, received })) if received == request.seq),
            "a producer with no remembered state must start at sequence 0"
        ),
        None => assert!(
            matches!(reply, None | Some(Err(AppendErr::Closed { .. }))),
            "a first sequence 0 is admitted unless the collection is closed"
        ),
        Some(_) => {}
    }
    kani::cover!(
        matches!(current, Some((epoch, ..)) if request.epoch < epoch) && sealed.is_some(),
        "staleness outranks closure"
    );
    kani::cover!(
        matches!(current, Some((epoch, ..)) if request.epoch > epoch) && request.seq == 0,
        "a new epoch is admitted"
    );
}

/// KANI-037: a retry at or below the remembered sequence of the same epoch
/// is a duplicate, never an admission and never a closure refusal. Only a
/// known hash conflict at the exact remembered sequence refuses it; the
/// stored offset answers the exact sequence when one was recorded, and the
/// documented fallback (the tail's last offset) answers everything else.
#[kani::proof]
fn kani_037_duplicates_conflicts_and_replay_results() {
    let request = any_request();
    let (epoch, seq, offset, stored_hash): (u64, u64, u64, [u8; 16]) = kani::any();
    kani::assume(request.epoch == epoch && request.seq <= seq);
    let tail = any_tail();
    let sealed = any_sealed();
    let reply = reply(decide_producer(
        &request,
        Some((epoch, seq, offset, stored_hash)),
        &tail,
        sealed,
    ));
    let exact = request.seq == seq;
    let conflict = exact
        && stored_hash != [0; 16]
        && request.request_hash.is_some_and(|hash| hash != stored_hash);
    if conflict {
        assert!(
            matches!(reply, Some(Err(AppendErr::ProducerSeqReused))),
            "a known hash conflict at the remembered sequence is refused"
        );
        return;
    }
    assert!(
        matches!(reply, Some(Ok(_))),
        "a duplicate is answered, not admitted or refused"
    );
    let Some(Ok(ack)) = reply else {
        return;
    };
    assert!(ack.duplicate, "the answer is marked duplicate");
    assert!(
        ack.producer == Some((epoch, seq)),
        "the answer reports the remembered producer state"
    );
    assert!(
        ack.next_offset == tail.next && ack.closed == tail.closed,
        "the answer reports the current tail"
    );
    let expected_last = if exact && offset != u64::MAX {
        offset
    } else {
        tail.next.wrapping_sub(1)
    };
    assert!(
        ack.last_offset == expected_last,
        "the stored offset where recorded, otherwise the tail's last offset"
    );
    kani::cover!(
        sealed.is_some(),
        "a duplicate is answered on a sealed collection"
    );
    kani::cover!(
        exact && stored_hash == [0; 16] && request.request_hash.is_some(),
        "a legacy all-zero stored hash cannot detect a conflict"
    );
    kani::cover!(
        exact && request.request_hash.is_none() && stored_hash != [0; 16],
        "a raw request without a hash cannot detect a conflict"
    );
    kani::cover!(
        exact && offset == u64::MAX,
        "a legacy unknown offset falls back"
    );
    kani::cover!(!exact, "an older duplicate falls back");
}

/// KANI-038: within one epoch, the next sequence is admitted and anything
/// beyond it is a gap naming the exact expected sequence, including at the
/// top of the `u64` range where no successor exists.
#[kani::proof]
fn kani_038_sequence_gaps_at_the_numeric_boundary() {
    let request = any_request();
    let (seq, offset, stored_hash): (u64, u64, [u8; 16]) = kani::any();
    kani::assume(request.seq > seq);
    let tail = any_tail();
    let sealed = any_sealed();
    let current = Some((request.epoch, seq, offset, stored_hash));
    let reply = reply(decide_producer(&request, current, &tail, sealed));
    // request.seq > seq, so seq < u64::MAX and its successor exists.
    let successor = seq + 1;
    if request.seq == successor {
        assert!(
            match &reply {
                None => sealed.is_none(),
                Some(Err(AppendErr::Closed { next_offset })) => {
                    sealed.is_some() && *next_offset == tail.next
                }
                Some(_) => false,
            },
            "the adjacent sequence is admitted unless the collection is closed"
        );
    } else {
        assert!(
            matches!(reply, Some(Err(AppendErr::ProducerGap { expected, received }))
                if expected == successor && received == request.seq),
            "a sequence beyond the successor is a gap naming the successor"
        );
    }
    kani::cover!(
        request.seq == u64::MAX && seq == u64::MAX - 1,
        "the last sequence is admitted"
    );
    kani::cover!(
        request.seq == u64::MAX && seq < u64::MAX - 1,
        "a gap reaching u64::MAX"
    );
}

/// KANI-038: at the top of the range every same-epoch request is a duplicate
/// of the remembered sequence or older, so exhaustion never admits or gaps.
#[kani::proof]
fn kani_038_an_exhausted_sequence_only_replays() {
    let request = any_request();
    let (offset, stored_hash): (u64, [u8; 16]) = kani::any();
    let tail = any_tail();
    let current = Some((request.epoch, u64::MAX, offset, stored_hash));
    let reply = reply(decide_producer(&request, current, &tail, any_sealed()));
    assert!(
        matches!(
            reply,
            Some(
                Ok(AppendAck {
                    duplicate: true,
                    ..
                }) | Err(AppendErr::ProducerSeqReused)
            )
        ),
        "an exhausted producer lane only replays or refuses a reused sequence"
    );
}

/// KANI-039: a supplied generation is authorized exactly when it has reached
/// the fence; an untagged closing operation only while no fence exists; an
/// untagged non-closing write is left to the ordinary admission contract.
#[kani::proof]
fn kani_039_seal_generation_truth_table() {
    let generation: Option<u64> = kani::any();
    let closing: bool = kani::any();
    let fence: u64 = kani::any();
    let authorized = seal_authorized(generation, closing, fence);
    match generation {
        Some(generation) => assert!(
            authorized == (generation >= fence),
            "a tagged operation is authorized exactly at or above the fence"
        ),
        None if closing => assert!(
            authorized == (fence == 0),
            "an untagged close is refused once a fence exists"
        ),
        None => assert!(
            authorized,
            "an untagged ordinary write is not a seal decision"
        ),
    }
    kani::cover!(
        generation == Some(fence) && fence > 0,
        "the exact fence generation"
    );
    kani::cover!(
        generation.is_some_and(|generation| generation < fence),
        "a superseded generation"
    );
    kani::cover!(
        generation.is_none() && closing && fence > 0,
        "an untagged close after a fence"
    );
}

/// A tail as the committer keeps it: `trimmed <= trim_safe_to <= absorbed
/// <= next`, every frontier full width.
fn frontier_tail() -> TailFields {
    let tail = TailFields {
        next: kani::any(),
        absorbed: kani::any(),
        trimmed: kani::any(),
        trim_safe_to: kani::any(),
        unabsorbed_bytes: kani::any(),
        ..TailFields::default()
    };
    kani::assume(
        tail.trimmed <= tail.trim_safe_to
            && tail.trim_safe_to <= tail.absorbed
            && tail.absorbed <= tail.next,
    );
    tail
}

/// KANI-046: one trim step reaches past neither `trim_safe_to` nor the
/// absorbed boundary, never moves `trimmed` back, and deletes at most its
/// budget, a budget that would pass `u64::MAX` included.
#[kani::proof]
fn kani_046_a_trim_step_stays_behind_the_safe_boundary() {
    let tail = frontier_tail();
    let allowed: u64 = kani::any();
    let target = trim_target(&tail, allowed);
    assert!(
        target <= tail.trim_safe_to && target <= tail.absorbed,
        "a trim never passes the safe boundary"
    );
    assert!(target >= tail.trimmed, "a trim never moves backwards");
    assert!(
        target - tail.trimmed <= allowed,
        "a trim deletes at most its budget"
    );
    kani::cover!(
        target == tail.trim_safe_to && target < tail.absorbed,
        "a trim reaches the safe boundary, one advance behind"
    );
    kani::cover!(
        allowed > 0 && target - tail.trimmed == allowed && target < tail.trim_safe_to,
        "the budget stops a trim"
    );
    kani::cover!(
        tail.trimmed.checked_add(allowed).is_none(),
        "a budget past u64::MAX saturates"
    );
}

/// KANI-046: an advance that starts at the boundary moves it forward to its
/// end, never past the log's next offset, retires exactly its copied bytes
/// and makes the old boundary trimmable, keeping the frontiers ordered; any
/// other advance changes nothing. `upto` is past the boundary, as the
/// committer's caller requires before it retires an advance.
#[kani::proof]
fn kani_046_an_advance_moves_the_boundary_forward_within_the_log() {
    let before = frontier_tail();
    let upto: u64 = kani::any_where(|upto: &u64| *upto > before.absorbed);
    let copied = CopiedBytes::new(kani::any(), kani::any());
    let mut after = before.clone();
    let outcome = retire_absorbed(&mut after, upto, &copied);
    let unchanged = |a: &TailFields, b: &TailFields| {
        (
            a.next,
            a.ts,
            a.logical,
            a.trimmed,
            a.closed,
            a.history_v2,
            a.route,
        ) == (
            b.next,
            b.ts,
            b.logical,
            b.trimmed,
            b.closed,
            b.history_v2,
            b.route,
        ) && a.seq == b.seq
    };
    assert!(
        unchanged(&before, &after),
        "a retirement moves only the boundary and its ledger"
    );
    if outcome == AbsorbRetirement::Exact {
        assert!(
            before.absorbed <= after.absorbed && after.absorbed <= after.next,
            "the boundary moves forward within the log"
        );
        assert!(
            after.unabsorbed_bytes.checked_add(copied.len) == Some(before.unabsorbed_bytes),
            "exactly the copied bytes retire"
        );
        assert!(
            after.trim_safe_to == before.absorbed,
            "the old boundary becomes trimmable"
        );
        assert!(
            after.trimmed <= after.trim_safe_to && after.trim_safe_to <= after.absorbed,
            "the frontiers stay ordered"
        );
    } else {
        assert!(
            (after.absorbed, after.unabsorbed_bytes, after.trim_safe_to)
                == (
                    before.absorbed,
                    before.unabsorbed_bytes,
                    before.trim_safe_to
                ),
            "a refused advance changes nothing"
        );
    }
    kani::cover!(
        outcome == AbsorbRetirement::Exact && upto > before.next,
        "an advance past the log is held to its next offset"
    );
    kani::cover!(
        outcome == AbsorbRetirement::Diverged,
        "a claim larger than the ledger moves nothing"
    );
    kani::cover!(
        outcome == AbsorbRetirement::Detached,
        "an advance that does not start at the boundary moves nothing"
    );
}
