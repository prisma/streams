//! Attempt-local seal planning, execution authority and stable operation IDs.
use crate::registry::{Mutation, StreamDesc};

/// Enter Sealing for a seal-with-final operation. A different seal
/// already in flight is a conflict; the SAME operation resumes.
/// What a seal-intent CAS actually did. A declined CAS is not a
/// failure and not a success — it is information, and treating it as
/// either is how a collection ends up stuck (Sealing over a pending
/// split, which phase B then refuses to finish) or how a client is told
/// `{"sealed": true}` about a descriptor that is still mid-transition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EnterSeal {
    /// The claim is ours, with the generation every claim-authorized
    /// append must carry. Installation ALLOCATES the generation and a
    /// same-operation re-entry RE-allocates it (renewal): the claim is
    /// a lease, and an actively retrying owner must always hold a
    /// generation no fence can be above.
    Installed {
        generation: u64,
    },
    /// This exact operation already owns the IN-FLIGHT transition. The
    /// re-entry renewed the claim (fresh timestamp, fresh generation).
    AlreadyOurs {
        generation: u64,
    },
    /// This exact operation already finished: answer idempotent success.
    AlreadyCompleted,
    /// Somebody else's seal is already terminal.
    AlreadySealed,
    /// A topology transition is in flight; resolve it and retry.
    PendingTopology,
    /// Somebody else's seal owns the collection.
    Conflicting(String),
    /// The live claim's lease lapsed. Takeover is PERMITTED but not
    /// performed by the CAS: the old operation's final append may still
    /// be queued inside the committer, so the old generation must be
    /// fenced there — and the fence's closed-report consulted — before
    /// anything replaces this claim. [`super::claim_seal`] runs that protocol.
    AbandonedClaim {
        old_op: String,
        old_gen: u64,
        old_intent: crate::registry::SealIntent,
    },
    Missing,
}

/// The execution token a claimed seal operates under: the incarnation
/// it was issued against and the generation its appends must carry.
/// Everything after the claim — the final append, the mark, the
/// segment closes, the publication — is fenced by BOTH.
#[derive(Debug, Clone)]
pub(crate) struct SealTicket {
    pub epoch: String,
    pub generation: u64,
}

#[derive(Debug)]
pub(crate) enum SealClaim {
    Active(SealTicket),
    Completed,
}

/// An attempt-local decision. No captured output can leak from a CAS that lost.
pub(super) fn decide_claim(
    current: &StreamDesc,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    now: i64,
) -> Mutation<EnterSeal> {
    if current.sealed {
        return Mutation::Decline(
            if current.seal_op.as_deref() == Some(op_id) && !op_id.is_empty() {
                EnterSeal::AlreadyCompleted
            } else {
                EnterSeal::AlreadySealed
            },
        );
    }
    let mut next = current.to_persisted();
    if let Some(claim) = &current.sealing {
        let ours = claim.operation_id == op_id;
        let may_join = op_id.is_empty() && !claim.owes_final();
        if ours || may_join {
            next.seal_gen_counter += 1;
            let generation = next.seal_gen_counter;
            let renewed = next.sealing.as_mut().expect("claim was observed");
            renewed.claim_generation = generation;
            renewed.claimed_ms = now;
            return Mutation::Write(next, EnterSeal::AlreadyOurs { generation });
        }
        let abandoned = now.saturating_sub(claim.claimed_ms) > crate::registry::SEAL_CLAIM_MS;
        return Mutation::Decline(if claim.owes_final() && abandoned {
            EnterSeal::AbandonedClaim {
                old_op: claim.operation_id.clone(),
                old_gen: claim.claim_generation,
                old_intent: claim.intent.clone(),
            }
        } else if claim.owes_final() {
            EnterSeal::Conflicting(
                "a seal with a final record is in flight; retry that request to finish it".into(),
            )
        } else {
            EnterSeal::Conflicting("a different seal operation is in flight".into())
        });
    }
    if current
        .segments
        .as_ref()
        .is_some_and(|map| map.pending.is_some())
    {
        return Mutation::Decline(EnterSeal::PendingTopology);
    }
    next.seal_gen_counter += 1;
    let generation = next.seal_gen_counter;
    next.sealing = Some(crate::registry::SealState {
        operation_id: op_id.to_string(),
        intent: intent.clone(),
        claimed_ms: now,
        claim_generation: generation,
    });
    Mutation::Write(next, EnterSeal::Installed { generation })
}

/// Identity of a seal-with-final operation: the record it promised,
/// under the routing key it promised it for. A retry of the same seal
/// derives the same id and resumes; anything else is a different
/// operation and may not finish this one.
pub(crate) fn seal_op_id_full(
    final_value: &serde_json::Value,
    routing_key: &str,
    producer: Option<(&str, &str, &str)>,
) -> String {
    use sha2::{Digest, Sha256};
    // The identity covers the WHOLE attempt, not just the record. Two
    // requests carrying the same final value under the same key but
    // different producer coordination are different operations: sharing
    // one id let a request that was definitively refused tear down the
    // intent a concurrent valid attempt was still committing under.
    let record = final_value.to_string();
    let (pid, pep, pseq) = producer.unwrap_or(("", "", ""));
    let mut h = Sha256::new();
    h.update(b"prisma-seal-v2\0");
    for part in [routing_key, &record, pid, pep, pseq] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    crate::crypto::hex(&h.finalize()[..16])
}

/// Identity of a raw close that carries content. The raw surface has
/// no typed final record, so the identity is the create-request hash
/// plus EVERY coordination input the committer can rule on: producer
/// trio, explicit sequence, timestamp. Two closes that agree on all of
/// it are the same operation and may resume each other; anything else
/// is a different one and may not finish this seal.
pub(crate) fn seal_op_id_semantic(
    request_hash: &str,
    routing_key: &str,
    coordination: &[String],
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"prisma-seal-raw-v2\0");
    for part in std::iter::once(routing_key)
        .chain(std::iter::once(request_hash))
        .chain(coordination.iter().map(|s| s.as_str()))
    {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    crate::crypto::hex(&h.finalize()[..16])
}

/// The TRUSTED execution token a product seal's final append carries:
/// the operation, the claim generation, and the incarnation the whole
/// seal was validated against. The append refuses to run unless the
/// CURRENT descriptor still matches all three — an epoch-less token
/// let a seal claimed on incarnation A write its final record into
/// (and physically close a segment of) a same-name, same-key
/// replacement created while the request was in flight.
#[derive(Debug, Clone)]
pub(crate) struct SealAuthz {
    pub op_id: String,
    pub generation: u64,
    pub epoch: String,
}

/// What a refused FINAL append does to the seal intent it belongs to
/// — ONE policy, shared verbatim by the raw and product surfaces
/// (they previously kept separate stringly-typed lists, which drifted:
/// the product list named codes its own translator never produces, so
/// stale-epoch was "retained" in the comment and definitive in fact).
///
/// After round 11 every one of these verdicts is durability-barriered,
/// and after round 8 every claim is generation-fenced — so releasing a
/// definitively-refused generation's intent can never destroy a
/// concurrent exact retry (the retry renewed to a newer generation the
/// release cannot name).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FinalDisposition {
    /// About the moment or the ordering, not the request: the exact
    /// retry can still succeed, so the intent stays. Producer gaps
    /// (the predecessor may already be inside the server) and
    /// epoch-must-start-at-zero (the producer's epoch can advance and
    /// make this sequence meaningful) are ordering; timeouts,
    /// throttles, write failures and ownership moves are the moment.
    AmbiguousOrTransient,
    /// About THIS request, forever: epochs never decrease (stale),
    /// bodies and content types do not change on retry, a reused or
    /// conflicting sequence row is durable, and a segment closed by
    /// another operation stays closed. The uncommitted intent comes
    /// down NOW — retaining it held the collection Sealing behind a
    /// promise that could never be delivered, renewable indefinitely
    /// by the very request that can never deliver it.
    DefinitivelyRejected,
}

pub(crate) fn final_err_disposition(e: &crate::shard::AppendErr) -> FinalDisposition {
    use crate::shard::AppendErr::*;
    match e {
        ProducerGap { .. } | ProducerEpochSeq => FinalDisposition::AmbiguousOrTransient,
        ProducerStale { .. }
        | ProducerSeqReused
        | CtMismatch
        | BadBody(_)
        | SeqConflict { .. }
        | Closed { .. }
        | SealSuperseded => FinalDisposition::DefinitivelyRejected,
        _ => FinalDisposition::AmbiguousOrTransient,
    }
}
