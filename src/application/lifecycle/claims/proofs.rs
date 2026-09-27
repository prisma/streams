//! KANI-040: the seal-claim decision matrix (`claim_step`) over a
//! descriptor's persisted state with every sealed and claim state, three
//! operation ids (the plain seal's empty id and two distinct ids), both
//! intents with the final committed or owed, pending topology, and
//! full-width generations and clocks.
//! KANI-042: the final-append error disposition. The harness enumerates
//! every `AppendErr` variant (the exhaustive `variant` match stops compiling
//! when one is added) with symbolic numeric payloads. String payloads are
//! empty: neither classifier reads them. It checks the debt-retention contract of
//! docs/seal-transitions.md against both production classifiers: the raw
//! surface's `final_err_disposition` and the product surface's
//! `AppendFailure::definitively_rejected` over the committer translation.
use super::{ClaimStep, EnterSeal, FinalDisposition, claim_step, final_err_disposition};
use crate::application::append::AppendFailure;
use crate::shard::AppendErr;

const VARIANTS: u8 = 11;

fn construct(index: u8) -> AppendErr {
    match index {
        0 => AppendErr::SeqConflict {
            current: kani::any::<bool>().then(String::new),
        },
        1 => AppendErr::ProducerSeqReused,
        2 => AppendErr::Closed {
            next_offset: kani::any(),
        },
        3 => AppendErr::SealSuperseded,
        4 => AppendErr::ProducerGap {
            expected: kani::any(),
            received: kani::any(),
        },
        5 => AppendErr::ProducerStale {
            current_epoch: kani::any(),
        },
        6 => AppendErr::ProducerEpochSeq,
        7 => AppendErr::CtMismatch,
        8 => AppendErr::BadBody(String::new()),
        9 => AppendErr::Internal(String::new()),
        _ => AppendErr::Moved,
    }
}

/// No wildcard: a new variant fails to compile here until it has a
/// constructor above and a row in `contract` below.
fn variant(error: &AppendErr) -> u8 {
    match error {
        AppendErr::SeqConflict { .. } => 0,
        AppendErr::ProducerSeqReused => 1,
        AppendErr::Closed { .. } => 2,
        AppendErr::SealSuperseded => 3,
        AppendErr::ProducerGap { .. } => 4,
        AppendErr::ProducerStale { .. } => 5,
        AppendErr::ProducerEpochSeq => 6,
        AppendErr::CtMismatch => 7,
        AppendErr::BadBody(_) => 8,
        AppendErr::Internal(_) => 9,
        AppendErr::Moved => 10,
    }
}

/// The adopted contract (docs/seal-transitions.md, `FinalDisposition`):
/// ordering disagreements (producer gap, epoch-must-start-at-zero) and the
/// moment (internal failure, shard move) retain the owed final; refusals
/// about the request itself release it.
fn contract(index: u8) -> FinalDisposition {
    match index {
        4 | 6 | 9 | 10 => FinalDisposition::AmbiguousOrTransient,
        _ => FinalDisposition::DefinitivelyRejected,
    }
}

#[kani::proof]
#[kani::unwind(8)]
fn kani_042_final_append_error_disposition() {
    let index: u8 = kani::any();
    kani::assume(index < VARIANTS);
    let error = construct(index);
    assert!(
        variant(&error) == index,
        "the constructor covers this variant"
    );
    let raw = final_err_disposition(&error);
    assert!(
        raw == contract(index),
        "the raw surface follows the contract"
    );
    let product =
        AppendFailure::from_commit(kani::any(), kani::any(), error.clone()).definitively_rejected();
    assert!(
        product == (raw == FinalDisposition::DefinitivelyRejected),
        "the product surface agrees with the raw surface"
    );
    kani::cover!(
        raw == FinalDisposition::AmbiguousOrTransient,
        "debt is retained"
    );
    kani::cover!(
        raw == FinalDisposition::DefinitivelyRejected,
        "debt is released"
    );
    kani::cover!(index == 10, "the last variant is reached");
}

/// The plain seal's empty id and two distinct operations.
fn operation() -> &'static str {
    match kani::any::<u8>() % 3 {
        0 => "",
        1 => "op-a",
        _ => "op-b",
    }
}

fn intent() -> crate::registry::SealIntent {
    if kani::any() {
        crate::registry::SealIntent::Empty
    } else {
        crate::registry::SealIntent::Final {
            routing_key: String::new(),
            request_hash: String::new(),
            final_committed: kani::any(),
        }
    }
}

/// A persisted descriptor in a state validation admits: a sealed one holds
/// no claim, a claim excludes pending topology, and no claim's generation
/// is above the allocator, which has room for one more.
fn descriptor() -> crate::registry::PersistedDescriptor {
    let counter: u64 = kani::any_where(|counter: &u64| *counter < u64::MAX);
    let sealed: bool = kani::any();
    let sealing = (!sealed && kani::any()).then(|| crate::registry::SealState {
        operation_id: operation().into(),
        intent: intent(),
        claimed_ms: kani::any(),
        claim_generation: kani::any_where(|generation: &u64| *generation <= counter),
    });
    let pending = sealing.is_none() && kani::any();
    let segments = pending.then(|| {
        let mut map = crate::segmap::SegmentMap::initial("", 1);
        map.pending = Some(crate::segmap::PendingTransition {
            kind: "split".into(),
            segs: vec![0],
            split_at: 1 << 63,
            started_ms: 1,
            seal_gen: 0,
        });
        map
    });
    crate::registry::PersistedDescriptor {
        seal_gen_counter: counter,
        account_id: None,
        project_id: crate::tenant::ProjectId::new("proj").unwrap_or_else(|_| unreachable!()),
        name: "stream".into(),
        stream_epoch: String::new(),
        key_fingerprint: String::new(),
        created_ms: 1,
        expires_at_ms: None,
        deleted: false,
        content_type: String::new(),
        ttl_secs: None,
        segments,
        sealed,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing,
        seal_op: sealed.then(|| operation().to_string()),
        layout_version: crate::registry::LAYOUT_VERSION,
    }
}

/// KANI-040: a terminal seal answers completed only to its own operation;
/// only the claim's own operation, or a plain seal joining a claim that owes
/// no final record, renews it, keeping its operation and intent; any other
/// request meets a conflict, or an abandoned final-bearing claim it may take
/// over only once the lease has lapsed; pending topology declines; otherwise
/// the request installs its own claim. Every renewal or installation takes
/// the next generation, above every earlier claim, and a declined step
/// carries no installation.
#[kani::proof]
#[kani::unwind(8)]
fn kani_040_the_seal_claim_decision_matrix() {
    let current = descriptor();
    let (op_id, requested, now) = (operation(), intent(), kani::any::<i64>());
    let step = claim_step(&current, op_id, &requested, now);
    let next = current.seal_gen_counter + 1;
    if let Some(claim) = &current.sealing {
        assert!(
            !(op_id.is_empty() && claim.owes_final() && matches!(step, ClaimStep::Renew(_))),
            "a plain seal never joins a claim that owes a final record"
        );
    }
    match (&step, &current.sealing) {
        (ClaimStep::Decline(EnterSeal::Installed { .. } | EnterSeal::AlreadyOurs { .. }), _) => {
            panic!("a declined step carries no installation")
        }
        (ClaimStep::Decline(EnterSeal::AlreadyCompleted), _) => assert!(
            current.sealed && current.seal_op.as_deref() == Some(op_id) && !op_id.is_empty(),
            "only a terminal seal's own operation is told it completed"
        ),
        (ClaimStep::Decline(EnterSeal::AlreadySealed), _) => assert!(
            current.sealed && (op_id.is_empty() || current.seal_op.as_deref() != Some(op_id)),
            "a terminal seal answers sealed to every other request"
        ),
        (ClaimStep::Renew(renewed), Some(claim)) => {
            assert!(
                !current.sealed
                    && if op_id.is_empty() {
                        !claim.owes_final()
                    } else {
                        claim.operation_id == op_id
                    },
                "only the claim's own operation, or a plain seal joining a claim that owes no final record, renews it"
            );
            assert!(
                (&renewed.operation_id, &renewed.intent) == (&claim.operation_id, &claim.intent)
                    && (renewed.claimed_ms, renewed.claim_generation) == (now, next),
                "a renewal keeps the operation and intent under the next generation"
            );
        }
        (
            ClaimStep::Decline(EnterSeal::AbandonedClaim {
                old_op, old_gen, ..
            }),
            Some(claim),
        ) => {
            assert!(
                claim.owes_final()
                    && (op_id.is_empty() || claim.operation_id != op_id)
                    && now.saturating_sub(claim.claimed_ms) > crate::registry::SEAL_CLAIM_MS
                    && (old_op, *old_gen) == (&claim.operation_id, claim.claim_generation),
                "only a lapsed claim that owes a final record is offered for takeover, as it is"
            );
        }
        (ClaimStep::Decline(EnterSeal::Conflicting(_)), Some(claim)) => assert!(
            if op_id.is_empty() {
                claim.owes_final()
            } else {
                claim.operation_id != op_id
            },
            "a conflict is another operation's claim"
        ),
        (ClaimStep::Decline(EnterSeal::PendingTopology), None) => assert!(
            !current.sealed
                && current
                    .segments
                    .as_ref()
                    .is_some_and(|m| m.pending.is_some()),
            "pending topology declines a new claim"
        ),
        (ClaimStep::Install(installed), None) => assert!(
            !current.sealed
                && installed.operation_id == op_id
                && installed.intent == requested
                && (installed.claimed_ms, installed.claim_generation) == (now, next),
            "a new claim is this operation's, under the next generation"
        ),
        _ => panic!("the decision matches the claim state"),
    }
    kani::cover!(
        matches!(step, ClaimStep::Renew(_)) && op_id.is_empty(),
        "a plain seal joins a claim"
    );
    kani::cover!(
        matches!(step, ClaimStep::Decline(EnterSeal::AbandonedClaim { .. })),
        "a lapsed final-bearing claim is offered for takeover"
    );
    kani::cover!(
        matches!(step, ClaimStep::Install(_)),
        "a claim is installed"
    );
}
