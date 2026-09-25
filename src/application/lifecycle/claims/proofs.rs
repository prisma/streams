//! KANI-042: the final-append error disposition. The harness enumerates
//! every `AppendErr` variant (the exhaustive `variant` match stops compiling
//! when one is added) with symbolic numeric payloads. String payloads are
//! empty: neither classifier reads them. It checks the debt-retention contract of
//! docs/seal-transitions.md against both production classifiers: the raw
//! surface's `final_err_disposition` and the product surface's
//! `AppendFailure::definitively_rejected` over the committer translation.
use super::{FinalDisposition, final_err_disposition};
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
