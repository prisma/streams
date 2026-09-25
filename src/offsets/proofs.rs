//! Kani proofs for the offset codec: KANI-001 (round trip), KANI-002
//! (start-of-stream and the resume index) and KANI-003 (injectivity and
//! order). The harnesses call the production `digits`, whose bytes `encode`
//! collects into its `String`, and `read`, the value computation `parse`
//! runs after its "-1" and length gates; symbolic `String` growth is out of
//! scope for CBMC here, and the unit and property tests exercise the
//! wrappers. Epochs stay in the documented domain below 2^30 (the module
//! doc of `src/offsets.rs`); what the codec does above it, and its lax
//! reading of non-canonical tokens, await review item 88's wire decision.
//! Loops are fixed at 26 digits and a 32-symbol alphabet, so `unwind(40)`
//! covers them with Kani's unwinding checks left on.
use super::{ALPHABET, OffsetError, digits, parse, read};

/// The epochs that round-trip: the padding shifts the top two bits out.
const EPOCHS: u32 = 1 << 30;

fn admitted_epoch() -> u32 {
    kani::any_where(|epoch: &u32| *epoch < EPOCHS)
}

fn read_token(token: [u8; 26]) -> Result<(u32, u64), OffsetError> {
    read(token.into_iter().map(char::from))
}

/// KANI-001: every admitted epoch and every `next` survive the codec, as
/// exactly 26 alphabet chars.
#[kani::proof]
#[kani::unwind(40)]
fn kani_001_every_admitted_epoch_and_next_round_trips() {
    let epoch = admitted_epoch();
    let next: u64 = kani::any();
    let token = digits(epoch, next);
    assert!(
        token.iter().all(|digit| ALPHABET.contains(digit)),
        "every char is a canonical alphabet symbol"
    );
    assert!(
        read_token(token) == Ok((epoch, next)),
        "epoch and next survive the round trip"
    );
    kani::cover!(epoch == EPOCHS - 1, "the top admitted epoch round-trips");
    kani::cover!(next == u64::MAX, "the last next round-trips");
    kani::cover!(epoch == 0 && next == 0, "start-of-stream round-trips");
}

/// KANI-002: "-1" and `next == 0` both name start-of-stream, and a token
/// resumes at exactly the `next` it was issued for, u64::MAX included, so
/// nothing wraps. The resume index survives every `u32` epoch: the bits
/// the padding drops are the epoch's, never `next`'s.
#[kani::proof]
#[kani::unwind(40)]
fn kani_002_start_and_resume_index() {
    assert!(parse("-1") == Ok((0, 0)), "-1 names start-of-stream");
    assert!(
        read_token(digits(0, 0)) == Ok((0, 0)),
        "next 0 names start-of-stream"
    );
    let epoch: u32 = kani::any();
    let next: u64 = kani::any();
    let resumed = read_token(digits(epoch, next)).map(|(_, next)| next);
    assert!(
        resumed == Ok(next),
        "a token resumes at the next it was issued for"
    );
    kani::cover!(next == 0, "a token resumes at the start");
    kani::cover!(next == u64::MAX, "the last next resumes without wrapping");
    kani::cover!(epoch == EPOCHS - 1, "the top admitted epoch resumes");
}

/// KANI-003: distinct admitted (epoch, next) positions have distinct
/// tokens, and token byte order (which is `String` order) is position
/// order. "-1" is never emitted, so no order is assumed for it.
#[kani::proof]
#[kani::unwind(40)]
fn kani_003_tokens_are_injective_and_ordered() {
    let left: (u32, u64) = (admitted_epoch(), kani::any());
    let right: (u32, u64) = (admitted_epoch(), kani::any());
    let (left_token, right_token) = (digits(left.0, left.1), digits(right.0, right.1));
    let positions = left.cmp(&right);
    // Injectivity first: order implies it, so a collision is reported
    // under its own name as well.
    assert!(
        (left_token == right_token) == positions.is_eq(),
        "distinct positions have distinct tokens"
    );
    assert!(
        left_token.cmp(&right_token) == positions,
        "token byte order is position order"
    );
    kani::cover!(left.0 == EPOCHS - 1, "the top admitted epoch is compared");
    kani::cover!(left.1 == u64::MAX, "the last next is compared");
    kani::cover!(right == (0, 0), "start-of-stream is compared");
}
