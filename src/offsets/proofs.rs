//! Kani proofs for the offset codec: KANI-001 (epoch-aware round trip),
//! KANI-002 (START and the successor index) and KANI-003 (injectivity and
//! order). The harnesses call the production digit codec that `encode_ep`
//! and `parse_ep` wrap, over full-width `u32` epochs and `u64` positions.
//! The wrappers only add the "-1" literal and the byte-for-char `String`
//! conversion of ASCII digits; symbolic `String` growth is out of scope
//! for CBMC here, and the unit and property tests exercise those wrappers.
//! Loops are fixed at 26 digits and a 32-symbol alphabet, so `unwind(40)`
//! covers them with Kani's unwinding checks left on.
use super::{DIGITS, Offset, parse_digits, token_digits};

/// KANI-001: every admitted epoch and position survives the codec, as
/// exactly 26 alphabet digits.
#[kani::proof]
#[kani::unwind(40)]
fn kani_001_every_epoch_and_position_round_trips() {
    let epoch: u32 = kani::any();
    let offset = Offset::before(kani::any());
    let digits = token_digits(epoch, offset);
    assert!(digits.len() == DIGITS, "a token is 26 digits");
    assert!(
        digits
            .iter()
            .all(|digit| super::ALPHABET.iter().any(|symbol| symbol == digit)),
        "every digit is a canonical alphabet symbol"
    );
    assert!(
        parse_digits(&digits) == Ok((epoch, offset)),
        "epoch and position survive the round trip"
    );
    kani::cover!(epoch >= 1 << 30, "an epoch with a top bit set round-trips");
    kani::cover!(
        offset.scan_from() == u64::MAX,
        "the last position round-trips"
    );
}

/// KANI-002: START is scan index 0 and position 0 of its epoch; every
/// position resumes exactly at its own index, so none wraps into START or
/// below itself, including the last `u64`.
#[kani::proof]
#[kani::unwind(40)]
fn kani_002_start_and_successor_indices() {
    assert!(Offset::START.scan_from() == 0, "START scans from index 0");
    let next: u64 = kani::any();
    let offset = Offset::before(next);
    assert!(
        offset.scan_from() == next,
        "a position resumes at its own index"
    );
    assert!(
        (offset == Offset::START) == (next == 0),
        "only index 0 is START"
    );
    let epoch: u32 = kani::any();
    let resumed = parse_digits(&token_digits(epoch, offset)).map(|(_, offset)| offset.scan_from());
    assert!(
        resumed == Ok(next),
        "a token resumes at the index it was issued for"
    );
    kani::cover!(next == 0, "an empty stream's position is START");
    kani::cover!(next == u64::MAX, "the exhausted index round-trips");
}

/// KANI-003: distinct (epoch, position) tuples have distinct tokens, and
/// byte order (which is `String` order) is tuple order. START is position 0
/// of its epoch; "-1" is never emitted, so no order is assumed for it.
#[kani::proof]
#[kani::unwind(40)]
fn kani_003_tokens_are_injective_and_ordered() {
    let (left_epoch, left_next): (u32, u64) = (kani::any(), kani::any());
    let (right_epoch, right_next): (u32, u64) = (kani::any(), kani::any());
    let left = token_digits(left_epoch, Offset::before(left_next));
    let right = token_digits(right_epoch, Offset::before(right_next));
    let tuples = (left_epoch, left_next).cmp(&(right_epoch, right_next));
    // Injectivity first: once order holds it implies injectivity, so a
    // collision must be caught here for its own control to break it.
    assert!(
        (left == right) == tuples.is_eq(),
        "distinct tuples have distinct tokens"
    );
    assert!(left.cmp(&right) == tuples, "token order is tuple order");
    kani::cover!(
        left_epoch != right_epoch && left_epoch & 0x3fff_ffff == right_epoch & 0x3fff_ffff,
        "epochs that differ only in their top two bits are compared"
    );
}
