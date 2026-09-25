//! Kani proofs for the postings varint codec: KANI-006. The harnesses call
//! the production `put_varint` and `get_varint`, whose runs every postings
//! page is made of (spec §6.4). The decoder is checked against an
//! independent wide-arithmetic oracle over symbolic bytes, so a shift or an
//! accumulation that drops a significant bit, or a consumption past the
//! terminator, is reported by name. The format promises continuation and
//! termination semantics, not canonical encodings: an overlong but
//! in-range encoding decodes, and the proofs do not require it refused.
//! Loops are bounded by the 10-byte maximum representation, so
//! `unwind(12)` covers them with Kani's unwinding checks left on.
use super::{get_varint, put_varint};

/// A u64 needs at most ten 7-bit groups.
const MAX_LEN: usize = 10;

/// KANI-006 round trip: every u64 encodes to at most ten bytes, every byte
/// but the last carries the continuation bit, and decoding reads back
/// exactly the value while consuming exactly those bytes.
#[kani::proof]
#[kani::unwind(12)]
fn kani_006_every_u64_round_trips_and_consumes_its_encoding() {
    let value: u64 = kani::any();
    let mut encoded = Vec::new();
    put_varint(&mut encoded, value);
    assert!(
        !encoded.is_empty() && encoded.len() <= MAX_LEN,
        "an encoding is one to ten bytes"
    );
    let (last, body) = encoded.split_last().unwrap();
    assert!(
        last & 0x80 == 0 && body.iter().all(|byte| byte & 0x80 != 0),
        "only the last byte terminates"
    );
    let mut input: &[u8] = &encoded;
    assert!(
        get_varint(&mut input) == Some(value),
        "the value survives the round trip"
    );
    assert!(input.is_empty(), "decoding consumes exactly the encoding");
    kani::cover!(value == u64::MAX, "the widest value round-trips");
    kani::cover!(value == 0, "zero round-trips");
    kani::cover!(encoded.len() == MAX_LEN, "a ten-byte encoding round-trips");
}

/// The oracle: what `bytes` spell under the format's continuation rule, in
/// u128 so no bit can fall off. `(value, consumed)` for the first
/// terminated run of at most ten bytes, `None` when none terminates there.
fn oracle(bytes: &[u8]) -> Option<(u128, usize)> {
    let mut value: u128 = 0;
    for (i, byte) in bytes.iter().enumerate().take(MAX_LEN) {
        value |= u128::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// KANI-006 decoder over every symbolic input of up to eleven bytes (the
/// maximum representation plus an overlong byte): the decoder answers the
/// oracle's value exactly when that value fits a u64, consumes exactly the
/// oracle's bytes, and otherwise refuses; it never reads past the
/// terminator or the input.
#[kani::proof]
#[kani::unwind(13)]
fn kani_006_decoding_matches_the_wide_oracle() {
    let bytes: [u8; MAX_LEN + 1] = kani::any();
    let len: usize = kani::any_where(|len: &usize| *len <= MAX_LEN + 1);
    let mut input: &[u8] = &bytes[..len];
    let decoded = get_varint(&mut input);
    let consumed = len - input.len();
    match oracle(&bytes[..len]) {
        Some((value, used)) if value <= u128::from(u64::MAX) => {
            assert!(
                decoded == u64::try_from(value).ok(),
                "no significant bit is dropped"
            );
            assert!(consumed == used, "decoding stops at the terminator");
        }
        _ => assert!(
            decoded.is_none(),
            "an unterminated or overflowing run is refused"
        ),
    }
    assert!(consumed <= len, "decoding never reads past its input");
    kani::cover!(
        decoded == Some(u64::MAX),
        "the widest value decodes from symbolic bytes"
    );
    kani::cover!(
        len == MAX_LEN && bytes[MAX_LEN - 1] & 0x7f > 1 && decoded.is_none(),
        "a tenth byte past u64 is refused"
    );
    kani::cover!(
        oracle(&bytes[..len]).is_some_and(|(_, used)| used < len),
        "trailing bytes after a terminator are left unread"
    );
}
