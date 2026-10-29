//! Kani proofs for the page format's minimal varint: KANI-006. Every page
//! table, page header count and seal directory row is read through
//! `get_varint` from bytes an attacker may have written, so the decoder
//! must terminate within ten bytes and accept exactly one spelling of each
//! u64: its minimal LEB128 encoding. Both harnesses compare the production
//! codec with oracles of their own: a minimal encoder (low seven bits
//! first, stop when nothing is left) and a u128 reading of the continuation
//! rule, so no bit can fall off the oracle. A run is classified as
//! unterminated, overflowing (past u64), overlong (terminated and in range
//! but not the minimal encoding of its value) or accepted, and each refusal
//! is its own property. The decoder reads at most ten bytes of an input of
//! eleven, so it reads at most ten of any longer input too: what it does
//! depends only on the bytes it reads. Loops run at most eleven times, so
//! `unwind(12)` and `unwind(13)` cover them with Kani's unwinding checks on.
use super::{get_varint, put_varint, varint_len};

/// A u64 needs at most ten seven-bit groups.
const MAX_LEN: usize = 10;

/// The oracle encoder: the minimal LEB128 encoding of `value` into `out`,
/// and its length.
fn minimal(value: u64, out: &mut [u8; MAX_LEN]) -> usize {
    let mut rest = value;
    let mut n = 0;
    loop {
        let low = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest == 0 {
            out[n] = low;
            return n + 1;
        }
        out[n] = low | 0x80;
        n += 1;
    }
}

/// KANI-006 both sides of the codec for every u64: `put_varint` writes
/// exactly the oracle's minimal encoding, `varint_len` counts its bytes,
/// and `get_varint` reads that encoding back as the value, consuming
/// exactly it. The two halves are asserted apart, so a writer defect and a
/// reader defect each fail their own property.
#[kani::proof]
#[kani::unwind(12)]
fn kani_006_every_u64_is_written_and_read_as_its_minimal_encoding() {
    let value: u64 = kani::any();
    let mut spelling = [0u8; MAX_LEN];
    let len = minimal(value, &mut spelling);
    let mut written = Vec::new();
    put_varint(&mut written, value);
    assert!(
        written.as_slice() == &spelling[..len],
        "put_varint writes the minimal encoding of its value"
    );
    assert!(
        varint_len(value) == len,
        "varint_len counts the minimal encoding's bytes"
    );
    let mut input: &[u8] = &spelling[..len];
    let read = get_varint(&mut input);
    assert!(
        read == Some(value) && input.is_empty(),
        "the minimal encoding is read back as its value, consuming exactly itself"
    );
    kani::cover!(value == 0, "zero is one byte");
    kani::cover!(value == 128, "the first two-byte value");
    kani::cover!(value == u64::MAX, "the widest value takes ten bytes");
}

/// What a run of bytes spells under the continuation rule.
#[derive(Clone, Copy)]
enum Run {
    /// No byte with the top bit clear among the first ten.
    Unterminated,
    /// Terminated, but its value is past u64.
    Overflowing,
    /// Terminated and in range, but not the minimal encoding of its value.
    Overlong,
    /// The minimal encoding of `value`, `used` bytes long.
    Minimal { value: u64, used: usize },
}

/// The oracle decoder: read up to ten groups in u128, then accept the run
/// only when its bytes are exactly the oracle encoder's spelling of the
/// value.
fn classify(bytes: &[u8]) -> Run {
    let mut wide: u128 = 0;
    for (i, byte) in bytes.iter().enumerate().take(MAX_LEN) {
        wide |= u128::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            let Ok(value) = u64::try_from(wide) else {
                return Run::Overflowing;
            };
            let mut spelling = [0u8; MAX_LEN];
            let len = minimal(value, &mut spelling);
            return if bytes[..=i] == spelling[..len] {
                Run::Minimal { value, used: i + 1 }
            } else {
                Run::Overlong
            };
        }
    }
    Run::Unterminated
}

/// KANI-006 the decoder over every symbolic input of 0 to 11 bytes (the
/// ten-byte maximum plus one): it accepts exactly the minimal encoding of a
/// u64 and consumes exactly it, refuses every unterminated, overflowing or
/// overlong run, and never reads more than ten bytes.
#[kani::proof]
#[kani::unwind(13)]
fn kani_006_decoding_accepts_exactly_minimal_encodings() {
    let bytes: [u8; MAX_LEN + 1] = kani::any();
    let len: usize = kani::any_where(|len: &usize| *len <= MAX_LEN + 1);
    let mut input: &[u8] = &bytes[..len];
    let decoded = get_varint(&mut input);
    let consumed = len - input.len();
    let run = classify(&bytes[..len]);
    match run {
        Run::Minimal { value, used } => assert!(
            decoded == Some(value) && consumed == used,
            "the minimal encoding of a u64 decodes to its value and consumes exactly itself"
        ),
        Run::Unterminated => assert!(decoded.is_none(), "an unterminated run is refused"),
        Run::Overflowing => assert!(decoded.is_none(), "an overflowing run is refused"),
        Run::Overlong => assert!(decoded.is_none(), "an overlong run is refused"),
    }
    assert!(consumed <= MAX_LEN, "decoding reads at most ten bytes");
    kani::cover!(
        decoded == Some(u64::MAX),
        "the widest value decodes from symbolic bytes"
    );
    kani::cover!(
        matches!(run, Run::Overflowing),
        "a tenth group past u64 is refused"
    );
    kani::cover!(
        matches!(run, Run::Overlong),
        "a redundant zero last group is refused"
    );
    kani::cover!(
        matches!(run, Run::Minimal { used, .. } if used < len),
        "bytes after the terminator are left unread"
    );
}
