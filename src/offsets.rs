//! Canonical Durable Streams offset encoding.
//!
//! Offsets are 26-char Crockford base32 of a big-endian 128-bit tuple
//! (epoch u32, rawSeq u64 split hi/lo, in_block u32) followed by two zero pad
//! bits, 130 bits in all. rawSeq = seq + 1 so the reserved "-1"
//! (start-of-stream) never appears in encoded form. The epoch is the whole
//! `u32` segment ordinal: the leading digit carries its top bits.

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
/// Digits in a token: 26 * 5 bits hold the 128-bit tuple and its 2 pad bits.
const DIGITS: usize = 26;

/// A read position, held as the first record index a read at it covers
/// (the token's rawSeq): 0 is start-of-stream ("-1"), `n + 1` is "after
/// entry n". Every `u64` is a valid position, so there is no successor
/// arithmetic to overflow, wrap into START, or differ between debug and
/// release builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Offset {
    next: u64,
}

impl Offset {
    pub(crate) const START: Offset = Offset { next: 0 };

    /// The position a read resumes from to cover record index `next` onward:
    /// START for 0, otherwise "after entry `next - 1`".
    pub(crate) const fn before(next: u64) -> Offset {
        Offset { next }
    }

    /// First record index covered by a read at this offset.
    pub(crate) const fn scan_from(self) -> u64 {
        self.next
    }

    pub(crate) fn encode(self) -> String {
        encode_ep(0, self)
    }

    pub(crate) fn parse(input: &str) -> Result<Offset, String> {
        match parse_ep(input)? {
            (0, offset) => Ok(offset),
            (epoch, _) => Err(format!("unsupported offset epoch: {epoch}")),
        }
    }
}

/// The alphabet position of one token byte, accepting Crockford's case and
/// O/I/L aliases. A non-ASCII byte is never a digit.
fn decode_digit(byte: u8) -> Option<u8> {
    match byte.to_ascii_uppercase() {
        b'O' => Some(0),
        b'I' | b'L' => Some(1),
        upper => ALPHABET
            .iter()
            .position(|&symbol| symbol == upper)
            .and_then(|position| u8::try_from(position).ok()),
    }
}

/// Per-key streams (PER-KEY-ORDERING.md §3): epoch = segment ordinal.
pub(crate) fn encode_ep(epoch: u32, offset: Offset) -> String {
    token_digits(epoch, offset)
        .iter()
        .copied()
        .map(char::from)
        .collect()
}

/// Parse accepting any epoch (per-key streams). "-1" => (0, START).
pub(crate) fn parse_ep(input: &str) -> Result<(u32, Offset), String> {
    if input == "-1" {
        return Ok((0, Offset::START));
    }
    let digits = <&[u8; DIGITS]>::try_from(input.as_bytes())
        .map_err(|_| format!("invalid offset length: {}", input.len()))?;
    parse_digits(digits).map_err(|byte| format!("invalid base32 char: {}", byte.escape_ascii()))
}

/// The token's ASCII digits, most significant first.
fn token_digits(epoch: u32, offset: Offset) -> [u8; DIGITS] {
    let tuple = (u128::from(epoch) << 96) | (u128::from(offset.next) << 32);
    // The last digit is the tuple's three lowest bits above the two pad
    // bits; each earlier digit takes the next five bits up, so the leading
    // digit holds the top of the epoch rather than shifting it out.
    let mut digits = [0u8; DIGITS];
    let mut value = (tuple & 0b111) << 2;
    let mut rest = tuple >> 3;
    for digit in digits.iter_mut().rev() {
        *digit = ALPHABET[(value & 31) as usize];
        value = rest;
        rest >>= 5;
    }
    digits
}

/// The (epoch, position) a token's digits encode, or the first byte that is
/// not a digit.
fn parse_digits(digits: &[u8; DIGITS]) -> Result<(u32, Offset), u8> {
    let mut tuple: u128 = 0;
    for (index, &byte) in digits.iter().enumerate() {
        let digit = decode_digit(byte).ok_or(byte)?;
        // The final digit contributes its three data bits; its two pad bits
        // lie below the tuple.
        tuple = if index + 1 < DIGITS {
            (tuple << 5) | u128::from(digit)
        } else {
            (tuple << 3) | u128::from(digit >> 2)
        };
    }
    let epoch = (tuple >> 96) as u32;
    let raw_seq = ((tuple >> 32) & 0xffff_ffff_ffff_ffff) as u64;
    Ok((epoch, Offset::before(raw_seq)))
}

#[cfg(kani)]
mod proofs;

#[cfg(test)]
mod tests {
    use super::{Offset, encode_ep, parse_ep};
    use proptest::prelude::{any, prop_assert, prop_assert_eq};

    #[test]
    fn round_trip() {
        for next in [1u64, 2, 3, 42, (1 << 33) + 1, u64::MAX] {
            let s = Offset::before(next).encode();
            assert_eq!(s.len(), 26);
            assert_eq!(Offset::parse(&s).unwrap(), Offset::before(next));
        }
        assert_eq!(Offset::parse("-1").unwrap(), Offset::START);
        assert_eq!(
            Offset::parse(&Offset::START.encode()).unwrap(),
            Offset::START
        );
        assert_eq!(Offset::START.encode(), "00000000000000000000000000");
    }

    #[test]
    fn epoch_round_trip() {
        for (e, next) in [(0u32, 6u64), (3, 1), (255, (1 << 40) + 1)] {
            let s = encode_ep(e, Offset::before(next));
            assert_eq!(parse_ep(&s).unwrap(), (e, Offset::before(next)));
        }
        // Epoch-0 encoding matches the total-order codec exactly.
        assert_eq!(encode_ep(0, Offset::before(8)), Offset::before(8).encode());
        // Tokens order lexicographically within a segment and across ordinals.
        assert!(encode_ep(1, Offset::before(1)) > encode_ep(0, Offset::before(1000)));
        let top = (1u32 << 30) - 1;
        assert!(encode_ep(top + 1, Offset::START) > encode_ep(top, Offset::before(u64::MAX)));
        assert!(encode_ep(u32::MAX, Offset::START) > encode_ep(u32::MAX - 1, Offset::before(1)));
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config { cases: 1024, ..Default::default() })]
        #[test]
        fn quality_every_epoch_and_position_round_trips(epoch in any::<u32>(), next in any::<u64>()) {
            let token = encode_ep(epoch, Offset::before(next));
            prop_assert_eq!(token.len(), 26);
            prop_assert_eq!(parse_ep(&token), Ok((epoch, Offset::before(next))));
            prop_assert_eq!(Offset::parse(&token).is_ok(), epoch == 0);
        }

        #[test]
        fn quality_a_parsed_token_is_its_position(bytes in proptest::collection::vec(any::<u8>(), 0..40)) {
            let input = String::from_utf8_lossy(&bytes);
            if let Ok((epoch, offset)) = parse_ep(&input) {
                prop_assert!(input == "-1" || (input.len() == 26 && input.is_ascii()));
                prop_assert_eq!(parse_ep(&encode_ep(epoch, offset)), Ok((epoch, offset)));
            }
        }
    }

    #[test]
    fn a_multibyte_char_is_not_a_digit() {
        // 24 ASCII digits plus a two-byte char is 26 bytes but 25 chars. The
        // char-wise parser read it as 25 digits and truncated U+0141 to 'A'.
        let token = format!("{}\u{141}", "0".repeat(24));
        assert_eq!(token.len(), 26);
        assert!(parse_ep(&token).is_err());
        assert!(Offset::parse(&token).is_err());
    }
}
