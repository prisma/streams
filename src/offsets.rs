//! Canonical Durable Streams offset encoding: the one codec every raw
//! surface (reads, appends, creates, fork offsets, SSE controls, peer
//! relays) speaks, so a position is the same string wherever it is minted.
//!
//! Offsets are 26-char Crockford base32 of a big-endian 128-bit tuple
//! (epoch u32, rawSeq u64 split hi/lo, in_block u32) padded to 130 bits.
//! rawSeq is `next`, the first record index a read at the token returns,
//! so the reserved "-1" and rawSeq 0 both name start-of-stream. The epoch
//! is the segment ordinal on per-key streams (PER-KEY-ORDERING.md §3) and
//! 0 elsewhere; the padding shifts its top two bits out of u128, so only
//! ordinals below 2^30 round-trip (segment ids are allocated from 1).

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Why a token names no position. The Display words are wire text: the
/// fork-offset refusal hands them to clients verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OffsetError {
    Length(usize),
    Char(char),
    Epoch(u32),
}

impl std::fmt::Display for OffsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetError::Length(len) => write!(f, "invalid offset length: {len}"),
            OffsetError::Char(ch) => write!(f, "invalid base32 char: {ch}"),
            OffsetError::Epoch(epoch) => write!(f, "unsupported offset epoch: {epoch}"),
        }
    }
}

/// The one token for "records from `next` on" in `epoch`; in_block is
/// always 0, so equal positions are equal strings on every surface.
pub(crate) fn encode(epoch: u32, next: u64) -> String {
    // Epoch and next occupy disjoint bits, so their sum is their concatenation.
    let n: u128 = ((epoch as u128) << 96) + ((next as u128) << 32);
    let padded = n << 2; // 128 -> 130 bits
    let mut out = String::with_capacity(26);
    for i in 0..26 {
        let shift = 5 * (25 - i);
        let idx = ((padded >> shift) & 31) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

/// The one decoder behind every raw surface; segmented reads take the
/// epoch as the segment. It reads, rather than refuses, bits no encoder
/// sets (pad, in_block, bits past u128, a non-ASCII char's low byte):
/// refusing them is a pending wire decision.
pub(crate) fn parse(input: &str) -> Result<(u32, u64), OffsetError> {
    if input == "-1" {
        return Ok((0, 0));
    }
    if input.len() != 26 {
        return Err(OffsetError::Length(input.len()));
    }
    let mut n: u128 = 0;
    for ch in input.chars() {
        let v = decode_char(ch).ok_or(OffsetError::Char(ch))?;
        n = (n << 5) + v as u128; // v < 32 fills the five bits the shift cleared
    }
    let n = n >> 2; // strip pad bits
    let epoch = (n >> 96) as u32;
    let next = ((n >> 32) & 0xffff_ffff_ffff_ffff) as u64;
    Ok((epoch, next))
}

/// Unsplit reads and fork offsets name positions in epoch 0 only: a
/// segment token there is refused, never re-based onto segment 0.
pub(crate) fn parse_scalar(input: &str) -> Result<u64, OffsetError> {
    match parse(input)? {
        (0, next) => Ok(next),
        (epoch, _) => Err(OffsetError::Epoch(epoch)),
    }
}

/// Digits and letters read as their ALPHABET index; O and I/L are
/// Crockford's only other spellings.
fn decode_char(ch: char) -> Option<u8> {
    match ch {
        'O' | 'o' => Some(0),
        'I' | 'i' | 'L' | 'l' => Some(1),
        _ => {
            let up = ch.to_ascii_uppercase() as u8;
            ALPHABET
                .iter()
                .position(|&a| a == up)
                .and_then(|p| u8::try_from(p).ok())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::{any, prop_assert, prop_assert_eq};

    #[test]
    fn round_trip() {
        for next in [0u64, 1, 2, 3, 42, (1 << 33) + 1, u64::MAX] {
            let t = encode(0, next);
            assert_eq!(t.len(), 26);
            assert_eq!(parse(&t), Ok((0, next)));
            assert_eq!(parse_scalar(&t), Ok(next));
        }
        assert_eq!(parse("-1"), Ok((0, 0)));
        assert_eq!(parse_scalar("-1"), Ok(0));
        assert_eq!(encode(0, 0), "00000000000000000000000000");
    }

    #[test]
    fn epoch_round_trip() {
        for (e, next) in [(0u32, 6u64), (3, 1), (255, (1 << 40) + 1)] {
            assert_eq!(parse(&encode(e, next)), Ok((e, next)));
        }
        assert_eq!(parse_scalar(&encode(3, 1)), Err(OffsetError::Epoch(3)));
        // Tokens order lexicographically within a segment and across ordinals.
        assert!(encode(1, 1) > encode(0, 1000));
    }

    /// Today's decoder reads four kinds of non-canonical token as a position
    /// instead of refusing it: a first digit above '7' (its top bits fall off
    /// u128), a two-byte char (the gate counts bytes, the loop counts chars and
    /// `as u8` keeps the low byte), nonzero pad bits and nonzero in_block bits.
    /// Refusing them is a pending wire decision (review item 88 step 2); until
    /// it is made, no refactor may move them.
    #[test]
    fn non_canonical_tokens_keep_their_lax_reading() {
        assert_eq!(parse("G0000000000000000000000000"), Ok((0, 0)));
        assert_eq!(parse("00000000000000000\u{131}0000000"), Ok((0, 2)));
        assert_eq!(parse("0000000000000000000G000003"), Ok((0, 1)));
        assert_eq!(parse("0000000000000000000G000010"), Ok((0, 1)));
    }

    /// Crockford's O and I/L spellings are a wire-visible reading a strict
    /// decoder must decide on explicitly (review item 88 step 2); until then
    /// no refactor may move them.
    #[test]
    fn crockford_aliases_read_as_their_digits() {
        for alias in [
            "000000000000000000I0000000",
            "000000000000000000i0000000",
            "000000000000000000L0000000",
            "000000000000000000l0000000",
        ] {
            assert_eq!(parse(alias), Ok((0, 2)), "{alias}");
        }
        assert_eq!(parse("OOOOOOOOOOOOOOOOOOOOOOOOOo"), Ok((0, 0)));
    }

    /// The fork-offset refusal hands these words to clients verbatim.
    #[test]
    fn refusal_words_are_wire_text() {
        assert_eq!(
            OffsetError::Length(1).to_string(),
            "invalid offset length: 1"
        );
        assert_eq!(OffsetError::Char('U').to_string(), "invalid base32 char: U");
        assert_eq!(
            OffsetError::Epoch(3).to_string(),
            "unsupported offset epoch: 3"
        );
    }

    /// The characters a token can carry, canonical or not.
    fn token_char() -> impl proptest::strategy::Strategy<Value = char> {
        proptest::sample::select(vec![
            '0', '1', '7', '8', 'G', 'Z', 'O', 'o', 'I', 'i', 'L', 'l', 'a', 'z', 'U', '-',
            '\u{131}',
        ])
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(1024))]
        /// Every position the codec can carry (epochs below 2^30, see the
        /// module doc) has one 26-char token that decodes back to it, and
        /// tokens sort like positions (offsets are opaque, lexicographically
        /// sortable strings).
        #[test]
        fn quality_offsets_every_position_has_one_ordered_token(
            a in (0u32..(1 << 30), any::<u64>()),
            b in (0u32..(1 << 30), any::<u64>()),
        ) {
            let (left, right) = (encode(a.0, a.1), encode(b.0, b.1));
            prop_assert_eq!(left.len(), 26);
            prop_assert_eq!(parse(&left), Ok(a));
            prop_assert_eq!(left.cmp(&right), a.cmp(&b));
        }

        /// Any input is either refused or names one position the encoder
        /// represents canonically: a wrong length is always the length
        /// refusal, an accepted epoch fits the 30 bits that round-trip, and
        /// the scalar decoder is the general one restricted to epoch 0.
        #[test]
        fn quality_offsets_any_accepted_token_names_one_canonical_position(
            chars in proptest::collection::vec(token_char(), 0..31),
            reserved in any::<bool>(),
        ) {
            let input: String = if reserved { "-1".to_string() } else { chars.into_iter().collect() };
            let parsed = parse(&input);
            if input != "-1" && input.len() != 26 {
                prop_assert_eq!(parsed, Err(OffsetError::Length(input.len())));
            }
            if let Ok((epoch, next)) = parsed {
                prop_assert!(epoch < (1 << 30), "epoch {} past the 30 bits", epoch);
                prop_assert_eq!(parse(&encode(epoch, next)), Ok((epoch, next)));
            }
            let scalar = match parsed {
                Ok((0, next)) => Ok(next),
                Ok((epoch, _)) => Err(OffsetError::Epoch(epoch)),
                Err(e) => Err(e),
            };
            prop_assert_eq!(parse_scalar(&input), scalar);
        }
    }
}
