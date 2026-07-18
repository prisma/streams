//! Canonical Durable Streams offset encoding.
//!
//! Offsets are 26-char Crockford base32 of a big-endian 128-bit tuple
//! (epoch u32, rawSeq u64 split hi/lo, in_block u32) where rawSeq = seq + 1 so
//! the reserved "-1" (start-of-stream) never appears in encoded form.

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Logical position: `None` = start-of-stream ("-1"), `Some(n)` = entry n.
/// A read at position P returns entries with index strictly greater than P.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Offset(pub Option<u64>);

impl Offset {
    pub const START: Offset = Offset(None);

    /// First record index covered by a read at this offset.
    pub fn scan_from(self) -> u64 {
        match self.0 {
            None => 0,
            Some(n) => n + 1,
        }
    }

    pub fn encode(self) -> String {
        let raw_seq: u64 = match self.0 {
            None => 0,
            Some(n) => n + 1,
        };
        let n: u128 = (raw_seq as u128) << 32; // epoch=0 (bits 96..128), in_block=0 (bits 0..32)
        let padded = n << 2; // 128 -> 130 bits
        let mut out = String::with_capacity(26);
        for i in 0..26 {
            let shift = 5 * (25 - i);
            let idx = ((padded >> shift) & 31) as usize;
            out.push(ALPHABET[idx] as char);
        }
        out
    }

    pub fn parse(input: &str) -> Result<Offset, String> {
        if input == "-1" {
            return Ok(Offset::START);
        }
        if input.len() != 26 {
            return Err(format!("invalid offset length: {}", input.len()));
        }
        let mut n: u128 = 0;
        for ch in input.chars() {
            let v = decode_char(ch).ok_or_else(|| format!("invalid base32 char: {ch}"))?;
            n = (n << 5) | v as u128;
        }
        let n = n >> 2; // strip pad bits
        let epoch = (n >> 96) as u32;
        let raw_seq = ((n >> 32) & 0xffff_ffff_ffff_ffff) as u64;
        if epoch != 0 {
            return Err(format!("unsupported offset epoch: {epoch}"));
        }
        if raw_seq == 0 {
            Ok(Offset::START)
        } else {
            Ok(Offset(Some(raw_seq - 1)))
        }
    }
}

fn decode_char(ch: char) -> Option<u8> {
    match ch {
        '0'..='9' => Some(ch as u8 - b'0'),
        'O' | 'o' => Some(0),
        'I' | 'i' | 'L' | 'l' => Some(1),
        _ => {
            let up = ch.to_ascii_uppercase() as u8;
            ALPHABET.iter().position(|&a| a == up).map(|p| p as u8)
        }
    }
}

/// Per-key streams (PER-KEY-ORDERING.md §3): epoch = segment ordinal.
pub fn encode_ep(epoch: u32, o: Offset) -> String {
    let raw_seq: u64 = match o.0 {
        None => 0,
        Some(n) => n + 1,
    };
    let n: u128 = ((epoch as u128) << 96) | ((raw_seq as u128) << 32);
    let padded = n << 2;
    let mut out = String::with_capacity(26);
    for i in 0..26 {
        let shift = 5 * (25 - i);
        let idx = ((padded >> shift) & 31) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

/// Parse accepting any epoch (per-key streams). "-1" => (0, START).
pub fn parse_ep(input: &str) -> Result<(u32, Offset), String> {
    if input == "-1" {
        return Ok((0, Offset::START));
    }
    if input.len() != 26 {
        return Err(format!("invalid offset length: {}", input.len()));
    }
    let mut n: u128 = 0;
    for ch in input.chars() {
        let v = decode_char(ch).ok_or_else(|| format!("invalid base32 char: {ch}"))?;
        n = (n << 5) | v as u128;
    }
    let n = n >> 2;
    let epoch = (n >> 96) as u32;
    let raw_seq = ((n >> 32) & 0xffff_ffff_ffff_ffff) as u64;
    let off = if raw_seq == 0 {
        Offset::START
    } else {
        Offset(Some(raw_seq - 1))
    };
    Ok((epoch, off))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for n in [0u64, 1, 2, 41, 1 << 33, u64::MAX - 1] {
            let s = Offset(Some(n)).encode();
            assert_eq!(s.len(), 26);
            assert_eq!(Offset::parse(&s).unwrap(), Offset(Some(n)));
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
        for (e, n) in [(0u32, 5u64), (3, 0), (255, 1 << 40)] {
            let s = encode_ep(e, Offset(Some(n)));
            assert_eq!(parse_ep(&s).unwrap(), (e, Offset(Some(n))));
        }
        // Epoch-0 encoding matches the total-order codec exactly.
        assert_eq!(encode_ep(0, Offset(Some(7))), Offset(Some(7)).encode());
        // Tokens order lexicographically within a segment and across ordinals.
        assert!(encode_ep(1, Offset(Some(0))) > encode_ep(0, Offset(Some(999))));
    }
}
