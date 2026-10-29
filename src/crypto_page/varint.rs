//! The minimal LEB128 varint every page table, page header and seal
//! directory row is written in: seven bits per byte, low group first, the
//! top bit set on every byte but the last. A leaf with no dependencies, so
//! the seal directory's row codec and its fuzz target share it unchanged.

/// Append `value` as a minimal LEB128 varint: its `varint_len` seven-bit
/// groups, low first. A group that another follows is the remaining
/// value's low byte with its top bit set; the last is the remaining value,
/// below 0x80. The group count bounds the loop, so the writer always ends.
pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    for _ in 1..varint_len(value) {
        let [low, ..] = value.to_le_bytes();
        out.push(low | 0x80);
        value >>= 7;
    }
    let [last, ..] = value.to_le_bytes();
    out.push(last);
}

/// Read one minimal LEB128 varint and consume exactly its bytes. An
/// unterminated run, a value past u64 and a redundant zero last group (an
/// overlong encoding) are refused.
pub(crate) fn get_varint(input: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input.split_first()?;
        *input = rest;
        let group = u64::from(byte & 0x7f);
        if shift == 63 && group > 1 {
            return None;
        }
        value |= group << shift;
        if byte & 0x80 == 0 {
            return (byte != 0 || shift == 0).then_some(value);
        }
    }
    None
}

/// Bytes the minimal varint of `value` takes: one per started seven bits.
pub(super) fn varint_len(value: u64) -> usize {
    let bits = u64::BITS.saturating_sub(value.leading_zeros()).max(1);
    bits.div_ceil(7) as usize
}
