//! A shard's hash range (S6-DESIGN rev 4 §3.1): the streams whose chunks
//! this shard counts in its `'R'` rows. After a split a child inherits run-
//! index objects and segment objects that also hold its sibling's streams;
//! every walk that counts an object's chunks or carries an inherited run's
//! entries keeps only the hashes in this range, so a child's count of an
//! object is exactly its own half's (L2).
//!
//! A production shard's prefix is a string of bits (`'0'`/`'1'`, the
//! registry's longest-prefix code, `registry::shard_for_hash`); its range is
//! every hash whose leading bits are that string. A prefix with any other
//! character (a test rig's label) covers every hash, as the root prefix `""`
//! does.

/// The hashes whose first `len` bits equal `bits`' first `len` bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Range {
    bits: u128,
    len: u32,
}

impl Range {
    /// Every hash.
    pub(crate) const ALL: Self = Self { bits: 0, len: 0 };

    /// The range of the shard named `prefix`.
    pub(crate) fn of(prefix: &str) -> Self {
        let is_bits = prefix.len() <= 128 && prefix.bytes().all(|b| b == b'0' || b == b'1');
        if !is_bits {
            return Self::ALL;
        }
        let Ok(value) = u128::from_str_radix(prefix, 2) else {
            return Self::ALL;
        };
        let len = u32::try_from(prefix.len()).unwrap_or(128);
        Self {
            bits: value.checked_shl(128 - len).unwrap_or(0),
            len,
        }
    }

    /// Whether `hash` is in the range.
    pub(crate) fn contains(self, hash: &[u8; 16]) -> bool {
        if self.len == 0 {
            return true;
        }
        let shift = 128 - self.len;
        u128::from_be_bytes(*hash) >> shift == self.bits >> shift
    }
}

#[cfg(test)]
mod tests;
