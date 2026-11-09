//! A shard's hash range: the hashes whose leading bits are its prefix. In layout 8 every entity row of a
//! shard lies in one key interval because keys start with the hash (S3a-DESIGN §1), so this is the one
//! matcher the registry's routing and the seal's walks share (S6-DESIGN rev 4 §3.1: after a split, a
//! child counts and carries only the hashes in its own range).
//!
//! A production shard's prefix is a string of bits (`'0'`/`'1'`, the registry's longest-prefix code,
//! `registry::shard_for_hash`). `parse` reads only that and refuses anything else: a test rig's label, a
//! run longer than a hash, a sign. The registry routes a refused prefix nowhere; the seal reads it as
//! every hash (`of`), as the root prefix `""` is, since its rigs name shards with labels.

/// The hashes whose first `len` bits equal `bits`' first `len` bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Range {
    bits: u128,
    len: u32,
}

impl Range {
    /// Every hash.
    pub(crate) const ALL: Self = Self { bits: 0, len: 0 };

    /// The range of the shard named `prefix` for the seal's walks: its bit prefix, or every hash for
    /// anything else (a rig's label). Test-only until layout 8's entity hash re-lands the seal's
    /// filters (ROUTE-HASH-DECISION-2026-11-08 §4.3).
    #[cfg(test)]
    pub(crate) fn of(prefix: &str) -> Self {
        Self::parse(prefix).unwrap_or(Self::ALL)
    }

    /// The range of the bit prefix `prefix`; `None` for anything that is not one: a character other
    /// than `'0'`/`'1'` (which `from_str_radix` would otherwise accept as a sign), or more bits than a
    /// hash has (whose leading zeros would otherwise fit a `u128`). The root `""` is every hash, which
    /// `from_str_radix` would refuse as empty.
    pub(crate) fn parse(prefix: &str) -> Option<Self> {
        if prefix.is_empty() {
            return Some(Self::ALL);
        }
        let is_bits = prefix.len() <= 128 && prefix.bytes().all(|b| b == b'0' || b == b'1');
        if !is_bits {
            return None;
        }
        let value = u128::from_str_radix(prefix, 2).ok()?;
        let len = u32::try_from(prefix.len()).ok()?;
        Some(Self {
            bits: value.checked_shl(128 - len).unwrap_or(0),
            len,
        })
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
