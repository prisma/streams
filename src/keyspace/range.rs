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
    /// anything else (a rig's label).
    pub(crate) fn of(prefix: &str) -> Self {
        Self::parse(prefix).unwrap_or(Self::ALL)
    }

    /// The range of the bit prefix `prefix`; `None` for anything that is not one: a character other
    /// than `'0'`/`'1'` (which `from_str_radix` would otherwise accept as a sign), or more bits than
    /// the route an entity hash carries (`keyspace::ROUTE_PREFIX_BITS`; past them a prefix would
    /// split one route's rows, and past 128 leading zeros would fit a `u128`). The root `""` is every
    /// hash, which `from_str_radix` would refuse as empty.
    pub(crate) fn parse(prefix: &str) -> Option<Self> {
        if prefix.is_empty() {
            return Some(Self::ALL);
        }
        let is_bits = prefix.len() <= super::ROUTE_PREFIX_BITS
            && prefix.bytes().all(|b| b == b'0' || b == b'1');
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

    /// Kani: the range of any `len`-bit prefix `bits` (KANI-106), without `parse`'s string.
    #[cfg(kani)]
    pub(crate) fn from_bits(bits: u128, len: u32) -> Self {
        Self { bits, len }
    }

    /// Whether the row of entity hash `hash` is in the range: the one test of which shard holds a
    /// row, typed so that a route or a raw engine identity cannot be passed for it (the S6 filter
    /// once tested footers' identities against a route-space range).
    pub(crate) fn contains(self, hash: super::EntityHash) -> bool {
        self.holds(&hash.bytes())
    }

    /// Whether a segment's route bytes are in the range: where the registry places the segment.
    pub(crate) fn holds_route(self, route: &[u8; 16]) -> bool {
        self.holds(route)
    }

    fn holds(self, bytes: &[u8; 16]) -> bool {
        if self.len == 0 {
            return true;
        }
        let shift = 128 - self.len;
        u128::from_be_bytes(*bytes) >> shift == self.bits >> shift
    }
}

#[cfg(test)]
mod tests;
