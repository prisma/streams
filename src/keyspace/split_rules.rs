#![cfg(test)]
//! What a split does with each of the parent's sentinel rows (S3a-DESIGN §2.8, agreed with S6 §7.1):
//! one rule per tag, the table as code, so S4's split and S6's merge build a child's sentinel by
//! the same rules and a new tag cannot exist without one (`tests::every_sentinel_tag_has_a_split_rule`).
//! "Hash" is the entity hash (§1.1), so a rule that routes by hash routes by the segment's route bits.

use super::{EntityHash, SENTINEL, tag};

/// A tag's rule at a split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SplitRule {
    /// Recomputed from the child's own entries (`'R'`, `'O'`).
    Recompute,
    /// Both children take the row as it is, and every reader keeps only its range's entries (`'K'`
    /// and `'I'`: both index the parent's objects; K-SPLIT-DECISION-2026-11-09).
    CopyToBoth,
    /// Neither child takes it (`'G'`, `'L'`, `'E'`, `'C'`).
    Drop,
    /// The child whose range holds the hash right after the tag takes it (`'H'`, `'U'`, `'W'`,
    /// `'M'`, `'D'`).
    ByHash,
    /// The child whose range holds the key's last sixteen bytes takes it (`'Z'`, `'Q'`).
    ByTrailingHash,
    /// Written fresh for the child (`'Y'`: its layout and its own home).
    WrittenFresh,
    /// Recomputed in S4's preparation from the child's queues' state rows, never at open (`'N'`).
    RecomputedInPreparation,
    /// Not in a child at all (`'T'`, written into the retired parent).
    Absent,
}

/// The rule of sentinel tag `t`; `None` for a byte that is no sentinel tag.
pub(crate) fn split_rule(t: u8) -> Option<SplitRule> {
    use SplitRule::{
        Absent, ByHash, ByTrailingHash, CopyToBoth, Drop, Recompute, RecomputedInPreparation,
        WrittenFresh,
    };
    Some(match t {
        tag::DIRECTORY | tag::POINTER => CopyToBoth,
        tag::REFCOUNT | tag::ORPHAN => Recompute,
        tag::GARBAGE | tag::CURSOR | b'E' | b'C' => Drop,
        tag::HEAD | tag::USAGE_DEBT | tag::UNSEALED | tag::USAGE_DIRTY | b'D' => ByHash,
        tag::ROSTER | b'Z' => ByTrailingHash,
        tag::IDENTITY => WrittenFresh,
        b'N' => RecomputedInPreparation,
        b'T' => Absent,
        _ => return None,
    })
}

/// The entity hash a routed sentinel key names (`ByHash`: the sixteen bytes after the tag;
/// `ByTrailingHash`: the key's last sixteen); `None` for a key of another rule, or one too short.
pub(crate) fn routed_hash(key: &[u8]) -> Option<EntityHash> {
    let (t, rest) = key.strip_prefix(&SENTINEL)?.split_first()?;
    let hash: [u8; 16] = match split_rule(*t)? {
        SplitRule::ByHash => rest.get(..16)?.try_into().ok()?,
        SplitRule::ByTrailingHash => rest.get(rest.len().checked_sub(16)?..)?.try_into().ok()?,
        _ => return None,
    };
    Some(EntityHash::from_stored(hash))
}
