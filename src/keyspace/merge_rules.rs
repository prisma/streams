//! What a merge does with each of its two sources' sentinel rows (S6-DESIGN rev 6 §7.3, S3a §2.8's
//! merge column): one rule per tag, the table as code beside `split_rules`, so a new tag cannot
//! exist without one (`tests::every_sentinel_tag_has_a_merge_rule`). The merged leaf's rows are
//! built by `spill::gc::merge::merged_sentinel` from these rules.

use super::tag;

/// A tag's rule at a merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeRule {
    /// Both sources' rows, once each. A key both hold must have equal values (asserted). For
    /// `'K'` and `'I'` both children of one parent carry its copied rows
    /// (K-SPLIT-DECISION-2026-11-09); for the routed tags an entity lives in one source only, so a
    /// key in both is a corruption unless its values are equal.
    Union,
    /// Per object, the sources' counts summed; a zero sum is absent (`'R'`, `'O'`; L2).
    Sum,
    /// Neither source's row is carried (`'G'`: the sources' own objects become ancestor objects
    /// the merged leaf does not hold; `'L'`, `'C'`: cursors; `'E'`: a source's pending release is
    /// not in the merged leaf's holdings).
    Drop,
    /// Written fresh for the merged leaf (`'Y'`: its layout and its own home).
    WrittenFresh,
    /// Recomputed from the merged leaf's queues' state rows (`'N'`, S3b's gauges).
    Recomputed,
    /// Not in the merged leaf at all (`'T'`, written into the retired sources).
    Absent,
}

/// The rule of sentinel tag `t`; `None` for a byte that is no sentinel tag.
pub(crate) fn merge_rule(t: u8) -> Option<MergeRule> {
    use MergeRule::{Absent, Drop, Recomputed, Sum, Union, WrittenFresh};
    Some(match t {
        // The two indexes of the parent's objects, then the entity-routed rows.
        tag::DIRECTORY
        | tag::POINTER
        | tag::HEAD
        | tag::USAGE_DEBT
        | tag::UNSEALED
        | tag::USAGE_DIRTY
        | tag::ROSTER
        | b'D'
        | b'Z'
        // A source that owes an index repair passes it on: M's 'W' and 'M' rows are that
        // source's unioned, so M's open recomputes them from every tail.
        | tag::REPAIR => Union,
        tag::REFCOUNT | tag::ORPHAN => Sum,
        tag::GARBAGE | tag::CURSOR | tag::RELEASE | b'C' => Drop,
        tag::IDENTITY => WrittenFresh,
        b'N' => Recomputed,
        b'T' => Absent,
        _ => return None,
    })
}
