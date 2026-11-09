//! The sentinel helper (S4-DESIGN §7; S3a-DESIGN §2.8; shared with S6's merge): how a split builds
//! each child's sentinel rows from its sources' snapshot, one rule per tag
//! (`keyspace::split_rules`).
//!
//! - **Routed** ([`route`]): `'K'` and `'I'` go to every child as they are (every reader keeps only
//!   its range's entries, K-SPLIT-DECISION-2026-11-09); the rows keyed by an entity hash go to the
//!   child whose range holds it; `'G'`, `'L'`, `'E'` and `'C'` go to none.
//! - **Built for the child, not routed:** `'R'` and `'O'` are recomputed from the child's own
//!   entries, `'N'` from its queues' state rows, and `'Y'` is written fresh.
//! - **Refused:** a source holding `'T'` (a retired parent is never a source), a sentinel tag with
//!   no rule, a key that is no sentinel row, or a routed hash no child holds.

use crate::keyspace::SENTINEL;
use crate::keyspace::range::Range;
use crate::keyspace::split_rules::{SplitRule, routed_hash, split_rule};

/// One stored row: its key and its value.
pub(crate) type Row = (Vec<u8>, Vec<u8>);

/// Why the helper refused a source's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlanError {
    /// A key outside the sentinel range.
    NotSentinel(Vec<u8>),
    /// A sentinel tag with no split rule.
    UnknownTag(u8),
    /// A retired parent's `'T'`: it is never a source.
    Retired,
    /// A routed row whose hash no child's range holds, or whose key is too short to name one.
    Unrouted(Vec<u8>),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSentinel(key) => write!(f, "not a sentinel row: {}", crate::crypto::hex(key)),
            Self::UnknownTag(tag) => write!(f, "sentinel tag {tag:#04x} has no split rule"),
            Self::Retired => f.write_str("the source holds 'T': a retired parent"),
            Self::Unrouted(key) => write!(f, "no child holds {}", crate::crypto::hex(key)),
        }
    }
}

/// Each child's routed rows from one source's sentinel `rows`, in `children`' order and in the
/// rows' order (see the module).
pub(crate) fn route(rows: &[Row], children: &[Range]) -> Result<Vec<Vec<Row>>, PlanError> {
    let mut out = vec![Vec::new(); children.len()];
    for (key, value) in rows {
        let tag = key
            .strip_prefix(&SENTINEL)
            .and_then(|rest| rest.first().copied())
            .ok_or_else(|| PlanError::NotSentinel(key.clone()))?;
        match split_rule(tag).ok_or(PlanError::UnknownTag(tag))? {
            SplitRule::CopyToBoth => {
                for child in &mut out {
                    child.push((key.clone(), value.clone()));
                }
            }
            SplitRule::ByHash | SplitRule::ByTrailingHash => {
                let hash = routed_hash(key).ok_or_else(|| PlanError::Unrouted(key.clone()))?;
                let at = children
                    .iter()
                    .position(|range| range.contains(hash))
                    .ok_or_else(|| PlanError::Unrouted(key.clone()))?;
                out[at].push((key.clone(), value.clone()));
            }
            SplitRule::Absent => return Err(PlanError::Retired),
            SplitRule::Drop
            | SplitRule::Recompute
            | SplitRule::WrittenFresh
            | SplitRule::RecomputedInPreparation => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
