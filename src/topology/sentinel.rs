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

use std::collections::BTreeMap;

use crate::keyspace::range::Range;
use crate::keyspace::split_rules::{SplitRule, routed_hash, split_rule};
use crate::keyspace::{EntityHash, SENTINEL, tag};
use crate::spill::directory::{self, DirEntry};
use crate::spill::gc::{self, Refcount};
use crate::spill::object::ObjectId;
use crate::spill::runindex::{self, Orphans};

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
    /// A `'K'`, `'R'` or `'O'` row that does not decode.
    Corrupt(Vec<u8>),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSentinel(key) => write!(f, "not a sentinel row: {}", crate::crypto::hex(key)),
            Self::UnknownTag(tag) => write!(f, "sentinel tag {tag:#04x} has no split rule"),
            Self::Retired => f.write_str("the source holds 'T': a retired parent"),
            Self::Unrouted(key) => write!(f, "no child holds {}", crate::crypto::hex(key)),
            Self::Corrupt(key) => write!(f, "undecodable row {}", crate::crypto::hex(key)),
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

/// A child's recomputed counts (S3a-DESIGN §2.8): its `'R'` rows, and its `'O'` rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    /// Per object the child's entries name: its live chunks and bytes among them, with the source's
    /// creation time and length. Zero is absent.
    pub refcounts: Vec<(ObjectId, Refcount)>,
    /// The orphans (chunks a consolidation or a cleaner's copy superseded) of the objects whose
    /// first entry the child's range holds: an orphan count names no stream, so exactly one child
    /// takes each row, whole.
    pub orphans: Vec<(ObjectId, Orphans)>,
}

/// The counts of the child of `range`, from a source's sentinel `rows` (its `'K'`, `'R'` and `'O'`)
/// and the `(hash, entry)` pairs of the source's run-index objects (the footers its `'I'` rows
/// name): the entries whose entity hash the range holds, each `'K'` row decoded whole so every kept
/// chunk keeps the offset the row gives it (K-SPLIT-DECISION-2026-11-09).
pub(crate) fn child_counts(
    rows: &[Row],
    run_entries: &[([u8; 16], DirEntry)],
    range: Range,
) -> Result<Counts, PlanError> {
    let mut parent: BTreeMap<ObjectId, Refcount> = BTreeMap::new();
    let mut orphans: BTreeMap<ObjectId, Orphans> = BTreeMap::new();
    let mut entries: Vec<DirEntry> = Vec::new();
    let mut first: BTreeMap<ObjectId, [u8; 16]> = BTreeMap::new();
    for (key, value) in rows {
        let corrupt = || PlanError::Corrupt(key.clone());
        match key.get(SENTINEL.len()).copied() {
            Some(tag::DIRECTORY) => {
                let id = directory::parse_object_key(tag::DIRECTORY, key).ok_or_else(corrupt)?;
                let whole = directory::decode_row(id, value).ok_or_else(corrupt)?;
                if let Some((hash, _)) = whole.first() {
                    first.entry(id).or_insert(*hash);
                }
                let kept = whole
                    .into_iter()
                    .filter(|(hash, _)| range.contains(EntityHash::from_stored(*hash)));
                entries.extend(kept.map(|(_, entry)| entry));
            }
            Some(tag::REFCOUNT) => {
                let id = directory::parse_object_key(tag::REFCOUNT, key).ok_or_else(corrupt)?;
                parent.insert(id, gc::decode_refcount(value).ok_or_else(corrupt)?);
            }
            Some(tag::ORPHAN) => {
                let id = runindex::parse_sentinel_key(tag::ORPHAN, key).ok_or_else(corrupt)?;
                orphans.insert(id, runindex::decode_orphans(value).ok_or_else(corrupt)?);
            }
            _ => {}
        }
    }
    for (hash, entry) in run_entries {
        first.entry(entry.object).or_insert(*hash);
    }
    let in_range = run_entries
        .iter()
        .filter(|(hash, _)| range.contains(EntityHash::from_stored(*hash)))
        .map(|(_, entry)| entry);
    entries.extend(in_range.cloned());
    let refcounts = gc::recompute::recompute(&entries, &parent);
    let takes = |id: &ObjectId| {
        first
            .get(id)
            .is_some_and(|hash| range.contains(EntityHash::from_stored(*hash)))
    };
    let orphans = orphans.into_iter().filter(|(id, _)| takes(id)).collect();
    Ok(Counts { refcounts, orphans })
}

#[cfg(test)]
mod tests;
