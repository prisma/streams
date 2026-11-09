//! A shard's reads of `'K'` rows across a split (S3a-DESIGN §2.8, K-SPLIT-DECISION-2026-11-09): a
//! split copies the parent's `'K'` rows to both children byte for byte (`'I'`'s rule), so a row may
//! name chunks of streams outside the reading shard's range. A row stores no chunk offsets (each
//! chunk starts where the previous one ended), so no row is ever re-encoded with entries dropped:
//! every reader decodes the whole row and keeps only its range's entries.

use super::{DirEntry, decode_row};
use crate::keyspace::EntityHash;
use crate::keyspace::range::Range;
use crate::spill::object::ObjectId;

/// The `(hash, entry)` pairs of `object`'s `'K'` row whose entity hash `range` holds, each with
/// the offset the whole row gives it; `None` for bytes `decode_row` refuses.
pub(crate) fn decode_row_in_range(
    object: ObjectId,
    raw: &[u8],
    range: Range,
) -> Option<Vec<([u8; 16], DirEntry)>> {
    let mut entries = decode_row(object, raw)?;
    entries.retain(|(hash, _)| range.contains(EntityHash::from_stored(*hash)));
    Some(entries)
}
