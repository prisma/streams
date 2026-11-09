#![cfg(test)]
//! A child's refcount rows at a split (S3a-DESIGN §2.8): `'R'` is recomputed per object from the
//! entries the child keeps (its filtered `'K'` entries and its run-index entries), never copied, so
//! each child counts exactly the chunks it can still reclaim. S6's merge sums the same rows.

use std::collections::BTreeMap;

use super::Refcount;
use crate::spill::directory::DirEntry;
use crate::spill::object::ObjectId;

/// The `'R'` rows `entries` need: per object they name, its live chunks and its live bytes (the
/// encoded length each entry carries, the unit a decrement takes), with the creation time and the
/// object's length from the parent's row, or 0 where the parent had none. An object no entry names
/// has no row: zero is absent.
pub(crate) fn recompute<'a>(
    entries: impl IntoIterator<Item = &'a DirEntry>,
    parent: &BTreeMap<ObjectId, Refcount>,
) -> Vec<(ObjectId, Refcount)> {
    let mut out: BTreeMap<ObjectId, Refcount> = BTreeMap::new();
    for e in entries {
        let row = out.entry(e.object).or_insert_with(|| {
            let of = parent.get(&e.object).copied().unwrap_or_default();
            Refcount {
                created_ms: of.created_ms,
                object_len: of.object_len,
                ..Refcount::default()
            }
        });
        row.live_bytes += u64::from(e.chunk_len);
        row.live_chunks += 1;
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Refcount, recompute};
    use crate::spill::directory::{DirEntry, KeySummary};
    use crate::spill::object::ObjectId;

    fn entry(seq: u64, len: u32) -> DirEntry {
        DirEntry {
            object: ObjectId {
                home: 0,
                epoch: 1,
                seq,
            },
            chunk_off: 0,
            chunk_len: len,
            first: 0,
            last: 0,
            page_count: 1,
            max_ts_ms: 0,
            keys: KeySummary::Keys(Vec::new()),
            anchors: Vec::new(),
        }
    }

    /// A split's two children recompute rows that sum to the parent's for every object they share,
    /// keep the parent's creation time and length, and name no object neither keeps an entry of.
    #[test]
    fn a_splits_children_sum_to_the_parents_rows() {
        let row = |live_bytes, live_chunks| Refcount {
            live_bytes,
            live_chunks,
            created_ms: 7,
            object_len: 900,
        };
        let id = |seq| ObjectId {
            home: 0,
            epoch: 1,
            seq,
        };
        let parent = BTreeMap::from([(id(1), row(30, 3)), (id(2), row(40, 1)), (id(3), row(5, 1))]);
        let low = [entry(1, 10), entry(1, 5), entry(2, 40)];
        let high = [entry(1, 15)];
        assert_eq!(
            recompute(&low, &parent),
            [(id(1), row(15, 2)), (id(2), row(40, 1))]
        );
        assert_eq!(recompute(&high, &parent), [(id(1), row(15, 1))]);
        let unknown = [entry(9, 4)];
        let fresh = Refcount {
            live_bytes: 4,
            live_chunks: 1,
            ..Refcount::default()
        };
        assert_eq!(
            recompute(&unknown, &parent),
            [(id(9), fresh)],
            "no parent row: no time or length"
        );
        assert_eq!(recompute(&[], &parent), [], "zero is absent");
    }
}
