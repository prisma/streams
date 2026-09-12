#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
//! Admitted plaintext ownership. A record is a range, not an independently
//! transferable byte owner. Selection compacts incomplete blocks; complete
//! blocks move unchanged. Every published owner has capacity exactly equal to
//! its admitted bytes, so page admission charges its full backing allocation.
use super::read_budget::PageBudget;
use bytes::Bytes;
use std::ops::{Deref, Index, Range};

pub(crate) struct PlainPayload {
    owner: Bytes,
    range: Range<usize>,
}
impl AsRef<[u8]> for PlainPayload {
    #[expect(
        clippy::indexing_slicing,
        reason = "PlainPayload::as_ref; the range was checked to cover its owner when the record was admitted; a fallible slice would return an empty payload for a record admission proved present"
    )]
    fn as_ref(&self) -> &[u8] {
        &self.owner[self.range.clone()]
    }
}
impl Deref for PlainPayload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}
pub(crate) struct PlainRec {
    pub(crate) off: u64,
    pub(crate) payload: PlainPayload,
    pub(crate) rkey: String,
}
struct Block {
    owner: Bytes,
    records: Range<usize>,
}
#[derive(Default)]
pub(crate) struct PlainBatch {
    blocks: Vec<Block>,
    records: Vec<PlainRec>,
}
pub(crate) struct Admission {
    pub(crate) withheld: Option<u64>,
    pub(crate) last: Option<u64>,
}
impl<'a> IntoIterator for &'a PlainBatch {
    type Item = &'a PlainRec;
    type IntoIter = std::slice::Iter<'a, PlainRec>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl Index<usize> for PlainBatch {
    type Output = PlainRec;
    #[expect(
        clippy::indexing_slicing,
        reason = "PlainBatch::index; Index panics on an out-of-range index by the trait's contract; a fallible lookup would change what indexing means for every caller"
    )]
    fn index(&self, index: usize) -> &PlainRec {
        &self.records[index]
    }
}
impl PlainBatch {
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, PlainRec> {
        self.records.iter()
    }
    /// Only a complete single owner may escape to a binary response. Its
    /// storage and charge survive until the last body or byte owner drops.
    pub(crate) fn contiguous(&self) -> Option<Bytes> {
        match self.blocks.as_slice() {
            [block] => Some(block.owner.clone()),
            _ => None,
        }
    }
    pub(crate) fn retained_capacity(&self) -> usize {
        self.blocks.iter().map(|block| block.owner.len()).sum()
    }
    /// One constructor for authenticated/admitted ranges. The iterator also
    /// accepts one independently decoded record without a temporary vector.
    pub(super) fn push_decoded(
        &mut self,
        bytes: Vec<u8>,
        ranges: impl IntoIterator<Item = (u64, Range<usize>, String)>,
    ) {
        let mut ranges = ranges.into_iter().peekable();
        if ranges.peek().is_none() {
            return;
        }
        let exact = bytes.into_boxed_slice();
        #[cfg(test)]
        let charge = super::read_retention_probe::charge(exact.len());
        #[cfg(not(test))]
        let charge = ();
        let owner = crate::retained_bytes::with_charge(exact, charge);
        let first = self.records.len();
        self.records.reserve(ranges.size_hint().0);
        let mut end = 0;
        for (off, range, rkey) in ranges {
            assert_eq!(
                range.start, end,
                "admitted ranges must exactly cover their owner"
            );
            end = range.end;
            self.records.push(PlainRec {
                off,
                payload: PlainPayload {
                    owner: owner.clone(),
                    range,
                },
                rkey,
            });
        }
        assert_eq!(end, owner.len());
        self.blocks.push(Block {
            owner,
            records: first..self.records.len(),
        });
    }
    pub(crate) fn admit_owned(
        &mut self,
        off: u64,
        bytes: Vec<u8>,
        key: String,
        budget: &mut PageBudget,
    ) -> bool {
        if !budget.admit(bytes.len(), &key) {
            return false;
        }
        let len = bytes.len();
        self.push_decoded(bytes, std::iter::once((off, 0..len, key)));
        true
    }
    /// Scans, forks and SSE use this one selection/admission boundary.
    /// Complete owners transfer unchanged; partial owners are compacted.
    #[expect(
        clippy::too_many_arguments,
        reason = "PlainBatch::append_selected; the selection takes the source batch, the offset range, the selector, the budget and the lineage shift separately as the read resolved them; a request struct would exist for this single boundary"
    )]
    #[expect(
        clippy::expect_used,
        reason = "PlainBatch::append_selected; a lineage offset shift was validated against the fork's parent range before selection, so the remapped offset fits; a fallible remap would add an error path no validated lineage reaches"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "PlainBatch::append_selected; the selection nests each record's eligibility and budget verdicts inside the per-block walk; flattening them would separate the verdicts from the block they cut"
    )]
    pub(crate) fn append_selected(
        &mut self,
        mut source: Self,
        range: Range<u64>,
        selector: Option<&str>,
        budget: &mut PageBudget,
        offset_shift: u64,
    ) -> Admission {
        let mut result = Admission {
            withheld: None,
            last: None,
        };
        let eligible = |record: &PlainRec| {
            range.contains(&record.off) && selector.is_none_or(|key| record.rkey == key)
        };
        let remap = |record: &mut PlainRec| {
            record.off = record
                .off
                .checked_add(offset_shift)
                .expect("validated lineage offset");
        };
        // The usual bounded physical page transfers its entire metadata vectors
        // as well as its storage owners. No per-record allocation or copy.
        let mut planned = budget.clone();
        if source
            .records
            .iter()
            .all(|record| eligible(record) && planned.admit(record.payload.len(), &record.rkey))
        {
            result.last = source.records.last().map(|record| record.off);
            source.records.iter_mut().for_each(remap);
            *budget = planned;
            self.append_admitted(source);
            return result;
        }
        let mut records = source.records.into_iter();
        for mut block in source.blocks {
            let count = block.records.len();
            let mut planned = budget.clone();
            if let Some(head) = records.as_slice().get(..count)
                && head.iter().all(|record| {
                    eligible(record) && planned.admit(record.payload.len(), &record.rkey)
                })
            {
                result.last = head.last().map(|record| record.off);
                let first = self.records.len();
                self.records
                    .extend(records.by_ref().take(count).map(|mut record| {
                        remap(&mut record);
                        record
                    }));
                block.records = first..self.records.len();
                self.blocks.push(block);
                *budget = planned;
                continue;
            }
            let mut selected = Vec::new();
            for mut record in records.by_ref().take(count) {
                if !eligible(&record) {
                    continue;
                }
                if !budget.admit(record.payload.len(), &record.rkey) {
                    result.withheld = Some(record.off);
                    break;
                }
                result.last = Some(record.off);
                remap(&mut record);
                selected.push(record);
            }
            if !selected.is_empty() {
                let len = selected.iter().map(|record| record.payload.len()).sum();
                let mut bytes = Vec::with_capacity(len);
                let mut ranges = Vec::with_capacity(selected.len());
                for record in selected {
                    let start = bytes.len();
                    bytes.extend_from_slice(&record.payload);
                    ranges.push((record.off, start..bytes.len(), record.rkey));
                }
                self.push_decoded(bytes, ranges);
            }
            if result.withheld.is_some() {
                break;
            }
        }
        result
    }
    pub(super) fn append_admitted(&mut self, mut source: Self) {
        if self.is_empty() {
            *self = source;
            return;
        }
        let first = self.records.len();
        for block in &mut source.blocks {
            block.records.start = block.records.start.saturating_add(first);
            block.records.end = block.records.end.saturating_add(first);
        }
        self.records.append(&mut source.records);
        self.blocks.append(&mut source.blocks);
    }
}

#[cfg(test)]
#[path = "read_batch/tests.rs"]
mod tests;
