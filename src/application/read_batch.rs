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
impl std::fmt::Debug for PlainPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}
impl PartialEq for PlainPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
pub(crate) struct PlainRec {
    pub(crate) off: u64,
    pub(crate) payload: PlainPayload,
    pub(crate) rkey: String,
}
struct Block {
    first_record: usize,
    owner: Bytes,
    records: Vec<PlainRec>,
}
#[derive(Default)]
pub(crate) struct PlainBatch {
    blocks: Vec<Block>,
    count: usize,
}
pub(crate) struct Admission {
    pub(crate) withheld: Option<u64>,
    pub(crate) last: Option<u64>,
}
pub(crate) struct PlainIter<'a> {
    blocks: std::slice::Iter<'a, Block>,
    records: std::slice::Iter<'a, PlainRec>,
}
impl<'a> Iterator for PlainIter<'a> {
    type Item = &'a PlainRec;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(record) = self.records.next() {
                return Some(record);
            }
            self.records = self.blocks.next()?.records.iter();
        }
    }
}
impl<'a> IntoIterator for &'a PlainBatch {
    type Item = &'a PlainRec;
    type IntoIter = PlainIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl Index<usize> for PlainBatch {
    type Output = PlainRec;
    fn index(&self, index: usize) -> &PlainRec {
        assert!(index < self.count, "record index");
        let block = &self.blocks[self
            .blocks
            .partition_point(|block| block.first_record <= index)
            - 1];
        &block.records[index - block.first_record]
    }
}
impl PlainBatch {
    pub(crate) fn len(&self) -> usize {
        self.count
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub(crate) fn iter(&self) -> PlainIter<'_> {
        PlainIter {
            blocks: self.blocks.iter(),
            records: [].iter(),
        }
    }
    /// Exactly one complete owner can be shared with a binary response. The
    /// Bytes owner carries storage/charge until the last body or slice drops.
    pub(crate) fn contiguous(&self) -> Option<Bytes> {
        (self.blocks.len() == 1).then(|| self.blocks[0].owner.clone())
    }
    pub(crate) fn retained_capacity(&self) -> usize {
        self.blocks.iter().map(|block| block.owner.len()).sum()
    }
    /// The decoder has authenticated/admitted every range before publication.
    /// This is the only constructor for a shared plaintext block.
    pub(super) fn push_decoded(
        &mut self,
        bytes: Vec<u8>,
        ranges: Vec<(u64, Range<usize>, String)>,
    ) {
        if ranges.is_empty() {
            return;
        }
        let mut end = 0;
        for (_, range, _) in &ranges {
            assert_eq!(
                range.start, end,
                "admitted ranges must exactly cover their owner"
            );
            end = range.end;
        }
        assert_eq!(end, bytes.len());
        // Shrink a partial prefix or independently decoded fallback once, then
        // transfer ownership. A complete pre-sized batch retains its allocation.
        let exact = bytes.into_boxed_slice();
        #[cfg(test)]
        let owner = super::read_retention_probe::track(exact.into_vec());
        #[cfg(not(test))]
        let owner = Bytes::from_owner(exact);
        let first_record = self.count;
        self.count += ranges.len();
        self.blocks.push(Block {
            first_record,
            records: ranges
                .into_iter()
                .map(|(off, range, rkey)| PlainRec {
                    off,
                    payload: PlainPayload {
                        owner: owner.clone(),
                        range,
                    },
                    rkey,
                })
                .collect(),
            owner,
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
        self.push_decoded(bytes, vec![(off, 0..len, key)]);
        true
    }
    /// One selection/admission boundary for scans, fork ancestors and SSE
    /// lineage. A partial block is compacted before its old owner is released;
    /// no consumer can move individual shared records out of this abstraction.
    pub(crate) fn append_selected(
        &mut self,
        source: Self,
        range: Range<u64>,
        selector: Option<&str>,
        budget: &mut PageBudget,
        offset_shift: u64,
    ) -> Admission {
        let mut result = Admission {
            withheld: None,
            last: None,
        };
        for mut block in source.blocks {
            let mut planned = budget.clone();
            if block.records.iter().all(|record| {
                range.contains(&record.off)
                    && selector.is_none_or(|key| record.rkey == key)
                    && planned.admit(record.payload.len(), &record.rkey)
            }) {
                result.last = block.records.last().map(|record| record.off);
                for record in &mut block.records {
                    record.off = record
                        .off
                        .checked_add(offset_shift)
                        .expect("validated lineage offset");
                }
                *budget = planned;
                block.first_record = self.count;
                self.count += block.records.len();
                self.blocks.push(block);
                continue;
            }
            let full_count = block.records.len();
            let mut selected = Vec::new();
            for mut record in block.records {
                if !range.contains(&record.off) || selector.is_some_and(|key| record.rkey != key) {
                    continue;
                }
                if !budget.admit(record.payload.len(), &record.rkey) {
                    result.withheld = Some(record.off);
                    break;
                }
                result.last = Some(record.off);
                record.off = record
                    .off
                    .checked_add(offset_shift)
                    .expect("validated lineage offset");
                selected.push(record);
            }
            if selected.len() == full_count {
                let first_record = self.count;
                self.count += full_count;
                self.blocks.push(Block {
                    first_record,
                    owner: block.owner,
                    records: selected,
                });
            } else if !selected.is_empty() {
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
        for block in &mut source.blocks {
            block.first_record += self.count;
        }
        self.count += source.count;
        self.blocks.append(&mut source.blocks);
    }
}

#[cfg(test)]
#[path = "read_batch/tests.rs"]
mod tests;
