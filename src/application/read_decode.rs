//! Authenticated page construction. Stored layout 5 pages, from the shard log
//! and from history alike, open whole into one buffer per read.
use super::read::ReadPage;
use super::read_batch::PlainBatch;
use super::read_budget::PageBudget;
use super::read_keys::ReadKeys;
use crate::shard::record::{PageSlice, PageSlices};
use std::ops::Range;

/// Decode the page slices a read returned into `out` under `budget`; false
/// when the budget ended the decode, with `out.last` on the last admitted
/// record. Each page authenticates and parses whole before any of its
/// records is admitted, and only its slice's records are kept: records
/// below the slice (below the read's cursor) are skipped. A budget that
/// ends inside a page stops at the last admitted record, so the next read
/// resumes inside that page. Admitted records share one buffer, published
/// once; an error publishes nothing.
pub(super) fn decode_frames_into(
    frames: &PageSlices,
    keys: &mut ReadKeys<'_>,
    out: &mut ReadPage,
    budget: &mut PageBudget,
) -> Result<bool, String> {
    let mut decoded = Decoded::default();
    for slice in frames {
        if !decoded.slice(slice, keys, out, budget)? {
            decoded.publish(out, budget);
            return Ok(false);
        }
    }
    decoded.publish(out, budget);
    Ok(true)
}

/// The records a page decode has admitted so far, not yet published.
#[derive(Default)]
struct Decoded {
    plaintext: Vec<u8>,
    pending: Vec<(u64, Range<usize>, String)>,
}

impl Decoded {
    /// Admit `slice`'s records; false once the budget refuses one, with
    /// `out.last` on the record before it.
    fn slice(
        &mut self,
        slice: &PageSlice,
        keys: &mut ReadKeys<'_>,
        out: &mut ReadPage,
        budget: &mut PageBudget,
    ) -> Result<bool, String> {
        let page = slice.page();
        let key = page.routing_key();
        if budget.full() || !budget.metadata_fits(key) {
            out.last = slice.first().checked_sub(1);
            return Ok(false);
        }
        let opened = keys.open_page(page)?;
        let records = opened
            .records()
            .filter(|record| record.offset >= slice.first());
        for record in records.take_while(|record| record.offset <= slice.last()) {
            if !budget.admit(record.payload.len(), key) {
                out.last = record.offset.checked_sub(1);
                return Ok(false);
            }
            let start = self.plaintext.len();
            self.plaintext.extend_from_slice(record.payload);
            let range = start..self.plaintext.len();
            self.pending.push((record.offset, range, key.to_owned()));
            out.last = Some(record.offset);
        }
        Ok(true)
    }

    fn publish(self, out: &mut ReadPage, budget: &PageBudget) {
        let mut batch = PlainBatch::default();
        batch.push_decoded(self.plaintext, self.pending);
        out.recs.append_admitted(batch);
        debug_assert_eq!(out.recs.retained_capacity(), budget.admitted_bytes());
    }
}

#[cfg(test)]
#[path = "read_decode/tests.rs"]
mod tests;

#[cfg(test)]
mod paging_tests;
