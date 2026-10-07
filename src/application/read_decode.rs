//! Authenticated page construction. Stored layout 5 pages, from the shard log
//! and from history alike, open whole into one buffer per read.
use super::read::ReadPage;
use super::read_batch::PlainBatch;
use super::read_budget::PageBudget;
use super::read_keys::ReadKeys;
use crate::crypto::Decrypted;
use crate::shard::record::{PageSlice, PageSlices};
use std::mem::take;
use std::ops::Range;

/// Stored rows a read decodes into its page: the page slices the shard log
/// and history return. Stored frames keep an implementation only for the
/// frame decode tests until the layout 5 cutover removes the stored frame
/// decoders.
pub(super) trait StoredRows {
    /// Decode the rows into `out` under `budget`; false when the budget
    /// ended the decode, with `out.last` on the last admitted record.
    fn decode_into(
        &self,
        keys: &mut ReadKeys<'_>,
        out: &mut ReadPage,
        budget: &mut PageBudget,
    ) -> Result<bool, String>;
}

/// Decode `frames` into `out` under `budget`. An error publishes nothing.
pub(super) fn decode_frames_into<R: StoredRows>(
    frames: &R,
    keys: &mut ReadKeys<'_>,
    out: &mut ReadPage,
    budget: &mut PageBudget,
) -> Result<bool, String> {
    frames.decode_into(keys, out, budget)
}

/// The shard log's pages: each page authenticates and parses whole before
/// any of its records is admitted, and only its slice's records are kept.
/// A budget that ends inside a page stops at the last admitted record, so
/// the next read resumes inside that page. Admitted records share one
/// buffer.
impl StoredRows for PageSlices {
    fn decode_into(
        &self,
        keys: &mut ReadKeys<'_>,
        out: &mut ReadPage,
        budget: &mut PageBudget,
    ) -> Result<bool, String> {
        let mut decoded = Decoded::default();
        for slice in self {
            if !decoded.slice(slice, keys, out, budget)? {
                decoded.publish(out, budget);
                return Ok(false);
            }
        }
        decoded.publish(out, budget);
        Ok(true)
    }
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

/// Stored frames: no read returns them since history reads moved to pages;
/// only the frame decode tests call this until the layout 5 cutover removes
/// it. Uncompressed runs share one planned buffer; fallback decoders transfer
/// independent buffers without an aggregate copy.
impl StoredRows for Vec<crate::shard::record::CheckedFrame> {
    #[expect(
        clippy::excessive_nesting,
        reason = "decode_frames_into for stored frames; the decode nests the first-record oversize verdict and the truncation of an admitted-then-refused append inside the per-frame loop; flattening them would separate the verdicts from the frame they refuse"
    )]
    fn decode_into(
        &self,
        keys: &mut ReadKeys<'_>,
        out: &mut ReadPage,
        budget: &mut PageBudget,
    ) -> Result<bool, String> {
        let mut planned = budget.clone();
        let mut capacity = 0;
        for raw in self {
            let frame = raw.view();
            if matches!(
                frame.ver,
                crate::crypto::FRAME_VER_Z | crate::crypto::LEGACY_FRAME_VER_Z
            ) {
                break;
            }
            let len = frame.ciphertext.len().saturating_sub(16);
            if !planned.admit(len, frame.header.routing_key) {
                break;
            }
            capacity += len;
        }
        let mut plaintext = Vec::with_capacity(capacity);
        let mut auth = Vec::new();
        let mut pending = Vec::new();
        let mut batch = PlainBatch::default();
        let result = (|| {
            for raw in self {
                let frame = raw.view();
                let offset = frame.header.offset;
                if !budget.metadata_fits(frame.header.routing_key) {
                    out.last = offset.checked_sub(1);
                    return Ok(false);
                }
                let Some(decoded) = keys.decrypt_append(
                    &frame,
                    raw,
                    budget.decode_limit(),
                    &mut plaintext,
                    &mut auth,
                )?
                else {
                    if out.recs.is_empty() && batch.is_empty() && pending.is_empty() {
                        return Err("decoded record exceeds 32 MiB".into());
                    }
                    out.last = offset.checked_sub(1);
                    return Ok(false);
                };
                let len = match &decoded {
                    Decrypted::Appended(range) => range.len(),
                    Decrypted::Owned(bytes) => bytes.len(),
                };
                if !budget.admit(len, frame.header.routing_key) {
                    if let Decrypted::Appended(range) = decoded {
                        plaintext.truncate(range.start);
                    }
                    out.last = offset.checked_sub(1);
                    return Ok(false);
                }
                match decoded {
                    Decrypted::Appended(range) => {
                        pending.push((offset, range, frame.header.routing_key.to_owned()))
                    }
                    Decrypted::Owned(bytes) => {
                        batch.push_decoded(take(&mut plaintext), take(&mut pending));
                        batch.push_decoded(
                            bytes,
                            std::iter::once((offset, 0..len, frame.header.routing_key.to_owned())),
                        );
                    }
                }
                out.last = Some(offset);
            }
            Ok(true)
        })();
        // No tentative plaintext escapes a failed authentication. A bounded partial
        // publishes exactly the previously admitted prefix, including mixed formats.
        if result.is_ok() {
            batch.push_decoded(plaintext, pending);
            out.recs.append_admitted(batch);
            debug_assert_eq!(out.recs.retained_capacity(), budget.admitted_bytes());
        }
        result
    }
}

#[cfg(test)]
#[path = "read_decode/tests.rs"]
mod tests;
