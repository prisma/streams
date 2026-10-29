//! A stream's pending pages (layout 6, DESIGN-6A b1).
//!
//! The pages a stream has committed above its sealed boundary are merge
//! operands on its pending row, `'S' ‖ hash ‖ 'P'`: each group appends its
//! pages of the stream, raw and back to back (each page's clear header
//! gives its own length, `crypto_page::page_extent`), and the merge
//! operator concatenates them oldest first. The stream's tail stays a plain
//! put. A seal's commit **puts** the pending row's remainder, every page at
//! or above the new boundary at the moment the committer stages it
//! (REVIEW-6A F1), which shadows every older operand: no page row and no
//! page tombstone exists.
//!
//! The merge is associative (concatenation) and is done in one pass over
//! every operand (`merge_batch`; a pairwise merge of n operands would copy
//! O(n²) bytes, REVIEW-6A F3). Only pending rows take operands: a merge on
//! any other key is refused.

use bytes::{Bytes, BytesMut};
use slatedb::{MergeOperator, MergeOperatorError};

use crate::crypto_page::page_extent;

/// The pending row's tag after the stream hash (no other stream row uses
/// it: `B C F G c l q s t x`).
pub(crate) const PENDING_TAG: u8 = b'P';

/// `'S' ‖ hash ‖ 'P'`: a stream's pending pages.
pub(crate) fn pending_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = crate::keyspace::stream_row(hash, 1);
    k.push(PENDING_TAG);
    k
}

/// Whether `key` is a pending row's.
fn is_pending(key: &[u8]) -> bool {
    key.len() == 18
        && key.first() == Some(&crate::keyspace::STREAM)
        && key.last() == Some(&PENDING_TAG)
}

/// Pages back to back: an operand, or a remainder.
pub(crate) fn concat_pages<'a>(pages: impl IntoIterator<Item = &'a Bytes>) -> Bytes {
    let pages: Vec<&Bytes> = pages.into_iter().collect();
    let mut out = BytesMut::with_capacity(pages.iter().map(|p| p.len()).sum());
    for p in pages {
        out.extend_from_slice(p);
    }
    out.freeze()
}

/// The pages held back to back in `raw`, each as (last offset, page
/// bytes), sliced without a copy. The caller admits each page against its
/// last offset (`CheckedPage::admit`) and checks that they tile.
pub(crate) fn parse_pages(raw: &Bytes) -> Result<Vec<(u64, Bytes)>, String> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(rest) = raw.get(at..).filter(|rest| !rest.is_empty()) {
        let (len, last) =
            page_extent(rest).map_err(|e| format!("pending page at byte {at}: {e:?}"))?;
        out.push((last, raw.slice(at..at + len)));
        at += len;
    }
    Ok(out)
}

/// The shard log's merge operator: pending rows concatenate.
pub(crate) struct PendingMerge;

impl MergeOperator for PendingMerge {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.merge_batch(key, existing_value, std::slice::from_ref(&value))
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        if !is_pending(key) {
            return Err(MergeOperatorError::Callback {
                message: format!("a merge on a key that takes no operand ({} B)", key.len()),
            });
        }
        if existing_value.is_none() && operands.is_empty() {
            return Err(MergeOperatorError::EmptyBatch);
        }
        Ok(concat_pages(existing_value.iter().chain(operands)))
    }
}

/// The merge operator every shard log is built with.
pub(crate) fn merge_operator() -> std::sync::Arc<dyn MergeOperator + Send + Sync> {
    std::sync::Arc::new(PendingMerge)
}

/// One unsealed page in memory: its first and last offsets and its stored
/// bytes (the ring's own allocation).
pub(crate) type PendingPage = (u64, u64, Bytes);

/// A stream's unsealed pages, oldest first: every page in `[absorbed,
/// next)` its handle has applied. They stay in memory until a seal takes
/// them (the handle is never evicted while it holds any), so a seal's
/// snapshot and remainder and a read of unsealed data never read the store;
/// the stored pending row is read only when the handle is loaded.
#[derive(Clone, Debug, Default)]
pub(crate) struct PendingPages {
    pages: std::collections::VecDeque<PendingPage>,
}

impl PendingPages {
    /// The pages of a stored pending value, which must tile `[from, next)`
    /// exactly (`from` is the stream's sealed boundary).
    pub(crate) fn tiled(raw: &Bytes, from: u64, next: u64) -> Result<Self, String> {
        let mut out = Self::default();
        let mut expected = from;
        for (last, page) in parse_pages(raw)? {
            let checked = crate::crypto_page::CheckedPage::admit(page.clone(), last)
                .map_err(|e| format!("pending page {last}: {e:?}"))?;
            if checked.first() != expected {
                return Err(format!(
                    "pending pages: offset {expected} due, page [{}, {last}]",
                    checked.first()
                ));
            }
            out.push((expected, last, page));
            expected = last.checked_add(1).ok_or("pending page past u64::MAX")?;
        }
        if expected != next {
            return Err(format!(
                "pending pages end at {expected}, the tail at {next}"
            ));
        }
        Ok(out)
    }

    pub(crate) fn push(&mut self, page: PendingPage) {
        self.pages.push_back(page);
    }

    /// Drop every page below `boundary` (a seal took them).
    pub(crate) fn seal_below(&mut self, boundary: u64) {
        while self
            .pages
            .front()
            .is_some_and(|(_, last, _)| *last < boundary)
        {
            self.pages.pop_front();
        }
    }

    /// The pages at or above `from`, oldest first.
    pub(crate) fn from(&self, from: u64) -> impl Iterator<Item = &PendingPage> {
        self.pages.iter().filter(move |(_, last, _)| *last >= from)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}
