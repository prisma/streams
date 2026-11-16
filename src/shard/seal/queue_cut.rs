//! A native queue's seal (S3b-DESIGN §5.2). A queue stream seals as any
//! stream, with two rules of its own:
//! - its chunks hold at most [`QUEUE_CHUNK_BYTES`] of pages and never cross
//!   a range row's span ([`crate::native_queue::range::SPAN`] offsets), so a
//!   chunk's references are one range row's and a claim's handler reads one
//!   ranged GET per chunk (§6.2);
//! - every page admits at its own offset, or the queue is not sealed: the
//!   committer sealed each page itself, so one that does not admit is a
//!   fault, and a chunk must never record a page time it could not read
//!   (F5).
use crate::crypto_page::CheckedPage;
use crate::native_queue::range::index_of;
use crate::spill::pending::PendingPage;

/// The most page bytes a queue chunk holds (a page larger than it is a
/// chunk alone).
pub(crate) const QUEUE_CHUNK_BYTES: u64 = 1 << 20;

/// The runs a window's pages are cut into, one chunk each: at most
/// `max_pages` pages; for a queue, also at most [`QUEUE_CHUNK_BYTES`] and
/// inside one range row's span.
pub(super) fn runs(pages: &[PendingPage], queue: bool, max_pages: usize) -> Vec<&[PendingPage]> {
    if !queue {
        return pages.chunks(max_pages.max(1)).collect();
    }
    let mut out = Vec::new();
    let (mut start, mut bytes) = (0, 0u64);
    for (i, (first, _, raw)) in pages.iter().enumerate() {
        let len = raw.len() as u64;
        let crosses = index_of(*first) != index_of(pages[start].0);
        if i > start && (crosses || i - start >= max_pages || bytes + len > QUEUE_CHUNK_BYTES) {
            out.push(&pages[start..i]);
            (start, bytes) = (i, 0);
        }
        bytes += len;
    }
    if start < pages.len() {
        out.push(&pages[start..]);
    }
    out
}

/// Whether every page of a queue's window admits at its own offset.
pub(super) fn admits(pages: &[PendingPage]) -> bool {
    pages
        .iter()
        .all(|(first, last, raw)| first == last && CheckedPage::admit(raw.clone(), *first).is_ok())
}

#[cfg(test)]
mod tests {
    use super::{QUEUE_CHUNK_BYTES, admits, runs};
    use crate::native_queue::range::SPAN;

    fn page(offset: u64, len: usize) -> crate::spill::pending::PendingPage {
        (offset, offset, bytes::Bytes::from(vec![0u8; len]))
    }

    fn lens(cut: &[&[crate::spill::pending::PendingPage]]) -> Vec<(u64, usize)> {
        cut.iter().map(|run| (run[0].0, run.len())).collect()
    }

    #[test]
    fn a_queue_window_is_cut_at_its_range_rows_span_and_at_one_mebibyte() {
        // Four pages across a span boundary: cut there.
        let pages: Vec<_> = (SPAN - 2..SPAN + 2).map(|o| page(o, 10)).collect();
        assert_eq!(lens(&runs(&pages, true, 100)), [(SPAN - 2, 2), (SPAN, 2)]);
        // A stream's window is cut by pages alone.
        assert_eq!(lens(&runs(&pages, false, 100)), [(SPAN - 2, 4)]);
        // 300 KiB pages: three fit a mebibyte, the fourth starts a chunk.
        let big: Vec<_> = (0..5).map(|o| page(o, 300 << 10)).collect();
        assert_eq!(lens(&runs(&big, true, 100)), [(0, 3), (3, 2)]);
        // Exactly a mebibyte fits; a page past it alone is a chunk.
        let half = usize::try_from(QUEUE_CHUNK_BYTES / 2).unwrap();
        let exact: Vec<_> = (0..2).map(|o| page(o, half)).collect();
        assert_eq!(lens(&runs(&exact, true, 100)), [(0, 2)]);
        let huge = [page(0, 2 * half + 1), page(1, 1)];
        assert_eq!(lens(&runs(&huge, true, 100)), [(0, 1), (1, 1)]);
        // And by the page cap.
        assert_eq!(
            lens(&runs(&pages[..2], true, 1)),
            [(SPAN - 2, 1), (SPAN - 1, 1)]
        );
    }

    #[test]
    fn an_empty_queue_window_is_cut_into_no_run() {
        assert!(runs(&[], true, 100).is_empty(), "no empty chunk");
    }

    #[test]
    fn a_queue_window_with_a_page_that_does_not_admit_is_not_sealed() {
        assert!(admits(&[]));
        assert!(!admits(&[page(0, 10)]), "zeros are no page");
    }
}
