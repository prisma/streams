//! Item 35: a chunk whose postings pages fail their own round trip is
//! refused whole, before any of its rows or pages reach the shared batch.
use super::{StreamGatherFailure, check_postings};
use crate::postings::{BUCKET_OFFSETS, PageBuilder, rk_hash};

#[test]
fn pages_that_fail_their_self_decode_refuse_the_chunk() {
    let mut pages = PageBuilder::default();
    // A zero-byte frame yields a run with no matching bytes, which the page
    // decoder refuses: the encoder's own output fails its round trip.
    pages.note_frame(rk_hash(""), 0, 0);
    let refused = check_postings(pages).err();
    assert_eq!(refused, Some(StreamGatherFailure::PostingsSelfDecode));
}

#[test]
fn pages_that_overlap_refuse_the_chunk() {
    let mut pages = PageBuilder::default();
    // Out of order across a bucket seam: each page decodes, but the second
    // starts below the first one's end.
    pages.note_frame(rk_hash(""), BUCKET_OFFSETS + 1, 10);
    pages.note_frame(rk_hash(""), 0, 10);
    let refused = check_postings(pages).err();
    assert_eq!(refused, Some(StreamGatherFailure::PostingsOverlap));
}
