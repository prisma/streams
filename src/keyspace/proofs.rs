//! Kani proofs for the shard log's segment extractor: KANI-098. SlateDB
//! (RFC 0024) routes every key to its segment's compaction tree by the
//! prefix width the extractor names, and a prefix scan to the same tree by
//! the width it names for the scan's prefix, so they must agree: `segment_len`
//! names width 1 exactly for keys that start with `'S'` and width 5 exactly
//! for keys that start with `'Q'` and hold at least five bytes, never more
//! than the key; a prefix that has a segment names the same segment for
//! every key that extends it; and every row key the engine builds
//! (`stream_row`, `queue_row`) has its family's segment. Keys are up to
//! `KEY` symbolic bytes.
use super::{QUEUE_PREFIX_LEN, QUEUE_TAG, STREAM, queue_row, segment_len, stream_row};

const KEY: usize = 8;

/// KANI-098: the width the extractor names, and a prefix's agreement with
/// every key that extends it.
#[kani::proof]
#[kani::unwind(10)]
fn kani_098_a_key_and_every_prefix_of_it_name_one_segment() {
    let bytes: [u8; KEY] = kani::any();
    let len = kani::any_where(|len: &usize| *len <= KEY);
    let key = &bytes[..len];
    let width = segment_len(key);
    let expected = match key.first() {
        Some(&STREAM) => Some(1),
        Some(&QUEUE_TAG) if len >= QUEUE_PREFIX_LEN => Some(QUEUE_PREFIX_LEN),
        _ => None,
    };
    assert!(width == expected, "a key's segment is its tag's");
    assert!(
        width.is_none_or(|w| w <= len),
        "a segment is never wider than its key"
    );
    let cut = kani::any_where(|cut: &usize| *cut <= len);
    if let Some(w) = segment_len(&key[..cut])
        && w <= cut
    {
        assert!(
            width == Some(w),
            "a prefix names the segment of every key that extends it"
        );
    }
    kani::cover!(width == Some(QUEUE_PREFIX_LEN), "a queue key");
    kani::cover!(
        width.is_none() && key.first() == Some(&QUEUE_TAG),
        "a short queue prefix"
    );
}

/// KANI-098: every row key the engine builds has its family's segment.
#[kani::proof]
#[kani::unwind(40)]
fn kani_098_every_row_key_has_its_family_segment() {
    let hash: [u8; 16] = kani::any();
    assert!(
        segment_len(&stream_row(&hash, 0)) == Some(1),
        "a stream row is in the stream segment"
    );
    assert!(
        segment_len(&queue_row(&hash, 0)) == Some(QUEUE_PREFIX_LEN),
        "a queue row is in the queue segment"
    );
}
