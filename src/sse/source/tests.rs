use super::*;

/// The linearization rule maps linearized offsets back to
/// (seg_id, segment-local), with sealed-cap boundaries owned by
/// the NEXT span.
#[test]
fn linearized_cursor_space_roundtrips() {
    let spans = [(0u32, 0u64, Some(5)), (1, 5, Some(3)), (2, 8, None)];
    // Inside the first span.
    let wp = |seg, local| WirePosition {
        seg_id: seg,
        local_after: local,
    };
    assert_eq!(locate_in_spans(&spans, 0), wp(0, 0));
    assert_eq!(locate_in_spans(&spans, 4), wp(0, 4));
    // The cap boundary belongs to the next span at local 0.
    assert_eq!(locate_in_spans(&spans, 5), wp(1, 0));
    assert_eq!(locate_in_spans(&spans, 7), wp(1, 2));
    assert_eq!(locate_in_spans(&spans, 8), wp(2, 0));
    // The live tail is open-ended.
    assert_eq!(locate_in_spans(&spans, 100), wp(2, 92));
}

#[test]
fn sig_compatibility_rules() {
    // A live span may gain its sealed cap; spans may be appended.
    let old = [(0u32, 0u64, None)];
    let new = [(0u32, 0u64, Some(5)), (1, 5, None)];
    assert!(sig_compatible(&old, &new));
    // A different segment id is never a continuation.
    let bad_seg = [(1u32, 0u64, Some(5)), (2, 5, None)];
    assert!(!sig_compatible(&old, &bad_seg));
    // A span may not vanish.
    let old2 = [(0u32, 0u64, Some(5)), (1, 5, None)];
    let shrunk = [(0u32, 0u64, Some(5))];
    assert!(!sig_compatible(&old2, &shrunk));
    // A sealed cap may not change.
    let changed = [(0u32, 0u64, Some(6)), (1, 6, None)];
    assert!(!sig_compatible(&old2, &changed));
    // Identity.
    assert!(sig_compatible(&old2, &old2));
}
