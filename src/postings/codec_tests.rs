//! Wire-format and allocation-admission regressions for the production decoder.
use super::{
    AbsRun, PAGE_MAX_ENCODED_BYTES, PostingRun, RouteHash, RoutingKeyHash, SegmentHash,
    admitted_run_count, decode_page, decode_stored_page, encode_page, get_varint, postings_key,
};

#[test]
fn varint_rejects_truncation_overflow_and_an_eleventh_byte() {
    for invalid in [
        vec![],
        vec![0x80],
        vec![0x80; 10],
        vec![0xff; 10],
        vec![0x80; 11],
    ] {
        assert_eq!(get_varint(&mut invalid.as_slice()), None);
    }
    let mut high_bit = vec![0x80; 9];
    high_bit.push(2);
    assert_eq!(get_varint(&mut high_bit.as_slice()), None);
    let mut input = [0x80, 0x01, 7].as_slice();
    assert_eq!(get_varint(&mut input), Some(128));
    assert_eq!(input, &[7], "consume exactly one number");
    // Existing format accepts redundant high zero groups.
    assert_eq!(get_varint(&mut [0x80, 0].as_slice()), Some(0));
}

#[test]
fn allocation_count_is_bounded_before_reserving_runs() {
    for encoded_bytes in 0..=16 {
        for count in [0, 1, 2, 3, 4, 5, u32::MAX] {
            let expected = match (count, encoded_bytes) {
                (1, 4..=16) => Some(1),
                (2, 8..=16) => Some(2),
                (3, 12..=16) => Some(3),
                (4, 16) => Some(4),
                _ => None,
            };
            assert_eq!(
                admitted_run_count(&count.to_le_bytes(), encoded_bytes),
                expected
            );
        }
    }
}

#[test]
fn page_version_codec_and_exact_wire_limit_are_enforced() {
    assert_eq!(PAGE_MAX_ENCODED_BYTES, 32_768, "persisted format limit");
    let runs = vec![
        PostingRun {
            gap_offsets: 0,
            record_count: 1,
            matching_frame_bytes: 1,
            gap_frame_bytes_before: 0,
        };
        8_184
    ];
    let encoded = encode_page(0, &runs);
    assert_eq!(encoded.len(), 32_766);
    for (prefix, accepted) in [(vec![0x80; 2], true), (vec![0x80; 3], false)] {
        let mut page = encoded.clone();
        let body = page.split_off(30);
        // Redundant zero groups keep the first gap valid while testing the byte cap.
        page.extend_from_slice(&prefix);
        page.extend_from_slice(&body);
        assert_eq!(decode_page(&page).is_some(), accepted);
    }
    for (field, value) in [(0, 0), (0, 2), (1, 1)] {
        let mut invalid = encoded.clone();
        *invalid.get_mut(field).unwrap() = value;
        assert!(decode_page(&invalid).is_none());
    }
}

#[test]
fn stored_page_requires_every_identity_field_and_exact_key_shape() {
    let route = RouteHash([1; 16]);
    let inc = SegmentHash([2; 16]);
    let hash = RoutingKeyHash([3; 16]);
    let key = postings_key(route, inc, &hash, 0, 7);
    let value = encode_page(
        7,
        &[PostingRun {
            gap_offsets: 0,
            record_count: 1,
            matching_frame_bytes: 10,
            gap_frame_bytes_before: 0,
        }],
    );
    let expected = vec![AbsRun {
        start: 7,
        count: 1,
        matching_bytes: 10,
        gap_bytes_before: 0,
    }];
    assert_eq!(
        decode_stored_page(route, inc, &hash, &key, &value),
        Some(expected)
    );
    for field in [0, 16, 32, 33, 49, 57] {
        let mut invalid = key.clone();
        *invalid.get_mut(field).unwrap() ^= 1;
        assert!(decode_stored_page(route, inc, &hash, &invalid, &value).is_none());
    }
    for length in 0..key.len() {
        assert!(
            decode_stored_page(route, inc, &hash, key.get(..length).unwrap(), &value).is_none()
        );
    }
    let mut trailing = key;
    trailing.push(0);
    assert!(decode_stored_page(route, inc, &hash, &trailing, &value).is_none());
}
