#![cfg(test)]
//! A lineage body round-trips its entries in order, once each, and refuses
//! any body `encode` did not write.

use super::{
    Entry, decode, encode, reference_list_path, release_log_path, released_all_path,
    supplementary_list_path,
};
use crate::spill::gc::ObjectKind::{RunIndex, Segment};

#[test]
fn a_body_round_trips_its_entries_sorted_and_once_each() {
    let entries: Vec<Entry> = vec![
        (RunIndex, 2, 1),
        (Segment, 3, 9),
        (Segment, 3, 2),
        (Segment, 3, 9),
    ];
    let body = encode(&entries);
    assert_eq!(body.len(), 1 + 4 + 3 * 17 + 4);
    assert_eq!(body[..5], [1, 0, 0, 0, 3]);
    assert_eq!(
        body[5..22],
        [0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2]
    );
    assert_eq!(
        decode(&body),
        Some(vec![(Segment, 3, 2), (Segment, 3, 9), (RunIndex, 2, 1)])
    );
    assert_eq!(decode(&encode(&[])), Some(vec![]));
}

#[test]
fn a_body_encode_did_not_write_is_refused() {
    let body = encode(&[(Segment, 1, 1), (Segment, 1, 2)]);
    for i in 0..body.len() {
        let mut flipped = body.clone();
        flipped[i] ^= 0x01;
        assert_eq!(decode(&flipped), None, "byte {i} flipped");
    }
    assert_eq!(decode(&body[..body.len() - 1]), None);
    assert_eq!(decode(&[]), None);
    let resealed = |mut head: Vec<u8>| {
        let crc = crate::spill::crc32c::crc32c(&head);
        head.extend_from_slice(&crc.to_be_bytes());
        head
    };
    let head = body[..body.len() - 4].to_vec();
    let mut version = head.clone();
    version[0] = 2;
    assert_eq!(decode(&resealed(version)), None, "another version");
    let mut short = head.clone();
    short[4] = 3;
    assert_eq!(decode(&resealed(short)), None, "a count past the entries");
    let mut unsorted = head.clone();
    unsorted[5..22].copy_from_slice(&head[22..39]);
    assert_eq!(decode(&resealed(unsorted)), None, "entries out of order");
    let mut kind = head;
    kind[5] = 7;
    assert_eq!(decode(&resealed(kind)), None, "an unknown kind");
}

#[test]
fn a_holders_logs_and_marker_sit_under_the_ancestors_root() {
    assert_eq!(
        release_log_path("shards/01", 5, 3, 0),
        "shards/01/lineage/rel/5/00000000000000000003-00000000000000000000"
    );
    assert_eq!(
        released_all_path("shards/01", 5),
        "shards/01/lineage/rel/5/all"
    );
    assert_eq!(
        reference_list_path("shards/01", 5),
        "shards/01/lineage/refs/5"
    );
    assert_eq!(
        supplementary_list_path("shards/01", 5),
        "shards/01/lineage/refs/5-d"
    );
}
