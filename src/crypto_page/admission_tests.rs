//! Admission refuses every malformed or misplaced row from its bytes and
//! row key alone, before any key is used.
#![cfg(test)]

use bytes::Bytes;

use super::header::{HeaderFields, encode_header};
use super::tests::{FIRST, HASH, NONCE, TS, cipher, flip, lane, prefix, reference};
use super::{
    CheckedPage, PAGE_MAX_RECORDS, PAGE_TARGET_PLAINTEXT, PageCorruption, PageLane, body_cap,
    history_page_key, history_page_prefix, shard_page_key,
};
use crate::crypto::{RouteHash, SegmentHash};

fn admit_at(raw: &[u8], last: u64) -> Result<CheckedPage, PageCorruption> {
    let key = shard_page_key(&HASH, last);
    CheckedPage::from_row(&key, &prefix(), Bytes::copy_from_slice(raw))
}

/// A page whose clear header says `ver`, `first` and `count` and whose
/// ciphertext is `sealed` zero bytes; nothing in it authenticates.
fn forged(ver: u8, first: u64, count: usize, sealed: usize) -> Vec<u8> {
    let lane = lane();
    let fields = HeaderFields {
        ver,
        first,
        count,
        lane: &lane,
        nonce: NONCE,
    };
    let mut page = encode_header(&fields).unwrap();
    page.extend_from_slice(&u32::try_from(sealed).unwrap().to_be_bytes());
    page.extend(std::iter::repeat_n(0, sealed));
    page
}

/// `raw` with its bytes `range` replaced by `with`.
fn spliced(raw: &[u8], range: std::ops::Range<usize>, with: &[u8]) -> Vec<u8> {
    let mut out = raw.to_vec();
    out.splice(range, with.iter().copied());
    out
}

#[test]
fn an_admitted_page_keeps_its_exact_clear_fields_and_bytes() {
    let raw = reference();
    assert_eq!(raw.len(), 78);
    assert_eq!(raw.get(9), Some(&3), "count");
    assert_eq!(raw.get(22..26), Some(&b"\0\x02rk"[..]), "routing key");
    assert_eq!(raw.get(26..38), Some(&NONCE[..]));
    assert_eq!(raw.get(38..42), Some(&36u32.to_be_bytes()[..]), "ct_len");
    let page = admit_at(&raw, 42).unwrap();
    assert_eq!((page.first(), page.last(), page.count()), (FIRST, 42, 3));
    assert_eq!(
        (page.ts_ms(), page.key_version(), page.routing_key()),
        (TS, 1, "rk")
    );
    assert!(!page.is_compressed());
    assert_eq!(page.raw(), &raw);
    let ring = CheckedPage::admit(Bytes::from(raw), 42).unwrap();
    assert_eq!(ring, page, "the ring admits what the row admits");
}

#[test]
fn a_row_key_outside_the_page_namespace_is_refused() {
    let raw = reference();
    let key = shard_page_key(&HASH, 42);
    let bytes = || Bytes::from(raw.clone());
    let record_prefix = [&HASH[..], b"r"].concat();
    let refused = |key: &[u8], prefix: &[u8]| CheckedPage::from_row(key, prefix, bytes());
    assert_eq!(refused(&key, &record_prefix), Err(PageCorruption::RowTag));
    let (_, short) = key.split_last().unwrap();
    assert_eq!(refused(short, &prefix()), Err(PageCorruption::KeyWidth));
    let long = [key.as_slice(), &[0]].concat();
    assert_eq!(refused(&long, &prefix()), Err(PageCorruption::KeyWidth));
    assert_eq!(refused(b"tiny", &prefix()), Err(PageCorruption::KeyWidth));
    let other = shard_page_key(&[0x45; 16], 42);
    assert_eq!(refused(&other, &prefix()), Err(PageCorruption::Namespace));
    assert!(refused(&key, &prefix()).is_ok());
}

/// Each keyspace admits its own page tag only: the shard log's `'p'`, and
/// history's `'g'`, whose keyspace already spends `'p'` on postings pages.
/// A prefix of the other keyspace's tag, or of any other width, is refused
/// before its row is looked at, so neither keyspace can read the other's
/// rows as pages.
#[test]
fn each_keyspace_admits_only_its_own_page_tag() {
    let raw = reference();
    let refused =
        |key: &[u8], prefix: &[u8]| CheckedPage::from_row(key, prefix, Bytes::from(raw.clone()));
    let (route, inc) = (RouteHash([0x0a; 16]), SegmentHash([0x0b; 16]));
    let history = history_page_key(route, inc, 42);
    let history_prefix = history_page_prefix(route, inc);
    assert_eq!(history_prefix.last(), Some(&b'g'));
    assert!(refused(&history, &history_prefix).is_ok());
    let postings_prefix = [&history_prefix[..32], b"p"].concat();
    let postings_key = [postings_prefix.as_slice(), &42u64.to_be_bytes()].concat();
    assert_eq!(
        refused(&postings_key, &postings_prefix),
        Err(PageCorruption::RowTag),
        "history's 'p' rows are postings"
    );
    assert_eq!(
        refused(&postings_key, &history_prefix),
        Err(PageCorruption::Namespace)
    );
    let shard = shard_page_key(&HASH, 42);
    let shard_as_history = [&HASH[..], b"g"].concat();
    assert_eq!(
        refused(&shard, &shard_as_history),
        Err(PageCorruption::RowTag)
    );
    assert_eq!(refused(&history, &[]), Err(PageCorruption::RowTag));
    assert_eq!(
        refused(&history, &history_prefix[1..]),
        Err(PageCorruption::RowTag)
    );
}

#[test]
fn only_versions_6_and_7_are_pages() {
    for ver in [0, 2, 3, 4, 5, 8, 0x80, 0xff] {
        let raw = spliced(&reference(), 0..1, &[ver]);
        assert_eq!(
            admit_at(&raw, 42),
            Err(PageCorruption::Version),
            "version {ver}"
        );
    }
    let raw = spliced(&reference(), 0..1, &[7]);
    assert!(admit_at(&raw, 42).unwrap().is_compressed());
}

#[test]
fn the_record_count_is_1_to_4096_in_a_minimal_varint() {
    let raw = reference();
    assert_eq!(
        admit_at(&spliced(&raw, 9..10, &[0]), 39),
        Err(PageCorruption::Count)
    );
    let overlong = spliced(&raw, 9..10, &[0x83, 0x00]);
    assert_eq!(admit_at(&overlong, 42), Err(PageCorruption::Count));
    let unterminated = spliced(&raw, 9..raw.len(), &[0x80]);
    assert_eq!(admit_at(&unterminated, 42), Err(PageCorruption::Count));
    let most = forged(6, 0, PAGE_MAX_RECORDS, 16);
    assert_eq!(
        most.get(9..11),
        Some(&[0x80, 0x20][..]),
        "4096 takes two bytes"
    );
    assert_eq!(admit_at(&most, 4095).unwrap().count(), PAGE_MAX_RECORDS);
    let over = forged(6, 0, PAGE_MAX_RECORDS + 1, 16);
    assert_eq!(admit_at(&over, 4096), Err(PageCorruption::Count));
}

#[test]
fn every_cut_short_page_is_refused() {
    let raw = reference();
    for cut in 0..raw.len() {
        let expected = if cut == 9 {
            PageCorruption::Count
        } else {
            PageCorruption::Truncated
        };
        let short = raw.get(..cut).unwrap();
        assert_eq!(admit_at(short, 42), Err(expected), "cut at {cut}");
    }
}

#[test]
fn a_routing_key_must_be_utf8() {
    let raw = spliced(&reference(), 24..25, &[0xff]);
    assert_eq!(admit_at(&raw, 42), Err(PageCorruption::RoutingKey));
}

#[test]
fn a_routing_key_of_the_full_u16_length_is_admitted() {
    let routing_key = "k".repeat(usize::from(u16::MAX));
    let lane = PageLane {
        routing_key: &routing_key,
        ..lane()
    };
    let raw = cipher().seal(&lane, 0, &[b"x".to_vec()]).unwrap().bytes;
    let page = admit_at(&raw, 0).unwrap();
    assert_eq!(page.routing_key(), routing_key);
    let records = cipher().open(&page).unwrap();
    assert_eq!(
        records
            .records()
            .map(|record| record.payload)
            .collect::<Vec<_>>(),
        [b"x"]
    );
}

#[test]
fn the_ciphertext_must_hold_its_tag_and_fit_its_cap() {
    assert_eq!(admit_at(&forged(6, 0, 2, 15), 1), Err(PageCorruption::Tag));
    assert_eq!(admit_at(&forged(6, 0, 2, 0), 1), Err(PageCorruption::Tag));
    let at_cap = PAGE_TARGET_PLAINTEXT + 16;
    assert!(admit_at(&forged(6, 0, 2, at_cap), 1).is_ok());
    let over = PAGE_TARGET_PLAINTEXT + 17;
    assert_eq!(
        admit_at(&forged(6, 0, 2, over), 1),
        Err(PageCorruption::Oversized)
    );
    assert_eq!(
        admit_at(&forged(7, 0, 2, over), 1),
        Err(PageCorruption::Oversized)
    );
    let single = body_cap(1) + 16;
    assert!(admit_at(&forged(7, 0, 1, single), 0).is_ok());
    let past_single = body_cap(1) + 17;
    assert_eq!(
        admit_at(&forged(6, 0, 1, past_single), 0),
        Err(PageCorruption::Oversized)
    );
}

#[test]
fn nothing_may_follow_the_ciphertext() {
    let raw = [reference().as_slice(), &[0]].concat();
    assert_eq!(admit_at(&raw, 42), Err(PageCorruption::Trailing));
}

#[test]
fn the_row_key_must_name_the_pages_last_offset() {
    let raw = reference();
    for key in [40, 41, 43, u64::MAX] {
        let refused = admit_at(&raw, key);
        assert_eq!(refused, Err(PageCorruption::Offset { key, page: 42 }));
    }
    let mut moved = raw;
    flip(&mut moved, 8);
    assert_eq!(
        admit_at(&moved, 43).unwrap().first(),
        41,
        "first is a clear field"
    );
}

#[test]
fn a_page_may_not_pass_the_last_offset() {
    let two = forged(6, u64::MAX, 2, 16);
    assert_eq!(
        admit_at(&two, u64::MAX),
        Err(PageCorruption::OffsetOverflow)
    );
    let one = forged(6, u64::MAX, 1, 16);
    assert_eq!(admit_at(&one, u64::MAX).unwrap().last(), u64::MAX);
}
