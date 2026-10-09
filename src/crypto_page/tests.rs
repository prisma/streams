//! Page round trips, request splitting and the compression choice, plus the
//! fixtures the other page test modules share.
#![cfg(test)]

use super::stamped;
use bytes::Bytes;

use super::body::{get_varint, put_varint, varint_len};
use super::seal::split_pages;
use super::{
    CheckedPage, PAGE_MAX_RECORDS, PageCipher, PageLane, SHARD_PAGE_TAG, SealError, SealRecord,
    SealedPage, history_page_key, shard_page_key,
};
use crate::crypto::{MAX_RECORD_PLAINTEXT, RouteHash, SegmentHash, StreamKey, derive_subkey};

pub(super) const SEGMENT: [u8; 16] = [0x22; 16];
pub(super) const HASH: [u8; 16] = [0x44; 16];
pub(super) const TS: i64 = 1_760_000_000_000;

/// One record as a page returns it: offset, timestamp, payload.
pub(super) type Opened = (u64, i64, Vec<u8>);

pub(super) fn subkey(routing_key: &str, key_version: u32) -> [u8; 32] {
    derive_subkey(&StreamKey([7; 32]), &[3; 16], routing_key, key_version)
}

/// The cipher of the fixture lane: routing key "rk", key version 1.
pub(super) fn cipher() -> PageCipher {
    PageCipher::new(&subkey("rk", 1), &SEGMENT)
}

pub(super) fn lane() -> PageLane<'static> {
    PageLane {
        key_version: 1,
        routing_key: "rk",
    }
}

/// The canonical shard-log page prefix of the fixture segment.
pub(super) fn prefix() -> Vec<u8> {
    [HASH.as_slice(), &[SHARD_PAGE_TAG]].concat()
}

/// Admit a sealed page under its canonical shard-log key.
pub(super) fn admit(page: &SealedPage) -> CheckedPage {
    let key = shard_page_key(&HASH, page.last);
    CheckedPage::from_row(&key, &prefix(), Bytes::from(page.bytes.clone())).unwrap()
}

/// Seal `records` as one request from `first`, every record at TS, admit
/// every page under its canonical key and open it with the fixture cipher.
pub(super) fn round_trip(records: &[Vec<u8>], first: u64) -> (Vec<CheckedPage>, Vec<Opened>) {
    round_trip_at(&stamped(TS, records), first)
}

/// Seal `records` as one request from `first`, admit every page under its
/// canonical key and open it with the fixture cipher.
pub(super) fn round_trip_at(
    records: &[SealRecord<'_>],
    first: u64,
) -> (Vec<CheckedPage>, Vec<Opened>) {
    let cipher = cipher();
    let sealed = cipher.seal_request(&lane(), first, records).unwrap();
    let pages: Vec<CheckedPage> = sealed.iter().map(admit).collect();
    let mut opened = Vec::new();
    for page in &pages {
        let records = cipher.open(page).unwrap();
        assert_eq!(
            (records.first(), records.last()),
            (page.first(), page.last())
        );
        opened.extend(
            records
                .records()
                .map(|record| (record.offset, record.ts_ms, record.payload.to_vec())),
        );
    }
    (pages, opened)
}

/// What a request of `records` from `first`, every record at TS, must read
/// back as.
pub(super) fn expected(records: &[Vec<u8>], first: u64) -> Vec<Opened> {
    expected_at(&stamped(TS, records), first)
}

/// What a request of `records` from `first` must read back as.
pub(super) fn expected_at(records: &[SealRecord<'_>], first: u64) -> Vec<Opened> {
    records
        .iter()
        .zip(first..)
        .map(|(record, offset)| (offset, record.ts_ms, record.payload.to_vec()))
        .collect()
}

pub(super) const NONCE: [u8; 12] = [0x33; 12];
pub(super) const FIRST: u64 = 40;

/// The reference page: three raw records from offset 40 in the fixture lane
/// under a fixed nonce. Its layout: ver 0, first 1..9, count 9, ts_ms
/// 10..18, key version 18..22, rk_len 22..24, "rk" 24..26, nonce 26..38,
/// ct_len 38..42, then the 20-byte body and the 16-byte tag from 42.
pub(super) fn reference() -> Vec<u8> {
    let records = [b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()];
    cipher()
        .seal_with_nonce(&lane(), FIRST, &stamped(TS, &records), NONCE)
        .unwrap()
        .bytes
}

/// Flip the low bit of one byte.
pub(super) fn flip(raw: &mut [u8], at: usize) {
    *raw.get_mut(at).unwrap() ^= 1;
}

/// Deterministic bytes zstd cannot shrink.
pub(super) fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        })
        .collect()
}

/// A JSON event of about `size` bytes, as batched producers send them.
pub(super) fn event(index: usize, size: usize) -> Vec<u8> {
    let head = format!(
        r#"{{"id":{index},"user":"user-{}","event":"page_view","path":"/products/{}","pad":""#,
        index % 37,
        index % 11
    );
    let mut event = head.into_bytes();
    event.resize(size.saturating_sub(2), b'x');
    event.extend_from_slice(br#""}"#);
    event
}

fn counts(pages: &[CheckedPage]) -> Vec<usize> {
    pages.iter().map(CheckedPage::count).collect()
}

fn lasts(pages: &[CheckedPage]) -> Vec<u64> {
    pages.iter().map(CheckedPage::last).collect()
}

#[test]
fn one_record_round_trips_as_one_raw_page() {
    let records = vec![b"hello".to_vec()];
    let (pages, opened) = round_trip(&records, 100);
    assert_eq!(counts(&pages), [1]);
    let [page] = pages.as_slice() else {
        panic!("one page")
    };
    assert_eq!((page.first(), page.last()), (100, 100));
    assert_eq!(
        (page.ts_ms(), page.key_version(), page.routing_key()),
        (TS, 1, "rk")
    );
    assert!(!page.is_compressed(), "a 7-byte body is stored raw");
    // 56 header and tag bytes, the routing key, and a 7-byte body.
    assert_eq!(page.raw().len(), 56 + 2 + 7);
    assert_eq!(opened, expected(&records, 100));
}

#[test]
fn two_records_round_trip_from_offset_zero() {
    let records = vec![b"hello".to_vec(), b"world!".to_vec()];
    let (pages, opened) = round_trip(&records, 0);
    assert_eq!((counts(&pages), lasts(&pages)), (vec![2], vec![1]));
    assert_eq!(opened, expected(&records, 0));
}

#[test]
fn sixty_two_one_kib_events_are_one_compressed_page() {
    let records: Vec<Vec<u8>> = (0..62).map(|index| event(index, 1024)).collect();
    let (pages, opened) = round_trip(&records, 1000);
    assert_eq!((counts(&pages), lasts(&pages)), (vec![62], vec![1061]));
    let [page] = pages.as_slice() else {
        panic!("one page")
    };
    assert!(page.is_compressed());
    assert!(
        page.raw().len() < 62 * 1024 / 3,
        "the page stores {} bytes",
        page.raw().len()
    );
    assert_eq!(opened, expected(&records, 1000));
}

#[test]
fn a_page_holds_at_most_4096_records() {
    let full: Vec<Vec<u8>> = (0..PAGE_MAX_RECORDS).map(|i| noise(i as u64, 8)).collect();
    let (pages, opened) = round_trip(&full, 7);
    assert_eq!((counts(&pages), lasts(&pages)), (vec![4096], vec![4102]));
    assert_eq!(opened, expected(&full, 7));

    let mut over = full;
    over.push(b"one more".to_vec());
    let (pages, opened) = round_trip(&over, 7);
    assert_eq!(
        (counts(&pages), lasts(&pages)),
        (vec![4096, 1], vec![4102, 4103])
    );
    assert_eq!(opened, expected(&over, 7));
}

#[test]
fn a_page_body_reaches_exactly_64_kib_and_one_byte_more_splits() {
    let cipher = cipher();
    // Each 32,764-byte record adds 3 length bytes, 1 delta byte and itself.
    let exact = vec![noise(1, 32_764), noise(2, 32_764)];
    let (pages, opened) = round_trip(&exact, 0);
    assert_eq!(counts(&pages), [2]);
    let [page] = pages.as_slice() else {
        panic!("one page")
    };
    assert!(!page.is_compressed(), "noise does not compress");
    assert_eq!(cipher.open(page).unwrap().body_len(), 64 << 10);
    assert_eq!(opened, expected(&exact, 0));

    let over = vec![noise(1, 32_764), noise(2, 32_765)];
    let (pages, opened) = round_trip(&over, 0);
    assert_eq!((counts(&pages), lasts(&pages)), (vec![1, 1], vec![0, 1]));
    assert_eq!(opened, expected(&over, 0));
}

#[test]
fn a_record_larger_than_the_target_is_a_page_alone() {
    let records = vec![
        noise(1, 100),
        noise(2, 64 << 10),
        noise(3, 70_000),
        noise(4, 100),
    ];
    let (pages, opened) = round_trip(&records, 50);
    assert_eq!(
        (counts(&pages), lasts(&pages)),
        (vec![1, 1, 1, 1], vec![50, 51, 52, 53])
    );
    let body = cipher().open(pages.get(1).unwrap()).unwrap();
    assert_eq!(body.body_len(), (64 << 10) + 3 + 1);
    assert_eq!(opened, expected(&records, 50));
}

#[test]
fn a_record_at_the_record_cap_round_trips_and_one_byte_more_is_refused() {
    let records = vec![vec![0x5a; MAX_RECORD_PLAINTEXT]];
    let (pages, opened) = round_trip(&records, 9);
    assert_eq!(counts(&pages), [1]);
    assert_eq!(opened, expected(&records, 9));

    let over = vec![b"small".to_vec(), vec![0; MAX_RECORD_PLAINTEXT + 1]];
    assert_eq!(
        split_pages(&stamped(TS, &over)).err(),
        Some(SealError::RecordTooLarge)
    );
    let refused = cipher().seal_request(&lane(), 9, &stamped(TS, &over));
    assert_eq!(refused.unwrap_err(), SealError::RecordTooLarge);
}

/// Each record keeps its own timestamp through its page, stored as a delta
/// to the record before it, and a seal refuses records that go back in
/// time, within a page and across the pages of a request.
#[test]
fn every_record_keeps_its_own_timestamp() {
    let payloads = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
    let stamps = [TS, TS, TS + 1, TS + 300_000];
    let records: Vec<SealRecord<'_>> = payloads
        .iter()
        .zip(stamps)
        .map(|(payload, ts_ms)| SealRecord { ts_ms, payload })
        .collect();
    let (pages, opened) = round_trip_at(&records, 9);
    assert_eq!(pages.len(), 1);
    let [page] = pages.as_slice() else {
        panic!("one page")
    };
    assert_eq!(page.ts_ms(), TS, "the header holds the first timestamp");
    let seen: Vec<(u64, i64)> = opened.iter().map(|(at, ts, _)| (*at, *ts)).collect();
    assert_eq!(seen, [(9, TS), (10, TS), (11, TS + 1), (12, TS + 300_000)]);
    let mut back = records.clone();
    back.swap(2, 3);
    assert_eq!(
        cipher().seal(&lane(), 0, &back).err(),
        Some(SealError::TimestampOrder)
    );
    let wide = [noise(1, 40_000), noise(2, 40_000)];
    let mut across = stamped(TS, &wide);
    across.get_mut(1).unwrap().ts_ms = TS - 1;
    assert_eq!(
        cipher().seal_request(&lane(), 0, &across).err(),
        Some(SealError::TimestampOrder),
        "the second page may not start before the first ends"
    );
}

/// A seal refuses a record older than the one before it at the cuts the
/// page cut makes without reading that record: after a record whose body
/// alone passes the page target, and after the 4,096-record cap. The same
/// requests in order seal as two pages each.
#[test]
fn a_seal_refuses_time_going_back_where_a_page_is_cut_before_it() {
    let big = [noise(3, 70_000), b"next".to_vec()];
    let capped = vec![b"r".to_vec(); PAGE_MAX_RECORDS + 1];
    for payloads in [&big[..], &capped[..]] {
        let mut records = stamped(TS, payloads);
        assert_eq!(split_pages(&records).unwrap().len(), 2);
        let sealed = cipher().seal_request(&lane(), 0, &records).unwrap();
        assert_eq!(sealed.len(), 2);
        records.last_mut().unwrap().ts_ms = TS - 1;
        assert_eq!(
            cipher().seal_request(&lane(), 0, &records).err(),
            Some(SealError::TimestampOrder),
            "{} records",
            records.len()
        );
    }
}

/// The cut counts each delta at its varint width: 4,094 records whose
/// bodies fill a page exactly with one-byte deltas split once each delta
/// takes three bytes.
#[test]
fn the_cut_counts_each_timestamp_delta() {
    // 4,094 records of 14 payload bytes: 1 + 1 + 14 = 16 body bytes each,
    // 65,504 in all, plus one record of 30 bytes (32 body bytes): 65,536.
    let mut payloads = vec![vec![b'x'; 14]; 4094];
    payloads.push(vec![b'y'; 30]);
    let flat = stamped(TS, &payloads);
    assert_eq!(split_pages(&flat).unwrap().len(), 1);
    // A 16,384 ms step is a three-byte delta: two more bytes per record.
    let mut ts_ms = TS;
    let stepped: Vec<SealRecord<'_>> = payloads
        .iter()
        .map(|payload| {
            ts_ms += 16_384;
            SealRecord { ts_ms, payload }
        })
        .collect();
    let groups = split_pages(&stepped).unwrap();
    let counts: Vec<usize> = groups.iter().map(|group| group.len()).collect();
    // 2 + 16 + (n - 1) * 18 <= 65,536 gives n = 3,641.
    assert_eq!(counts, [3641, 454]);
    let (pages, opened) = round_trip_at(&stepped, 0);
    assert_eq!(pages.len(), 2);
    assert_eq!(opened, expected_at(&stepped, 0));
}

#[test]
fn compression_needs_a_256_byte_body_and_a_gain() {
    // One 252-byte record: 2 length bytes, 1 delta byte, 252 payload bytes.
    let below = round_trip(&[vec![0; 252]], 0).0;
    let at = round_trip(&[vec![0; 253]], 0).0;
    let incompressible = round_trip(&[noise(5, 2000)], 0).0;
    let flags = |pages: &[CheckedPage]| -> Vec<bool> {
        pages.iter().map(CheckedPage::is_compressed).collect()
    };
    assert_eq!(flags(&below), vec![false], "a 255-byte body is not tried");
    assert_eq!(
        flags(&at),
        vec![true],
        "a 256-byte body that shrinks is compressed"
    );
    assert_eq!(flags(&incompressible), vec![false], "no gain, stored raw");
}

#[test]
fn sealing_refuses_what_no_page_can_hold() {
    let cipher = cipher();
    let none: [&[u8]; 0] = [];
    assert_eq!(
        cipher.seal(&lane(), 0, &stamped(TS, &none)).unwrap_err(),
        SealError::Empty
    );
    assert!(
        cipher
            .seal_request(&lane(), 0, &stamped(TS, &none))
            .unwrap()
            .is_empty()
    );
    let many = vec![Vec::new(); PAGE_MAX_RECORDS + 1];
    assert_eq!(
        cipher.seal(&lane(), 0, &stamped(TS, &many)).unwrap_err(),
        SealError::TooManyRecords
    );
    let wide = vec![noise(1, 40_000), noise(2, 40_000)];
    assert_eq!(
        cipher.seal(&lane(), 0, &stamped(TS, &wide)).unwrap_err(),
        SealError::PageTooLarge
    );
    let huge = vec![vec![0; MAX_RECORD_PLAINTEXT + 1]];
    assert_eq!(
        cipher.seal(&lane(), 0, &stamped(TS, &huge)).unwrap_err(),
        SealError::RecordTooLarge
    );
    let long_key = "k".repeat(usize::from(u16::MAX) + 1);
    let long = PageLane {
        routing_key: &long_key,
        ..lane()
    };
    let one = [b"x".to_vec()];
    assert_eq!(
        cipher.seal(&long, 0, &stamped(TS, &one)).unwrap_err(),
        SealError::RoutingKeyTooLong
    );
    let two = [b"x".to_vec(), b"y".to_vec()];
    assert_eq!(
        cipher
            .seal(&lane(), u64::MAX, &stamped(TS, &two))
            .unwrap_err(),
        SealError::OffsetOverflow
    );
    assert!(
        cipher.seal(&lane(), u64::MAX, &stamped(TS, &one)).is_ok(),
        "the last offset is u64::MAX"
    );
}

#[test]
fn a_request_may_end_at_the_last_offset_but_not_pass_it() {
    let cipher = cipher();
    let records = vec![Vec::new(); PAGE_MAX_RECORDS + 1];
    let first = u64::MAX - PAGE_MAX_RECORDS as u64;
    let pages = cipher
        .seal_request(&lane(), first, &stamped(TS, &records))
        .unwrap();
    let ends: Vec<u64> = pages.iter().map(|page| page.last).collect();
    assert_eq!(ends, [u64::MAX - 1, u64::MAX]);
    let mut past = records;
    past.push(Vec::new());
    let refused = cipher.seal_request(&lane(), first, &stamped(TS, &past));
    assert_eq!(refused.unwrap_err(), SealError::OffsetOverflow);
}

#[test]
fn every_page_gets_a_fresh_nonce() {
    let cipher = cipher();
    let one = [b"same".to_vec()];
    assert_ne!(
        cipher.seal(&lane(), 0, &stamped(TS, &one)).unwrap().bytes,
        cipher.seal(&lane(), 0, &stamped(TS, &one)).unwrap().bytes
    );
    let records = vec![noise(1, 40_000), noise(2, 40_000)];
    let pages = cipher
        .seal_request(&lane(), 0, &stamped(TS, &records))
        .unwrap();
    let nonces: Vec<Vec<u8>> = pages
        .iter()
        .map(|page| page.bytes.get(26..38).unwrap().to_vec())
        .collect();
    let [first, second] = nonces.as_slice() else {
        panic!("two pages")
    };
    assert_ne!(first, second);
}

/// A window's scan reaches, exclusive, 4,095 offsets past its last record:
/// the page holding that record ends below there whatever its size.
#[test]
fn a_scan_bound_reaches_the_page_holding_the_last_record() {
    use super::page_scan_bound;
    assert_eq!(page_scan_bound(1), PAGE_MAX_RECORDS as u64);
    assert_eq!(page_scan_bound(10), 9 + PAGE_MAX_RECORDS as u64);
    assert_eq!(page_scan_bound(u64::MAX - 1), u64::MAX);
    assert_eq!(page_scan_bound(u64::MAX), u64::MAX);
}

#[test]
fn row_keys_are_namespace_tag_and_last_offset() {
    let last = 0x0102_0304_0506_0708;
    let shard = shard_page_key(&HASH, last);
    assert_eq!(shard, [&HASH[..], b"p", &last.to_be_bytes()].concat());
    let (route, inc) = (RouteHash([0x0a; 16]), SegmentHash([0x0b; 16]));
    let history = history_page_key(route, inc, last);
    assert_eq!(
        history,
        [&[0x0a; 16][..], &[0x0b; 16], b"g", &last.to_be_bytes()].concat()
    );

    let sealed = cipher()
        .seal_request(&lane(), 5, &stamped(TS, &[b"a".to_vec()]))
        .unwrap();
    let [page] = sealed.as_slice() else {
        panic!("one page")
    };
    let key = history_page_key(route, inc, 5);
    let prefix = history_page_key(route, inc, 0);
    let (prefix, _) = prefix.split_last_chunk::<8>().unwrap();
    let admitted = CheckedPage::from_row(&key, prefix, Bytes::from(page.bytes.clone()));
    assert_eq!(admitted.unwrap(), admit(page));
}

#[test]
fn varints_are_minimal_leb128_and_refuse_every_other_spelling() {
    let read = |bytes: &[u8]| {
        let mut input = bytes;
        get_varint(&mut input).map(|value| (value, input.len()))
    };
    for value in [
        0,
        1,
        127,
        128,
        16_383,
        16_384,
        1 << 28,
        u64::MAX >> 1,
        u64::MAX,
    ] {
        let mut encoded = Vec::new();
        put_varint(&mut encoded, value);
        assert_eq!(encoded.len(), varint_len(value), "{value}");
        encoded.push(0xee);
        assert_eq!(
            read(&encoded),
            Some((value, 1)),
            "{value} consumes only itself"
        );
    }
    assert_eq!(varint_len(4096), 2);
    assert_eq!(varint_len(MAX_RECORD_PLAINTEXT as u64), 4);
    let widest = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
    assert_eq!(read(&widest), Some((u64::MAX, 0)));
    let past_u64 = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
    assert_eq!(read(&past_u64), None, "a 65th bit is refused, not dropped");
    let eleven = [
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x81, 0x00,
    ];
    assert_eq!(read(&eleven), None);
    assert_eq!(
        read(&[0x80, 0x00]),
        None,
        "a redundant zero group is overlong"
    );
    assert_eq!(read(&[0xff, 0x80, 0x00]), None);
    assert_eq!(read(&[0x80]), None, "unterminated");
    assert_eq!(read(&[]), None);
}
