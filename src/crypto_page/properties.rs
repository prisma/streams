//! Properties over random requests and random bytes: a request reads back
//! exactly through its pages, its split is the greedy one, no single-byte
//! change of a sealed page survives admission and opening, and admission
//! and the body parser refuse arbitrary bytes without panicking.
#![cfg(test)]

use bytes::Bytes;
use proptest::prelude::{Just, Strategy, any};

use super::body::{build_body, parse_body, record_body_bytes};
use super::seal::split_pages;
use super::tests::{HASH, TS, cipher, expected_at, lane, noise, prefix, round_trip_at};
use super::{
    CheckedPage, PAGE_MAX_RECORDS, PAGE_TARGET_PLAINTEXT, SealRecord, body_cap, shard_page_key,
    stamped,
};

/// One record: a size class, a size jitter, a fill byte, and whether its
/// bytes compress. Mostly small; some near half a page, some past a page.
fn record() -> impl Strategy<Value = Vec<u8>> {
    (0u8..16, any::<u16>(), any::<u8>(), any::<bool>()).prop_map(
        |(class, jitter, fill, compressible)| {
            let jitter = usize::from(jitter);
            let len = match class {
                0..=9 => jitter % 300,
                10..=12 => jitter % 4096,
                13 | 14 => (jitter % 4_000).saturating_add(30_000),
                _ => (jitter % 4_000).saturating_add(64_000),
            };
            if compressible {
                vec![fill; len]
            } else {
                noise(u64::from(fill), len)
            }
        },
    )
}

/// `payloads` stamped from TS on: each record `step` milliseconds after
/// the one before it.
fn timeline(payloads: &[(Vec<u8>, u32)]) -> Vec<SealRecord<'_>> {
    let mut ts_ms = TS;
    payloads
        .iter()
        .map(|(payload, step)| {
            ts_ms = ts_ms.saturating_add(i64::from(*step));
            SealRecord { ts_ms, payload }
        })
        .collect()
}

/// The delta a record at `ts_ms` takes after a record at `before`.
fn delta(before: Option<&SealRecord<'_>>, ts_ms: i64) -> u64 {
    before.map_or(0, |before| ts_ms.abs_diff(before.ts_ms))
}

/// The body bytes of a group of records, as the splitter counts them: each
/// length and each delta to the record before at its varint width.
fn body_of(records: &[SealRecord<'_>]) -> usize {
    let mut before = None;
    records
        .iter()
        .map(|record| {
            let bytes = record_body_bytes(record.payload.len(), delta(before, record.ts_ms));
            before = Some(record);
            bytes
        })
        .sum()
}

/// Every page but the last is full: the next record, with its delta to the
/// page's last record, would pass the count or the body cap.
fn greedy(pages: &[&[SealRecord<'_>]]) -> bool {
    pages.windows(2).all(|window| match window {
        [page, next] => {
            let next_body = next.first().map_or(0, |record| {
                record_body_bytes(record.payload.len(), delta(page.last(), record.ts_ms))
            });
            page.len() == PAGE_MAX_RECORDS
                || body_of(page).saturating_add(next_body) > PAGE_TARGET_PLAINTEXT
        }
        _ => false,
    })
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 48, ..proptest::prelude::ProptestConfig::default() })]

    /// Any request reads back exactly, every record with its own
    /// timestamp, in contiguous pages of greedy size within their caps,
    /// from any first offset.
    #[test]
    fn quality_page_requests_round_trip_exactly(
        payloads in proptest::collection::vec((record(), proptest::prop_oneof![Just(0u32), 0u32..300_000]), 0..=24),
        first in 0u64..=u64::MAX - 64,
    ) {
        let records = timeline(&payloads);
        let (pages, opened) = round_trip_at(&records, first);
        proptest::prop_assert_eq!(&opened, &expected_at(&records, first));
        let groups = split_pages(&records).unwrap();
        proptest::prop_assert_eq!(groups.len(), pages.len());
        proptest::prop_assert!(greedy(&groups));
        let mut next = first;
        for (page, group) in pages.iter().zip(&groups) {
            proptest::prop_assert_eq!((page.first(), page.count()), (next, group.len()));
            proptest::prop_assert!(body_of(group) <= body_cap(group.len()));
            next = page.last().saturating_add(1);
        }
    }

    /// Requests of many tiny records split at exactly 4,096 records.
    #[test]
    fn quality_page_counts_split_at_the_record_cap(
        lens in proptest::collection::vec(0usize..4, 0..=9000),
    ) {
        let records: Vec<Vec<u8>> = lens.iter().map(|len| vec![7; *len]).collect();
        let records = stamped(TS, &records);
        let groups = split_pages(&records).unwrap();
        let counts: Vec<usize> = groups.iter().map(|group| group.len()).collect();
        let full = records.len() / PAGE_MAX_RECORDS;
        let rest = records.len() % PAGE_MAX_RECORDS;
        let mut expected = vec![PAGE_MAX_RECORDS; full];
        if rest > 0 {
            expected.push(rest);
        }
        proptest::prop_assert_eq!(counts, expected);
    }

    /// No single-byte change of a sealed page is both admitted under its
    /// row key and opened.
    #[test]
    fn quality_page_any_changed_byte_is_refused(
        records in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..400), 1..=8),
        at in any::<proptest::sample::Index>(),
        mask in 1u8..,
    ) {
        let sealed = cipher().seal_request(&lane(), 77, &stamped(TS, &records)).unwrap();
        let [page] = sealed.as_slice() else {
            return Err(proptest::test_runner::TestCaseError::fail("one page"));
        };
        let mut raw = page.bytes.clone();
        let at = at.index(raw.len());
        *raw.get_mut(at).unwrap() ^= mask;
        let key = shard_page_key(&HASH, page.last);
        let opened = CheckedPage::from_row(&key, &prefix(), Bytes::from(raw))
            .ok()
            .and_then(|page| cipher().open(&page).ok());
        proptest::prop_assert!(opened.is_none(), "byte {} xor {:#04x} survived", at, mask);
    }

    /// Admission refuses arbitrary rows without panicking; whatever it
    /// admits is exactly one page keyed by its last offset.
    #[test]
    fn quality_page_admission_holds_for_arbitrary_rows(
        raw in proptest::collection::vec(any::<u8>(), 0..=160),
        last in any::<u64>(),
    ) {
        let key = shard_page_key(&HASH, last);
        if let Ok(page) = CheckedPage::from_row(&key, &prefix(), Bytes::from(raw)) {
            proptest::prop_assert!((1..=PAGE_MAX_RECORDS).contains(&page.count()));
            let span = u64::try_from(page.count()).unwrap().saturating_sub(1);
            proptest::prop_assert_eq!(page.first().checked_add(span), Some(last));
        }
    }

    /// Built bodies parse back to their records and timestamps; arbitrary
    /// bodies parse only into spans that tile their payload bytes exactly,
    /// the first at the header's timestamp and none going back.
    #[test]
    fn quality_page_bodies_parse_exactly(
        payloads in proptest::collection::vec((proptest::collection::vec(any::<u8>(), 0..300), any::<u32>()), 1..=40),
        arbitrary in proptest::collection::vec(any::<u8>(), 0..=48),
        count in 1usize..=6,
        ts_ms in any::<i64>(),
    ) {
        let records = timeline(&payloads);
        let body = build_body(&records).unwrap();
        let head = records.first().map_or(TS, |record| record.ts_ms);
        let table = parse_body(&body, records.len(), head).unwrap();
        let built: Vec<(usize, i64)> = records.iter().map(|record| (record.payload.len(), record.ts_ms)).collect();
        let parsed: Vec<(usize, i64)> = table.records.iter().map(|span| (span.len, span.ts_ms)).collect();
        proptest::prop_assert_eq!(parsed, built);
        if let Ok(table) = parse_body(&arbitrary, count, ts_ms) {
            let payloads: usize = table.records.iter().map(|span| span.len).sum();
            proptest::prop_assert_eq!(table.records.len(), count);
            proptest::prop_assert_eq!(table.payload_start.checked_add(payloads), Some(arbitrary.len()));
            proptest::prop_assert_eq!(table.records.first().map(|span| span.ts_ms), Some(ts_ms));
            proptest::prop_assert!(table.records.windows(2).all(|pair| matches!(pair, [a, b] if a.ts_ms <= b.ts_ms)));
        }
    }
}
