//! Properties over random requests and random bytes: a request reads back
//! exactly through its pages, its split is the greedy one, no single-byte
//! change of a sealed page survives admission and opening, and admission
//! and the body parser refuse arbitrary bytes without panicking.
#![cfg(test)]

use bytes::Bytes;
use proptest::prelude::{Strategy, any};

use super::body::{build_body, parse_body, record_body_bytes};
use super::seal::split_pages;
use super::tests::{HASH, cipher, expected, lane, noise, prefix, round_trip};
use super::{CheckedPage, PAGE_MAX_RECORDS, PAGE_TARGET_PLAINTEXT, body_cap, shard_page_key};

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

/// The body bytes of a group of records, as the splitter counts them.
fn body_of(records: &[Vec<u8>]) -> usize {
    records
        .iter()
        .map(|record| record_body_bytes(record.len()))
        .sum()
}

/// Every page but the last is full: the next record would pass the count
/// or the body cap.
fn greedy(pages: &[&[Vec<u8>]]) -> bool {
    pages.windows(2).all(|window| match window {
        [page, next] => {
            let next_body = next
                .first()
                .map_or(0, |record| record_body_bytes(record.len()));
            page.len() == PAGE_MAX_RECORDS
                || body_of(page).saturating_add(next_body) > PAGE_TARGET_PLAINTEXT
        }
        _ => false,
    })
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 48, ..proptest::prelude::ProptestConfig::default() })]

    /// Any request reads back exactly, in contiguous pages of greedy size
    /// within their caps, from any first offset.
    #[test]
    fn quality_page_requests_round_trip_exactly(
        records in proptest::collection::vec(record(), 0..=24),
        first in 0u64..=u64::MAX - 64,
    ) {
        let (pages, opened) = round_trip(&records, first);
        proptest::prop_assert_eq!(&opened, &expected(&records, first));
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
        let sealed = cipher().seal_request(&lane(), 77, &records).unwrap();
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

    /// Built bodies parse back to their records; arbitrary bodies parse
    /// only into spans that tile their payload bytes exactly.
    #[test]
    fn quality_page_bodies_parse_exactly(
        records in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..300), 1..=40),
        arbitrary in proptest::collection::vec(any::<u8>(), 0..=48),
        count in 1usize..=6,
        ts_ms in any::<i64>(),
    ) {
        let body = build_body(&records);
        let table = parse_body(&body, records.len(), ts_ms).unwrap();
        let lens: Vec<usize> = records.iter().map(Vec::len).collect();
        proptest::prop_assert_eq!(table.records.iter().map(|span| span.len).collect::<Vec<_>>(), lens);
        proptest::prop_assert!(table.records.iter().all(|span| span.ts_ms == ts_ms));
        if let Ok(table) = parse_body(&arbitrary, count, ts_ms) {
            let payloads: usize = table.records.iter().map(|span| span.len).sum();
            proptest::prop_assert_eq!(table.records.len(), count);
            proptest::prop_assert_eq!(table.payload_start.checked_add(payloads), Some(arbitrary.len()));
            proptest::prop_assert!(table.records.iter().all(|span| span.ts_ms >= ts_ms));
        }
    }
}
