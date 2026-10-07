//! Layout 5 history reads: the pages the absorber copies are read back by
//! their last-offset keys, clipped at both ends of a window, filtered by
//! their clear routing key, and never confused with postings rows.
use super::super::bounded_discovery_tests::rig;
use super::super::{Absorber, AbsorberConfig};
use super::{PageScan, read_history2, read_history2_keyed_envelope, read_history2_scan};
use crate::crypto::{RouteHash, SegmentHash, StreamKey};
use crate::crypto_page::{
    CheckedPage, PageCipher, PageLane, SealedPage, history_page_key, history_page_prefix,
    shard_page_key,
};
use crate::postings::{PageBuilder, postings_key, postings_range, rk_hash};
use crate::shard::record::PageSlices;
use crate::shard::{Deliver, ShardEngine};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;
use std::time::Duration;

const ROUTE: RouteHash = RouteHash([0x31; 16]);
const INC: SegmentHash = SegmentHash([0x32; 16]);
const KEY: StreamKey = StreamKey([7; 32]);

/// An empty in-memory history partition.
async fn partition(path: &str) -> Arc<Db> {
    let store = Arc::new(object_store::memory::InMemory::new());
    Arc::new(Db::builder(path, store).build().await.unwrap())
}

/// The payload of record `offset` of routing key `rk` in the direct fixtures.
fn payload(rk: &str, offset: u64) -> Vec<u8> {
    format!("{rk}:{offset}").into_bytes()
}

/// One page of `count` records of `rk` from `first`, as a request seals it.
fn page(rk: &str, first: u64, count: u64) -> SealedPage {
    let lane = PageLane {
        ts_ms: 1,
        key_version: 0,
        routing_key: rk,
    };
    let records: Vec<_> = (first..first + count).map(|o| payload(rk, o)).collect();
    PageCipher::new(&[9; 32], &INC.0)
        .seal(&lane, first, &records)
        .unwrap()
}

/// Store `pages` under their history keys with the postings notes the
/// absorber writes for them (one per record, the page's bytes spread over
/// its records).
async fn store(db: &Db, pages: &[(&str, SealedPage)]) {
    let mut batch = WriteBatch::new();
    let mut notes = PageBuilder::default();
    for (rk, page) in pages {
        let admitted = CheckedPage::admit(page.bytes.clone().into(), page.last).unwrap();
        for offset in admitted.first()..=admitted.last() {
            notes.note_frame(rk_hash(rk), offset, 1);
        }
        batch.put(history_page_key(ROUTE, INC, page.last), page.bytes.clone());
    }
    for (kh, bucket, first, value) in notes.finish().0 {
        batch.put(postings_key(ROUTE, INC, &kh, bucket, first), value);
    }
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
}

/// The record runs `pages` serve, as (first, last) per page.
fn runs(pages: &PageSlices) -> Vec<(u64, u64)> {
    pages
        .iter()
        .map(|page| (page.first(), page.last()))
        .collect()
}

/// Pages [0, 9], [10, 19] and [20, 24] of the default key.
async fn three_pages(path: &str) -> Arc<Db> {
    let db = partition(path).await;
    store(
        &db,
        &[
            ("", page("", 0, 10)),
            ("", page("", 10, 10)),
            ("", page("", 20, 5)),
        ],
    )
    .await;
    db
}

/// A read of [from, to) serves exactly the records of the window: the first
/// page it meets holds `from` and is clipped there, the page holding
/// `to - 1` is clipped after it, and the page that starts at `to` ends the
/// scan unread. Each slice carries its whole stored page.
#[tokio::test]
async fn a_history_read_starts_and_ends_inside_pages() {
    let db = three_pages("history-mid-page").await;
    let read = |from, to| read_history2(&db, ROUTE, INC, from, to, None, 1 << 20);
    let (pages, last, completed) = read(3, 22).await.unwrap();
    assert_eq!(runs(&pages), [(3, 9), (10, 19), (20, 21)]);
    assert_eq!((pages.len(), last, completed), (19, Some(21), true));
    let whole: Vec<_> = pages
        .iter()
        .map(|p| (p.page().first(), p.page().last()))
        .collect();
    assert_eq!(whole, [(0, 9), (10, 19), (20, 24)]);
    let (pages, last, _) = read(12, 15).await.unwrap();
    assert_eq!((runs(&pages), last), (vec![(12, 14)], Some(14)));
    let (pages, last, _) = read(10, 20).await.unwrap();
    assert_eq!((runs(&pages), last), (vec![(10, 19)], Some(19)));
    let (pages, _, _) = read(0, 1).await.unwrap();
    assert_eq!(runs(&pages), [(0, 0)]);
    let (pages, last, completed) = read(24, 40).await.unwrap();
    assert_eq!(
        (runs(&pages), last, completed),
        (vec![(24, 24)], Some(24), true)
    );
    let (pages, last, completed) = read(25, 40).await.unwrap();
    assert_eq!((runs(&pages), last, completed), (vec![], None, true));
}

/// A page of 4,096 records is keyed by its last offset, 4,095 past its
/// first: a read of its first record alone still meets it, so the scan
/// bound is exactly `to - 1 + 4096`.
#[tokio::test]
async fn a_full_page_is_met_from_its_first_record() {
    let db = partition("history-full-page").await;
    store(&db, &[("", page("", 0, 4096)), ("", page("", 4096, 1))]).await;
    for (from, to, expected) in [(0, 1, (0, 0)), (1, 2, (1, 1)), (4095, 4096, (4095, 4095))] {
        let (pages, last, _) = read_history2(&db, ROUTE, INC, from, to, None, 1 << 20)
            .await
            .unwrap();
        assert_eq!(runs(&pages), [expected], "window [{from}, {to})");
        assert_eq!(last, Some(expected.1));
    }
}

/// The budget counts the stored bytes of the pages a scan inspects; the
/// page that reaches it is kept and ends the read, and the next read
/// resumes after it.
#[tokio::test]
async fn a_history_budget_ends_on_the_page_that_reaches_it() {
    let db = three_pages("history-budget").await;
    let first_page = page("", 0, 10).bytes.len();
    let (pages, last, completed) = read_history2_scan(&db, ROUTE, INC, 2, 25, first_page)
        .await
        .unwrap();
    assert_eq!(
        (runs(&pages), last, completed),
        (vec![(2, 9)], Some(9), false)
    );
    let (pages, last, completed) = read_history2_scan(&db, ROUTE, INC, 2, 25, first_page + 1)
        .await
        .unwrap();
    assert_eq!(runs(&pages), [(2, 9), (10, 19)]);
    assert_eq!((last, completed), (Some(19), false));
    let (pages, last, completed) = read_history2_scan(&db, ROUTE, INC, 10, 25, 1)
        .await
        .unwrap();
    assert_eq!(
        (runs(&pages), last, completed),
        (vec![(10, 19)], Some(19), false)
    );
}

/// A keyed read its budget cuts resumes after the last record it served:
/// paging through multi-record pages under a one-page budget, from inside
/// the first page, serves every record once and in order.
#[tokio::test]
async fn a_budgeted_keyed_read_resumes_after_its_last_served_record() {
    let db = three_pages("history-keyed-paging").await;
    let budget = page("", 0, 10).bytes.len();
    let (mut from, mut served, mut reads) = (3, Vec::new(), 0);
    loop {
        reads += 1;
        assert!(reads <= 6, "the paged read did not finish: {served:?}");
        let (pages, last, completed) = read_history2(&db, ROUTE, INC, from, 25, Some(""), budget)
            .await
            .unwrap();
        served.extend(pages.iter().flat_map(|p| p.first()..=p.last()));
        if completed {
            break;
        }
        from = last.expect("a partial read names where it stopped") + 1;
    }
    assert_eq!(served, (3..25).collect::<Vec<_>>());
}

/// Pages of "a" around a page of "b" whose ciphertext is garbled: admission
/// does not read ciphertext, so only opening it could refuse it.
async fn two_keys(path: &str) -> (Arc<Db>, SealedPage) {
    let db = partition(path).await;
    let mut other = page("b", 5, 5);
    let tag = other.bytes.len() - 1;
    other.bytes[tag] ^= 0xff;
    store(
        &db,
        &[
            ("a", page("a", 0, 5)),
            ("b", other),
            ("a", page("a", 10, 5)),
        ],
    )
    .await;
    (db, page("a", 0, 5))
}

/// A keyed history read serves only its key's pages, whichever path plans
/// it (the postings planner, or the envelope scan it falls back to), and
/// skips another key's page from its clear header; the skipped records
/// still count as consumed progress.
#[tokio::test]
async fn a_keyed_history_read_skips_other_keys_pages_as_progress() {
    let (db, first) = two_keys("history-keyed").await;
    let (pages, last, completed) = read_history2(&db, ROUTE, INC, 2, 13, Some("a"), 1 << 20)
        .await
        .unwrap();
    assert_eq!(
        (runs(&pages), last, completed),
        (vec![(2, 4), (10, 12)], Some(12), true)
    );
    let envelope =
        |from, to, budget| read_history2_keyed_envelope(&db, ROUTE, INC, "a", from, to, budget);
    let (pages, last, completed) = envelope(2, 13, 1 << 20).await.unwrap();
    assert_eq!(
        (runs(&pages), last, completed),
        (vec![(2, 4), (10, 12)], Some(12), true)
    );
    let (pages, last, completed) = envelope(2, 13, first.bytes.len()).await.unwrap();
    assert_eq!(
        (runs(&pages), last, completed),
        (vec![(2, 4)], Some(4), false)
    );
    let (pages, last, completed) = envelope(5, 9, 1 << 20).await.unwrap();
    assert_eq!((runs(&pages), last, completed), (vec![], Some(8), true));
    // A budget that ends on the skipped page still consumes it.
    let skipped = page("b", 5, 5).bytes.len();
    let (pages, last, completed) = envelope(5, 13, skipped).await.unwrap();
    assert_eq!((runs(&pages), last, completed), (vec![], Some(9), false));
    let (pages, last, _) = read_history2(&db, ROUTE, INC, 6, 9, Some("b"), 1 << 20)
        .await
        .unwrap();
    assert_eq!((runs(&pages), last), (vec![(6, 8)], Some(8)));
}

/// History pages ('g') and postings pages ('p') share an incarnation's
/// namespace: a page scan never returns or trips over a postings row, even
/// when its bound saturates at the end of the offset space, and a postings
/// scan never meets a page row.
#[tokio::test]
async fn history_pages_and_postings_rows_never_meet() {
    let db = three_pages("history-postings").await;
    let mut scan = PageScan::open(&db, ROUTE, INC, 0..u64::MAX, &Default::default())
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Some(slice) = scan.next().await.unwrap() {
        seen.push((slice.first(), slice.last()));
    }
    assert_eq!(seen, [(0, 9), (10, 19), (20, 24)]);
    let (pages, last, completed) = read_history2(&db, ROUTE, INC, 0, u64::MAX, None, 1 << 20)
        .await
        .unwrap();
    assert_eq!((pages.len(), last, completed), (25, Some(24), true));
    let kh = rk_hash("");
    let (lo, hi) = postings_range(ROUTE, INC, &kh, 0, u64::MAX);
    let mut postings = db.scan(lo..hi).await.unwrap();
    let mut decoded = 0;
    while let Some(row) = postings.next().await.unwrap() {
        let page = crate::postings::decode_stored_page(ROUTE, INC, &kh, &row.key, &row.value);
        assert!(page.is_some(), "a postings scan met a non-postings row");
        decoded += 1;
    }
    assert!(decoded > 0);
    let prefix = history_page_prefix(ROUTE, INC);
    let namespace = prefix.split_last().unwrap().1.to_vec();
    let mut all = db.scan_prefix(&namespace, ..).await.unwrap();
    let (mut pages, mut others) = (0, 0);
    while let Some(row) = all.next().await.unwrap() {
        let admitted = CheckedPage::from_row(&row.key, &prefix, row.value.clone());
        match row.key.get(32) {
            Some(b'g') => pages += usize::from(admitted.is_ok()),
            Some(b'p') => others += usize::from(admitted.is_err()),
            tag => panic!("unexpected history row tag {tag:?}"),
        }
    }
    assert_eq!((pages, others), (3, decoded));
}

/// Deterministic incompressible bytes, so pages hold what a test sizes.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Append one request of `records` under `rk`; returns its last offset.
async fn append(engine: &ShardEngine, hash: [u8; 16], rk: &str, records: &[Vec<u8>]) -> u64 {
    let (resp, answer) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: records.iter().cloned().map(bytes::Bytes::from).collect(),
        usage: crate::usage::counters(&hash),
        routing_key: rk.to_string(),
        key_hash: rk_hash(rk).0,
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: crate::crypto::derive_subkey(&KEY, &hash, rk, 0),
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        finish: crate::shard::AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp,
    };
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    answer.await.unwrap().unwrap().last_offset
}

/// One gather of `hash`, waited until the committer applied its advance;
/// returns the new absorbed boundary.
async fn absorb_once(engine: &ShardEngine, absorber: &Absorber, hash: [u8; 16]) -> u64 {
    let outcome = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    let upto = outcome.advanced.first().map(|advance| advance.1);
    let upto = upto.expect("the gather advanced");
    let handle = engine.stream_handle(hash).await.unwrap();
    for _ in 0..10_000 {
        if handle.state.lock().unwrap().durable.absorbed == upto {
            return upto;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("the advance to {upto} never applied");
}

/// What a durable read from `from` serves, as (offset, payload).
async fn read_from(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    from: u64,
    selector: Option<&str>,
) -> Result<Vec<(u64, Vec<u8>)>, String> {
    let handle = engine.stream_handle(hash).await.unwrap();
    let read = crate::application::read::read_merged(
        &KEY,
        &hash,
        &handle,
        engine,
        from,
        selector,
        8 << 20,
        Deliver::Durable,
    )
    .await?;
    assert!(read.completed, "the read ended early at {:?}", read.last);
    Ok(read
        .recs
        .iter()
        .map(|r| (r.off, r.payload.to_vec()))
        .collect())
}

/// The history pages of `hash`'s incarnation as (last offset, stored bytes).
async fn history_rows(engine: &ShardEngine, hash: [u8; 16]) -> Vec<(u64, bytes::Bytes)> {
    let (route, inc) = (RouteHash(hash), SegmentHash(hash));
    let part = engine.history_partition().await.unwrap();
    let prefix = history_page_prefix(route, inc);
    let mut rows = part.scan_prefix(&prefix, ..).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let page = CheckedPage::from_row(&row.key, &prefix, row.value.clone()).unwrap();
        out.push((page.last(), row.value));
    }
    out
}

/// A request split over several pages is absorbed whole: history holds the
/// shard log's pages byte for byte under their last offsets, the postings
/// note every record once, and reads starting inside a page serve exactly
/// the records from there, unfiltered and keyed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_page_request_is_absorbed_whole_and_read_from_inside_a_page() {
    let (engine, absorber, _store) = rig("history-multi-page").await;
    let hash = [0x61; 16];
    let big: Vec<_> = (0..3).map(|seed| noise(40 << 10, seed)).collect();
    let small: Vec<_> = (3..13).map(|offset| payload("", offset)).collect();
    assert_eq!(append(&engine, hash, "", &big).await, 2);
    assert_eq!(append(&engine, hash, "", &small).await, 12);
    let mut shard = Vec::new();
    for last in [0, 1, 2, 12] {
        let row = engine.db.get(shard_page_key(&hash, last)).await.unwrap();
        shard.push((last, row.expect("a shard-log page")));
    }
    assert_eq!(absorb_once(&engine, &absorber, hash).await, 13);
    assert_eq!(history_rows(&engine, hash).await, shard);
    let kh = rk_hash("");
    let (lo, hi) = postings_range(RouteHash(hash), SegmentHash(hash), &kh, 0, u64::MAX);
    let part = engine.history_partition().await.unwrap();
    let mut rows = part.scan(lo..hi).await.unwrap();
    let mut runs = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let page = crate::postings::decode_stored_page(
            RouteHash(hash),
            SegmentHash(hash),
            &kh,
            &row.key,
            &row.value,
        );
        crate::postings::append_page_runs(&mut runs, page.unwrap()).unwrap();
    }
    let noted: u64 = runs.iter().map(|run| u64::from(run.count)).sum();
    assert_eq!(noted, 13, "the postings note every record once");
    let expected: Vec<_> = (0u64..).zip(big.into_iter().chain(small)).collect();
    assert_eq!(read_from(&engine, hash, 0, None).await.unwrap(), expected);
    for from in [1, 7, 12] {
        let tail = expected.get(usize::try_from(from).unwrap()..).unwrap();
        assert_eq!(read_from(&engine, hash, from, None).await.unwrap(), tail);
        assert_eq!(
            read_from(&engine, hash, from, Some("")).await.unwrap(),
            tail
        );
    }
    engine.begin_close();
}

/// A gather whose byte cap is below one page still absorbs whole pages: the
/// absorbed boundary only ever lands on a page edge, and every absorbed
/// range reads back exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capped_gather_ends_the_absorbed_boundary_on_a_page_edge() {
    let (engine, _, _store) = rig("history-page-edges").await;
    let hash = [0x62; 16];
    let mut expected = Vec::new();
    for request in 0..4u64 {
        let records: Vec<_> = (0..5).map(|r| noise(1 << 10, request * 5 + r)).collect();
        assert_eq!(append(&engine, hash, "", &records).await, request * 5 + 4);
        expected.extend((request * 5..).zip(records));
    }
    let cfg = AbsorberConfig {
        gather_max_bytes: 1,
        ..Default::default()
    };
    let absorber = Absorber::new(engine.clone(), cfg);
    let mut boundaries = Vec::new();
    while boundaries.last() != Some(&20) {
        assert!(boundaries.len() < 4, "too many gathers: {boundaries:?}");
        boundaries.push(absorb_once(&engine, &absorber, hash).await);
        let lasts: Vec<_> = history_rows(&engine, hash)
            .await
            .into_iter()
            .map(|r| r.0)
            .collect();
        assert_eq!(
            lasts,
            (0..boundaries.len() as u64)
                .map(|p| p * 5 + 4)
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(boundaries, [5, 10, 15, 20]);
    assert_eq!(read_from(&engine, hash, 0, None).await.unwrap(), expected);
    assert_eq!(
        read_from(&engine, hash, 13, None).await.unwrap(),
        expected[13..]
    );
    engine.begin_close();
}

/// A keyed read of history opens only its own key's pages: with another
/// key's absorbed page garbled (only opening it could notice), the keyed
/// read serves its records exactly, while a read that must open the garbled
/// page refuses rather than serve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_history_read_never_opens_another_keys_page() {
    let (engine, absorber, _store) = rig("history-keyed-skip").await;
    let hash = [0x63; 16];
    let records = |rk: &str, first: u64| (first..first + 5).map(|o| payload(rk, o)).collect();
    let (a1, b, a2): (Vec<_>, Vec<_>, Vec<_>) =
        (records("a", 0), records("b", 5), records("a", 10));
    assert_eq!(append(&engine, hash, "a", &a1).await, 4);
    assert_eq!(append(&engine, hash, "b", &b).await, 9);
    assert_eq!(append(&engine, hash, "a", &a2).await, 14);
    assert_eq!(absorb_once(&engine, &absorber, hash).await, 15);
    let key = history_page_key(RouteHash(hash), SegmentHash(hash), 9);
    let part = engine.history_partition().await.unwrap();
    let mut garbled = part.get(&key).await.unwrap().unwrap().to_vec();
    let tag = garbled.len() - 1;
    garbled[tag] ^= 0xff;
    part.put(&key, garbled).await.unwrap();
    let expected: Vec<_> = (0..5).chain(10..15).zip(a1.into_iter().chain(a2)).collect();
    assert_eq!(
        read_from(&engine, hash, 0, Some("a")).await.unwrap(),
        expected
    );
    assert_eq!(
        read_from(&engine, hash, 3, Some("a")).await.unwrap(),
        expected[3..]
    );
    let refused = read_from(&engine, hash, 0, None).await.unwrap_err();
    assert!(refused.contains("did not open"), "{refused}");
    assert!(read_from(&engine, hash, 0, Some("b")).await.is_err());
    engine.begin_close();
}
