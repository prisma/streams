//! Layout 5 shard log: a request's records are sealed into pages keyed by
//! their last offset, every page of a request lands in one write batch, and
//! the readers, the ring, tail repair and trims work in offsets over them.
#![cfg(test)]
use super::{
    AppendAck, AppendErr, AppendFinish, AppendReq, CommitOp, CopiedBytes, Deliver, ShardConfig,
    ShardEngine, StreamHandle, TailFields, read_frames, read_frames_range,
};
use crate::crypto_page::{CheckedPage, PageCipher, SHARD_PAGE_TAG, shard_page_key};
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::Db;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const HASH: [u8; 16] = [0x51; 16];
const LANE: &str = "lane";
const ANSWER_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);
type Answer = oneshot::Receiver<Result<AppendAck, AppendErr>>;

fn subkey() -> [u8; 32] {
    crate::crypto::derive_subkey(&crate::crypto::StreamKey([7; 32]), &HASH, LANE, 1)
}

/// One opened record: its offset and payload.
type Opened = (u64, Vec<u8>);

/// An engine over an in-memory store whose ring holds `ring_bytes`.
async fn engine(name: &str, ring_bytes: usize) -> Arc<ShardEngine> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    engine_on(store, name, ring_bytes).await
}

/// An engine over `store` at `name`, as a restart opens it again.
async fn engine_on(store: Arc<dyn ObjectStore>, name: &str, ring_bytes: usize) -> Arc<ShardEngine> {
    let db = Db::builder(name, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let (tx, _signals) = mpsc::channel(1);
    ShardEngine::start(
        name.into(),
        Arc::new(db),
        store,
        ShardConfig {
            tail_ring_bytes: ring_bytes,
            ..ShardConfig::default()
        },
        tx,
        None,
        Default::default(),
    )
}

/// One request of `records` under the fixture lane; the receiver is its
/// answer.
fn request(records: &[Vec<u8>]) -> (AppendReq, Answer) {
    let (resp, answer) = oneshot::channel();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash: HASH,
        route: HASH,
        entries: records.iter().cloned().map(Bytes::from).collect(),
        usage: crate::usage::counters(&HASH),
        routing_key: LANE.into(),
        key_hash: [7; 16],
        producer_lineage: Vec::new(),
        key_version: 1,
        subkey: subkey(),
        ts_hint_ms: Some(1_760_000_000_000),
        seq: None,
        bytes: records.iter().map(Vec::len).sum(),
        finish: AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp,
    };
    (req, answer)
}

/// Append `records` as one request and wait for its answer.
async fn append(engine: &ShardEngine, records: &[Vec<u8>]) -> Result<AppendAck, AppendErr> {
    let (req, answer) = request(records);
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    tokio::time::timeout(ANSWER_WITHIN, answer)
        .await
        .unwrap()
        .unwrap()
}

/// Append `records` as one request metered on counters of its own; the
/// counters once it is answered.
async fn append_metered(
    engine: &ShardEngine,
    records: &[Vec<u8>],
) -> (Result<AppendAck, AppendErr>, Arc<crate::usage::Counters>) {
    let (mut req, answer) = request(records);
    let usage: Arc<crate::usage::Counters> = Arc::new(Default::default());
    req.usage = usage.clone();
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    let answer = tokio::time::timeout(ANSWER_WITHIN, answer).await.unwrap();
    (answer.unwrap(), usage)
}

/// `n` records of `size` bytes, each filled with its own index.
fn records(n: usize, size: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| vec![u8::try_from(i % 251).unwrap(); size])
        .collect()
}

/// Every record row of the fixture stream: its tag, the offset its key
/// names and its value.
async fn record_rows(engine: &ShardEngine) -> Vec<(u8, u64, Bytes)> {
    let mut scan = engine.db.scan_prefix(HASH, ..).await.unwrap();
    let mut rows = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        let Some((namespace, offset)) = row.key.split_last_chunk::<8>() else {
            continue;
        };
        if let [.., tag @ (b'r' | b'p')] = namespace
            && namespace.len() == 17
        {
            rows.push((*tag, u64::from_be_bytes(*offset), row.value));
        }
    }
    rows
}

/// Admit and open one stored page row of the fixture stream.
fn open(last: u64, value: &Bytes) -> (CheckedPage, Vec<Opened>) {
    let key = shard_page_key(&HASH, last);
    let prefix = [HASH.as_slice(), &[SHARD_PAGE_TAG]].concat();
    let page = CheckedPage::from_row(&key, &prefix, value.clone()).unwrap();
    let opened = PageCipher::new(&subkey(), &HASH).open(&page).unwrap();
    let records = opened
        .records()
        .map(|record| (record.offset, record.payload.to_vec()))
        .collect();
    (page, records)
}

fn applied(handle: &StreamHandle) -> TailFields {
    handle.state.lock().unwrap().applied.clone()
}

/// A request's records are stored as pages keyed by their last offset, one
/// row per page: three 40 KiB records cannot share a 64 KiB page, ten small
/// records share one. Every page opens to exactly its request's records, the
/// tail's unabsorbed gauge and the stored-byte meter are the sum of the page
/// bytes, and ingest stays metered on payload bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_is_stored_as_pages_keyed_by_their_last_offset() {
    let engine = engine("pages-stored", 0).await;
    let large = records(3, 40 << 10);
    let small = records(10, 100);
    let (ack, usage) = append_metered(&engine, &large).await;
    assert_eq!(ack.unwrap().last_offset, 2);
    assert_eq!(append(&engine, &small).await.unwrap().last_offset, 12);
    let rows = record_rows(&engine).await;
    let keys: Vec<(u8, u64)> = rows.iter().map(|(tag, last, _)| (*tag, *last)).collect();
    assert_eq!(keys, [(b'p', 0), (b'p', 1), (b'p', 2), (b'p', 12)]);
    let expected: Vec<Vec<u8>> = large.iter().chain(&small).cloned().collect();
    let mut opened = Vec::new();
    for (_, last, value) in &rows {
        let (page, records) = open(*last, value);
        assert_eq!(page.routing_key(), LANE);
        opened.extend(records);
    }
    let offsets: Vec<u64> = opened.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(offsets, (0..13).collect::<Vec<u64>>());
    let payloads: Vec<Vec<u8>> = opened.into_iter().map(|(_, payload)| payload).collect();
    assert_eq!(payloads, expected);
    let handle = engine.stream_handle(HASH).await.unwrap();
    let stored: u64 = rows.iter().map(|(_, _, value)| value.len() as u64).sum();
    assert_eq!(applied(&handle).unabsorbed_bytes, stored);
    let large_pages: u64 = rows[..3].iter().map(|(_, _, page)| page.len() as u64).sum();
    let ordering = std::sync::atomic::Ordering::SeqCst;
    let meters = (
        usage.plaintext_bytes.load(ordering),
        usage.frame_bytes.load(ordering),
    );
    assert_eq!(meters, (3 * (40 << 10), large_pages));
    engine.begin_close();
}

/// A request whose pages refuse to seal stages nothing, even in a group that
/// writes: no page, no sequence row, no advance; it is refused as a bad
/// body, and the request committed beside it takes its offsets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_that_cannot_be_sealed_stages_nothing() {
    let engine = engine("pages-refused", 0).await;
    let oversized = vec![
        vec![0u8; 8],
        vec![0u8; crate::crypto::MAX_RECORD_PLAINTEXT + 1],
    ];
    let (mut refused, refused_answer) = request(&oversized);
    refused.seq = Some("s1".into());
    let (accepted, accepted_answer) = request(&records(1, 100));
    let group = vec![CommitOp::Append(refused), CommitOp::Append(accepted)];
    engine.commit_group(group, &ShardConfig::default()).await;
    let refused = tokio::time::timeout(ANSWER_WITHIN, refused_answer).await;
    assert!(matches!(
        refused.unwrap().unwrap(),
        Err(AppendErr::BadBody(_))
    ));
    let accepted = tokio::time::timeout(ANSWER_WITHIN, accepted_answer).await;
    assert_eq!(accepted.unwrap().unwrap().unwrap().last_offset, 0);
    let lasts: Vec<u64> = record_rows(&engine).await.iter().map(|r| r.1).collect();
    assert_eq!(lasts, [0], "only the accepted request's page is stored");
    let seq = engine
        .db
        .get(super::seq_key(&HASH, &[7; 16]))
        .await
        .unwrap();
    assert_eq!(seq, None, "the refused request staged its sequence row");
    let handle = engine.stream_handle(HASH).await.unwrap();
    assert_eq!((applied(&handle).next, applied(&handle).seq), (1, None));
    engine.begin_close();
}

/// The offsets and payloads a read result's slices serve, opened.
fn served(result: &super::FrameReadResult) -> Vec<Opened> {
    let cipher = PageCipher::new(&subkey(), &HASH);
    let mut out = Vec::new();
    for slice in &result.frames {
        let opened = cipher.open(slice.page()).unwrap();
        let records = opened
            .records()
            .filter(|record| (slice.first()..=slice.last()).contains(&record.offset));
        out.extend(records.map(|record| (record.offset, record.payload.to_vec())));
    }
    out
}

/// The fixture's records `range`, as `records(20, 100)` numbers them.
fn expected(range: std::ops::Range<u64>) -> Vec<Opened> {
    let all = records(20, 100);
    range
        .map(|offset| (offset, all[usize::try_from(offset).unwrap()].clone()))
        .collect()
}

/// Two requests of ten 100-byte records: pages [0, 9] and [10, 19].
async fn two_pages(engine: &ShardEngine) {
    let all = records(20, 100);
    assert_eq!(append(engine, &all[..10]).await.unwrap().last_offset, 9);
    assert_eq!(append(engine, &all[10..]).await.unwrap().last_offset, 19);
}

/// A request split over several pages is atomic: when its group's write
/// fails, none of its pages is stored, the stream does not advance and no
/// page reaches the ring; the retried request stores every page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_split_over_several_pages_is_atomic() {
    let engine = engine("pages-atomic", 1 << 20).await;
    let large = records(3, 40 << 10);
    engine.fail_next_group_for(HASH);
    assert!(matches!(
        append(&engine, &large).await,
        Err(AppendErr::Internal(_))
    ));
    assert!(record_rows(&engine).await.is_empty());
    let handle = engine.stream_handle(HASH).await.unwrap();
    assert_eq!(
        (applied(&handle).next, applied(&handle).unabsorbed_bytes),
        (0, 0)
    );
    assert_eq!(handle.ring.lock().unwrap().bytes, 0);
    assert_eq!(append(&engine, &large).await.unwrap().last_offset, 2);
    let rows = record_rows(&engine).await;
    let keys: Vec<(u8, u64)> = rows.iter().map(|(tag, last, _)| (*tag, *last)).collect();
    assert_eq!(keys, [(b'p', 0), (b'p', 1), (b'p', 2)]);
    let ring = handle.ring.lock().unwrap();
    let ring_pages: Vec<u64> = ring
        .batches
        .iter()
        .flat_map(|b| &b.frames)
        .map(|p| p.0)
        .collect();
    assert_eq!(
        ring_pages,
        [0, 1, 2],
        "the request's pages publish together"
    );
    drop(ring);
    engine.begin_close();
}

/// A scan that starts inside a page returns that page sliced from `from`:
/// the records before `from` are neither served nor counted, from the store
/// and from the ring alike, and the decoded read resumes there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_starting_mid_page_serves_from_its_first_offset() {
    for ring in [0, 1 << 20] {
        let engine = engine("pages-mid-start", ring).await;
        two_pages(&engine).await;
        let handle = engine.stream_handle(HASH).await.unwrap();
        let hits = engine.ring_hits.load(std::sync::atomic::Ordering::Relaxed);
        let range = read_frames_range(&engine, &handle, 4, 20, 1 << 20)
            .await
            .unwrap();
        let firsts: Vec<(u64, u64)> = range.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(firsts, [(4, 9), (10, 19)], "ring {ring}");
        assert_eq!((range.frames.len(), range.last_offset), (16, Some(19)));
        assert_eq!(range.frames[0].view().header.offset, 4);
        let pages: Vec<(u64, u64)> = range
            .frames
            .iter()
            .map(|s| (s.page().first(), s.page().last()))
            .collect();
        assert_eq!(
            pages,
            [(0, 9), (10, 19)],
            "the first page is cut, the second whole"
        );
        assert_eq!(served(&range), expected(4..20));
        let hit = engine.ring_hits.load(std::sync::atomic::Ordering::Relaxed) > hits;
        assert_eq!(
            hit,
            ring > 0,
            "the ring serves a window that starts inside a page"
        );
        let tail = read_frames(&engine, &handle, 7, None, 1 << 20, Deliver::Durable)
            .await
            .unwrap();
        assert_eq!(served(&tail), expected(7..20));
        let key = crate::crypto::StreamKey([7; 32]);
        let page = crate::application::read::read_merged(
            &key,
            &HASH,
            &handle,
            &engine,
            7,
            None,
            1 << 20,
            Deliver::Durable,
        )
        .await
        .unwrap();
        let offsets: Vec<u64> = page.recs.iter().map(|record| record.off).collect();
        assert_eq!(offsets, (7..20).collect::<Vec<u64>>());
        assert!(page.completed);
        engine.begin_close();
    }
}

/// A scan that ends inside a page returns that page sliced to `to - 1`, and
/// a page that starts at `to` is not returned at all: the consumed progress
/// is `to - 1`, never a record past the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_ending_mid_page_stops_at_its_last_offset() {
    for ring in [0, 1 << 20] {
        let engine = engine("pages-mid-end", ring).await;
        two_pages(&engine).await;
        let handle = engine.stream_handle(HASH).await.unwrap();
        let cut = read_frames_range(&engine, &handle, 3, 12, 1 << 20)
            .await
            .unwrap();
        let firsts: Vec<(u64, u64)> = cut.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(firsts, [(3, 9), (10, 11)], "ring {ring}");
        assert_eq!((cut.frames.len(), cut.last_offset), (9, Some(11)));
        assert_eq!(served(&cut), expected(3..12));
        let edge = read_frames_range(&engine, &handle, 0, 10, 1 << 20)
            .await
            .unwrap();
        let firsts: Vec<(u64, u64)> = edge.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(firsts, [(0, 9)], "the page starting at `to` stays out");
        assert_eq!(edge.last_offset, Some(9));
        let until = super::record::read_frames_until(
            &engine,
            &handle,
            0,
            5,
            None,
            1 << 20,
            Deliver::Applied,
        )
        .await
        .unwrap();
        assert_eq!((until.frames.len(), until.last_offset), (5, Some(4)));
        assert_eq!(served(&until), expected(0..5));
        engine.begin_close();
    }
}

/// A keyed scan skips another lane's page from its clear header without
/// decrypting it, and still counts its records as consumed progress; the
/// byte budget ends the scan after the page that reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_scan_skips_other_lanes_pages_and_a_budget_ends_on_a_page() {
    let engine = engine("pages-keyed", 0).await;
    let all = records(20, 100);
    append(&engine, &all[..10]).await.unwrap();
    let (mut other, answer) = request(&all[10..15]);
    other.routing_key = "other".into();
    other.subkey =
        crate::crypto::derive_subkey(&crate::crypto::StreamKey([7; 32]), &HASH, "other", 1);
    assert!(engine.try_enqueue(other).is_ok());
    answer.await.unwrap().unwrap();
    append(&engine, &all[15..]).await.unwrap();
    let handle = engine.stream_handle(HASH).await.unwrap();
    let keyed = read_frames(&engine, &handle, 2, Some(LANE), 1 << 20, Deliver::Durable)
        .await
        .unwrap();
    let firsts: Vec<(u64, u64)> = keyed.frames.iter().map(|s| (s.first(), s.last())).collect();
    assert_eq!(firsts, [(2, 9), (15, 19)]);
    assert_eq!(keyed.last_offset, Some(19));
    let mut want = expected(2..10);
    want.extend(expected(15..20));
    assert_eq!(served(&keyed), want);
    let until = super::record::read_frames_until(
        &engine,
        &handle,
        2,
        15,
        Some(LANE),
        1 << 20,
        Deliver::Durable,
    )
    .await
    .unwrap();
    let firsts: Vec<(u64, u64)> = until.frames.iter().map(|s| (s.first(), s.last())).collect();
    assert_eq!(firsts, [(2, 9)]);
    assert_eq!(
        until.last_offset,
        Some(14),
        "the skipped page's records are consumed"
    );
    let first_page = record_rows(&engine).await[0].2.len();
    let budget = read_frames(&engine, &handle, 2, None, first_page, Deliver::Durable)
        .await
        .unwrap();
    let firsts: Vec<(u64, u64)> = budget
        .frames
        .iter()
        .map(|s| (s.first(), s.last()))
        .collect();
    assert_eq!(
        firsts,
        [(2, 9)],
        "the page that reaches the budget ends the scan"
    );
    assert_eq!(budget.last_offset, Some(9));
    let over = read_frames(&engine, &handle, 2, None, first_page + 1, Deliver::Durable)
        .await
        .unwrap();
    let firsts: Vec<(u64, u64)> = over.frames.iter().map(|s| (s.first(), s.last())).collect();
    assert_eq!(
        firsts,
        [(2, 9), (10, 14)],
        "the page that passes the budget is kept"
    );
    assert_eq!(over.last_offset, Some(14));
    engine.begin_close();
}

/// A read budget that ends inside a page stops at the last admitted
/// record, and the next read resumes inside the same page: every record is
/// served exactly once and in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_budget_cut_inside_a_page_resumes_there() {
    let engine = engine("pages-budget", 1 << 20).await;
    two_pages(&engine).await;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let key = crate::crypto::StreamKey([7; 32]);
    let (mut cursor, mut seen) = (0, Vec::new());
    while cursor < 20 {
        let page = crate::application::read::read_merged(
            &key,
            &HASH,
            &handle,
            &engine,
            cursor,
            None,
            250,
            Deliver::Durable,
        )
        .await
        .unwrap();
        assert!(page.recs.len() <= 2, "250 bytes hold two 100-byte records");
        seen.extend(
            page.recs
                .iter()
                .map(|record| (record.off, record.payload.to_vec())),
        );
        let next = page.scanned_through(cursor);
        assert!(next > cursor, "a read made no progress at {cursor}");
        cursor = next;
    }
    assert_eq!(seen, expected(0..20));
    engine.begin_close();
}

/// Tail repair (R26-4) after a restart counts records per page: a tail
/// whose exact gauge is missing is repaired to the bytes of the pages that
/// hold [absorbed, next), and a missing page fails the open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_repair_counts_records_per_page_after_a_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let engine = engine_on(store.clone(), "pages-repair", 0).await;
    two_pages(&engine).await;
    append(&engine, &records(3, 40 << 10)).await.unwrap();
    let rows = record_rows(&engine).await;
    let lasts: Vec<u64> = rows.iter().map(|(_, last, _)| *last).collect();
    assert_eq!(lasts, [9, 19, 20, 21, 22]);
    let first = rows[0].2.len() as u64;
    let rest: u64 = rows[1..].iter().map(|(_, _, page)| page.len() as u64).sum();
    let advance = CommitOp::Absorbed {
        hash: HASH,
        upto: 10,
        bytes: CopiedBytes::new(0, first),
        v2: true,
    };
    engine
        .commit_group(vec![advance], &ShardConfig::default())
        .await;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let exact = applied(&handle);
    assert_eq!(
        (exact.absorbed, exact.next, exact.unabsorbed_bytes),
        (10, 23, rest)
    );
    let mut downgrade = slatedb::WriteBatch::new();
    downgrade.put(
        super::tail_key(&HASH),
        super::encode_tail_without_gauge_for_tests(&exact),
    );
    downgrade.delete(super::shard_maint_key());
    engine.db.write(downgrade).await.unwrap();
    engine.db.flush().await.unwrap();
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let db = Db::builder("pages-repair", store.clone())
        .build()
        .await
        .unwrap();
    let rebuilt = super::load_or_rebuild_maintenance(&db).await.unwrap();
    assert_eq!(rebuilt.unabsorbed_frame_bytes, rest);
    let tail = db.get(super::tail_key(&HASH)).await.unwrap().unwrap();
    let repaired = super::decode_tail_for_tests(&tail).unwrap();
    assert_eq!(
        (repaired.absorbed, repaired.next, repaired.unabsorbed_bytes),
        (10, 23, rest)
    );
    let mut hole = slatedb::WriteBatch::new();
    hole.put(
        super::tail_key(&HASH),
        super::encode_tail_without_gauge_for_tests(&exact),
    );
    hole.delete(super::shard_maint_key());
    hole.delete(shard_page_key(&HASH, 20));
    db.write(hole).await.unwrap();
    let refused = super::load_or_rebuild_maintenance(&db).await.unwrap_err();
    assert_eq!(
        refused.to_string(),
        "tail repair found a page [21, 21] where offset 20 was due"
    );
    db.close().await.unwrap();
}

/// A trim deletes a shard-log page only once its LAST offset is below the
/// trim point: a budget that stops inside an absorbed page keeps the page,
/// and a page holding an unabsorbed record is never deleted, even when the
/// trim point reaches into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trim_never_deletes_a_page_holding_an_unabsorbed_record() {
    let engine = engine("pages-trim", 0).await;
    two_pages(&engine).await;
    let rows = record_rows(&engine).await;
    let (first, second) = (rows[0].2.len() as u64, rows[1].2.len() as u64);
    let lasts = |rows: Vec<(u8, u64, Bytes)>| rows.iter().map(|r| r.1).collect::<Vec<u64>>();
    let budget = ShardConfig {
        max_trim_per_op: 3,
        ..ShardConfig::default()
    };
    let advance = |from, upto, len| CommitOp::Absorbed {
        hash: HASH,
        upto,
        bytes: CopiedBytes::new(from, len),
        v2: true,
    };
    // Absorbed to 12, inside the second page; the trim point then reaches
    // 12 as well, but records 12..=19 of that page are not absorbed.
    engine
        .commit_group(vec![advance(0, 12, first)], &budget)
        .await;
    let handle = engine.stream_handle(HASH).await.unwrap();
    engine.commit_group(vec![advance(12, 15, 1)], &budget).await;
    assert_eq!(
        (applied(&handle).trim_safe_to, applied(&handle).trimmed),
        (12, 3)
    );
    assert_eq!(
        lasts(record_rows(&engine).await),
        [9, 19],
        "3 offsets trimmed, no page"
    );
    for trimmed in [6, 9] {
        engine.commit_group(vec![CommitOp::TrimTick], &budget).await;
        assert_eq!(applied(&handle).trimmed, trimmed);
        assert_eq!(
            lasts(record_rows(&engine).await),
            [9, 19],
            "a trim to {trimmed} keeps the page holding record 9"
        );
    }
    engine.commit_group(vec![CommitOp::TrimTick], &budget).await;
    assert_eq!(applied(&handle).trimmed, 12);
    assert_eq!(
        lasts(record_rows(&engine).await),
        [19],
        "the first page goes once the trim passes its last offset; the second holds 15..=19"
    );
    let tail = applied(&handle);
    assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (15, second - 1));
    engine.begin_close();
}
