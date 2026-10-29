//! Layout 6 pages: a request's records are sealed into pages, every page of
//! a request lands in one write batch as one operand on the stream's pending
//! row, and the readers, the ring and the open work in offsets over them,
//! unsealed (in memory) and sealed (segment objects).
#![cfg(test)]
use super::record::{FrameReadResult, read_frames_until};
use super::{
    AppendAck, AppendErr, AppendFinish, AppendReq, CommitOp, Deliver, ShardConfig, ShardEngine,
    StreamHandle, TailFields, read_frames,
};
use crate::crypto_page::{CheckedPage, PageCipher, PageLane, stamped};
use crate::shard::seal::Trigger;
use crate::spill::pending::{concat_pages, parse_pages, pending_key};
use bytes::Bytes;
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::sync::oneshot;

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

/// An engine over `store` at `name`, as a restart opens it again; its
/// sealer is open and seals only when a test asks (`seal`).
async fn engine_on(store: Arc<dyn ObjectStore>, name: &str, ring_bytes: usize) -> Arc<ShardEngine> {
    let db = crate::keyspace::db_builder(name, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let maintenance = super::load_or_rebuild_maintenance(&db).await.unwrap();
    let engine = ShardEngine::start(
        name.into(),
        Arc::new(db),
        store,
        ShardConfig {
            tail_ring_bytes: ring_bytes,
            ..ShardConfig::default()
        },
        None,
        maintenance,
    );
    engine.configure_seal(|s| s.auto = false);
    engine.seal_ready().await.unwrap();
    engine
}

/// Seal every durable unsealed page of the engine's streams.
async fn seal(engine: &Arc<ShardEngine>) {
    engine.seal_once(Trigger::Requested).await.unwrap();
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

/// The pages the fixture stream's stored pending row holds, oldest first,
/// each as (last offset, page bytes).
async fn pending_pages(engine: &ShardEngine) -> Vec<(u64, Bytes)> {
    let raw = engine
        .db
        .get(pending_key(&HASH))
        .await
        .unwrap()
        .unwrap_or_default();
    parse_pages(&raw).unwrap()
}

/// The last offsets of `pages`.
fn lasts(pages: &[(u64, Bytes)]) -> Vec<u64> {
    pages.iter().map(|(last, _)| *last).collect()
}

/// Admit and open one stored page of the fixture stream.
fn open(last: u64, value: &Bytes) -> (CheckedPage, Vec<Opened>) {
    let page = CheckedPage::admit(value.clone(), last).unwrap();
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

/// A request's records are stored as pages, each named by its last offset
/// and held back to back in the stream's pending row: three 40 KiB records
/// cannot share a 64 KiB page, ten small records share one. Every page opens to exactly its request's records, the
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
    let rows = pending_pages(&engine).await;
    assert_eq!(lasts(&rows), [0, 1, 2, 12]);
    let expected: Vec<Vec<u8>> = large.iter().chain(&small).cloned().collect();
    let mut opened = Vec::new();
    for (last, value) in &rows {
        let (page, records) = open(*last, value);
        assert_eq!(page.routing_key(), LANE);
        opened.extend(records);
    }
    let offsets: Vec<u64> = opened.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(offsets, (0..13).collect::<Vec<u64>>());
    let payloads: Vec<Vec<u8>> = opened.into_iter().map(|(_, payload)| payload).collect();
    assert_eq!(payloads, expected);
    let handle = engine.stream_handle(HASH).await.unwrap();
    let stored: u64 = rows.iter().map(|(_, value)| value.len() as u64).sum();
    assert_eq!(applied(&handle).unabsorbed_bytes, stored);
    let large_pages: u64 = rows[..3].iter().map(|(_, page)| page.len() as u64).sum();
    let ordering = std::sync::atomic::Ordering::SeqCst;
    let meters = (
        usage.plaintext_bytes.load(ordering),
        usage.frame_bytes.load(ordering),
    );
    assert_eq!(meters, (3 * (40 << 10), large_pages));
    engine.begin_close();
}

/// A commit group counts the payload bytes of the records it appends, not
/// their stored page bytes: one request of a 100-byte and a 23-byte record
/// is a group of one request, two records and 123 bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_group_counts_its_records_payload_bytes() {
    let engine = engine("group-payload", 0).await;
    append(&engine, &[vec![1; 100], vec![2; 23]]).await.unwrap();
    let groups: Vec<(u32, u32, u64)> = engine
        .timings
        .lock()
        .unwrap()
        .iter()
        .map(|group| (group.reqs, group.records, group.bytes))
        .collect();
    assert_eq!(groups, [(1, 2, 123)]);
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
    engine.commit_group(group).await;
    let refused = tokio::time::timeout(ANSWER_WITHIN, refused_answer).await;
    assert!(matches!(
        refused.unwrap().unwrap(),
        Err(AppendErr::BadBody(_))
    ));
    let accepted = tokio::time::timeout(ANSWER_WITHIN, accepted_answer).await;
    assert_eq!(accepted.unwrap().unwrap().unwrap().last_offset, 0);
    let stored = lasts(&pending_pages(&engine).await);
    assert_eq!(stored, [0], "only the accepted request's page is stored");
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

/// How many records a read result's slices serve.
fn records_served(result: &FrameReadResult) -> u64 {
    result.frames.iter().map(|s| s.last() - s.first() + 1).sum()
}

/// `[from, to)` of `handle`'s stream, as a durable read with a 1 MiB budget
/// serves it.
async fn window(
    engine: &ShardEngine,
    handle: &StreamHandle,
    from: u64,
    to: u64,
) -> FrameReadResult {
    read_frames_until(engine, handle, from, to, None, 1 << 20, Deliver::Durable)
        .await
        .unwrap()
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
    assert!(pending_pages(&engine).await.is_empty());
    let handle = engine.stream_handle(HASH).await.unwrap();
    assert_eq!(
        (applied(&handle).next, applied(&handle).unabsorbed_bytes),
        (0, 0)
    );
    assert_eq!(handle.ring.lock().unwrap().bytes, 0);
    assert_eq!(append(&engine, &large).await.unwrap().last_offset, 2);
    assert_eq!(lasts(&pending_pages(&engine).await), [0, 1, 2]);
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

/// A scan that starts inside a sealed page returns that page sliced from
/// `from`: the records before `from` are neither served nor counted, from
/// the segment object and from the ring alike (the ring serves only below
/// the sealed boundary), and the decoded read resumes there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_starting_mid_page_serves_from_its_first_offset() {
    for ring in [0, 1 << 20] {
        let engine = engine("pages-mid-start", ring).await;
        two_pages(&engine).await;
        seal(&engine).await;
        let handle = engine.stream_handle(HASH).await.unwrap();
        let hits = engine.ring_hits.load(std::sync::atomic::Ordering::Relaxed);
        let range = window(&engine, &handle, 4, 20).await;
        let firsts: Vec<(u64, u64)> = range.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(firsts, [(4, 9), (10, 19)], "ring {ring}");
        assert_eq!((records_served(&range), range.last_offset), (16, Some(19)));
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
/// is `to - 1`, never a record past the window. The same from the unsealed
/// pages and, sealed, from the segment object and from the ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_ending_mid_page_stops_at_its_last_offset() {
    for (ring, sealed) in [(0, false), (1 << 20, false), (0, true), (1 << 20, true)] {
        let engine = engine("pages-mid-end", ring).await;
        two_pages(&engine).await;
        if sealed {
            seal(&engine).await;
        }
        let leg = format!("ring {ring} sealed {sealed}");
        let handle = engine.stream_handle(HASH).await.unwrap();
        let cut = window(&engine, &handle, 3, 12).await;
        let firsts: Vec<(u64, u64)> = cut.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(firsts, [(3, 9), (10, 11)], "{leg}");
        assert_eq!(
            (records_served(&cut), cut.last_offset),
            (9, Some(11)),
            "{leg}"
        );
        assert_eq!(served(&cut), expected(3..12), "{leg}");
        let edge = window(&engine, &handle, 0, 10).await;
        let firsts: Vec<(u64, u64)> = edge.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(
            firsts,
            [(0, 9)],
            "the page starting at `to` stays out ({leg})"
        );
        assert_eq!(edge.last_offset, Some(9), "{leg}");
        let until = read_frames_until(&engine, &handle, 0, 5, None, 1 << 20, Deliver::Applied)
            .await
            .unwrap();
        assert_eq!(
            (records_served(&until), until.last_offset),
            (5, Some(4)),
            "{leg}"
        );
        assert_eq!(served(&until), expected(0..5), "{leg}");
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
    let first_page = pending_pages(&engine).await[0].1.len();
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

/// After a restart the shard's ledger is rebuilt from the stored tails and
/// a stream's handle loads its pending pages, which must tile
/// `[absorbed, next)` page by page: with [0, 10) sealed, the ledger is the
/// bytes of the pages holding [10, 23), the handle holds exactly those
/// pages, and every record reads back once. A pending row missing page 20,
/// or holding an authentic page [11, 20] that overlaps the page [10, 19]
/// before it, fails the stream's load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_repair_counts_records_per_page_after_a_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let engine = engine_on(store.clone(), "pages-repair", 0).await;
    let all = records(20, 100);
    assert_eq!(append(&engine, &all[..10]).await.unwrap().last_offset, 9);
    seal(&engine).await;
    assert_eq!(append(&engine, &all[10..]).await.unwrap().last_offset, 19);
    append(&engine, &records(3, 40 << 10)).await.unwrap();
    let rows = pending_pages(&engine).await;
    assert_eq!(lasts(&rows), [19, 20, 21, 22]);
    let rest: u64 = rows.iter().map(|(_, page)| page.len() as u64).sum();
    let handle = engine.stream_handle(HASH).await.unwrap();
    let exact = applied(&handle);
    assert_eq!(
        (exact.absorbed, exact.next, exact.unabsorbed_bytes),
        (10, 23, rest)
    );
    stop(&engine).await;
    let reopened = engine_on(store.clone(), "pages-repair", 0).await;
    let rebuilt = super::load_or_rebuild_maintenance(&reopened.db)
        .await
        .unwrap();
    assert_eq!(rebuilt.unabsorbed_frame_bytes, rest);
    let handle = reopened.stream_handle(HASH).await.unwrap();
    let held: Vec<(u64, u64)> = handle
        .state
        .lock()
        .unwrap()
        .pending
        .from(0)
        .map(|(first, last, _)| (*first, *last))
        .collect();
    assert_eq!(held, [(10, 19), (20, 20), (21, 21), (22, 22)]);
    let read = window(&reopened, &handle, 0, 23).await;
    let offsets: Vec<u64> = read
        .frames
        .iter()
        .flat_map(|s| s.first()..=s.last())
        .collect();
    assert_eq!(offsets, (0..23).collect::<Vec<u64>>());
    drop(handle);
    stop(&reopened).await;
    let hole = concat_pages([&rows[0].1, &rows[2].1, &rows[3].1]);
    let refused = planted_load_error(&store, "pages-repair", hole).await;
    assert!(
        refused.contains("offset 20 due, page [21, 21]"),
        "{refused}"
    );
    let lane = PageLane {
        key_version: 1,
        routing_key: LANE,
    };
    let overlap = PageCipher::new(&subkey(), &HASH)
        .seal(&lane, 11, &stamped(1, &records(10, 100)))
        .unwrap();
    let overlapping = concat_pages([&rows[0].1, &Bytes::from(overlap.bytes)]);
    let refused = planted_load_error(&store, "pages-repair", overlapping).await;
    assert!(
        refused.contains("offset 20 due, page [11, 20]"),
        "{refused}"
    );
}

/// Stop `engine` and wait until it has terminated.
async fn stop(engine: &ShardEngine) {
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}

/// Plant `pending` as the fixture stream's pending row in the log at
/// `name`, open an engine there and return why the stream's load fails.
async fn planted_load_error(store: &Arc<dyn ObjectStore>, name: &str, pending: Bytes) -> String {
    let db = crate::keyspace::db_builder(name, store.clone())
        .build()
        .await
        .unwrap();
    db.put(pending_key(&HASH), pending).await.unwrap();
    db.close().await.unwrap();
    let engine = engine_on(store.clone(), name, 0).await;
    let refused = engine
        .stream_handle(HASH)
        .await
        .err()
        .map(|e| e.to_string());
    stop(&engine).await;
    refused.expect("a pending row that does not tile was loaded")
}

/// A commit that stores no page of a stream publishes nothing to its ring:
/// after a seal's commit (its directory rows and the pending row's
/// remainder, no page) turns durable, the ring holds the two batches the
/// appends published, with their bytes, and the publish count is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_that_stores_no_page_publishes_nothing_to_the_ring() {
    let engine = engine("pages-ring-quiet", 1 << 20).await;
    two_pages(&engine).await;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let rows = pending_pages(&engine).await;
    let stored: usize = rows.iter().map(|(_, page)| page.len()).sum();
    let ring = || {
        let ring = handle.ring.lock().unwrap();
        let batches: Vec<(u64, u64, usize)> = ring
            .batches
            .iter()
            .map(|batch| (batch.first, batch.next, batch.bytes))
            .collect();
        let published = engine
            .ring_published
            .load(std::sync::atomic::Ordering::Relaxed);
        (batches, ring.bytes, published, engine.ring_resident_bytes())
    };
    let pages = vec![(0, 10, rows[0].1.len()), (10, 20, rows[1].1.len())];
    let published = (pages, stored, 2, stored as u64);
    assert_eq!(ring(), published);
    // A group's ring publications run when it turns durable, before its
    // durable tail moves; a seal returns once its tails are durable.
    let report = engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!((report.pages, report.page_bytes), (2, stored as u64));
    assert_eq!(handle.state.lock().unwrap().durable.absorbed, 20);
    assert_eq!(ring(), published, "the seal published to the ring");
    engine.begin_close();
}

/// The ring serves only the sealed part of a window, at either visibility:
/// before the seal a durable and an applied read of a window the ring holds
/// are both served from the unsealed pages, without a ring hit; after it
/// each is served from the ring, one hit each; all serve the same records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_sealed_window_is_served_from_the_ring() {
    let engine = engine("pages-ring-sealed", 1 << 20).await;
    two_pages(&engine).await;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let hits = || engine.ring_hits.load(std::sync::atomic::Ordering::Relaxed);
    for sealed in [false, true] {
        if sealed {
            seal(&engine).await;
        }
        for deliver in [Deliver::Durable, Deliver::Applied] {
            let before = hits();
            let read = read_frames(&engine, &handle, 4, None, 1 << 20, deliver)
                .await
                .unwrap();
            assert_eq!(
                served(&read),
                expected(4..20),
                "{deliver:?} sealed {sealed}"
            );
            assert_eq!(
                hits() - before,
                u64::from(sealed),
                "{deliver:?} sealed {sealed}"
            );
        }
    }
    engine.begin_close();
}

/// The read bounds at the top of the offset space. A stream's `next`
/// cannot pass u64::MAX, so its last page ends at u64::MAX - 1 at most:
/// windows ending at u64::MAX, starting inside, at the first and at the
/// last record of the top pages, are served exactly, from the unsealed
/// pages and, once sealed, from the segment object.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scans_at_the_top_of_the_offset_space_are_exact() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cipher = PageCipher::new(&subkey(), &HASH);
    let lane = PageLane {
        key_version: 1,
        routing_key: LANE,
    };
    let top = u64::MAX - 1;
    let pages: Vec<Bytes> = [top - 29, top - 19, top - 9]
        .into_iter()
        .map(|first| {
            let page = cipher
                .seal(&lane, first, &stamped(1, &records(10, 8)))
                .unwrap();
            Bytes::from(page.bytes)
        })
        .collect();
    let tail = TailFields {
        next: u64::MAX,
        absorbed: top - 29,
        unabsorbed_bytes: pages.iter().map(|p| p.len() as u64).sum(),
        ..Default::default()
    };
    let db = crate::keyspace::db_builder("pages-u64", store.clone())
        .build()
        .await
        .unwrap();
    let mut batch = slatedb::WriteBatch::new();
    batch.put(super::tail_key(&HASH), super::encode_tail(&tail));
    batch.put(pending_key(&HASH), concat_pages(&pages));
    db.write(batch).await.unwrap();
    db.close().await.unwrap();
    let engine = engine_on(store, "pages-u64", 0).await;
    let handle = engine.stream_handle(HASH).await.unwrap();
    let windows = [
        (top - 29, u64::MAX, top - 29, top),
        (top - 15, u64::MAX, top - 15, top),
        (top, u64::MAX, top, top),
        (top - 9, top, top - 9, top - 1),
        (top - 20, top - 19, top - 20, top - 20),
        (top - 19, top - 18, top - 19, top - 19),
    ];
    for sealed in [false, true] {
        if sealed {
            seal(&engine).await;
            assert_eq!(handle.state.lock().unwrap().applied.absorbed, u64::MAX);
        }
        for (from, to, first, last) in windows {
            let got = read_frames_until(
                &engine,
                &handle,
                from,
                to,
                None,
                usize::MAX,
                Deliver::Durable,
            )
            .await
            .unwrap();
            let served: Vec<u64> = served(&got).iter().map(|(offset, _)| *offset).collect();
            let leg = format!("[{from}, {to}) sealed {sealed}");
            assert_eq!(served, (first..=last).collect::<Vec<u64>>(), "{leg}");
            assert_eq!(got.last_offset, Some(last), "{leg}");
        }
    }
    engine.begin_close();
}

/// G4 across a crash: a request cut into three pages is one write batch,
/// so it reaches the store in one WAL object or not at all. With that
/// object's PUT parked, a replacement owner opened over the same store
/// (the crash: the PUT has not landed when the new owner fences the old
/// one) finds none of the request's pages, the stream's tail where it was,
/// and the old owner's answer is no acknowledgement; released before the
/// replacement opens, the request is acknowledged and the replacement finds
/// all three pages and the tail past them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_page_request_is_all_or_nothing_across_a_crash() {
    use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
    for lands in [false, true] {
        let path = if lands {
            "pages-crash-lands"
        } else {
            "pages-crash-lost"
        };
        let store = FaultStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            7,
            FaultProfile::clean(),
        );
        let engine = engine_on(store.clone(), path, 0).await;
        assert_eq!(
            append(&engine, &records(1, 10)).await.unwrap().last_offset,
            0
        );
        let engaged = store.hold_class(StoreOp::Put, ObjClass::Wal, 1);
        let (req, answer) = request(&records(3, 40 << 10));
        assert!(engine.try_enqueue(req).is_ok(), "enqueue");
        let deadline = std::time::Instant::now() + ANSWER_WITHIN;
        while engaged.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the WAL PUT never parked"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let unanswered = if lands {
            store.release_hold();
            let ack = tokio::time::timeout(ANSWER_WITHIN, answer).await.unwrap();
            assert_eq!(ack.unwrap().unwrap().last_offset, 3);
            None
        } else {
            Some(answer)
        };
        let replacement = engine_on(store.clone(), path, 0).await;
        let stored = lasts(&pending_pages(&replacement).await);
        let handle = replacement.stream_handle(HASH).await.unwrap();
        let next = handle.state.lock().unwrap().durable.next;
        if let Some(answer) = unanswered {
            assert_eq!((stored, next), (vec![0], 1), "no page landed");
            store.release_hold();
            let answered = tokio::time::timeout(ANSWER_WITHIN, answer).await.unwrap();
            assert!(
                !matches!(answered, Ok(Ok(_))),
                "a fenced owner acknowledged a request no replacement holds"
            );
            let stored = lasts(&pending_pages(&replacement).await);
            assert_eq!(stored, [0], "the fenced write never lands");
        } else {
            assert_eq!((stored, next), (vec![0, 1, 2, 3], 4), "every page landed");
        }
        replacement.begin_close();
        engine.begin_close();
    }
}
