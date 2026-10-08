//! Layout 5 page reads under concurrent absorption and trims, across a page
//! of the full 4,096 records, and at the TLA-018-F1 frontier with a cursor
//! inside a page: every pager serves every record exactly once, in order
//! (from the G3 correctness review of 2026-10-08).
#![cfg(test)]
use crate::application::read::read_merged;
use crate::crypto::StreamKey;
use crate::history::{Absorber, AbsorberConfig};
use crate::shard::{Deliver, ShardConfig, ShardEngine};
use slatedb::Db;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

const KEY: StreamKey = StreamKey([7; 32]);
const HASH: [u8; 16] = [0x6b; 16];

/// One served record: offset, routing key, payload.
type Served = (u64, String, Vec<u8>);

/// Record `offset`'s payload under `rk`: compressible text of `size` bytes
/// that names its own offset.
fn payload(rk: &str, offset: u64, size: usize) -> Vec<u8> {
    let mut bytes = format!("{{\"key\":\"{rk}\",\"offset\":{offset},\"pad\":\"").into_bytes();
    bytes.resize(size.max(bytes.len()), b'x');
    bytes
}

/// A nine-byte record, so 4,096 of them fit one 64 KiB page body.
fn tiny(rk: &str, offset: u64) -> Vec<u8> {
    let mut bytes = rk.as_bytes().get(..1).unwrap_or_default().to_vec();
    bytes.extend_from_slice(&offset.to_be_bytes());
    bytes
}

async fn append(engine: &ShardEngine, rk: &str, records: Vec<Vec<u8>>) -> u64 {
    let (resp, answer) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash: HASH,
        route: HASH,
        entries: records.into_iter().map(bytes::Bytes::from).collect(),
        usage: crate::usage::counters(&HASH),
        routing_key: rk.to_string(),
        key_hash: crate::postings::rk_hash(rk).0,
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: crate::crypto::derive_subkey(&KEY, &HASH, rk, 0),
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
    let answer = tokio::time::timeout(Duration::from_secs(30), answer).await;
    answer
        .expect("append answered")
        .unwrap()
        .unwrap()
        .last_offset
}

/// An engine on `store` at `path` with `shard`'s configuration.
async fn engine_on(
    store: Arc<dyn object_store::ObjectStore>,
    path: &str,
    shard: ShardConfig,
) -> Arc<ShardEngine> {
    let db = Db::builder(path, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let (tx, _signals) = tokio::sync::mpsc::channel(1);
    ShardEngine::start(
        path.into(),
        Arc::new(db),
        store,
        shard,
        tx,
        None,
        maintenance,
    )
}

async fn engine(path: &str, shard: ShardConfig) -> Arc<ShardEngine> {
    engine_on(Arc::new(object_store::memory::InMemory::new()), path, shard).await
}

/// One read of `selector` from `from` under `budget` at `deliver`.
async fn read(
    engine: &Arc<ShardEngine>,
    from: u64,
    selector: Option<&str>,
    budget: usize,
    deliver: Deliver,
) -> crate::application::read::ReadPage {
    let handle = engine.stream_handle(HASH).await.unwrap();
    let page = read_merged(
        &KEY, &HASH, &handle, engine, from, selector, budget, deliver,
    );
    page.await.unwrap()
}

fn served(page: &crate::application::read::ReadPage) -> Vec<Served> {
    let records = page.recs.iter();
    records
        .map(|r| (r.off, r.rkey.clone(), r.payload.to_vec()))
        .collect()
}

/// A tiny deterministic generator (xorshift64*), so a failing seed replays.
struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
    }
}

/// One pager's shape: selector, byte budget and visibility.
type Pager = (Option<&'static str>, usize, Deliver);

/// The writer's progress, shared with the pagers: the stream's end once
/// the writer is done.
#[derive(Default)]
struct Progress {
    done: AtomicBool,
    end: AtomicU64,
}

/// Page from 0 until the writer is done and the pager has consumed the
/// whole stream, resuming each read at its consumed offset.
async fn run_pager(engine: &Arc<ShardEngine>, pager: Pager, progress: &Progress) -> Vec<Served> {
    let (selector, budget, deliver) = pager;
    let (mut from, mut all) = (0u64, Vec::new());
    loop {
        let finished = progress.done.load(Ordering::SeqCst);
        let page = read(engine, from, selector, budget, deliver).await;
        all.extend(served(&page));
        let next = page.scanned_through(from);
        assert!(
            next >= from,
            "{pager:?}: the cursor went back from {from} to {next}"
        );
        from = next;
        if finished && from >= progress.end.load(Ordering::SeqCst) {
            return all;
        }
        if page.recs.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// One small gather and a trim pulse. A gather that fails only skips its
/// turn; the readers judge G3.
async fn absorb_step(engine: &Arc<ShardEngine>, absorber: &Absorber) {
    if absorber.absorb_gather_v2(&[HASH]).await.is_err() {
        tokio::task::yield_now().await;
    }
    engine.pump_trim_tick();
}

/// Append 150 requests of 1 to 40 records of 40 to 640 bytes, two thirds
/// under key a, gathering one small chunk after every third request so
/// the absorbed boundary and the trims behind it trail the tail; returns
/// every record written. Fifty gathers stay below the history partition's
/// 64 L0 tables: past them a flush waits for the 300 s manifest poll.
async fn write(
    engine: &Arc<ShardEngine>,
    absorber: &Absorber,
    seed: u64,
    progress: &Progress,
) -> Vec<Served> {
    let (mut rng, mut expected, mut offset) = (Rng(seed), Vec::new(), 0u64);
    for request in 0..150 {
        if request % 3 == 2 {
            absorb_step(engine, absorber).await;
        }
        let rk = if rng.below(3) == 0 { "b" } else { "a" };
        let count = 1 + rng.below(40);
        let size = 40 + usize::try_from(rng.below(600)).unwrap();
        let records: Vec<Vec<u8>> = (offset..offset + count)
            .map(|o| payload(rk, o, size))
            .collect();
        let rows = (offset..).zip(&records);
        expected.extend(rows.map(|(o, r)| (o, rk.to_string(), r.clone())));
        offset += count;
        assert_eq!(append(engine, rk, records).await, offset - 1);
    }
    progress.end.store(offset, Ordering::SeqCst);
    progress.done.store(true, Ordering::SeqCst);
    expected
}

const PAGERS: [Pager; 8] = [
    (None, 1, Deliver::Durable),
    (None, 700, Deliver::Durable),
    (None, 2500, Deliver::Applied),
    (None, 1, Deliver::Applied),
    (Some("a"), 1, Deliver::Durable),
    (Some("a"), 900, Deliver::Applied),
    (Some("b"), 1, Deliver::Durable),
    (Some("b"), 3000, Deliver::Durable),
];

/// G3 under the absorption race with cursors inside pages: a writer appends
/// requests of 1 to 40 records under two keys and gathers one small chunk
/// after every third request (each gather one page edge further, the
/// trims seven offsets per step behind it), and pagers whose budgets end inside
/// pages read concurrently, unfiltered and keyed, durable and applied.
/// Every pager must serve exactly its key's records, each once, in order.
async fn race_round(seed: u64, ring: usize) {
    let shard = ShardConfig {
        tail_ring_bytes: ring,
        max_trim_per_op: 7,
        ..ShardConfig::default()
    };
    let engine = engine(&format!("g3-race-{seed:x}"), shard).await;
    let config = AbsorberConfig {
        gather_max_bytes: 3000,
        ..AbsorberConfig::default()
    };
    let absorber = Absorber::new(engine.clone(), config);
    let progress = Progress::default();
    let pagers = PAGERS.map(|pager| run_pager(&engine, pager, &progress));
    let work = futures_util::future::join(
        write(&engine, &absorber, seed, &progress),
        futures_util::future::join_all(pagers),
    );
    let settled = tokio::time::timeout(Duration::from_secs(120), work).await;
    let (expected, results) = settled.expect("the race settled");
    for (pager, got) in PAGERS.iter().zip(results) {
        let want: Vec<Served> = expected
            .iter()
            .filter(|(_, rk, _)| pager.0.is_none_or(|key| key == rk))
            .cloned()
            .collect();
        let offsets = |v: &[Served]| v.iter().map(|r| r.0).collect::<Vec<_>>();
        assert_eq!(
            offsets(&got),
            offsets(&want),
            "seed {seed:x}: {pager:?} offsets"
        );
        assert!(
            got == want,
            "seed {seed:x}: {pager:?} served a wrong payload"
        );
    }
    let handle = engine.stream_handle(HASH).await.unwrap();
    let tail = handle.state.lock().unwrap().durable.clone();
    assert!(
        0 < tail.trimmed && tail.absorbed < tail.next,
        "seed {seed:x}: no trims, or no lag: {} {} {}",
        tail.trimmed,
        tail.absorbed,
        tail.next
    );
    engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_absorb_and_trim_never_skip_or_repeat_a_record_for_mid_page_pagers() {
    race_round(0x9e37_79b9_7f4a_7c15, 0).await;
}

/// The same race with a 48 KiB tail ring, so pagers are served from ring
/// slices that eviction keeps cutting.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_absorb_and_trim_with_an_evicting_ring_never_skip_or_repeat_a_record() {
    race_round(0xdead_beef_cafe_f00d, 48 << 10).await;
}

/// Two requests of 5,000 nine-byte records: a's pages [0, 4095] and
/// [4096, 4999] absorbed into history, b's [5000, 9095] and [9096, 9999]
/// in the tail.
async fn capped_stream(ring: usize) -> Arc<ShardEngine> {
    let shard = ShardConfig {
        tail_ring_bytes: ring,
        ..ShardConfig::default()
    };
    let engine = engine(&format!("g3-cap-{ring}"), shard).await;
    let big = |first: u64, rk: &str| (first..first + 5000).map(|o| tiny(rk, o)).collect();
    assert_eq!(append(&engine, "a", big(0, "a")).await, 4999);
    let absorber = Absorber::new(engine.clone(), AbsorberConfig::default());
    let outcome = absorber.absorb_gather_v2(&[HASH]).await.unwrap();
    assert_eq!(outcome.advanced.first().map(|a| a.1), Some(5000));
    absorbed_reaches(&engine, Deliver::Durable, 5000).await;
    assert_eq!(append(&engine, "b", big(5000, "b")).await, 9999);
    engine
}

/// Read `selector` from `from` to the end of the capped stream.
async fn read_to_end(engine: &Arc<ShardEngine>, from: u64, selector: Option<&str>) -> Vec<Served> {
    let (mut cursor, mut all) = (from, Vec::new());
    for _ in 0..40 {
        let page = read(engine, cursor, selector, 8 << 20, Deliver::Durable).await;
        all.extend(served(&page));
        cursor = page.scanned_through(cursor);
        if cursor >= 10_000 {
            return all;
        }
    }
    panic!("from {from}, key {selector:?}: the reads did not reach the end");
}

/// Every record of the capped stream.
fn capped_records() -> Vec<Served> {
    let key = |o: u64| if o < 5000 { "a" } else { "b" };
    (0..10_000)
        .map(|o| (o, key(o).to_string(), tiny(key(o), o)))
        .collect()
}

/// A one-byte pager from `from` serves exactly one record per read for
/// twelve reads.
async fn one_byte_pager(engine: &Arc<ShardEngine>, from: u64, selector: Option<&str>) {
    let mut cursor = from;
    for expected in from..from + 12 {
        let page = read(engine, cursor, selector, 1, Deliver::Durable).await;
        let offsets: Vec<u64> = page.recs.iter().map(|r| r.off).collect();
        assert_eq!(offsets, [expected], "one-byte pager at {cursor}");
        cursor = page.scanned_through(cursor);
    }
}

/// A request of more than 4,096 records is cut at the page's record cap.
/// Reads from both sides of that edge, from the first and last record of
/// the full page, in history and in the tail, serve exactly the records
/// from there, and a one-byte pager crosses the edge one record per read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_past_the_page_record_cap_reads_exactly_across_its_page_edge() {
    const STARTS: [u64; 12] = [
        0, 1, 4094, 4095, 4096, 4097, 4999, 5000, 5001, 9095, 9096, 9999,
    ];
    let all = capped_records();
    for ring in [0, 8 << 20] {
        let engine = capped_stream(ring).await;
        let reads = STARTS.map(|from| [None, Some("a"), Some("b")].map(|key| (from, key)));
        for (from, selector) in reads.into_iter().flatten() {
            let got = read_to_end(&engine, from, selector).await;
            let want = all
                .iter()
                .filter(|(o, rk, _)| *o >= from && selector.is_none_or(|k| k == rk));
            let want: Vec<Served> = want.cloned().collect();
            assert!(got == want, "ring {ring}, from {from}, key {selector:?}");
        }
        for (from, selector) in [(4090u64, None), (4090, Some("a")), (9090, Some("b"))] {
            one_byte_pager(&engine, from, selector).await;
        }
        engine.begin_close();
    }
}

/// Wait until `engine`'s tail row at `visibility` names `absorbed`.
async fn absorbed_reaches(engine: &ShardEngine, visibility: Deliver, absorbed: u64) {
    let reached = tokio::time::timeout(Duration::from_secs(10), async {
        while engine.visible_absorbed(&HASH, visibility).await.unwrap().0 != absorbed {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "{visibility:?} absorbed never reached {absorbed}"
    );
}

/// The offsets one unfiltered read of `deliver` from `from` serves, and
/// its last consumed offset.
async fn offsets_from(
    engine: &Arc<ShardEngine>,
    from: u64,
    deliver: Deliver,
) -> (Vec<u64>, Option<u64>) {
    let page = read(engine, from, None, 8 << 20, deliver).await;
    for record in page.recs.iter() {
        assert_eq!(record.payload.to_vec(), payload("lane", record.off, 200));
    }
    (page.recs.iter().map(|r| r.off).collect(), page.last)
}

/// TLA-018-F1 with a cursor inside a page (the branch's own frontier test
/// appends one record per request): four requests of ten records are
/// pages [0, 9] .. [30, 39]. With the shard log's WAL write held, three
/// one-page gathers advance the applied boundary to 30, and the trims the
/// second and third advances enable delete pages [0, 9] and [10, 19],
/// applied only; the durable boundary still says 0. An applied read from 15
/// (inside the deleted page) sees the head gap, adopts the applied boundary
/// and serves 15..=19 from history; a durable read from 15 still finds the
/// page in the durable shard log. Both serve exactly 15..=39, also once the
/// write lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_applied_read_from_inside_a_page_its_trim_deleted_rereads_it_from_history() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2411,
        crate::dst::FaultProfile::clean(),
    );
    let engine = engine_on(store.clone(), "g3-frontier", ShardConfig::default()).await;
    for request in 0..4u64 {
        let records = (request * 10..request * 10 + 10)
            .map(|o| payload("lane", o, 200))
            .collect();
        assert_eq!(append(&engine, "lane", records).await, request * 10 + 9);
    }
    let config = AbsorberConfig {
        gather_max_bytes: 1,
        ..AbsorberConfig::default()
    };
    let absorber = Absorber::new(engine.clone(), config);
    // Open the history partition first: its open writes objects the WAL
    // hold would otherwise park.
    engine.history_partition().await.unwrap();
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    for upto in [10, 20, 30] {
        let outcome = absorber.absorb_gather_v2(&[HASH]).await.unwrap();
        assert_eq!(outcome.advanced.first().map(|a| a.1), Some(upto));
        absorbed_reaches(&engine, Deliver::Applied, upto).await;
    }
    assert!(
        engaged.load(Ordering::SeqCst) > 0,
        "the advances' WAL write is parked"
    );
    let handle = engine.stream_handle(HASH).await.unwrap();
    let (durable, applied) = {
        let st = handle.state.lock().unwrap();
        (st.durable.clone(), st.applied.clone())
    };
    assert_eq!(
        (durable.absorbed, applied.absorbed, applied.trimmed),
        (0, 30, 20)
    );
    let want = ((15..40).collect::<Vec<u64>>(), Some(39));
    assert_eq!(
        offsets_from(&engine, 15, Deliver::Applied).await,
        want,
        "applied"
    );
    assert_eq!(
        offsets_from(&engine, 15, Deliver::Durable).await,
        want,
        "durable, held"
    );
    store.release_hold();
    absorbed_reaches(&engine, Deliver::Durable, 30).await;
    assert_eq!(
        offsets_from(&engine, 15, Deliver::Durable).await,
        want,
        "durable, landed"
    );
    engine.begin_close();
}
