//! Layout 5 reads through the production executor (`read_merged`) over a
//! stream whose pages lie in history and in the shard-log tail: unkeyed and
//! keyed reads cross page edges, a read starting inside a page serves from
//! its cursor, and a byte budget that ends inside a page resumes at the next
//! record, down to a budget of one byte.
use crate::application::read::read_merged;
use crate::crypto::StreamKey;
use crate::history::{Absorber, AbsorberConfig};
use crate::shard::{Deliver, ShardConfig, ShardEngine};
use slatedb::Db;
use std::sync::Arc;
use std::time::Duration;

const KEY: StreamKey = StreamKey([7; 32]);
const HASH: [u8; 16] = [0x5a; 16];

/// The requests of the fixture stream: (routing key, records, record
/// size). 40 KiB records are a page each; the others share their request's
/// page. Requests 0 to 3 are absorbed, 4 and 5 stay in the tail.
const REQUESTS: [(&str, u64, usize); 6] = [
    ("a", 20, 1000),
    ("b", 5, 300),
    ("a", 3, 40 << 10),
    ("a", 10, 1000),
    ("b", 7, 300),
    ("a", 30, 1000),
];
const ABSORBED: u64 = 38;
const RECORDS: u64 = 75;

/// Record `offset`'s payload: text that zstd shrinks, sized per request.
fn payload(rk: &str, offset: u64, size: usize) -> Vec<u8> {
    let line = format!("{{\"key\":\"{rk}\",\"offset\":{offset},\"pad\":\"");
    let mut bytes = line.into_bytes();
    bytes.resize(size, b'x');
    bytes
}

/// Every record of the fixture stream as (routing key, offset, payload).
fn expected() -> Vec<(&'static str, u64, Vec<u8>)> {
    let mut offset = 0;
    let mut out = Vec::new();
    for (rk, count, size) in REQUESTS {
        for _ in 0..count {
            out.push((rk, offset, payload(rk, offset, size)));
            offset += 1;
        }
    }
    out
}

/// The records a read of `selector` from `from` must serve, in order.
fn wanted(selector: Option<&str>, from: u64) -> Vec<(u64, Vec<u8>)> {
    expected()
        .into_iter()
        .filter(|(rk, offset, _)| *offset >= from && selector.is_none_or(|key| key == *rk))
        .map(|(_, offset, payload)| (offset, payload))
        .collect()
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
    answer.await.unwrap().unwrap().last_offset
}

/// One gather, waited until the committer applied its advance.
async fn absorb(engine: &ShardEngine, absorber: &Absorber) -> u64 {
    let outcome = absorber.absorb_gather_v2(&[HASH]).await.unwrap();
    let upto = outcome.advanced.first().map(|advance| advance.1);
    let upto = upto.expect("the gather advanced");
    let handle = engine.stream_handle(HASH).await.unwrap();
    for _ in 0..10_000 {
        if handle.state.lock().unwrap().durable.absorbed == upto {
            return upto;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("the advance to {upto} never applied");
}

/// The fixture stream on an engine with a tail ring of `ring` bytes:
/// requests 0 to 3 absorbed into history, 4 and 5 in the tail.
async fn stream(path: &str, ring: usize) -> Arc<ShardEngine> {
    let store = Arc::new(object_store::memory::InMemory::new());
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
    let config = ShardConfig {
        tail_ring_bytes: ring,
        ..Default::default()
    };
    let engine = ShardEngine::start(
        path.into(),
        Arc::new(db),
        store,
        config,
        tx,
        None,
        maintenance,
    );
    let absorber = Absorber::new(engine.clone(), AbsorberConfig::default());
    let mut offset = 0;
    for (index, (rk, count, size)) in REQUESTS.into_iter().enumerate() {
        let records = (offset..offset + count)
            .map(|o| payload(rk, o, size))
            .collect();
        offset += count;
        assert_eq!(append(&engine, rk, records).await, offset - 1);
        if index == 3 {
            assert_eq!(absorb(&engine, &absorber).await, ABSORBED);
        }
    }
    assert_eq!(offset, RECORDS);
    engine
}

/// One durable read of `selector` from `from` under a `budget` of bytes:
/// (served records, last consumed offset, completed).
async fn read(
    engine: &Arc<ShardEngine>,
    from: u64,
    selector: Option<&str>,
    budget: usize,
) -> (Vec<(u64, Vec<u8>)>, Option<u64>, bool) {
    let handle = engine.stream_handle(HASH).await.unwrap();
    let page = read_merged(
        &KEY,
        &HASH,
        &handle,
        engine,
        from,
        selector,
        budget,
        Deliver::Durable,
    )
    .await
    .unwrap();
    let served = page
        .recs
        .iter()
        .map(|r| (r.off, r.payload.to_vec()))
        .collect();
    (served, page.last, page.completed)
}

const SELECTORS: [Option<&str>; 3] = [None, Some("a"), Some("b")];

/// A read from any offset, the first and last of a page, a record inside
/// one, either side of the absorbed boundary, serves exactly the records
/// from there to the end, unfiltered and for each key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_from_inside_pages_cross_page_edges_in_history_and_tail() {
    const STARTS: [u64; 18] = [
        0, 1, 19, 20, 24, 25, 26, 27, 28, 33, 37, 38, 41, 44, 45, 60, 74, 75,
    ];
    for ring in [0, 8 << 20] {
        let engine = stream(&format!("paging-cross-{ring}"), ring).await;
        let reads = STARTS
            .iter()
            .flat_map(|from| SELECTORS.map(|key| (*from, key)));
        for (from, selector) in reads {
            let (served, last, completed) = read(&engine, from, selector, 8 << 20).await;
            let label = format!("ring {ring}, from {from}, key {selector:?}");
            assert!(completed, "{label}: the read ended early at {last:?}");
            assert_eq!(served, wanted(selector, from), "{label}");
        }
        engine.begin_close();
    }
}

/// Page through the stream from 0 under `budget`, resuming each read at
/// the last consumed offset plus one; every read must make progress, and
/// with a budget of one byte an unfiltered read serves exactly one record.
async fn page_through(
    engine: &Arc<ShardEngine>,
    selector: Option<&str>,
    budget: usize,
) -> Vec<(u64, Vec<u8>)> {
    let label = format!("budget {budget}, key {selector:?}");
    let (mut from, mut served) = (0, Vec::new());
    for _ in 0..2 * RECORDS {
        let (got, last, completed) = read(engine, from, selector, budget).await;
        let single = budget == 1 && selector.is_none() && !completed;
        assert!(!single || got.len() == 1, "{label}: from {from}: {got:?}");
        served.extend(got);
        if completed {
            return served;
        }
        let next = last.map(|last| last + 1).expect("progress");
        assert!(next > from, "{label}: no progress from {from}");
        from = next;
    }
    panic!("{label}: the pager did not settle");
}

/// A pager whose byte budget ends inside pages resumes at the last consumed
/// offset plus one: every record of the key comes back exactly once, in
/// order, and every read makes progress. A budget of one byte serves
/// exactly one record per unfiltered read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_budget_cut_inside_a_page_resumes_at_the_next_record() {
    for ring in [0, 8 << 20] {
        let engine = stream(&format!("paging-budget-{ring}"), ring).await;
        let pagers = [1, 1500, 2500, 50_000]
            .into_iter()
            .flat_map(|budget| SELECTORS.map(|key| (budget, key)));
        for (budget, selector) in pagers {
            let served = page_through(&engine, selector, budget).await;
            let label = format!("ring {ring}, budget {budget}, key {selector:?}");
            assert_eq!(served, wanted(selector, 0), "{label}");
        }
        engine.begin_close();
    }
}
