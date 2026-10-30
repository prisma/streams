#![cfg(test)]
//! The seal path's acceptance, in process (in-memory store):
//! - **flushdrop**: one seal's L0 holds a few percent of the sealed bytes;
//!   a flush without a seal writes the values; REVIEW-6A F2's control (no
//!   step 7a) writes them too;
//! - **tiling with aborts**: after every seal, stopped at any step or not,
//!   the directory tiles `[0, absorbed)`, the `'K'` rows rebuild exactly
//!   it, the stored pending row tiles `[absorbed, next)` (no page below the
//!   boundary survives the seal's put), and the only unreferenced objects
//!   are those of PUTs whose commit never landed; a reopened engine reads
//!   every record back exactly once in order, at both visibilities;
//! - **REVIEW-6A F1**: an append committed between a seal's snapshot and
//!   its commit survives a restart, and the negative control (the
//!   remainder pre-built from the snapshot) loses it.
//!
//! The rig and its helpers serve the other seal test modules too
//! (`lifecycle_tests`: eviction, the open and the close).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::{Rng, SeedableRng, rngs::StdRng};
use slatedb::config::{FlushOptions, FlushType, Settings};
use slatedb::db_stats;

use super::test_hooks::{Hold, SealStep};
use super::{SealError, SealSettings, Trigger};
use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::CheckedPage;
use crate::shard::{Deliver, ShardConfig, ShardEngine};
use crate::spill::directory::{DirEntry, decode_row, directory_prefix, parse_directory_key};
use crate::spill::metrics::ShardLogMetrics;
use crate::spill::object::{ObjectId, decode_chunk, object_path};
use crate::spill::pending::{parse_pages, pending_key};

pub(super) struct Rig {
    pub(super) engine: Arc<ShardEngine>,
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) fault: Arc<crate::dst::FaultStore>,
    pub(super) prefix: String,
    pub(super) path: String,
    pub(super) metrics: Arc<ShardLogMetrics>,
    pub(super) settings: SealSettings,
    pub(super) ring_bytes: usize,
}

pub(super) fn unique(label: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "seal-{label}-{nanos:x}-{}",
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// Settings for a test that drives `seal_once` itself.
pub(super) fn manual() -> SealSettings {
    SealSettings {
        auto: false,
        ..SealSettings::default()
    }
}

/// An engine at `prefix` on `store`, its shard log built as production
/// builds it (`keyspace::db_builder`: one tree (layout 7), the pending
/// rows' merge operator, the metrics recorder), the group-commit pump at
/// `gap`, a ring of `ring_bytes` and no compactor (the tests inspect L0
/// SSTs; the L0 caps are raised so none stalls). Waits for the sealer's
/// open.
pub(super) async fn open(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    settings: SealSettings,
    ring_bytes: usize,
    gap: Duration,
) -> Arc<ShardEngine> {
    let path = crate::sharddir::shard_db_path(prefix);
    let db = Arc::new(
        crate::keyspace::db_builder(path.as_str(), store.clone())
            .with_settings(Settings {
                flush_interval: Some(Duration::from_secs(60)),
                l0_sst_size_bytes: 8 << 20,
                max_unflushed_bytes: 16 << 20,
                l0_max_ssts: 4096,
                l0_max_ssts_per_key: 4096,
                compactor_options: None,
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let cfg = ShardConfig {
        tail_ring_bytes: ring_bytes,
        wal_group_commit: true,
        wal_flush_gap: gap,
        ..Default::default()
    };
    let engine = ShardEngine::start(prefix.to_string(), db, store, cfg, None, maintenance);
    engine.layout.seal.configure(|s| *s = settings);
    // An open that keeps failing (its rebuild refused) retries for ever;
    // a test fails instead of waiting with it.
    tokio::time::timeout(Duration::from_secs(20), engine.seal_ready())
        .await
        .expect("the sealer opened within 20 s")
        .unwrap();
    engine
}

pub(super) async fn rig_with(
    label: &str,
    settings: SealSettings,
    ring_bytes: usize,
    gap: Duration,
) -> Rig {
    let prefix = unique(label);
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let fault = crate::dst::FaultStore::uniform(inner, 0x6AB, crate::dst::FaultPlan::CLEAN);
    let store: Arc<dyn ObjectStore> = fault.clone();
    let engine = open(store.clone(), &prefix, settings, ring_bytes, gap).await;
    let path = crate::sharddir::shard_db_path(&prefix);
    Rig {
        engine,
        store,
        fault,
        metrics: crate::spill::metrics::of_log(&path)
            .expect("the builder registered the log's metrics"),
        path,
        prefix,
        settings,
        ring_bytes,
    }
}

pub(super) async fn rig(label: &str, settings: SealSettings, gap: Duration) -> Rig {
    rig_with(label, settings, 8 << 20, gap).await
}

/// Close the rig's engine and open a fresh one on the same store and path
/// (a restart: nothing in memory survives).
pub(super) async fn reopen(rig: Rig) -> Rig {
    rig.engine.begin_close();
    if tokio::time::timeout(Duration::from_secs(10), rig.engine.closed())
        .await
        .is_err()
    {
        eprintln!("seal reopen: the old engine did not close within 10 s");
    }
    if let Err(e) = rig.engine.db.close().await {
        eprintln!("seal reopen: close of the old writer: {e}");
    }
    let engine = open(
        rig.store.clone(),
        &rig.prefix,
        rig.settings,
        rig.ring_bytes,
        Duration::from_millis(2),
    )
    .await;
    Rig { engine, ..rig }
}

pub(super) fn random_bytes(rng: &mut StdRng, len: usize) -> Bytes {
    let mut v = vec![0u8; len];
    rng.fill(&mut v[..]);
    Bytes::from(v)
}

/// A length drawn log-uniformly from `[lo, hi]`: the draw's exponential,
/// floored and clamped to `[lo, hi]`.
pub(super) fn log_uniform(rng: &mut StdRng, lo: usize, hi: usize) -> usize {
    let (a, b) = ((lo as f64).ln(), (hi as f64).ln());
    let target = rng.random_range(a..=b).exp();
    // The largest length in `[lo, hi]` at most `target` (or `lo`), found
    // without a float-to-integer cast.
    let (mut low, mut high) = (lo, hi);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if mid as f64 <= target {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    low
}

/// One record of a length drawn log-uniformly from `[lo, hi]`.
pub(super) fn random_record(rng: &mut StdRng, lo: usize, hi: usize) -> Bytes {
    let len = log_uniform(rng, lo, hi);
    random_bytes(rng, len)
}

/// One append of `records` to `hash` under routing key `rk`, acked once
/// durable: its last offset.
pub(super) async fn append_lane(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    rk: &str,
    records: Vec<Bytes>,
) -> u64 {
    let subkey = derive_subkey(&StreamKey([7; 32]), &hash, rk, 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: Instant::now(),
        hash,
        route: hash,
        entries: records,
        usage: crate::usage::counters(&hash),
        routing_key: rk.to_string(),
        key_hash: crate::crypto::RoutingKeyHash::of(rk).0,
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey,
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
        resp: tx,
    };
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    rx.await.expect("resp").expect("ack").last_offset
}

pub(super) async fn append(engine: &Arc<ShardEngine>, hash: [u8; 16], records: Vec<Bytes>) -> u64 {
    append_lane(engine, hash, "", records).await
}

/// Hashes for `n` streams, some with a first byte at the segment tags'
/// neighbours or at 0xFF.
pub(super) fn hashes(rng: &mut StdRng, n: usize) -> Vec<[u8; 16]> {
    (0..n)
        .map(|i| {
            let mut h = [0u8; 16];
            rng.fill(&mut h[..]);
            match i % 5 {
                1 => h[0] = 0x52,
                2 => h[0] = 0x54,
                3 => h[0] = 0xFF,
                _ => {}
            }
            h
        })
        .collect()
}

/// Every record appended, by stream and offset.
pub(super) type Oracle = HashMap<[u8; 16], BTreeMap<u64, Bytes>>;

/// One append of `records` to `hash`, recorded in `oracle` once acked.
pub(super) async fn append_recorded(
    engine: &Arc<ShardEngine>,
    oracle: &std::sync::Mutex<Oracle>,
    hash: [u8; 16],
    records: Vec<Bytes>,
) {
    let n = records.len() as u64;
    let last = append(engine, hash, records.clone()).await;
    let mut o = oracle.lock().unwrap();
    let stream = o.entry(hash).or_default();
    for (off, rec) in (last + 1 - n..=last).zip(records) {
        stream.insert(off, rec);
    }
}

/// Where a test read starts, the routing key it selects (if any) and its
/// byte budget.
#[derive(Clone, Copy, Debug)]
pub(super) struct ReadAt<'a> {
    pub(super) from: u64,
    pub(super) selector: Option<&'a str>,
    pub(super) max_bytes: usize,
}

impl ReadAt<'static> {
    /// A read of every key from `from`, of at most `max_bytes`.
    pub(super) fn all(from: u64, max_bytes: usize) -> Self {
        Self {
            from,
            selector: None,
            max_bytes,
        }
    }
}

/// A read through the production reader of `hash` at `at`.
pub(super) async fn merged(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    at: ReadAt<'_>,
    deliver: Deliver,
) -> Result<crate::application::read::ReadPage, String> {
    let handle = engine
        .stream_handle(hash)
        .await
        .map_err(|e| e.to_string())?;
    let ReadAt {
        from,
        selector,
        max_bytes,
    } = at;
    crate::application::read::read_merged(
        &StreamKey([7; 32]),
        &hash,
        &handle,
        engine,
        from,
        selector,
        max_bytes,
        deliver,
    )
    .await
}

/// Read every record of `hash` from 0 through the production reader and
/// check it against `oracle`: each acked record exactly once, in order.
pub(super) async fn read_back(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    oracle: &BTreeMap<u64, Bytes>,
    deliver: Deliver,
) -> Result<(), String> {
    let mut from = 0u64;
    let end = oracle.keys().next_back().map_or(0, |l| l + 1);
    let mut stalls = 0;
    while from < end {
        let page = merged(engine, hash, ReadAt::all(from, 1 << 20), deliver).await?;
        let mut progressed = false;
        for r in page.recs.iter() {
            if r.off != from {
                return Err(format!("offset {} where {from} was due", r.off));
            }
            match oracle.get(&r.off) {
                Some(rec) if rec[..] == r.payload[..] => {}
                Some(_) => return Err(format!("offset {} holds other bytes", r.off)),
                None => return Err(format!("offset {} was never acked", r.off)),
            }
            from += 1;
            progressed = true;
        }
        if !progressed {
            stalls += 1;
            if stalls > 50 {
                return Err(format!(
                    "no progress at {from} of {end} (completed {}, end {})",
                    page.completed, page.end
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Ok(())
}

/// The shard's segment objects, by id, with their sizes.
pub(super) async fn objects(rig: &Rig) -> BTreeMap<ObjectId, u64> {
    let prefix = object_store::path::Path::from(format!("{}/hseg", rig.path));
    let mut out = BTreeMap::new();
    let mut list = rig.store.list(Some(&prefix));
    while let Some(meta) = list.next().await {
        let meta = meta.unwrap();
        let parts: Vec<_> = meta
            .location
            .parts()
            .map(|p| p.as_ref().to_string())
            .collect();
        let n = parts.len();
        let epoch = u64::from_str_radix(&parts[n - 2], 16).unwrap();
        let seq = u64::from_str_radix(&parts[n - 1], 16).unwrap();
        out.insert(ObjectId { epoch, seq }, meta.size);
    }
    out
}

/// Every applied group durable and published: the WAL flushed and each
/// resident stream's durable tail caught up with its applied one.
pub(super) async fn settle(engine: &Arc<ShardEngine>) {
    engine
        .db
        .flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();
    let t0 = Instant::now();
    loop {
        let behind = engine.streams.lock().unwrap().values().any(|h| {
            let st = h.state.lock().unwrap();
            st.durable.next != st.applied.next || st.durable.absorbed != st.applied.absorbed
        });
        if !behind {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "durable dispatch never caught up"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The shard's `'K'` rows, decoded: each stream's entries.
pub(super) async fn directory_rows(rig: &Rig) -> HashMap<[u8; 16], Vec<DirEntry>> {
    let mut iter = rig
        .engine
        .db
        .scan_prefix(&directory_prefix()[..], ..)
        .await
        .unwrap();
    let mut out: HashMap<[u8; 16], Vec<DirEntry>> = HashMap::new();
    while let Some(kv) = iter.next().await.unwrap() {
        let id = parse_directory_key(&kv.key).expect("a directory key");
        for (hash, entry) in decode_row(id, &kv.value).expect("a directory row") {
            out.entry(hash).or_default().push(entry);
        }
    }
    for v in out.values_mut() {
        v.sort_by_key(|e| e.last);
    }
    out
}

/// The stream's stored pending pages, as a read at Memory sees them.
async fn stored_pending(rig: &Rig, hash: [u8; 16]) -> Vec<(u64, Bytes)> {
    let raw = rig.engine.db.get(pending_key(&hash)).await.unwrap();
    raw.map_or_else(Vec::new, |r| parse_pages(&r).unwrap())
}

/// What one stream holds after seals.
#[derive(Debug)]
pub(super) struct StreamCheck {
    pub(super) absorbed: u64,
    pub(super) next: u64,
    pub(super) chunks: usize,
    pub(super) objects: BTreeSet<ObjectId>,
    pub(super) max_pages: u32,
}

/// The tiling invariant for one stream (every applied group settled):
/// - the in-memory directory tiles `[0, absorbed)` exactly, and the `'K'`
///   rows decode to exactly its entries (the open's rebuild);
/// - each entry's chunk, fetched by its offset and length alone, holds
///   exactly the entry's pages, each admitted at its offsets;
/// - the stored pending row tiles `[absorbed, next)`: no page below the
///   boundary survives the seal's put.
pub(super) async fn check_stream(
    rig: &Rig,
    hash: [u8; 16],
    rows: &HashMap<[u8; 16], Vec<DirEntry>>,
) -> StreamCheck {
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let durable = handle.state.lock().unwrap().durable.clone();
    let (absorbed, next) = (durable.absorbed, durable.next);
    let entries = rig
        .engine
        .layout
        .seal
        .directory
        .read()
        .unwrap()
        .from(&hash, 0, usize::MAX);
    assert_eq!(
        rows.get(&hash).cloned().unwrap_or_default(),
        entries,
        "the 'K' rows rebuild exactly the in-memory directory"
    );
    let mut expected = 0u64;
    let mut objects = BTreeSet::new();
    let mut max_pages = 0u32;
    for e in &entries {
        assert_eq!(e.first, expected, "entries tile with no gap or overlap");
        let at = object_store::path::Path::from(object_path(&rig.path, e.object));
        let range = e.chunk_off..e.chunk_off + u64::from(e.chunk_len);
        let raw = rig.store.get_range(&at, range).await.unwrap();
        let chunk = decode_chunk(&raw).unwrap();
        assert_eq!(
            (chunk.hash, chunk.first, chunk.last),
            (hash, e.first, e.last)
        );
        assert_eq!(u32::try_from(chunk.pages.len()).unwrap(), e.page_count);
        let mut o = e.first;
        for (last, page) in &chunk.pages {
            let admitted = CheckedPage::admit(page.clone(), *last)
                .expect("a sealed page admits at its offsets");
            assert_eq!(admitted.first(), o);
            o = last + 1;
        }
        assert_eq!(o, e.last + 1);
        expected = e.last + 1;
        max_pages = max_pages.max(e.page_count);
        objects.insert(e.object);
    }
    assert_eq!(expected, absorbed, "the directory tiles [0, absorbed)");
    let mut o = absorbed;
    for (last, page) in stored_pending(rig, hash).await {
        let p = CheckedPage::admit(page, last).unwrap();
        assert_eq!(
            p.first(),
            o,
            "the stored pending pages tile [absorbed, next): none below the boundary"
        );
        o = last + 1;
    }
    assert_eq!(o, next, "the stored pending pages end at next");
    StreamCheck {
        absorbed,
        next,
        chunks: entries.len(),
        objects,
        max_pages,
    }
}

/// Fill `streams` with about 512 KiB each of pages of 240 B to 64 KiB.
async fn fill(engine: &Arc<ShardEngine>, streams: &[[u8; 16]], seed: u64) {
    for (i, hash) in streams.iter().enumerate() {
        let mut rng = StdRng::seed_from_u64(seed * 100 + i as u64);
        let mut bytes = 0usize;
        while bytes < (512 << 10) {
            let len = log_uniform(&mut rng, 240, 65_000);
            bytes += len;
            append(engine, *hash, vec![random_bytes(&mut rng, len)]).await;
        }
    }
}

/// One seal, waiting for durable dispatch first; the L0 bytes its forced
/// flush wrote.
async fn seal_l0(rig: &Rig) -> (super::SealReport, u64) {
    let before = rig.metrics.counter(db_stats::L0_FLUSH_BYTES);
    let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    (
        report,
        rig.metrics.counter(db_stats::L0_FLUSH_BYTES) - before,
    )
}

/// **flushdrop**: 4 MiB of pages on 8 streams, one seal, and the L0 its
/// flush wrote: at most 5% of the sealed page bytes (the seal's puts of
/// empty remainders, the tails, the `'K'` row). Two controls: a flush
/// without a seal writes the values; and a seal whose flush skips step 7a
/// (REVIEW-6A F2) writes them too, because the put that shadows them is
/// above the flush's durable boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn flushdrop() {
    let rig = rig("flushdrop", manual(), Duration::from_millis(5)).await;
    let sp = &rig.engine.layout.seal;
    let mut rng = StdRng::seed_from_u64(0xF1D0);
    let streams = hashes(&mut rng, 8);
    fill(&rig.engine, &streams, 1).await;
    let (report, l0) = seal_l0(&rig).await;
    eprintln!(
        "flushdrop: sealed {} pages, {} B in {} chunks ({} streams, object {} B); L0 {l0} B: {:.3}%",
        report.pages,
        report.page_bytes,
        report.chunks,
        report.streams,
        report.object_bytes,
        100.0 * l0 as f64 / report.page_bytes as f64,
    );
    assert!(report.page_bytes >= 4 << 20, "{report:?}");
    assert_eq!(report.streams, 8);
    assert_eq!(report.remainder_bytes, 0, "every page sealed, no remainder");
    assert!(
        l0 * 100 <= report.page_bytes * 5,
        "the L0 holds {l0} B of {} B sealed",
        report.page_bytes
    );
    assert_eq!(sp.counters.snapshot_gaps.load(Ordering::Relaxed), 0);
    let rows = directory_rows(&rig).await;
    for hash in &streams {
        let c = check_stream(&rig, *hash, &rows).await;
        assert_eq!(c.absorbed, c.next, "every page sealed");
    }
    // Control 1: the same load again, flushed without a seal.
    assert_eq!(rig.engine.maintenance_snapshot().unabsorbed_frame_bytes, 0);
    fill(&rig.engine, &streams, 2).await;
    let unsealed = rig.engine.maintenance_snapshot().unabsorbed_frame_bytes;
    let before = rig.metrics.counter(db_stats::L0_FLUSH_BYTES);
    rig.engine.flush_memtable().await.unwrap();
    let control = rig.metrics.counter(db_stats::L0_FLUSH_BYTES) - before;
    eprintln!("flushdrop control (no seal): {unsealed} B of pages flushed; L0 {control} B");
    assert!(
        control >= unsealed,
        "without the seal the values reach the L0"
    );
    // Control 2 (F2): a seal of fresh pages whose flush skips 7a, with the
    // WAL PUTs parked from the commit on, so the flush's durable boundary
    // is the one before the seal's put.
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    fill(&rig.engine, &streams, 3).await;
    sp.hooks.skip_7a.store(true, Ordering::Relaxed);
    let hold = Hold::new();
    *sp.hooks.before_commit.lock().unwrap() = Some(hold.clone());
    let before = rig.metrics.counter(db_stats::L0_FLUSH_BYTES);
    let (report, ()) = tokio::join!(
        rig.engine.seal_once(Trigger::Requested),
        hold_wal_across_the_commit(&hold, &rig.fault),
    );
    let report = report.unwrap();
    let l0 = rig.metrics.counter(db_stats::L0_FLUSH_BYTES) - before;
    *sp.hooks.before_commit.lock().unwrap() = None;
    sp.hooks.skip_7a.store(false, Ordering::Relaxed);
    eprintln!(
        "flushdrop F2 control (seal without 7a): sealed {} B; L0 {l0} B",
        report.page_bytes
    );
    assert!(
        l0 * 2 >= report.page_bytes,
        "without 7a the shadowed operands reach the L0 ({l0} of {})",
        report.page_bytes
    );
    rig.engine.begin_close();
}

/// **A forced flush takes step 7a** (REVIEW-6A F2): a seal applied but not
/// durable (stopped after its commit, the shard's WAL PUTs parked from the
/// commit on), then a forced memtable flush, as the deadline and the queue
/// collector take it. Its awaited WAL flush makes the seal's puts durable
/// before the memtable freezes, so the L0 holds at most 5% of the sealed
/// page bytes; without it the shadowed operands reach the L0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_forced_flush_makes_an_applied_seal_durable_first() {
    let rig = rig("forced-7a", manual(), Duration::from_millis(5)).await;
    let sp = &rig.engine.layout.seal;
    let mut rng = StdRng::seed_from_u64(0x07A);
    let streams = hashes(&mut rng, 8);
    fill(&rig.engine, &streams, 4).await;
    let sealed_before = sp.counters.page_bytes.load(Ordering::Relaxed);
    *sp.hooks.abort_at.lock().unwrap() = Some(SealStep::Applied);
    let hold = Hold::new();
    *sp.hooks.before_commit.lock().unwrap() = Some(hold.clone());
    let park_wal = async {
        let t0 = Instant::now();
        while hold.entered.load(Ordering::Acquire) == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "the seal never reached its commit"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        rig.fault.hold_class(
            crate::dst::StoreOp::Put,
            crate::dst::ObjClass::Wal,
            u64::MAX,
        );
        hold.release();
    };
    let (sealed, ()) =
        futures_util::future::join(rig.engine.seal_once(Trigger::Requested), park_wal).await;
    assert!(
        matches!(sealed, Err(SealError::Aborted(SealStep::Applied))),
        "{sealed:?}"
    );
    *sp.hooks.abort_at.lock().unwrap() = None;
    *sp.hooks.before_commit.lock().unwrap() = None;
    let page_bytes = sp.counters.page_bytes.load(Ordering::Relaxed) - sealed_before;
    assert!(page_bytes >= 4 << 20, "{page_bytes} B sealed");
    let before = rig.metrics.counter(db_stats::L0_FLUSH_BYTES);
    let release = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        rig.fault.release_hold();
    };
    let (flushed, ()) = futures_util::future::join(rig.engine.flush_memtable(), release).await;
    flushed.unwrap();
    let l0 = rig.metrics.counter(db_stats::L0_FLUSH_BYTES) - before;
    eprintln!(
        "forced 7a: sealed {page_bytes} B applied, not durable; the forced flush's L0 {l0} B"
    );
    assert!(
        l0 * 100 <= page_bytes * 5,
        "the forced flush wrote the shadowed operands: L0 {l0} B of {page_bytes} B"
    );
    rig.engine.begin_close();
}

/// Once a seal parks before its commit: hold the shard's WAL PUTs, let the
/// seal commit, and release the PUTs 300 ms later.
async fn hold_wal_across_the_commit(hold: &Hold, fault: &crate::dst::FaultStore) {
    let t0 = Instant::now();
    while hold.entered.load(Ordering::Acquire) == 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the seal never reached its commit"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    fault.hold_class(
        crate::dst::StoreOp::Put,
        crate::dst::ObjClass::Wal,
        u64::MAX,
    );
    hold.release();
    tokio::time::sleep(Duration::from_millis(300)).await;
    fault.release_hold();
}

/// One seal of the property test, stopped at `abort` if any.
async fn seal_with_abort(rig: &Rig, abort: Option<SealStep>) -> Result<(), SealError> {
    *rig.engine.layout.seal.hooks.abort_at.lock().unwrap() = abort;
    let result = rig.engine.seal_once(Trigger::Requested).await.map(|_| ());
    *rig.engine.layout.seal.hooks.abort_at.lock().unwrap() = None;
    result
}

/// Random appends to `streams`: 1 to 4 appends of 1 to 3 records.
async fn random_appends(
    rig: &Rig,
    rng: &mut StdRng,
    streams: &[[u8; 16]],
    oracle: &std::sync::Mutex<Oracle>,
) {
    let hash = streams[rng.random_range(0..streams.len())];
    for _ in 0..rng.random_range(1..=4) {
        let records = (0..rng.random_range(1..=3))
            .map(|_| {
                let len = log_uniform(rng, 16, 20_000);
                random_bytes(rng, len)
            })
            .collect();
        append_recorded(&rig.engine, oracle, hash, records).await;
    }
}

/// After a seal, settled: every stream tiles, no chunk is over the page
/// cap, and the unreferenced objects are exactly the orphans.
async fn check_shard(rig: &Rig, streams: &[[u8; 16]], orphans: usize, seed: u64) -> usize {
    settle(&rig.engine).await;
    let objects = objects(rig).await;
    let rows = directory_rows(rig).await;
    let mut referenced = BTreeSet::new();
    let mut chunks = 0;
    for hash in streams {
        let c = check_stream(rig, *hash, &rows).await;
        assert!(
            c.max_pages as usize <= rig.settings.max_pages_per_chunk,
            "seed {seed}: a chunk over the page cap"
        );
        referenced.extend(c.objects);
        chunks += c.chunks;
    }
    let unreferenced = objects.keys().filter(|id| !referenced.contains(id)).count();
    assert!(
        referenced.iter().all(|id| objects.contains_key(id)),
        "seed {seed}: an entry names a missing object"
    );
    assert_eq!(
        unreferenced, orphans,
        "seed {seed}: one unreferenced object per PUT whose commit never landed"
    );
    chunks
}

/// **Tiling with aborts.** Random appends to random streams, seals at
/// random points, each stopped at a random step: after the WAL flush,
/// after the PUT, after the commit is applied, after it is durable (before
/// the memtable flush), or not at all. After every seal, once settled:
/// `check_stream` for every stream, and the shard's objects are the
/// referenced ones plus exactly one per seal stopped after its PUT. A final
/// seal seals everything; then the engine is reopened (the directory
/// rebuilt from the `'K'` rows, the unsealed pages from the pending rows)
/// and every record reads back exactly once, in order, at both visibilities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn tiling_with_an_abort_between_every_pair_of_steps() {
    let mut aborted_at = [0usize; 5];
    for seed in 0..8u64 {
        tiling_seed(seed, &mut aborted_at).await;
    }
    eprintln!(
        "tiling seals by abort point (completed, wal flush, put, applied, durable): {aborted_at:?}"
    );
    assert!(
        aborted_at.iter().all(|n| *n > 0),
        "every abort point ran: {aborted_at:?}"
    );
}

/// The tiling test's abort points: none, then each step a seal may stop after.
const ABORTS: [Option<SealStep>; 5] = [
    None,
    Some(SealStep::WalFlushed),
    Some(SealStep::Put),
    Some(SealStep::Applied),
    Some(SealStep::Durable),
];

/// One seed of the tiling test: its 50 rounds of appends and aborted seals,
/// the final seal, the restart and the read back; counts each abort point
/// that ran in `aborted_at`.
async fn tiling_seed(seed: u64, aborted_at: &mut [usize; 5]) {
    let mut rng = StdRng::seed_from_u64(0x7111 ^ seed);
    let mut settings = manual();
    settings.max_pages_per_chunk = rng.random_range(2..=6);
    let mut rig = rig("tiling", settings, Duration::from_millis(2)).await;
    let n = rng.random_range(2..=6);
    let streams = hashes(&mut rng, n);
    let oracle = std::sync::Mutex::new(Oracle::new());
    let mut orphans = 0usize;
    for _ in 0..50 {
        if rng.random_bool(0.6) {
            random_appends(&rig, &mut rng, &streams, &oracle).await;
            continue;
        }
        let pick = rng.random_range(0..ABORTS.len());
        let unsealed = rig.engine.maintenance_snapshot().unabsorbed_frame_bytes > 0;
        let result = seal_with_abort(&rig, ABORTS[pick]).await;
        match (ABORTS[pick], result) {
            (None, Ok(())) => aborted_at[pick] += 1,
            (Some(step), Err(SealError::Aborted(at))) if at == step => aborted_at[pick] += 1,
            (Some(SealStep::Put | SealStep::Applied), Ok(())) if !unsealed => {}
            (want, got) => panic!("seed {seed}: abort {want:?} gave {got:?}"),
        }
        if ABORTS[pick] == Some(SealStep::Put) && unsealed {
            orphans += 1;
        }
        check_shard(&rig, &streams, orphans, seed).await;
    }
    seal_with_abort(&rig, None).await.unwrap();
    let chunks = check_shard(&rig, &streams, orphans, seed).await;
    for hash in &streams {
        let c = check_stream(&rig, *hash, &directory_rows(&rig).await).await;
        assert_eq!(
            c.absorbed, c.next,
            "seed {seed}: the final seal seals everything"
        );
    }
    let sp = &rig.engine.layout.seal;
    assert_eq!(sp.counters.snapshot_gaps.load(Ordering::Relaxed), 0);
    assert_eq!(sp.counters.refused.load(Ordering::Relaxed), 0);
    eprintln!(
        "tiling seed {seed}: {} streams, {chunks} chunks, {orphans} orphans",
        streams.len()
    );
    // A few more appends left unsealed, then a restart.
    for hash in &streams {
        let records = vec![random_bytes(&mut rng, 300)];
        append_recorded(&rig.engine, &oracle, *hash, records).await;
    }
    rig = reopen(rig).await;
    let oracle = oracle.into_inner().unwrap();
    for (hash, o) in &oracle {
        for deliver in [Deliver::Durable, Deliver::Applied] {
            read_back(&rig.engine, *hash, o, deliver)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: after reopen: {e}"));
        }
    }
    rig.engine.begin_close();
}

/// Appends to one stream while a seal is parked between its PUT and its
/// commit (after its snapshot), then the seal completes, one more append,
/// a restart and a read back at both visibilities.
async fn appended_between_snapshot_and_commit(prebuilt: bool) -> Result<(), String> {
    let rig = rig("f1", manual(), Duration::from_millis(2)).await;
    let sp = &rig.engine.layout.seal;
    sp.hooks
        .prebuilt_remainder
        .store(prebuilt, Ordering::Relaxed);
    let mut rng = StdRng::seed_from_u64(0xF1);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = std::sync::Mutex::new(Oracle::new());
    for _ in 0..6 {
        let records = (0..rng.random_range(1..=3))
            .map(|_| random_bytes(&mut rng, 700))
            .collect();
        append_recorded(&rig.engine, &oracle, hash, records).await;
    }
    let hold = Hold::new();
    *sp.hooks.before_commit.lock().unwrap() = Some(hold.clone());
    let appends = async {
        let t0 = Instant::now();
        while hold.entered.load(Ordering::Acquire) == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "the seal never reached its commit"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for _ in 0..4 {
            let records = (0..rng.random_range(1..=3))
                .map(|_| random_bytes(&mut rng, 500))
                .collect();
            append_recorded(&rig.engine, &oracle, hash, records).await;
        }
        hold.release();
    };
    let (report, ()) = tokio::join!(rig.engine.seal_once(Trigger::Requested), appends);
    let report = report.expect("seal");
    assert!(report.pages > 0);
    // The control's pre-built remainder holds only what the snapshot left.
    assert!(
        prebuilt || report.remainder_bytes > 0,
        "the pages after the snapshot are the remainder: {report:?}"
    );
    append_recorded(
        &rig.engine,
        &oracle,
        hash,
        vec![random_bytes(&mut rng, 400)],
    )
    .await;
    let rig = reopen(rig).await;
    let oracle = oracle.into_inner().unwrap().remove(&hash).unwrap();
    let result = async {
        read_back(&rig.engine, hash, &oracle, Deliver::Durable).await?;
        read_back(&rig.engine, hash, &oracle, Deliver::Applied).await
    }
    .await;
    rig.engine.begin_close();
    result
}

/// **REVIEW-6A F1.** An append committed between a seal's snapshot and its
/// commit survives: the committer builds the remainder from the live
/// stream, so the seal's put carries it. The negative control pre-builds
/// the remainder from the pages the snapshot knew, as a seal task handing
/// the committer a ready-made batch would; the put then shadows the later
/// pages, and after a restart they are gone (the stream fails to load, or
/// the read misses them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_append_committed_between_the_snapshot_and_the_commit_survives() {
    appended_between_snapshot_and_commit(false).await.unwrap();
    let control = appended_between_snapshot_and_commit(true).await;
    eprintln!("F1 negative control: {control:?}");
    assert!(
        control.is_err(),
        "the pre-built remainder lost nothing: the control did not bite"
    );
}
