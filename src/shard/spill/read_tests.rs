//! L6 spike M4 acceptance (SPIKE-PLAN §3 M4): the sealed-range reader.
//!
//! - exact reads across chunks, byte anchors, budgets and both
//!   visibilities, through the production merged reader;
//! - at most `MAX_IN_FLIGHT` GETs in flight;
//! - a missing index row below the boundary is an honest partial, a 404
//!   with a live row is retryable, a keyed history read fails loudly;
//! - the two DSTs: a zero-stale reader racing a seal's advance and trims
//!   gets exact data or an honest partial (DESIGN §3.2 item 7), and the
//!   negative control that reads the index at Remote while the boundary
//!   comes from Memory finds the hole (rBc G2-V).
//!
//! In process, dev profile; each engine has its own shard path.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::{Rng, SeedableRng, rngs::StdRng};
use slatedb::config::{DurabilityLevel, FlushOptions, FlushType, Settings};

use super::read::{GetHold, MAX_IN_FLIGHT, SpillReadError, Stop, is_retryable, read_range};
use super::tests::{append, append_lane, hashes, log_uniform, random_bytes};
use crate::application::read::{ReadPage, read_merged};
use crate::crypto::StreamKey;
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::shard::{Deliver, RingBudget, RingSettings, ShardConfig, ShardEngine, ShardMaintenance};
use crate::spill::index::index_key;
use crate::spill::object::{ObjectId, object_path};
use crate::spill::settings::{SpillSettings, Trigger};

fn unique(label: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("l6m4-{label}-{nanos:x}-{}", N.fetch_add(1, Ordering::Relaxed))
}

/// A sealer driven by the test alone, chunks of at most `max_pages` pages.
fn manual(max_pages: usize) -> SpillSettings {
    SpillSettings {
        auto: false,
        max_pages_per_chunk: max_pages,
        ..SpillSettings::default()
    }
}

/// A spill-arm engine on `store`: group commit at `gap`, SlateDB's metrics
/// recorder, the ring under a 64/8/2 MiB budget or off, no compactor.
async fn engine_on(
    store: Arc<dyn ObjectStore>,
    label: &str,
    settings: SpillSettings,
    ring: bool,
    gap: Duration,
) -> (Arc<ShardEngine>, String) {
    let prefix = unique(label);
    let path = crate::sharddir::shard_db_path(&prefix);
    let metrics = crate::l6_metrics::for_db(&path);
    let db = Arc::new(
        slatedb::Db::builder(path.as_str(), store.clone())
            .with_settings(Settings {
                flush_interval: Some(Duration::from_secs(60)),
                l0_sst_size_bytes: 8 << 20,
                max_unflushed_bytes: 16 << 20,
                l0_max_ssts: 4096,
                l0_max_ssts_per_key: 4096,
                compactor_options: None,
                ..Default::default()
            })
            .with_metrics_recorder(metrics.recorder())
            .build()
            .await
            .unwrap(),
    );
    let cfg = ShardConfig {
        l6_spill: true,
        shared_ring: ring.then(|| {
            RingBudget::new(RingSettings {
                process_bytes: 64 << 20,
                shard_cap_bytes: 8 << 20,
                shard_floor_bytes: 2 << 20,
            })
        }),
        wal_group_commit: true,
        wal_flush_gap: gap,
        spill_settings: settings,
        ..Default::default()
    };
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(prefix, db, store, cfg, tx, None, ShardMaintenance::default());
    (engine, path)
}

fn key() -> StreamKey {
    StreamKey([7; 32])
}

/// Every record appended, by stream and offset.
type Oracle = HashMap<[u8; 16], BTreeMap<u64, Bytes>>;

/// One append of `records` to `hash`, recorded in `oracle` once acked.
async fn append_recorded(engine: &Arc<ShardEngine>, oracle: &Mutex<Oracle>, hash: [u8; 16], records: Vec<Bytes>) {
    let n = records.len() as u64;
    let last = append(engine, hash, records.clone()).await;
    let mut o = oracle.lock().unwrap();
    let stream = o.entry(hash).or_default();
    for (off, rec) in (last + 1 - n..=last).zip(records) {
        stream.insert(off, rec);
    }
}

/// A merged read (the production reader) of `hash` from `from`.
async fn merged(engine: &Arc<ShardEngine>, hash: [u8; 16], from: u64, max_bytes: usize, deliver: Deliver) -> Result<ReadPage, String> {
    let handle = engine.stream_handle(hash).await.map_err(|e| e.to_string())?;
    read_merged(&key(), &hash, &handle, engine, from, None, max_bytes, deliver).await
}

/// `page`, read from `from`, serves exactly the oracle's records from
/// `from` on, contiguously, and all of them up to its end if complete.
fn check_page(oracle: &BTreeMap<u64, Bytes>, from: u64, page: &ReadPage) -> Result<usize, String> {
    let mut expect = from;
    for r in page.recs.iter() {
        if r.off != expect {
            return Err(format!("from {from}: offset {} where {expect} was due", r.off));
        }
        match oracle.get(&r.off) {
            Some(rec) if rec[..] == r.payload[..] => {}
            Some(_) => return Err(format!("from {from}: offset {} holds other bytes", r.off)),
            None => return Err(format!("from {from}: offset {} was never acked", r.off)),
        }
        expect += 1;
    }
    if page.completed && expect < page.end && !page.recs.is_empty() {
        return Err(format!("from {from}: complete page ends at {expect}, its end is {}", page.end));
    }
    Ok(page.recs.len())
}

fn reads(engine: &ShardEngine) -> &crate::spill::stats::ReadStats {
    &engine.spill.as_ref().expect("spill arm").stats.reads
}

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// Appends of 1-3 records of `lo`-`hi` bytes to `streams`, then a seal,
/// `rounds` times; returns the oracle.
async fn sealed_streams(engine: &Arc<ShardEngine>, rng: &mut StdRng, streams: &[[u8; 16]], rounds: usize, appends: usize, lo: usize, hi: usize) -> Oracle {
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..rounds {
        for _ in 0..appends {
            let hash = streams[rng.random_range(0..streams.len())];
            let records = (0..rng.random_range(1..=3))
                .map(|_| {
                    let len = log_uniform(rng, lo, hi);
                    random_bytes(rng, len)
                })
                .collect();
            append_recorded(engine, &oracle, hash, records).await;
        }
        engine.spill_once(Trigger::Manual).await.expect("seal");
    }
    oracle.into_inner().unwrap()
}

/// Exact reads of sealed ranges at random offsets and budgets, both
/// visibilities, through `read_merged` and `read_range` directly: whole
/// chunks (CRC), chunks entered or left at a byte anchor, budget partials.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_ranges_read_back_exactly_across_chunks_anchors_and_budgets() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (engine, _) = engine_on(store, "exact", manual(48), true, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x4d34);
    let streams = hashes(&mut rng, 3);
    let oracle = sealed_streams(&engine, &mut rng, &streams, 3, 120, 64, 24_000).await;
    let r = reads(&engine);
    let budgets = [1usize, 1024, 100 << 10, 300 << 10, 1 << 20, 8 << 20];
    let mut served = 0usize;
    for i in 0..300 {
        let hash = streams[i % streams.len()];
        let o = &oracle[&hash];
        let absorbed = engine.stream_handle(hash).await.unwrap().state.lock().unwrap().durable.absorbed;
        assert_eq!(absorbed, o.len() as u64, "every record sealed");
        let from = rng.random_range(0..absorbed);
        let budget = budgets[rng.random_range(0..budgets.len())];
        let deliver = if rng.random_bool(0.5) { Deliver::Durable } else { Deliver::Applied };
        let page = merged(&engine, hash, from, budget, deliver).await.expect("merged read");
        served += check_page(o, from, &page).unwrap_or_else(|e| panic!("read {i}: {e}"));
        assert!(!page.recs.is_empty(), "read {i}: the first record always fits");
        // The reader itself, on an inner window.
        let upto = rng.random_range(from + 1..=absorbed);
        let read = read_range(&engine, hash, from, upto, deliver, budget, None).await.expect("read_range");
        let mut due = from;
        for s in read.frames.iter() {
            assert_eq!(s.first(), due, "read {i}: slices tile from {from}");
            due = s.last() + 1;
        }
        assert!(due <= upto, "read {i}: served past its window");
        if read.completed {
            assert_eq!(due, upto, "read {i}: a complete read serves its window");
        } else {
            assert_eq!(read.stop, Stop::Budget, "read {i}: only the budget stops a read of rows that tile");
        }
        assert_eq!(read.last, due.checked_sub(1).filter(|_| due > from));
    }
    let (whole, partial, budget_partials, completed) = (
        get(&r.whole_chunks),
        get(&r.partial_chunks),
        get(&r.budget_partials),
        get(&r.completed),
    );
    println!(
        "exact: {served} records served; GETs {} ({} B), whole chunks {whole}, partial chunks {partial}, completed {completed}, budget partials {budget_partials}, missing-row partials {}, in flight max {}",
        get(&r.gets),
        get(&r.get_bytes),
        get(&r.missing_row_partials),
        get(&r.in_flight_max)
    );
    assert!(whole > 0 && partial > 0, "both the CRC path and the anchor path ran");
    assert!(budget_partials > 0 && completed > 0, "both budget partials and complete reads ran");
    assert_eq!(get(&r.missing_row_partials), 0, "rows tile: no read met a hole");
    assert!(get(&r.in_flight_max) <= MAX_IN_FLIGHT as u64);
    engine.begin_close();
}

/// A read of 20 one-page chunks parks its GETs: exactly `MAX_IN_FLIGHT`
/// enter, the read completes once they are released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_has_at_most_eight_gets_in_flight() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (engine, _) = engine_on(store, "inflight", manual(1), false, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(8);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..20 {
        let rec = random_bytes(&mut rng, 900);
        append_recorded(&engine, &oracle, hash, vec![rec]).await;
    }
    engine.spill_once(Trigger::Manual).await.expect("seal");
    let hold = GetHold::new();
    *engine.spill.as_ref().unwrap().get_hold.lock().unwrap() = Some(hold.clone());
    let reader = {
        let engine = engine.clone();
        tokio::spawn(async move { read_range(&engine, hash, 0, 20, Deliver::Durable, 8 << 20, None).await })
    };
    let t0 = std::time::Instant::now();
    while hold.entered.load(Ordering::Acquire) < MAX_IN_FLIGHT as u64 {
        assert!(t0.elapsed() < Duration::from_secs(10), "GETs never reached the cap");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(hold.entered.load(Ordering::Acquire), MAX_IN_FLIGHT as u64, "more than the cap in flight");
    hold.release();
    let read = reader.await.unwrap().expect("read");
    assert!(read.completed);
    assert_eq!(read.frames.len(), 20);
    assert_eq!(get(&reads(&engine).in_flight_max), MAX_IN_FLIGHT as u64);
    assert_eq!(hold.entered.load(Ordering::Acquire), 20, "one GET per chunk");
    engine.begin_close();
}

/// Rows missing below the boundary, a 404 with a live row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_rows_are_honest_partials_and_404s_are_retryable() {
    let inner = Arc::new(object_store::memory::InMemory::new());
    let store: Arc<dyn ObjectStore> = inner.clone();
    let (engine, path) = engine_on(store, "gone", manual(4), false, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(404);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..16 {
        let rec = random_bytes(&mut rng, 2000);
        append_recorded(&engine, &oracle, hash, vec![rec]).await;
    }
    engine.spill_once(Trigger::Manual).await.expect("seal");
    let oracle = oracle.into_inner().unwrap().remove(&hash).unwrap();
    let r = reads(&engine);
    // Four chunks of four pages: [0, 3], [4, 7], [8, 11], [12, 15].
    let full = merged(&engine, hash, 0, 8 << 20, Deliver::Durable).await.unwrap();
    assert_eq!(check_page(&oracle, 0, &full).unwrap(), 16);
    assert!(full.completed);

    // The row of [4, 7] goes, durably: [0, 4) is served, then nothing.
    engine.db.delete(index_key(&hash, 7)).await.unwrap();
    engine
        .db
        .flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();
    let missing_before = get(&r.missing_row_partials);
    for deliver in [Deliver::Durable, Deliver::Applied] {
        let read = read_range(&engine, hash, 0, 16, deliver, 8 << 20, None).await.unwrap();
        assert_eq!((read.completed, read.stop, read.last), (false, Stop::MissingRow, Some(3)));
        let page = merged(&engine, hash, 0, 8 << 20, deliver).await.unwrap();
        assert_eq!(check_page(&oracle, 0, &page).unwrap(), 4);
        assert_eq!((page.completed, page.last), (false, Some(3)), "an honest partial up to the hole");
        let at_hole = merged(&engine, hash, 4, 8 << 20, deliver).await.unwrap();
        assert_eq!(
            (at_hole.recs.len(), at_hole.completed, at_hole.last),
            (0, false, None),
            "no progress over the hole"
        );
    }
    // Per visibility: read_range, the merged read from 0, the one from 4.
    assert_eq!(get(&r.missing_row_partials) - missing_before, 6);

    // The object goes while rows still name it: retryable, never expired.
    let object = object_path(&path, ObjectId { epoch: engine.writer_epoch, seq: 1 });
    inner.delete(&object_store::path::Path::from(object)).await.unwrap();
    let err = read_range(&engine, hash, 8, 16, Deliver::Durable, 8 << 20, None).await.unwrap_err();
    assert!(matches!(err, SpillReadError::Retryable(_)), "{err:?}");
    let Err(err) = merged(&engine, hash, 8, 8 << 20, Deliver::Durable).await else {
        panic!("a read of a deleted object succeeded");
    };
    assert!(is_retryable(&err), "{err}");
    assert!(get(&r.not_found_retryable) >= 2);

    engine.begin_close();
}

/// The default lane (`Some("")`, what the product surface reads an
/// unkeyed stream with) is served from sealed chunks by a header-filtered
/// scan: other lanes' pages are skipped and still consumed. A routing key
/// is a keyed history read: it fails loudly and is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_lane_is_served_by_a_filtered_scan_and_keyed_reads_fail_loudly() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (engine, _) = engine_on(store, "lane", manual(5), false, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x1a4e);
    let hash = hashes(&mut rng, 1)[0];
    let mut default_lane = BTreeMap::new();
    for i in 0..24 {
        let rec = random_bytes(&mut rng, 1500);
        let rk = if i % 3 == 2 { "a" } else { "" };
        let last = append_lane(&engine, hash, rk, vec![rec.clone()]).await;
        if rk.is_empty() {
            default_lane.insert(last, rec);
        }
    }
    engine.spill_once(Trigger::Manual).await.expect("seal");
    let r = reads(&engine);
    let handle = engine.stream_handle(hash).await.unwrap();
    for deliver in [Deliver::Durable, Deliver::Applied] {
        let (lane0, skipped0) = (get(&r.lane_reads), get(&r.lane_skipped_pages));
        let page = read_merged(&key(), &hash, &handle, &engine, 0, Some(""), 8 << 20, deliver)
            .await
            .expect("default-lane read");
        let offs: Vec<u64> = page.recs.iter().map(|rec| rec.off).collect();
        assert_eq!(offs, default_lane.keys().copied().collect::<Vec<_>>(), "default lane only");
        for rec in page.recs.iter() {
            assert_eq!(rec.payload[..], default_lane[&rec.off][..]);
        }
        assert_eq!((page.last, page.completed), (Some(23), true), "the skipped pages count as consumed");
        assert_eq!(get(&r.lane_reads) - lane0, 1);
        assert_eq!(get(&r.lane_skipped_pages) - skipped0, 8);
    }
    let keyed_before = get(&r.keyed_unsupported);
    let Err(err) = read_merged(&key(), &hash, &handle, &engine, 0, Some("a"), 8 << 20, Deliver::Durable).await else {
        panic!("a keyed history read succeeded");
    };
    assert!(err.contains("keyed history read unsupported"), "{err}");
    assert_eq!(get(&r.keyed_unsupported) - keyed_before, 1);
    engine.begin_close();
}

/// What the held-WAL race read: the applied read's page and the counters.
struct HeldRace {
    oracle: BTreeMap<u64, Bytes>,
    applied: ReadPage,
    durable: ReadPage,
    missing_rows: u64,
    race_adoptions: u64,
    after_applied: ReadPage,
    after_durable: ReadPage,
}

/// The race of rBc G2-V made deterministic. A seal's PUT lands; then shard
/// WAL PUTs are held, so the seal's commit (its index rows, its advance,
/// the tombstones of exactly its pages) is applied in the memtable but not
/// Remote-durable. A zero-stale `deliver=applied` reader from 0 snapshots
/// the published boundary (0), scans the tail at Memory, meets the hole the
/// trims left, adopts the Memory boundary (`visible_absorbed(Applied)`),
/// and re-serves the gap from the index rows, read at `index` (None: the
/// read's own visibility). Then the hold is released and both views read
/// again.
async fn held_wal_race(label: &str, index: Option<DurabilityLevel>) -> HeldRace {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = FaultStore::uniform(inner, 0x6276, FaultPlan::CLEAN);
    let (engine, _) = engine_on(store.clone() as Arc<dyn ObjectStore>, label, manual(3), false, Duration::from_millis(2)).await;
    let sp = engine.spill.clone().unwrap();
    *sp.index_durability_override.lock().unwrap() = index;
    let mut rng = StdRng::seed_from_u64(0x6276);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..9 {
        let records = (0..rng.random_range(1..=3)).map(|_| random_bytes(&mut rng, 700)).collect();
        append_recorded(&engine, &oracle, hash, records).await;
    }
    let oracle = oracle.into_inner().unwrap().remove(&hash).unwrap();
    let n = oracle.len() as u64;
    let handle = engine.stream_handle(hash).await.unwrap();

    let before_commit = GetHold::new();
    *sp.before_commit.lock().unwrap() = Some(before_commit.clone());
    let seal = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.spill_once(Trigger::Manual).await })
    };
    let t0 = std::time::Instant::now();
    while before_commit.entered.load(Ordering::Acquire) == 0 {
        assert!(t0.elapsed() < Duration::from_secs(10), "the seal never reached its commit");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let parked = store.hold_class(StoreOp::Put, ObjClass::Wal, u64::MAX);
    before_commit.release();
    loop {
        let (applied, durable) = {
            let st = handle.state.lock().unwrap();
            ((st.applied.absorbed, st.applied.trimmed), st.durable.absorbed)
        };
        if applied == (n, n) {
            assert_eq!(durable, 0, "the seal's commit must stay non-durable while WAL PUTs are held");
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(10), "the seal's commit never applied: {applied:?}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let remote = engine.durable_absorbed(&hash).await.unwrap();
    assert_eq!(remote.0, 0, "the Remote tail row still says absorbed 0");
    let (missing0, races0) = (get(&sp.stats.reads.missing_row_partials), get(&sp.stats.reads.race_adoptions));
    let applied = merged(&engine, hash, 0, 8 << 20, Deliver::Applied).await.expect("applied read");
    let durable = merged(&engine, hash, 0, 8 << 20, Deliver::Durable).await.expect("durable read");
    let (missing_rows, race_adoptions) = (
        get(&sp.stats.reads.missing_row_partials) - missing0,
        get(&sp.stats.reads.race_adoptions) - races0,
    );
    assert!(parked.load(Ordering::SeqCst) > 0, "no WAL PUT was ever parked");
    store.release_hold();
    let report = seal.await.unwrap().expect("the seal completes once WAL PUTs flow");
    assert!(report.pages > 0, "the seal sealed the stream's pages");
    *sp.index_durability_override.lock().unwrap() = None;
    let after_applied = merged(&engine, hash, 0, 8 << 20, Deliver::Applied).await.unwrap();
    let after_durable = merged(&engine, hash, 0, 8 << 20, Deliver::Durable).await.unwrap();
    engine.begin_close();
    HeldRace {
        oracle,
        applied,
        durable,
        missing_rows,
        race_adoptions,
        after_applied,
        after_durable,
    }
}

/// One random race: writers append to three streams while a sealer seals
/// every few milliseconds and zero-stale readers (`deliver=applied`, from
/// the boundary as a seal moves it) and durable readers read; every page
/// is checked against the records acked, once the writers are done.
async fn random_race(seed: u64, ring: bool) -> (u64, u64, u64, u64) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut rng = StdRng::seed_from_u64(seed);
    let max_pages = rng.random_range(2..=64);
    let (engine, _) = engine_on(store, &format!("race{seed}"), manual(max_pages), ring, Duration::from_millis(2)).await;
    let streams = hashes(&mut rng, 3);
    let oracle = Arc::new(Mutex::new(Oracle::new()));
    let done = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = streams
        .iter()
        .enumerate()
        .map(|(w, &hash)| {
            let (engine, oracle) = (engine.clone(), oracle.clone());
            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed * 100 + w as u64);
                for _ in 0..150 {
                    let records = (0..rng.random_range(1..=3))
                        .map(|_| {
                            let len = log_uniform(&mut rng, 16, 20_000);
                            random_bytes(&mut rng, len)
                        })
                        .collect();
                    append_recorded(&engine, &oracle, hash, records).await;
                }
            })
        })
        .collect();
    let sealer = {
        let (engine, done) = (engine.clone(), done.clone());
        tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed ^ 0x5ea1);
            let mut seals = 0u64;
            while !done.load(Ordering::Acquire) {
                engine.spill_once(Trigger::Manual).await.expect("seal");
                seals += 1;
                tokio::time::sleep(Duration::from_millis(rng.random_range(0..8))).await;
            }
            seals
        })
    };
    type Seen = Vec<(u64, [u8; 16], Vec<(u64, u64)>, bool, u64)>;
    let readers: Vec<_> = (0..9)
        .map(|r| {
            let (engine, done, hash) = (engine.clone(), done.clone(), streams[r % 3]);
            let deliver = if r < 6 { Deliver::Applied } else { Deliver::Durable };
            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed * 1000 + r as u64);
                let mut seen: Seen = Vec::new();
                while !done.load(Ordering::Acquire) {
                    let handle = engine.stream_handle(hash).await.unwrap();
                    let (durable_absorbed, applied_absorbed, next) = {
                        let st = handle.state.lock().unwrap();
                        (st.durable.absorbed, st.applied.absorbed, st.applied.next.max(st.durable.next))
                    };
                    // Zero-stale: start at the boundary a seal is moving
                    // (its trims land at once), or anywhere.
                    let from = match rng.random_range(0..3) {
                        0 => durable_absorbed,
                        1 => applied_absorbed.saturating_sub(rng.random_range(0..4)),
                        _ => rng.random_range(0..=next),
                    };
                    let budget = [1usize, 4096, 64 << 10, 1 << 20][rng.random_range(0..4)];
                    let page = merged(&engine, hash, from, budget, deliver).await.expect("read");
                    let recs = page
                        .recs
                        .iter()
                        .map(|rec| {
                            let mut h = std::collections::hash_map::DefaultHasher::new();
                            rec.payload[..].hash(&mut h);
                            (rec.off, h.finish())
                        })
                        .collect();
                    seen.push((from, hash, recs, page.completed, page.end));
                    tokio::task::yield_now().await;
                }
                seen
            })
        })
        .collect();
    for w in writers {
        w.await.unwrap();
    }
    done.store(true, Ordering::Release);
    let seals = sealer.await.unwrap();
    engine.spill_once(Trigger::Manual).await.expect("final seal");
    let oracle = oracle.lock().unwrap().clone();
    let hashed: HashMap<[u8; 16], BTreeMap<u64, u64>> = oracle
        .iter()
        .map(|(h, recs)| {
            let m = recs
                .iter()
                .map(|(off, rec)| {
                    let mut hh = std::collections::hash_map::DefaultHasher::new();
                    rec[..].hash(&mut hh);
                    (*off, hh.finish())
                })
                .collect();
            (*h, m)
        })
        .collect();
    let (mut pages, mut partials) = (0u64, 0u64);
    for reader in readers {
        for (from, hash, recs, completed, end) in reader.await.unwrap() {
            pages += 1;
            let o = &hashed[&hash];
            let mut expect = from;
            for (off, h) in &recs {
                assert_eq!(*off, expect, "seed {seed}: from {from}: a gap or a duplicate");
                assert_eq!(o.get(off), Some(h), "seed {seed}: from {from}: offset {off} differs from the record acked");
                expect += 1;
            }
            if completed {
                assert!(recs.is_empty() || expect == end, "seed {seed}: from {from}: complete page ends at {expect} of {end}");
            } else {
                partials += 1;
            }
        }
    }
    for &hash in &streams {
        for deliver in [Deliver::Durable, Deliver::Applied] {
            let mut from = 0;
            let o = &oracle[&hash];
            while from < o.len() as u64 {
                let page = merged(&engine, hash, from, 1 << 20, deliver).await.unwrap();
                let n = check_page(o, from, &page).unwrap_or_else(|e| panic!("seed {seed}: drain: {e}"));
                assert!(n > 0, "seed {seed}: the drain made no progress at {from}");
                from += n as u64;
            }
        }
    }
    let r = reads(&engine);
    assert_eq!(get(&r.missing_row_partials), 0, "seed {seed}: a read at its own visibility met a missing row");
    let out = (pages, partials, get(&r.race_adoptions), get(&r.calls));
    println!(
        "race seed {seed} ring {ring} chunk cap {max_pages}: {seals} seals, {} pages read ({} partial), race adoptions {}, sealed-range reads {}, GETs {}",
        out.0,
        out.1,
        out.2,
        out.3,
        get(&r.gets)
    );
    engine.begin_close();
    out
}

/// DST 1 (DESIGN §3.2 item 7): a zero-stale reader racing a seal's advance
/// and trims gets exact data or an honest partial. The held-WAL race serves
/// the applied reader exact data from the index rows at Memory; random
/// races (four seeds, ring on and off) never yield a gap, a duplicate, a
/// wrong byte or a missing row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dst_a_zero_stale_reader_racing_a_seal_gets_exact_data_or_an_honest_partial() {
    let race = held_wal_race("zerostale", None).await;
    let n = race.oracle.len() as u64;
    for (what, page) in [
        ("applied, during", &race.applied),
        ("durable, during", &race.durable),
        ("applied, after", &race.after_applied),
        ("durable, after", &race.after_durable),
    ] {
        assert_eq!(check_page(&race.oracle, 0, page).unwrap_or_else(|e| panic!("{what}: {e}")), n as usize, "{what}: exact data");
        assert!(page.completed, "{what}: complete");
    }
    assert_eq!(race.missing_rows, 0, "the index rows at Memory hold the hole's records");
    assert!(race.race_adoptions >= 1, "the applied reader met the trims and adopted the Memory boundary");
    let mut adopted = 0;
    let mut sealed_reads = 0;
    for (seed, ring) in [(11, false), (12, true), (13, false), (14, true)] {
        let (pages, _, races, calls) = random_race(seed, ring).await;
        assert!(pages > 100, "seed {seed}: too few reads ({pages})");
        adopted += races;
        sealed_reads += calls;
    }
    assert!(sealed_reads > 0, "no read reached the sealed ranges");
    assert!(adopted > 0, "no reader raced a seal's trims");
}

/// DST 2, the negative control (rBc G2-V): the same race with the index
/// read at Remote while the boundary comes from Memory. The reader finds no
/// index row for the records the Memory boundary says are sealed: the hole.
/// With the honest-partial rule it serves nothing and claims nothing (a
/// reader that took the hole for consumed or expired would skip acked
/// records); the zero-stale DST's exact-data assertion would fail here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dst_negative_control_index_at_remote_with_the_boundary_at_memory_finds_the_hole() {
    let race = held_wal_race("negctl", Some(DurabilityLevel::Remote)).await;
    assert!(race.missing_rows >= 1, "the hole was not found: no missing index row below the boundary");
    assert!(race.race_adoptions >= 1, "the applied reader adopted the Memory boundary");
    assert!(
        race.applied.recs.is_empty() && !race.applied.completed && race.applied.last.is_none(),
        "the hole must be an honest partial: delivered {}, completed {}, last {:?}",
        race.applied.recs.len(),
        race.applied.completed,
        race.applied.last
    );
    assert!(
        check_page(&race.oracle, 0, &race.applied).map_or(true, |n| n < race.oracle.len()),
        "the control served exact data: it did not reproduce the hole"
    );
    // The durable reader is unaffected (its boundary and rows are Remote),
    // and once the commit is durable the Remote rows exist.
    let n = race.oracle.len();
    assert_eq!(check_page(&race.oracle, 0, &race.durable).unwrap(), n);
    assert_eq!(check_page(&race.oracle, 0, &race.after_applied).unwrap(), n);
    assert_eq!(check_page(&race.oracle, 0, &race.after_durable).unwrap(), n);
}

/// The DST fixture honours `L6_SPILL=1`: a fixture's `Absorber::start` on a
/// spill-arm engine starts no absorber (history2 is never opened) and puts
/// the sealer at the absorber's cadence; aborting the returned task stops
/// sealing, as aborting an absorber stops absorbing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dst_fixture_absorber_is_the_sealer_at_its_cadence() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let settings = SpillSettings {
        auto: true,
        ..SpillSettings::default()
    };
    let (engine, _) = engine_on(store, "cadence", settings, false, Duration::from_millis(2)).await;
    let (_tx, rx) = crate::history::absorber_channel();
    let absorber = crate::history::Absorber::start(
        engine.clone(),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: Duration::from_millis(1),
            tick: Duration::from_millis(20),
            ..Default::default()
        },
        rx,
    );
    let sp = engine.spill.clone().unwrap();
    assert_eq!(get(&sp.stats.absorber_cadences), 1);
    let mut rng = StdRng::seed_from_u64(7);
    let hash = hashes(&mut rng, 1)[0];
    let absorbed = async |engine: &Arc<ShardEngine>| {
        let h = engine.stream_handle(hash).await.unwrap();
        let st = h.state.lock().unwrap();
        (st.durable.absorbed, st.durable.next)
    };
    append(&engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    let t0 = std::time::Instant::now();
    while absorbed(&engine).await != (1, 1) {
        assert!(t0.elapsed() < Duration::from_secs(10), "the cadence never sealed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(engine.db.get(index_key(&hash, 0)).await.unwrap().is_some(), "sealed: an index row");
    assert!(engine.history_partition_if_open().is_none(), "history2 never opened");
    absorber.abort();
    let _ = absorber.await;
    append(&engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(absorbed(&engine).await, (1, 2), "an aborted fixture absorber stops the sealer");
    engine.begin_close();
}
