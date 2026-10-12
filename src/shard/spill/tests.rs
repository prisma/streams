//! L6 spike M3 acceptance (SPIKE-PLAN §3 M3): the flushdrop probe, the
//! tiling property with an abort between steps, and the freeze accounting
//! over 10,000 random appends. In process, dev profile, in-memory store;
//! each engine has its own shard path, so the process-wide metric and
//! sealer registries never mix two tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::{Rng, SeedableRng, rngs::StdRng};
use slatedb::config::Settings;
use slatedb::db_stats;

use super::{SealError, SealStep, SpillState};
use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::{CheckedPage, shard_page_prefix};
use crate::shard::{RingBudget, RingSettings, ShardConfig, ShardEngine, ShardMaintenance};
use crate::spill::index::{IndexRow, index_prefix, parse_index_key};
use crate::spill::object::{ObjectId, decode_chunk, object_path};
use crate::spill::settings::{Deadline, SpillSettings, Trigger, TriggerMode};

pub(super) struct Rig {
    pub(super) engine: Arc<ShardEngine>,
    pub(super) store: Arc<object_store::memory::InMemory>,
    pub(super) path: String,
    pub(super) metrics: Arc<crate::l6_metrics::ShardDbMetrics>,
}

impl Rig {
    fn sp(&self) -> Arc<SpillState> {
        self.engine.spill.clone().expect("spill arm")
    }

    fn counter(&self, metric: &str) -> u64 {
        self.metrics.counter(metric)
    }
}

fn unique(label: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("l6m3-{label}-{nanos:x}-{}", N.fetch_add(1, Ordering::Relaxed))
}

/// Sealer settings for a test that drives `spill_once` itself.
pub(super) fn manual() -> SpillSettings {
    SpillSettings {
        auto: false,
        ..SpillSettings::default()
    }
}

/// A spill-arm engine on an in-memory store with the shard DB's L0 and
/// unflushed-bytes settings, SlateDB's metrics recorder, the group-commit
/// pump at `gap`, a 64/8/2 MiB ring budget and no compactor (L0 SSTs are
/// what the tests inspect; the L0 caps are raised so none stalls).
pub(super) async fn rig(label: &str, settings: SpillSettings, gap: Duration) -> Rig {
    let prefix = unique(label);
    let path = crate::sharddir::shard_db_path(&prefix);
    let metrics = crate::l6_metrics::for_db(&path);
    let store = Arc::new(object_store::memory::InMemory::new());
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
        shared_ring: Some(RingBudget::new(RingSettings {
            process_bytes: 64 << 20,
            shard_cap_bytes: 8 << 20,
            shard_floor_bytes: 2 << 20,
        })),
        wal_group_commit: true,
        wal_flush_gap: gap,
        spill_settings: settings,
        ..Default::default()
    };
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        prefix,
        db,
        store.clone(),
        cfg,
        tx,
        None,
        ShardMaintenance::default(),
    );
    Rig {
        engine,
        store,
        path,
        metrics,
    }
}

pub(super) fn random_bytes(rng: &mut StdRng, len: usize) -> Bytes {
    let mut v = vec![0u8; len];
    rng.fill(&mut v[..]);
    Bytes::from(v)
}

/// A length drawn log-uniformly from `[lo, hi]`.
pub(super) fn log_uniform(rng: &mut StdRng, lo: usize, hi: usize) -> usize {
    let (a, b) = ((lo as f64).ln(), (hi as f64).ln());
    (rng.random_range(a..=b).exp() as usize).clamp(lo, hi)
}

/// One append of `records` to `hash`, acked once durable: its last offset.
pub(super) async fn append(engine: &Arc<ShardEngine>, hash: [u8; 16], records: Vec<Bytes>) -> u64 {
    append_lane(engine, hash, "", records).await
}

/// `append` under routing key `rk` (M4's lane test).
pub(super) async fn append_lane(engine: &Arc<ShardEngine>, hash: [u8; 16], rk: &str, records: Vec<Bytes>) -> u64 {
    let subkey = derive_subkey(&StreamKey([7; 32]), &hash, rk, 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: Instant::now(),
        hash,
        route: hash,
        entries: records,
        usage: crate::usage::counters(&hash),
        routing_key: rk.to_string(),
        key_hash: crate::crypto::stream_hash(rk),
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

/// Hashes for `n` streams, some with a first byte at the tags' neighbours
/// (0x4F, 0x50) or at 0xFF.
pub(super) fn hashes(rng: &mut StdRng, n: usize) -> Vec<[u8; 16]> {
    (0..n)
        .map(|i| {
            let mut h = [0u8; 16];
            rng.fill(&mut h[..]);
            match i % 5 {
                1 => h[0] = 0x4F,
                2 => h[0] = 0x50,
                3 => h[0] = 0xFF,
                _ => {}
            }
            h
        })
        .collect()
}

/// The shard's segment objects: every `hseg/` object under the shard path,
/// by id, with its size.
async fn hseg_objects(rig: &Rig) -> BTreeMap<ObjectId, u64> {
    let prefix = object_store::path::Path::from(format!("{}/hseg", rig.path));
    let mut out = BTreeMap::new();
    let mut list = rig.store.list(Some(&prefix));
    while let Some(meta) = list.next().await {
        let meta = meta.unwrap();
        let parts: Vec<_> = meta.location.parts().map(|p| p.as_ref().to_string()).collect();
        let n = parts.len();
        let epoch = u64::from_str_radix(&parts[n - 2], 16).unwrap();
        let seq = u64::from_str_radix(&parts[n - 1], 16).unwrap();
        out.insert(ObjectId { epoch, seq }, meta.size);
    }
    out
}

/// Bytes of the L0 SSTs under the shard path (`compacted/`).
async fn compacted_bytes(rig: &Rig) -> BTreeMap<String, u64> {
    let prefix = object_store::path::Path::from(format!("{}/compacted", rig.path));
    let mut out = BTreeMap::new();
    let mut list = rig.store.list(Some(&prefix));
    while let Some(meta) = list.next().await {
        let meta = meta.unwrap();
        out.insert(meta.location.to_string(), meta.size);
    }
    out
}

/// What one stream holds in the shard DB after seals.
#[derive(Debug)]
pub(super) struct StreamCheck {
    pub(super) absorbed: u64,
    pub(super) next: u64,
    pub(super) rows: usize,
    pub(super) objects: BTreeSet<ObjectId>,
    /// The most pages any of the stream's chunks holds.
    pub(super) max_pages: u32,
}

/// The tiling invariant for one stream, at the shard DB's committed view
/// (every submitted advance settled first):
/// - the index rows tile `[0, absorbed)` exactly, in key order, with no
///   duplicate or overlapping row, and the tail's trim fields are at the
///   boundary (zero lag);
/// - each row's chunk, fetched by its offset and length alone, holds
///   exactly the row's pages, each admitted at its offsets;
/// - no page key below `absorbed` is left (one tombstone per sealed page),
///   and the pages left tile `[absorbed, next)`.
pub(super) async fn check_stream(rig: &Rig, hash: [u8; 16]) -> StreamCheck {
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let durable = handle.state.lock().unwrap().durable.clone();
    let (absorbed, next) = (durable.absorbed, durable.next);
    assert_eq!(durable.trim_safe_to, absorbed, "zero lag: trim_safe_to");
    assert_eq!(durable.trimmed, absorbed, "zero lag: trimmed");
    let prefix = index_prefix(&hash);
    let mut iter = rig.engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut expected = 0u64;
    let mut rows = 0usize;
    let mut objects = BTreeSet::new();
    let mut max_pages = 0u32;
    while let Some(kv) = iter.next().await.unwrap() {
        let (h, last) = parse_index_key(&kv.key).expect("an index key");
        assert_eq!(h, hash);
        let row = IndexRow::decode(&kv.value).unwrap();
        assert_eq!(row.last, last, "row keyed by its chunk's last offset");
        assert_eq!(row.first, expected, "rows tile with no gap or overlap");
        let at = object_store::path::Path::from(object_path(&rig.path, row.object));
        let range = row.chunk_off..row.chunk_off + u64::from(row.chunk_len);
        let raw = rig.store.get_range(&at, range).await.unwrap();
        let chunk = decode_chunk(&raw).unwrap();
        assert_eq!(chunk.first, row.first);
        assert_eq!(chunk.pages.len() as u32, row.page_count);
        let mut o = row.first;
        for ((last, page), count) in chunk.pages.iter().zip(&chunk.counts) {
            assert_eq!(*last, o + count - 1, "the directory's counts give each page's offsets");
            let admitted = CheckedPage::admit(page.clone(), *last).expect("a sealed page admits at its offsets");
            assert_eq!(admitted.first(), o);
            o = last + 1;
        }
        assert_eq!(o, row.last + 1, "the chunk's pages end at the row's last offset");
        expected = row.last + 1;
        rows += 1;
        max_pages = max_pages.max(row.page_count);
        objects.insert(row.object);
    }
    assert_eq!(expected, absorbed, "index rows tile [0, absorbed)");
    let page_prefix = shard_page_prefix(&hash);
    let mut pages = rig.engine.db.scan_prefix(&page_prefix[..], ..).await.unwrap();
    let mut o = absorbed;
    while let Some(kv) = pages.next().await.unwrap() {
        let page = CheckedPage::from_row(&kv.key, &page_prefix, kv.value).unwrap();
        assert_eq!(page.first(), o, "pages left tile [absorbed, next): no page below absorbed");
        o = page.last() + 1;
    }
    assert_eq!(o, next, "pages left end at next");
    StreamCheck {
        absorbed,
        next,
        rows,
        objects,
        max_pages,
    }
}

/// **The flushdrop probe.** 4 MiB of pages (log-uniform 300 B to 64 KiB) on
/// 8 streams, one seal, and the L0 its flush wrote: at most 5% of the sealed
/// page bytes (tombstones, index rows and the newest metadata only; the
/// design's reference is 46,638 B of 2,089,468 B, `gt-write-path.md` §8).
/// The control flushes another 4 MiB without a seal: its values reach the
/// L0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flushdrop() {
    let rig = rig("flushdrop", manual(), Duration::from_millis(5)).await;
    let sp = rig.sp();
    let mut rng = StdRng::seed_from_u64(0xF1D0);
    let streams = hashes(&mut rng, 8);
    let fill = |seed: u64| {
        let engine = rig.engine.clone();
        let streams = streams.clone();
        async move {
            let tasks: Vec<_> = streams
                .iter()
                .enumerate()
                .map(|(i, hash)| {
                    let (engine, hash) = (engine.clone(), *hash);
                    tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(seed * 100 + i as u64);
                        let mut bytes = 0usize;
                        while bytes < (512 << 10) {
                            let len = log_uniform(&mut rng, 240, 65_000);
                            bytes += len;
                            append(&engine, hash, vec![random_bytes(&mut rng, len)]).await;
                        }
                    })
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
        }
    };
    fill(1).await;
    let l0_before = compacted_bytes(&rig).await;
    let report = rig.engine.spill_once(Trigger::Manual).await.unwrap();
    let l0_after = compacted_bytes(&rig).await;
    let new_l0: Vec<u64> = l0_after
        .iter()
        .filter(|(k, _)| !l0_before.contains_key(*k))
        .map(|(_, v)| *v)
        .collect();
    let listed: u64 = new_l0.iter().sum();
    eprintln!(
        "L6 M3 flushdrop: sealed {} pages, {} B in {} chunks ({} streams, object {} B); \
         L0 written by the seal's flush: {} B by l0_flush_bytes, {} B by listing ({} SST); \
         {:.3}% of the sealed bytes; imm flushes {}; ring fallbacks {}",
        report.pages,
        report.page_bytes,
        report.chunks,
        report.streams,
        report.object_bytes,
        report.l0_bytes,
        listed,
        new_l0.len(),
        100.0 * report.l0_bytes as f64 / report.page_bytes as f64,
        report.imm_flushes,
        report.ring_fallbacks,
    );
    assert!(report.page_bytes >= 4 << 20, "{report:?}");
    assert_eq!(report.streams, 8);
    assert_eq!(report.ring_fallbacks, 0, "the ring held every page");
    assert_eq!(report.imm_flushes, 1);
    assert_eq!(new_l0.len(), 1, "one L0 SST");
    assert_eq!(listed, report.l0_bytes, "l0_flush_bytes is the SST the flush wrote");
    assert!(
        report.l0_bytes * 100 <= report.page_bytes * 5,
        "the L0 holds {} B of {} B sealed",
        report.l0_bytes,
        report.page_bytes
    );
    for hash in &streams {
        let c = check_stream(&rig, *hash).await;
        assert_eq!(c.absorbed, c.next, "every page sealed");
        assert_eq!(c.rows, 1);
    }
    // Control: the same load again, flushed without a seal.
    let pages_before = rig.engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(pages_before, 0);
    fill(2).await;
    let unsealed = rig.engine.maintenance_snapshot().unabsorbed_frame_bytes;
    let l0 = rig.counter(db_stats::L0_FLUSH_BYTES);
    rig.engine.spill_deadline_flush(&sp, Deadline::Final).await;
    let control = rig.counter(db_stats::L0_FLUSH_BYTES) - l0;
    eprintln!(
        "L6 M3 flushdrop control (no seal): {unsealed} B of pages flushed; L0 {control} B ({:.1}%)",
        100.0 * control as f64 / unsealed as f64
    );
    assert!(control >= unsealed, "without the seal the values reach the L0");
}

/// One random seal of the property test, stopped at `abort` if any.
async fn seal_with_abort(rig: &Rig, abort: Option<SealStep>) -> Result<(), SealError> {
    *rig.sp().abort_at.lock().unwrap() = abort;
    let result = rig.engine.spill_once(Trigger::Manual).await.map(|_| ());
    *rig.sp().abort_at.lock().unwrap() = None;
    result
}

/// **Tiling with aborts.** Random appends to random streams, seals at
/// random points, and a seal stopped at a random step: after the WAL flush,
/// after the PUT, after the commit is applied, after it is durable (W2:
/// before the memtable flush), or not at all. After every seal, once its
/// advances settle: every stream's index rows tile `[0, absorbed)` exactly
/// with no duplicate row (`check_stream`), and the shard's `hseg/` objects
/// are the referenced ones plus exactly one per seal stopped after its PUT
/// (its commit never lands). A final seal absorbs everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiling_with_an_abort_between_every_pair_of_steps() {
    const ABORTS: [Option<SealStep>; 5] = [
        None,
        Some(SealStep::AfterWalFlush),
        Some(SealStep::AfterPut),
        Some(SealStep::AfterApplied),
        Some(SealStep::AfterDurable),
    ];
    let mut aborted_at = [0usize; 5];
    for seed in 0..16u64 {
        let mut rng = StdRng::seed_from_u64(0x7111 ^ seed);
        let mut settings = manual();
        // Small chunks and groups so the splits run too.
        settings.max_pages_per_chunk = rng.random_range(2..=6);
        settings.max_streams_per_group = rng.random_range(1..=3);
        let rig = rig("tiling", settings, Duration::from_millis(2)).await;
        let n = rng.random_range(2..=6);
        let streams = hashes(&mut rng, n);
        let mut orphans = 0usize;
        let mut seals = 0usize;
        for _ in 0..60 {
            if rng.random_bool(0.6) {
                // 1-4 appends of 1-3 records of up to 20 KiB to one stream.
                let hash = streams[rng.random_range(0..streams.len())];
                for _ in 0..rng.random_range(1..=4) {
                    let records = (0..rng.random_range(1..=3))
                        .map(|_| {
                            let len = log_uniform(&mut rng, 16, 20_000);
                            random_bytes(&mut rng, len)
                        })
                        .collect();
                    append(&rig.engine, hash, records).await;
                }
                continue;
            }
            let pick = rng.random_range(0..ABORTS.len());
            let pending_pages = rig.engine.maintenance_snapshot().unabsorbed_frame_bytes > 0;
            let result = seal_with_abort(&rig, ABORTS[pick]).await;
            seals += 1;
            match (ABORTS[pick], result) {
                (None, Ok(())) => aborted_at[pick] += 1,
                (Some(step), Err(SealError::Aborted(at))) if at == step => aborted_at[pick] += 1,
                // Nothing to seal: the seal never reaches a PUT or a commit.
                (Some(SealStep::AfterPut | SealStep::AfterApplied | SealStep::AfterDurable), Ok(()))
                    if !pending_pages => {}
                (want, got) => panic!("seed {seed}: abort {want:?} gave {got:?}"),
            }
            if ABORTS[pick] == Some(SealStep::AfterPut) && pending_pages {
                orphans += 1;
            }
            rig.engine.spill_wait_settled(&rig.sp()).await.unwrap();
            let objects = hseg_objects(&rig).await;
            let mut referenced = BTreeSet::new();
            for hash in &streams {
                let c = check_stream(&rig, *hash).await;
                assert!(c.max_pages as usize <= settings.max_pages_per_chunk, "seed {seed}: a chunk over the page cap");
                referenced.extend(c.objects);
            }
            let unreferenced: Vec<_> = objects.keys().filter(|id| !referenced.contains(id)).collect();
            assert!(referenced.iter().all(|id| objects.contains_key(id)), "seed {seed}: a row names a missing object");
            assert_eq!(unreferenced.len(), orphans, "seed {seed}: one unreferenced object per PUT whose commit never landed");
        }
        // The last seal absorbs everything left.
        seal_with_abort(&rig, None).await.unwrap();
        rig.engine.spill_wait_settled(&rig.sp()).await.unwrap();
        let mut total_rows = 0;
        for hash in &streams {
            let c = check_stream(&rig, *hash).await;
            assert_eq!(c.absorbed, c.next, "seed {seed}: the final seal absorbs everything");
            total_rows += c.rows;
        }
        let stats = &rig.sp().stats;
        eprintln!(
            "L6 M3 tiling seed {seed}: {} streams, {seals} seals, {} index rows, {orphans} orphans, \
             {} objects, groups {} (max streams {} / tombstones {}), detached {}",
            streams.len(),
            total_rows,
            hseg_objects(&rig).await.len(),
            stats.groups.load(Ordering::Relaxed),
            stats.streams_per_group_max.load(Ordering::Relaxed),
            stats.tombstones_per_group_max.load(Ordering::Relaxed),
            stats.advances_detached.load(Ordering::Relaxed),
        );
        assert_eq!(stats.advances_detached.load(Ordering::Relaxed), 0);
        assert_eq!(stats.side_table_missing.load(Ordering::Relaxed), 0);
    }
    eprintln!("L6 M3 tiling seals by abort point (completed, after wal flush, after put, after applied, after durable): {aborted_at:?}");
    assert!(aborted_at.iter().all(|n| *n > 0), "every abort point ran: {aborted_at:?}");
}

/// The freeze accounting's shape: how big pages are and how many
/// lease-sized side rows ride with each append.
#[derive(Clone, Copy, Debug)]
pub(super) struct Shape {
    pub(super) appends: usize,
    pub(super) streams: usize,
    pub(super) writers: usize,
    pub(super) page_lo: usize,
    pub(super) page_hi: usize,
    pub(super) side_rows: (usize, usize),
}

#[derive(Debug, Default)]
pub(super) struct FreezeOutcome {
    pub(super) total: u64,
    pub(super) spill_attributed: u64,
    pub(super) deadline_attributed: u64,
    pub(super) excess: u64,
    pub(super) seals: [u64; 6],
    pub(super) deadlines: (u64, u64, u64),
    pub(super) failures: u64,
    pub(super) mem_max: u64,
    pub(super) wal_max: u64,
    pub(super) page_bytes: u64,
    pub(super) side_bytes: u64,
    pub(super) leak_bytes: u64,
    pub(super) backpressure: u64,
    pub(super) preempted: u64,
    pub(super) secs: f64,
}

/// Run `shape` against a spill-arm engine whose sealer runs by itself with
/// `mode`, then a last seal; every forced flush is attributed by delta.
pub(super) async fn freeze_run(label: &str, mode: TriggerMode, shape: Shape, seed: u64) -> FreezeOutcome {
    freeze_run_at(label, mode, shape, seed, Duration::from_millis(100)).await
}

/// `freeze_run` at WAL gap `gap` (100 ms is today's tier, 1 s the E6 tier).
pub(super) async fn freeze_run_at(
    label: &str,
    mode: TriggerMode,
    shape: Shape,
    seed: u64,
    gap: Duration,
) -> FreezeOutcome {
    let settings = SpillSettings {
        mode,
        auto: true,
        ..SpillSettings::default()
    };
    let rig = Arc::new(rig(label, settings, gap).await);
    let mut rng = StdRng::seed_from_u64(seed);
    let streams = Arc::new(hashes(&mut rng, shape.streams));
    let issued = Arc::new(AtomicU64::new(0));
    let side_bytes = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let tasks: Vec<_> = (0..shape.writers)
        .map(|w| {
            let (rig, streams, issued, side_bytes) =
                (rig.clone(), streams.clone(), issued.clone(), side_bytes.clone());
            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed * 1000 + w as u64);
                while issued.fetch_add(1, Ordering::Relaxed) < shape.appends as u64 {
                    let hash = streams[rng.random_range(0..streams.len())];
                    let len = log_uniform(&mut rng, shape.page_lo, shape.page_hi);
                    let last = append(&rig.engine, hash, vec![random_bytes(&mut rng, len)]).await;
                    // Lease-sized side rows: one lease row per delivered
                    // record of a consumer group, 60-80 B values.
                    for _ in 0..rng.random_range(shape.side_rows.0..=shape.side_rows.1) {
                        let key = crate::queue::lease_key(&hash, "g", 1, last + rng.random_range(0..1_000_000));
                        let vlen = rng.random_range(60..=80);
                        let value = random_bytes(&mut rng, vlen);
                        side_bytes.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
                        rig.engine.db.put(key, value).await.unwrap();
                    }
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    // The last seal: everything left, and its flush, attributed like any.
    rig.engine.spill_once(Trigger::Manual).await.unwrap();
    let secs = t0.elapsed().as_secs_f64();
    let sp = rig.sp();
    let s = &sp.stats;
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    let out = FreezeOutcome {
        total: rig.counter(db_stats::IMMUTABLE_MEMTABLE_FLUSHES),
        spill_attributed: g(&s.spill_flush_attributed),
        deadline_attributed: g(&s.deadline_flush_attributed),
        excess: g(&s.forced_flush_excess),
        seals: std::array::from_fn(|i| g(&s.seals_started[i])),
        deadlines: (
            g(&s.deadline_flushes_wal),
            g(&s.deadline_flushes_age),
            g(&s.deadline_flushes_final),
        ),
        failures: g(&s.seal_failures),
        mem_max: g(&s.mem_estimate_at_flush_max),
        wal_max: g(&s.wal_objects_at_flush_max),
        page_bytes: g(&s.page_bytes_sealed),
        side_bytes: side_bytes.load(Ordering::Relaxed),
        leak_bytes: g(&s.leak_bytes),
        backpressure: rig.counter(db_stats::BACKPRESSURE_COUNT),
        preempted: g(&s.deadline_preempted_seal),
        secs,
    };
    for hash in streams.iter() {
        let c = check_stream(&rig, *hash).await;
        assert_eq!(c.absorbed, c.next, "{label}: the last seal absorbs everything");
    }
    eprintln!("L6 M3 freeze {label} ({mode:?}, {shape:?}, gap {gap:?}): {out:?}");
    eprintln!("L6 M3 freeze {label} stats: {}", sp.stats.json());
    out
}

/// **SlateDB never freezes on its own.** 10,000 random appends of 300 B to
/// 64 KiB pages (log-uniform) to 32 streams from 16 writers at the 100 ms
/// tier, each with 0-3 lease-sized side rows: every immutable memtable
/// SlateDB flushed is one the sealer's flushes or deadlines forced
/// (`immutable_memtable_flushes` = spill flushes + deadline flushes, each
/// attributed by its own delta, none carrying an own freeze).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn immutable_flushes_are_spill_plus_deadline_flushes() {
    let shape = Shape {
        appends: 10_000,
        streams: 32,
        writers: 16,
        page_lo: 240,
        page_hi: 65_000,
        side_rows: (0, 3),
    };
    let out = freeze_run("freeze", TriggerMode::Memtable, shape, 0xF2EE).await;
    assert_eq!(out.failures, 0);
    assert_eq!(out.excess, 0, "a forced flush carried an own freeze");
    assert_eq!(
        out.total,
        out.spill_attributed + out.deadline_attributed,
        "SlateDB froze a memtable on its own: {out:?}"
    );
    assert!(out.seals[Trigger::Memtable.index()] > 0, "the memtable trigger sealed: {out:?}");
}

/// Wait until `cond` holds, polling every 20 ms, for at most `secs`.
async fn wait_for(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cond()
}

/// **The deadlines hold when a seal fails or stalls** (review B1, rBc
/// G6-Q). Small pages at a 10 ms WAL gap with the WAL-object trigger at 6
/// and its deadline at 8 objects, so the WAL dimension drives everything:
/// - every object PUT fails: seals fail, appends keep acking, and the
///   deadline flushes the memtable (values into the L0, degraded mode);
/// - every PUT stalls 1.5 s: the deadline flushes while the seal runs;
/// - then normal seals resume, and a last seal absorbs everything.
/// Throughout, every immutable memtable is a forced flush's, attributed by
/// its delta.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlines_flush_when_a_seal_fails_or_stalls() {
    let settings = SpillSettings {
        auto: true,
        wal_objects_seal: 6,
        wal_objects_deadline: 8,
        tick: Duration::from_millis(50),
        ..SpillSettings::default()
    };
    let rig = Arc::new(rig("deadline", settings, Duration::from_millis(10)).await);
    let sp = rig.sp();
    let mut rng = StdRng::seed_from_u64(0xDEAD);
    let streams = Arc::new(hashes(&mut rng, 8));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..8)
        .map(|w| {
            let (rig, streams, stop, acked) = (rig.clone(), streams.clone(), stop.clone(), acked.clone());
            tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(w);
                while !stop.load(Ordering::Relaxed) {
                    let hash = streams[rng.random_range(0..streams.len())];
                    let len = rng.random_range(200..2_000);
                    append(&rig.engine, hash, vec![random_bytes(&mut rng, len)]).await;
                    acked.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    // Phase 1: every PUT fails.
    sp.fail_puts.store(true, Ordering::Relaxed);
    let a0 = g(&acked);
    assert!(
        wait_for(30, || g(&sp.stats.seal_failures) >= 3 && g(&sp.stats.deadline_flushes_wal) >= 3).await,
        "failed seals and WAL deadlines: {}",
        sp.stats.json()
    );
    let failed_phase_acks = g(&acked) - a0;
    let objects_failed_phase = g(&sp.stats.objects_put);
    sp.fail_puts.store(false, Ordering::Relaxed);
    // Phase 2: every PUT stalls 1.5 s.
    *sp.put_delay.lock().unwrap() = Duration::from_millis(1_500);
    assert!(
        wait_for(30, || g(&sp.stats.deadline_preempted_seal) >= 2).await,
        "deadlines during a stalled seal: {}",
        sp.stats.json()
    );
    *sp.put_delay.lock().unwrap() = Duration::ZERO;
    // Phase 3: normal seals.
    let done = g(&sp.stats.seals_completed);
    assert!(wait_for(30, || g(&sp.stats.seals_completed) >= done + 3).await);
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }
    rig.engine.spill_once(Trigger::Manual).await.unwrap();
    let total = rig.counter(db_stats::IMMUTABLE_MEMTABLE_FLUSHES);
    let attributed = g(&sp.stats.spill_flush_attributed) + g(&sp.stats.deadline_flush_attributed);
    eprintln!(
        "L6 M3 deadlines: acks during failed PUTs {failed_phase_acks} (objects put then {objects_failed_phase}); \
         imm flushes {total}, attributed {attributed}; {}",
        sp.stats.json()
    );
    assert!(failed_phase_acks > 0, "appends ack while seals fail");
    assert_eq!(objects_failed_phase, 0);
    assert_eq!(g(&sp.stats.forced_flush_excess), 0);
    assert_eq!(total, attributed, "every immutable memtable is a forced flush's");
    for hash in streams.iter() {
        let c = check_stream(&rig, *hash).await;
        assert_eq!(c.absorbed, c.next);
    }
}

/// **The age trigger and the age deadline.** One append, then nothing: the
/// age trigger seals it. With every PUT failing, the age deadline flushes
/// the memtable instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_age_trigger_seals_a_quiet_stream_and_the_age_deadline_flushes() {
    let settings = SpillSettings {
        auto: true,
        age_seal: Duration::from_millis(600),
        age_deadline: Duration::from_millis(1_200),
        tick: Duration::from_millis(50),
        ..SpillSettings::default()
    };
    let rig = rig("age", settings, Duration::from_millis(5)).await;
    let sp = rig.sp();
    let mut rng = StdRng::seed_from_u64(0xA6E);
    let hash = hashes(&mut rng, 1)[0];
    let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 1_000)]).await;
    let t = Instant::now();
    assert!(wait_for(10, || g(&sp.stats.seals_completed) >= 1).await);
    let sealed_after = t.elapsed();
    assert_eq!(g(&sp.stats.seals_started[Trigger::Age.index()]), 1);
    let c = check_stream(&rig, hash).await;
    assert_eq!((c.absorbed, c.rows), (c.next, 1));
    // The deadline: seals fail, the memtable is flushed at 1.2 s.
    sp.fail_puts.store(true, Ordering::Relaxed);
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 1_000)]).await;
    assert!(wait_for(10, || g(&sp.stats.deadline_flushes_age) >= 1).await, "{}", sp.stats.json());
    sp.fail_puts.store(false, Ordering::Relaxed);
    // Failed seals back off (0.5 s, doubling): a handful of attempts in the
    // ~1.2 s to the deadline, not one per wake.
    assert!(g(&sp.stats.seal_failures) <= 4, "seal retries back off: {}", sp.stats.json());
    assert!(wait_for(10, || g(&sp.stats.seals_completed) >= 2).await);
    eprintln!("L6 M3 age: sealed {sealed_after:?} after the append; {}", sp.stats.json());
    assert!(sealed_after >= Duration::from_millis(600));
    let c = check_stream(&rig, hash).await;
    assert_eq!(c.absorbed, c.next);
}

/// **The trigger fix against its control** (review B2/B3, rBn F1): a
/// consumer-group-heavy shape at the 1 s tier, small pages (300 B to 1 KiB)
/// each with 4-6 lease-sized rows, so the rows outweigh the pages. With the
/// control trigger (`pages`, the design's original: 6 MiB of unabsorbed
/// page bytes) SlateDB freezes the memtable on its own first; with the
/// memtable trigger in SlateDB's units it never does.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_pages_control_lets_slatedb_freeze_first_on_a_lease_heavy_shape() {
    let shape = Shape {
        appends: 12_000,
        streams: 16,
        writers: 384,
        page_lo: 240,
        page_hi: 1_000,
        side_rows: (4, 6),
    };
    let fixed = freeze_run_at("lease-heavy", TriggerMode::Memtable, shape, 0xF1, Duration::from_secs(1)).await;
    let control = freeze_run_at("lease-heavy-pages", TriggerMode::Pages, shape, 0xF1, Duration::from_secs(1)).await;
    // A forced flush that carried an own freeze counted one (its excess is
    // in `total` and not attributed), so this is every own freeze.
    let own = |o: &FreezeOutcome| o.total - o.spill_attributed - o.deadline_attributed;
    eprintln!(
        "L6 M3 F1 shape: memtable trigger own freezes {}, pages control own freezes {}",
        own(&fixed),
        own(&control)
    );
    assert_eq!(own(&fixed), 0, "{fixed:?}");
    assert!(own(&control) > 0, "the control should let SlateDB freeze first: {control:?}");
}
