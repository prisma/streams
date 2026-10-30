#![cfg(test)]
//! Read-triggered consolidation (layout 6, DESIGN-6A §2.4 (d)):
//! - a cold walk over many small chunks consolidates them into one chunk:
//!   the next walk costs one GET, the superseded chunks are counted as
//!   orphans durably (the `'O'` row, then the fold's pointer row, then a
//!   restart), and every record reads back exactly;
//! - consolidations racing appends, seals, folds and other reads lose and
//!   duplicate nothing;
//! - which runs of a read's chunks are worth consolidating.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::Trigger;
use super::consolidate::{MIN_CHUNKS, SMALL_CHUNK, runs_to_consolidate};
use super::tests::{
    Oracle, ReadAt, append_recorded, hashes, manual, merged, random_bytes, read_back, reopen,
    rig_with,
};
use crate::shard::{Deliver, ShardEngine};
use crate::spill::directory::{DirEntry, KeySummary};
use crate::spill::object::ObjectId;

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

async fn wait_done(engine: &Arc<ShardEngine>, n: u64) {
    let c = &engine.layout.seal.fold.consolidator;
    let t0 = Instant::now();
    while get(&c.done) + get(&c.skipped) + get(&c.failed) < n {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the consolidator never finished"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_cold_walk_consolidates_small_chunks_and_counts_the_orphans() {
    let rig = rig_with("consolidate", manual(), 0, Duration::from_millis(2)).await;
    let hash = [0x37; 16];
    let oracle = Mutex::new(Oracle::new());
    let mut rng = StdRng::seed_from_u64(0xD0);
    for i in 0..24 {
        append_recorded(
            &rig.engine,
            &oracle,
            hash,
            vec![random_bytes(&mut rng, 500)],
        )
        .await;
        rig.engine.seal_once(Trigger::Requested).await.unwrap();
        if i == 11 {
            rig.engine.fold_once().await.unwrap();
        }
    }
    let oracle = oracle.into_inner().unwrap();
    let o = &oracle[&hash];
    let entries_before: u64 = 24;
    let c = &rig.engine.layout.seal.fold.consolidator;
    read_back(&rig.engine, hash, o, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(
        get(&c.offered),
        1,
        "one walk over 24 small chunks offers one job"
    );
    wait_done(&rig.engine, 1).await;
    assert_eq!(get(&c.done), 1);
    assert_eq!(get(&c.chunks_in), entries_before);
    let orphans = rig.engine.layout.seal.fold.orphans();
    assert_eq!(
        orphans.chunks, 24,
        "the superseded chunks are counted as orphans"
    );
    let gets = &rig.engine.layout.seal.counters.sealed_gets;
    let before = get(gets);
    read_back(&rig.engine, hash, o, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(get(gets) - before, 1, "the next cold walk costs one GET");
    assert_eq!(
        get(&c.offered),
        1,
        "a consolidated stream offers nothing more"
    );
    let report = rig.engine.fold_once().await.unwrap();
    assert_eq!(
        report.rows, 13,
        "the twelve seals left and the consolidation's row"
    );
    assert_eq!(
        report.dropped, 12,
        "the chunks the consolidated one contains in this fold"
    );
    assert_eq!(
        rig.engine.layout.seal.fold.orphans(),
        orphans,
        "the orphan row folded into the pointer row"
    );
    let rig = reopen(rig).await;
    assert_eq!(
        rig.engine.layout.seal.fold.orphans(),
        orphans,
        "the orphan gauge is durable"
    );
    let counters = &rig.engine.layout.seal.counters;
    let fetches = || get(&counters.sealed_gets) + get(&counters.chunk_cache_hits);
    let before = fetches();
    read_back(&rig.engine, hash, o, Deliver::Applied)
        .await
        .unwrap();
    assert_eq!(
        fetches() - before,
        1,
        "one chunk, from the store or the chunk cache"
    );
    rig.engine.begin_close();
}

/// Appends and seals on one task, cold walks on three, folds on another,
/// consolidations behind the walks; then a restart and every record read
/// back exactly at both visibilities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn consolidations_racing_reads_seals_and_folds_lose_nothing() {
    let rig = rig_with("consolidate-race", manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xC0A1);
    let streams = hashes(&mut rng, 3);
    let oracle = Arc::new(Mutex::new(Oracle::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let walkers = futures_util::future::join_all((0..3u64).map(|w| {
        walker(
            rig.engine.clone(),
            oracle.clone(),
            stop.clone(),
            streams.clone(),
            w,
        )
    }));
    let appender = async {
        for _ in 0..120 {
            let hash = streams[rng.random_range(0..streams.len())];
            let len = rng.random_range(50..2000);
            let records = vec![random_bytes(&mut rng, len)];
            append_recorded(&rig.engine, &oracle, hash, records).await;
            let seal = rng.random_bool(0.5);
            seal_if(&rig.engine, seal).await;
        }
        stop.store(true, Ordering::Relaxed);
    };
    tokio::join!(appender, walkers, folder(rig.engine.clone(), stop.clone()));
    let c = &rig.engine.layout.seal.fold.consolidator;
    wait_done(&rig.engine, get(&c.offered)).await;
    assert_eq!(get(&c.failed), 0);
    assert!(get(&c.done) > 0, "the walks consolidated something");
    let rig = reopen(rig).await;
    let oracle = oracle.lock().unwrap().clone();
    for (hash, o) in &oracle {
        for deliver in [Deliver::Durable, Deliver::Applied] {
            read_back(&rig.engine, *hash, o, deliver).await.unwrap();
        }
    }
    rig.engine.begin_close();
}

/// Cold walks of random streams until `stop`, each checked against the
/// records acked before it started.
async fn walker(
    engine: Arc<ShardEngine>,
    oracle: Arc<Mutex<Oracle>>,
    stop: Arc<AtomicBool>,
    streams: Vec<[u8; 16]>,
    seed: u64,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    while !stop.load(Ordering::Relaxed) {
        let hash = streams[rng.random_range(0..streams.len())];
        let snapshot = oracle
            .lock()
            .unwrap()
            .get(&hash)
            .cloned()
            .unwrap_or_default();
        walk(&engine, hash, &snapshot).await;
        tokio::task::yield_now().await;
    }
}

/// Read `hash` from 0 to the end of `snapshot`, every record exact (records
/// appended since may follow; they are not checked).
async fn walk(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    snapshot: &std::collections::BTreeMap<u64, Bytes>,
) {
    let end = snapshot.keys().next_back().map_or(0, |l| l + 1);
    let mut from = 0;
    while from < end {
        let page = merged(engine, hash, ReadAt::all(from, 256 << 10), Deliver::Durable)
            .await
            .unwrap();
        for r in page.recs.iter().take_while(|r| r.off < end) {
            assert_eq!(r.off, from, "dense from 0");
            assert_eq!(
                snapshot.get(&r.off).map(|b| &b[..]),
                Some(&r.payload[..]),
                "offset {} exact",
                r.off
            );
            from += 1;
        }
        tokio::task::yield_now().await;
    }
}

async fn seal_if(engine: &Arc<ShardEngine>, seal: bool) {
    if seal {
        engine.seal_once(Trigger::Requested).await.unwrap();
    }
}

/// Folds every 30 ms until `stop`.
async fn folder(engine: Arc<ShardEngine>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        engine.fold_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

fn entry(first: u64, last: u64, len: u32) -> (DirEntry, Bytes) {
    let e = DirEntry {
        object: ObjectId {
            epoch: 1,
            seq: first,
        },
        chunk_off: 0,
        chunk_len: len,
        first,
        last,
        page_count: 1,
        keys: KeySummary::Keys(vec![1]),
        anchors: Vec::new(),
    };
    (e, Bytes::new())
}

#[test]
fn only_contiguous_runs_of_enough_small_chunks_are_offered() {
    let n = MIN_CHUNKS as u64;
    let small: Vec<_> = (0..n).map(|i| entry(i, i, 100)).collect();
    assert_eq!(runs_to_consolidate(small.clone()).len(), 1);
    assert!(runs_to_consolidate(small[..MIN_CHUNKS - 1].to_vec()).is_empty());
    let mut gap = small.clone();
    gap.push(entry(n + 1, n + 1, 100));
    assert_eq!(
        runs_to_consolidate(gap).first().map(Vec::len),
        Some(MIN_CHUNKS)
    );
    let mut big = small[..4].to_vec();
    big.push(entry(4, 4, SMALL_CHUNK));
    big.extend((5..5 + n).map(|i| entry(i, i, 100)));
    let runs = runs_to_consolidate(big);
    assert_eq!(runs.len(), 1, "a large chunk breaks a run");
    assert_eq!(runs[0][0].0.first, 5);
    let many: Vec<_> = (0..40).map(|i| entry(i, i, 60 << 10)).collect();
    let runs = runs_to_consolidate(many);
    assert!(
        runs.iter()
            .all(|r| r.iter().map(|(e, _)| u64::from(e.chunk_len)).sum::<u64>() <= 1 << 20)
    );
}
