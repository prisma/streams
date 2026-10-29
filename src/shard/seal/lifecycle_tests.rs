#![cfg(test)]
//! The sealer across a handle's and an engine's life: unsealed pages are
//! never evicted (a ring that holds nothing, handles evicted as fast as
//! possible, everything read back after a restart); a corrupt pending row
//! fails only its stream at the open; and a close leaves a seal in flight
//! at once.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::Trigger;
use super::test_hooks::Hold;
use super::tests::{
    Oracle, append, append_recorded, hashes, manual, open, random_bytes, random_record, read_back,
    reopen, rig, rig_with, settle,
};
use crate::shard::Deliver;
use crate::spill::pending::pending_key;

/// **Unsealed pages are never evicted.** A ring of 1 KiB (it holds no
/// batch) and handles evicted down to one resident as fast as possible:
/// every stream with unsealed pages stays resident, every seal's snapshot
/// finds its pages in memory, and after a restart everything reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsealed_pages_are_never_evicted() {
    let mut rig = rig_with("noevict", manual(), 1024, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xE71C);
    let streams = hashes(&mut rng, 40);
    let oracle = std::sync::Mutex::new(Oracle::new());
    for round in 0..6 {
        for hash in &streams {
            let records = (0..rng.random_range(1..=2))
                .map(|_| random_record(&mut rng, 100, 4000))
                .collect();
            append_recorded(&rig.engine, &oracle, *hash, records).await;
            rig.engine.evict_idle_handles(Duration::from_millis(1), 1);
        }
        let resident = rig.engine.resident_streams();
        assert!(
            resident >= streams.len(),
            "round {round}: a stream with unsealed pages was evicted ({resident} resident)"
        );
        if round % 2 == 1 {
            rig.engine.seal_once(Trigger::Requested).await.unwrap();
            settle(&rig.engine).await;
            rig.engine.evict_idle_handles(Duration::from_millis(1), 1);
        }
    }
    assert_eq!(
        rig.engine
            .layout
            .seal
            .counters
            .snapshot_gaps
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        rig.engine.ring_evicted.load(Ordering::Relaxed) > 0,
        "the ring never evicted: the test proves nothing"
    );
    rig = reopen(rig).await;
    let oracle = oracle.into_inner().unwrap();
    for hash in &streams {
        read_back(&rig.engine, *hash, &oracle[hash], Deliver::Durable)
            .await
            .unwrap();
    }
    rig.engine.begin_close();
}

/// A stream whose pending row does not tile its tail fails alone at the
/// open: the sealer opens, every other stream reads back, the stream's own
/// load meets the corruption, and the open counts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_pending_row_fails_only_its_stream_at_the_open() {
    let mut rig = rig("corrupt-open", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xC0DE);
    let streams = hashes(&mut rng, 2);
    let oracle = std::sync::Mutex::new(Oracle::new());
    for hash in &streams {
        for _ in 0..3 {
            append_recorded(
                &rig.engine,
                &oracle,
                *hash,
                vec![random_bytes(&mut rng, 300)],
            )
            .await;
        }
    }
    settle(&rig.engine).await;
    let (bad, good) = (streams[0], streams[1]);
    let path = rig.path.clone();
    rig.engine.begin_close();
    if tokio::time::timeout(Duration::from_secs(10), rig.engine.closed())
        .await
        .is_err()
    {
        eprintln!("corrupt open: the old engine did not close within 10 s");
    }
    rig.engine.db.close().await.unwrap();
    {
        let db = crate::keyspace::db_builder(path.as_str(), rig.store.clone())
            .build()
            .await
            .unwrap();
        db.put(pending_key(&bad), b"not pages").await.unwrap();
        db.close().await.unwrap();
    }
    rig.engine = open(
        rig.store.clone(),
        &rig.prefix,
        rig.settings,
        rig.ring_bytes,
        Duration::from_millis(2),
    )
    .await;
    assert_eq!(
        rig.engine
            .layout
            .seal
            .counters
            .corrupt_streams
            .load(Ordering::Relaxed),
        1
    );
    let oracle = oracle.into_inner().unwrap();
    read_back(&rig.engine, good, &oracle[&good], Deliver::Durable)
        .await
        .unwrap();
    assert!(
        rig.engine.stream_handle(bad).await.is_err(),
        "the corrupt stream fails its own load"
    );
    rig.engine.begin_close();
}

/// A close does not wait for a seal in flight. The sealer's seal parks
/// before its commit (as one parks in a forced memtable flush while the
/// log is at its L0 cap) and the engine closes: the sealer and deadline
/// loops leave it at once, so the engine terminates well inside the
/// workers' 5 s grace instead of at the grace's abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_leaves_a_seal_in_flight_at_once() {
    let rig = rig("close-inflight", manual(), Duration::from_millis(2)).await;
    let sp = &rig.engine.layout.seal;
    append(&rig.engine, [7; 16], vec![Bytes::from_static(b"parked")]).await;
    let hold = Hold::new();
    *sp.hooks.before_commit.lock().unwrap() = Some(hold.clone());
    sp.configure(|s| {
        s.auto = true;
        s.age_seal = Duration::ZERO;
    });
    let parked = Instant::now();
    while hold.entered.load(Ordering::Acquire) == 0 {
        assert!(
            parked.elapsed() < Duration::from_secs(10),
            "the sealer never started a seal"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let closing = Instant::now();
    rig.engine.begin_close();
    rig.engine
        .shutdown_handle()
        .wait(Duration::from_secs(10))
        .await
        .unwrap();
    let took = closing.elapsed();
    assert!(
        took < Duration::from_secs(2),
        "the close waited {took:?} for the parked seal"
    );
    assert_eq!(hold.entered.load(Ordering::Acquire), 1);
}

/// **A handle load racing a seal reads one state.** A load parked after it
/// read a stream's tail while a seal takes the stream's only page: the load
/// still sees a tail and a pending row of one moment, before the seal or
/// after it, never the tail before and the row after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handle_load_racing_a_seal_reads_one_state() {
    let rig = rig("load-race", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x10AD);
    let hash = hashes(&mut rng, 1)[0];
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    settle(&rig.engine).await;
    let hold = Hold::new();
    *rig.engine.layout.seal.hooks.load_hold.lock().unwrap() = Some(hold.clone());
    let drive = async {
        let t0 = Instant::now();
        while hold.entered.load(Ordering::Acquire) == 0 {
            assert!(t0.elapsed() < Duration::from_secs(10), "the load never parked");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        *rig.engine.layout.seal.hooks.load_hold.lock().unwrap() = None;
        let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
        assert_eq!(report.pages, 1, "the seal took the page");
        hold.release();
    };
    let (loaded, ()) = tokio::join!(rig.engine.load_tail(&hash), drive);
    loaded.expect("the load reads one state");
    rig.engine.begin_close();
}
