#![cfg(test)]
//! The sealer across a handle's and an engine's life: unsealed pages are
//! never evicted (a ring that holds nothing, handles evicted as fast as
//! possible, everything read back after a restart); a corrupt pending row
//! fails only its stream at the open; a close leaves a seal in flight at
//! once; and an open seals what the engine before it left unsealed.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::test_hooks::Hold;
use super::tests::{
    Oracle, append, append_recorded, hashes, manual, open, random_bytes, random_record, read_back,
    reopen, rig, rig_with, settle,
};
use super::{SealError, SealSettings, Trigger};
use crate::shard::Deliver;
use crate::spill::pending::pending_key;

/// **Unsealed pages are never evicted.** A ring of 1 KiB (it holds no
/// batch) and handles evicted down to one resident as fast as possible:
/// every stream with unsealed pages stays resident, every seal's snapshot
/// finds its pages in memory, and after a restart everything reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
    // The engine's own close may have closed the log first.
    if let Err(e) = rig.engine.db.close().await {
        assert_eq!(
            e.kind(),
            slatedb::ErrorKind::Closed(slatedb::CloseReason::Clean),
            "{e}"
        );
    }
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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

/// **The open loads only the streams holding unsealed pages.** Three
/// streams sealed whole, then one of them appended to: after a restart the
/// open made that one stream resident and left the two sealed ones to their
/// first touch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_open_loads_only_streams_holding_unsealed_pages() {
    let mut rig = rig("open-loads", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x0BE7);
    let streams = hashes(&mut rng, 3);
    for hash in &streams {
        append(&rig.engine, *hash, vec![random_bytes(&mut rng, 500)]).await;
    }
    settle(&rig.engine).await;
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    append(&rig.engine, streams[0], vec![random_bytes(&mut rng, 500)]).await;
    settle(&rig.engine).await;
    rig = reopen(rig).await;
    let resident: Vec<[u8; 16]> = rig.engine.streams.lock().unwrap().keys().copied().collect();
    assert_eq!(resident, vec![streams[0]], "only the unsealed stream");
    rig.engine.begin_close();
}

/// **A handle load racing a seal reads one state.** A load parked after it
/// read a stream's tail while a seal takes the stream's only page: the load
/// still sees a tail and a pending row of one moment, before the seal or
/// after it, never the tail before and the row after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "the load never parked"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        *rig.engine.layout.seal.hooks.load_hold.lock().unwrap() = None;
        let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
        assert_eq!(report.pages, 1, "the seal took the page");
        hold.release();
    };
    let (loaded, ()) = futures_util::future::join(rig.engine.load_tail(&hash), drive).await;
    loaded.expect("the load reads one state");
    rig.engine.begin_close();
}

/// **A seal whose group fails publishes nothing.** The commit group that
/// stages the seal fails: the seal reports that its group was not applied,
/// the directory holds none of its chunks, the stream's boundary has not
/// moved, and the next seal takes the same pages and publishes them once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_seal_whose_group_fails_publishes_nothing() {
    let rig = rig("group-fails", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xFA11);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..3 {
        append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    }
    settle(&rig.engine).await;
    let entries = || rig.engine.layout.seal.directory.read().unwrap().entries();
    rig.engine.fail_next_seal_group();
    let failed = tokio::time::timeout(
        Duration::from_secs(10),
        rig.engine.seal_once(Trigger::Requested),
    )
    .await
    .expect("a failed group ends the seal");
    assert!(
        matches!(&failed, Err(SealError::Store(why)) if why == "the seal's group was not applied"),
        "{failed:?}"
    );
    assert_eq!(entries(), 0, "the failed seal's chunks were withdrawn");
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    assert_eq!(handle.state.lock().unwrap().applied.absorbed, 0);
    let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!(report.pages, 3);
    assert_eq!(
        entries(),
        report.chunks as u64,
        "the next seal published once"
    );
    rig.engine.begin_close();
}

/// **No sealer, no wait.** An engine whose sealer task is gone before its
/// open finished answers a sealed read's readiness wait at once, naming the
/// missing task, instead of leaving the read waiting for an open no task
/// will finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn readiness_without_a_sealer_task_fails_at_once() {
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let prefix = super::tests::unique("no-sealer");
    crate::shard::inject_seal_open_faults(&prefix, u32::MAX);
    let db = std::sync::Arc::new(
        crate::keyspace::db_builder(
            crate::sharddir::shard_db_path(&prefix).as_str(),
            store.clone(),
        )
        .build()
        .await
        .unwrap(),
    );
    let engine = crate::shard::ShardEngine::start(
        prefix.clone(),
        db,
        store,
        crate::shard::ShardConfig::default(),
        None,
        crate::shard::ShardMaintenance::default(),
    );
    let sealer = engine.test_abort_task("sealer");
    let t0 = Instant::now();
    while !sealer.is_finished() {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "the sealer never stopped"
        );
        tokio::task::yield_now().await;
    }
    let refused = tokio::time::timeout(Duration::from_secs(5), engine.seal_ready())
        .await
        .expect("readiness answers at once");
    let error = refused.expect_err("no sealer is ready").to_string();
    assert!(error.contains("no sealer task"), "{error}");
    crate::shard::inject_seal_open_faults(&prefix, 0);
    engine.begin_close();
}

/// **One handle per stream.** After a restart, each stream the sealer
/// holds is held through the very handle the engine serves it from, also
/// when requests load the streams while the sealer's open does: a load
/// holds nothing itself, so the sealer never seals a handle the committer
/// does not write to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_sealer_holds_only_the_handle_the_engine_serves() {
    let mut rig = rig("one-handle", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x0AE1);
    let streams = hashes(&mut rng, 16);
    for hash in &streams {
        append(&rig.engine, *hash, vec![random_bytes(&mut rng, 100)]).await;
    }
    settle(&rig.engine).await;
    rig = reopen(rig).await;
    let served = rig.engine.streams.lock().unwrap().clone();
    let held = rig.engine.layout.unsealed();
    assert_eq!(held.len(), streams.len(), "every stream with pages is held");
    for handle in held {
        assert!(
            std::sync::Arc::ptr_eq(&handle, &served[&handle.hash]),
            "the sealer holds the served handle"
        );
    }
    rig.engine.begin_close();
}

/// An engine started with `cfg`'s handle eviction, its sealer open.
async fn eviction_engine(
    label: &str,
    cfg: crate::shard::ShardConfig,
) -> std::sync::Arc<crate::shard::ShardEngine> {
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let prefix = super::tests::unique(label);
    let db = std::sync::Arc::new(
        crate::keyspace::db_builder(
            crate::sharddir::shard_db_path(&prefix).as_str(),
            store.clone(),
        )
        .build()
        .await
        .unwrap(),
    );
    let engine = crate::shard::ShardEngine::start(
        prefix,
        db,
        store,
        cfg,
        None,
        crate::shard::ShardMaintenance::default(),
    );
    engine.seal_ready().await.unwrap();
    engine
}

/// Wait (at most 10 s) until at most `left` handles are resident.
async fn evicted_down_to(engine: &crate::shard::ShardEngine, left: usize) {
    let t0 = Instant::now();
    while engine.resident_streams() > left {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the sealer's loop never evicted down to {left}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **The sealer's loop evicts idle handles.** An engine whose handles idle
/// out after a millisecond: a stream loaded and left alone (no unsealed
/// page, so not held) is evicted by the loop's next eviction round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_sealer_loop_evicts_an_idle_handle() {
    let engine = eviction_engine(
        "idle-evict",
        crate::shard::ShardConfig {
            handle_idle_evict: Duration::from_millis(1),
            ..Default::default()
        },
    )
    .await;
    let hash = hashes(&mut StdRng::seed_from_u64(0x1D1E), 1)[0];
    drop(engine.stream_handle(hash).await.unwrap());
    assert_eq!(engine.resident_streams(), 1, "the load made it resident");
    evicted_down_to(&engine, 0).await;
    engine.begin_close();
}

/// **With idle eviction off, the cap still evicts.** An engine that never
/// evicts by age but keeps at most one resident handle: of three loaded
/// streams, the loop's next round leaves one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_sealer_loop_evicts_down_to_the_cap_with_idle_eviction_off() {
    let engine = eviction_engine(
        "cap-evict",
        crate::shard::ShardConfig {
            handle_idle_evict: Duration::ZERO,
            handle_max_resident: 1,
            ..Default::default()
        },
    )
    .await;
    for hash in hashes(&mut StdRng::seed_from_u64(0xCA9), 3) {
        drop(engine.stream_handle(hash).await.unwrap());
    }
    assert_eq!(engine.resident_streams(), 3, "the loads made them resident");
    evicted_down_to(&engine, 1).await;
    engine.begin_close();
}

/// **A segment object is never overwritten.** The name the next seal's
/// object takes already holds bytes: the seal's create-only PUT is refused,
/// the seal fails, the bytes stay, and the stream's pages stay unsealed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_seal_never_overwrites_an_existing_object() {
    use object_store::ObjectStoreExt;
    let rig = rig("create-only", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xC4EA);
    let hash = hashes(&mut rng, 1)[0];
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    settle(&rig.engine).await;
    let sp = &rig.engine.layout.seal;
    let id = crate::spill::object::ObjectId {
        epoch: rig.engine.writer_epoch,
        seq: sp.next_seq.load(Ordering::Relaxed) + 1,
    };
    let path = object_store::path::Path::from(crate::spill::object::object_path(&rig.path, id));
    rig.store
        .put(&path, Bytes::from_static(b"not this seal's").into())
        .await
        .unwrap();
    let refused = rig.engine.seal_once(Trigger::Requested).await;
    assert!(matches!(refused, Err(SealError::Store(_))), "{refused:?}");
    let kept = rig.store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(&kept[..], b"not this seal's", "the object was overwritten");
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    assert_eq!(
        handle.state.lock().unwrap().applied.absorbed,
        0,
        "nothing sealed"
    );
    rig.engine.begin_close();
}

/// A live engine's task registry answers by role and state: its sealer is
/// running, a role nobody spawned is not, and an aborted sealer is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_task_registry_answers_by_role_and_state() {
    let rig = rig("roles", manual(), Duration::from_millis(2)).await;
    let tasks = &rig.engine.tasks;
    assert!(tasks.running("sealer"), "the sealer runs");
    assert!(tasks.running("committer"), "the committer runs");
    assert!(!tasks.running("no-such-role"), "nobody spawned this role");
    let sealer = rig.engine.test_abort_task("sealer");
    let t0 = Instant::now();
    while !sealer.is_finished() {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "the sealer never stopped"
        );
        tokio::task::yield_now().await;
    }
    assert!(!tasks.running("sealer"), "an aborted sealer is not running");
    rig.engine.begin_close();
}

/// **An open seals what the engine before it left unsealed.** The pages an
/// engine closes with sit below every trigger, and an open restarts their
/// age; a fresh engine on the same store seals them at its first
/// evaluation instead of at the age trigger, so a moved shard's backlog is
/// not counted as maintenance without progress until then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_open_seals_the_pages_the_engine_before_it_left_unsealed() {
    let settings = SealSettings {
        age_seal: Duration::from_secs(3600),
        age_deadline: Duration::from_secs(7200),
        tick: Duration::from_millis(10),
        ..SealSettings::default()
    };
    let rig = rig("inherit", settings, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x5EA1);
    for hash in hashes(&mut rng, 4) {
        append(&rig.engine, hash, vec![random_bytes(&mut rng, 1000)]).await;
    }
    let backlog = rig.engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert!(backlog > 0, "the appends left a backlog");
    assert_eq!(
        rig.engine
            .layout
            .seal
            .counters
            .seals
            .load(Ordering::Relaxed),
        0,
        "the appends stay below every trigger"
    );
    let rig = reopen(rig).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while rig.engine.maintenance_snapshot().unabsorbed_frame_bytes > 0 {
        assert!(
            Instant::now() < deadline,
            "the inherited backlog was not sealed within 5 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    rig.engine.begin_close();
}
