//! Reads ring.

use super::fixture_storage::{
    append_sized, eventually, mem, open_engine_cfg, open_engine_with_settings, skey,
};
use crate::dst::{
    FaultPlan, FaultStore, ObjClass, OpLog, Outcome, StoreOp, Workload, drain_observed,
};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The durable scan window `[from, to)` a ring read serves within `max_bytes`.
fn window(from: u64, to: u64, max_bytes: usize) -> crate::shard::RingScan {
    crate::shard::RingScan {
        from,
        to,
        max_bytes,
    }
}

/// The durable read of `[from, to)` within `max_bytes` (the ring for the
/// sealed part, then the segment objects, then the unsealed pages).
async fn durable_frames(
    engine: &crate::shard::ShardEngine,
    handle: &crate::shard::StreamHandle,
    from: u64,
    to: u64,
    max_bytes: usize,
) -> Result<crate::shard::FrameReadResult, slatedb::Error> {
    crate::shard::record::read_frames_until(
        engine,
        handle,
        from,
        to,
        None,
        max_bytes,
        crate::shard::Deliver::Durable,
    )
    .await
}

/// **Review #2's ring ordering + paging asks, pinned at offset level.**
///
/// 1. Publish-before-NOTIFY: a waiter woken by the tail notify must find
///    the ring already covering the new offset — publish-before-ACK is
///    not enough, because the woken reader races the ack path.
/// 2. A read starting MID-batch returns exactly the tail of that batch.
/// 3. A producer-idempotence duplicate publishes nothing (no offset was
///    consumed, so ring ceiling must not move).
/// 4. Budget progress: max_bytes=1 still returns the first record and
///    advances — an oversized record can never wedge a cursor, on a warm
///    engine or a cold one.
#[expect(
    clippy::too_many_lines,
    reason = "ring ordering scenario; publish-before-notify, paging and duplicate suppression are pinned at offset level in one sequence; helper phases would hide which offset was delivered twice"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "ring ordering fixture; the appender is joined before the delivered offsets are compared; it must append concurrently with the parked waiter"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "ring_ordering_paging_and_duplicates_at_offset_level; the fixture's frame counts are small offsets it published itself; a checked conversion would only restate the fixture"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn ring_ordering_paging_and_duplicates_at_offset_level() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 93, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [43u8; 16];
    let cfg = crate::shard::ShardConfig {
        tail_ring_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };
    let engine = open_engine_cfg(store.clone(), "dst-ring-ord", cfg).await;
    let w = Workload::new(cov.clone());

    // (1) Arm a notify waiter BEFORE the append; on wake, the ring must
    // already cover the appended offset.
    let o = w
        .attempt_with_deadline(&engine, hash, &key, "o", "first", None, None)
        .await;
    assert!(matches!(o, Outcome::Acked { .. }));
    let handle = engine.stream_handle(hash).await.expect("handle");
    let notified = handle.notify.notified();
    let before_next = handle.state.lock().unwrap().durable.next;
    let e2 = engine.clone();
    let k2 = key.clone();
    let c2 = cov.clone();
    let appender = tokio::spawn(async move {
        let w2 = Workload::new(c2);
        w2.attempt_with_deadline(&e2, hash, &k2, "o", "second", None, None)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), notified)
        .await
        .expect("waiter must be woken");
    // The instant of wake: ring must already hold [before_next, next).
    let next = handle.state.lock().unwrap().durable.next;
    assert!(next > before_next);
    let hit = engine
        .ring_read(&handle, window(before_next, next, 1 << 20), None)
        .expect("a woken reader must hit the ring, not fall to the DB");
    assert_eq!(hit.frames.len(), (next - before_next) as usize);
    assert!(matches!(
        appender.await.expect("join"),
        Outcome::Acked { .. }
    ));

    // (2) Mid-batch: append a 4-record batch (one commit group), read
    // starting inside it.
    let mut log = OpLog::default();
    let mut w2 = Workload::new(cov.clone());
    w2.run(&engine, hash, &key, &["o"], 4, false, &mut log)
        .await;
    let end = handle.state.lock().unwrap().durable.next;
    let mid = end - 2;
    let part = engine
        .ring_read(&handle, window(mid, end, 1 << 20), None)
        .expect("mid-batch start must be servable from the ring");
    assert_eq!(part.frames.len(), 2, "exactly the batch tail");
    assert_eq!(part.last_offset, Some(end - 1));

    // (3) Duplicate publishes nothing.
    let pr = crate::shard::ProducerReq {
        id: "ring-dup".into(),
        epoch: 1,
        seq: 0,
        request_hash: None,
    };
    let first = w
        .attempt_with_deadline(&engine, hash, &key, "o", "dup-body", Some(pr.clone()), None)
        .await;
    assert!(matches!(
        first,
        Outcome::Acked {
            duplicate: false,
            ..
        }
    ));
    let ceil_before = {
        let r = handle.ring.lock().unwrap();
        r.batches.back().map(|b| b.next)
    };
    let published_before = engine.ring_published.load(Ordering::Relaxed);
    let retry = w
        .attempt_with_deadline(&engine, hash, &key, "o", "dup-body", Some(pr), None)
        .await;
    assert!(matches!(
        retry,
        Outcome::Acked {
            duplicate: true,
            ..
        }
    ));
    let ceil_after = {
        let r = handle.ring.lock().unwrap();
        r.batches.back().map(|b| b.next)
    };
    assert_eq!(
        ceil_before, ceil_after,
        "a duplicate must not move the ring ceiling"
    );
    assert_eq!(
        engine.ring_published.load(Ordering::Relaxed),
        published_before,
        "a duplicate must not publish a batch"
    );

    // (4) Oversized-record progress, ring and DB path alike.
    let tail_end = handle.state.lock().unwrap().durable.next;
    let one = durable_frames(&engine, &handle, 0, tail_end, 1)
        .await
        .expect("budget-1 read");
    assert_eq!(one.frames.len(), 1, "the first record always fits");
    assert!(one.last_offset.is_some(), "and the cursor advances");
    // Same via a cold engine (its unsealed pages, loaded from the log).
    let b = open_engine_cfg(
        store.clone(),
        "dst-ring-ord",
        crate::shard::ShardConfig {
            tail_ring_bytes: 32 * 1024 * 1024,
            ..Default::default()
        },
    )
    .await;
    let hb = b.stream_handle(hash).await.expect("handle");
    let one_db = durable_frames(&b, &hb, 0, tail_end, 1)
        .await
        .expect("db budget-1 read");
    assert_eq!(one_db.frames.len(), 1);
    assert_eq!(
        one.frames[0], one_db.frames[0],
        "same first frame either path"
    );
}

/// **Durable-tail ring: correct under load, evictions, and fallback.**
///
/// Small budget forces evictions mid-run, so reads exercise all three
/// paths — ring hit, ring miss -> DB scan, and mixed ranges — and the
/// full I1–I7 audit runs over the production merged reader. Anti-vacuity:
/// the run must have produced hits AND evictions, or the scenario proves
/// nothing about the ring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn tail_ring_serves_live_reads_and_survives_eviction() {
    for seed in [9u64, 33] {
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), seed, FaultPlan::new(0, 0, 25));
        let cov = store.coverage();
        let key = skey();
        let hash = [30u8; 16];
        let cfg = crate::shard::ShardConfig {
            // Tiny: a couple of groups' worth, so eviction is constant.
            tail_ring_bytes: 2 * 1024,
            ..Default::default()
        };
        let engine = open_engine_cfg(store.clone(), &format!("dst-ring-{seed}"), cfg).await;

        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        for _ in 0..4 {
            w.run(&engine, hash, &key, &["r1", "r2"], 12, false, &mut log)
                .await;
        }

        let hits = engine.ring_hits.load(Ordering::Relaxed);
        let evicted = engine.ring_evicted.load(Ordering::Relaxed);
        assert!(
            engine.ring_published.load(Ordering::Relaxed) > 0,
            "seed {seed}: nothing was ever published to the ring"
        );
        assert!(evicted > 0, "seed {seed}: budget never forced an eviction");

        let _ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&engine, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: ring-backed reads broke the canon: {e}");
        }
        let _ = hits; // hit-path asserted in the equivalence scenario below
    }
}

/// **Ring/segment equivalence, and a restart starts cold.**
///
/// The same sealed offset range read through the ring (fresh engine,
/// everything resident) and through the segment objects (reopened engine,
/// ring necessarily empty) must be byte-identical — the ring is a cache,
/// not a second source of truth. The ring serves only offsets below the
/// sealed boundary, so the range is sealed first. Also pins
/// publish-before-ack: immediately after an ack returns, the ring already
/// covers the acked offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn tail_ring_matches_the_db_scan_and_restarts_cold() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 13, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [31u8; 16];
    let cfg = crate::shard::ShardConfig {
        tail_ring_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };
    let a = open_engine_cfg(store.clone(), "dst-ring-eq", cfg.clone()).await;
    let w = Workload::new(cov.clone());

    for i in 0..20u64 {
        let o = w
            .attempt_with_deadline(&a, hash, &key, "eq", &format!("rec{i}"), None, None)
            .await;
        assert!(matches!(o, Outcome::Acked { .. }));
    }
    let handle_a = a.stream_handle(hash).await.expect("handle");
    let next = handle_a.state.lock().unwrap().durable.next;

    // Publish-before-ack: the last acked offset is ring-resident NOW.
    let tail1 = a
        .ring_read(&handle_a, window(next - 1, next, 1 << 20), None)
        .expect("the ring must cover an offset the ack already exposed");
    assert_eq!(tail1.frames.len(), 1);

    a.seal_requested().await.expect("seal");
    assert_eq!(
        handle_a.state.lock().unwrap().durable.absorbed,
        next,
        "all sealed"
    );
    let hits_before = a.ring_hits.load(Ordering::Relaxed);
    let via_ring = durable_frames(&a, &handle_a, 0, next, 8 << 20)
        .await
        .expect("ring-backed read");
    assert!(
        a.ring_hits.load(Ordering::Relaxed) > hits_before,
        "full-range read on the fresh engine must be a ring hit"
    );

    // Reopen: cold ring, same range must come from the segment objects,
    // byte-equal.
    let b = open_engine_cfg(store.clone(), "dst-ring-eq", cfg).await;
    let handle_b = b.stream_handle(hash).await.expect("handle");
    let via_db = durable_frames(&b, &handle_b, 0, next, 8 << 20)
        .await
        .expect("segment read");
    assert_eq!(
        b.ring_hits.load(Ordering::Relaxed),
        0,
        "cold ring cannot hit"
    );
    let sealed_gets = b.seal_json()["sealed_gets"].as_u64().unwrap();
    assert!(sealed_gets > 0, "the cold read fetched the segment object");
    assert_eq!(via_ring.frames.len(), via_db.frames.len(), "frame count");
    for (i, (ra, rb)) in via_ring.frames.iter().zip(via_db.frames.iter()).enumerate() {
        assert_eq!(ra, rb, "frame {i} differs between ring and segment object");
    }
}

// ---- #271 hub registry identity + lifecycle (Søren review F2) ------

// ---- #272 hub scanned progress (Søren review F3) --------------------

/// #272 part 2 red: the DEFAULT-lane read (key_filter Some("")) must
/// serve from the durable-tail ring like unfiltered reads do — the
/// pre-fix gate (`key_filter.is_none()`) sent every hub-pump read to
/// the DB scan path, so the "ring-preferring" claim was false. The
/// filtered ring read must also return the CONSUMED offset over
/// trailing non-matching frames (scanned progress, F3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn keyed_tail_reads_serve_from_ring() {
    let store = mem();
    let db = crate::keyspace::db_builder("ringkeyed", store.clone() as Arc<dyn ObjectStore>)
        .build()
        .await
        .expect("open db");
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("maint");
    let engine = crate::shard::ShardEngine::start(
        "ringkeyed".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig {
            tail_ring_bytes: 1024 * 1024,
            ..Default::default()
        },
        None,
        __maint,
    );
    let key = skey();
    let h = [0xB1u8; 16];
    append_sized(&engine, h, &key, "", 512).await; // default @0
    append_sized(&engine, h, &key, "cust", 512).await; // foreign @1
    append_sized(&engine, h, &key, "", 512).await; // default @2
    append_sized(&engine, h, &key, "cust", 512).await; // foreign @3 (trailing)
    // The ring serves only offsets below the sealed boundary.
    engine.seal_requested().await.expect("seal");
    let handle = engine.stream_handle(h).await.unwrap();
    let hits0 = engine.ring_hits.load(std::sync::atomic::Ordering::Relaxed);
    let out = crate::http::read_merged(
        &key,
        &h,
        &handle,
        &engine,
        0,
        Some(""),
        8 * 1024 * 1024,
        crate::shard::Deliver::Durable,
    )
    .await
    .expect("read");
    let offs: Vec<u64> = out.recs.iter().map(|r| r.off).collect();
    assert_eq!(offs, vec![0, 2], "default lane records only");
    assert_eq!(
        out.last,
        Some(3),
        "consumed offset must cover the trailing foreign frame"
    );
    let hits1 = engine.ring_hits.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        hits1 > hits0,
        "the default-lane read must be served from the tail ring (hits {hits0} -> {hits1})"
    );
}

/// The stream a woken read follows, and a neighbour above it in the key
/// space: a shared cell's one shard holds every project's streams.
const STREAM: [u8; 16] = [0x5c; 16];
const NEIGHBOUR: [u8; 16] = [0xf0; 16];

/// A shard whose older data sits in a sorted run, as its compactor leaves
/// it: eight 1 KiB records of each stream over four memtable flushes, the
/// four L0 tables compacted into one run (compaction output caches no data
/// block, so a scan reads the run's blocks from the store).
async fn compacted_shard(
    tail_ring_bytes: usize,
) -> (Arc<FaultStore>, Arc<crate::shard::ShardEngine>) {
    use std::time::Duration;
    let store = FaultStore::uniform(mem(), 61, FaultPlan::CLEAN);
    let compactor = slatedb::config::CompactorOptions {
        poll_interval: Duration::from_millis(100),
        commit_compacted_interval: Duration::from_millis(100),
        worker: Some(slatedb::config::CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(100),
            ..Default::default()
        }),
        ..Default::default()
    };
    let settings = slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(5)),
        manifest_poll_interval: Duration::from_millis(50),
        compactor_options: Some(compactor),
        ..Default::default()
    };
    let cfg = crate::shard::ShardConfig {
        tail_ring_bytes,
        ..Default::default()
    };
    let engine = open_engine_with_settings(store.clone(), "ring-sorted-run", cfg, settings).await;
    let key = skey();
    for _ in 0..4 {
        for hash in [STREAM, STREAM, NEIGHBOUR, NEIGHBOUR] {
            append_sized(&engine, hash, &key, "", 1024).await;
        }
        let flush = slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        };
        engine.db.flush_with_options(flush).await.expect("flush");
    }
    eventually("the L0 tables compact into a sorted run", || {
        let manifest = engine.db.manifest();
        let stream_tree = manifest.segment(&[crate::keyspace::STREAM]);
        stream_tree.is_some_and(|tree| !tree.compacted().is_empty())
    })
    .await;
    (store, engine)
}

/// A long-poll wake on the compacted shard's stream: the waiter is armed,
/// one record is appended, and the woken read takes its 1 MiB page from
/// the tail it waited at. Returns the offsets it served and the GETs of
/// table objects it issued.
async fn woken_read_table_gets(tail_ring_bytes: usize) -> (Vec<u64>, u64) {
    let (store, engine) = compacted_shard(tail_ring_bytes).await;
    let key = skey();
    let handle = engine.stream_handle(STREAM).await.expect("handle");
    let tail = handle.state.lock().unwrap().durable.next;
    let woken = handle.notify.notified();
    append_sized(&engine, STREAM, &key, "", 1024).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), woken)
        .await
        .expect("the append wakes the waiter");
    let before = store.count(StoreOp::Get, ObjClass::Sst);
    let page = crate::http::read_merged(
        &key,
        &STREAM,
        &handle,
        &engine,
        tail,
        None,
        1 << 20,
        crate::shard::Deliver::Durable,
    )
    .await
    .expect("woken read");
    let gets = store.count(StoreOp::Get, ObjClass::Sst) - before;
    engine.begin_close();
    (page.recs.iter().map(|r| r.off).collect(), gets)
}

/// A long-poll woken on a stream whose older data sits in a sorted run
/// takes the appended record from the stream's unsealed pages in memory
/// and fetches no table object, with the ring off (the binary's default)
/// and with the shared-cell profile's ring (parsed as a deploy layers
/// compute-1g.env and shared-cell.env), which serves only sealed offsets.
/// Layout 5 needed the ring for this (the cost report of 2026-10-24, lever
/// 6): its shard-log scan merged the sorted run's block from the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_woken_read_takes_an_unsealed_record_from_memory_without_a_store_get() {
    let profile =
        crate::config::shared_cell_profile(&[("SLATE_S3_ENDPOINT", "http://127.0.0.1:1")]);
    let ring = profile.cli.tail_ring_bytes;
    assert!(ring > 0, "the shared-cell profile gives a ring");
    assert_eq!(
        woken_read_table_gets(0).await,
        (vec![8], 0),
        "with the ring off the woken read is served from memory"
    );
    assert_eq!(
        woken_read_table_gets(ring).await,
        (vec![8], 0),
        "with the shared-cell profile's ring of {ring} bytes the woken read is served from memory"
    );
}
