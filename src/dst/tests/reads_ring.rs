//! Reads ring.

use super::fixture_storage::{append_sized, mem, open_engine, open_engine_cfg, skey};
use crate::dst::{FaultPlan, FaultStore, OpLog, Outcome, Workload, drain_observed};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// **Review #2's ring ordering + paging asks, pinned at offset level.**
///
/// 1. Publish-before-NOTIFY: a waiter woken by the tail notify must find
///    the ring already covering the new offset — publish-before-ACK is
///    not enough, because the woken reader races the ack path.
/// 2. A read starting MID-batch returns exactly the tail of that batch.
/// 3. A producer-idempotence duplicate publishes nothing (no offset was
///    consumed, so ring ceiling must not move).
/// 4. Budget progress: max_bytes=1 still returns the first record and
///    advances — an oversized record can never wedge a cursor, on the
///    ring path or the DB path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        .ring_read(&handle, before_next, next, 1 << 20)
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
        .ring_read(&handle, mid, end, 1 << 20)
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
    let one = crate::shard::read_frames_range(&engine, &handle, 0, tail_end, 1)
        .await
        .expect("budget-1 read");
    assert_eq!(one.frames.len(), 1, "the first record always fits");
    assert!(one.last_offset.is_some(), "and the cursor advances");
    // Same via a cold engine (DB path).
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
    let one_db = crate::shard::read_frames_range(&b, &hb, 0, tail_end, 1)
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// **Ring/DB equivalence, and a restart starts cold.**
///
/// The same offset range read through the ring (fresh engine, everything
/// resident) and through the canonical DB scan (reopened engine, ring
/// necessarily empty) must be byte-identical — the ring is a cache, not
/// a second source of truth. Also pins publish-before-ack: immediately
/// after an ack returns, the ring already covers the acked offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        .ring_read(&handle_a, next - 1, next, 1 << 20)
        .expect("the ring must cover an offset the ack already exposed");
    assert_eq!(tail1.frames.len(), 1);

    let hits_before = a.ring_hits.load(Ordering::Relaxed);
    let via_ring = crate::shard::read_frames_range(&a, &handle_a, 0, next, 8 << 20)
        .await
        .expect("ring-backed read");
    assert!(
        a.ring_hits.load(Ordering::Relaxed) > hits_before,
        "full-range read on the fresh engine must be a ring hit"
    );

    // Reopen: cold ring, same range must come from the DB, byte-equal.
    let b = open_engine_cfg(store.clone(), "dst-ring-eq", cfg).await;
    let handle_b = b.stream_handle(hash).await.expect("handle");
    let via_db = crate::shard::read_frames_range(&b, &handle_b, 0, next, 8 << 20)
        .await
        .expect("db read");
    assert_eq!(
        b.ring_hits.load(Ordering::Relaxed),
        0,
        "cold ring cannot hit"
    );
    assert_eq!(via_ring.frames.len(), via_db.frames.len(), "frame count");
    for (i, (ra, rb)) in via_ring.frames.iter().zip(via_db.frames.iter()).enumerate() {
        assert_eq!(ra, rb, "frame {i} differs between ring and DB");
    }
}

/// A duplicate `Absorbed{upto}` op must not advance the trim.
///
/// The absorber paces passes off the PUBLISHED absorbed boundary, which
/// lags the committer by durability + dispatch, so under load it can
/// re-submit an `upto` the committer has already applied. The committer
/// used to treat that duplicate like any other pass and trim toward
/// `prev_absorbed` — by then the LIVE boundary — collapsing the deferred-
/// trim lag that protects readers holding a stale absorbed snapshot
/// mid-merge. That collapse, plus the snapshot/tail-scan TOCTOU in
/// `read_merged`, is the 2026-07-27 boundary-race DST failure: records
/// vanished from a `completed = true` page at exactly the sampled
/// absorbed boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_absorbed_op_does_not_advance_the_trim() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 29, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [31u8; 16];
    let engine = open_engine(store.clone(), "dst-duptrim").await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    // Offsets [0, 20).
    w.run(&engine, hash, &key, &["d"], 20, false, &mut log)
        .await;
    assert_eq!(log.total_acked(), 20, "need all 20 offsets acked");

    let published = |engine: &Arc<crate::shard::ShardEngine>| {
        let engine = engine.clone();
        async move {
            let h = engine.stream_handle(hash).await.expect("handle");
            let st = h.state.lock().unwrap();
            (st.durable.absorbed, st.durable.trimmed)
        }
    };
    let wait_absorbed = |engine: &Arc<crate::shard::ShardEngine>, want: u64| {
        let engine = engine.clone();
        async move {
            for _ in 0..400 {
                let h = engine.stream_handle(hash).await.expect("handle");
                if h.state.lock().unwrap().durable.absorbed >= want {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("absorbed never reached {want}");
        }
    };

    // Advance 0 -> 10: deferred trim means nothing is deleted yet.
    engine.submit_absorbed(hash, 10, 0).await;
    wait_absorbed(&engine, 10).await;
    // Advance 10 -> 18: trims up to the previous boundary, 10.
    engine.submit_absorbed(hash, 18, 0).await;
    wait_absorbed(&engine, 18).await;
    let (absorbed, trimmed) = published(&engine).await;
    assert_eq!(absorbed, 18);
    assert_eq!(
        trimmed, 10,
        "an advancing op trims to the previous boundary"
    );

    // The duplicate: re-submit the boundary the committer already holds,
    // exactly as an absorber pass that raced dispatch does.
    engine.submit_absorbed(hash, 18, 0).await;
    // Sentinel append: the committer queue is FIFO, so this ack proves the
    // duplicate op was processed and its state published.
    w.run(&engine, hash, &key, &["d"], 1, false, &mut log).await;
    assert_eq!(log.total_acked(), 21, "sentinel append must ack");

    let (absorbed, trimmed) = published(&engine).await;
    assert_eq!(absorbed, 18, "a duplicate must not move the boundary");
    assert_eq!(
        trimmed, 10,
        "a duplicate Absorbed op advanced the trim to the live boundary — \
         the deferred-trim lag protecting stale-snapshot readers is gone"
    );

    // The lag is not bookkeeping: [10, 18) must still be readable from the
    // shard log, because a reader that snapshotted absorbed=10 before the
    // 10 -> 18 dispatch scans its tail from exactly there.
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mid = crate::shard::read_frames_range(&engine, &handle, 10, 18, 1 << 20)
        .await
        .expect("scan [10, 18)");
    assert_eq!(
        mid.frames.len(),
        8,
        "records above the previous boundary must survive a duplicate op"
    );
}

// ---- #271 hub registry identity + lifecycle (Søren review F2) ------

// ---- #272 hub scanned progress (Søren review F3) --------------------

/// #272 part 2 red: the DEFAULT-lane read (key_filter Some("")) must
/// serve from the durable-tail ring like unfiltered reads do — the
/// pre-fix gate (`key_filter.is_none()`) sent every hub-pump read to
/// the DB scan path, so the "ring-preferring" claim was false. The
/// filtered ring read must also return the CONSUMED offset over
/// trailing non-matching frames (scanned progress, F3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_tail_reads_serve_from_ring() {
    let store = mem();
    let db = slatedb::Db::builder("ringkeyed", store.clone() as Arc<dyn ObjectStore>)
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
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
        absorb_tx,
        None,
        __maint,
    );
    let key = skey();
    let h = [0xB1u8; 16];
    append_sized(&engine, h, &key, "", 512).await; // default @0
    append_sized(&engine, h, &key, "cust", 512).await; // foreign @1
    append_sized(&engine, h, &key, "", 512).await; // default @2
    append_sized(&engine, h, &key, "cust", 512).await; // foreign @3 (trailing)
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

/// The held operation is inside durable_absorbed itself, after handle warming.
/// A retained dense keyed ring page never enters it; fallback and applied reads do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o3_retained_ring_coverage_skips_only_the_redundant_marker() {
    use crate::shard::{Deliver, record::TEST_MARKER_HOLD};
    use std::time::Duration;
    use tokio::sync::Notify;
    let key = skey();
    let hash = [0x83; 16];
    let engine = open_engine_cfg(
        mem(),
        "o3-retained-ring",
        crate::shard::ShardConfig {
            tail_ring_bytes: 1024 * 1024,
            ..Default::default()
        },
    )
    .await;
    for lane in ["other", "hot", "other", "hot"] {
        append_sized(&engine, hash, &key, lane, 1024).await;
    }
    let handle = engine.stream_handle(hash).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let read = |deliver| {
        crate::http::read_merged(
            &key,
            &hash,
            &handle,
            &engine,
            0,
            Some("hot"),
            1024 * 1024,
            deliver,
        )
    };
    let page = tokio::time::timeout(
        Duration::from_secs(2),
        TEST_MARKER_HOLD.scope((entered.clone(), release.clone()), read(Deliver::Durable)),
    )
    .await
    .expect("proven ring must not wait on marker")
    .unwrap();
    assert_eq!(
        page.recs.iter().map(|r| r.off).collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert!(page.recs.iter().all(|r| r.payload.as_ref() == [0x5a; 1024]));
    assert_eq!(page.last, Some(3));
    assert!(page.completed);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), entered.notified())
            .await
            .is_err()
    );

    let partial = engine.ring_read_keyed(&handle, 0, 4, "hot", 1).unwrap();
    assert!(partial.frames.is_empty());
    assert_eq!(partial.last_offset, Some(0));
    assert!(partial.proves_durable_ring(&engine, hash, 0));
    assert!(!partial.proves_durable_ring(&engine, hash, 1));
    assert!(!partial.proves_durable_ring(&engine, [0x84; 16], 0));
    let other = open_engine(mem(), "o3-other-owner").await;
    assert!(!partial.proves_durable_ring(&other, hash, 0));
    other.begin_close();

    // Applied visibility cannot borrow a durable proof, even when this particular
    // requested prefix also happens to be durable. The canonical check remains.
    let applied =
        TEST_MARKER_HOLD.scope((entered.clone(), release.clone()), read(Deliver::Applied));
    tokio::pin!(applied);
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut applied => panic!("applied bypassed the marker"),
        _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("applied never entered marker"),
    }
    release.notify_one();
    let page = applied.await.unwrap();
    assert!(page.completed);
    assert_eq!(page.last, Some(3));

    // Remove a middle row while preserving floor/ceiling. Neither keyed nor
    // unfiltered ring reads may use endpoint metadata as a density proof.
    {
        let mut ring = handle.ring.lock().unwrap();
        for batch in &mut ring.batches {
            batch.frames.retain(|(off, _)| *off != 1);
        }
    }
    assert!(engine.ring_read(&handle, 0, 4, usize::MAX).is_none());
    assert!(
        engine
            .ring_read_keyed(&handle, 0, 4, "hot", usize::MAX)
            .is_none()
    );
    let fallback =
        TEST_MARKER_HOLD.scope((entered.clone(), release.clone()), read(Deliver::Durable));
    tokio::pin!(fallback);
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut fallback => panic!("fallback bypassed marker"),
        _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("fallback never entered marker"),
    }
    release.notify_one();
    let page = fallback.await.unwrap();
    assert_eq!(
        page.recs.iter().map(|r| r.off).collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert!(page.completed);
    assert_eq!(page.last, Some(3));
    engine.begin_close();
}
