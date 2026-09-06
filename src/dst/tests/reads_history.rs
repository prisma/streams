//! Reads history.

use super::fixture_storage::{
    append_n, append_sized, drain_filtered, mem, skey, wait_all_absorbed,
};
use crate::dst::{FaultPlan, FaultStore};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Round-4 root cause: the absorber's lane classification races
/// dispatch (a signal can arrive before its append's tail publishes, so
/// the zero-route guard briefly reads route==0 and picks v1; a tick
/// later a stale absorbed==0 re-admits v2). The two lanes then
/// interleave and a flagged-v2 stream ends up with ranges that exist
/// ONLY in the v1 per-stream DB — acked records the v2 read path can
/// never see. The COMMITTER seals the layout at the first advance:
/// cross-layout advances are dropped, boundaries never cover a range
/// the sealed tier doesn't hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_advance_seals_the_history_layout() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 103, FaultPlan::new(0, 0, 0));
    let key = skey();

    let db = slatedb::Db::builder("dst-seal", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-seal".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );

    async fn wait_absorbed(
        engine: &Arc<crate::shard::ShardEngine>,
        hash: [u8; 16],
        want: u64,
    ) -> (u64, bool) {
        for _ in 0..400 {
            let h = engine.stream_handle(hash).await.unwrap();
            let (a, f) = {
                let s = h.state.lock().unwrap();
                (s.durable.absorbed, s.durable.history_v2)
            };
            if a >= want {
                return (a, f);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let h = engine.stream_handle(hash).await.unwrap();
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    }

    // Stream A: sealed v2 by its first advance; a later v1 advance (the
    // racy in-flight v1 pass) must be DROPPED — boundary and flag hold.
    let a = [0xF1u8; 16];
    for _ in 0..5 {
        append_sized(&engine, a, &key, "", 512).await;
    }
    engine.submit_absorbed_batch_v2(vec![(a, 3, 0)]).await;
    let (abs, flag) = wait_absorbed(&engine, a, 3).await;
    assert_eq!((abs, flag), (3, true), "first v2 advance seals v2");
    engine.submit_absorbed(a, 5, 0).await; // cross-layout v1 advance
    // Sentinel append proves the committer processed the op above.
    append_sized(&engine, a, &key, "", 64).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let h = engine.stream_handle(a).await.unwrap();
    let (abs, flag) = {
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    };
    assert_eq!(
        (abs, flag),
        (3, true),
        "a v1 advance on a sealed-v2 stream must be dropped whole"
    );

    // Stream B: sealed v1 by its first advance; a later v2 AbsorbedBatch
    // entry must be dropped — the flag must never flip mid-stream.
    let b = [0xF2u8; 16];
    for _ in 0..5 {
        append_sized(&engine, b, &key, "", 512).await;
    }
    engine.submit_absorbed(b, 3, 0).await;
    let (abs, flag) = wait_absorbed(&engine, b, 3).await;
    assert_eq!((abs, flag), (3, false), "first v1 advance seals v1");
    engine.submit_absorbed_batch_v2(vec![(b, 5, 0)]).await;
    append_sized(&engine, b, &key, "", 64).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let h = engine.stream_handle(b).await.unwrap();
    let (abs, flag) = {
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    };
    assert_eq!(
        (abs, flag),
        (3, false),
        "a v2 advance on a sealed-v1 stream must be dropped whole"
    );
    // Continuation on the SEALED lane still works.
    engine.submit_absorbed(b, 5, 0).await;
    let (abs, flag) = wait_absorbed(&engine, b, 5).await;
    assert_eq!((abs, flag), (5, false));
    assert!(
        engine
            .absorb_lane_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 2,
        "both cross-layout advances must be counted"
    );
    engine.begin_close();
}

/// ROUTING-V3 §5/§8.5: a sparse key spread across many canonical gaps
/// pages through the planner under the span budget (≤ 8 per response)
/// and the cursor advances via consumed_to — every match returned
/// exactly once, in order, across multiple partial responses, with no
/// per-offset GET pattern (structural: the reader only range-scans).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_key_reads_page_with_bounded_spans() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 104, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB1u8; 16];

    let db = slatedb::Db::builder("dst-sparsekey", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-sparsekey".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    // 24 appends of 512 records each (12,288 offsets): the key "sp"
    // hits every 512th offset (24 matches), buried in default-key
    // records. Batched multi-entry appends with per-entry keys are not
    // a workload-helper shape, so append per batch with the FIRST
    // record keyed via a dedicated single append then a filler batch.
    let mut expected = Vec::new();
    let mut off = 0u64;
    for i in 0..24u64 {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "sp", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(
                serde_json::json!({"op": i, "att": 0, "k": "sp"})
                    .to_string()
                    .into_bytes(),
            )],
            usage: crate::usage::counters(&hash),
            routing_key: "sp".to_string(),
            key_hash: crate::crypto::stream_hash("sp"),
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
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
        expected.push((i, 0u32));
        off += 1;
        append_n(&engine, hash, &key, 511, 40).await;
        off += 511;
    }
    let _ = off;
    wait_all_absorbed(&engine, &[hash]).await;

    let _ds: Arc<dyn ObjectStore> = store.clone();
    let got = drain_filtered(&engine, hash, &key, "sp").await;
    assert_eq!(
        got, expected,
        "sparse keyed paging lost or reordered records"
    );
    assert!(
        crate::history::READ_FRAMES_MATCHED.load(Ordering::Relaxed) > 0,
        "the postings planner path must have served this"
    );
    engine.begin_close();
}

/// ROUTING-V3 §8.6: a corrupt postings page must never surface as
/// completed=true over an unverified range — the reader falls back to
/// ONE bounded canonical envelope scan (exact-key filtered), counts
/// the corruption, and still returns every record exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_postings_fall_back_to_the_envelope() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 105, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB3u8; 16];

    let db = slatedb::Db::builder("dst-corruptp", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-corruptp".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    for i in 0..30u64 {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "ck", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(
                serde_json::json!({"op": i, "att": 0, "k": "ck"})
                    .to_string()
                    .into_bytes(),
            )],
            usage: crate::usage::counters(&hash),
            routing_key: "ck".to_string(),
            key_hash: crate::crypto::stream_hash("ck"),
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
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
    }
    wait_all_absorbed(&engine, &[hash]).await;

    // Corrupt the key's page in place: contiguous matches from offset 0
    // make the page key fully deterministic (bucket 0, page_first 0).
    let part = engine.history_partition().await.expect("partition");
    let pk = crate::postings::postings_key(
        crate::crypto::RouteHash(hash),
        crate::crypto::SegmentHash(hash),
        &crate::postings::rk_hash("ck"),
        0,
        0,
    );
    // The partition is WAL-disabled: a default (await-durable) put would
    // wait for a flush that only comes later — write like the gather
    // does, then flush explicitly.
    let mut wb = slatedb::WriteBatch::new();
    wb.put(&pk, b"garbage-not-a-page");
    part.write_with_options(wb, &slatedb::config::WriteOptions::default())
        .await
        .expect("corrupt");
    part.flush().await.expect("flush corruption");

    let before = crate::history::POSTINGS_CORRUPT.load(Ordering::Relaxed);
    let _ds: Arc<dyn ObjectStore> = store.clone();
    // The absorber write-through-warmed the slice cache with the (valid)
    // runs it encoded; served from there, the corruption would never be
    // touched. The envelope contract is about a COLD index read — model
    // the instance that did not absorb this data.
    engine.postings_cache.sweep_idle(std::time::Duration::ZERO);
    let got = drain_filtered(&engine, hash, &key, "ck").await;
    let want: Vec<(u64, u32)> = (0..30u64).map(|i| (i, 0u32)).collect();
    assert_eq!(got, want, "envelope fallback lost records");
    assert!(
        crate::history::POSTINGS_CORRUPT.load(Ordering::Relaxed) > before,
        "corruption must be counted"
    );
    engine.begin_close();
}

/// Spec §7: a key's second read must be served from the decoded slice
/// cache — no new physical index load — and repeated reads keep
/// hitting. (The ≥90% active-window hit-rate gate runs in the
/// acceptance campaign; this pins the mechanism.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_keyed_reads_hit_the_postings_cache() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 107, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB5u8; 16];

    let db = slatedb::Db::builder("dst-pcache", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-pcache".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );
    for i in 0..8u64 {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "hot", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: vec![bytes::Bytes::from(
                serde_json::json!({"op": i, "att": 0, "k": "hot"})
                    .to_string()
                    .into_bytes(),
            )],
            usage: crate::usage::counters(&hash),
            routing_key: "hot".to_string(),
            key_hash: crate::crypto::stream_hash("hot"),
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
        assert!(engine.try_enqueue(req).is_ok());
        rx.await.expect("resp").expect("ack");
        append_n(&engine, hash, &key, 32, 64).await;
    }
    wait_all_absorbed(&engine, &[hash]).await;

    let _ds: Arc<dyn ObjectStore> = store.clone();
    let cache = &engine.postings_cache;

    // Write-through warming (spec §7): the absorber installed the runs
    // it just wrote, so even the FIRST read pays no index round trip.
    let first = drain_filtered(&engine, hash, &key, "hot").await;
    assert_eq!(first.len(), 8);
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        0,
        "first read after in-process absorption must be warm: {} / slice {:?}",
        cache.stats(),
        cache.debug_slice(
            &crate::crypto::SegmentHash(hash),
            &crate::postings::rk_hash("hot")
        ),
    );
    assert!(cache.hits.load(Ordering::Relaxed) >= 1);
    assert!(cache.warm_installs.load(Ordering::Relaxed) >= 1);

    // Simulate an instance that did NOT absorb this data (restart /
    // ownership move): sweep everything, then the cold-load contract
    // applies — one physical load, then hits.
    cache.sweep_idle(std::time::Duration::ZERO);
    let again = drain_filtered(&engine, hash, &key, "hot").await;
    assert_eq!(again, first);
    let loads_after_cold = cache.index_loads.load(Ordering::Relaxed);
    let hits_after_cold = cache.hits.load(Ordering::Relaxed);
    assert!(loads_after_cold >= 1, "swept cache must load the index");

    for _ in 0..5 {
        let warm = drain_filtered(&engine, hash, &key, "hot").await;
        assert_eq!(warm, first);
    }
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        loads_after_cold,
        "warm reads must not touch the physical index"
    );
    assert!(
        cache.hits.load(Ordering::Relaxed) >= hits_after_cold + 5,
        "warm reads must be cache hits"
    );
    engine.begin_close();
}
