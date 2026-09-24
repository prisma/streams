//! History absorption.

use super::fixture_storage::{
    append_n, drain_filtered, mem, open_engine, open_engine_with_absorber, skey,
};
use crate::dst::{
    FaultPlan, FaultProfile, FaultStore, ObjClass, OpLog, Outcome, StoreOp, Workload,
    drain_observed, mech,
};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// I1 across the tier boundary. Records acknowledged on the shard log must
/// still be readable — through the production merged reader — after the
/// absorber has moved them into the history tier and the shard log has
/// been trimmed behind them.
///
/// The first version of this harness discarded the absorber channel
/// entirely and read straight off the shard log, so history DB creation,
/// block encryption, the absorbed-boundary publication, trimming and the
/// merge were all untested.
/// Looks at the stream's durable tail up to `tries` times, 25 ms apart,
/// until `ready` holds; returns the last tail seen, if the handle existed.
async fn durable_tail(
    engine: &crate::shard::ShardEngine,
    hash: [u8; 16],
    tries: usize,
    ready: impl Fn(&crate::shard::TailFields) -> bool,
) -> Option<crate::shard::TailFields> {
    let mut last = None;
    for _ in 0..tries {
        let Ok(handle) = engine.stream_handle(hash).await else {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            continue;
        };
        let tail = handle.state.lock().unwrap().durable.clone();
        let done = ready(&tail);
        last = Some(tail);
        if done {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acked_records_survive_absorption_into_history() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 17, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();
    let hash = [8u8; 16];
    let (engine, absorber) = open_engine_with_absorber(store.clone(), "dst-hist", hash, &key).await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["h1", "h2"], 40, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    // Wait for the absorbed boundary to advance past zero.
    let absorbed = durable_tail(&engine, hash, 400, |t| t.absorbed > 0)
        .await
        .map_or(0, |t| t.absorbed);
    assert!(
        absorbed > 0,
        "the absorber never advanced the boundary — the scenario would only \
         have tested the shard log again"
    );

    let _ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("absorbed={absorbed}: {e}\ncoverage={:?}", cov.snapshot());
    }
    if let Err(e) = cov.require(&[mech::READ_FROM_HISTORY]) {
        panic!("{e}");
    }
    absorber.abort();
}

/// Signals are the absorber's fast path, not its source of truth. The
/// signal channel is a bounded `try_send` (it provably drops ~35k of
/// 100k seed signals, docs/COST-WIDE1.md §3), and a restarted instance
/// has no signals for pre-crash data. The re-discovery sweep must find
/// unabsorbed streams from the engine's resident handles with NO signal
/// ever delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absorber_sweep_recovers_streams_whose_signals_were_lost() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 51, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [35u8; 16];

    let db = slatedb::Db::builder("dst-sweep", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    // The engine's signal channel goes nowhere: rx dropped on the spot.
    let (engine_tx, engine_rx) = crate::history::absorber_channel();
    drop(engine_rx);
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-sweep".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        engine_tx,
        None,
        __maint,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    keys.put(hash, key.clone(), hash);
    // The absorber listens on a channel that never carries a signal. Keep
    // the sender alive: a closed channel would exit the absorber loop.
    let (_quiet_tx, quiet_rx) = crate::history::absorber_channel();
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            sweep_every: 2,
            ..Default::default()
        },
        quiet_rx,
    );

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["s"], 15, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    // No signal was ever delivered; only the sweep can find this stream.
    let caught = |t: &crate::shard::TailFields| t.absorbed > 0 && t.absorbed == t.next;
    let caught_up = durable_tail(&engine, hash, 400, caught)
        .await
        .is_some_and(|t| caught(&t));
    assert!(
        caught_up,
        "the sweep never absorbed the signal-less stream — lost signals \
         mean lost absorption"
    );
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("sweep-absorbed stream lost records: {e}");
    }
    absorber.abort();
}

/// CHAOS 2026-08-09: a gather TRUNCATED by the per-stream byte cap
/// reported its stream as `advanced` — fully drained — and the tick
/// loop retired it from the pending set. Nothing re-drove the
/// remainder: 8x100 KiB behind a 64 KiB cap absorbed exactly ONE
/// record and then stopped forever (field repro: 3x32 MiB frames left
/// 67 MB parked in the shard log across sweeps, rescans, restarts and
/// fresh append signals — one record per process boot). Reads stayed
/// correct (they merge the shard log), so the damage is unbounded hot-
/// tier growth and a trim boundary that can never advance, which is
/// precisely the cost/memory failure the absorber exists to prevent.
/// The gather now reports a PARTIAL advance and the caller keeps such
/// streams pending, so absorption CONVERGES.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absorber_drains_records_larger_than_the_per_stream_gather_cap() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 77, FaultPlan::CLEAN);
    let key = skey();
    let hash = [77u8; 16];

    let db = slatedb::Db::builder("dst-bigrec", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (engine_tx, engine_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-bigrec".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        engine_tx,
        None,
        __maint,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    keys.put(hash, key.clone(), hash);
    // 64 KiB gather cap: EVERY record below is bigger, so every gather
    // is truncated after exactly one record.
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            gather_max_bytes: 64 * 1024,
            sweep_every: 100_000, // the sweep must NOT be what saves us
            ..Default::default()
        },
        engine_rx,
    );

    // ONE batched append carrying SIX oversized records: one signal,
    // six gathers needed. (Six separate appends would emit six signals
    // and mask the wedge — each fresh signal re-adds the stream to the
    // pending set, which is exactly why the field only saw this with
    // few-appends/much-data.)
    const RECORDS: usize = 6;
    const SIZE: usize = 100 * 1024;
    {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "k", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: (b'a'..)
                .take(RECORDS)
                .map(|fill| bytes::Bytes::from(vec![fill; SIZE]))
                .collect(),
            usage: crate::usage::counters(&hash),
            routing_key: "k".to_string(),
            key_hash: crate::crypto::stream_hash("k"),
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
        assert!(engine.try_enqueue(req).is_ok(), "enqueue rejected");
        rx.await.expect("ack channel").expect("append acked");
    }

    // Convergence: absorbed must REACH next. Before the fix this stuck
    // one record in and never moved again.
    let fully_drained = |t: &crate::shard::TailFields| t.next > 0 && t.absorbed == t.next;
    let tail = durable_tail(&engine, hash, 400, fully_drained).await;
    let last_seen = tail.as_ref().map_or((0, 0), |t| (t.absorbed, t.next));
    let drained = tail.is_some_and(|t| fully_drained(&t));
    assert!(
        drained,
        "absorption stalled with records larger than the gather cap: \
         absorbed={} next={} (a truncated gather must keep the stream pending)",
        last_seen.0, last_seen.1
    );
    absorber.abort();
}

/// History v2's headline property: absorption WITHOUT the customer key.
/// The gather lane copies raw encrypted frames into the shared
/// partition, so an absorber whose KeyCache is EMPTY must still absorb
/// — and the records must decode correctly on read, where the client
/// supplies the key. (v1 required the key server-side and stranded
/// key-expired backlogs; docs/COST-WIDE1.md §2.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_absorbs_without_customer_keys() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 61, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();
    let hash = [50u8; 16];

    let db = slatedb::Db::builder("dst-v2nokey", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-v2nokey".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    // NO keys.put: the v1 absorber would return key-missing forever.
    let keys = Arc::new(crate::history::KeyCache::default());
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            ..Default::default()
        },
        absorb_rx,
    );

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["", "vk"], 25, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    let tail = durable_tail(&engine, hash, 400, |t| t.absorbed > 0).await;
    let absorbed = tail.as_ref().map_or(0, |t| t.absorbed);
    if absorbed > 0 {
        assert!(
            tail.is_some_and(|t| t.history_v2),
            "absorption advanced without the v2 flag"
        );
    }
    assert!(
        absorbed > 0,
        "keyless v2 absorption never advanced — the gather lane still \
         depends on the customer key"
    );

    // Reads (client-supplied key) must see every acked record across the
    // boundary, and filters must work against the shared partition.
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("v2 keyless absorption lost records (absorbed={absorbed}): {e}");
    }
    let unkeyed = drain_filtered(&engine, hash, &key, "").await;
    assert_eq!(&unkeyed, &log.acked[""], "v2 empty-key filter broken");
    let keyed = drain_filtered(&engine, hash, &key, "vk").await;
    assert_eq!(&keyed, &log.acked["vk"], "v2 keyed filter broken");
    absorber.abort();
}

/// v2 history must survive the owner handing the shard to a NEW engine:
/// the flags/route round-trip through the durable tail, the successor
/// opens the shared partition itself (fencing the old writer), and every
/// acked record stays readable — without any customer key ever reaching
/// an absorber.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_history_survives_engine_handoff() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 67, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [51u8; 16];
    let prefix = "dst-v2reopen";

    let (a, absorber_a) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["r"], 20, false, &mut log).await;
    let absorbed = durable_tail(&a, hash, 400, |t| t.absorbed > 0)
        .await
        .map_or(0, |t| t.absorbed);
    assert!(absorbed > 0, "no v2 absorption before the handoff");

    // Successor opens the same shard; its first commit fences the old
    // owner, its partition open fences the old partition writer.
    let (b, absorber_b) = open_engine_with_absorber(store.clone(), prefix, hash, &key).await;
    // Same Workload: op numbering must continue, or the post-handoff ops
    // collide with the pre-handoff ones in the shared OpLog.
    w.run(&b, hash, &key, &["r"], 5, false, &mut log).await;

    let _ds: Arc<dyn ObjectStore> = store.clone();
    let observed = drain_observed(&b, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("v2 history lost records across the handoff: {e}");
    }
    {
        let h = b.stream_handle(hash).await.expect("handle");
        let st = h.state.lock().unwrap();
        assert!(
            st.durable.history_v2,
            "v2 flag lost across the tail round-trip"
        );
    }
    absorber_a.abort();
    absorber_b.abort();
    a.begin_close();
}

/// R26-1 regression: age absorption takes EVERYTHING — there is no
/// sparse floor. The deleted interim policy (age absorption gated on
/// min_age_bytes) could permanently trap an instance: a sub-threshold
/// residual never retires, the durable no-progress clock ages past
/// MAX_ABSORB_LAG_SECS, the LagSecs latch sheds every append on the
/// instance — and shed appends are the only way the residual could ever
/// grow eligible. The 2026-08-11 soak measured a 154 KiB residual at
/// 938 s of stall, one evaluator tick from that deadlock.
///
/// Gate: a tiny stream (far under the old 256 KiB floor) and a fat one
/// BOTH age-absorb; the shard's durable maintenance ledger returns to
/// zero, so the progress latch has nothing to trip on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 57, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let tiny = [40u8; 16];
    let fat = [41u8; 16];

    let db = slatedb::Db::builder("dst-defer", store.clone() as Arc<dyn ObjectStore>)
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
        "dst-defer".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    keys.put(tiny, key.clone(), tiny);
    keys.put(fat, key.clone(), fat);
    let absorber = crate::history::Absorber::start(
        store.clone(),
        engine.clone(),
        keys,
        crate::history::AbsorberConfig {
            // Byte threshold out of reach; age immediate — absorption
            // happens purely through the age trigger, which must take
            // the 2-record stream exactly like the 60-record one.
            threshold_bytes: 64 * 1024 * 1024,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            ..Default::default()
        },
        absorb_rx,
    );

    // Pause absorption while the workloads run so the final
    // "absorbed == next" comparison is not racing a mid-workload pass.
    engine
        .history_resources
        .paused
        .store(true, Ordering::Relaxed);
    let mut tiny_log = OpLog::default();
    let mut w1 = Workload::new(cov.clone());
    // ~2 small frames pending: far under the deleted 256 KiB floor —
    // exactly the residual shape that used to defer forever.
    w1.run(&engine, tiny, &key, &["d"], 2, false, &mut tiny_log)
        .await;
    let mut fat_log = OpLog::default();
    let mut w2 = Workload::new(cov.clone());
    w2.run(&engine, fat, &key, &["d"], 60, false, &mut fat_log)
        .await;
    assert!(
        engine.maintenance_snapshot().unabsorbed_frame_bytes > 0,
        "workloads committed but the durable ledger shows no backlog"
    );
    engine
        .history_resources
        .paused
        .store(false, Ordering::Relaxed);

    // BOTH streams must age-absorb — the tiny one especially.
    let aged = |t: &crate::shard::TailFields| t.absorbed > 0 && t.absorbed == t.next;
    for (name, hash) in [("tiny", tiny), ("fat", fat)] {
        let absorbed = durable_tail(&engine, hash, 400, aged)
            .await
            .is_some_and(|t| aged(&t));
        assert!(absorbed, "the {name} stream never age-absorbed");
    }

    // With everything retired the shard's durable maintenance ledger is
    // zero — the no-progress latch has nothing to age on. THIS is the
    // deadlock gate: a permanent residual here means an eventual
    // instance-wide LagSecs shed that no append could ever clear.
    let mut drained = false;
    for _ in 0..400 {
        if engine.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        drained,
        "durable maintenance ledger kept a residual after full absorption"
    );

    // Both streams stay fully readable through the merged reader.
    let _ds: Arc<dyn ObjectStore> = store.clone();
    let obs_tiny = drain_observed(&engine, tiny, &key, &cov).await;
    tiny_log
        .audit(&obs_tiny)
        .expect("tiny stream readable from the shard log");
    let obs_fat = drain_observed(&engine, fat, &key, &cov).await;
    fat_log
        .audit(&obs_fat)
        .expect("fat stream readable after absorption");
    absorber.abort();
}

/// TLA-016-F1: an advance whose chunk does not start at the boundary
/// retires the stored bytes of the range it advances over, never the
/// chunk's reported count. When that range cannot be read whole the
/// committer guesses nothing: the group, a rider append included, is
/// refused, and the boundary, the ledger and the durable row stay put.
#[expect(
    clippy::disallowed_methods,
    reason = "maintenance group fixture; the rider request is joined after the refused group is released; it must ride the group concurrently to observe the refusal"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn misaligned_absorbed_chunk_retires_stored_bytes_or_refuses_the_group() {
    let store = mem();
    let engine = open_engine(store.clone(), "dst-f1exact").await;
    let key = skey();
    let hash = [30u8; 16];
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov.clone());
    let mut frames = Vec::new();
    for i in 0..4 {
        let out = w
            .attempt_with_deadline(&engine, hash, &key, "k", &format!("m{i}"), None, None)
            .await;
        assert!(matches!(out, Outcome::Acked { .. }));
        let row = engine.db.get(crate::shard::record_key(&hash, i)).await;
        frames.push(row.unwrap().unwrap().len() as u64);
    }
    // Chunk [1, 2) with a bogus count, applied at boundary 0: the committer
    // retires the stored bytes of [0, 2).
    engine.submit_absorbed(hash, 1, 2, 999_999).await;
    let mut tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    for _ in 0..400 {
        if tail.absorbed == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    }
    let owed = frames[2] + frames[3];
    assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (2, owed));
    assert_eq!(engine.maintenance_snapshot().unabsorbed_frame_bytes, owed);
    let row_before = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();

    // Record 3 disappears underneath the committer, so [2, 4) cannot be
    // read whole for the chunk [3, 4).
    let _deleted = engine
        .db
        .delete(crate::shard::record_key(&hash, 3))
        .await
        .unwrap();
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let (e2, k2) = (engine.clone(), key.clone());
    let mut w2 = Workload::new(cov.clone());
    w2.max_attempts = 1;
    let rider = tokio::spawn(async move {
        w2.attempt_with_deadline(&e2, hash, &k2, "k", "doomed", None, None)
            .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.submit_absorbed(hash, 3, 4, frames[3]).await;
    drop(hold);
    let out = rider.await.unwrap();
    assert!(
        !matches!(out, Outcome::Acked { .. }),
        "an unreadable retirement must refuse the group, but the rider acked: {out:?}"
    );
    let after = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!(
        (after.absorbed, after.next, after.unabsorbed_bytes),
        (2, 4, owed)
    );
    let row_after = crate::shard::decode_shard_maint(
        &engine
            .db
            .get(crate::shard::shard_maint_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(row_after, row_before, "durable row must not move");
    engine.begin_close();
}

/// TLA-016-F1 cost: a mis-started advance recounts every chunk refused
/// groups carried, inside the committer, so its scan must read ahead like
/// the gather did. Over 6,144 stored records (~1,600 blocks) with no block
/// cache and 10 ms per SST read, it issues a handful of requests, several
/// in flight at once, where one block per request would be ~1,600 reads in
/// series. An aligned advance reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mis_started_recount_reads_ahead_and_an_aligned_advance_reads_nothing() {
    let slow_sst_reads = FaultPlan {
        latency_pct: 100,
        latency_ms: (10, 10),
        ..FaultPlan::CLEAN
    };
    let profile = FaultProfile::clean().with_op_class(StoreOp::Get, ObjClass::Sst, slow_sst_reads);
    let store = FaultStore::new(mem(), 1, profile);
    let db = slatedb::Db::builder("dst-f1ahead/shard", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            ..Default::default()
        })
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let engine = crate::shard::ShardEngine::start(
        "dst-f1ahead".into(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        maintenance,
    );
    let key = skey();
    let (lagging, aligned) = ([31u8; 16], [32u8; 16]);
    for _ in 0..24 {
        append_n(&engine, lagging, &key, 256, 1024).await;
    }
    append_n(&engine, aligned, &key, 4, 1024).await;
    engine
        .db
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    // Tails are read from the resident handles: a stored-tail read would
    // itself be an SST read.
    let sst_reads = || store.count(StoreOp::Get, ObjClass::Sst);
    let owed = durable_tail(&engine, aligned, 1, |_| true)
        .await
        .unwrap()
        .unabsorbed_bytes;

    let before = sst_reads();
    engine
        .submit_absorbed_batch_v2(vec![(aligned, 0, 4, owed)])
        .await;
    let tail = durable_tail(&engine, aligned, 400, |t| t.absorbed == 4)
        .await
        .unwrap();
    assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (4, 0));
    assert_eq!(
        sst_reads(),
        before,
        "an aligned advance read stored records"
    );

    // The last record's chunk at boundary 0: the committer recounts all of
    // [0, 6144) at its read level.
    let next = 24 * 256;
    let before = sst_reads();
    engine
        .submit_absorbed_batch_v2(vec![(lagging, next - 1, next, 1)])
        .await;
    let tail = durable_tail(&engine, lagging, 400, |t| t.absorbed == next)
        .await
        .unwrap();
    let reads = sst_reads() - before;
    assert_eq!(
        (tail.absorbed, tail.unabsorbed_bytes),
        (next, 0),
        "the recount did not retire the whole range within 10 s ({reads} SST reads)"
    );
    assert!(
        reads <= 32,
        "the recount read one block per request: {reads} SST reads"
    );
    assert!(
        store.peak_gets_in_flight(ObjClass::Sst) >= 2,
        "the recount fetched its read-ahead windows one at a time"
    );
    engine.begin_close();
}

/// A refused absorption group rolls its lane marks back, so refusals do not
/// stack chunks onto the next accepted advance's recount. With one record
/// per chunk, eight consecutive groups carrying the stream's advance are
/// refused; each gather settles the committer's answer first and replays
/// the refused chunk. The first accepted advance then starts at the boundary
/// and the committer reads no stored record for it. Before the rollback the
/// absorber planned each chunk from the mark the refused one raised, and the
/// first accepted advance recounted all nine chunks inside the committer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_absorbed_groups_do_not_widen_the_next_recount() {
    const REFUSALS: usize = 8;
    let store = FaultStore::new(mem(), 1, FaultProfile::clean());
    let db = slatedb::Db::builder("dst-lagmark/shard", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            ..Default::default()
        })
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let engine = crate::shard::ShardEngine::start(
        "dst-lagmark".into(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        maintenance,
    );
    let key = skey();
    let (hash, rider) = ([33u8; 16], [34u8; 16]);
    append_n(&engine, hash, &key, REFUSALS + 4, 1024).await;
    engine
        .db
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 1,
            ..Default::default()
        },
    );
    for refused in 1..=REFUSALS {
        engine.fail_next_absorbed_group();
        let gather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
        assert_eq!(gather.advanced.len(), 1);
        let mut polls = 0;
        while engine.group_failures_tripped() < refused && polls < 400 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            polls += 1;
        }
        assert_eq!(
            engine.group_failures_tripped(),
            refused,
            "refusal {refused}"
        );
        // The committer runs groups in order: the rider's ack proves the
        // refused group is finished and its receipt answered.
        append_n(&engine, rider, &key, 1, 16).await;
    }
    // The accepted gather runs before its group, so every SST read after it
    // is the committer's.
    let sst_reads = || store.count(StoreOp::Get, ObjClass::Sst);
    let hold = engine.test_hold_commit().await;
    let accepted = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    let before = sst_reads();
    drop(hold);
    let (_, from, upto, _) = accepted.advanced[0];
    let tail = durable_tail(&engine, hash, 400, |t| t.absorbed >= upto)
        .await
        .unwrap();
    let recount_reads = sst_reads() - before;
    assert_eq!(tail.absorbed, upto, "the accepted advance never landed");
    assert!(
        upto <= 2,
        "after {REFUSALS} refused groups the first accepted advance (chunk [{from}, {upto})) \
         moved the boundary over {upto} one-record chunks, all recounted in the committer \
         ({recount_reads} SST reads)"
    );
    assert_eq!(
        (from, recount_reads),
        (0, 0),
        "a settled refusal replays its chunk from the boundary, which the committer trusts unread"
    );
    engine.begin_close();
}
