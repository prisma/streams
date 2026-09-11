//! History absorption.

use super::fixture_storage::{drain_filtered, mem, open_engine_with_absorber, skey};
use crate::dst::{FaultPlan, FaultStore, OpLog, Workload, drain_observed, mech};
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
            batch_puts: 256,
            pass_bytes: 8 * 1024 * 1024,
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
            batch_puts: 256,
            pass_bytes: 8 * 1024 * 1024,
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
            batch_puts: 256,
            pass_bytes: 8 * 1024 * 1024,
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
