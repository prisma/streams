use super::*;
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::shard::{AppendFinish, AppendReq, ShardConfig};
use bytes::Bytes;
use std::sync::atomic::Ordering;

async fn active_absorber_cancel(hold_store: bool) {
    let shard_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let data_store = FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        9091,
        FaultPlan::CLEAN,
    );
    let resources = Arc::new(HistoryResources::with_body_limit(
        &crate::config::HistoryConfig {
            absorb_global_budget_bytes: 1 << 20,
            absorb_global_gathers: 2,
            ..Default::default()
        },
        1 << 16,
        1 << 16,
    ));
    let held_budget = (!hold_store).then(|| resources.budget.reserve(resources.budget.capacity()));
    let held_budget = match held_budget {
        Some(reservation) => Some(reservation.await),
        None => None,
    };
    let path = if hold_store {
        "r09-absorb-store"
    } else {
        "r09-absorb-budget"
    };
    let cfg = ShardConfig {
        shared_history: Some(resources.clone()),
        ..Default::default()
    };
    let open = || {
        let store = shard_store.clone();
        async move {
            crate::bootstrap::on_slatedb_rt(async move {
                Db::builder(path, store)
                    .with_settings(Settings {
                        flush_interval: Some(Duration::from_millis(5)),
                        ..Default::default()
                    })
                    .build()
                    .await
            })
            .await
            .unwrap()
        }
    };
    let db = Arc::new(open().await);
    let (absorb_tx, absorb_rx) = absorber_channel();
    let engine = ShardEngine::start(
        path.into(),
        db.clone(),
        data_store.clone(),
        cfg.clone(),
        absorb_tx,
        None,
        Default::default(),
    );
    let hash = [0x91; 16];
    let (reply, ack) = tokio::sync::oneshot::channel();
    engine
        .try_enqueue(AppendReq {
            usage: Default::default(),
            hash,
            // Every current-format stream has a name-level route; zero
            // denotes the removed legacy layout and is intentionally refused.
            route: [8; 16],
            enqueued_at: Instant::now(),
            entries: vec![Bytes::from_static(b"durable retry payload")],
            routing_key: String::new(),
            key_hash: crate::crypto::stream_hash(""),
            producer_lineage: Vec::new(),
            key_version: 1,
            subkey: [7; 32],
            ts_hint_ms: None,
            seq: None,
            bytes: 21,
            finish: AppendFinish::Open,
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            billing: None,
            resp: reply,
        })
        .unwrap();
    ack.await.unwrap().unwrap();
    let entered = hold_store.then(|| data_store.hold_class(StoreOp::Put, ObjClass::Sst, u64::MAX));
    let absorber_cfg = AbsorberConfig {
        tick: Duration::from_millis(10),
        threshold_bytes: 1,
        threshold_age: Duration::ZERO,
        ..Default::default()
    };
    Absorber::start_owned(
        data_store.clone(),
        engine.clone(),
        Arc::new(KeyCache::default()),
        absorber_cfg.clone(),
        absorb_rx,
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let parked = match &entered {
                Some(entered) => entered.load(Ordering::SeqCst) > 0,
                // The holder owns slot one and ALL bytes. Slot two can
                // only disappear once the actual pump entered reserve.
                None => resources.budget.gathers.available_permits() == 0,
            };
            if parked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual absorber must enter its held budget/storage operation");
    let tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!((tail.absorbed, tail.next), (0, 1));
    let (debt, _) = engine.scan_dirty_streams_page(None, 10).await.unwrap();
    assert_eq!(debt.len(), 1);
    engine.begin_close();
    engine
        .await_workers(Duration::from_secs(1))
        .await
        .expect("joined absorber must cancel without releasing held operation");
    assert_eq!(resources.budget.inflight(), u64::from(!hold_store));
    assert_eq!(
        resources.budget.gathers.available_permits(),
        if hold_store { 2 } else { 1 }
    );
    assert_eq!(engine.usage.absorb_pending_summary_for(path), None);
    assert_eq!(engine.usage.absorb_lag(SegmentHash(hash)), 0);
    data_store.release_hold();
    drop(held_budget);
    // Worker reservations are released before the barrier; the sole close
    // owner must also finish both stores before a fresh writer starts.
    engine
        .await_terminated(Duration::from_secs(10))
        .await
        .unwrap();
    let db = Arc::new(open().await);
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let (absorb_tx, absorb_rx) = absorber_channel();
    let retry = ShardEngine::start(
        path.into(),
        db.clone(),
        data_store.clone(),
        cfg,
        absorb_tx,
        None,
        maintenance,
    );
    Absorber::start_owned(
        data_store,
        retry.clone(),
        Arc::new(KeyCache::default()),
        absorber_cfg,
        absorb_rx,
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let tail = retry.tail_fields(&hash).await.unwrap().unwrap();
            if tail.absorbed == 1 {
                assert_eq!(tail.next, 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("durable dirty marker must replay without new customer traffic");
    retry.begin_close();
    retry
        .await_terminated(Duration::from_secs(1))
        .await
        .unwrap();
    let _ = db.close().await;
    if let Some(partition) = retry.history_partition_if_open() {
        let _ = partition.close().await;
    }
    assert_eq!(resources.budget.reserved_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_owned_absorber_cancels_entered_budget_and_replays_dirty_debt() {
    active_absorber_cancel(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_owned_absorber_cancels_entered_storage_and_replays_dirty_debt() {
    active_absorber_cancel(true).await;
}
