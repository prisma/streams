//! Fixture storage.

use crate::dst::AttemptId;
use object_store::ObjectStore;
use std::sync::Arc;

pub(super) fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

pub(super) fn skey() -> crate::crypto::StreamKey {
    crate::crypto::StreamKey([7u8; 32])
}

// ---- scenarios over the real engine ---------------------------------

/// Open the engine WITHOUT an absorber: reads come from the shard log
/// only. Used by scenarios that are about the commit path.
pub(super) async fn open_engine(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> Arc<crate::shard::ShardEngine> {
    open_engine_cfg(store, prefix, crate::shard::ShardConfig::default()).await
}

pub(super) async fn open_engine_cfg(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    cfg: crate::shard::ShardConfig,
) -> Arc<crate::shard::ShardEngine> {
    // Mirror production: with the pump on, SlateDB's own flush timer is a
    // long failsafe (else it flushes mid-PUT commits itself and the pump's
    // gather/skip machinery never sees a busy generation).
    let flush_interval = if cfg.wal_group_commit {
        std::time::Duration::from_secs(1)
    } else {
        std::time::Duration::from_millis(5)
    };
    open_engine_with_settings(
        store,
        prefix,
        cfg,
        slatedb::config::Settings {
            flush_interval: Some(flush_interval),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
}

/// Preserve a scenario's storage timing while sharing the real maintenance/open path.
pub(super) async fn open_engine_with_settings(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    cfg: crate::shard::ShardConfig,
    settings: slatedb::config::Settings,
) -> Arc<crate::shard::ShardEngine> {
    let db = slatedb::Db::builder(prefix, store.clone())
        .with_settings(settings)
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
    crate::shard::ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store,
        cfg,
        absorb_tx,
        None,
        __maint,
    )
}

// ---- the tiered read path -------------------------------------------

/// Open the engine WITH a real absorber, so records migrate into the
/// history tier and reads exercise the production merge.
///
/// Runs multi-threaded on purpose: the absorber reaches
/// `crate::bootstrap::on_slatedb_rt` (a process-global multi-threaded runtime) and
/// `spawn_blocking`, so it cannot be driven from a paused current-thread
/// test. That is precisely the coupling docs/DST.md's roadmap has to break
/// before whole-scenario replay is possible.
pub(super) async fn open_engine_with_absorber(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
) -> (Arc<crate::shard::ShardEngine>, tokio::task::JoinHandle<()>) {
    open_engine_with_absorber_layout(store, prefix, hash, key).await
}

pub(super) async fn open_engine_with_absorber_layout(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
) -> (Arc<crate::shard::ShardEngine>, tokio::task::JoinHandle<()>) {
    let db = slatedb::Db::builder(prefix, store.clone())
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
        prefix.to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let keys = Arc::new(crate::history::KeyCache::default());
    // The absorber derives subkeys from (key, epoch); the workload uses the
    // stream hash as the epoch, so the cache must agree or nothing decodes.
    keys.put(hash, key.clone(), hash);
    let cfg = crate::history::AbsorberConfig {
        threshold_bytes: 1,
        threshold_age: std::time::Duration::from_millis(1),
        tick: std::time::Duration::from_millis(20),
        batch_puts: 256,
        pass_bytes: 8 * 1024 * 1024,
        ..Default::default()
    };
    let handle = crate::history::Absorber::start(store, engine.clone(), keys, cfg, absorb_rx);
    (engine, handle)
}

/// Drain the merged reader WITH a key filter (drain_observed hardcodes
/// unfiltered reads): paginate read_merged and collect attempt ids.
#[expect(
    clippy::cast_possible_truncation,
    reason = "drain_filtered; the fixture's attempt numbers are small counters it wrote itself; a checked conversion would only restate the fixture"
)]
pub(super) async fn drain_filtered(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    filter: &str,
) -> Vec<AttemptId> {
    let mut out = Vec::new();
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut from = 0u64;
    for _ in 0..1024 {
        let res = crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            Some(filter),
            8 * 1024 * 1024,
            crate::shard::Deliver::Durable,
        )
        .await
        .expect("filtered read");
        for rec in &res.recs {
            let v: serde_json::Value = serde_json::from_slice(&rec.payload).expect("payload");
            let (op, att) = (v["op"].as_u64().unwrap(), v["att"].as_u64().unwrap() as u32);
            assert_eq!(
                v["k"].as_str().unwrap(),
                filter,
                "filter {filter:?} returned a record for key {:?}",
                v["k"]
            );
            out.push((op, att));
        }
        if res.completed {
            break;
        }
        match res.last {
            Some(last) if last + 1 > from => from = last + 1,
            _ => {}
        }
    }
    out
}

/// Direct append of a payload of chosen size (the workload helper only
/// sends tiny JSON bodies; the gather-budget tests need real volume).
pub(super) async fn append_sized(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    rk: &str,
    payload_bytes: usize,
) -> u64 {
    let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: vec![bytes::Bytes::from(vec![0x5au8; payload_bytes])],
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

pub(super) async fn wait_all_absorbed(
    engine: &Arc<crate::shard::ShardEngine>,
    hashes: &[[u8; 16]],
) {
    for h in hashes {
        let mut ok = false;
        for _ in 0..400 {
            let st = engine.stream_handle(*h).await.unwrap();
            let (a, n) = {
                let s = st.state.lock().unwrap();
                (s.durable.absorbed, s.durable.next)
            };
            if a == n && n > 0 {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(ok, "stream {:02x?} never fully absorbed", &h[..2]);
    }
}

/// Multi-record variant of append_sized: one request carrying `n`
/// records of `each` bytes (the mature-wave test needs deep per-stream
/// prefixes without 4,800 round-trips).
pub(super) async fn append_n(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    n: usize,
    each: usize,
) -> u64 {
    let subkey = crate::crypto::derive_subkey(key, &hash, "", 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: (0..n)
            .map(|_| bytes::Bytes::from(vec![0x5au8; each]))
            .collect(),
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
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
