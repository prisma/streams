//! R09: dirty-index discovery pages progress without exceeding the pending
//! capacity, and a rescan must not make a gather retire bytes twice.
#![cfg(test)]
use super::*;
use object_store::ObjectStore;
use slatedb::WriteBatch;

#[test]
fn r09_hot_prefix_cannot_starve_other_due_streams() {
    let now = Instant::now();
    let cfg = AbsorberConfig::default();
    let pending: HashMap<_, _> = (0..6u8)
        .map(|id| {
            (
                [id; 16],
                PendingAbsorb {
                    bytes: u64::MAX - id as u64,
                    since: now,
                    failures: 0,
                    retry_after: None,
                },
            )
        })
        .collect();
    let first: Vec<_> = due_streams(&pending, &cfg, now, None)
        .into_iter()
        .take(3)
        .map(|(hash, _)| hash)
        .collect();
    let second: Vec<_> = due_streams(&pending, &cfg, now, first.last().copied())
        .into_iter()
        .take(3)
        .map(|(hash, _)| hash)
        .collect();
    assert_eq!(first, vec![[0; 16], [1; 16], [2; 16]]);
    assert_eq!(second, vec![[3; 16], [4; 16], [5; 16]]);
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "r09_discovery_pages_progress_without_exceeding_pending_capacity; the fixture's stream ids are small loop counters far inside every width they convert to; checked conversions would only restate the loop bounds"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r09_discovery_pages_progress_without_exceeding_pending_capacity; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r09_discovery_pages_progress_without_exceeding_pending_capacity() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r09-history", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let mut batch = WriteBatch::new();
    for id in 0..260u64 {
        let mut hash = [0; 16];
        hash[..8].copy_from_slice(&id.to_be_bytes());
        let marker = crate::shard::dirty_value_for_tests(&crate::shard::StreamMaintenance {
            next: 1,
            unabsorbed_bytes: 64,
            ..Default::default()
        });
        let width = [16, 24, 32][id as usize % 3];
        batch.put(crate::shard::dirty_key(&hash), &marker[..width]);
        batch.put(
            crate::shard::tail_key(&hash),
            crate::shard::encode_tail_for_tests(&crate::shard::TailFields {
                next: 1,
                unabsorbed_bytes: 64,
                route: [1; 16],
                ..Default::default()
            }),
        );
    }
    db.write(batch).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "r09-history".into(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let absorber = Absorber::new(engine.clone(), AbsorberConfig::default());
    let mut pending = HashMap::new();
    assert_eq!(
        absorber.seed_from_dirty_index(&mut pending).await.unwrap(),
        DISCOVERY_PAGE_STREAMS
    );
    assert_eq!(pending.len(), DISCOVERY_PAGE_STREAMS);
    assert!(absorber.discovery_after.lock().unwrap().is_some());
    assert_eq!(
        absorber.seed_from_dirty_index(&mut pending).await.unwrap(),
        4
    );
    assert_eq!(pending.len(), 260);
    assert!(absorber.discovery_after.lock().unwrap().is_none());
    pending.clear();
    for id in 0..MAX_PENDING_STREAMS {
        let mut hash = [255; 16];
        hash[..8].copy_from_slice(&(id as u64).to_be_bytes());
        pending.insert(
            hash,
            PendingAbsorb {
                bytes: 1,
                since: Instant::now(),
                failures: 0,
                retry_after: None,
            },
        );
    }
    absorber.seed_from_dirty_index(&mut pending).await.unwrap();
    assert_eq!(pending.len(), MAX_PENDING_STREAMS);
    engine.begin_close();
    let _ = db.close().await;
}

// ---- a rescan racing an in-flight boundary advance ----------------------
// The release hold on the capacity run's one-off 500: the discovery rescan
// reads the dirty index while a gather's advance still waits in the
// committer queue, takes the submitted mark for a stranded one and rolls it
// back; the next gather re-reads from the durable boundary, and the
// committer retires the overlap's bytes a second time. When the ledger is
// smaller than the double count the whole commit group is rejected
// ("maintenance accounting diverged"), and every append in it answers
// Internal: HTTP 500 on both append surfaces.

type AppendAnswer = Result<crate::shard::AppendAck, crate::shard::AppendErr>;

/// One 100-byte record for `hash`, enqueued; the receiver is its answer.
fn enqueue_record(
    engine: &ShardEngine,
    hash: [u8; 16],
) -> tokio::sync::oneshot::Receiver<AppendAnswer> {
    let key = crate::crypto::StreamKey([7; 32]);
    let (resp, answer) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: Instant::now(),
        hash,
        route: hash,
        entries: vec![bytes::Bytes::from(vec![0x5a; 100])],
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: [7; 16],
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: crate::crypto::derive_subkey(&key, &hash, "", 0),
        ts_hint_ms: None,
        seq: None,
        bytes: 100,
        finish: crate::shard::AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp,
    };
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    answer
}

async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(Instant::now() < deadline, "{what} never happened");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// What a regather left behind: the answer of the append queued behind it,
/// the stream's applied tail and the shard ledger once that append
/// answered, and the stored bytes of each record by offset (all nine when
/// the append committed).
struct Regather {
    answer: AppendAnswer,
    tail: crate::shard::TailFields,
    ledger: u64,
    stored: Vec<u64>,
}

/// The stored bytes of [absorbed, next), given each record's stored bytes
/// by offset.
fn exact_ledger(tail: &crate::shard::TailFields, stored: &[u64]) -> u64 {
    (0u64..)
        .zip(stored)
        .filter(|(offset, _)| (tail.absorbed..tail.next).contains(offset))
        .map(|(_, bytes)| bytes)
        .sum()
}

/// The stream ledger holds exactly the stored bytes of [absorbed, next),
/// and the shard's maintenance ledger agrees with it.
fn assert_ledger_is_exact(regather: &Regather) {
    let tail = &regather.tail;
    let exact = exact_ledger(tail, &regather.stored);
    assert_eq!(
        tail.unabsorbed_bytes, exact,
        "the stream ledger is not the stored bytes of [{}, {})",
        tail.absorbed, tail.next
    );
    assert_eq!(
        regather.ledger, exact,
        "the shard ledger is not the stream's"
    );
}

/// Stored frame bytes of one record, if it is stored.
async fn stored_len(engine: &ShardEngine, hash: &[u8; 16], offset: u64) -> Option<u64> {
    let row = engine.db.get(crate::shard::record_key(hash, offset)).await;
    row.unwrap().map(|value| value.len() as u64)
}

/// An engine over a fault store whose WAL puts a test can hold, with an
/// absorber whose gathers the test drives.
async fn rig(name: &str) -> (Arc<ShardEngine>, Absorber, Arc<crate::dst::FaultStore>) {
    let store = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        0x47,
        crate::dst::FaultPlan::new(0, 0, 0),
    );
    let db = Db::builder(name, store.clone() as Arc<dyn ObjectStore>)
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(5)),
            manifest_poll_interval: Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let (tx, _signals) = mpsc::channel(1);
    let engine = ShardEngine::start(
        name.into(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        tx,
        None,
        maintenance,
    );
    let absorber = Absorber::new(engine.clone(), AbsorberConfig::default());
    (engine, absorber, store)
}

/// Four records durable and gathered by G1, whose advance waits in the held
/// committer queue; four more records made durable meanwhile; with
/// `rescan`, a discovery pass before G2 gathers. The answer is that of an
/// append queued behind G2's advance, in the one group that applies both.
async fn append_behind_a_regather(rescan: bool) -> Regather {
    let (engine, absorber, store) = rig("regather").await;
    let hash = [0x47; 16];
    let handle = engine.stream_handle(hash).await.unwrap();
    for _ in 0..4 {
        enqueue_record(&engine, hash).await.unwrap().unwrap();
    }
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let held: Vec<_> = (0..4).map(|_| enqueue_record(&engine, hash)).collect();
    wait_until("four records applied behind a held WAL write", || {
        engaged.load(std::sync::atomic::Ordering::SeqCst) >= 1
            && handle.state.lock().unwrap().applied.next == 8
    })
    .await;
    let gate = engine.test_hold_commit().await;
    let g1 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        g1.advanced.first().map(|a| a.1),
        Some(4),
        "G1 gathers [0, 4)"
    );
    store.release_hold();
    for answer in held {
        answer.await.unwrap().unwrap();
    }
    let mut stored = Vec::new();
    for offset in 0..8 {
        stored.push(stored_len(&engine, &hash, offset).await.unwrap());
    }
    if rescan {
        absorber
            .seed_from_dirty_index(&mut HashMap::new())
            .await
            .unwrap();
    }
    let g2 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(g2.advanced.first().map(|a| a.1), Some(8), "G2 gathers to 8");
    let queued = enqueue_record(&engine, hash);
    drop(gate);
    let answer = queued.await.unwrap();
    stored.extend(stored_len(&engine, &hash, 8).await);
    let tail = handle.state.lock().unwrap().applied.clone();
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    engine.begin_close();
    Regather {
        answer,
        tail,
        ledger,
        stored,
    }
}

/// The control: without the rescan, G2 starts at G1's submitted mark and
/// the group applies both advances and the append.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_regather_from_the_submitted_mark_retires_each_byte_once() {
    let regather = append_behind_a_regather(false).await;
    assert!(regather.answer.is_ok(), "{:?}", regather.answer);
    assert_ledger_is_exact(&regather);
}

/// The rescan used to roll G1's in-flight mark back, so G2 re-read [0, 4)
/// and the committer retired those bytes twice: the co-grouped append was
/// refused as `Internal("maintenance accounting diverged")`. Whatever G2
/// covers, the append commits and each byte leaves the ledger once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rescan_during_an_inflight_advance_never_fails_an_append() {
    let regather = append_behind_a_regather(true).await;
    assert!(
        regather.answer.is_ok(),
        "an append co-grouped with a re-gathered advance was refused: {:?}",
        regather.answer
    );
    assert_ledger_is_exact(&regather);
}
