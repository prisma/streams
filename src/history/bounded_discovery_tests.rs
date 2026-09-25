//! R09: dirty-index discovery pages progress without exceeding the pending
//! capacity, and a rescan must not make a gather retire bytes twice. A
//! re-gather over flushed chunks (after a refusal, or by a new owner) keeps
//! the index readable, and warms the slice cache only over rows it staged
//! (TLA-016-F3); a warm bridge never crosses a chunk the cache declined.
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

pub(super) type AppendAnswer = Result<crate::shard::AppendAck, crate::shard::AppendErr>;

/// One 100-byte record for `hash`, enqueued; the receiver is its answer.
pub(super) fn enqueue_record(
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

pub(super) async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
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
pub(super) async fn stored_len(engine: &ShardEngine, hash: &[u8; 16], offset: u64) -> Option<u64> {
    let row = engine.db.get(crate::shard::record_key(hash, offset)).await;
    row.unwrap().map(|value| value.len() as u64)
}

/// An engine over a fault store whose WAL puts a test can hold, with an
/// absorber whose gathers the test drives.
pub(super) async fn rig(name: &str) -> (Arc<ShardEngine>, Absorber, Arc<crate::dst::FaultStore>) {
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

/// The causal fix: the rescan leaves G1's in-flight mark alone, so G2
/// regathers from it, [4, 8), and both advances land exactly: the
/// boundary reaches 8, trimming follows one advance behind, and only the
/// queued append is left in the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rescan_during_an_inflight_advance_regathers_from_the_submitted_mark() {
    let regather = append_behind_a_regather(true).await;
    assert!(regather.answer.is_ok(), "{:?}", regather.answer);
    let tail = &regather.tail;
    assert_eq!(
        tail.absorbed, 8,
        "the regather after a rescan started below the in-flight advance"
    );
    assert_eq!((tail.trim_safe_to, tail.trimmed), (4, 4));
    assert_eq!(tail.unabsorbed_bytes, regather.stored[8]);
    assert_ledger_is_exact(&regather);
}

/// Append one record, wait for its answer and note its stored bytes.
pub(super) async fn append_stored(engine: &ShardEngine, hash: [u8; 16], stored: &mut Vec<u64>) {
    enqueue_record(engine, hash).await.unwrap().unwrap();
    let offset = u64::try_from(stored.len()).unwrap();
    stored.push(stored_len(engine, &hash, offset).await.unwrap());
}

pub(super) async fn applied_tail(engine: &ShardEngine, hash: [u8; 16]) -> crate::shard::TailFields {
    let handle = engine.stream_handle(hash).await.unwrap();
    let state = handle.state.lock().unwrap();
    state.applied.clone()
}

/// The stream's postings pages for its (empty) routing key tile: every
/// page decodes and none overlaps another, so keyed reads never fall back
/// to the envelope scan.
pub(super) async fn pages_tile(engine: &ShardEngine, hash: [u8; 16]) -> bool {
    let part = engine.history_partition().await.unwrap();
    let (route, inc, key) = (
        RouteHash(hash),
        SegmentHash(hash),
        crate::postings::rk_hash(""),
    );
    let (lo, hi) = crate::postings::postings_range(route, inc, &key, 0, u64::MAX);
    let mut pages = part.scan(lo..hi).await.unwrap();
    let mut runs = Vec::new();
    let mut seen = 0;
    while let Some(page) = pages.next().await.unwrap() {
        seen += 1;
        let decoded = crate::postings::decode_stored_page(route, inc, &key, &page.key, &page.value);
        if decoded
            .and_then(|page| crate::postings::append_page_runs(&mut runs, page))
            .is_none()
        {
            return false;
        }
    }
    seen > 0
}

/// A refused advance settles with its group: the stream's next gather
/// finds its mark stranded, rolls it back and regathers from the durable
/// boundary, so the ledger keeps no phantom backlog of the refused range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_advance_is_regathered_from_the_durable_boundary() {
    let (engine, absorber, _store) = rig("refused-regather").await;
    let hash = [0x48; 16];
    let mut stored = Vec::new();
    for _ in 0..4 {
        append_stored(&engine, hash, &mut stored).await;
    }
    engine.fail_next_absorbed_group();
    let g1 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        g1.advanced.first().map(|a| a.1),
        Some(4),
        "G1 gathers [0, 4)"
    );
    wait_until("G1's group refused", || {
        engine.group_failures_tripped() >= 1
    })
    .await;
    // FIFO behind the refused group: its receipt is gone once this answers.
    append_stored(&engine, hash, &mut stored).await;
    let g2 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(g2.advanced.first().map(|a| a.1), Some(5), "G2 gathers to 5");
    append_stored(&engine, hash, &mut stored).await;
    let tail = applied_tail(&engine, hash).await;
    assert_eq!(
        tail.absorbed, 5,
        "a refused advance was not regathered from the durable boundary"
    );
    assert_eq!(
        tail.unabsorbed_bytes, stored[5],
        "a refused advance left a phantom backlog"
    );
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(ledger, stored[5], "the shard ledger is not the stream's");
    engine.begin_close();
}

/// Skeptic C5: a refused advance heals at its stream's next gather even
/// when another stream's advance is in flight at every one of its plans —
/// a rollback waits only on its own stream's settlement bucket. Every
/// advance lands (none is dropped for not starting at its boundary), each
/// ledger is exact after every round, and the healed stream's postings
/// pages tile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_advance_heals_at_its_next_gather_under_a_busy_absorber() {
    let (engine, absorber, _store) = rig("busy-regather").await;
    let (x, y) = ([0x58; 16], [0x59; 16]);
    let (mut xs, mut ys) = (Vec::new(), Vec::new());
    for _ in 0..4 {
        append_stored(&engine, x, &mut xs).await;
    }
    engine.fail_next_absorbed_group();
    absorber.absorb_gather_v2(&[x]).await.unwrap();
    wait_until("X's group refused", || engine.group_failures_tripped() >= 1).await;
    append_stored(&engine, x, &mut xs).await;
    for round in 0..3 {
        append_stored(&engine, y, &mut ys).await;
        let gate = engine.test_hold_commit().await;
        let gy = absorber.absorb_gather_v2(&[y]).await.unwrap();
        let busy = applied_tail(&engine, y).await.absorbed;
        assert!(
            busy < ys.len() as u64,
            "round {round}: Y's advance is in flight"
        );
        let gx = absorber.absorb_gather_v2(&[x]).await.unwrap();
        let queued = enqueue_record(&engine, x);
        drop(gate);
        queued.await.unwrap().unwrap();
        xs.push(stored_len(&engine, &x, xs.len() as u64).await.unwrap());
        let (tx, ty) = (
            applied_tail(&engine, x).await,
            applied_tail(&engine, y).await,
        );
        let (ux, uy) = (gx.advanced[0].1, gy.advanced[0].1);
        assert_eq!(tx.absorbed, ux, "round {round}: X's advance was dropped");
        assert_eq!(ty.absorbed, uy, "round {round}: Y's advance was dropped");
        assert_eq!(tx.absorbed + 1, tx.next, "round {round}: X is not healed");
        let (lx, ly) = (exact_ledger(&tx, &xs), exact_ledger(&ty, &ys));
        assert_eq!(
            (tx.unabsorbed_bytes, ty.unabsorbed_bytes),
            (lx, ly),
            "round {round}"
        );
        let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
        assert_eq!(ledger, lx + ly, "round {round}: the shard ledger");
    }
    assert!(pages_tile(&engine, x).await, "X's postings pages overlap");
    engine.begin_close();
}

// ---- re-gathers over flushed chunks, and the warm install ---------------

/// Polls the stream's committed tail until `done` holds.
async fn tail_until(
    engine: &ShardEngine,
    hash: [u8; 16],
    done: impl Fn(&crate::shard::TailFields) -> bool,
) -> crate::shard::TailFields {
    for _ in 0..1000 {
        if let Some(tail) = engine.tail_fields(&hash).await.unwrap()
            && done(&tail)
        {
            return tail;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the stream's committed tail never reached the awaited state");
}

/// The keyed reader's own admission over `[0, next)`: every postings page of
/// `rk` decodes, agrees with every page it overlaps, and together they cover
/// each record exactly once.
async fn assert_postings_admit(
    engine: &ShardEngine,
    hash: [u8; 16],
    tail: &crate::shard::TailFields,
    rk: &str,
) {
    let part = engine.history_partition().await.unwrap();
    let route = crate::crypto::RouteHash(tail.route);
    let inc = crate::crypto::SegmentHash(hash);
    let kh = crate::postings::rk_hash(rk);
    let (lo, hi) = crate::postings::postings_range(route, inc, &kh, 0, tail.next);
    let mut pages = part.scan(lo..hi).await.unwrap();
    let mut runs = Vec::new();
    while let Some(kv) = pages.next().await.unwrap() {
        let admitted = crate::postings::decode_stored_page(route, inc, &kh, &kv.key, &kv.value)
            .and_then(|page| crate::postings::append_page_runs(&mut runs, page));
        assert!(
            admitted.is_some(),
            "a postings page fails to decode or disagrees with one it overlaps"
        );
    }
    let covered: u64 = runs.iter().map(|run| u64::from(run.count)).sum();
    assert!(crate::postings::ValidatedRuns::new(runs).is_some());
    assert_eq!(
        covered, tail.next,
        "the postings must cover every record once"
    );
}

/// The new upto of each advance a gather submitted.
fn ends(gather: &gather::GatherOutcome) -> Vec<u64> {
    gather.advanced.iter().map(|advance| advance.1).collect()
}

/// Once the stream is absorbed to its end, its ledger is exact, the key's
/// pages admit and a cold keyed history read serves every record from the
/// index: the cold load publishes a slice only when every page it scanned
/// admitted, and a refused index is served from the POSTINGS_CORRUPT
/// envelope instead. `stored` holds each record's stored bytes by offset.
async fn assert_regathered_index_admits(engine: &ShardEngine, hash: [u8; 16], stored: &[u64]) {
    let next = u64::try_from(stored.len()).unwrap();
    let tail = tail_until(engine, hash, |t| t.absorbed == next).await;
    assert_eq!(
        tail.unabsorbed_bytes,
        exact_ledger(&tail, stored),
        "the stream ledger is not the stored bytes of [{}, {})",
        tail.absorbed,
        tail.next
    );
    assert_postings_admit(engine, hash, &tail, "").await;
    engine.postings_cache.sweep_idle(Duration::ZERO);
    let part = engine.history_partition().await.unwrap();
    let (inc, route) = (SegmentHash(hash), RouteHash(tail.route));
    let (read, _, complete) = crate::history::read_history2_keyed_cached(
        &engine.postings_cache,
        &part,
        route,
        inc,
        "",
        0,
        next,
        next,
        1 << 20,
    )
    .await
    .unwrap();
    assert_eq!((read.len(), complete), (stored.len(), true));
    let slice = engine
        .postings_cache
        .debug_slice(&inc, &crate::postings::rk_hash(""));
    assert!(slice.is_some(), "the cold keyed read refused the index");
}

/// An engine over `store` as the shard opener starts it, with the
/// maintenance row loaded before it serves, and an absorber whose gathers
/// the test drives.
async fn open_owner(
    name: &str,
    store: &Arc<crate::dst::FaultStore>,
    cfg: crate::shard::ShardConfig,
) -> (Arc<ShardEngine>, Absorber) {
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
        cfg,
        tx,
        None,
        maintenance,
    );
    let absorber = Absorber::new(engine.clone(), AbsorberConfig::default());
    (engine, absorber)
}

/// `n` durable records gathered as [0, n), whose advance waits behind the
/// held committer; `n` more made durable meanwhile and gathered from that
/// chunk's lane mark as [n, 2n). Returns every record's stored bytes and
/// the held committer.
async fn two_chunks_in_flight<'a>(
    engine: &'a ShardEngine,
    absorber: &Absorber,
    store: &crate::dst::FaultStore,
    hash: [u8; 16],
    n: u64,
) -> (Vec<u64>, tokio::sync::MutexGuard<'a, ()>) {
    let mut stored = Vec::new();
    for _ in 0..n {
        append_stored(engine, hash, &mut stored).await;
    }
    let handle = engine.stream_handle(hash).await.unwrap();
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let held: Vec<_> = (0..n).map(|_| enqueue_record(engine, hash)).collect();
    wait_until("records applied behind a held WAL write", || {
        engaged.load(std::sync::atomic::Ordering::SeqCst) >= 1
            && handle.state.lock().unwrap().applied.next == 2 * n
    })
    .await;
    let commit = engine.test_hold_commit().await;
    let first = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        ends(&first),
        [n],
        "the first chunk stops at the durable end"
    );
    store.release_hold();
    for answer in held {
        answer.await.unwrap().unwrap();
    }
    for offset in n..2 * n {
        stored.push(stored_len(engine, &hash, offset).await.unwrap());
    }
    let second = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        ends(&second),
        [2 * n],
        "the second chunk starts at the first's mark"
    );
    (stored, commit)
}

/// G1 gathers [0, 4); G2, planned from G1's lane mark while G1 is still in
/// flight, gathers [4, 8), and each advance commits in a group of its own.
/// G1's group is refused, so G2 no longer starts at the boundary and is
/// dropped as detached, leaving the mark at 8 over a durable boundary of 0.
/// Both are settled at the next plan, which rolls the mark back and
/// regathers [0, 8): its page from 0 overlaps G2's flushed page from 4.
/// Both describe the same records, so the key's index still admits and the
/// ledger stays exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_regather_across_a_detached_chunk_keeps_the_index_readable() {
    let store = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        0x49,
        crate::dst::FaultPlan::new(0, 0, 0),
    );
    let one_op_groups = crate::shard::ShardConfig {
        max_batch_reqs: 1,
        ..Default::default()
    };
    let (engine, absorber) = open_owner("detached-regather", &store, one_op_groups).await;
    let hash = [0x49; 16];
    let (stored, commit) = two_chunks_in_flight(&engine, &absorber, &store, hash, 4).await;
    engine.fail_next_absorbed_group();
    drop(commit);
    wait_until("G1's group refused", || {
        engine.group_failures_tripped() >= 1
    })
    .await;
    // FIFO behind G2's group: once another stream's append answers, G2 was
    // staged and dropped, and both receipts are settled.
    enqueue_record(&engine, [0x4a; 16]).await.unwrap().unwrap();
    assert_eq!(
        applied_tail(&engine, hash).await.absorbed,
        0,
        "G2 moved the boundary without G1"
    );
    let mark = absorber.submitted.lock().unwrap().get(&hash).copied();
    assert_eq!(mark, Some((8, true)), "G2 raised the lane mark to 8");
    let regather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        ends(&regather),
        [8],
        "the settled stranded mark was not rolled back"
    );
    assert_regathered_index_admits(&engine, hash, &stored).await;
    engine.begin_close();
}

/// The owner closes with [0, 1) and [1, 2) flushed and both advances still
/// queued. The next owner has no lane marks and re-gathers [0, 2) over the
/// inherited pages; the key's index still admits and the ledger stays exact.
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable; the fixture closes the first owner's databases best effort; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable() {
    let store = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        0x4b,
        crate::dst::FaultPlan::new(0, 0, 0),
    );
    let hash = [0x4b; 16];
    let cfg = crate::shard::ShardConfig::default;
    let (engine, absorber) = open_owner("inherited-regather", &store, cfg()).await;
    let (stored, commit) = two_chunks_in_flight(&engine, &absorber, &store, hash, 1).await;
    engine.begin_close();
    if let Some(part) = engine.history_partition_if_open() {
        let _ = part.close().await;
    }
    let _ = engine.db.close().await;
    drop(commit);
    let (engine, absorber) = open_owner("inherited-regather", &store, cfg()).await;
    let regather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(ends(&regather), [2]);
    assert_regathered_index_admits(&engine, hash, &stored).await;
    engine.begin_close();
}

/// Polls until row 0 of the stream is trimmed and its absorbed boundary is
/// `absorbed`, both in the Remote-durable view a gather's scan reads.
async fn until_row0_trimmed_durably(engine: &ShardEngine, hash: [u8; 16], absorbed: u64) {
    let remote = slatedb::config::ReadOptions {
        durability_filter: slatedb::config::DurabilityLevel::Remote,
        ..Default::default()
    };
    for _ in 0..1000 {
        let row0 = engine
            .db
            .get_with_options(crate::shard::record_key(&hash, 0), &remote)
            .await
            .unwrap();
        let durable = engine
            .visible_absorbed(&hash, crate::shard::Deliver::Durable)
            .await
            .unwrap();
        if row0.is_none() && durable.0 == absorbed {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the advances and the trim of row 0 never became durable");
}

/// TLA-016-F3: a re-gather planned from a stale boundary warms the slice
/// cache only over the rows it staged. Two one-record chunks queue behind
/// the held committer and their lane mark is pruned, as a rescan prunes the
/// mark of a stream whose handle is not resident, settled or not. Both
/// advances then commit, the second trimming row 0, while publication is
/// held, so the re-gather plans from the published boundary 0 and its Remote
/// scan finds only row 1. An idle sweep has evicted the key's slice before
/// that install. A durable keyed read from 0 must still deliver record 0.
#[expect(
    clippy::let_underscore_must_use,
    reason = "stale_regather_never_warms_a_trimmed_head_as_absent; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_regather_never_warms_a_trimmed_head_as_absent() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("f3-warm", store.clone()).build().await.unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "f3-warm".into(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let (hash, key) = ([31u8; 16], crate::crypto::StreamKey([7u8; 32]));
    let coverage = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        1,
        crate::dst::FaultPlan::new(0, 0, 0),
    )
    .coverage();
    let writer = crate::dst::Workload::new(coverage);
    for body in ["r0", "r1"] {
        let out = writer
            .attempt_with_deadline(&engine, hash, &key, "k", body, None, None)
            .await;
        assert!(matches!(out, crate::dst::Outcome::Acked { .. }));
    }
    // A one-byte gather cap makes every record its own chunk.
    let absorber = Absorber::new(
        engine.clone(),
        AbsorberConfig {
            gather_max_bytes: 1,
            ..Default::default()
        },
    );
    let commit = engine.test_hold_commit().await;
    for upto in [1, 2] {
        let gather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
        assert_eq!(ends(&gather), [upto]);
    }
    // Stands for the rescan's prune of a non-resident stream's mark 2.
    assert!(absorber.submitted.lock().unwrap().remove(&hash).is_some());
    let dispatch = engine.test_hold_dispatch().await;
    drop(commit);
    until_row0_trimmed_durably(&engine, hash, 2).await;
    let handle = engine.stream_handle(hash).await.unwrap();
    assert_eq!(handle.state.lock().unwrap().durable.absorbed, 0);
    engine.postings_cache.sweep_idle(Duration::ZERO);
    let regather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        ends(&regather),
        [2],
        "the re-gather must reach the durable end"
    );
    drop(dispatch);
    for _ in 0..1000 {
        if handle.state.lock().unwrap().durable.absorbed == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let page = crate::http::read_merged(
        &key,
        &hash,
        &handle,
        &engine,
        0,
        Some("k"),
        1 << 20,
        crate::shard::Deliver::Durable,
    )
    .await
    .unwrap();
    let delivered: Vec<u64> = page.recs.iter().map(|rec| rec.off).collect();
    let slice = engine.postings_cache.debug_slice(
        &crate::crypto::SegmentHash(hash),
        &crate::postings::rk_hash("k"),
    );
    assert_eq!(
        delivered,
        [0, 1],
        "a keyed durable read skipped a durable record (completed={}, durable resume={}); the key's slice (covered_from, indexed_to, runs) is {slice:?}",
        page.completed,
        page.durable_resume(0),
    );
    engine.begin_close();
    let _ = db.close().await;
}

/// Polls until the stream's published durable absorbed boundary is `absorbed`.
async fn until_published(handle: &crate::shard::StreamHandle, absorbed: u64) {
    for _ in 0..1000 {
        if handle.state.lock().unwrap().durable.absorbed == absorbed {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the absorbed boundary never published {absorbed}");
}

/// Installs another segment's slice weighing over half of a 1 MiB cache, so
/// the cache admits no further fresh installs.
fn cross_admission_line(cache: &crate::postings_cache::PostingsCache) {
    let fat = (0..20_000u64)
        .map(|i| crate::postings::AbsRun {
            start: i * 2,
            count: 1,
            matching_bytes: 1,
            gap_bytes_before: 0,
        })
        .collect();
    cache.install_chunk(SegmentHash([33; 16]), 0, 40_000, vec![([34; 16], fat)]);
}

/// A warm bridge never crosses a chunk the slice cache declined to admit.
/// Record 0 (key "a") is absorbed and published. Another segment's slice then
/// lifts the cache over its admission line, so record 1's chunk (key "k",
/// no slice yet) installs nothing while the window stays clean. That advance
/// waits behind the held committer, and a durable keyed read at the
/// published boundary 1 cold-loads key "k" over `[0, 1)`. Once the boundary
/// publishes 2, a keyed read from 0 must deliver record 1.
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_warm_bridge_never_crosses_an_unadmitted_install; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_warm_bridge_never_crosses_an_unadmitted_install() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("admit-warm", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "admit-warm".into(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig {
            postings_cache_bytes: 1, // clamps to the 1 MiB floor
            ..Default::default()
        },
        tx,
        None,
        Default::default(),
    );
    let (hash, key) = ([32u8; 16], crate::crypto::StreamKey([8u8; 32]));
    let coverage = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        1,
        crate::dst::FaultPlan::new(0, 0, 0),
    )
    .coverage();
    let writer = crate::dst::Workload::new(coverage);
    for (rk, body) in [("a", "r0"), ("k", "r1")] {
        let out = writer
            .attempt_with_deadline(&engine, hash, &key, rk, body, None, None)
            .await;
        assert!(matches!(out, crate::dst::Outcome::Acked { .. }));
    }
    // A one-byte gather cap makes every record its own chunk.
    let absorber = Absorber::new(
        engine.clone(),
        AbsorberConfig {
            gather_max_bytes: 1,
            ..Default::default()
        },
    );
    let handle = engine.stream_handle(hash).await.unwrap();
    assert_eq!(
        ends(&absorber.absorb_gather_v2(&[hash]).await.unwrap()),
        [1]
    );
    until_published(&handle, 1).await;
    cross_admission_line(&engine.postings_cache);
    let commit = engine.test_hold_commit().await;
    assert_eq!(
        ends(&absorber.absorb_gather_v2(&[hash]).await.unwrap()),
        [2]
    );
    let (inc, kh) = (SegmentHash(hash), crate::postings::rk_hash("k"));
    assert_eq!(engine.postings_cache.debug_slice(&inc, &kh), None);
    let read = || {
        crate::http::read_merged(
            &key,
            &hash,
            &handle,
            &engine,
            0,
            Some("k"),
            1 << 20,
            crate::shard::Deliver::Durable,
        )
    };
    let offsets = |page: &crate::application::read::ReadPage| -> Vec<u64> {
        page.recs.iter().map(|rec| rec.off).collect()
    };
    assert_eq!(
        offsets(&read().await.unwrap()),
        [1],
        "the tail serves record 1"
    );
    assert_eq!(
        engine.postings_cache.debug_slice(&inc, &kh),
        Some((0, 1, 0)),
        "the stale read cold-loads [0, 1)"
    );
    drop(commit);
    until_published(&handle, 2).await;
    let page = read().await.unwrap();
    assert_eq!(
        offsets(&page),
        [1],
        "a keyed durable read skipped a durable record (completed={}); the key's slice is {:?}",
        page.completed,
        engine.postings_cache.debug_slice(&inc, &kh),
    );
    engine.begin_close();
    let _ = db.close().await;
}
