//! R09: dirty-index discovery pages progress without exceeding the pending capacity,
//! and its mark rollback cannot make a re-gather over-retire (TLA-016-F1) or
//! warm the slice cache over a head it never read (TLA-016-F3).
#![cfg(test)]
use super::*;
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
    let absorber = Absorber::new(
        store,
        engine.clone(),
        Arc::new(KeyCache::default()),
        AbsorberConfig::default(),
    );
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
/// `rk` decodes, no page overlaps another, and together they cover each
/// record exactly once.
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
            "a postings page overlaps or fails to decode"
        );
    }
    let covered: u64 = runs.iter().map(|run| u64::from(run.count)).sum();
    assert!(crate::postings::ValidatedRuns::new(runs).is_some());
    assert_eq!(
        covered, tail.next,
        "the postings must cover every record once"
    );
}

/// TLA-016-F1 (over-retirement): a rescan rolls the lane mark back while the
/// advance that raised it is still queued at the committer, and the re-gather
/// re-plans from the lagging published boundary over more than the queued
/// chunk. Applied behind that advance, it must not retire the queued chunk's
/// bytes twice: after both commits the tail gauge holds exactly the frame
/// bytes of `[absorbed, next)`, later passes still drain it to zero, and the
/// re-gather's postings replace the queued chunk's pages instead of
/// overlapping them, so a keyed read still admits the index.
#[expect(
    clippy::disallowed_methods,
    reason = "rolled_back_mark_regather_keeps_the_ledger_exact; the fixture's appends wait on the held dispatch while the test drives the gathers, and each is joined once dispatch is released; only concurrent requests can commit behind a lagging published end"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "rolled_back_mark_regather_keeps_the_ledger_exact; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolled_back_mark_regather_keeps_the_ledger_exact() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("f1-ledger", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "f1-ledger".into(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let hash = [29u8; 16];
    let coverage = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        1,
        crate::dst::FaultPlan::new(0, 0, 0),
    )
    .coverage();
    let append = |body: &'static str| {
        let (engine, w) = (engine.clone(), crate::dst::Workload::new(coverage.clone()));
        tokio::spawn(async move {
            let key = crate::crypto::StreamKey([7u8; 32]);
            let out = w
                .attempt_with_deadline(&engine, hash, &key, "k", body, None, None)
                .await;
            matches!(out, crate::dst::Outcome::Acked { .. })
        })
    };
    // Record 0 is published. Records 1 and 2 commit one at a time while
    // dispatch is held, so the published end lags at 1.
    assert!(append("r0").await.unwrap());
    let mut frames = vec![tail_until(&engine, hash, |_| true).await.unabsorbed_bytes];
    let dispatch = engine.test_hold_dispatch().await;
    let mut riders = Vec::new();
    for (next, body) in [(2, "r1"), (3, "r2")] {
        riders.push(append(body));
        let tail = tail_until(&engine, hash, |t| t.next == next).await;
        frames.push(tail.unabsorbed_bytes - frames.iter().sum::<u64>());
    }
    let owed = |tail: &crate::shard::TailFields| -> u64 {
        let skip = usize::try_from(tail.absorbed).unwrap();
        frames.iter().skip(skip).sum()
    };
    // Two frames per chunk, so the re-gather covers more than the queued one.
    let absorber = Absorber::new(
        store,
        engine.clone(),
        Arc::new(KeyCache::default()),
        AbsorberConfig {
            gather_max_bytes: usize::try_from(frames[0] + frames[1]).unwrap(),
            ..Default::default()
        },
    );
    // G1 plans [0, 1) from the lagging published end; its advance queues
    // behind the held committer and raises the lane mark to 1.
    let commit = engine.test_hold_commit().await;
    let g1 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!((g1.advanced.len(), g1.partial.len()), (1, 0));
    assert_eq!(
        absorber.submitted.lock().unwrap().get(&hash),
        Some(&(1, true))
    );
    drop(dispatch);
    for rider in riders {
        assert!(rider.await.unwrap());
    }
    // The rescan reads absorbed 0 under mark 1 and rolls the mark back.
    let mut pending = HashMap::new();
    absorber.seed_from_dirty_index(&mut pending).await.unwrap();
    assert!(!absorber.submitted.lock().unwrap().contains_key(&hash));
    // G2 re-plans [0, 2) from the published boundary and queues behind G1.
    let g2 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(g2.partial, vec![(hash, 1)]);
    drop(commit);
    // Both queued advances apply: G1 moves 0 -> 1, then G2 moves 1 -> 2.
    let mut tail = tail_until(&engine, hash, |t| t.absorbed == 2).await;
    for _ in 0..50 {
        assert_eq!(
            tail.unabsorbed_bytes,
            owed(&tail),
            "the tail gauge must hold exactly [absorbed={}, next={}) of frames {frames:?}",
            tail.absorbed,
            tail.next,
        );
        if tail.absorbed == tail.next {
            break;
        }
        absorber.seed_from_dirty_index(&mut pending).await.unwrap();
        absorber.absorb_gather_v2(&[hash]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    }
    assert_eq!(
        (tail.absorbed, tail.unabsorbed_bytes),
        (tail.next, 0),
        "later passes never drained the stream"
    );
    assert_postings_admit(&engine, hash, &tail, "k").await;
    engine.begin_close();
    let _ = db.close().await;
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
/// the held committer and the rescan rolls their lane mark back. Both
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
        store,
        engine.clone(),
        Arc::new(KeyCache::default()),
        AbsorberConfig {
            gather_max_bytes: 1,
            ..Default::default()
        },
    );
    let commit = engine.test_hold_commit().await;
    for upto in [1, 2] {
        let gather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
        let ends: Vec<u64> = gather.advanced.iter().map(|advance| advance.2).collect();
        assert_eq!(ends, [upto]);
    }
    // The rescan reads absorbed 0 under mark 2 and rolls the mark back.
    let mut pending = HashMap::new();
    absorber.seed_from_dirty_index(&mut pending).await.unwrap();
    assert!(!absorber.submitted.lock().unwrap().contains_key(&hash));
    let dispatch = engine.test_hold_dispatch().await;
    drop(commit);
    until_row0_trimmed_durably(&engine, hash, 2).await;
    let handle = engine.stream_handle(hash).await.unwrap();
    assert_eq!(handle.state.lock().unwrap().durable.absorbed, 0);
    engine.postings_cache.sweep_idle(Duration::ZERO);
    let regather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    let chunks: Vec<(u64, u64)> = regather
        .advanced
        .iter()
        .map(|advance| (advance.1, advance.2))
        .collect();
    assert_eq!(
        chunks,
        [(0, 2)],
        "the re-gather must plan from the stale boundary"
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
