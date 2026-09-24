//! R09: dirty-index discovery pages progress without exceeding the pending capacity,
//! and its mark rollback cannot make a re-gather over-retire (TLA-016-F1) or
//! warm the slice cache over a head it never read (TLA-016-F3); a warm bridge
//! never crosses a chunk the cache declined to admit. A refused group's lane
//! marks roll back to replay its chunks, keeping the ledger exact and the
//! postings pages disjoint, and a gather never waits on the committer. A
//! re-gather across flushed chunks, by a rescan or a new owner, stays readable.
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

/// TLA-016-F1 (over-retirement): a rescan rolls the lane mark back while the
/// advance that raised it is still queued at the committer, and the re-gather
/// re-plans from the lagging published boundary over more than the queued
/// chunk. Applied behind that advance, it must not retire the queued chunk's
/// bytes twice: after both commits the tail gauge holds exactly the frame
/// bytes of `[absorbed, next)`, later passes still drain it to zero, and the
/// re-gather's postings replace the queued chunk's pages instead of
/// overlapping them, so a keyed read still admits the index.
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
    let append = |body| commit_behind_held_dispatch(&engine, hash, body);
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
    assert_eq!(lane_mark(&absorber, hash).map(|mark| mark.from), Some(1));
    drop(dispatch);
    for rider in riders {
        assert!(rider.await.unwrap());
    }
    // The rescan reads absorbed 0 under mark 1 and rolls the mark back.
    let mut pending = HashMap::new();
    absorber.seed_from_dirty_index(&mut pending).await.unwrap();
    assert_eq!(lane_mark(&absorber, hash), None);
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
    assert_eq!(lane_mark(&absorber, hash), None);
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
        store,
        engine.clone(),
        Arc::new(KeyCache::default()),
        AbsorberConfig {
            gather_max_bytes: 1,
            ..Default::default()
        },
    );
    let handle = engine.stream_handle(hash).await.unwrap();
    let ends =
        |gather: &GatherOutcome| -> Vec<u64> { gather.advanced.iter().map(|a| a.2).collect() };
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

/// The absorber's lane mark for `hash`, if it holds one.
fn lane_mark(absorber: &Absorber, hash: [u8; 16]) -> Option<gather::LaneMark> {
    absorber.submitted.lock().unwrap().marks.get(&hash).copied()
}

/// A shard engine and an absorber whose gather copies at most `cap` bytes
/// per stream, over one in-memory store.
async fn refusal_rig(prefix: &str, cap: usize) -> (Arc<Db>, Arc<ShardEngine>, Absorber) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder(prefix, store.clone()).build().await.unwrap());
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        prefix.into(),
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
        AbsorberConfig {
            gather_max_bytes: cap,
            ..Default::default()
        },
    );
    (db, engine, absorber)
}

/// Appends one acknowledged record per body under routing key "k" and
/// returns each record's stored frame bytes.
async fn append_records(engine: &Arc<ShardEngine>, hash: [u8; 16], bodies: &[&str]) -> Vec<u64> {
    let coverage = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        1,
        crate::dst::FaultPlan::new(0, 0, 0),
    )
    .coverage();
    let writer = crate::dst::Workload::new(coverage);
    let key = crate::crypto::StreamKey([7u8; 32]);
    let mut frames = Vec::new();
    for body in bodies {
        let out = writer
            .attempt_with_deadline(engine, hash, &key, "k", body, None, None)
            .await;
        let crate::dst::Outcome::Acked { last_offset, .. } = out else {
            panic!("append {body} was not acknowledged: {out:?}");
        };
        let row = engine
            .db
            .get(crate::shard::record_key(&hash, last_offset))
            .await
            .unwrap();
        frames.push(row.unwrap().len() as u64);
    }
    frames
}

/// Waits until `refused` absorbed groups were refused, then until an append
/// on another stream is acknowledged: the committer runs groups in order, so
/// the refused group is finished and its receipt dropped.
async fn until_refused(engine: &Arc<ShardEngine>, refused: usize) {
    for _ in 0..1000 {
        if engine.group_failures_tripped() >= refused {
            append_records(engine, [99; 16], &["barrier"]).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the committer never refused group {refused}");
}

/// A gather that plans before the committer's answers are settled, as a
/// pump tick does when the committer lags behind the previous tick.
async fn gather_unsettled(absorber: &Absorber, hash: [u8; 16]) -> GatherOutcome {
    let budget = &absorber.shard.history_resources.budget;
    let mut reservation = budget.reserve(absorber.adaptive_gather_est()).await;
    absorber
        .absorb_gather_v2_with(&[hash], &mut reservation)
        .await
        .unwrap()
}

/// The (chunk start, new upto) of each advance a gather submitted.
fn chunks(gather: &GatherOutcome) -> Vec<(u64, u64)> {
    gather.advanced.iter().map(|a| (a.1, a.2)).collect()
}

/// Waits until the committed boundary reaches `absorbed`, then checks the
/// tail gauge holds exactly the frame bytes of `[absorbed, next)`.
async fn until_exact(
    engine: &ShardEngine,
    hash: [u8; 16],
    frames: &[u64],
    absorbed: u64,
) -> crate::shard::TailFields {
    let tail = tail_until(engine, hash, |t| t.absorbed == absorbed).await;
    let owed: u64 = frames.iter().skip(usize::try_from(absorbed).unwrap()).sum();
    assert_eq!(
        tail.unabsorbed_bytes, owed,
        "the tail gauge must hold exactly [absorbed={absorbed}, next={}) of {frames:?}",
        tail.next
    );
    tail
}

/// A refusal learned while a later chunk of the same stream is already in
/// flight leaves the mark on that later chunk: its advance recounts the
/// refused chunk too, and no recount spans more than the chunks in flight
/// when the refusal happened. Two chunks refused in one group roll the mark
/// back to the second, whose replay recounts both; a refused chunk whose
/// successor planned before the refusal was settled is recounted by that
/// successor. The ledger stays exact and every postings page stays disjoint.
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_refusal_behind_an_in_flight_chunk_recounts_only_the_chunks_in_flight; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_behind_an_in_flight_chunk_recounts_only_the_chunks_in_flight() {
    let (db, engine, absorber) = refusal_rig("lag-inflight", 1).await;
    let hash = [41u8; 16];
    let frames = append_records(&engine, hash, &["r0", "r1", "r2", "r3"]).await;
    // Chunks [0, 1) and [1, 2) queue behind the held committer and share
    // one refused group.
    engine.fail_next_absorbed_group();
    let commit = engine.test_hold_commit().await;
    for chunk in [(0, 1), (1, 2)] {
        let gather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
        assert_eq!(chunks(&gather), [chunk]);
    }
    drop(commit);
    until_refused(&engine, 1).await;
    // The mark rests on [1, 2)'s end: it rolls back to replay [1, 2), whose
    // advance at boundary 0 recounts both refused chunks.
    let replay = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&replay), [(1, 2)]);
    assert_eq!(replay.partial, [(hash, 2)]);
    until_exact(&engine, hash, &frames, 2).await;
    // [2, 3) is refused, and [3, 4) plans from its raised mark before the
    // refusal is settled: [3, 4)'s advance recounts [2, 4), and the settled
    // refusal finds the mark already past its chunk.
    engine.fail_next_absorbed_group();
    let refused = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&refused), [(2, 3)]);
    until_refused(&engine, 2).await;
    let successor = gather_unsettled(&absorber, hash).await;
    assert_eq!(chunks(&successor), [(3, 4)]);
    let tail = until_exact(&engine, hash, &frames, 4).await;
    let settled = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert!(settled.advanced.is_empty(), "nothing is left to replay");
    assert_eq!(tail.next, 4);
    assert_postings_admit(&engine, hash, &tail, "k").await;
    engine.begin_close();
    let _ = db.close().await;
}

/// A rolled-back mark replays exactly the refused chunk even after the
/// stream grew: the refused chunk [0, 1) stopped at the durable end, and the
/// replay stops there too instead of re-gathering [0, 3) over pages a chunk
/// above it may have flushed. The rolled-back stream is due again at once,
/// though its pending entry went with the refused gather's outcome, and it
/// stays pending past the replay.
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_rolled_back_mark_replays_exactly_the_refused_chunk; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rolled_back_mark_replays_exactly_the_refused_chunk() {
    let (db, engine, absorber) = refusal_rig("lag-replay", GATHER_PER_STREAM_CAP).await;
    let hash = [42u8; 16];
    let mut frames = append_records(&engine, hash, &["r0"]).await;
    engine.fail_next_absorbed_group();
    let refused = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&refused), [(0, 1)]);
    until_refused(&engine, 1).await;
    frames.extend(append_records(&engine, hash, &["r1", "r2"]).await);
    let mut pending = HashMap::new();
    absorber.settle_submissions(&mut pending);
    assert!(
        pending.contains_key(&hash),
        "the refused stream is due again"
    );
    assert_eq!(
        lane_mark(&absorber, hash),
        Some(gather::LaneMark {
            from: 0,
            v2: true,
            replay_to: Some(1)
        })
    );
    let replay = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        chunks(&replay),
        [(0, 1)],
        "the replay re-read past its chunk"
    );
    assert_eq!(replay.partial, [(hash, 2)]);
    until_exact(&engine, hash, &frames, 1).await;
    let rest = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&rest), [(1, 3)]);
    let tail = until_exact(&engine, hash, &frames, 3).await;
    assert_postings_admit(&engine, hash, &tail, "k").await;
    engine.begin_close();
    let _ = db.close().await;
}

/// Gathers never wait on the committer: with the committer held, four
/// gathers each submit and return, and settling finds nothing answered.
/// Once released, the four batches share one refused group; the mark rolls
/// back to replay the last, whose advance recounts the four chunks that were
/// in flight, and absorption then completes with an exact ledger.
#[expect(
    clippy::let_underscore_must_use,
    reason = "absorber_gathers_never_wait_on_a_stalled_committer; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absorber_gathers_never_wait_on_a_stalled_committer() {
    let (db, engine, absorber) = refusal_rig("lag-stalled", 1).await;
    let hash = [43u8; 16];
    let frames = append_records(&engine, hash, &["r0", "r1", "r2", "r3", "r4", "r5"]).await;
    engine.fail_next_absorbed_group();
    let commit = engine.test_hold_commit().await;
    for upto in 1..=4 {
        let gather =
            tokio::time::timeout(Duration::from_secs(10), absorber.absorb_gather_v2(&[hash]))
                .await
                .expect("a gather waited on the stalled committer")
                .unwrap();
        assert_eq!(chunks(&gather), [(upto - 1, upto)]);
    }
    let mut pending = HashMap::new();
    absorber.settle_submissions(&mut pending);
    assert!(pending.is_empty(), "no receipt is answered while held");
    drop(commit);
    until_refused(&engine, 1).await;
    let replay = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&replay), [(3, 4)]);
    until_exact(&engine, hash, &frames, 4).await;
    for upto in [5, 6] {
        let gather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
        assert_eq!(chunks(&gather), [(upto - 1, upto)]);
        until_exact(&engine, hash, &frames, upto).await;
    }
    let tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!((tail.absorbed, tail.next, tail.unabsorbed_bytes), (6, 6, 0));
    assert_postings_admit(&engine, hash, &tail, "k").await;
    engine.begin_close();
    let _ = db.close().await;
}

/// Appends one record under "k" in a task of its own, so it can commit while
/// dispatch is held and the published end lags; resolves to whether it was
/// acknowledged.
#[expect(
    clippy::disallowed_methods,
    reason = "commit_behind_held_dispatch; each append waits on the held dispatch while its test drives the gathers, and is joined once dispatch is released; only concurrent requests can commit behind a lagging published end"
)]
fn commit_behind_held_dispatch(
    engine: &Arc<ShardEngine>,
    hash: [u8; 16],
    body: &'static str,
) -> tokio::task::JoinHandle<bool> {
    let coverage = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        1,
        crate::dst::FaultPlan::new(0, 0, 0),
    )
    .coverage();
    let (engine, writer) = (engine.clone(), crate::dst::Workload::new(coverage));
    tokio::spawn(async move {
        let key = crate::crypto::StreamKey([7u8; 32]);
        let out = writer
            .attempt_with_deadline(&engine, hash, &key, "k", body, None, None)
            .await;
        matches!(out, crate::dst::Outcome::Acked { .. })
    })
}

/// Flushes [0, 1) and then [1, 2) with both advances queued behind the held
/// committer: [0, 1) stops at the published end, which then moves to 2.
/// Returns the stored frame bytes and the held committer.
async fn two_chunks_in_flight<'a>(
    engine: &'a Arc<ShardEngine>,
    absorber: &Absorber,
    hash: [u8; 16],
) -> (Vec<u64>, tokio::sync::MutexGuard<'a, ()>) {
    let mut frames = append_records(engine, hash, &["r0"]).await;
    let dispatch = engine.test_hold_dispatch().await;
    let rider = commit_behind_held_dispatch(engine, hash, "r1");
    let committed = tail_until(engine, hash, |t| t.next == 2).await;
    frames.push(committed.unabsorbed_bytes - frames[0]);
    let commit = engine.test_hold_commit().await;
    let first = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&first), [(0, 1)]);
    drop(dispatch);
    assert!(rider.await.unwrap());
    let second = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&second), [(1, 2)]);
    (frames, commit)
}

/// Once [0, 2) is absorbed, the key's pages admit and a cold keyed history
/// read serves both records from the index: the cold load publishes a slice
/// only when every page it scanned admitted, and a refused index is served
/// from the POSTINGS_CORRUPT envelope instead.
async fn assert_regathered_index_admits(engine: &Arc<ShardEngine>, hash: [u8; 16], frames: &[u64]) {
    let tail = until_exact(engine, hash, frames, 2).await;
    assert_postings_admit(engine, hash, &tail, "k").await;
    engine.postings_cache.sweep_idle(Duration::ZERO);
    let part = engine.history_partition().await.unwrap();
    let (inc, route) = (
        crate::crypto::SegmentHash(hash),
        crate::crypto::RouteHash(tail.route),
    );
    let (read, _, complete) = crate::history::read_history2_keyed_cached(
        &engine.postings_cache,
        &part,
        route,
        inc,
        "k",
        0,
        2,
        2,
        1 << 20,
    )
    .await
    .unwrap();
    assert_eq!((read.len(), complete), (2, true));
    let slice = engine
        .postings_cache
        .debug_slice(&inc, &crate::postings::rk_hash("k"));
    assert!(slice.is_some(), "the cold keyed read refused the index");
}

/// A rescan finds the durable boundary behind a lane mark while the chunks
/// that raised it, [0, 1) and [1, 2), are still in flight, and the stream is
/// re-gathered as [0, 2): its page from 0 overlaps [1, 2)'s flushed page.
/// Both describe the same records, so the key's index still admits and the
/// ledger stays exact.
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_rescan_regather_across_a_chunk_in_flight_keeps_the_index_readable; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rescan_regather_across_a_chunk_in_flight_keeps_the_index_readable() {
    let (db, engine, absorber) = refusal_rig("overlap-rescan", GATHER_PER_STREAM_CAP).await;
    let hash = [44u8; 16];
    let (frames, commit) = two_chunks_in_flight(&engine, &absorber, hash).await;
    absorber
        .seed_from_dirty_index(&mut HashMap::new())
        .await
        .unwrap();
    let regather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&regather), [(0, 2)]);
    drop(commit);
    assert_regathered_index_admits(&engine, hash, &frames).await;
    engine.begin_close();
    let _ = db.close().await;
}

/// Opens an engine and an absorber over `store` as the shard opener does,
/// with the maintenance row loaded before the engine serves.
async fn open_owner(store: Arc<dyn ObjectStore>) -> (Arc<Db>, Arc<ShardEngine>, Absorber) {
    let db = Arc::new(
        Db::builder("overlap-owner", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "overlap-owner".into(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig::default(),
        tx,
        None,
        maintenance,
    );
    let absorber = Absorber::new(
        store,
        engine.clone(),
        Arc::new(KeyCache::default()),
        AbsorberConfig::default(),
    );
    (db, engine, absorber)
}

/// The owner closes with [0, 1) and [1, 2) flushed and both advances still
/// queued. The next owner has no lane marks and re-gathers [0, 2) over the
/// inherited pages; the key's index still admits and the ledger stays exact.
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable; the fixture closes each owner's databases best effort; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let hash = [45u8; 16];
    let (db, engine, absorber) = open_owner(store.clone()).await;
    let (frames, commit) = two_chunks_in_flight(&engine, &absorber, hash).await;
    engine.begin_close();
    if let Some(part) = engine.history_partition_if_open() {
        let _ = part.close().await;
    }
    let _ = db.close().await;
    drop(commit);
    let (db, engine, absorber) = open_owner(store).await;
    let regather = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(chunks(&regather), [(0, 2)]);
    assert_regathered_index_admits(&engine, hash, &frames).await;
    engine.begin_close();
    let _ = db.close().await;
}
