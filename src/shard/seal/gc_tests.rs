#![cfg(test)]
//! Physical reclamation (layout 6, DESIGN-6A §2.5, package 6), in process.
//! After every step the refcount audit recomputes each segment object's
//! unreclaimed chunks from the index (directory and run-index objects) and
//! compares them with the `'R'` rows, exactly; every stored object is named
//! by a row unless the test made it an orphan.
//! - a purge decrements exactly the dead stream's chunks, its objects are
//!   deleted, the other streams read back exactly, and a later load of the
//!   stream fails; purging every stream leaves no segment object;
//! - a crash between an object's PUT and its commit (seal, fold,
//!   consolidation) leaves an orphan the sweep deletes, never an object in
//!   flight; a fenced writer's object is swept by the new owner;
//! - a `'G'` row outlives a restart and its object is deleted only after
//!   the grace, counted from the restart;
//! - a sealed read that planned with a chunk keeps its object stored while
//!   it runs, and reads it exactly;
//! - a consolidation offered before a purge and run after it supersedes
//!   nothing (no second decrement);
//! - a purge waits for the stream's usage to reach the ledger, refuses a
//!   group that also writes the stream, and an append after it fails;
//! - the physical size (G10) is exact through consolidation and purge.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::ObjectStoreExt;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::Trigger;
use super::gc::purge::{PurgeOutcome, PurgeStep};
use super::test_hooks::{Hold, SealStep};
use super::tests::{
    Oracle, Rig, append_recorded, directory_rows, hashes, manual, open, random_bytes, read_back,
    reopen, rig_with, settle,
};
use crate::crypto::{StreamKey, derive_subkey};
use crate::shard::{CommitOp, Deliver, ShardEngine};
use crate::spill::directory::DirEntry;
use crate::spill::gc::{self, HeadState};
use crate::spill::object::ObjectId;

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// A rig with no ring (sealed reads go to the objects), small chunks, and
/// the fold, merges and collector driven by the test, at grace zero.
async fn gc_rig(label: &str) -> Rig {
    let mut settings = manual();
    settings.max_pages_per_chunk = 4;
    let rig = rig_with(label, settings, 0, Duration::from_millis(2)).await;
    quiet(&rig.engine);
    rig
}

fn quiet(engine: &Arc<ShardEngine>) {
    engine.layout.seal.fold.configure(|s| {
        s.auto = false;
        s.fan_in = [2, 2];
    });
    engine.gc_state().configure(|s| {
        s.auto = false;
        s.grace = Duration::ZERO;
        s.orphan_age = Duration::ZERO;
    });
}

/// The store's objects under `dir` (`hseg` or `ridx`), by id, with sizes.
async fn stored(rig: &Rig, dir: &str) -> BTreeMap<ObjectId, u64> {
    let prefix = object_store::path::Path::from(format!("{}/{dir}", rig.path));
    let mut out = BTreeMap::new();
    let mut list = rig.store.list(Some(&prefix));
    while let Some(meta) = list.next().await {
        let meta = meta.unwrap();
        let parts: Vec<String> = meta
            .location
            .parts()
            .map(|p| p.as_ref().to_string())
            .collect();
        let n = parts.len();
        let id = ObjectId {
            epoch: u64::from_str_radix(&parts[n - 2], 16).unwrap(),
            seq: u64::from_str_radix(&parts[n - 1], 16).unwrap(),
        };
        out.insert(id, meta.size);
    }
    out
}

/// What the audit found.
#[derive(Debug, Default)]
struct Audit {
    live_objects: usize,
    live_chunks: u64,
    garbage: usize,
}

/// The refcount audit: every `'R'` row equals the object's unreclaimed
/// chunks the index holds (count and bytes), every object with such a
/// chunk has a row, and every segment and run-index object in the store is
/// named by a row (`'R'`/`'X'`/`'G'`) unless listed in `orphans`.
async fn audit(rig: &Rig, orphans: &BTreeSet<ObjectId>, what: &str) -> Audit {
    settle(&rig.engine).await;
    let engine = &rig.engine;
    let mut streams: BTreeSet<[u8; 16]> = directory_rows(rig).await.into_keys().collect();
    for p in engine.layout.seal.fold.set().values() {
        for f in engine.fetch_footer(p).await.unwrap().iter() {
            streams.insert(f.hash);
        }
    }
    let mut expected: BTreeMap<ObjectId, (u64, u64)> = BTreeMap::new();
    for hash in &streams {
        let reclaimed = engine
            .head_row(hash)
            .await
            .unwrap()
            .map_or(0, |h| h.reclaimed);
        for e in engine.live_chunks(*hash).await.unwrap() {
            if e.first >= reclaimed {
                let slot = expected.entry(e.object).or_default();
                slot.0 += u64::from(e.chunk_len);
                slot.1 += 1;
            }
        }
    }
    let rows: BTreeMap<ObjectId, (u64, u64)> = engine
        .refcount_rows()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, r)| {
            let r = r.unwrap_or_else(|| panic!("{what}: an 'R' row of {id:?} does not decode"));
            (id, (r.live_bytes, r.live_chunks))
        })
        .collect();
    assert_eq!(
        rows, expected,
        "{what}: the 'R' rows are the index's unreclaimed chunks"
    );
    let garbage: BTreeSet<ObjectId> = engine
        .garbage_rows()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let pointers: BTreeSet<ObjectId> = engine.layout.seal.fold.set().keys().copied().collect();
    for (dir, named) in [
        ("hseg", &rows.keys().copied().collect::<BTreeSet<_>>()),
        ("ridx", &pointers),
    ] {
        for id in stored(rig, dir).await.into_keys() {
            assert!(
                named.contains(&id) || garbage.contains(&id) || orphans.contains(&id),
                "{what}: {dir} object {id:?} is named by no row"
            );
        }
    }
    for id in rows.keys() {
        assert!(
            stored(rig, "hseg").await.contains_key(id),
            "{what}: object {id:?} has a row and is not stored"
        );
    }
    Audit {
        live_objects: rows.len(),
        live_chunks: rows.values().map(|(_, n)| n).sum(),
        garbage: garbage.len(),
    }
}

/// `rounds` rounds of appends to each of `streams`, each round sealed.
async fn rounds(
    rig: &Rig,
    rng: &mut StdRng,
    streams: &[[u8; 16]],
    oracle: &Mutex<Oracle>,
    n: usize,
) {
    for _ in 0..n {
        for hash in streams {
            let records = (0..rng.random_range(1..=2))
                .map(|_| {
                    let len = rng.random_range(16..900);
                    random_bytes(rng, len)
                })
                .collect();
            append_recorded(&rig.engine, oracle, *hash, records).await;
        }
        rig.engine
            .seal_once(Trigger::Requested)
            .await
            .expect("seal");
    }
}

async fn read_all(rig: &Rig, oracle: &Oracle, skip: &BTreeSet<[u8; 16]>, what: &str) {
    for (hash, o) in oracle.iter().filter(|(h, _)| !skip.contains(*h)) {
        read_back(&rig.engine, *hash, o, Deliver::Durable)
            .await
            .unwrap_or_else(|e| panic!("{what}: {e}"));
    }
}

async fn purge_now(engine: &Arc<ShardEngine>, hash: [u8; 16]) -> u64 {
    engine.request_purge(hash, 0).await.unwrap();
    match engine.purge_stream(hash).await.unwrap() {
        PurgeOutcome::Reclaimed { chunks } => chunks,
        other => panic!("the purge did not run: {other:?}"),
    }
}

async fn wait_consolidations(engine: &Arc<ShardEngine>, n: u64) {
    let c = &engine.layout.seal.fold.consolidator;
    let t0 = Instant::now();
    while get(&c.done) + get(&c.skipped) + get(&c.failed) < n {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the consolidator never finished"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// One append that may fail (a purged stream's).
async fn try_append(engine: &Arc<ShardEngine>, hash: [u8; 16]) -> Result<u64, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: Instant::now(),
        hash,
        route: hash,
        entries: vec![Bytes::from_static(b"late")],
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: crate::crypto::RoutingKeyHash::of("").0,
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: derive_subkey(&StreamKey([7; 32]), &hash, "", 0),
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
    engine.try_enqueue(req).map_err(|e| format!("{e:?}"))?;
    match rx.await {
        Ok(Ok(ok)) => Ok(ok.last_offset),
        Ok(Err(e)) => Err(format!("{e:?}")),
        Err(e) => Err(e.to_string()),
    }
}

/// The stream's chunks as one consolidation job would carry them: each
/// whole chunk's bytes from the store.
async fn whole_chunks(rig: &Rig, hash: [u8; 16]) -> Vec<(DirEntry, Bytes)> {
    let mut out = Vec::new();
    for e in rig.engine.live_chunks(hash).await.unwrap() {
        let path = crate::spill::object::object_path(&rig.path, e.object);
        let body = rig
            .store
            .get_range(
                &object_store::path::Path::from(path),
                e.chunk_off..e.chunk_off + u64::from(e.chunk_len),
            )
            .await
            .unwrap();
        out.push((e, body));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_reclaims_exactly_its_streams_chunks_and_their_objects() {
    let rig = gc_rig("gc-purge").await;
    let mut rng = StdRng::seed_from_u64(0x6C01);
    let streams = hashes(&mut rng, 4);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 3).await;
    rig.engine.fold_once().await.unwrap();
    rounds(&rig, &mut rng, &streams[..1], &oracle, 10).await;
    rounds(&rig, &mut rng, &streams, &oracle, 3).await;
    let none = BTreeSet::new();
    let a = audit(&rig, &none, "after the seals").await;
    assert!(a.live_chunks > 16, "{a:?}");
    // A cold walk of stream 0 consolidates its ten one-stream seals.
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &BTreeSet::new(), "the first walk").await;
    wait_consolidations(&rig.engine, 1).await;
    assert_eq!(get(&rig.engine.layout.seal.fold.consolidator.done), 1);
    let a = audit(&rig, &none, "after the consolidation").await;
    assert!(
        a.garbage >= 8,
        "the one-chunk objects it superseded wait for their delete: {a:?}"
    );
    assert!(rig.engine.collect().await.unwrap() >= 8);
    audit(&rig, &none, "after their deletes").await;
    read_all(&rig, &oracle, &BTreeSet::new(), "after the deletes").await;
    // Purge stream 1: exactly its chunks are decremented.
    let victim = streams[1];
    let chunks = rig.engine.live_chunks(victim).await.unwrap().len() as u64;
    assert_eq!(purge_now(&rig.engine, victim).await, chunks);
    let head = rig.engine.head_row(&victim).await.unwrap().unwrap();
    assert_eq!((head.state, head.reclaimed), (HeadState::Purged, head.head));
    assert!(head.head > 0);
    audit(&rig, &none, "after the purge").await;
    assert_eq!(
        rig.engine.physical_bytes(victim).await.unwrap(),
        0,
        "a purged stream has no physical size"
    );
    rig.engine.collect().await.unwrap();
    audit(&rig, &none, "after the purge's deletes").await;
    let gone = BTreeSet::from([victim]);
    read_all(&rig, &oracle, &gone, "the other streams").await;
    assert!(
        rig.engine.stream_handle(victim).await.is_err(),
        "a purged stream does not load"
    );
    assert!(
        try_append(&rig.engine, victim).await.is_err(),
        "nor takes an append"
    );
    // Every stream purged: no segment object is left, and the fold and
    // merges drop every entry.
    for hash in &streams[..] {
        if *hash != victim {
            purge_now(&rig.engine, *hash).await;
        }
    }
    rig.engine.collect().await.unwrap();
    assert_eq!(
        stored(&rig, "hseg").await.len(),
        0,
        "no segment object is left"
    );
    let report = rig.engine.fold_once().await.unwrap();
    assert_eq!(
        report.object, None,
        "a fold of dead entries writes no object"
    );
    while let Some(r) = rig.engine.merge_once().await {
        r.expect("merge");
    }
    rig.engine.collect().await.unwrap();
    let a = audit(&rig, &none, "everything purged").await;
    assert_eq!((a.live_objects, a.live_chunks, a.garbage), (0, 0, 0));
    assert!(
        stored(&rig, "ridx").await.len() <= 1,
        "at most one object the merges could not pair"
    );
    let rig = reopen(rig).await;
    quiet(&rig.engine);
    audit(&rig, &none, "after a restart").await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn objects_put_without_their_commit_are_swept_and_objects_in_flight_are_not() {
    let rig = gc_rig("gc-orphans").await;
    let mut rng = StdRng::seed_from_u64(0x6C02);
    let streams = hashes(&mut rng, 3);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 2).await;
    // A seal stopped after its PUT.
    *rig.engine.layout.seal.hooks.abort_at.lock().unwrap() = Some(SealStep::Put);
    rounds_failing(&rig, &mut rng, &streams, &oracle).await;
    *rig.engine.layout.seal.hooks.abort_at.lock().unwrap() = None;
    rounds(&rig, &mut rng, &streams, &oracle, 1).await;
    // A fold stopped after its read-back.
    *rig.engine.layout.seal.fold.hooks.abort_at.lock().unwrap() =
        Some(super::fold::test_hooks::FoldStep::Verified);
    assert!(rig.engine.fold_once().await.is_err());
    *rig.engine.layout.seal.fold.hooks.abort_at.lock().unwrap() = None;
    let orphans = unnamed(&rig, &["hseg", "ridx"]).await;
    assert_eq!(orphans.len(), 2, "one seal's and one fold's object");
    audit(&rig, &orphans, "with the orphans").await;
    // A seal parked between its PUT and its commit is in flight: the sweep
    // leaves its object alone.
    let hold = Hold::new();
    *rig.engine.layout.seal.hooks.before_commit.lock().unwrap() = Some(hold.clone());
    append_recorded(
        &rig.engine,
        &oracle,
        streams[0],
        vec![random_bytes(&mut rng, 300)],
    )
    .await;
    let sweep = async {
        while hold.entered.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let report = rig.engine.sweep_orphans().await.unwrap();
        hold.release();
        report
    };
    let (parked, report) = tokio::join!(rig.engine.seal_once(Trigger::Requested), sweep);
    assert_eq!(
        report.orphans, 2,
        "the stopped seal's and fold's objects: {report:?}"
    );
    *rig.engine.layout.seal.hooks.before_commit.lock().unwrap() = None;
    parked.expect("the parked seal commits");
    let none = BTreeSet::new();
    audit(&rig, &none, "after the sweep").await;
    // A consolidation whose batch fails: its object is swept too, and it
    // decremented nothing.
    rounds(&rig, &mut rng, &streams[..1], &oracle, 8).await;
    let job = whole_chunks(&rig, streams[0]).await;
    assert!(job.len() >= 8);
    rig.engine
        .layout
        .seal
        .fold
        .hooks
        .fail_next_batch
        .store(true, Ordering::Relaxed);
    rig.engine.offer_consolidation(streams[0], job);
    wait_consolidations(&rig.engine, 1).await;
    assert_eq!(get(&rig.engine.layout.seal.fold.consolidator.failed), 1);
    let orphans = unnamed(&rig, &["hseg"]).await;
    assert_eq!(orphans.len(), 1, "the consolidation's object");
    audit(&rig, &orphans, "after the failed consolidation").await;
    assert_eq!(rig.engine.sweep_orphans().await.unwrap().orphans, 1);
    audit(&rig, &none, "after the second sweep").await;
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &BTreeSet::new(), "after the sweeps").await;
    rig.engine.begin_close();
}

/// Whether a row names `id` (`'R'`, `'X'` or `'G'`).
async fn named(rig: &Rig, id: ObjectId) -> bool {
    for key in [
        gc::refcount_key(id),
        crate::spill::runindex::pointer_key(id),
        gc::garbage_key(id),
    ] {
        if rig.engine.db.get(&key).await.unwrap().is_some() {
            return true;
        }
    }
    false
}

/// The stored objects under `dirs` no row names.
async fn unnamed(rig: &Rig, dirs: &[&str]) -> BTreeSet<ObjectId> {
    let mut out = BTreeSet::new();
    for dir in dirs {
        for id in stored(rig, dir).await.into_keys() {
            if !named(rig, id).await {
                out.insert(id);
            }
        }
    }
    out
}

/// One round of appends whose seal fails (an abort hook is set).
async fn rounds_failing(rig: &Rig, rng: &mut StdRng, streams: &[[u8; 16]], oracle: &Mutex<Oracle>) {
    for hash in streams {
        append_recorded(&rig.engine, oracle, *hash, vec![random_bytes(rng, 200)]).await;
    }
    assert!(rig.engine.seal_once(Trigger::Requested).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fenced_writers_object_is_swept_by_the_new_owner() {
    let rig = gc_rig("gc-fence").await;
    let mut rng = StdRng::seed_from_u64(0x6C03);
    let streams = hashes(&mut rng, 2);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 2).await;
    for hash in &streams {
        append_recorded(
            &rig.engine,
            &oracle,
            *hash,
            vec![random_bytes(&mut rng, 400)],
        )
        .await;
    }
    let hold = Hold::new();
    *rig.engine.layout.seal.hooks.before_commit.lock().unwrap() = Some(hold.clone());
    let fence = async {
        while hold.entered.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let fresh = open(
            rig.store.clone(),
            &rig.prefix,
            rig.settings,
            0,
            Duration::from_millis(2),
        )
        .await;
        quiet(&fresh);
        hold.release();
        fresh
    };
    let (result, fresh) = tokio::join!(rig.engine.seal_once(Trigger::Requested), fence);
    assert!(
        result.is_err(),
        "the fenced seal does not commit: {result:?}"
    );
    let old = rig.engine.writer_epoch;
    let rig = Rig {
        engine: fresh,
        ..rig
    };
    let theirs: Vec<ObjectId> = unnamed(&rig, &["hseg"])
        .await
        .into_iter()
        .filter(|id| id.epoch == old)
        .collect();
    assert_eq!(theirs.len(), 1, "the fenced seal's object");
    let report = rig.engine.sweep_orphans().await.unwrap();
    assert_eq!(report.orphans, 1, "{report:?}");
    let none = BTreeSet::new();
    audit(&rig, &none, "the new owner").await;
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &BTreeSet::new(), "the new owner").await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn garbage_outlives_a_restart_and_waits_a_grace_from_it() {
    let rig = gc_rig("gc-restart").await;
    let mut rng = StdRng::seed_from_u64(0x6C04);
    let streams = hashes(&mut rng, 2);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 4).await;
    for _ in 0..4 {
        rig.engine.fold_once().await.unwrap();
        rounds(&rig, &mut rng, &streams, &oracle, 1).await;
    }
    while let Some(r) = rig.engine.merge_once().await {
        r.expect("merge");
    }
    purge_now(&rig.engine, streams[0]).await;
    let none = BTreeSet::new();
    let before = audit(&rig, &none, "before the restart").await;
    assert!(
        before.garbage > 0,
        "merged-away and zeroed objects wait: {before:?}"
    );
    let rig = reopen(rig).await;
    quiet(&rig.engine);
    rig.engine
        .gc_state()
        .configure(|s| s.grace = Duration::from_secs(600));
    rig.engine.gc_state().configure(|s| s.auto = false);
    let after = audit(&rig, &none, "after the restart").await;
    assert_eq!(after.garbage, before.garbage, "every 'G' row is durable");
    assert_eq!(
        rig.engine.collect().await.unwrap(),
        0,
        "a grace from the restart holds them"
    );
    rig.engine
        .gc_state()
        .configure(|s| s.grace = Duration::ZERO);
    assert_eq!(rig.engine.collect().await.unwrap(), before.garbage as u64);
    let a = audit(&rig, &none, "after the deletes").await;
    assert_eq!(a.garbage, 0);
    let oracle = oracle.into_inner().unwrap();
    read_all(
        &rig,
        &oracle,
        &BTreeSet::from([streams[0]]),
        "after the deletes",
    )
    .await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_that_planned_with_a_chunk_keeps_its_object_until_it_ends() {
    let rig = gc_rig("gc-pin").await;
    let hash = [0x41; 16];
    let oracle = Mutex::new(Oracle::new());
    let mut rng = StdRng::seed_from_u64(0x6C05);
    rounds(&rig, &mut rng, &[hash], &oracle, 10).await;
    rig.engine.fold_once().await.unwrap();
    let oracle = oracle.into_inner().unwrap();
    let job = whole_chunks(&rig, hash).await;
    assert_eq!(job.len(), 10);
    // A read parks at its first GET, having planned with the originals.
    let hold = Hold::new();
    *rig.engine.layout.seal.hooks.get_hold.lock().unwrap() = Some(hold.clone());
    let o = oracle[&hash].clone();
    let consolidate = async {
        while hold.entered.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // The consolidation supersedes every original: each one-chunk
        // object reaches zero.
        rig.engine.offer_consolidation(hash, job);
        wait_consolidations(&rig.engine, 1).await;
        assert_eq!(get(&rig.engine.layout.seal.fold.consolidator.done), 1);
        let a = audit(&rig, &BTreeSet::new(), "after the consolidation").await;
        assert_eq!(a.garbage, 10);
        assert_eq!(
            rig.engine.collect().await.unwrap(),
            0,
            "the running read pins every one"
        );
        assert!(get(&rig.engine.gc_state().counters.pinned) >= 10);
        *rig.engine.layout.seal.hooks.get_hold.lock().unwrap() = None;
        hold.release();
    };
    let (read, ()) = tokio::join!(
        read_back(&rig.engine, hash, &o, Deliver::Durable),
        consolidate
    );
    read.expect("the pinned read reads its chunks exactly");
    let none = BTreeSet::new();
    assert_eq!(
        rig.engine.collect().await.unwrap(),
        10,
        "deleted once it ended"
    );
    audit(&rig, &none, "after the deletes").await;
    read_all(
        &rig,
        &oracle,
        &BTreeSet::new(),
        "from the consolidated chunk",
    )
    .await;
    // The consolidated chunk folded into a later run-index object than the
    // originals' (whose objects are gone): a read takes both runs and
    // picks the consolidated chunk.
    // A restart: no chunk cache holds the originals any more.
    rig.engine.fold_once().await.unwrap();
    let rig = reopen(rig).await;
    quiet(&rig.engine);
    assert_eq!(rig.engine.layout.seal.fold.set().len(), 2);
    assert_eq!(
        rig.engine.layout.seal.directory.read().unwrap().entries(),
        0
    );
    read_all(
        &rig,
        &oracle,
        &BTreeSet::new(),
        "through two run-index objects",
    )
    .await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_consolidation_offered_before_a_purge_supersedes_nothing_after_it() {
    let rig = gc_rig("gc-race").await;
    let mut rng = StdRng::seed_from_u64(0x6C06);
    let streams = hashes(&mut rng, 2);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 10).await;
    let job = whole_chunks(&rig, streams[0]).await;
    purge_now(&rig.engine, streams[0]).await;
    rig.engine.offer_consolidation(streams[0], job);
    wait_consolidations(&rig.engine, 1).await;
    let c = &rig.engine.layout.seal.fold.consolidator;
    assert_eq!(
        (get(&c.done), get(&c.skipped)),
        (0, 1),
        "a dead stream is not consolidated"
    );
    assert_eq!(get(&rig.engine.gc_state().counters.inexact), 0);
    let none = BTreeSet::new();
    audit(&rig, &none, "after the refused job").await;
    rig.engine.collect().await.unwrap();
    audit(&rig, &none, "after the deletes").await;
    let oracle = oracle.into_inner().unwrap();
    read_all(
        &rig,
        &oracle,
        &BTreeSet::from([streams[0]]),
        "the live stream",
    )
    .await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_waits_for_the_ledger_and_for_its_own_group() {
    let rig = gc_rig("gc-refuse").await;
    let mut rng = StdRng::seed_from_u64(0x6C07);
    let streams = hashes(&mut rng, 2);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 2).await;
    let hash = streams[0];
    rig.engine.request_purge(hash, 0).await.unwrap();
    rig.engine.layout.mark_usage(hash, 7);
    assert_eq!(
        rig.engine.purge_stream(hash).await.unwrap(),
        PurgeOutcome::Refused
    );
    rig.engine.layout.clear_usage(hash, None);
    // A group that appends to the stream and purges it: the purge refuses.
    let (resp, refusal) = tokio::sync::oneshot::channel();
    let prior = rig.engine.head_row(&hash).await.unwrap().unwrap();
    let (tx, answer) = tokio::sync::oneshot::channel();
    let append = crate::shard::AppendReq {
        resp: tx,
        ..late_append(hash)
    };
    rig.engine
        .commit_group(vec![
            CommitOp::Append(append),
            CommitOp::Seal(Box::new(super::SealCommit::Purge(PurgeStep {
                hash,
                prior,
                resp,
            }))),
        ])
        .await;
    assert!(answer.await.unwrap().is_ok(), "the append commits");
    let why = refusal.await.expect("a refused purge is answered");
    assert!(why.contains("another op in this group"), "{why}");
    // Then it purges, and an append staged after it in one group fails.
    let (resp, refusal) = tokio::sync::oneshot::channel();
    let (tx, answer) = tokio::sync::oneshot::channel();
    let append = crate::shard::AppendReq {
        resp: tx,
        ..late_append(hash)
    };
    rig.engine
        .commit_group(vec![
            CommitOp::Seal(Box::new(super::SealCommit::Purge(PurgeStep {
                hash,
                prior,
                resp,
            }))),
            CommitOp::Append(append),
        ])
        .await;
    assert!(
        refusal.await.is_err(),
        "the purge is accepted (its reply dropped with the group)"
    );
    assert!(answer.await.unwrap().is_err(), "the append after it fails");
    settle(&rig.engine).await;
    assert_eq!(
        rig.engine.head_row(&hash).await.unwrap().map(|h| h.state),
        Some(HeadState::Purged)
    );
    assert!(matches!(
        rig.engine.purge_stream(hash).await.unwrap(),
        PurgeOutcome::Reclaimed { .. }
    ));
    let none = BTreeSet::new();
    audit(&rig, &none, "after the purge").await;
    rig.engine.begin_close();
}

fn late_append(hash: [u8; 16]) -> crate::shard::AppendReq {
    let (resp, _) = tokio::sync::oneshot::channel();
    crate::shard::AppendReq {
        enqueued_at: Instant::now(),
        hash,
        route: hash,
        entries: vec![Bytes::from_static(b"late")],
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: crate::crypto::RoutingKeyHash::of("").0,
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: derive_subkey(&StreamKey([7; 32]), &hash, "", 0),
        ts_hint_ms: None,
        seq: None,
        bytes: 4,
        finish: crate::shard::AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_physical_size_is_exact_through_consolidation_and_purge() {
    let rig = gc_rig("gc-g10").await;
    let hash = [0x52; 16];
    let oracle = Mutex::new(Oracle::new());
    let mut rng = StdRng::seed_from_u64(0x6C08);
    rounds(&rig, &mut rng, &[hash], &oracle, 9).await;
    append_recorded(
        &rig.engine,
        &oracle,
        hash,
        vec![random_bytes(&mut rng, 333)],
    )
    .await;
    settle(&rig.engine).await;
    let chunks = |v: &[DirEntry]| v.iter().map(|e| u64::from(e.chunk_len)).sum::<u64>();
    let sealed = chunks(&rig.engine.live_chunks(hash).await.unwrap());
    let unsealed = {
        let h = rig.engine.stream_handle(hash).await.unwrap();
        let st = h.state.lock().unwrap();
        st.applied.unabsorbed_bytes
    };
    assert!(unsealed > 0);
    assert_eq!(
        rig.engine.physical_bytes(hash).await.unwrap(),
        sealed + unsealed
    );
    let refs: u64 = rig
        .engine
        .refcount_rows()
        .await
        .unwrap()
        .iter()
        .map(|(_, r)| r.unwrap().live_bytes)
        .sum();
    assert_eq!(
        refs, sealed,
        "the shard's live bytes are the stream's sealed chunks"
    );
    let job = whole_chunks(&rig, hash).await;
    rig.engine.offer_consolidation(hash, job);
    wait_consolidations(&rig.engine, 1).await;
    let after = chunks(&rig.engine.live_chunks(hash).await.unwrap());
    assert!(after < sealed, "one chunk header instead of nine");
    assert_eq!(
        rig.engine.physical_bytes(hash).await.unwrap(),
        after + unsealed
    );
    purge_now(&rig.engine, hash).await;
    assert_eq!(rig.engine.physical_bytes(hash).await.unwrap(), 0);
    let none = BTreeSet::new();
    audit(&rig, &none, "after the purge").await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_reclaim_runs_in_bounded_batches_and_resumes_after_a_failed_one() {
    let rig = gc_rig("gc-batches").await;
    rig.engine.gc_state().configure(|s| s.reclaim_batch = 3);
    let mut rng = StdRng::seed_from_u64(0x6C09);
    let streams = hashes(&mut rng, 2);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 10).await;
    rig.engine.fold_once().await.unwrap();
    let hash = streams[0];
    let chunks = rig.engine.live_chunks(hash).await.unwrap().len() as u64;
    assert_eq!(chunks, 10);
    rig.engine.request_purge(hash, 0).await.unwrap();
    // The second reclaim batch fails (its outcome known: nothing written).
    rig.engine
        .layout
        .seal
        .fold
        .hooks
        .fail_nth_batch
        .store(2, Ordering::Relaxed);
    let first = rig.engine.purge_stream(hash).await;
    assert!(
        first.is_err(),
        "the failed batch stops the reclaim: {first:?}"
    );
    let head = rig.engine.head_row(&hash).await.unwrap().unwrap();
    assert_eq!(head.state, HeadState::Purged);
    assert!(
        head.reclaimed > 0 && head.reclaimed < head.head,
        "one batch landed: {head:?}"
    );
    let none = BTreeSet::new();
    audit(&rig, &none, "between the batches").await;
    assert_eq!(
        rig.engine.purge_stream(hash).await.unwrap(),
        PurgeOutcome::Reclaimed { chunks: chunks - 3 },
        "the next pass resumes after the landed batch"
    );
    audit(&rig, &none, "after the resumed reclaim").await;
    let rows = crate::keyspace::stream_row(&hash, 0);
    let mut left = rig.engine.db.scan_prefix(&rows[..], ..).await.unwrap();
    if let Some(kv) = left.next().await.unwrap() {
        panic!(
            "a purged stream's row is left: {}",
            crate::crypto::hex(&kv.key)
        );
    }
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &BTreeSet::from([hash]), "the other stream").await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_racing_a_seal_in_flight_counts_nothing_the_seal_never_committed() {
    let rig = gc_rig("gc-inflight").await;
    let mut rng = StdRng::seed_from_u64(0x6C0A);
    let streams = hashes(&mut rng, 2);
    let oracle = Mutex::new(Oracle::new());
    rounds(&rig, &mut rng, &streams, &oracle, 3).await;
    for hash in &streams {
        append_recorded(
            &rig.engine,
            &oracle,
            *hash,
            vec![random_bytes(&mut rng, 500)],
        )
        .await;
    }
    // A seal of both streams parks after its PUT, its entries published.
    let hold = Hold::new();
    *rig.engine.layout.seal.hooks.after_publish.lock().unwrap() = Some(hold.clone());
    let victim = streams[0];
    let purge = async {
        while hold.entered.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let out = purge_now(&rig.engine, victim).await;
        *rig.engine.layout.seal.hooks.after_publish.lock().unwrap() = None;
        hold.release();
        out
    };
    let (sealed, chunks) = tokio::join!(rig.engine.seal_once(Trigger::Requested), purge);
    assert_eq!(
        chunks, 3,
        "the three committed chunks, not the one in flight"
    );
    assert_eq!(get(&rig.engine.gc_state().counters.inexact), 0);
    assert!(
        sealed.is_err(),
        "the seal of a purged stream is refused: {sealed:?}"
    );
    let orphans = unnamed(&rig, &["hseg"]).await;
    assert_eq!(orphans.len(), 1, "the refused seal's object");
    audit(&rig, &orphans, "after the race").await;
    rounds(&rig, &mut rng, &streams[1..], &oracle, 1).await;
    assert_eq!(rig.engine.sweep_orphans().await.unwrap().orphans, 1);
    audit(&rig, &BTreeSet::new(), "after the sweep").await;
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &BTreeSet::from([victim]), "the other stream").await;
    rig.engine.begin_close();
}
