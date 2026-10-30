#![cfg(test)]
//! Reading run-index objects: a footer whose trailer disagrees with the
//! pointer row naming it, or a run that disagrees with its footer entry, is
//! corruption and never cached; a transient store error is tried three
//! times; a stream's runs are those overlapping the window; a cached run is
//! charged its entries.

use std::sync::Arc;
use std::time::Duration;

use rand::{SeedableRng, rngs::StdRng};

use super::{GET_ATTEMPTS, run_heap};
use crate::dst::{FaultPlan, FaultProfile, FaultStore, ObjClass, StoreOp};
use crate::shard::ShardEngine;
use crate::shard::record::RangeReadError;
use crate::shard::seal::Trigger;
use crate::shard::seal::tests::{
    Rig, append_lane, hashes, manual, open, random_bytes, reopen, rig_with,
};
use crate::spill::directory::DirEntry;
use crate::spill::runindex::Pointer;
use crate::spill::runindex::format::FooterEntry;

/// One field of a row changed.
type Change<T> = fn(&mut T);

/// A rig whose one run-index object holds two streams, each sealed twice,
/// reopened with cold caches; its pointer row and the first stream.
async fn folded(label: &str) -> (Rig, Pointer, [u8; 16]) {
    let rig = rig_with(label, manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x7E5);
    let streams = hashes(&mut rng, 2);
    for _ in 0..2 {
        for &hash in &streams {
            append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 300)]).await;
        }
        rig.engine.seal_once(Trigger::Requested).await.unwrap();
    }
    rig.engine.fold_once().await.unwrap();
    // A new owner: the fold cached the footer it wrote.
    let rig = reopen(rig).await;
    let set = rig.engine.layout.seal.fold.set();
    let p = set.values().next().unwrap().clone();
    assert_eq!((set.len(), p.runs, p.entries), (1, 2, 4));
    (rig, p, streams[0])
}

fn corrupt<T: std::fmt::Debug>(r: &Result<T, RangeReadError>, what: &str) {
    assert!(
        matches!(r, Err(RangeReadError::Store(e)) if e.to_string().contains("disagrees")),
        "{what}: {r:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_footer_or_run_disagreeing_with_what_names_it_is_corruption() {
    let (rig, p, hash) = folded("runs-agree").await;
    let e = &rig.engine;
    let bad: [(&str, Change<Pointer>); 4] = [
        ("footer length", |q| q.footer_len += 1),
        ("runs", |q| q.runs += 1),
        ("entries", |q| q.entries += 1),
        ("level", |q| q.level += 1),
    ];
    for (what, change) in bad {
        let mut q = p.clone();
        change(&mut q);
        corrupt(&e.fetch_footer(&q).await, what);
    }
    let mut long = p.clone();
    long.len += 16;
    assert!(
        e.fetch_footer(&long).await.is_err(),
        "a range past the object"
    );
    let footer = e.fetch_footer(&p).await.unwrap();
    assert_eq!(footer.len(), 2);
    let f = footer.iter().find(|f| f.hash == hash).unwrap().clone();
    assert_eq!(f.entries, 2);
    let bad: [(&str, Change<FooterEntry>); 3] = [
        ("entries", |g| g.entries += 1),
        ("first", |g| g.first += 1),
        ("last", |g| g.last += 1),
    ];
    for (what, change) in bad {
        let mut g = f.clone();
        change(&mut g);
        corrupt(&e.fetch_run(&p, &g).await, what);
    }
    let run = e.fetch_run(&p, &f).await.unwrap();
    assert_eq!((run.len(), run[0].first, run[1].last), (2, f.first, f.last));
    let runs = |from, to| {
        let e = Arc::clone(e);
        async move { e.runs_of(&hash, from, to).await.unwrap().len() }
    };
    assert_eq!(runs(f.last, f.first + 1).await, 1, "both ends overlap");
    assert_eq!(runs(f.last + 1, u64::MAX).await, 0, "a window past its end");
    assert_eq!(runs(0, f.first).await, 0, "a window ending at its first");
    rig.engine.begin_close();
}

/// A footer GET the store keeps failing is tried three times, then the
/// read fails as the store's unavailability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_failing_index_get_is_tried_three_times() {
    let (rig, p, _) = folded("runs-retry").await;
    rig.engine.begin_close();
    let profile = FaultProfile::clean().with_op_class(
        StoreOp::Get,
        ObjClass::Other,
        FaultPlan::new(100, 0, 0),
    );
    let store = FaultStore::new(rig.store.clone(), 0x7E6, profile);
    let fresh: Arc<ShardEngine> = open(
        store.clone(),
        &rig.prefix,
        manual(),
        0,
        Duration::from_millis(2),
    )
    .await;
    let before = store.count(StoreOp::Get, ObjClass::Other);
    let read = tokio::time::timeout(Duration::from_secs(5), fresh.fetch_footer(&p))
        .await
        .expect("the footer read gives up");
    assert!(
        matches!(&read, Err(RangeReadError::Store(e)) if !e.to_string().contains("corrupt")),
        "the store's unavailability: {read:?}"
    );
    assert_eq!(GET_ATTEMPTS, 3);
    assert_eq!(store.count(StoreOp::Get, ObjClass::Other) - before, 3);
    fresh.begin_close();
}

#[test]
fn a_cached_run_is_charged_its_entries_and_their_heap() {
    let e = DirEntry {
        object: crate::spill::object::ObjectId { epoch: 1, seq: 1 },
        chunk_off: 0,
        chunk_len: 1,
        first: 0,
        last: 0,
        page_count: 1,
        keys: crate::spill::directory::KeySummary::Keys(vec![]),
        anchors: Vec::new(),
    };
    let per = size_of::<DirEntry>() as u64 + 64;
    assert_eq!(run_heap(&[]), 0);
    assert_eq!(run_heap(&[e.clone(), e.clone(), e]), 3 * per);
}

/// A read the directory tiles alone consults no run-index object: after a
/// fold and a reopen (a cold index cache), a chunk sealed since the fold is
/// read without a footer GET.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_the_directory_tiles_alone_fetches_no_footer() {
    let (rig, _, hash) = folded("runs-dir-only").await;
    let mut rng = StdRng::seed_from_u64(0x7E7);
    append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 300)]).await;
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let footers = &rig.engine.layout.seal.fold.counters.footer_gets;
    let before = footers.load(std::sync::atomic::Ordering::Relaxed);
    let window = crate::shard::RingScan {
        from: 2,
        to: 3,
        max_bytes: 1 << 20,
    };
    let out = rig
        .engine
        .read_stream_window(&handle, window, None, crate::shard::Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(out.last_offset, Some(2), "the chunk sealed since the fold");
    assert_eq!(
        footers.load(std::sync::atomic::Ordering::Relaxed) - before,
        0,
        "the directory alone tiles the window"
    );
    rig.engine.begin_close();
}
