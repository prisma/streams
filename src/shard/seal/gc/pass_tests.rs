#![cfg(test)]
//! The collector's own paths (layout 6, package 6), each pinned by what it
//! leaves behind:
//! - a pass acts on its signals only: a purge request and a zeroing raise
//!   them, a pass that finds nothing left lowers them, and rows written
//!   behind their back wait (an idle shard scans nothing);
//! - a read pins only the objects zeroed after it started;
//! - a decrement leaving bytes without a chunk is inexact and refused;
//! - the recount counts every dead incarnation that still waits and how
//!   late the oldest is; the totals are the rows' sums;
//! - the loop recounts and sweeps at their cadences;
//! - `purge_due` counts what still waits; a purge runs from its instant;
//!   an open finds the heads; a purged stream's other rows go in bounded
//!   batches, its billing row stays.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::{SeedableRng, rngs::StdRng};
use slatedb::WriteBatch;

use super::super::gc_tests::{gc_rig, late_append, purge_now, quiet, rounds, stored};
use super::super::tests::{Oracle, Rig, reopen};
use super::purge::PurgeOutcome;
use super::{GcError, GcState};
use crate::spill::gc::{self, Decrements, Garbage, Head, HeadState, ObjectKind};
use crate::spill::object::ObjectId;

fn signals(gc: &GcState) -> (u64, u64) {
    (
        gc.heads_signal.load(Ordering::SeqCst),
        gc.garbage_signal.load(Ordering::SeqCst),
    )
}

fn now_ms() -> u64 {
    crate::shard::now_ms().unsigned_abs()
}

async fn put_head(rig: &Rig, hash: [u8; 16], head: Head) {
    let row = gc::encode_head(&head);
    rig.engine.db.put(gc::head_key(&hash), row).await.unwrap();
}

async fn state_of(rig: &Rig, hash: [u8; 16]) -> Option<Head> {
    rig.engine.head_row(&hash).await.unwrap()
}

/// `rounds` of `hash` alone, each sealed into an object of its own: the
/// objects it added.
async fn objects_of(rig: &Rig, hash: [u8; 16], n: usize, seed: u64) -> BTreeSet<ObjectId> {
    let before: BTreeSet<ObjectId> = stored(rig, "hseg").await.into_keys().collect();
    let mut rng = StdRng::seed_from_u64(seed);
    rounds(rig, &mut rng, &[hash], &Mutex::new(Oracle::new()), n).await;
    let after: BTreeSet<ObjectId> = stored(rig, "hseg").await.into_keys().collect();
    after.difference(&before).copied().collect()
}

/// Wait until `ready`, at most 5 s.
async fn until(what: &str, ready: impl Fn() -> bool) {
    let t0 = Instant::now();
    while !ready() {
        assert!(t0.elapsed() < Duration::from_secs(5), "{what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The opening's signals are lowered by a pass that finds nothing; a purge
/// request's signal makes one pass purge and reclaim, whose zeroing makes
/// the same pass delete the objects; rows written without a signal wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pass_acts_on_its_signals_only() {
    let rig = gc_rig("gc-signals").await;
    let (dead, live) = ([0x31; 16], [0x32; 16]);
    let dead_objects = objects_of(&rig, dead, 2, 0x6C10).await;
    let live_objects = objects_of(&rig, live, 1, 0x6C11).await;
    assert_eq!((dead_objects.len(), live_objects.len()), (2, 1));
    let gc = rig.engine.gc_state();
    assert!(signals(gc).0 > 0 && signals(gc).1 > 0, "the opening's");
    rig.engine.gc_pass().await;
    assert_eq!(signals(gc), (0, 0), "a pass that found nothing lowers both");

    rig.engine.request_purge(dead, 0).await.unwrap();
    assert!(signals(gc).0 > 0, "a request raises the heads signal");
    rig.engine.gc_pass().await;
    let head = state_of(&rig, dead).await.expect("a head row");
    assert_eq!(
        (head.state, head.owes()),
        (HeadState::Purged, false),
        "purged and reclaimed"
    );
    let left: BTreeSet<ObjectId> = stored(&rig, "hseg").await.into_keys().collect();
    assert_eq!(left, live_objects, "the zeroing's signal deleted them");
    assert_eq!(signals(gc), (0, 0), "nothing waits: both lowered");

    // A due 'G' row and a due request written behind the signals' back.
    let stray = ObjectId {
        epoch: rig.engine.writer_epoch,
        seq: 1 << 40,
    };
    let path =
        object_store::path::Path::from(rig.engine.object_path_of(ObjectKind::Segment, stray));
    object_store::ObjectStoreExt::put(&*rig.store, &path, Bytes::from_static(b"x").into())
        .await
        .unwrap();
    let g = Garbage {
        kind: ObjectKind::Segment,
        object_len: 1,
        due_ms: 0,
    };
    let row = gc::encode_garbage(&g);
    rig.engine
        .db
        .put(gc::garbage_key(stray), row)
        .await
        .unwrap();
    put_head(&rig, live, Head::requested(0)).await;
    rig.engine.gc_pass().await;
    assert!(
        stored(&rig, "hseg").await.contains_key(&stray),
        "an idle pass scans no 'G' row"
    );
    assert_eq!(
        state_of(&rig, live).await.map(|h| h.state),
        Some(HeadState::Requested),
        "an idle pass scans no 'H' row"
    );
    rig.engine.begin_close();
}

/// A read pins the objects zeroed after it started, not those zeroed
/// before; the pins and the zeroings are counted and forgotten once the
/// objects are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_pins_only_the_objects_zeroed_after_it_started() {
    let rig = gc_rig("gc-pins").await;
    let dead = [0x33; 16];
    let objects = objects_of(&rig, dead, 2, 0x6C12).await;
    assert_eq!(objects.len(), 2);
    let gc = rig.engine.gc_state();
    let before = gc.pin();
    assert_eq!(gc.readers(), 1);
    purge_now(&rig.engine, dead).await;
    let after = gc.pin();
    assert_eq!(gc.readers(), 2);
    assert_eq!(
        rig.engine.collect().await.unwrap(),
        0,
        "the earlier read pins both"
    );
    drop(before);
    assert_eq!(gc.readers(), 1);
    assert_eq!(
        rig.engine.collect().await.unwrap(),
        2,
        "the later read pins neither"
    );
    drop(after);
    assert_eq!(gc.readers(), 0);
    let v = gc.json();
    let got = [&v["zeroed"], &v["deleted"], &v["pinned"], &v["readers"]];
    let want = [2, 2, 2, 0].map(serde_json::Value::from);
    assert_eq!(got, want.each_ref(), "{v}");
    assert!(super::lock(&gc.zeroed).is_empty(), "deleted: forgotten");
    rig.engine.begin_close();
}

/// A decrement that would leave an object bytes and no chunk is refused as
/// inexact and counted; the exact one zeroes the object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_decrement_leaving_bytes_and_no_chunk_is_inexact() {
    let rig = gc_rig("gc-inexact").await;
    let hash = [0x34; 16];
    let objects = objects_of(&rig, hash, 1, 0x6C13).await;
    let object = *objects.first().expect("one object");
    let entries = rig.engine.live_chunks(hash).await.unwrap();
    let mut exact = Decrements::new();
    for e in &entries {
        gc::decrement(&mut exact, e);
    }
    let mut short = exact.clone();
    short.get_mut(&object).expect("its chunks").0 -= 1;
    assert_eq!(rig.engine.gc_inexact(), 0);
    let _refs = rig.engine.gc_state().refs.lock().await;
    let mut batch = WriteBatch::new();
    let refused = rig.engine.stage_decrements(&mut batch, &short).await;
    assert!(matches!(refused, Err(GcError::Inexact(_))), "{refused:?}");
    assert_eq!(rig.engine.gc_inexact(), 1);
    let mut batch = WriteBatch::new();
    let zeroed = rig
        .engine
        .stage_decrements(&mut batch, &exact)
        .await
        .unwrap();
    assert_eq!(zeroed, vec![object]);
    rig.engine.begin_close();
}

#[test]
fn a_gc_error_names_its_kind() {
    let shown = [
        GcError::Store("s".into()),
        GcError::Inexact("i".into()),
        GcError::Index("x".into()),
    ]
    .map(|e| e.to_string());
    assert_eq!(shown, ["store: s", "inexact refcount: i", "index: x"]);
}

/// The recount counts a request and a purge that owes, not a reclaimed
/// one, and reports how long the oldest has waited past its instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_recount_counts_what_waits_and_how_late_the_oldest_is() {
    let rig = gc_rig("gc-recount").await;
    let now = now_ms();
    let purged = |not_before_ms, reclaimed| Head {
        state: HeadState::Purged,
        not_before_ms,
        head: 9,
        reclaimed,
    };
    put_head(&rig, [0x35; 16], Head::requested(now - 10_000)).await;
    put_head(&rig, [0x36; 16], purged(now - 20_000, 0)).await;
    put_head(&rig, [0x37; 16], purged(now - 60_000, 9)).await;
    assert_eq!(rig.engine.gc_reclaim_lag(), (0, 0), "before a recount");
    rig.engine.recount_totals().await;
    let (waiting, late) = rig.engine.gc_reclaim_lag();
    assert_eq!(waiting, 2, "the request and the owing purge");
    assert!((20..25).contains(&late), "the oldest waits 20 s: {late}");
    rig.engine.begin_close();
}

/// The totals are the `'R'` and `'G'` rows' sums, and the recount puts them
/// in the debug JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_totals_are_the_rows_sums() {
    let rig = gc_rig("gc-totals").await;
    let (dead, live) = ([0x38; 16], [0x39; 16]);
    objects_of(&rig, dead, 2, 0x6C14).await;
    objects_of(&rig, live, 1, 0x6C15).await;
    rig.engine
        .gc_state()
        .configure(|s| s.grace = Duration::from_secs(3600));
    purge_now(&rig.engine, dead).await;
    let refs: Vec<gc::Refcount> = rig
        .engine
        .refcount_rows()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, r)| r)
        .collect();
    let garbage = rig.engine.garbage_rows().await.unwrap();
    assert_eq!((refs.len(), garbage.len()), (1, 2));
    let expected = serde_json::json!({
        "live_objects": 1,
        "live_bytes": refs[0].live_bytes,
        "live_object_bytes": refs[0].object_len,
        "garbage_objects": 2,
        "garbage_bytes": garbage.iter().map(|(_, g)| g.object_len).sum::<u64>(),
    });
    let totals = rig.engine.gc_totals().await.unwrap();
    assert_eq!(totals, expected);
    rig.engine.recount_totals().await;
    assert_eq!(rig.engine.gc_state().json()["stored"], expected);
    rig.engine.begin_close();
}

/// The loop recounts once at its first pass and then every
/// `totals_every`, and sweeps every `sweep_every` from its start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_loop_recounts_and_sweeps_at_their_cadences() {
    let rig = gc_rig("gc-loop").await;
    let gc = rig.engine.gc_state();
    gc.configure(|s| {
        s.tick = Duration::from_millis(10);
        s.totals_every = Duration::from_secs(3600);
        s.sweep_every = Duration::from_secs(3600);
        s.auto = true;
    });
    // The engine's own loop sleeps out the tick it read at its start (30
    // s); a loop of this test's runs beside it at the 10 ms tick.
    let body = async {
        until("the first pass recounts", || !gc.json()["stored"].is_null()).await;
        cadences(&rig).await;
    };
    tokio::select! {
        () = rig.engine.clone().gc_loop() => panic!("the loop ended"),
        () = body => {}
    }
    rig.engine.begin_close();
}

async fn cadences(rig: &Rig) {
    let gc = rig.engine.gc_state();
    put_head(rig, [0x3A; 16], Head::requested(u64::MAX / 2)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        (
            gc.counters.sweeps.load(Ordering::Relaxed),
            rig.engine.gc_reclaim_lag().0
        ),
        (0, 0),
        "neither is due within the hour"
    );
    gc.configure(|s| {
        s.totals_every = Duration::ZERO;
        s.sweep_every = Duration::ZERO;
    });
    until("both run every pass", || {
        gc.counters.sweeps.load(Ordering::Relaxed) > 1 && rig.engine.gc_reclaim_lag().0 == 1
    })
    .await;
}

/// `purge_due` counts what still waits: a request not yet due, a refused
/// one, and an owing purge whose reclaim fails; it purges the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn purge_due_counts_the_incarnations_that_still_wait() {
    let rig = gc_rig("gc-due").await;
    let (later, refused, due, failing) = ([0x3B; 16], [0x3C; 16], [0x3D; 16], [0x3E; 16]);
    for (i, hash) in [later, refused, due].into_iter().enumerate() {
        objects_of(&rig, hash, 1, 0x6C20 + i as u64).await;
    }
    let failing_objects = objects_of(&rig, failing, 1, 0x6C23).await;
    rig.engine.request_purge(later, now_ms()).await.unwrap();
    rig.engine.request_purge(refused, 0).await.unwrap();
    rig.engine.layout.mark_usage(refused, 7);
    rig.engine.request_purge(due, 0).await.unwrap();
    // A purge whose reclaim fails: its object's row is gone.
    let tail = rig.engine.live_chunks(failing).await.unwrap();
    let head = tail.iter().map(|e| e.last + 1).max().unwrap();
    let purged = Head {
        state: HeadState::Purged,
        not_before_ms: 0,
        head,
        reclaimed: 0,
    };
    put_head(&rig, failing, purged).await;
    for id in &failing_objects {
        rig.engine.db.delete(gc::refcount_key(*id)).await.unwrap();
    }
    assert_eq!(rig.engine.purge_due().await.unwrap(), 3);
    let state = |h| state_of(&rig, h);
    assert_eq!(state(due).await.map(|h| h.owes()), Some(false), "purged");
    assert_eq!(
        state(later).await.map(|h| h.state),
        Some(HeadState::Requested)
    );
    rig.engine.layout.clear_usage(refused, None);
    assert_eq!(
        rig.engine.purge_due().await.unwrap(),
        2,
        "the refused one purged now"
    );
    assert_eq!(rig.engine.gc_inexact(), 2, "the failing reclaim, twice");
    rig.engine.begin_close();
}

/// A purge runs from its `not_before` instant, not a millisecond before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_runs_from_its_instant() {
    let rig = gc_rig("gc-instant").await;
    let hash = [0x3F; 16];
    objects_of(&rig, hash, 1, 0x6C24).await;
    rig.engine.request_purge(hash, 0).await.unwrap();
    let at = state_of(&rig, hash).await.unwrap().not_before_ms;
    assert_eq!(
        rig.engine.purge_stream_at(hash, at - 1).await.unwrap(),
        PurgeOutcome::NotDue
    );
    assert!(matches!(
        rig.engine.purge_stream_at(hash, at).await.unwrap(),
        PurgeOutcome::Reclaimed { chunks } if chunks > 0
    ));
    rig.engine.begin_close();
}

/// A shard with no `'H'` row reads none in its folds; an open finds a
/// request's row, and a requested stream with no tail (its load reads the
/// head row) still loads and appends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_open_finds_the_heads_and_a_request_is_not_a_purge() {
    let rig = gc_rig("gc-probe").await;
    let (asked, other) = ([0x40; 16], [0x41; 16]);
    objects_of(&rig, other, 1, 0x6C26).await;
    let reads = |rig: &Rig| {
        rig.engine
            .gc_state()
            .counters
            .head_reads
            .load(Ordering::Relaxed)
    };
    assert!(!rig.engine.gc_state().heads_exist());
    rig.engine.fold_once().await.unwrap();
    assert_eq!(reads(&rig), 0, "no 'H' row: none read");
    rig.engine.request_purge(asked, now_ms()).await.unwrap();
    let rig = reopen(rig).await;
    quiet(&rig.engine);
    assert!(
        rig.engine.gc_state().heads_exist(),
        "the open found the row"
    );
    let (tx, answer) = tokio::sync::oneshot::channel();
    let append = crate::shard::AppendReq {
        resp: tx,
        ..late_append(asked)
    };
    rig.engine.try_enqueue(append).unwrap();
    assert!(answer.await.unwrap().is_ok(), "a requested stream loads");
    objects_of(&rig, other, 1, 0x6C27).await;
    rig.engine.fold_once().await.unwrap();
    assert!(reads(&rig) > 0, "with an 'H' row, the fold reads the heads");
    rig.engine.begin_close();
}

/// Every row key of `hash`'s stream.
async fn stream_rows(rig: &Rig, hash: [u8; 16]) -> Vec<Vec<u8>> {
    let prefix = crate::keyspace::stream_row(&hash, 0);
    let mut it = rig.engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut keys = Vec::new();
    while let Some(kv) = it.next().await.unwrap() {
        keys.push(kv.key.to_vec());
    }
    keys
}

/// A purge deletes the stream's other rows, `reclaim_batch` per batch, and
/// keeps its billing row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_deletes_the_streams_other_rows_in_bounded_batches() {
    let rig = gc_rig("gc-rows").await;
    let hash = [0x42; 16];
    for (n, id) in ["p1", "p2", "p3"].into_iter().enumerate() {
        let (tx, answer) = tokio::sync::oneshot::channel();
        let producer = crate::shard::ProducerReq {
            id: id.into(),
            epoch: 0,
            seq: 0,
            request_hash: None,
        };
        let append = crate::shard::AppendReq {
            resp: tx,
            producer: Some(producer),
            ..late_append(hash)
        };
        rig.engine.try_enqueue(append).unwrap();
        assert!(answer.await.unwrap().is_ok(), "append {n}");
    }
    rig.engine
        .seal_once(super::super::Trigger::Requested)
        .await
        .unwrap();
    let keep = crate::billing::billing_meta_key(&hash);
    let billed = stream_rows(&rig, hash).await.contains(&keep);
    rig.engine.gc_state().configure(|s| s.reclaim_batch = 2);
    purge_now(&rig.engine, hash).await;
    let left = stream_rows(&rig, hash).await;
    assert_eq!(left, if billed { vec![keep] } else { vec![] }, "{left:?}");
    let c = &rig.engine.gc_state().counters;
    let deleted = c.rows_deleted.load(Ordering::Relaxed);
    assert!(
        deleted >= 3,
        "the three producers' rows at least: {deleted}"
    );
    assert_eq!(c.row_batches.load(Ordering::Relaxed), deleted.div_ceil(2));
    rig.engine.begin_close();
}

/// A purge's reclaim decrements the chunks ending below its head, not one
/// ending at it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_reclaim_stops_below_its_head() {
    let rig = gc_rig("gc-head-edge").await;
    let hash = [0x43; 16];
    objects_of(&rig, hash, 2, 0x6C28).await;
    let chunks = rig.engine.live_chunks(hash).await.unwrap();
    let last = chunks.iter().max_by_key(|e| e.last).unwrap().clone();
    let at_head = Head {
        state: HeadState::Purged,
        not_before_ms: 0,
        head: last.last,
        reclaimed: 0,
    };
    put_head(&rig, hash, at_head).await;
    let outcome = rig.engine.purge_stream(hash).await.unwrap();
    let below = chunks.iter().filter(|e| e.last < last.last).count() as u64;
    assert_eq!(outcome, PurgeOutcome::Reclaimed { chunks: below });
    let rows: BTreeSet<ObjectId> = rig
        .engine
        .refcount_rows()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(rows.contains(&last.object), "the chunk at the head is live");
    rig.engine.begin_close();
}

async fn index_objects(rig: &Rig) -> BTreeSet<ObjectId> {
    stored(rig, "ridx").await.into_keys().collect()
}

/// A merge drops a reclaimed stream's entries and counts them; the run-
/// index objects it retires raise the garbage signal and stay while a read
/// that started before the merge runs, not one that started after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_drops_reclaimed_entries_and_its_inputs_are_pinned_by_earlier_reads() {
    let rig = gc_rig("gc-merge").await;
    let (dead, live) = ([0x44; 16], [0x45; 16]);
    let mut rng = StdRng::seed_from_u64(0x6C29);
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..2 {
        rounds(&rig, &mut rng, &[dead, live], &oracle, 1).await;
        rig.engine.fold_once().await.unwrap();
    }
    let inputs: BTreeSet<ObjectId> = rig.engine.layout.seal.fold.set().keys().copied().collect();
    assert_eq!(inputs.len(), 2);
    let dead_entries = rig.engine.live_chunks(dead).await.unwrap().len() as u64;
    purge_now(&rig.engine, dead).await;
    let gc = rig.engine.gc_state();
    rig.engine.gc_pass().await;
    assert_eq!(signals(gc), (0, 0));
    let before = gc.pin();
    rig.engine
        .merge_once()
        .await
        .expect("a merge is due")
        .unwrap();
    assert!(
        signals(gc).1 > 0,
        "the retired inputs raise the garbage signal"
    );
    let after = gc.pin();
    let fold = rig.engine.layout.seal.fold.json();
    assert_eq!(fold["entries_dropped"], dead_entries, "{fold}");
    rig.engine.collect().await.unwrap();
    assert!(
        inputs.is_subset(&index_objects(&rig).await),
        "the earlier read pins them"
    );
    drop(before);
    rig.engine.collect().await.unwrap();
    assert!(
        inputs.is_disjoint(&index_objects(&rig).await),
        "the later read pins neither"
    );
    drop(after);
    rig.engine.begin_close();
}
