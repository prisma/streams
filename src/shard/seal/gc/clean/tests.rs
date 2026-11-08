#![cfg(test)]
//! The segment cleaner (DESIGN-cleaner rev 2): its rule in dollars, and in
//! process, on a shard where a long-lived stream (the ledger) shares every
//! seal object with a stream that dies:
//! - the dead stream's purge leaves each object pinned by one ledger chunk;
//!   a pass copies the ledger's chunks into one consolidated chunk, the old
//!   objects are released and deleted, the ledger reads back exactly, and
//!   the refcounts stay exact (the gc audit after every step);
//! - a crash after the copy's PUT leaves an orphan the sweep deletes, the
//!   old objects untouched; a restart after the commit serves the copy;
//! - a read that started before the copy keeps the old objects stored;
//! - a purge request landing mid-pass abandons the copy whole; a purge after
//!   it reclaims the copy; a fold after it keeps the copy;
//! - a consolidation queued with the old chunks finds the copy and skips;
//! - an object pinned only by dead streams backs off; nothing runs inside
//!   the quiet period or at the L0 watermark's yield mark.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;

use super::super::super::gc_tests::{audit, gc_rig, purge_now, stored, unnamed, whole_chunks};
use super::super::super::test_hooks::Hold;
use super::super::super::tests::{Oracle, Rig, append_recorded, read_back, reopen};
use super::super::CleanHook;
use super::{CANDIDATES, Candidates, CleanReport, PASS_BYTES, clean_pays};
use crate::shard::Deliver;
use crate::spill::gc::Refcount;
use crate::spill::object::ObjectId;

const KB: u64 = 1000;
const MB: u64 = 1000 * KB;

/// The dead-byte floor: a nearly dead object is rewritten from about 295 KB
/// dead (two PUTs and two GETs), not below.
#[test]
fn an_object_needs_about_295_kb_dead_bytes() {
    assert!(
        !clean_pays(290 * KB, 0, 1),
        "290 KB dead is not worth a copy"
    );
    assert!(clean_pays(300 * KB, 0, 1), "300 KB dead pays");
    assert!(!clean_pays(300 * KB, 0, 20), "twenty GETs more");
}

/// A large object pays below half live; the rule never rewrites from half.
#[test]
fn a_large_object_is_rewritten_below_half_live() {
    let o = 100 * MB;
    assert!(clean_pays(o, 49 * MB, 4), "49% live pays");
    assert!(!clean_pays(o, o / 2, 1), "half live never");
    assert!(!clean_pays(o, o, 1), "all live never");
}

/// The soak's case: a 560 KB seal object kept by one 4 KB ledger chunk.
#[test]
fn the_soaks_ledger_pinned_object_is_rewritten() {
    assert!(clean_pays(560 * KB, 4 * KB, 1));
}

#[test]
fn an_object_with_no_live_or_no_dead_bytes_is_not_rewritten() {
    assert!(!clean_pays(10 * MB, 0, 0), "no live chunk: the collector's");
    assert!(!clean_pays(10 * MB, 10 * MB, 3), "nothing dead");
    assert!(!clean_pays(0, 0, 1));
}

/// The rule's edges: dead bytes are the object's length less its live
/// bytes, the copy's transfer is charged per live byte, and an exact tie
/// does not pay.
#[test]
fn the_rule_counts_dead_bytes_transfer_per_byte_and_no_tie() {
    assert!(
        !clean_pays(300 * KB, 100 * KB, 1),
        "200 KB dead is under the floor"
    );
    assert!(
        !clean_pays(1_100 * KB, 500 * KB, 1),
        "600 KB dead does not pay a 500 KB copy's transfer"
    );
    // 2^22 dead bytes x storage equals the copy x GiB exactly at k = 68 and
    // 4,190,625 live bytes.
    assert!(!clean_pays(8_384_929, 4_190_625, 68), "a tie does not pay");
    assert!(
        clean_pays(8_384_930, 4_190_625, 68),
        "one dead byte more does"
    );
}

fn row(object_len: u64, live_bytes: u64) -> Refcount {
    Refcount {
        live_bytes,
        live_chunks: 1,
        created_ms: 0,
        object_len,
    }
}

fn always(_: u64, _: u64, _: u64) -> bool {
    true
}

fn id(seq: u64) -> ObjectId {
    ObjectId { epoch: 1, seq }
}

/// At its cap the candidate set drops the one with the fewest dead bytes
/// (not the smallest length plus live bytes, nor the smallest ratio).
#[test]
fn a_full_candidate_set_drops_the_fewest_dead_bytes() {
    let c = Candidates::default();
    let cap = CANDIDATES as u64;
    // 70 KB dead (length per live byte 3); the 75 KB objects have the
    // smallest sum (76 KB), the 300 KB ones the smallest ratio (2).
    c.offer(id(0), row(100_000, 30_000), 0, always);
    for seq in 1..cap {
        let r = if seq % 2 == 0 {
            row(75_000, 1_000)
        } else {
            row(300_000, 149_000)
        };
        c.offer(id(seq), r, 0, always);
    }
    assert_eq!(c.len(), CANDIDATES, "full: nothing dropped yet");
    c.offer(id(cap), row(300_000, 149_000), 0, always);
    assert_eq!(c.len(), CANDIDATES);
    let set = c.set.lock().unwrap();
    assert!(!set.contains_key(&id(0)), "the fewest dead bytes left");
    assert!(set.contains_key(&id(cap)));
}

/// The recount's seed adds a candidate the set did not know and keeps the
/// decrement time of one it knows.
#[test]
fn a_seed_adds_unknown_candidates_and_keeps_known_times() {
    let c = Candidates::default();
    c.offer(id(1), row(100_000, 1_000), 0, always);
    c.seed(id(1), row(100_000, 1_000), 1_000, always);
    c.seed(id(2), row(100_000, 1_000), 1_000, always);
    assert_eq!(c.len(), 2);
    let due: Vec<ObjectId> = c.due(600, 500).into_iter().map(|(id, _)| id).collect();
    assert_eq!(due, [id(1)], "the known one kept its decrement at 0");
}

const LEDGER: [u8; 16] = [0x50; 16];
const DEAD: [u8; 16] = [0x51; 16];

/// A test rule: anything less than half live (the rig's objects are small).
fn half_dead(object_len: u64, live_bytes: u64, live_chunks: u64) -> bool {
    live_chunks > 0 && 2 * live_bytes < object_len
}

/// A rig of `n` seal objects, each holding a small ledger chunk and a large
/// chunk of a stream that is then purged: every object a candidate, pinned
/// by its ledger chunk. The ledger's acked records in `oracle`.
async fn pinned(label: &str, n: usize, oracle: &Mutex<Oracle>) -> (Rig, BTreeSet<ObjectId>) {
    let ledger = |i: usize| vec![Bytes::from(vec![u8::try_from(i).unwrap(); 32])];
    let dead = |_| vec![Bytes::from(vec![0xDD; 900]), Bytes::from(vec![0xEE; 900])];
    pinned_with(label, n, oracle, ledger, dead).await
}

/// `pinned` with each seal's ledger and dead records from `ledger` and
/// `dead`.
async fn pinned_with(
    label: &str,
    n: usize,
    oracle: &Mutex<Oracle>,
    ledger: impl Fn(usize) -> Vec<Bytes>,
    big: impl Fn(usize) -> Vec<Bytes>,
) -> (Rig, BTreeSet<ObjectId>) {
    let rig = gc_rig(label).await;
    rig.engine.gc_state().configure(|s| {
        s.purge_delay = Duration::ZERO;
        s.clean_pays = half_dead;
    });
    let dead = Mutex::new(Oracle::new());
    for i in 0..n {
        append_recorded(&rig.engine, oracle, LEDGER, ledger(i)).await;
        append_recorded(&rig.engine, &dead, DEAD, big(i)).await;
        rig.engine
            .seal_once(crate::shard::seal::Trigger::Requested)
            .await
            .expect("seal");
    }
    let objects: BTreeSet<ObjectId> = stored(&rig, "hseg").await.into_keys().collect();
    assert_eq!(objects.len(), n, "one object per seal");
    purge_now(&rig.engine, DEAD).await;
    assert_eq!(rig.engine.gc_state().clean.len(), n, "each a candidate");
    (rig, objects)
}

fn ledger(oracle: &Mutex<Oracle>) -> std::collections::BTreeMap<u64, Bytes> {
    oracle
        .lock()
        .unwrap()
        .get(&LEDGER)
        .cloned()
        .unwrap_or_default()
}

/// A pass copies the ledger's chunks into one chunk of one object; the old
/// objects are released and deleted; the ledger reads back exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pass_rewrites_objects_a_ledger_chunk_pins_and_releases_them() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, old) = pinned("clean-basic", 4, &oracle).await;
    let none = BTreeSet::new();
    audit(&rig, &none, "before the pass").await;
    let report = rig.engine.clean_pass().await.unwrap();
    assert_eq!(
        report,
        CleanReport {
            released: 4,
            copied_chunks: 4,
            abandoned: false
        }
    );
    let a = audit(&rig, &none, "after the pass").await;
    assert_eq!(
        (a.live_objects, a.live_chunks, a.garbage),
        (1, 1, 4),
        "one consolidated chunk"
    );
    assert_eq!(rig.engine.collect().await.unwrap(), 4);
    let left: BTreeSet<ObjectId> = stored(&rig, "hseg").await.into_keys().collect();
    assert!(left.is_disjoint(&old) && left.len() == 1, "{left:?}");
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(rig.engine.gc_state().clean.len(), 0);
    rig.engine.begin_close();
}

/// A crash after the copy's PUT: the old objects are untouched, the copy an
/// orphan the sweep deletes; the next pass completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_crash_after_the_copys_put_leaves_an_orphan_the_sweep_deletes() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, old) = pinned("clean-crash-put", 3, &oracle).await;
    *rig.engine.gc_state().clean_hook.lock().unwrap() = Some(CleanHook::Abort);
    assert!(rig.engine.clean_pass().await.is_err());
    *rig.engine.gc_state().clean_hook.lock().unwrap() = None;
    let orphans = unnamed(&rig, &["hseg"]).await;
    assert_eq!(orphans.len(), 1, "the copy, named by no row");
    audit(&rig, &orphans, "after the crash").await;
    assert!(old.iter().all(|id| !orphans.contains(id)));
    assert_eq!(rig.engine.sweep_orphans().await.unwrap().orphans, 1);
    audit(&rig, &BTreeSet::new(), "after the sweep").await;
    let report = rig.engine.clean_pass().await.unwrap();
    assert_eq!((report.released, report.abandoned), (3, false));
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}

/// A restart after the commit: the copy's entries come back from its `'K'`
/// row and win their spans; the old objects' `'G'` rows are collected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_restart_after_the_commit_serves_the_copy() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-restart", 3, &oracle).await;
    rig.engine.clean_pass().await.unwrap();
    let rig = reopen(rig).await;
    super::super::super::gc_tests::quiet(&rig.engine);
    assert_eq!(rig.engine.collect().await.unwrap(), 3);
    audit(&rig, &BTreeSet::new(), "after the restart").await;
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}

/// A read that started before the copy's commit keeps the old objects
/// stored until it ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_read_started_before_the_copy_keeps_the_old_objects() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, old) = pinned("clean-pin", 3, &oracle).await;
    let pin = rig.engine.gc_state().pin();
    rig.engine.clean_pass().await.unwrap();
    assert_eq!(rig.engine.collect().await.unwrap(), 0, "pinned");
    let kept: BTreeSet<ObjectId> = stored(&rig, "hseg").await.into_keys().collect();
    assert!(old.is_subset(&kept));
    drop(pin);
    assert_eq!(rig.engine.collect().await.unwrap(), 3);
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}

/// A purge request for the ledger landing while the pass holds its copy
/// abandons the copy whole; the old objects stay counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_purge_request_mid_pass_abandons_the_copy() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-race-purge", 3, &oracle).await;
    let hold = Hold::new();
    *rig.engine.gc_state().clean_hook.lock().unwrap() = Some(CleanHook::Hold(hold.clone()));
    let race = async {
        hold.wait_entered().await;
        let later = crate::shard::now_ms().unsigned_abs() + 3_600_000;
        rig.engine.request_purge(LEDGER, later).await.unwrap();
        hold.release();
    };
    let (report, ()) = tokio::join!(rig.engine.clean_pass(), race);
    assert!(report.unwrap().abandoned, "the ledger got an 'H' row");
    let orphans = unnamed(&rig, &["hseg"]).await;
    assert_eq!(orphans.len(), 1);
    audit(&rig, &orphans, "after the abandoned copy").await;
    rig.engine.begin_close();
}

/// After a copy, a purge of the ledger reclaims the copy; a fold keeps the
/// copy over the old entries; the audit stays exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fold_after_the_copy_keeps_it_and_a_purge_reclaims_it() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-fold-purge", 4, &oracle).await;
    rig.engine.clean_pass().await.unwrap();
    rig.engine.fold_once().await.unwrap();
    assert_eq!(rig.engine.collect().await.unwrap(), 4);
    audit(&rig, &BTreeSet::new(), "after the fold").await;
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    purge_now(&rig.engine, LEDGER).await;
    let a = audit(&rig, &BTreeSet::new(), "after the ledger's purge").await;
    assert_eq!((a.live_objects, a.live_chunks), (0, 0));
    rig.engine.begin_close();
}

/// A consolidation offered the old chunks after the copy finds the copy as
/// their pick and skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_consolidation_queued_with_the_old_chunks_skips() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-race-cons", 8, &oracle).await;
    let old = whole_chunks(&rig, LEDGER).await;
    assert_eq!(old.len(), 8);
    rig.engine.clean_pass().await.unwrap();
    let c = &rig.engine.layout.seal.fold.consolidator;
    let before = c.skipped.load(std::sync::atomic::Ordering::Relaxed);
    rig.engine.offer_consolidation(LEDGER, old);
    let t0 = std::time::Instant::now();
    while c.skipped.load(std::sync::atomic::Ordering::Relaxed) == before {
        assert!(t0.elapsed() < Duration::from_secs(10), "the job never ran");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    audit(&rig, &BTreeSet::new(), "after the stale job").await;
    rig.engine.begin_close();
}

/// An object whose live chunks are all a dead stream's backs off: a pass
/// reads it and copies nothing, the next one does not read it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_object_pinned_by_dead_streams_backs_off() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-backoff", 2, &oracle).await;
    let later = crate::shard::now_ms().unsigned_abs() + 3_600_000;
    rig.engine.request_purge(LEDGER, later).await.unwrap();
    let gets = || {
        rig.fault
            .count(crate::dst::StoreOp::Get, crate::dst::ObjClass::Other)
    };
    let before = gets();
    let report = rig.engine.clean_pass().await.unwrap();
    assert_eq!(report, CleanReport::default(), "nothing to copy");
    let read = gets();
    assert!(read > before, "the first pass read the tables");
    rig.engine.clean_pass().await.unwrap();
    assert_eq!(gets(), read, "the next one backed off: no table read");
    assert_eq!(rig.engine.gc_state().clean.len(), 2, "still candidates");
    rig.engine.begin_close();
}

/// Inside the quiet period after a decrement, and at the L0 watermark's
/// yield mark, a pass copies nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn nothing_runs_in_the_quiet_period_or_at_the_yield_mark() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-quiet", 2, &oracle).await;
    rig.engine
        .gc_state()
        .configure(|s| s.purge_delay = Duration::from_secs(3600));
    assert_eq!(
        rig.engine.clean_pass().await.unwrap(),
        CleanReport::default()
    );
    rig.engine
        .gc_state()
        .configure(|s| s.purge_delay = Duration::ZERO);
    rig.engine.set_l0_line(4);
    rig.engine.layout.l0.observe(2);
    assert_eq!(
        rig.engine.clean_pass().await.unwrap(),
        CleanReport::default()
    );
    rig.engine.layout.l0.observe(0);
    assert_eq!(rig.engine.clean_pass().await.unwrap().released, 2);
    rig.engine.begin_close();
}

/// A pass stops at 64 GETs: 40 objects of one ledger chunk each (a table
/// GET and a chunk GET apiece) take two passes, 32 and 8.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pass_stops_at_its_get_budget() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-budget-gets", 40, &oracle).await;
    assert_eq!(rig.engine.clean_pass().await.unwrap().released, 32);
    assert_eq!(rig.engine.clean_pass().await.unwrap().released, 8);
    audit(&rig, &BTreeSet::new(), "after both passes").await;
    rig.engine.begin_close();
}

/// Random bytes: pages that do not compress.
fn noise(seed: u64, len: usize) -> Bytes {
    use rand::{RngCore, SeedableRng};
    let mut b = vec![0u8; len];
    rand::rngs::StdRng::seed_from_u64(seed).fill_bytes(&mut b);
    Bytes::from(b)
}

/// A pass stops once its copies reach 4 MiB: the first pass takes the
/// objects in order until their ledger chunks (two per object here) sum to
/// the budget, the next one the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pass_stops_at_its_byte_budget() {
    let oracle = Mutex::new(Oracle::new());
    let seed = |i: usize, r: usize| u64::try_from(i * 100 + r).unwrap();
    let ledger = |i| (0..12).map(|r| noise(seed(i, r), 30_000)).collect();
    let big = |i| (0..30).map(|r| noise(seed(i, 50 + r), 30_000)).collect();
    let (rig, _) = pinned_with("clean-budget-bytes", 15, &oracle, ledger, big).await;
    // Per object, in the pass's (object) order: its ledger chunks' bytes.
    let mut per_object = std::collections::BTreeMap::<ObjectId, u64>::new();
    for (e, _) in whole_chunks(&rig, LEDGER).await {
        *per_object.entry(e.object).or_default() += u64::from(e.chunk_len);
    }
    let lens: Vec<u64> = per_object.into_values().collect();
    assert_eq!(lens.len(), 15);
    let mut sum = 0;
    let first = 1 + lens
        .iter()
        .take_while(|&&l| {
            sum += l;
            sum < PASS_BYTES
        })
        .count();
    assert!(first < 15, "the budget binds: {lens:?}");
    let released = |r: CleanReport| usize::try_from(r.released).unwrap();
    assert_eq!(released(rig.engine.clean_pass().await.unwrap()), first);
    assert_eq!(released(rig.engine.clean_pass().await.unwrap()), 15 - first);
    rig.engine.begin_close();
}

/// A consolidation of the ledger's old chunks landing while the pass holds
/// its copy supersedes them: the copy's re-check finds its chunks no longer
/// live and abandons it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_consolidation_landing_mid_pass_abandons_the_copy() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-race-cons-mid", 8, &oracle).await;
    let old = whole_chunks(&rig, LEDGER).await;
    let hold = Hold::new();
    *rig.engine.gc_state().clean_hook.lock().unwrap() = Some(CleanHook::Hold(hold.clone()));
    let c = &rig.engine.layout.seal.fold.consolidator;
    let race = async {
        hold.wait_entered().await;
        let before = c.done.load(std::sync::atomic::Ordering::Relaxed);
        rig.engine.offer_consolidation(LEDGER, old);
        let t0 = std::time::Instant::now();
        while c.done.load(std::sync::atomic::Ordering::Relaxed) == before {
            assert!(t0.elapsed() < Duration::from_secs(10), "the job never ran");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        hold.release();
    };
    let (report, ()) = tokio::join!(rig.engine.clean_pass(), race);
    assert!(report.unwrap().abandoned, "its chunks were superseded");
    let orphans = unnamed(&rig, &["hseg"]).await;
    assert_eq!(orphans.len(), 1, "the abandoned copy");
    audit(&rig, &orphans, "after the abandoned copy").await;
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}

/// The copy's PUT never overwrites: an object already at the path its claim
/// names fails the pass and stays as it was; the next pass, on a new
/// claim, completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_copy_never_overwrites_an_object_at_its_path() {
    use object_store::ObjectStoreExt;
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-put-create", 3, &oracle).await;
    let seq = rig
        .engine
        .layout
        .seal
        .next_seq
        .load(std::sync::atomic::Ordering::Relaxed)
        + 1;
    let taken = ObjectId {
        epoch: rig.engine.writer_epoch,
        seq,
    };
    let path = object_store::path::Path::from(crate::spill::object::object_path(&rig.path, taken));
    let squatter = Bytes::from_static(b"not a segment object");
    rig.store.put(&path, squatter.clone().into()).await.unwrap();
    assert!(rig.engine.clean_pass().await.is_err(), "the PUT is refused");
    let body = rig.store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(body, squatter, "untouched");
    let report = rig.engine.clean_pass().await.unwrap();
    assert_eq!((report.released, report.abandoned), (3, false));
    read_back(&rig.engine, LEDGER, &ledger(&oracle), Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}

/// After a restart the candidates come back from the recount, which seeds
/// every partly dead row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_restart_finds_its_candidates_at_the_recount() {
    let oracle = Mutex::new(Oracle::new());
    let (rig, _) = pinned("clean-seed", 3, &oracle).await;
    let rig = reopen(rig).await;
    super::super::super::gc_tests::quiet(&rig.engine);
    rig.engine
        .gc_state()
        .configure(|s| s.clean_pays = half_dead);
    assert_eq!(rig.engine.gc_state().clean.len(), 0, "a new opening");
    rig.engine.recount_totals().await;
    assert_eq!(rig.engine.gc_state().clean.len(), 3);
    rig.engine.begin_close();
}
