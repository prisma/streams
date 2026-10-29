#![cfg(test)]
//! A seal's budget (layout 6): one seal takes at most `max_object_bytes`
//! of pages and `max_streams` streams. A stream larger than what is left
//! seals a prefix within the budget; the streams a seal leaves wait for the
//! next one, which starts after the last stream taken (so none starves)
//! and follows at the loop's next evaluation whatever the triggers say.
//! Successors of the absorber's gather-budget scenarios (HIS-004, HIS-005,
//! HIS-006, HIS-012).

use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use rand::{SeedableRng, rngs::StdRng};

use super::Trigger;
use super::tests::{
    Oracle, append_recorded, check_stream, directory_rows, hashes, manual, random_bytes, read_back,
    rig, settle,
};
use crate::shard::Deliver;

/// An oversized stream seals a prefix of its pages within the object
/// budget, seal after seal, until it is all sealed; every seal stays within
/// the budget, the directory and the pending row tile at every step, and
/// every record reads back once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_stream_seals_within_the_object_budget() {
    const BUDGET: u64 = 20 << 10;
    let mut settings = manual();
    settings.max_object_bytes = BUDGET;
    let rig = rig("budget-bytes", settings, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xB0D6);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..15 {
        append_recorded(
            &rig.engine,
            &oracle,
            hash,
            vec![random_bytes(&mut rng, 4000)],
        )
        .await;
    }
    let mut seals = 0;
    loop {
        let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
        settle(&rig.engine).await;
        let c = check_stream(&rig, hash, &directory_rows(&rig).await).await;
        if report.pages == 0 {
            assert_eq!(c.absorbed, c.next, "nothing left");
            break;
        }
        seals += 1;
        assert!(
            report.page_bytes <= BUDGET,
            "seal {seals}: {} B over the {BUDGET} B budget",
            report.page_bytes
        );
        assert!(seals <= 15, "the stream never finished sealing");
    }
    assert!(
        seals >= 3,
        "about 60 KiB in 20 KiB seals takes at least three: {seals}"
    );
    let oracle = oracle.into_inner().unwrap();
    read_back(&rig.engine, hash, &oracle[&hash], Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}

/// Eight streams under a budget of three streams per seal: each seal takes
/// three, starting after the last one the seal before took, so three seals
/// seal every stream once (3, 3, 2) and none is left behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_streams_seal_under_the_streams_budget_in_turn() {
    let mut settings = manual();
    settings.max_streams = 3;
    let rig = rig("budget-streams", settings, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xB0D7);
    let streams = hashes(&mut rng, 8);
    let oracle = Mutex::new(Oracle::new());
    for hash in &streams {
        append_recorded(
            &rig.engine,
            &oracle,
            *hash,
            vec![random_bytes(&mut rng, 500)],
        )
        .await;
    }
    let mut taken = Vec::new();
    for _ in 0..3 {
        let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
        settle(&rig.engine).await;
        taken.push(report.streams);
    }
    assert_eq!(taken, vec![3, 3, 2], "three streams per seal, in turn");
    let rows = directory_rows(&rig).await;
    for hash in &streams {
        let c = check_stream(&rig, *hash, &rows).await;
        assert_eq!(c.absorbed, c.next, "every stream sealed once");
        assert_eq!(c.chunks, 1);
    }
    rig.engine.begin_close();
}

/// A seal its budget cut short leaves the rest to the next seal, which the
/// loop runs at its next evaluation with no trigger due: one requested seal
/// of five streams under a two-stream budget, and the sealer finishes the
/// other three by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_its_budget_cut_short_completes_at_the_next_evaluation() {
    let mut settings = manual();
    settings.max_streams = 2;
    settings.tick = Duration::from_millis(20);
    let rig = rig("budget-deferred", settings, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xB0D8);
    let streams = hashes(&mut rng, 5);
    let oracle = Mutex::new(Oracle::new());
    for hash in &streams {
        append_recorded(
            &rig.engine,
            &oracle,
            *hash,
            vec![random_bytes(&mut rng, 500)],
        )
        .await;
    }
    // Triggers that never fire (defaults: 6 MiB, 24 MiB, 48 WAL objects,
    // 45 s), the loop running.
    rig.engine.configure_seal(|s| s.auto = true);
    let report = rig.engine.seal_requested().await.unwrap();
    assert_eq!(report.streams, 2, "the budget cut the seal short");
    // Wait on the shard ledger alone: the exact checks read the stored rows
    // and the in-memory directory, which a running seal could move between.
    let t0 = Instant::now();
    let seals = || {
        rig.engine
            .layout
            .seal
            .counters
            .seals
            .load(Ordering::Relaxed)
    };
    while rig.engine.maintenance_snapshot().unabsorbed_frame_bytes > 0 || seals() < 3 {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "the deferred streams were never sealed (seals {})",
            seals()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    rig.engine.configure_seal(|s| s.auto = false);
    settle(&rig.engine).await;
    let rows = directory_rows(&rig).await;
    for hash in &streams {
        let c = check_stream(&rig, *hash, &rows).await;
        assert_eq!(c.absorbed, c.next, "every stream sealed");
    }
    let oracle = oracle.into_inner().unwrap();
    for hash in &streams {
        read_back(&rig.engine, *hash, &oracle[hash], Deliver::Durable)
            .await
            .unwrap();
    }
    rig.engine.begin_close();
}

/// A page that fits what is left of the budget exactly is taken; a stream
/// the budget cuts short defers the rest to the next seal, which starts
/// after it, and leaves the shard's oldest unsealed page counted; a seal
/// that takes everything defers nothing and leaves no age behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exact_fit_is_taken_and_a_cut_defers_the_rest() {
    let rig = rig("budget-exact", manual(), Duration::from_millis(2)).await;
    let sp = &rig.engine.layout.seal;
    let mut rng = StdRng::seed_from_u64(0xE8AC);
    let hash = hashes(&mut rng, 1)[0];
    for len in [300, 500, 700] {
        super::tests::append(&rig.engine, hash, vec![random_bytes(&mut rng, len)]).await;
    }
    settle(&rig.engine).await;
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let sizes: Vec<u64> = handle
        .state
        .lock()
        .unwrap()
        .pending
        .from(0)
        .map(|(_, _, raw)| raw.len() as u64)
        .collect();
    assert_eq!(sizes.len(), 3, "one page per append");
    rig.engine
        .configure_seal(|s| s.max_object_bytes = sizes[0] + sizes[1] - 1);
    let cut = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!(cut.pages, 1, "the second page does not fit");
    assert!(
        sp.deferred.load(Ordering::Acquire),
        "the cut defers the rest"
    );
    assert_eq!(*sp.cursor.lock().unwrap(), Some(hash));
    assert!(
        rig.engine.seal_measure().oldest.is_some(),
        "the rest is still unsealed"
    );
    sp.deferred.store(false, Ordering::Release);
    rig.engine
        .configure_seal(|s| s.max_object_bytes = sizes[1] + sizes[2]);
    let exact = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!(exact.pages, 2, "both pages fit the budget exactly");
    assert!(!sp.deferred.load(Ordering::Acquire), "nothing deferred");
    assert_eq!(*sp.cursor.lock().unwrap(), None);
    assert_eq!(rig.engine.seal_measure().oldest, None, "nothing unsealed");
    rig.engine.begin_close();
}

/// A seal's window holds a stream's pages from its boundary that tile up to
/// the durable frontier: never a page past it (applied, not yet durable),
/// never a page after a gap.
#[test]
fn a_window_takes_only_tiling_pages_below_the_durable_frontier() {
    let pending = |offsets: &[u64]| {
        let mut p = crate::spill::pending::PendingPages::default();
        for &o in offsets {
            p.push((o, o, bytes::Bytes::from_static(b"page")));
        }
        p
    };
    let window = |p: &crate::spill::pending::PendingPages, upto| {
        let (pages, due) = super::steps::durable_pages(p, 0, upto);
        (
            pages.iter().map(|(first, _, _)| *first).collect::<Vec<_>>(),
            due,
        )
    };
    let tiled = pending(&[0, 1, 2, 3, 4]);
    assert_eq!(
        window(&tiled, 3),
        (vec![0, 1, 2], 3),
        "the durable frontier"
    );
    assert_eq!(window(&tiled, 5), (vec![0, 1, 2, 3, 4], 5));
    let gap = pending(&[0, 1, 3, 4]);
    assert_eq!(window(&gap, 5), (vec![0, 1], 2), "no page after a gap");
}
