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
