#![cfg(test)]
//! The sealer against durable dispatch: a durable group that stays
//! pending (dispatch stalled) retires the engine through the sealer loop's
//! watchdog instead of leaving its writers waiting; a seal's step 1 waits
//! (at most a second) for dispatch to publish what its WAL flush made
//! durable; and a stream first applied during a seal keeps the shard's age
//! clock running after it.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use rand::{SeedableRng, rngs::StdRng};

use super::Trigger;
use super::test_hooks::Hold;
use super::tests::{append, hashes, manual, random_bytes, rig, settle};
use crate::shard::{ShardEngine, StreamHandle};

/// Manual settings for a test that parks dispatch on purpose: the
/// watchdog waits a minute rather than the test build's second.
fn parked_dispatch() -> super::SealSettings {
    super::SealSettings {
        dispatch_warn: Duration::from_secs(30),
        dispatch_stall: Duration::from_secs(60),
        ..manual()
    }
}

/// Wait (at most 10 s) until `handle`'s applied tail reached `next`.
async fn applied(handle: &StreamHandle, next: u64) {
    let t0 = Instant::now();
    while handle.state.lock().unwrap().applied.next < next {
        assert!(t0.elapsed() < Duration::from_secs(10), "never applied");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Wait (at most 20 s; the test build's dispatch stall is 1 s) until
/// `engine` closed.
async fn closed(engine: &ShardEngine) {
    let t0 = Instant::now();
    while !engine.is_closed() {
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "the engine kept a stalled dispatch"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_dispatch_that_leaves_a_durable_group_pending_retires_the_engine() {
    let rig = rig("dispatch-stall", manual(), Duration::from_millis(2)).await;
    rig.engine
        .layout
        .seal
        .hooks
        .skip_dispatch
        .store(true, Ordering::Relaxed);
    let mut rng = StdRng::seed_from_u64(0x57A1);
    let hash = hashes(&mut rng, 1)[0];
    let appended = AssertUnwindSafe(append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]));
    let (acked, ()) =
        futures_util::future::join(appended.catch_unwind(), closed(&rig.engine)).await;
    assert!(acked.is_err(), "acknowledged without a dispatch");
    assert_eq!(rig.engine.tasks.failure(), Some("dispatch"));
    assert_eq!(
        rig.engine
            .layout
            .seal
            .counters
            .dispatch_stall_warnings
            .load(Ordering::Relaxed),
        1,
        "the stall was warned about once before the engine retired"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_seal_waits_for_dispatch_to_publish_what_its_wal_flush_made_durable() {
    let rig = rig("catch-up", parked_dispatch(), Duration::from_millis(2)).await;
    let sp = &rig.engine.layout.seal;
    let mut rng = StdRng::seed_from_u64(0xCA7C);
    let hash = hashes(&mut rng, 1)[0];
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let hold = Hold::new();
    *sp.hooks.dispatch_hold.lock().unwrap() = Some(hold.clone());
    let appended = append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]);
    let drive = async {
        applied(&handle, 1).await;
        let t0 = Instant::now();
        let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
        let waited = t0.elapsed();
        *sp.hooks.dispatch_hold.lock().unwrap() = None;
        hold.release();
        (report, waited)
    };
    let (_, (report, waited)) = futures_util::future::join(appended, drive).await;
    assert!(
        waited >= Duration::from_millis(900),
        "step 1 waited its second for dispatch: {waited:?}"
    );
    assert_eq!(report.pages, 0, "dispatch never published the page");
    rig.engine.begin_close();
}

/// A stream first appended while a seal runs (applied and held, not yet
/// durable when the seal ends) keeps the shard's oldest-unsealed stamp, so
/// the age trigger and deadline still reach its pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_stream_applied_during_a_seal_keeps_the_age_clock_running() {
    let oldest = age_after_a_seal_with_a_stream_applied_during_it(false).await;
    assert!(oldest.is_some(), "B's unsealed page is aged");
}

/// **The negative control.** The same race, counting only durable pages as
/// left over: B's applied page loses the shard's age stamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn negative_control_durable_only_leftovers_drop_the_age_clock() {
    let oldest = age_after_a_seal_with_a_stream_applied_during_it(true).await;
    assert!(oldest.is_none(), "the control drops the age of B's page");
}

/// A seal of stream A while B's first append is applied but parked before
/// durable dispatch: the shard's age clock as the seal leaves it.
async fn age_after_a_seal_with_a_stream_applied_during_it(control: bool) -> Option<Duration> {
    let rig = rig("leftover-age", parked_dispatch(), Duration::from_millis(2)).await;
    let sp = &rig.engine.layout.seal;
    sp.hooks
        .durable_only_leftover
        .store(control, Ordering::Relaxed);
    let mut rng = StdRng::seed_from_u64(0xA6E);
    let [a, b] = hashes(&mut rng, 2)[..] else {
        unreachable!()
    };
    append(&rig.engine, a, vec![random_bytes(&mut rng, 300)]).await;
    settle(&rig.engine).await;
    let a_handle = rig.engine.stream_handle(a).await.unwrap();
    let b_handle = rig.engine.stream_handle(b).await.unwrap();
    let flush = sp.flush_lock.lock().await;
    let hold = Hold::new();
    let seal = async {
        let report = rig.engine.seal_once(Trigger::Requested).await;
        // The age clock as the seal left it, before B's group is durable.
        let oldest = rig.engine.seal_measure().oldest;
        *sp.hooks.dispatch_hold.lock().unwrap() = None;
        hold.release();
        (report, oldest)
    };
    // B is appended only once dispatch is parked, so its group is applied
    // and held but not durable-published when the seal ends.
    let go = tokio::sync::Notify::new();
    let record = random_bytes(&mut rng, 300);
    let appended = async {
        go.notified().await;
        append(&rig.engine, b, vec![record]).await
    };
    let drive = async {
        let t0 = Instant::now();
        while a_handle.state.lock().unwrap().durable.absorbed < 1 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "A's seal never durable"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        *sp.hooks.dispatch_hold.lock().unwrap() = Some(hold.clone());
        go.notify_one();
        applied(&b_handle, 1).await;
        drop(flush);
    };
    let ((report, oldest), (), _) = futures_util::future::join3(seal, drive, appended).await;
    assert_eq!(report.unwrap().pages, 1, "the seal took A's page");
    rig.engine.begin_close();
    oldest
}

/// A quiet engine (nothing pending) outlives the dispatch stall: the
/// watchdog retires only for a durable group left undispatched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiet_engine_outlives_the_dispatch_stall() {
    let rig = rig("quiet", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x9E7);
    let hash = hashes(&mut rng, 1)[0];
    append(&rig.engine, hash, vec![random_bytes(&mut rng, 300)]).await;
    let stall = rig.engine.layout.seal.settings().dispatch_stall;
    tokio::time::sleep(stall + Duration::from_secs(1)).await;
    assert!(!rig.engine.is_closed(), "a quiet engine was retired");
    assert_eq!(
        rig.engine
            .layout
            .seal
            .counters
            .dispatch_stall_warnings
            .load(Ordering::Relaxed),
        0
    );
    rig.engine.begin_close();
}
