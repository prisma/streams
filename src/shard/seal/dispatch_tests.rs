#![cfg(test)]
//! The sealer against durable dispatch: a durable group that stays
//! pending (dispatch stalled) retires the engine through the sealer loop's
//! watchdog instead of leaving its writers waiting, and a seal's step 1
//! waits (at most a second) for dispatch to publish what its WAL flush
//! made durable.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use rand::{SeedableRng, rngs::StdRng};

use super::Trigger;
use super::test_hooks::Hold;
use super::tests::{append, hashes, manual, random_bytes, rig};
use crate::shard::{ShardEngine, StreamHandle};

/// Wait (at most 10 s) until `handle`'s applied tail reached `next`.
async fn applied(handle: &StreamHandle, next: u64) {
    let t0 = Instant::now();
    while handle.state.lock().unwrap().applied.next < next {
        assert!(t0.elapsed() < Duration::from_secs(10), "never applied");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Wait (at most 20 s: the test build's 5 s dispatch stall and the loop's
/// tick) until `engine` closed.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_waits_for_dispatch_to_publish_what_its_wal_flush_made_durable() {
    let rig = rig("catch-up", manual(), Duration::from_millis(2)).await;
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
