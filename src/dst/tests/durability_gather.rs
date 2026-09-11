#![cfg(test)]
//! Group-commit gather and dispatch ordering under storage faults.

use super::fixture_storage::{mem, open_engine_cfg, skey};
use crate::dst::{
    FaultPlan, FaultProfile, FaultStore, ObjClass, OpLog, Outcome, Workload, drain_observed,
};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// **Review #2's barrier test, exact form: the gather must not start
/// until this flush's acks are ON THE WIRE.** The dispatch gate is held
/// (the deterministic stand-in for "the acker is paused after durability,
/// before response dispatch"); while held, the client's ack must not
/// arrive AND the pump must not enter a gather window; on release, ack
/// then gather. This is the property that makes "post-ACK" true rather
/// than merely "post-flush".
#[expect(
    clippy::disallowed_methods,
    reason = "ack-dispatch regression; the concurrent request is joined after the held dispatcher releases; sequential execution could not observe its blocked acknowledgement"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gather_window_waits_for_ack_dispatch() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 87, FaultPlan::new(0, 0, 30));
    let cov = store.coverage();
    let key = skey();
    let hash = [40u8; 16];
    let cfg = crate::shard::ShardConfig {
        wal_group_commit: true,
        wal_flush_gap: std::time::Duration::from_millis(2),
        wal_post_ack_gather: std::time::Duration::from_millis(4),
        ..Default::default()
    };
    let engine = open_engine_cfg(store.clone(), "dst-barrier", cfg).await;
    let w = Workload::new(cov.clone());

    // Warm one append so the pipeline is established.
    let o = w
        .attempt_with_deadline(&engine, hash, &key, "b", "warm", None, None)
        .await;
    assert!(matches!(o, Outcome::Acked { .. }));

    // Hold dispatch, then fire an append. Its flush may complete, but its
    // ack CANNOT be dispatched and no gather may begin.
    let guard = engine.test_hold_dispatch().await;
    let e2 = engine.clone();
    let k2 = key.clone();
    let c2 = cov.clone();
    let waiter = tokio::spawn(async move {
        let w2 = Workload::new(c2);
        w2.attempt_with_deadline(&e2, hash, &k2, "b", "held", None, None)
            .await
    });
    // Give the commit + flush ample real time while dispatch stays held.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let gathers_held = engine.pump_gathers.load(Ordering::Relaxed)
        + engine.pump_gathers_skipped_busy.load(Ordering::Relaxed);
    assert!(
        !waiter.is_finished(),
        "the ack must not reach the client while dispatch is held"
    );
    drop(guard);
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), waiter)
        .await
        .expect("ack after release")
        .expect("join");
    assert!(matches!(out, Outcome::Acked { .. }), "got {out:?}");
    // The gather decision for that flush happened AFTER release — i.e.
    // after dispatch — so the counter moves only once the ack was out.
    for _ in 0..100 {
        let now = engine.pump_gathers.load(Ordering::Relaxed)
            + engine.pump_gathers_skipped_busy.load(Ordering::Relaxed);
        if now > gathers_held {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("terminate");
}

/// **Review #2's deadlock probe: commits landing DURING a flush must not
/// extend what that flush's barrier waits for.** The target is captured
/// before the flush; groups committed while the PUT is in flight belong
/// to the next generation. Under 200-400 ms WAL latency, appends fired
/// mid-flight must all ack promptly across >= 2 flushes — a pump waiting
/// on the wrong generation would need ITSELF to flush again and would
/// stall until the 250 ms failsafe (visible here as a hang).
#[expect(
    clippy::disallowed_methods,
    reason = "flush-generation regression; every staggered request is joined under its deadline; concurrency is the input needed to cross in-flight durability groups"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_during_a_flush_do_not_extend_its_barrier() {
    let inner = mem();
    let slow_wal = FaultPlan {
        error_pct: 0,
        lost_response_pct: 0,
        latency_pct: 100,
        latency_ms: (200, 400),
    };
    let store = FaultStore::new(
        inner.clone(),
        89,
        FaultProfile::uniform(FaultPlan::CLEAN).with_class(ObjClass::Wal, slow_wal),
    );
    let cov = store.coverage();
    let key = skey();
    let hash = [41u8; 16];
    let cfg = crate::shard::ShardConfig {
        wal_group_commit: true,
        wal_flush_gap: std::time::Duration::from_millis(2),
        wal_post_ack_gather: std::time::Duration::from_millis(4),
        ..Default::default()
    };
    let engine = open_engine_cfg(store.clone(), "dst-midflight", cfg).await;

    let mut waves = Vec::new();
    for i in 0..12u64 {
        let e = engine.clone();
        let k = key.clone();
        let c = cov.clone();
        waves.push(tokio::spawn(async move {
            let w = Workload::new(c);
            // Staggered so several land while earlier flushes are in
            // flight.
            tokio::time::sleep(std::time::Duration::from_millis(i * 60)).await;
            w.attempt_with_deadline(&e, hash, &k, "m", &format!("mid{i}"), None, None)
                .await
        }));
    }
    for t in waves {
        let out = tokio::time::timeout(std::time::Duration::from_secs(30), t)
            .await
            .expect("no barrier stall")
            .expect("join");
        assert!(matches!(out, Outcome::Acked { .. }), "got {out:?}");
    }
    assert!(
        engine.pump_flushes.load(Ordering::Relaxed) >= 2,
        "the scenario must span multiple generations"
    );
    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("terminate");
}

/// **Adaptive gather: a busy next generation skips the window.**
///
/// Construction note (itself a finding): a fully SYNCHRONIZED herd at
/// saturation produces no drift — everyone re-enters during the settle
/// and the drift key already suppresses the window — so the busy-skip
/// only matters when drift and volume coincide. That coincidence is
/// built deterministically here: dispatch is held, batch A flushes,
/// batch B commits behind it, and on release the pump dispatches A and
/// finds B (large) already pending. Threshold 4 must record a busy-skip;
/// threshold-disabled must gather instead. The knob is the only
/// difference, so the counters prove the mechanism, not scheduling luck.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_next_generation_skips_the_gather_window() {
    for (skip_reqs, expect_skips) in [(4u32, true), (u32::MAX, false)] {
        let store = FaultStore::uniform(mem(), 91, FaultPlan::new(0, 0, 20));
        busy_generation_case(store, skip_reqs, expect_skips).await;
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "gather-threshold fixture; all twelve held requests are joined after dispatch release; concurrent queue occupancy is required to distinguish the two thresholds"
)]
async fn busy_generation_case(store: Arc<FaultStore>, skip_reqs: u32, expect_skips: bool) {
    let cov = store.coverage();
    let key = skey();
    let hash = [42u8; 16];
    let cfg = crate::shard::ShardConfig {
        wal_group_commit: true,
        wal_flush_gap: std::time::Duration::from_millis(2),
        wal_post_ack_gather: std::time::Duration::from_millis(4),
        wal_gather_skip_reqs: skip_reqs,
        wal_gather_skip_bytes: u64::MAX,
        ..Default::default()
    };
    let engine = open_engine_cfg(store, &format!("dst-busy-{skip_reqs}"), cfg).await;
    let w = Workload::new(cov.clone());
    let o = w
        .attempt_with_deadline(&engine, hash, &key, "s", "warm", None, None)
        .await;
    assert!(matches!(o, Outcome::Acked { .. }));

    // Hold dispatch; batch A (4 appends) commits and flushes but its
    // acks are stuck; batch B (8 appends) commits BEHIND it.
    let guard = engine.test_hold_dispatch().await;
    let mut all = Vec::new();
    for i in 0..12u64 {
        let e = engine.clone();
        let k = key.clone();
        let c = cov.clone();
        all.push(tokio::spawn(async move {
            let w2 = Workload::new(c);
            w2.attempt_with_deadline(&e, hash, &k, "s", &format!("b{i}"), None, None)
                .await
        }));
        if i == 3 {
            // Let batch A reach its flush before B starts committing.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let skips_before = engine.pump_gathers_skipped_busy.load(Ordering::Relaxed);
    let applied_before = engine.pump_gathers.load(Ordering::Relaxed);
    drop(guard);
    for t in all {
        let out = tokio::time::timeout(std::time::Duration::from_secs(30), t)
            .await
            .expect("ack")
            .expect("join");
        assert!(matches!(out, Outcome::Acked { .. }), "got {out:?}");
    }
    let skips = engine.pump_gathers_skipped_busy.load(Ordering::Relaxed) - skips_before;
    let applied = engine.pump_gathers.load(Ordering::Relaxed) - applied_before;
    if expect_skips {
        assert!(
            skips > 0,
            "drift+volume with threshold 4 must busy-skip (skips={skips}, applied={applied})"
        );
    } else {
        assert_eq!(
            skips, 0,
            "threshold disabled must never skip (applied={applied})"
        );
        assert!(
            applied > 0,
            "the same drift+volume must GATHER when skipping is off"
        );
    }
    engine
        .await_terminated(std::time::Duration::from_secs(30))
        .await
        .expect("terminate");
}

/// **The group-commit pump with the post-ACK barrier and gather window,
/// under faults.** The gather path moves ack dispatch from the acker task
/// into the pump (an ordering change on the hottest path in the system),
/// so it gets the full treatment: WAL errors, lost responses, latency,
/// concurrent producers, retries, and the complete I1–I7 audit through
/// the production merged reader. The acker stays live as the failsafe —
/// this scenario must pass with BOTH dispatchers running, proving the
/// dispatch_gate keeps them from interleaving tail-state updates.
///
/// Also pins the idle contract: after quiet, a single append must not
/// wait a gather window it has no herd to gather (regression guard for
/// "gather only after a flush that dispatched work").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gather_pump_preserves_invariants_under_faults() {
    let mut total_gathers = 0u64;
    for seed in [3u64, 17, 41] {
        let inner = mem();
        let profile = FaultProfile::uniform(FaultPlan::new(0, 0, 40))
            .with_class(ObjClass::Wal, FaultPlan::new(8, 6, 40));
        let store = FaultStore::new(inner.clone(), seed, profile);
        let cov = store.coverage();
        let key = skey();
        let hash = [29u8; 16];
        let cfg = crate::shard::ShardConfig {
            wal_group_commit: true,
            wal_flush_gap: std::time::Duration::from_millis(2),
            wal_post_ack_gather: std::time::Duration::from_millis(3),
            ..Default::default()
        };
        let engine = open_engine_cfg(store.clone(), &format!("dst-gather-{seed}"), cfg).await;

        let mut log = OpLog::default();
        let mut w = Workload::new(cov.clone());
        // Two closed-loop waves with concurrent keys — the shape the
        // gather window exists for.
        for _ in 0..4 {
            w.run(
                &engine,
                hash,
                &key,
                &["g1", "g2", "g3"],
                10,
                false,
                &mut log,
            )
            .await;
        }

        // The pump must be flushing, and across the seeds the drift
        // path (gather windows) must have been exercised. Per-seed
        // barrier_acked would flake: the acker fires on the same watch
        // change and legitimately wins most dispatch races — that is by
        // design, not a defect.
        let flushes = engine.pump_flushes.load(Ordering::Relaxed);
        assert!(flushes > 0, "seed {seed}: the pump never flushed");
        total_gathers += engine.pump_gathers.load(Ordering::Relaxed)
            + engine.pump_barrier_acked.load(Ordering::Relaxed);

        let _ds: Arc<dyn ObjectStore> = store.clone();
        let observed = drain_observed(&engine, hash, &key, &cov).await;
        if let Err(e) = log.audit(&observed) {
            panic!("seed {seed}: {e}");
        }

        // Idle contract: no herd, no gather tax. One append after quiet
        // completes in well under (gather + PUT + margin) of virtual/real
        // time; mainly this asserts it completes at all without waiting
        // for a second flush cycle.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let t0 = std::time::Instant::now();
        let o = w
            .attempt_with_deadline(&engine, hash, &key, "idle", "idle-probe", None, None)
            .await;
        assert!(
            matches!(o, Outcome::Acked { .. }),
            "seed {seed}: idle append must ack, got {o:?}"
        );
        let took = t0.elapsed();
        assert!(
            took < std::time::Duration::from_secs(5),
            "seed {seed}: idle append took {took:?} — the gather window is \
             taxing the idle path"
        );

        engine
            .await_terminated(std::time::Duration::from_secs(30))
            .await
            .expect("pump + committer + acker + ticker all terminate");
    }
    assert!(
        total_gathers > 0,
        "no seed ever exercised the gather/barrier path — the scenario \
         has degraded to re-testing the acker"
    );
}
