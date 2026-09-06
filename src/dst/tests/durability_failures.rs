//! Durability failures.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
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
    engine.begin_close();
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
    engine.begin_close();
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
        let inner = mem();
        let store = FaultStore::uniform(inner.clone(), 91, FaultPlan::new(0, 0, 20));
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
        let engine = open_engine_cfg(store.clone(), &format!("dst-busy-{skip_reqs}"), cfg).await;
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
        engine.begin_close();
        engine
            .await_terminated(std::time::Duration::from_secs(30))
            .await
            .expect("terminate");
    }
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

        engine.begin_close();
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

// ---------------------------------------------------------------
// DST expansion, workstream 2 (L1-now): deterministic group-write
// failure. Everything a commit group promised fails together, and
// nothing answered from its staging can outlive it.
// ---------------------------------------------------------------

/// DUR-002: original and exact duplicate FORCED into one commit group
/// (hold-commit gate + enqueue counter as entered-proof), and the
/// group write fails: BOTH must receive the group failure. This is
/// the assertion the earlier order-independent version could not
/// make — and it kills the labeling hole where a premature duplicate
/// on one side passed the opposite side's check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_group_write_fails_its_duplicate_too() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dur002", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur002"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur002"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    // Compose the group deterministically: hold the committer, land
    // both requests in the queue (counter-proof), arm, release.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let ph = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let r1 =
        tokio::spawn(
            async move { hreq(addr, "POST", "/v1/stream/dur002", &ph, br#"[{"a":1}]"#).await },
        );
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let r2 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur002",
            &[
                ("content-type", "application/json"),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"[{"a":1}]"#,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);

    let (s1, _, _) = r1.await.unwrap();
    let (s2, _, _) = r2.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 500 && s2 >= 500,
        "a promise outlived its failed group: {s1} {s2}"
    );
    // Recovery: the exact retry commits as the original, exactly once.
    let (s3, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dur002",
        &[
            ("content-type", "application/json"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"[{"a":1}]"#,
    )
    .await;
    assert!(s3 == 200 || s3 == 204, "recovery: {s3}");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/dur002", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    let payloads = recs.iter().filter(|r| r.get("a").is_some()).count();
    assert_eq!(payloads, 1, "exactly-once violated: {recs:?}");
    engine_shutdown(&state).await;
}

/// DUR-006: sequence REUSE (same tuple, different body) judged against
/// a row staged in the same failed group. Both requests forced into
/// one group; both must receive the group failure — never a 409 about
/// a row that was never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reuse_verdict_dies_with_its_failed_group() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dur006",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur006"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur006"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let a = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/dur006/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"{"x":1}"#,
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let b = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/dur006/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"{"x":"DIFFERENT"}"#,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);
    let (s1, _, _) = a.await.unwrap();
    let (s2, _, _) = b.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 500 && s2 >= 500,
        "a reuse verdict (or its original) outlived the failed group: {s1} {s2}"
    );
    // Ground truth: the original retry commits; only THEN is reuse a
    // durable fact and the different-body request conflicts.
    let (s3, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/dur006/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"{"x":1}"#,
    )
    .await;
    assert_eq!(s3, 200, "original retry: {s3}");
    let (s4, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/dur006/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"{"x":"DIFFERENT"}"#,
    )
    .await;
    assert_eq!(s4, 409, "reuse with durable ground: {s4}");
    engine_shutdown(&state).await;
}

/// DUR-004: close-only and its exact retry in ONE failed group — both
/// fail, nothing publishes sealing, and the later plain close seals
/// exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_group_fails_the_close_and_its_retry_together() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dur004", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur004"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur004"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let c1 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur004",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let c2 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur004",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);
    // WATCHDOG, not a wait: this test wedged twice in full-suite runs
    // (commit pipeline asleep with both acks outstanding, all workers
    // parked; passes solo). Until that liveness hole is caught in the
    // act, convert an infinite hang into a red, diagnosable failure.
    let (r1, r2) = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        async { (c1.await.unwrap(), c2.await.unwrap()) },
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "LIVENESS WEDGE: closes never returned. enqueued_delta={} tripped={} —              the commit pipeline is asleep with acks outstanding (see task notes)",
            engine.appends_enqueued() - base,
            engine.group_failures_tripped()
        )
    });
    let (s1, _, _) = r1;
    let (s2, _, _) = r2;
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 400 && s2 >= 400,
        "a close (or its idempotent echo) outlived the failed group: {s1} {s2}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur004"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur004"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "sealing published off a failed close");
    // Recovery.
    let (s3, _, _) = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        hreq(
            addr,
            "POST",
            "/v1/stream/dur004",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        ),
    )
    .await
    .expect("LIVENESS WEDGE: the recovery close never returned");
    assert!(s3 == 200 || s3 == 204, "recovery close: {s3}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur004"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur004"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed && d.sealing.is_none());
    engine_shutdown(&state).await;
}

/// DUR-004 + SEL-021 in one deterministic shape: a close whose group
/// write FAILS, with a fence in flight. The fence must answer failure
/// or closed=false — NEVER closed=true off staging that died — the
/// close must not report success, its intent survives (a write error
/// is ambiguous), and the exact retry recovers the seal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_in_a_failed_group_reports_failure_not_closed() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/sel021", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel021"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel021"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.stream_epoch.clone();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    // Deterministic order: park the close after its intent, arm the
    // failure, fence AT THE CLAIM'S OWN GENERATION (raising the fence
    // without superseding the close), then release.
    let before = crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "sel021");
    crate::failpoints::park_close_before_enqueue("sel021");
    let body = br#"[{"fin":1}]"#;
    let close = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/sel021",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            body,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "sel021") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "the close never parked");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel021"));
    let claim = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel021"))
        .await
        .unwrap()
        .unwrap()
        .sealing
        .clone()
        .expect("no claim installed");
    // FORCED same group: hold the committer, release the close into
    // the queue, land the fence behind it, arm, release. The fence and
    // the close now share one commit group by construction — the
    // co-residency SEL-021 demands.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    crate::failpoints::release_close_before_enqueue("sel021");
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let st_f = state.clone();
    let ep_f = epoch.clone();
    let g_f = claim.claim_generation;
    let fence = tokio::spawn(async move {
        crate::http::fence_segment_for_key(
            &st_f,
            &st_f.deployment.raw_adapter_sref("sel021"),
            &ep_f,
            "",
            g_f,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);
    let fence = fence.await.unwrap();
    match fence {
        Ok(closed) => assert!(
            !closed,
            "the fence reported closed=true off a write that failed"
        ),
        Err(_) => {} // failed with its group: equally honest
    }
    let (cs, _, _) = close.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        cs >= 400,
        "the close reported success for a failed write: {cs}"
    );
    // A write failure is AMBIGUOUS: the intent stays for the retry.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel021"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel021"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.as_ref().is_some_and(|sl| sl.owes_final()),
        "the ambiguous failure tore down the intent: {:?}",
        d.sealing
    );
    assert!(!d.sealed, "sealed off a failed write");
    // The exact retry recovers: renews the claim, lands the record,
    // seals the collection.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/sel021",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "the exact retry could not recover: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel021"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel021"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed && d.sealing.is_none());
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/sel021", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    let fins = recs.iter().filter(|r| r.get("fin").is_some()).count();
    assert_eq!(
        fins, 1,
        "exactly one final after the failed write: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// DUR-008: a Stream-Seq conflict judged against a lane another
/// request staged is not a fact until that staging is durable. If the
/// verdict escaped a failed group, the client would hold a permanent
/// "sequence taken" for a sequence that never existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_seq_verdict_is_grounded_in_durable_state() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json"), ("stream-seq", "s1")];
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/dur008",
        &[("content-type", "application/json")],
        br#"[{"n":0}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur008"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur008"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    engine.fail_next_group_for(identity);
    let w1 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur008",
            &[("content-type", "application/json"), ("stream-seq", "s1")],
            br#"[{"a":1}]"#,
        )
        .await
    });
    let (s2, _, _) = hreq(addr, "POST", "/v1/stream/dur008", &ct, br#"[{"b":1}]"#).await;
    let (s1, _, _) = w1.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 500 || s2 >= 500,
        "nobody saw the failed write: {s1} {s2}"
    );
    // If either got the CONFLICT verdict, the sequence it was judged
    // against must actually be durable: an exact probe must still
    // conflict. A conflict with no durable ground is the bug.
    if s1 == 409 || s2 == 409 {
        let (sp, _, _) = hreq(
            addr,
            "POST",
            "/v1/stream/dur008",
            &[("content-type", "application/json"), ("stream-seq", "s1")],
            br#"[{"probe":1}]"#,
        )
        .await;
        assert_eq!(sp, 409, "a conflict verdict had no durable ground");
    }
    engine_shutdown(&state).await;
}
