#![cfg(test)]

use super::{
    IDLE_EVICT_MS, MAX_TRACKED_PROJECTS, ProjectId, ProjectQuotas, QuotaRefusal, QuotaRegistry,
};

fn q(rps: u64, inflight: u64) -> ProjectQuotas {
    ProjectQuotas {
        requests_per_sec: rps,
        max_inflight_requests: inflight,
        ..Default::default()
    }
}

fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).unwrap()
}

/// Workload-cert W1xW2 shape (bench/WORKLOAD-CERT-PLAN.md): 10,000
/// resident tenants, 100 active per 5s window rotating over the
/// whole population = 20 first-seen projects/s sustained. With
/// IDLE_EVICT_MS of un-evictable recency, steady-state demand is
/// 20/s x 300s = 6,000 tracked entries — the cap must hold the
/// certified tenant population, not just its active window.
#[test]
fn cert_rotation_over_ten_thousand_tenants_never_hits_tracker_capacity() {
    let r = QuotaRegistry::default();
    let quotas = ProjectQuotas::default();
    let t0: i64 = 1_000_000;
    let name = |i: usize| pid(&format!("cert_{i}"));
    let mut refused = 0usize;
    // 10,000 distinct projects, 20 new per second (cert pacing),
    // each holding its guard only for the request instant.
    for i in 0..10_000usize {
        let now = t0 + (i as i64) * 50; // 20/s
        match r.admit(&name(i), &quotas, now) {
            Ok(_g) => {}
            Err(QuotaRefusal::TrackerCapacity) => refused += 1,
            Err(e) => panic!("unexpected refusal {e:?}"),
        }
    }
    assert_eq!(
        refused, 0,
        "the certification rotation must never see TrackerCapacity"
    );
}

/// SR-6 tracker-capacity churn: at the cap the tracker refuses
/// NEW projects with the typed refusal while every entry is
/// recent, evicts the idle mass (never a project with live
/// guards) once the horizon passes, and re-tracks returning
/// projects — a tracker that filled once must not refuse until
/// restart, and must never orphan an entry whose guards are held.
#[test]
fn tracker_capacity_churn_evicts_idle_never_active() {
    let r = QuotaRegistry::default();
    let quotas = q(0, 2);
    let t0: i64 = 1_000_000;
    let name = |i: usize| pid(&format!("churn_{i}"));
    // Pin churn_0 with BOTH its concurrency slots held live.
    let _g0 = r.admit(&name(0), &quotas, t0).expect("pin 1");
    let _g1 = r.admit(&name(0), &quotas, t0).expect("pin 2");
    for i in 1..MAX_TRACKED_PROJECTS {
        r.admit(&name(i), &quotas, t0).expect("fill");
    }
    // At capacity with every entry recent: typed refusal —
    // track-or-refuse, never merge into a stranger's buckets.
    match r.admit(&name(MAX_TRACKED_PROJECTS), &quotas, t0 + 1_000) {
        Err(QuotaRefusal::TrackerCapacity) => {}
        Ok(_) => panic!("expected TrackerCapacity, got admission"),
        Err(e) => panic!("expected TrackerCapacity, got {e:?}"),
    }
    // Past the idle horizon the idle mass is evictable: thousands
    // of NEW projects admit (the first triggers the sweep).
    let t1 = t0 + IDLE_EVICT_MS + 1_000;
    for i in 0..2_000 {
        r.admit(&name(MAX_TRACKED_PROJECTS + 1 + i), &quotas, t1)
            .expect("churn admit");
    }
    // churn_0 sat idle far past the horizon through the sweep, but
    // its guards are live: retention is observable as its inflight
    // count — the third admit refuses on concurrency. (Had the
    // sweep orphaned it, a FRESH entry with inflight=0 would have
    // admitted here.)
    match r.admit(&name(0), &quotas, t1) {
        Err(QuotaRefusal::Concurrency) => {}
        Ok(_) => panic!("pinned project was evicted (fresh entry admitted)"),
        Err(e) => panic!("pinned project was evicted: {e:?}"),
    }
    // An idle-evicted project simply re-tracks on return (full
    // burst — documented, an idle project lost no debt worth
    // keeping at this horizon).
    r.admit(&name(1), &quotas, t1)
        .expect("evicted project returns");
}

#[test]
fn rate_bucket_admits_burst_then_refuses_then_refills() {
    let r = QuotaRegistry::default();
    let p = pid("proj_a");
    let quotas = q(2, 0);
    assert!(r.admit(&p, &quotas, 1_000).is_ok());
    assert!(r.admit(&p, &quotas, 1_000).is_ok());
    match r.admit(&p, &quotas, 1_000) {
        Err(QuotaRefusal::Rate { retry_after_secs }) => assert!(retry_after_secs >= 1),
        _ => panic!("third request in the same second must be rate-refused"),
    }
    // 500ms later: one token refilled (2/sec).
    assert!(r.admit(&p, &quotas, 1_500).is_ok());
    assert!(matches!(
        r.admit(&p, &quotas, 1_500),
        Err(QuotaRefusal::Rate { .. })
    ));
}

#[test]
fn zero_rate_is_unlimited_and_projects_never_share_buckets() {
    let r = QuotaRegistry::default();
    let a = pid("proj_a");
    let b = pid("proj_b");
    for _ in 0..100 {
        assert!(r.admit(&a, &q(0, 0), 1_000).is_ok());
    }
    // Project A being hot must not consume B's tokens.
    let limited = q(1, 0);
    assert!(r.admit(&b, &limited, 1_000).is_ok());
    assert!(matches!(
        r.admit(&b, &limited, 1_000),
        Err(QuotaRefusal::Rate { .. })
    ));
    assert!(r.admit(&a, &q(0, 0), 1_000).is_ok(), "A unaffected by B");
}

#[test]
fn concurrency_releases_with_the_guard() {
    let r = QuotaRegistry::default();
    let p = pid("proj_c");
    let quotas = q(0, 2);
    let g1 = r.admit(&p, &quotas, 1_000).unwrap();
    let _g2 = r.admit(&p, &quotas, 1_000).unwrap();
    assert!(matches!(
        r.admit(&p, &quotas, 1_000),
        Err(QuotaRefusal::Concurrency)
    ));
    drop(g1);
    assert!(r.admit(&p, &quotas, 1_000).is_ok());
}

#[test]
fn append_volume_buckets_meter_bytes_and_records() {
    let r = QuotaRegistry::default();
    let p = pid("proj_v");
    let quotas = ProjectQuotas {
        append_bytes_per_sec: 1_000,
        append_records_per_sec: 10,
        ..Default::default()
    };
    // Track the project first (as the request-rate admit does).
    let _g = r.admit(&p, &quotas, 1_000).unwrap();
    assert!(r.admit_append(&p, &quotas, 600, 5, 1_000).is_ok());
    assert!(r.admit_append(&p, &quotas, 400, 5, 1_000).is_ok());
    // Bytes bucket dry (and records bucket dry).
    assert!(matches!(
        r.admit_append(&p, &quotas, 1, 1, 1_000),
        Err(QuotaRefusal::Rate { .. })
    ));
    // Half a second later: 500 bytes / 5 records refilled.
    assert!(r.admit_append(&p, &quotas, 500, 5, 1_500).is_ok());
    assert!(matches!(
        r.admit_append(&p, &quotas, 1, 0, 1_500),
        Err(QuotaRefusal::Rate { .. })
    ));
}

#[test]
fn oversized_single_append_admits_once_then_waits() {
    let r = QuotaRegistry::default();
    let p = pid("proj_o");
    let quotas = ProjectQuotas {
        append_bytes_per_sec: 100,
        ..Default::default()
    };
    let _g = r.admit(&p, &quotas, 1_000).unwrap();
    // 5x one second's budget: admitted from a full bucket (it
    // could otherwise never succeed), driving the bucket negative.
    assert!(r.admit_append(&p, &quotas, 500, 1, 1_000).is_ok());
    // The debt is real: even a tiny append waits it out...
    match r.admit_append(&p, &quotas, 1, 0, 1_000) {
        Err(QuotaRefusal::Rate { retry_after_secs }) => {
            assert!(retry_after_secs >= 4, "debt horizon: {retry_after_secs}")
        }
        _ => panic!("bucket must be in debt"),
    }
    // ...and clears after the debt window.
    assert!(r.admit_append(&p, &quotas, 50, 0, 7_000).is_ok());
}

#[test]
fn read_debit_runs_negative_and_blocks_until_refilled() {
    let r = QuotaRegistry::default();
    let p = pid("proj_r");
    let quotas = ProjectQuotas {
        read_bytes_per_sec: 100,
        ..Default::default()
    };
    let _g = r.admit(&p, &quotas, 1_000).unwrap();
    // First read passes (no debt), serves 350 bytes -> level -250.
    assert!(r.check_read(&p, &quotas, 1_000).is_ok());
    r.debit_read(&p, &quotas, 350, 1_000);
    match r.check_read(&p, &quotas, 1_000) {
        Err(QuotaRefusal::Rate { retry_after_secs }) => {
            assert!(retry_after_secs >= 2, "debt horizon: {retry_after_secs}")
        }
        _ => panic!("in-debt bucket must refuse reads"),
    }
    // 2.6s later the 250-byte debt has refilled past zero.
    assert!(r.check_read(&p, &quotas, 3_600).is_ok());
}

#[test]
fn subscription_slots_release_with_the_guard() {
    let r = QuotaRegistry::default();
    let p = pid("proj_s");
    let quotas = ProjectQuotas {
        max_live_subscriptions: 1,
        ..Default::default()
    };
    let _g = r.admit(&p, &quotas, 1_000).unwrap();
    let s1 = r.admit_subscription(&p, &quotas).unwrap();
    assert!(s1.is_some());
    assert!(matches!(
        r.admit_subscription(&p, &quotas),
        Err(QuotaRefusal::Concurrency)
    ));
    drop(s1);
    assert!(r.admit_subscription(&p, &quotas).unwrap().is_some());
    // Round-13.3: unlimited (0) still COUNTS — the guard exists so
    // the subscription is visible as memory pressure; only the
    // refusal line is gone.
    let unlimited = ProjectQuotas::default();
    let g = r.admit_subscription(&p, &unlimited).unwrap();
    assert!(g.is_some(), "counting guard under an unconfigured quota");
}

#[test]
fn refused_batch_charges_nothing_atomic_debit() {
    // Review item 5: bytes budget generous, records budget tiny.
    // A batch that the RECORDS bucket refuses must not burn BYTES.
    let r = QuotaRegistry::default();
    let p = pid("proj_at");
    let quotas = ProjectQuotas {
        append_bytes_per_sec: 1_000,
        append_records_per_sec: 2,
        ..Default::default()
    };
    let _g = r.admit(&p, &quotas, 1_000).unwrap();
    // Spend one record so the records bucket is NOT full (the
    // oversized-from-full rule must not apply).
    assert!(r.admit_append(&p, &quotas, 100, 1, 1_000).is_ok());
    // Refused on records (needs 2, has 1) — bytes must be
    // untouched by the refused attempt.
    assert!(matches!(
        r.admit_append(&p, &quotas, 800, 2, 1_000),
        Err(QuotaRefusal::Rate { .. })
    ));
    // Exactly the remaining byte budget still fits: had the
    // refused batch charged bytes, this would fail.
    assert!(r.admit_append(&p, &quotas, 900, 1, 1_000).is_ok());
}

#[test]
fn tracker_evicts_idle_projects_never_active_ones() {
    let r = QuotaRegistry::default();
    // Fill the tracker at t=0; keep p0 ACTIVE via a held guard.
    let g0 = r.admit(&pid("p0"), &q(0, 0), 0).unwrap();
    for i in 1..MAX_TRACKED_PROJECTS {
        let _ = r.admit(&pid(&format!("p{i}")), &q(0, 0), 0).unwrap();
    }
    // Before the idle horizon: full tracker refuses the newcomer.
    assert!(matches!(
        r.admit(&pid("p_new"), &q(0, 0), IDLE_EVICT_MS - 1),
        Err(QuotaRefusal::TrackerCapacity)
    ));
    // Past the horizon: idle entries evict, the newcomer fits...
    assert!(r.admit(&pid("p_new"), &q(0, 0), IDLE_EVICT_MS + 1).is_ok());
    // ...and ONLY the project with INFLIGHT work survived the
    // sweep beside the newcomer (p_new's own guard dropped at the
    // assert, so p0's held guard is the one live slot).
    let (tracked, inflight) = r.stats();
    assert_eq!(tracked, 2, "p0 (active) + p_new: {tracked}");
    assert_eq!(inflight, 1, "p0's held guard: {inflight}");
    drop(g0);
}

#[test]
fn tracker_bound_refuses_new_projects_only() {
    let r = QuotaRegistry::default();
    for i in 0..MAX_TRACKED_PROJECTS {
        assert!(r.admit(&pid(&format!("p{i}")), &q(0, 0), 1_000).is_ok());
    }
    assert!(matches!(
        r.admit(&pid("p_new"), &q(0, 0), 1_000),
        Err(QuotaRefusal::TrackerCapacity)
    ));
    // Already-tracked projects are untouched by tracker pressure.
    assert!(r.admit(&pid("p0"), &q(0, 0), 1_000).is_ok());
}
