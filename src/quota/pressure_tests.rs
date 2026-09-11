#![cfg(test)]

use super::{
    BufferedBodyGuard, FeedPressureGuard, IDLE_EVICT_MS, MAX_TRACKED_PROJECTS,
    PRESSURE_FEED_WEIGHT_BYTES, PRESSURE_SUB_WEIGHT_BYTES, ProjectAdmission, ProjectId,
    ProjectQuotas, QuotaRegistry, StreamPressureBinding,
};
use std::sync::{Arc, atomic::Ordering};

fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).unwrap()
}
fn adm(r: &QuotaRegistry, name: &str) -> Arc<ProjectAdmission> {
    let p = pid(name);
    let _ = r.admit(&p, &ProjectQuotas::default(), 1_000).unwrap();
    r.pressure_handle(&p).unwrap()
}

/// Battery 1: static subscription pressure alone reaches the high
/// watermark and engages the latch (reducing append headroom).
#[test]
fn static_subscription_pressure_engages_the_latch() {
    let r = QuotaRegistry::default();
    let a = adm(&r, "p1");
    for _ in 0..4 {
        a.live_subs.fetch_add(1, Ordering::Relaxed);
    }
    let high = 3 * PRESSURE_SUB_WEIGHT_BYTES; // 4 subs > high
    assert!(a.memory_gate(&pid("p1"), high, 75), "engages over high");
    assert_eq!(a.memory_engage_count.load(Ordering::Relaxed), 1);
}

/// Battery 2: a feed is charged once per feed via its guard —
/// never per subscriber — and releases exactly once on drop.
#[test]
fn feed_weight_charges_once_per_feed() {
    let r = QuotaRegistry::default();
    let a = adm(&r, "p2");
    let g1 = FeedPressureGuard::acquire(a.clone());
    assert_eq!(a.estimated_pressure_bytes(), PRESSURE_FEED_WEIGHT_BYTES);
    let g2 = FeedPressureGuard::acquire(a.clone());
    assert_eq!(a.estimated_pressure_bytes(), 2 * PRESSURE_FEED_WEIGHT_BYTES);
    drop(g1);
    drop(g2);
    assert_eq!(a.estimated_pressure_bytes(), 0);
}

/// Battery 3: retained SSE bytes enter the model EXACTLY once,
/// unweighted (the budget mirrors its own reservation; the model
/// never re-estimates it).
#[test]
fn retained_bytes_are_not_double_counted() {
    let r = QuotaRegistry::default();
    let a = adm(&r, "p3");
    a.retained_sse_add(100_000);
    assert_eq!(a.estimated_pressure_bytes(), 100_000);
    a.retained_sse_sub(40_000);
    assert_eq!(a.estimated_pressure_bytes(), 60_000);
    a.retained_sse_sub(1_000_000); // over-release clamps, never wraps
    assert_eq!(a.estimated_pressure_bytes(), 0);
}

/// Battery 4: the buffered-body guard releases on EVERY exit path
/// (parse failure, cancellation, refusal are all drops).
#[test]
fn body_guard_releases_on_drop() {
    let r = QuotaRegistry::default();
    let a = adm(&r, "p4");
    let mut g = BufferedBodyGuard::reserve(a.clone(), 1_000);
    g.grow(2_000);
    assert_eq!(a.estimated_pressure_bytes(), 3_000);
    drop(g);
    assert_eq!(a.estimated_pressure_bytes(), 0);
}

/// Battery 5: body -> queued transfer has no transient double
/// charge (the body guard ends before the queued charge begins).
#[test]
fn body_to_queued_transfer_never_double_charges() {
    let r = QuotaRegistry::default();
    let p = pid("p5");
    let a = adm(&r, "p5");
    let g = BufferedBodyGuard::reserve(a.clone(), 5_000);
    assert_eq!(a.estimated_pressure_bytes(), 5_000);
    drop(g); // the transfer point
    let quotas = ProjectQuotas {
        queued_append_bytes: 1 << 20,
        ..Default::default()
    };
    let _q = r.charge_queued(&p, &quotas, 5_000).unwrap();
    assert_eq!(
        a.estimated_pressure_bytes(),
        5_000,
        "queued only — never body+queued at once"
    );
}

/// Battery 6+7+8: exact frame-debt attribution — adds exact,
/// retires exact, dirty-stream count moves ONLY on 0->pos and
/// pos->0 edges.
#[test]
fn frame_debt_attribution_is_exact_with_edge_only_dirty_count() {
    let r = QuotaRegistry::default();
    let a = adm(&r, "p6");
    let b = StreamPressureBinding::bind(a.clone(), 0);
    b.frames_added(1_000);
    assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 1_000);
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
    b.frames_added(500); // still ONE dirty stream
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
    b.frames_retired(600); // partial: stays dirty
    assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 900);
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
    b.frames_retired(900); // pos -> 0
    assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 0);
    b.frames_added(10); // 0 -> pos again
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
    drop(b); // release outstanding attribution
    assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 0);
}

/// Battery 9 (unit leg): binding to a stream with existing durable
/// debt seeds from the tail — never from zero — and drop releases
/// exactly the seed plus subsequent net.
#[test]
fn binding_seeds_existing_durable_debt() {
    let r = QuotaRegistry::default();
    let a = adm(&r, "p9");
    let b = StreamPressureBinding::bind(a.clone(), 5_000_000);
    assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 5_000_000);
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 1);
    b.frames_retired(5_000_000);
    assert_eq!(a.dirty_streams.load(Ordering::Relaxed), 0);
    drop(b);
    assert_eq!(a.unabsorbed_frame_bytes.load(Ordering::Relaxed), 0);
}

/// Battery 11: the latch engages at high, HOLDS between the
/// release point and high (no flap), and releases only below
/// high x release_pct.
#[test]
fn hysteresis_latch_does_not_flap() {
    let r = QuotaRegistry::default();
    let p = pid("p11");
    let a = adm(&r, "p11");
    let high = 100 * 1024;
    a.retained_sse_add(101 * 1024);
    assert!(a.memory_gate(&p, high, 75), "engage over high");
    a.retained_sse_sub(21 * 1024); // 80 KiB: between 75 KiB and high
    assert!(a.memory_gate(&p, high, 75), "held engaged in the band");
    assert!(a.memory_gate(&p, high, 75), "still engaged (no flap)");
    a.retained_sse_sub(10 * 1024); // 70 KiB < 75 KiB release point
    assert!(!a.memory_gate(&p, high, 75), "releases below the point");
    assert!(!a.memory_gate(&p, high, 75), "stays released");
    assert_eq!(a.memory_engage_count.load(Ordering::Relaxed), 1);
}

/// Battery 12: tracker eviction can NEVER remove a project holding
/// pressure — orphaned attribution would leak forever.
#[test]
fn eviction_cannot_remove_a_project_with_pressure() {
    let r = QuotaRegistry::default();
    let old_ms = 1_000;
    // Fill the tracker with idle projects at an ancient timestamp.
    for i in 0..MAX_TRACKED_PROJECTS {
        drop(
            r.admit(&pid(&format!("f{i}")), &ProjectQuotas::default(), old_ms)
                .expect("seed every tracker entry"),
        );
    }
    // One of them holds pressure (a live feed's static charge).
    let pinned = r.pressure_handle(&pid("f7")).unwrap();
    let _feed = FeedPressureGuard::acquire(pinned);
    // A NEW project far past the idle horizon forces the eviction
    // sweep; the pressured entry must survive it.
    let now = old_ms + IDLE_EVICT_MS + 1;
    let _ = r
        .admit(&pid("fresh"), &ProjectQuotas::default(), now)
        .unwrap();
    assert!(
        r.pressure_handle(&pid("f7")).is_some(),
        "pressure pins the entry through eviction"
    );
    assert!(
        r.pressure_handle(&pid("f8")).is_none(),
        "idle peers evicted"
    );
}

/// Battery 13: project A engaging its latch never rejects
/// project B (isolation is the whole point).
#[test]
fn engaged_project_does_not_reject_neighbors() {
    let r = QuotaRegistry::default();
    let pa = pid("pa");
    let pb = pid("pb");
    let a = adm(&r, "pa");
    let b = adm(&r, "pb");
    let high = 64 * 1024;
    a.retained_sse_add(65 * 1024);
    assert!(a.memory_gate(&pa, high, 75), "A engaged");
    assert!(!b.memory_gate(&pb, high, 75), "B unaffected");
}

/// Battery 14 (backstop ordering): with the per-project gate OFF
/// (high = 0) nothing is refused here — several compliant projects
/// reaching the cell ceiling remains the GLOBAL RSS gate's job.
#[test]
fn per_project_gate_off_defers_to_the_global_gate() {
    let r = QuotaRegistry::default();
    let p = pid("p14");
    let a = adm(&r, "p14");
    a.retained_sse_add(1 << 30);
    assert!(
        !a.memory_gate(&p, 0, 75),
        "0 = off; the global gate owns it"
    );
}
