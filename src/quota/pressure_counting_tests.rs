#![cfg(test)]

use super::{PRESSURE_SUB_WEIGHT_BYTES, ProjectId, ProjectQuotas, QuotaRefusal, QuotaRegistry};

fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).unwrap()
}

/// Round-13.3 red (field A1 finding): the pressure model's EXACT
/// dimensions must count UNCONDITIONALLY — live subscriptions were
/// only counted when max_live_subscriptions was configured as a
/// refusal quota, so a default-quota noisy project showed subs=0
/// pressure while holding 200 connections.
#[test]
fn live_subs_count_without_a_configured_quota() {
    let r = QuotaRegistry::default();
    let p = pid("c1");
    let _ = r.admit(&p, &ProjectQuotas::default(), 1_000).unwrap();
    let g = r.admit_subscription(&p, &ProjectQuotas::default()).unwrap();
    assert!(
        g.is_some(),
        "an unconfigured quota still returns a counting guard"
    );
    let a = r.pressure_handle(&p).unwrap();
    assert_eq!(
        a.estimated_pressure_bytes(),
        PRESSURE_SUB_WEIGHT_BYTES,
        "the subscription is pressure even with no refusal quota"
    );
    drop(g);
    assert_eq!(a.estimated_pressure_bytes(), 0);
}

/// Round-13.3 red (field A1 finding): queued append bytes are the
/// standing committer-queue memory — they must charge pressure
/// even when queued_append_bytes is not configured as a ceiling
/// (the noisy project held ~12 MB of 10-second queue that the
/// model could not see).
#[test]
fn queued_bytes_charge_without_a_configured_ceiling() {
    let r = QuotaRegistry::default();
    let p = pid("c2");
    let _ = r.admit(&p, &ProjectQuotas::default(), 1_000).unwrap();
    let g = r
        .charge_queued(&p, &ProjectQuotas::default(), 500_000)
        .unwrap();
    assert!(g.is_some(), "an unconfigured ceiling still charges");
    let a = r.pressure_handle(&p).unwrap();
    assert_eq!(a.estimated_pressure_bytes(), 500_000);
    drop(g);
    assert_eq!(a.estimated_pressure_bytes(), 0);
    // And the configured ceiling still refuses at its line.
    let q = ProjectQuotas {
        queued_append_bytes: 100,
        ..Default::default()
    };
    assert!(matches!(
        r.charge_queued(&p, &q, 500),
        Err(QuotaRefusal::QueuedBytes)
    ));
    assert_eq!(a.estimated_pressure_bytes(), 0, "refusal rolls back");
}
