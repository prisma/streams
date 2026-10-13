//! Stored-cap B4 (T4): an append's charge first refuses a project at its
//! stored cap, by the figure the usage rollup published, and charges it
//! nothing.
#![cfg(test)]

use std::sync::atomic::Ordering;

use crate::project_policy::ProjectQuotas;
use crate::quota::stored::StoredFigure;
use crate::quota::{QuotaRefusal, QuotaRegistry};
use crate::tenant::ProjectId;

fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).unwrap()
}

/// A registry tracking `p`.
fn tracking(p: &ProjectId) -> QuotaRegistry {
    let r = QuotaRegistry::default();
    let _ = r.admit(p, &ProjectQuotas::default(), 1_000).unwrap();
    r
}

/// The rollup publishes that `p` stores `bytes`.
fn publish(r: &QuotaRegistry, p: &ProjectId, bytes: u64) {
    let figure = StoredFigure {
        project: p.clone(),
        bytes,
        written: bytes,
    };
    r.stored_table().publish(&[figure], 1);
}

fn capped(max_stored_bytes: u64) -> ProjectQuotas {
    ProjectQuotas {
        max_stored_bytes,
        ..ProjectQuotas::default()
    }
}

/// The bytes `p`'s appends hold in the committer queue.
fn queued(r: &QuotaRegistry, p: &ProjectId) -> u64 {
    let admission = r.pressure_handle(p).unwrap();
    admission.queued_bytes.load(Ordering::Relaxed)
}

/// The refusal a charge answered, if it refused.
fn refusal(r: &QuotaRegistry, p: &ProjectId, quotas: &ProjectQuotas) -> Option<(u64, u64)> {
    match r.charge_queued(p, quotas, 500) {
        Err(QuotaRefusal::StoredBytes { limit, stored }) => Some((limit, stored)),
        Err(other) => panic!("refused {other:?}"),
        Ok(_) => None,
    }
}

/// How many events of `kind` the registry emitted.
fn events(r: &QuotaRegistry, kind: &str) -> usize {
    let recent = r.ops.recent(256);
    recent.iter().filter(|e| e.event_type == kind).count()
}

/// Published 1,000 against a cap of 1,000 is refused with both figures and
/// charges nothing; 999 is admitted and charged; a cap of 0 admits any
/// figure; a project the rollup published nothing for is admitted.
#[test]
fn a_project_at_its_stored_cap_is_refused_and_charged_nothing() {
    let p = pid("p");
    let r = tracking(&p);
    publish(&r, &p, 1_000);
    assert_eq!(refusal(&r, &p, &capped(1_000)), Some((1_000, 1_000)));
    assert_eq!(queued(&r, &p), 0, "a refused append charges nothing");
    assert_eq!(refusal(&r, &p, &capped(1_001)), None);
    publish(&r, &p, 999);
    let charged = r.charge_queued(&p, &capped(1_000), 500).unwrap();
    assert_eq!(queued(&r, &p), 500);
    drop(charged);
    publish(&r, &p, 5_000);
    assert_eq!(refusal(&r, &p, &capped(0)), None, "0 is no limit");
    let q = pid("q");
    let r = tracking(&q);
    assert_eq!(refusal(&r, &q, &capped(1)), None, "no figure, no refusal");
}

/// The cap emits one event when it engages and one when it releases, never
/// one per refused append: a figure back under the quota releases it, and
/// so does a quota raised to no limit.
#[test]
fn the_stored_cap_emits_one_event_per_engage_and_release() {
    let reached = "project_stored_cap_reached";
    let cleared = "project_stored_cap_cleared";
    let p = pid("p");
    let r = tracking(&p);
    publish(&r, &p, 1_000);
    for _ in 0..3 {
        assert!(refusal(&r, &p, &capped(1_000)).is_some());
    }
    assert_eq!((events(&r, reached), events(&r, cleared)), (1, 0));
    assert_eq!(r.ops.recent(1)[0].severity, "warn");
    publish(&r, &p, 999);
    for _ in 0..3 {
        assert_eq!(refusal(&r, &p, &capped(1_000)), None);
    }
    assert_eq!((events(&r, reached), events(&r, cleared)), (1, 1));
    publish(&r, &p, 1_000);
    assert!(refusal(&r, &p, &capped(1_000)).is_some());
    assert_eq!(refusal(&r, &p, &capped(0)), None);
    assert_eq!(refusal(&r, &p, &capped(0)), None);
    assert_eq!((events(&r, reached), events(&r, cleared)), (2, 2));
    let event = &r.ops.recent(1)[0];
    let want = serde_json::Value::from_iter([
        ("project", serde_json::Value::from("p")),
        ("stored_bytes", 1_000.into()),
        ("max_stored_bytes", 0.into()),
    ]);
    assert_eq!((event.severity.as_str(), &event.fields), ("info", &want));
}
