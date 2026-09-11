use super::{drift_stats, push_drift};
use std::collections::VecDeque;
use std::sync::Mutex;

#[test]
fn drift_window_preserves_cutoff_percentiles_and_threshold() {
    let ring = Mutex::new(VecDeque::from([
        (99, 90_000),
        (100, 49_999),
        (101, 50_000),
        (102, 70_000),
    ]));
    assert_eq!(
        drift_stats(&ring, 100),
        serde_json::json!({"n":3,"p50_us":50_000,"p99_us":70_000,"max_us":70_000,"over_50ms":2})
    );
    assert_eq!(
        drift_stats(&ring, 103),
        serde_json::json!({"n":0,"p50_us":0,"p99_us":0,"max_us":0,"over_50ms":0})
    );
}

#[test]
fn drift_ring_is_bounded_and_poison_refuses_further_samples() {
    let ring = Mutex::new(VecDeque::new());
    for value in 0..4098 {
        push_drift(&ring, value);
    }
    assert_eq!(ring.lock().unwrap().len(), 4096);
    assert_eq!(ring.lock().unwrap().front().unwrap().1, 2);
    assert!(
        std::panic::catch_unwind(|| {
            let mut pending = ring.lock().unwrap();
            pending.clear();
            pending.push_back((1, 1));
            panic!("interrupted scheduler sample");
        })
        .is_err()
    );
    assert!(std::panic::catch_unwind(|| drift_stats(&ring, 0)).is_err());
    assert!(std::panic::catch_unwind(|| push_drift(&ring, 3)).is_err());
}
