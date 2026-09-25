use super::*;
use crate::runtime::{ManualClock, RuntimeCaps, SeededEntropy};
use std::time::Duration;

fn runtime(bytes: f64, clock: Arc<ManualClock>) -> RuntimeCaps {
    let mut config = crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    );
    config.admission.limit_bytes_per_sec = bytes;
    config.admission.limit_reqs_per_sec = 0.0;
    config.admission.limit_recs_per_sec = 0.0;
    config.admission.limit_burst_secs = 1.0;
    RuntimeCaps::with(clock, Arc::new(SeededEntropy::seeded(10)), "usage-owner")
        .with_config(&config)
}

#[test]
fn r10_runtime_usage_limits_counters_and_backlog_are_isolated() {
    // Touch an unrelated runtime before either configured owner exists.
    let early = runtime(999.0, Arc::new(ManualClock::at(0)));
    let hash = RouteHash([21; 16]);
    assert!(early.usage.admit_append(&hash.0, 999, 1).is_ok());
    let a = runtime(10.0, Arc::new(ManualClock::at(0)));
    let b = runtime(20.0, Arc::new(ManualClock::at(0)));
    assert_eq!(
        a.usage.permanently_unadmittable(11, 1).map(|r| r.dimension),
        Some("bytes")
    );
    assert_eq!(b.usage.permanently_unadmittable(11, 1), None);
    let admitted = a
        .usage
        .admit_append(&hash.0, 10, 1)
        .unwrap_or_else(|_| panic!("A capacity"));
    assert!(matches!(
        a.usage.admit_append(&hash.0, 1, 1),
        Err(LimitHit::Bytes { .. })
    ));
    let other = b
        .usage
        .admit_append(&hash.0, 11, 1)
        .unwrap_or_else(|_| panic!("B capacity"));
    assert!(!Arc::ptr_eq(&admitted, &other));
    admitted.requests.fetch_add(7, Ordering::Relaxed);
    assert_eq!(
        a.usage.counters(&hash.0).requests.load(Ordering::Relaxed),
        7
    );
    assert_eq!(other.requests.load(Ordering::Relaxed), 0);
    let shared = a.clone();
    assert!(Arc::ptr_eq(&a.usage, &shared.usage));
    assert!(matches!(
        shared.usage.admit_append(&hash.0, 1, 1),
        Err(LimitHit::Bytes { .. })
    ));
    let segment = SegmentHash([22; 16]);
    a.usage.link_storage(hash, segment);
    a.usage.set_absorb_lag(segment, 42);
    a.usage.set_shard_lag("same-shard", 42);
    a.usage.set_absorb_pending_summary("same-shard", 3, 42);
    assert_eq!(a.usage.absorb_lag_for_usage(hash), 42);
    assert_eq!(a.usage.absorb_pending_summary(), (3, 42));
    assert_eq!(b.usage.absorb_lag_for_usage(hash), 0);
    assert_eq!(b.usage.absorb_pending_summary(), (0, 0));
    assert!(b.usage.shard_lag_all().is_empty());
    b.usage.clear_shard_lag("same-shard");
    assert_eq!(a.usage.shard_lag_all(), [("same-shard".into(), 42)]);
    let weak = Arc::downgrade(&a.usage);
    drop(shared);
    drop(a);
    assert!(
        weak.upgrade().is_none(),
        "teardown releases maps even if counters remain held"
    );
    let restarted = runtime(10.0, Arc::new(ManualClock::at(0)));
    assert_eq!(restarted.usage.tracked_streams(), 0);
    assert_eq!(restarted.usage.absorb_pending_summary(), (0, 0));
    assert!(restarted.usage.admit_append(&hash.0, 10, 1).is_ok());
}

#[test]
fn r10_usage_refill_and_eviction_follow_only_owned_monotonic_time() {
    let clock = Arc::new(ManualClock::at(1000));
    let rt = runtime(10.0, clock.clone());
    let hash = [31; 16];
    assert!(rt.usage.admit_append(&hash, 10, 1).is_ok());
    for jump in [86_400_000, -172_800_000] {
        clock.jump_wall(jump);
        assert!(rt.usage.admit_append(&hash, 1, 1).is_err());
        assert!(!rt.usage.evict_idle_for_test(Duration::from_secs(1)));
    }
    clock.advance_monotonic(Duration::from_secs(1));
    assert!(rt.usage.admit_append(&hash, 10, 1).is_ok());
    clock.advance_monotonic(Duration::from_secs(600));
    assert!(rt.usage.evict_idle_for_test(Duration::from_secs(600)));
    assert_eq!(rt.usage.tracked_streams(), 0);
}

/// The request-token floor is a boot decision (`config::admission_limits`),
/// never a per-append verdict: the runtime owner answers only for the
/// request's own size, so a sub-token request bucket — which validation
/// refuses before any service exists — is not its concern.
#[test]
fn the_runtime_owner_decides_only_the_requests_own_size() {
    let sub_token = crate::config::AdmissionConfig {
        limit_reqs_per_sec: 0.1,
        limit_burst_secs: 2.0,
        ..Default::default()
    };
    let usage = UsageService::new(&sub_token, Arc::new(ManualClock::at(0)));
    assert_eq!(
        usage.permanently_unadmittable(1, 1),
        None,
        "the request-token floor is a boot decision, not a per-append verdict"
    );
    assert_eq!(
        usage.permanently_unadmittable(10_000_001, 1),
        Some(CapacityRefusal {
            dimension: "bytes",
            capacity: 10_000_000,
            requested: 10_000_001
        })
    );
    assert_eq!(
        usage.permanently_unadmittable(1, 10_001),
        Some(CapacityRefusal {
            dimension: "records",
            capacity: 10_000,
            requested: 10_001
        })
    );
    assert_eq!(
        usage.permanently_unadmittable(10_000_000, 10_000),
        None,
        "exactly the capacity fits"
    );
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 1024, ..proptest::prelude::ProptestConfig::default() })]
    /// Permanent refusal is exactly fresh-bucket refusal, kind for kind, for
    /// every posture validation accepts — including disabled buckets, the
    /// exact capacity, one over it, zero and u64::MAX — and names the exact
    /// capacity and the size requested. Validation is proven
    /// sufficient for the request bucket by its own unit tests; every
    /// generated posture here holds at least one request token.
    #[test]
    fn permanent_refusal_is_exactly_fresh_bucket_refusal(
        bytes_rate in 0u32..=20_000,
        recs_rate in 0u32..=20_000,
        reqs_rate in 1u32..=2_000,
        burst in 1u32..=3,
        bytes_pick in 0u8..6,
        recs_pick in 0u8..6,
        jitter in 0u64..40_000,
    ) {
        let cfg = crate::config::AdmissionConfig {
            limit_bytes_per_sec: f64::from(bytes_rate),
            limit_reqs_per_sec: f64::from(reqs_rate),
            limit_recs_per_sec: f64::from(recs_rate),
            limit_burst_secs: f64::from(burst),
            ..Default::default()
        };
        proptest::prop_assert!(
            crate::config::admission_limits::validate_admission_limits(&cfg).is_empty()
        );
        // Integer rate x integer burst: the capacity is exact, so cap-1 /
        // cap / cap+1 are exact.
        let pick = |rate: u32, which: u8| -> u64 {
            let cap = u64::from(rate) * u64::from(burst);
            match which {
                0 => 0,
                1 => cap.saturating_sub(1),
                2 => cap,
                3 => cap + 1,
                4 => u64::MAX,
                _ => jitter,
            }
        };
        let (bytes, records) = (pick(bytes_rate, bytes_pick), pick(recs_rate, recs_pick));
        let usage = UsageService::new(&cfg, Arc::new(ManualClock::at(0)));
        let fresh = match usage.admit_append(&[7u8; 16], bytes, records) {
            Ok(_) => None,
            Err(LimitHit::Bytes { .. }) => Some("bytes"),
            Err(LimitHit::Records { .. }) => Some("records"),
            Err(LimitHit::Requests { .. }) => Some("requests"),
        };
        let refusal = usage.permanently_unadmittable(bytes, records);
        proptest::prop_assert_eq!(refusal.map(|r| r.dimension), fresh);
        if let Some(r) = refusal {
            let (rate, size) = if r.dimension == "bytes" { (bytes_rate, bytes) } else { (recs_rate, records) };
            proptest::prop_assert_eq!((r.capacity, r.requested), (u64::from(rate) * u64::from(burst), size));
        }
    }
}
