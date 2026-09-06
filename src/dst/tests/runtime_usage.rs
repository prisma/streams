//! Actual append admission and committed accounting use the serving runtime.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_append_admission_and_committed_counters_share_only_their_runtime() {
    let mut rigs = Vec::new();
    for (incarnation, limit) in [(41, 10.0), (42, 20.0)] {
        let policy = crate::config::AdmissionConfig {
            limit_bytes_per_sec: limit,
            limit_reqs_per_sec: 0.0,
            limit_recs_per_sec: 0.0,
            limit_burst_secs: 1.0,
            ..Default::default()
        };
        let usage = Arc::new(crate::usage::UsageService::new(
            &policy,
            Arc::new(crate::runtime::ManualClock::at(0)),
        ));
        let rig = http_rig_build(
            mem(),
            RigRuntime::incarnation(incarnation),
            HttpRigOptions {
                shard: crate::shard::ShardConfig {
                    shared_usage: Some(usage.clone()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        assert!(Arc::ptr_eq(&rig.state.runtime.usage, &usage));
        rigs.push(rig);
    }
    let headers = [
        ("content-type", "text/plain"),
        ("stream-encryption-key", PRISMA_KEY),
    ];
    for rig in &rigs {
        assert_eq!(
            hreq(rig.addr, "PUT", "/v1/stream/same-usage", &headers, b"")
                .await
                .0,
            201
        );
    }
    for rig in &rigs {
        let status = hreq(
            rig.addr,
            "POST",
            "/v1/stream/same-usage",
            &headers,
            b"0123456789",
        )
        .await
        .0;
        assert!(matches!(status, 200 | 204));
    }
    assert_eq!(
        hreq(
            rigs[0].addr,
            "POST",
            "/v1/stream/same-usage",
            &headers,
            b"x"
        )
        .await
        .0,
        429
    );
    assert!(matches!(
        hreq(
            rigs[1].addr,
            "POST",
            "/v1/stream/same-usage",
            &headers,
            b"x"
        )
        .await
        .0,
        200 | 204
    ));
    let hash = crate::crypto::RouteHash::for_stream(
        &rigs[0].state.deployment.raw_adapter_sref("same-usage"),
    );
    let a = rigs[0].state.runtime.usage.counters(&hash.0);
    let b = rigs[1].state.runtime.usage.counters(&hash.0);
    assert!(!Arc::ptr_eq(&a, &b));
    assert_eq!(
        (
            a.requests.load(Ordering::Relaxed),
            a.plaintext_bytes.load(Ordering::Relaxed)
        ),
        (1, 10)
    );
    assert_eq!(
        (
            b.requests.load(Ordering::Relaxed),
            b.plaintext_bytes.load(Ordering::Relaxed)
        ),
        (2, 11)
    );
    for rig in &rigs {
        for engine in rig.state.shards.engines() {
            assert!(Arc::ptr_eq(&engine.usage, &rig.state.runtime.usage));
        }
        engine_shutdown(&rig.state).await;
    }
}
