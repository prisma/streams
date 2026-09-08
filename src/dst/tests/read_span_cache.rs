//! O5 through the actual product reader and delete lifecycle.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, wait_all_absorbed};
use std::{sync::Arc, time::Duration};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o5_http_history_cache_keeps_key_checks_and_delete_recreate_isolation() {
    let history = Arc::new(crate::history::HistoryResources::new(
        &crate::config::HistoryConfig {
            canonical_span_cache: true,
            ..Default::default()
        },
        1 << 16,
    ));
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                shared_history: Some(history.clone()),
                tail_ring_bytes: 0,
                ..Default::default()
            },
            absorber: Some(crate::history::AbsorberConfig {
                threshold_bytes: 1,
                threshold_age: Duration::from_millis(1),
                tick: Duration::from_millis(20),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let create = "/v1/streams/cache-lifecycle";
    let records = "/v1/streams/cache-lifecycle/records?routingKey=hot";
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            create,
            &headers,
            br#"{"format":{"kind":"bytes"}}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        preq(
            rig.addr,
            "POST",
            "/v1/streams/cache-lifecycle/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "hot")
            ],
            b"old payload"
        )
        .await
        .0,
        200
    );
    let desc = rig
        .state
        .registry
        .get(&rig.state.deployment.raw_adapter_sref("cache-lifecycle"))
        .await
        .unwrap()
        .unwrap();
    let engine = rig
        .state
        .engine_for(&desc.segment_route_by_id(0).unwrap())
        .await
        .unwrap();
    wait_all_absorbed(&engine, &[desc.storage_hash()]).await;
    for _ in 0..2 {
        let (status, h, body) = preq(rig.addr, "GET", records, &headers, b"").await;
        assert_eq!(status, 200);
        assert_eq!(body, b"old payload");
        assert_eq!(h.get("prisma-up-to-date").map(String::as_str), Some("true"));
    }
    assert!(
        history.spans.reserved() > 65536,
        "actual descriptor-bound product reads must populate cache"
    );
    let wrong = preq(
        rig.addr,
        "GET",
        records,
        &[(
            "prisma-encryption-key",
            "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk",
        )],
        b"",
    )
    .await;
    assert_ne!(wrong.0, 200);
    assert_ne!(wrong.2, b"old payload");
    assert!(matches!(
        preq(rig.addr, "DELETE", create, &headers, b"").await.0,
        200 | 204
    ));
    assert_eq!(
        history.spans.reserved(),
        65536,
        "delete must invalidate populated project proofs"
    );
    assert_ne!(preq(rig.addr, "GET", records, &headers, b"").await.0, 200);
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            create,
            &headers,
            br#"{"format":{"kind":"bytes"}}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        preq(
            rig.addr,
            "POST",
            "/v1/streams/cache-lifecycle/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "hot")
            ],
            b"new payload"
        )
        .await
        .0,
        200
    );
    assert_eq!(
        preq(rig.addr, "GET", records, &headers, b"").await.2,
        b"new payload"
    );
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}
