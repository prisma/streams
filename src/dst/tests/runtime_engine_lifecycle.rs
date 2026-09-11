//! R17-A observes actual required workers through the serving health surface.
use super::fixture_http::{HttpRigOptions, cold_absorber, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::time::Duration;

async fn ready_rig() -> super::fixture_http::HttpRig {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            absorber: Some(cold_absorber()),
            shard: crate::shard::ShardConfig {
                wal_group_commit: true,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            "/v1/streams/engine-health",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        preq(
            rig.addr,
            "POST",
            "/v1/streams/engine-health/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"value":1}"#
        )
        .await
        .0,
        200
    );
    for path in ["/health", "/readyz"] {
        assert_eq!(hreq(rig.addr, "GET", path, &[], b"").await.0, 200);
    }
    rig
}

#[expect(
    clippy::excessive_nesting,
    reason = "r17a_each_required_engine_exit_changes_real_health_and_readyz; the fixture nests the exit wait inside the timeout that bounds it inside the test; flattening it would separate the wait from the bound it must respect"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_each_required_engine_exit_changes_real_health_and_readyz() {
    let independent = ready_rig().await;
    for role in ["committer", "acker", "pump", "flush-ticker", "absorber"] {
        let rig = ready_rig().await;
        let engine = rig.state.shards.engines().into_iter().next().unwrap();
        // A rejected operation is not an unexpected task exit.
        let status = preq(
            rig.addr,
            "POST",
            "/v1/streams/engine-health/records",
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"invalid json",
        )
        .await
        .0;
        assert!((400..500).contains(&status));
        assert_eq!(hreq(rig.addr, "GET", "/readyz", &[], b"").await.0, 200);
        let stopped = engine.test_abort_task(role);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !stopped.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for path in ["/health", "/readyz"] {
            let (status, _, body) = hreq(rig.addr, "GET", path, &[], b"").await;
            assert_eq!(status, 503, "{role}: {path}");
            assert!(String::from_utf8_lossy(&body).contains(role));
            assert_eq!(hreq(independent.addr, "GET", path, &[], b"").await.0, 200);
        }
        let (resp, _) = tokio::sync::oneshot::channel();
        assert!(
            engine
                .try_close(crate::shard::CloseReq {
                    hash: [99; 16],
                    generation: None,
                    resp,
                })
                .is_err(),
            "failed engine must refuse commands"
        );
        let first = rig.state.shards.shutdown(Duration::from_secs(10)).await;
        assert!(
            first.is_err(),
            "unexpected exit must remain in shutdown report"
        );
        assert_eq!(
            rig.state.shards.shutdown(Duration::from_secs(1)).await,
            first
        );
        assert!(rig.state.shards.unready_reason().unwrap().contains(role));
        let weak = std::sync::Arc::downgrade(&engine);
        drop(engine);
        assert!(
            weak.upgrade().is_none(),
            "retirement receipt must not retain engine caches"
        );
        rig.tasks.shutdown(Duration::from_secs(1)).await;
    }
    independent
        .state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        independent.state.shards.unready_reason().is_none(),
        "intentional retirement stays distinct"
    );
    independent.tasks.shutdown(Duration::from_secs(1)).await;
}
