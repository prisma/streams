use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;

// Actual readiness requests after a required task terminates.
use crate::tasks::{Policy, TaskResult};
use std::time::Duration;

#[tokio::test]
async fn readiness_endpoint_refuses_each_permanent_critical_exit() {
    for outcome in 0..3 {
        let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
        let (release, wait) = tokio::sync::oneshot::channel();
        rig.tasks
            .spawn("review-required", Policy::Critical, |_| async move {
                wait.await.unwrap();
                match outcome {
                    0 => TaskResult::Done,
                    1 => TaskResult::Failed("scripted permanent failure".into()),
                    _ => panic!("scripted critical panic"),
                }
            })
            .unwrap();
        assert_eq!(hreq(rig.addr, "GET", "/readyz", &[], b"").await.0, 200);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while rig.state.tasks.unready_reason().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for path in ["/health", "/readyz"] {
            let (status, _, body) = hreq(rig.addr, "GET", path, &[], b"").await;
            assert_eq!(status, 503);
            assert!(String::from_utf8_lossy(&body).contains("review-required"));
        }
        rig.tasks.shutdown(Duration::from_secs(1)).await;
    }
}
