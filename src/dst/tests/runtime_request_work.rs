//! R09-A holds actual registry CAS requests, including distinct tenant keys.
use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

async fn setup(
    count: usize,
) -> (
    super::fixture_http::HttpRig,
    Arc<FaultStore>,
    Vec<crate::registry::StreamDesc>,
) {
    let store = FaultStore::new(mem(), 909, FaultProfile::clean());
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let service = rig.state.creation_service();
    let mut descriptors = Vec::new();
    for index in 0..count {
        let project = crate::tenant::ProjectId::new(&format!("r09-project-{}", index / 2)).unwrap();
        let sref = project.stream_ref(if index % 2 == 0 { "same" } else { "other" });
        let mut desc = crate::application::creation::fresh_desc(
            &service,
            &sref,
            &skey(),
            "application/octet-stream".into(),
            Some(600),
            None,
        );
        desc.expires_at_ms = Some(crate::shard::now_ms() + 300_000);
        descriptors.push(rig.state.registry.create(desc).await.unwrap().1);
    }
    (rig, store, descriptors)
}

async fn entered(count: &std::sync::atomic::AtomicU64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the actual registry conditional PUT must enter");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09a_request_ttl_jobs_bound_retained_keys_across_projects() {
    let (rig, store, descriptors) = setup(160).await;
    let service = rig.state.creation_service();
    let held = store.hold_class(StoreOp::Put, ObjClass::Other, 256);
    let _ = service.touch_ttl(&descriptors[0]);
    entered(&held).await;
    for desc in &descriptors {
        for _ in 0..4 {
            let _ = service.touch_ttl(desc);
        }
    }
    let retained = service.pending_ttl_for_tests();
    let (active, queued, keys, rejected) = rig.state.runtime.request_work.counts();
    assert!(active <= 8 && queued <= 64);
    assert_eq!(keys, active + queued);
    assert!(rejected > 0, "overflow must be explicit admission refusal");
    let (independent, _, own_descriptors) = setup(1).await;
    independent
        .state
        .creation_service()
        .renew_ttl(&own_descriptors[0])
        .await
        .unwrap();
    independent.tasks.shutdown(Duration::from_secs(1)).await;
    assert!(
        service.pending_ttl_for_tests() > 0,
        "another runtime's shutdown must not touch these jobs"
    );
    independent
        .state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
    store.release_hold();
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.pending_ttl_for_tests() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    rig.tasks.shutdown(Duration::from_secs(1)).await;
    rig.state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        retained <= 72,
        "retained {retained} jobs; contract is 8 active + 64 queued, including coalescing keys"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09a_runtime_shutdown_owns_the_entered_request_cas() {
    let (rig, store, descriptors) = setup(1).await;
    let service = rig.state.creation_service();
    let held = store.hold_class(StoreOp::Put, ObjClass::Other, 1);
    // Discarding the request's observer does not discard the job owner.
    let _ = service.touch_ttl(&descriptors[0]);
    entered(&held).await;
    rig.tasks.shutdown(Duration::from_secs(1)).await;
    let retained_after_shutdown = service.pending_ttl_for_tests();
    assert!(matches!(
        service.touch_ttl(&descriptors[0]),
        Err(crate::application::request_work::WorkError::Stopped)
    ));
    rig.state.registry.invalidate(&descriptors[0].sref());
    assert_eq!(
        rig.state
            .registry
            .get(&descriptors[0].sref())
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms,
        descriptors[0].expires_at_ms,
        "the held pre-dispatch CAS was cancelled"
    );
    let fresh = http_rig_build(
        store.clone(),
        RigRuntime::incarnation(1),
        HttpRigOptions::default(),
    )
    .await;
    fresh
        .state
        .creation_service()
        .renew_ttl(&descriptors[0])
        .await
        .unwrap();
    assert!(
        fresh
            .state
            .registry
            .get(&descriptors[0].sref())
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms
            > descriptors[0].expires_at_ms
    );
    fresh.tasks.shutdown(Duration::from_secs(1)).await;
    fresh
        .state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
    store.release_hold();
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.pending_ttl_for_tests() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    rig.state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(
        retained_after_shutdown, 0,
        "runtime reported joined while a request-triggered CAS remained owned by a detached task"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09a_old_ttl_cas_cannot_mutate_or_suppress_a_recreated_incarnation() {
    let (rig, store, descriptors) = setup(1).await;
    let service = rig.state.creation_service();
    let old = &descriptors[0];
    let held = store.hold_class(StoreOp::Put, ObjClass::Other, 1);
    let old_ticket = service.touch_ttl(old).unwrap();
    entered(&held).await;
    rig.state
        .registry
        .cas_update(&old.sref(), |desc| {
            desc.deleted = true;
            true
        })
        .await
        .unwrap();
    let mut replacement = crate::application::creation::fresh_desc(
        &service,
        &old.sref(),
        &skey(),
        "application/octet-stream".into(),
        Some(60),
        None,
    );
    replacement.expires_at_ms = Some(crate::shard::now_ms() + 30_000);
    let (created, replacement) = rig
        .state
        .registry
        .recreate(&old.sref(), replacement, |desc| desc.deleted)
        .await
        .unwrap();
    assert!(created);
    assert_ne!(old.stream_epoch, replacement.stream_epoch);
    let new_ticket = service.touch_ttl(&replacement).unwrap();
    tokio::time::timeout(Duration::from_secs(1), new_ticket.wait())
        .await
        .unwrap()
        .unwrap();
    let renewed = rig.state.registry.get(&old.sref()).await.unwrap().unwrap();
    assert!(renewed.expires_at_ms > replacement.expires_at_ms);
    assert!(renewed.expires_at_ms < Some(crate::shard::now_ms() + 120_000));
    store.release_hold();
    old_ticket.wait().await.unwrap();
    rig.state.registry.invalidate(&old.sref());
    let after = rig.state.registry.get(&old.sref()).await.unwrap().unwrap();
    assert_eq!(
        (after.stream_epoch.as_str(), after.expires_at_ms),
        (renewed.stream_epoch.as_str(), renewed.expires_at_ms)
    );
    rig.tasks.shutdown(Duration::from_secs(1)).await;
    rig.state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09a_ttl_overflow_returns_retryable_http_refusals_before_append_effects() {
    use super::fixture_requests::hreq;
    let (rig, store, descriptors) = setup(160).await;
    let service = rig.state.creation_service();
    let target = rig.state.deployment.raw_adapter_sref("ttl-overload");
    let mut desc = crate::application::creation::fresh_desc(
        &service,
        &target,
        &skey(),
        "application/octet-stream".into(),
        Some(600),
        None,
    );
    desc.expires_at_ms = Some(crate::shard::now_ms() + 300_000);
    let original = rig.state.registry.create(desc).await.unwrap().1;
    let held = store.hold_class(StoreOp::Put, ObjClass::Other, 256);
    let _ = service.touch_ttl(&descriptors[0]);
    entered(&held).await;
    for desc in &descriptors {
        let _ = service.touch_ttl(desc);
    }
    for (method, body) in [("GET", &b""[..]), ("POST", &b"payload"[..])] {
        let (status, headers, body) = hreq(
            rig.addr,
            method,
            "/v1/stream/ttl-overload",
            &[("content-type", "application/octet-stream")],
            body,
        )
        .await;
        assert_eq!(status, 503, "{method}: {}", String::from_utf8_lossy(&body));
        assert_eq!(headers.get("retry-after").map(String::as_str), Some("1"));
        assert!(String::from_utf8_lossy(&body).contains("ttl_renewal_unavailable"));
    }
    assert_eq!(
        rig.state.shards.open_count(),
        0,
        "renewal refusal precedes any append/engine side effect"
    );
    rig.state.registry.invalidate(&target);
    assert_eq!(
        rig.state
            .registry
            .get(&target)
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms,
        original.expires_at_ms
    );
    store.release_hold();
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.pending_ttl_for_tests() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (status, _, body) = hreq(rig.addr, "GET", "/v1/stream/ttl-overload", &[], b"").await;
    assert_eq!(status, 200);
    assert!(body.is_empty(), "the refused append wrote no data");
    rig.state.registry.invalidate(&target);
    assert!(
        rig.state
            .registry
            .get(&target)
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms
            > original.expires_at_ms,
        "a successful retry observes completed renewal"
    );
    rig.tasks.shutdown(Duration::from_secs(1)).await;
    rig.state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
}
