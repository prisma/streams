//! Runtime-local denial/ops journals, alerts and HTTP body ceilings.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::Arc;

async fn journal(state: &Arc<crate::http::AppState>, stream: &str) -> Vec<serde_json::Value> {
    let key = state.billing.usage_key().unwrap();
    let (body, _) = crate::billing::system_read(state, stream, &key, None)
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_runtime_journals_and_alerts_do_not_cross_drain_or_resolve() {
    let mut states = Vec::new();
    for incarnation in [1, 2] {
        let auth = Arc::new(
            crate::auth::AuthService::new(
                crate::auth::AuthMode::Enforce,
                "https://auth.prisma.io".into(),
                "test-cell",
            )
            .unwrap(),
        );
        let rig = http_rig_build(
            mem(),
            RigRuntime::incarnation(incarnation),
            HttpRigOptions {
                auth_service: Some(auth),
                ..Default::default()
            },
        )
        .await;
        states.push(rig.state);
    }
    for (index, state) in states.iter().enumerate() {
        state
            .runtime
            .ops
            .emit(crate::ops::OpsEvent::new("owned", format!("owner-{index}")));
        let refusal = crate::audit::tag(
            axum::response::IntoResponse::into_response(axum::http::StatusCode::UNAUTHORIZED),
            "unauthorized",
        );
        crate::audit::observe_denial(
            state,
            &format!("route-{index}"),
            &axum::http::Method::GET,
            &refusal,
        );
    }
    // B drains first while A has queued events. Each ledger still receives
    // exactly its own event, not whatever arrived first process-wide.
    for index in [1, 0] {
        let state = &states[index];
        assert_eq!(crate::ops::drain_ops_once(state).await.unwrap(), 1);
        assert_eq!(crate::audit::drain_audit_once(state).await.unwrap(), 1);
        let ops = journal(state, crate::billing::OPS_EVENTS_STREAM).await;
        let audit = journal(state, crate::billing::AUDIT_EVENTS_STREAM).await;
        assert_eq!(ops.len(), 1);
        assert_eq!(audit.len(), 1);
        assert_eq!(ops[0]["event_id"], format!("owner-{index}"));
        assert_eq!(audit[0]["route"], format!("route-{index}"));
        assert!(
            audit[0]["event_id"]
                .as_str()
                .unwrap()
                .contains(&state.runtime.identity.boot_id)
        );
        assert!(audit[0]["project_id"].is_null());
        assert_eq!(state.runtime.ops.recent(10).len(), 1);
    }
    let a = &states[0];
    let b = &states[1];
    let mut breached = crate::ops::collect_snapshot(a);
    breached
        .counters
        .insert("ops_events_dropped_total".into(), 1);
    crate::ops::evaluate_alerts(a, &breached).await;
    assert!(
        a.runtime
            .ops
            .open_alerts()
            .iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    );
    assert!(b.runtime.ops.open_alerts().is_empty());
    crate::ops::evaluate_alerts(b, &crate::ops::collect_snapshot(b)).await;
    assert!(
        a.runtime
            .ops
            .open_alerts()
            .iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    );
    assert_eq!(
        b.runtime.ops.recent(10).len(),
        1,
        "B cannot emit A's resolution"
    );
    crate::ops::evaluate_alerts(a, &crate::ops::collect_snapshot(a)).await;
    assert!(
        !a.runtime
            .ops
            .open_alerts()
            .iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    );
    engine_shutdown(a).await;
    engine_shutdown(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_runtime_body_limits_apply_to_each_collector_and_preflight() {
    let mut rigs = Vec::new();
    for (incarnation, limit) in [(1, 64 * 1024), (2, 128 * 1024)] {
        rigs.push(
            http_rig_build(
                mem(),
                RigRuntime::incarnation(incarnation),
                HttpRigOptions {
                    max_request_body_bytes: Some(limit),
                    ..Default::default()
                },
            )
            .await,
        );
    }
    let payload = bytes::Bytes::from(vec![b'x'; 96 * 1024]);
    for (index, rig) in rigs.iter().enumerate() {
        let raw = [
            ("stream-encryption-key", PRISMA_KEY),
            ("content-type", "application/octet-stream"),
        ];
        let product = [
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/octet-stream"),
        ];
        assert_eq!(
            preq(rig.addr, "PUT", "/v1/stream/raw", &raw, b"").await.0,
            201
        );
        assert_eq!(
            preq(
                rig.addr,
                "PUT",
                "/v1/streams/product",
                &product,
                br#"{"format":{"kind":"bytes"}}"#
            )
            .await
            .0,
            201
        );
        let raw_status = preq(rig.addr, "POST", "/v1/stream/raw", &raw, &payload)
            .await
            .0;
        let product_status = preq(
            rig.addr,
            "POST",
            "/v1/streams/product/records",
            &product,
            &payload,
        )
        .await
        .0;
        let create_status = preq(rig.addr, "PUT", "/v1/stream/initial", &raw, &payload)
            .await
            .0;
        if index == 0 {
            assert_eq!((raw_status, product_status, create_status), (413, 413, 413));
        } else {
            assert!(matches!(raw_status, 200 | 204));
            assert_eq!((product_status, create_status), (200, 201));
        }
        // No Content-Length: exercise the incremental raw collector separately
        // from the declared-length rejection above.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("stream-encryption-key", PRISMA_KEY.parse().unwrap());
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        let chunks = [
            Ok::<_, std::io::Error>(payload.slice(..48 * 1024)),
            Ok(payload.slice(48 * 1024..)),
        ];
        let result = crate::http::append_typed(
            rig.state.clone(),
            rig.state.deployment.raw_adapter_sref("raw"),
            headers,
            axum::body::Body::from_stream(futures_util::stream::iter(chunks)),
            None,
            None,
            None,
        )
        .await;
        if index == 0 {
            assert_eq!(
                result.unwrap_err().code,
                crate::application::append::AppendCode::TooLarge
            );
        } else {
            assert!(result.is_ok());
        }
        assert_eq!(
            rig.state.runtime.history.worst_frame_transient,
            crate::history::worst_frame_transient_for(rig.state.config.cli.max_request_body_bytes)
        );
    }
    for rig in rigs {
        engine_shutdown(&rig.state).await;
    }
}
