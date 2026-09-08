//! The scan RPC accepts old omissions without accepting malformed present bounds.
use super::fixture_http::{
    HttpRig, HttpRigOptions, cold_absorber, engine_shutdown, http_rig_build,
};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, skey};
use crate::application::read_remote::InternalTarget;
use std::time::Duration;

async fn seed(rig: &HttpRig) -> crate::registry::StreamDesc {
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            "/v1/streams/compat",
            &headers,
            br#"{"format":{"kind":"bytes"}}"#
        )
        .await
        .0,
        201
    );
    for payload in [b"x", b"y", b"z"] {
        assert_eq!(
            preq(
                rig.addr,
                "POST",
                "/v1/streams/compat/records",
                &headers,
                payload
            )
            .await
            .0,
            200
        );
    }
    rig.state
        .registry
        .get(&rig.state.deployment.raw_adapter_sref("compat"))
        .await
        .unwrap()
        .unwrap()
}

fn request(rig: &HttpRig, target: &InternalTarget) -> reqwest::RequestBuilder {
    let mut request = crate::peer::client()
        .get(format!(
            "http://{}/v1/internal/segment-scan/compat",
            rig.addr
        ))
        .bearer_auth("dst-internal-token")
        .header("streams-internal-from", "1")
        .header("streams-internal-max-bytes", "1024")
        .header("stream-encryption-key", PRISMA_KEY);
    for (key, value) in target.headers() {
        request = request.header(key, value);
    }
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o2c_old_scan_headers_preserve_records_progress_and_physical_frontier() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            absorber: Some(cold_absorber()),
            ..Default::default()
        },
    )
    .await;
    let desc = seed(&rig).await;
    let target = InternalTarget::of(&desc, 0).unwrap();
    for end in [None, Some("18446744073709551615"), Some("2"), Some("1")] {
        let mut request = request(&rig, &target);
        if let Some(end) = end {
            request = request.header("streams-internal-end", end);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200);
        let page: serde_json::Value = response.json().await.unwrap();
        let items = match end {
            Some("1") => serde_json::json!([]),
            Some("2") => serde_json::json!([{"off":1,"rk":"","p":"eQ=="}]),
            _ => serde_json::json!([{"off":1,"rk":"","p":"eQ=="},{"off":2,"rk":"","p":"eg=="}]),
        };
        assert_eq!(page["items"], items);
        assert_eq!(
            page["end"], 3,
            "request clipping cannot shrink the durable frontier"
        );
        assert_eq!(page["completed"], true);
        assert_eq!(
            page["last"],
            match end {
                Some("1") => serde_json::Value::Null,
                Some("2") => serde_json::json!(1),
                _ => serde_json::json!(2),
            }
        );
    }
    for invalid in ["", "no", "-1", "18446744073709551616", "1.5", "é"] {
        let response = request(&rig, &target)
            .header("streams-internal-end", invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "present invalid end: {invalid:?}");
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["error"]["code"],
            "invalid_body"
        );
    }
    for name in target.headers().into_iter().map(|(name, _)| name).chain([
        "streams-internal-from",
        "streams-internal-max-bytes",
        "stream-encryption-key",
    ]) {
        let mut raw = request(&rig, &target).build().unwrap();
        raw.headers_mut().remove(name);
        assert!(
            !crate::peer::client()
                .execute(raw)
                .await
                .unwrap()
                .status()
                .is_success(),
            "identity header {name} remains mandatory"
        );
    }
    let mut wrong = request(&rig, &target).build().unwrap();
    wrong.headers_mut().insert(
        "stream-encryption-key",
        "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk"
            .parse()
            .unwrap(),
    );
    let wrong = crate::peer::client().execute(wrong).await.unwrap();
    assert_eq!(wrong.status(), 403);
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}

// A contract fixture for the pre-bound receiver: it ignores the new header and
// executes an open-ended physical scan. This is not an old compiled binary.
async fn legacy_scan(
    state: axum::extract::State<std::sync::Arc<crate::http::AppState>>,
    path: axum::extract::Path<String>,
    mut headers: axum::http::HeaderMap,
) -> axum::response::Response {
    assert_eq!(headers.remove("streams-internal-end").unwrap(), "1");
    crate::product::internal_segment_scan(state, path, headers).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o2c_new_sender_clips_legacy_pages_across_owner_upgrade_and_rollback() {
    let store = mem();
    let owner = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("inst-a".into()),
            absorber: Some(cold_absorber()),
            ..Default::default()
        },
    )
    .await;
    let desc = seed(&owner).await;
    let reader = http_rig_build(
        store,
        RigRuntime::incarnation(1),
        HttpRigOptions {
            instance: Some("inst-b".into()),
            ..Default::default()
        },
    )
    .await;
    reader
        .state
        .ownership
        .set_ring_active(vec!["inst-a".into(), "inst-b".into()]);
    for prefix in reader.state.shards.prefixes() {
        reader.state.ownership.set_override(prefix, "inst-a");
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let legacy_addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route(
            "/v1/internal/segment-scan/{*name}",
            axum::routing::get(legacy_scan),
        )
        .with_state(owner.state.clone());
    let tasks = owner.tasks.clone();
    let _server = owner.tasks.spawn(
        "legacy-scan-contract",
        crate::tasks::Policy::Critical,
        move |_cancel| async move {
            crate::http::serve_h1(listener, app, 64 * 1024, tasks)
                .await
                .unwrap();
            crate::tasks::TaskResult::Done
        },
    );
    // Begin on the old contract, upgrade, then roll the owner back while the
    // current reader keeps its frozen cursor and peer client.
    for addr in [legacy_addr, owner.addr, legacy_addr] {
        reader
            .state
            .peer
            .set_peer("inst-a", &format!("http://{addr}"));
        let probe = crate::application::read_retention_probe::Probe::default();
        let result = probe
            .scope(reader.state.read_service().execute_scan(
                crate::application::read_scan::ScanCommand {
                    descriptor: desc.clone(),
                    key: skey(),
                    max_bytes: 1024,
                    now_ms: 0,
                    lifetime_ms: 1000,
                    cursor: Some(crate::product_cursor::ScanCursor {
                        epoch: desc.epoch(),
                        map_version: desc.segments.as_ref().map_or(0, |m| m.version),
                        segments: vec![(0, 1)],
                        current_index: 0,
                        current_offset: 0,
                        expires_at_ms: i64::MAX,
                    }),
                },
            ))
            .await
            .unwrap();
        assert!(result.continuation.is_none());
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].off, 0);
        assert_eq!(result.records[0].payload.as_ref(), b"x");
        assert_eq!(
            probe.live(),
            1,
            "discarded legacy suffix does not retain storage"
        );
        drop(result);
        assert_eq!(probe.live(), 0);
    }
    engine_shutdown(&reader.state).await;
    engine_shutdown(&owner.state).await;
    reader.tasks.shutdown(Duration::from_secs(5)).await;
    owner.tasks.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o2c_unauthorized_scan_does_not_wait_for_request_body() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_auth: Some((Some("fleet-secret".into()), None)),
            ..Default::default()
        },
    )
    .await;
    let mut socket = tokio::net::TcpStream::connect(rig.addr).await.unwrap();
    socket.write_all(b"GET /v1/internal/segment-scan/absent HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\n").await.unwrap();
    let mut socket = tokio::io::BufReader::new(socket);
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_line(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with("HTTP/1.1 401"),
        "reject before the withheld body arrives"
    );
    drop(socket);
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}
