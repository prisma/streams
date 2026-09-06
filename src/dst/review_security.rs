//! Regression mechanisms for R15/R16: operate the actual entry boundary with
//! a body whose first poll is observable and which otherwise never completes.
use super::*;

async fn sentinel_request(
    state: Arc<crate::http::AppState>,
    method: axum::http::Method,
    path: &str,
    authorization: Option<&str>,
) -> axum::response::Response {
    use std::sync::atomic::AtomicUsize;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let body = axum::body::Body::from_stream(futures_util::stream::poll_fn(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::<Option<Result<bytes::Bytes, std::io::Error>>>::Pending
    }));
    let mut request = axum::http::Request::builder()
        .uri(format!("/v1/streams/{path}"))
        .method(method.clone())
        // An oversized declared body must also remain unpolled.
        .header("content-length", u64::MAX.to_string());
    if let Some(auth) = authorization {
        request = request.header("authorization", auth);
    }
    let request = request.body(body).unwrap();
    let headers = request.headers().clone();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        crate::http::product_entry_axum_inner(
            state,
            path.split('?').next().unwrap().to_string(),
            method,
            headers,
            request,
        ),
    )
    .await
    .expect("bodyless or rejected request must not wait for the body");
    assert_eq!(polls.load(Ordering::SeqCst), 0, "request body was polled");
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r15_bodyless_and_unauthorized_requests_never_poll_the_body() {
    use axum::http::{Method, StatusCode};
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            auth: Some("secret".into()),
            ..Default::default()
        },
    )
    .await;
    for (method, path, auth, expected) in [
        (Method::OPTIONS, "orders", None, StatusCode::NO_CONTENT),
        (Method::GET, "orders", None, StatusCode::UNAUTHORIZED),
        (
            Method::POST,
            "orders/records",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            Method::GET,
            "orders/watches/w/keys/nothex",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            Method::GET,
            "orders/watches/w/keys/nothex?cap=broken",
            None,
            StatusCode::FORBIDDEN,
        ),
        (
            Method::GET,
            "orders/watches/w/keys/0000000000000000?cap=proj-test.1.deadbeef",
            None,
            StatusCode::FORBIDDEN,
        ),
        (
            Method::GET,
            "orders/watches/w/keys/0000000000000000",
            Some("Prisma-Watch proj-test.1.deadbeef"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let response = sentinel_request(rig.state.clone(), method, path, auth).await;
        assert_eq!(response.status(), expected, "{path}");
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
    }
    rig.tasks.shutdown(std::time::Duration::from_secs(2)).await;
    engine_shutdown(&rig.state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r16_unauthorized_watch_shapes_are_independent_of_existence() {
    use axum::http::{Method, StatusCode};
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    for name in ["live", "initializing", "deleted"] {
        let (status, _, body) = preq(
            rig.addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/id"]}]}"#,
        )
        .await;
        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    }
    let project = crate::tenant::ProjectId::new("proj-test").unwrap();
    for name in ["initializing", "deleted"] {
        rig.state
            .registry
            .cas_update(&project.stream_ref(name), |d| {
                if name == "deleted" {
                    d.deleted = true;
                } else {
                    d.init = Some(crate::registry::InitState {
                        request_hash: "request".into(),
                        key_fingerprint: d.key_fingerprint.clone(),
                        claimed_ms: 1,
                    });
                }
                true
            })
            .await
            .unwrap();
    }
    for cap in [
        "proj-test.1.deadbeef",
        "proj-test.999999999999.00000000000000000000000000000000",
        "proj-test.bad.signature",
    ] {
        for key in ["nothex", "deadbeef", "0000000000000000"] {
            for query in ["", "?bogus=1", "?cursor=a&cursor=b", "?timeoutMs=bad"] {
                let mut baseline = None;
                for name in ["missing", "live", "initializing", "deleted"] {
                    let response = sentinel_request(
                        rig.state.clone(),
                        Method::GET,
                        &format!("{name}/watches/w/keys/{key}{query}"),
                        Some(&format!("Prisma-Watch {cap}")),
                    )
                    .await;
                    assert_eq!(response.status(), StatusCode::FORBIDDEN);
                    let headers = response.headers().clone();
                    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                        .await
                        .unwrap();
                    let current = (headers, body);
                    if let Some(expected) = &baseline {
                        assert_eq!(&current, expected, "{name} {key} {query}");
                    } else {
                        baseline = Some(current);
                    }
                }
            }
        }
    }
    rig.tasks.shutdown(std::time::Duration::from_secs(2)).await;
    engine_shutdown(&rig.state).await;
}
