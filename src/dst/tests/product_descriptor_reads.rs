//! A product handler's own descriptor read (NEXT-WORK item 3's remainder,
//! edge change #64): a store failure is retryable, corruption is not.
use super::fixture_http::{engine_shutdown, http_rig, install_rollup};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

/// The product routes whose handler reads the descriptor itself and
/// classifies the failure: metadata, scan, append and usage. `:seal` and the
/// keyed read are not yet among them: their handlers' scopes carry exact
/// exception-growth rows, which only the owner can re-approve.
const ROUTES: [(&str, &str, &[u8]); 4] = [
    ("GET", "/v1/streams/typed-product-read", b""),
    ("GET", "/v1/streams/typed-product-read:scan", b""),
    (
        "POST",
        "/v1/streams/typed-product-read/records",
        br#"{"n":1}"#,
    ),
    ("GET", "/v1/streams/typed-product-read/usage/current", b""),
];

/// What a route answered: status, error code, retryable, Retry-After.
async fn answer(
    addr: std::net::SocketAddr,
    (method, path, body): (&str, &str, &[u8]),
) -> (u16, String, bool, Option<String>) {
    let (status, headers, bytes) = preq(
        addr,
        method,
        path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        body,
    )
    .await;
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (
        status,
        error["error"]["code"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        error["error"]["retryable"].as_bool().unwrap_or_default(),
        headers.get("retry-after").cloned(),
    )
}

/// A store failure on a product handler's own descriptor read answers 503
/// `temporarily_unavailable`, retryable, with `Retry-After: 1`: nothing was
/// read or written for the request, so the SDK's retry is right. A stored
/// descriptor that does not decode stays a fail-closed 500 that is not
/// retryable, since a retry reads the same bytes. Red before: both answered
/// 500 `internal` `retryable: true`, which the SDK does not retry, for a
/// store blip, and invited retries of corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_product_descriptor_read_the_store_fails_is_retryable_and_corruption_is_not() {
    use object_store::ObjectStoreExt;
    let (state, addr) = http_rig(mem()).await;
    // The usage route refuses before its descriptor read without a rollup.
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let name = "typed-product-read";
    let (status, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/typed-product-read",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201);
    for route in ROUTES {
        state.registry.fail_next_get(name);
        assert_eq!(
            answer(addr, route).await,
            (
                503,
                "temporarily_unavailable".into(),
                true,
                Some("1".into())
            ),
            "{} {}",
            route.0,
            route.1
        );
    }
    let sref = state.deployment.raw_adapter_sref(name);
    let path = object_store::path::Path::from(format!(
        "registry/v4/projects/{}/streams/{}.json",
        crate::crypto::hex(sref.project_id().as_bytes()),
        crate::crypto::hex(name.as_bytes())
    ));
    state
        .data_store
        .put(&path, bytes::Bytes::from_static(b"{ not json").into())
        .await
        .unwrap();
    state.registry.invalidate(&sref);
    for route in ROUTES {
        let (status, code, retryable, retry_after) = answer(addr, route).await;
        assert_eq!(
            (status, retryable, retry_after),
            (500, false, None),
            "{} {}: {code}",
            route.0,
            route.1
        );
    }
    engine_shutdown(&state).await;
}
