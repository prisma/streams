//! `Streams-<Name>` request headers on the product surface (edge change
//! #173): the platform edge in front of `*.prisma.build` strips every
//! request header named `Prisma-*`, so each request header the product
//! surface reads as `Prisma-<Name>` is also read as `Streams-<Name>`.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

/// A valid key other than `PRISMA_KEY` ([8u8; 32]).
const OTHER_KEY: &str = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=";

fn records(body: &[u8]) -> Vec<serde_json::Value> {
    serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("records body: {e}: {}", String::from_utf8_lossy(body)))
}

fn error_code(body: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    v["error"]["code"].as_str().unwrap_or("").to_string()
}

/// Create, keyed append, keyed and default reads, and the consumer
/// create/get/delete saga, every request naming its headers only in the
/// `Streams-` form: each answers as the `Prisma-` form does, and the
/// record lands under the routing key the alias carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_named_request_headers_serve_the_product_surface() {
    let (state, addr) = http_rig(mem()).await;
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/al",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "create: {}", String::from_utf8_lossy(&b));
    let keyed = [
        ("streams-encryption-key", PRISMA_KEY),
        ("streams-routing-key", "c1"),
    ];
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/al/records",
        &keyed,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200, "append: {}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/al/records?routingKey=c1",
        &key,
        b"",
    )
    .await;
    assert_eq!(st, 200, "keyed read: {}", String::from_utf8_lossy(&b));
    assert_eq!(records(&b), vec![serde_json::json!({"n": 1})]);
    let (st, _, b) = preq(addr, "GET", "/v1/streams/al/records", &key, b"").await;
    assert_eq!(st, 200, "default read: {}", String::from_utf8_lossy(&b));
    assert_eq!(
        records(&b),
        Vec::<serde_json::Value>::new(),
        "nothing on the default key"
    );

    let cfg = br#"{"visibilityTimeoutMs":5000,"maxAttempts":3}"#;
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/al/consumers/w", &key, cfg).await;
    assert_eq!(st, 201, "consumer create: {}", String::from_utf8_lossy(&b));
    let (st, h, _) = preq(addr, "GET", "/v1/streams/al/consumers/w", &key, b"").await;
    assert_eq!(st, 200);
    let version = h
        .get("prisma-consumer-version")
        .expect("response keeps the Prisma- name")
        .clone();
    let delete = [
        ("streams-encryption-key", PRISMA_KEY),
        ("streams-consumer-version", version.as_str()),
    ];
    let (st, _, b) = preq(addr, "DELETE", "/v1/streams/al/consumers/w", &delete, b"").await;
    assert_eq!(st, 204, "consumer delete: {}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(addr, "GET", "/v1/streams/al/consumers/w", &key, b"").await;
    assert_eq!(st, 404, "the consumer is gone");
    engine_shutdown(&state).await;
}

/// Both forms with different values are refused before any handler runs
/// (no stream, no record); both forms with one value are one header.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differing_prisma_and_streams_values_are_refused() {
    let (state, addr) = http_rig(mem()).await;
    let both = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("streams-encryption-key", OTHER_KEY),
    ];
    let (st, h, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cf",
        &both,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!((st, error_code(&b).as_str()), (400, "conflicting_header"));
    assert_eq!(
        h.get("access-control-allow-origin").map(String::as_str),
        Some("*")
    );
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(addr, "GET", "/v1/streams/cf", &key, b"").await;
    assert_eq!(st, 404, "the refused create made no stream");

    let same = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("Streams-Encryption-Key", PRISMA_KEY),
    ];
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cf",
        &same,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "equal values: {}", String::from_utf8_lossy(&b));
    let routed = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "c1"),
        ("streams-routing-key", "c2"),
    ];
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/records",
        &routed,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!((st, error_code(&b).as_str()), (400, "conflicting_header"));
    for path in [
        "/v1/streams/cf/records?routingKey=c1",
        "/v1/streams/cf/records?routingKey=c2",
        "/v1/streams/cf/records",
    ] {
        let (st, _, b) = preq(addr, "GET", path, &key, b"").await;
        assert_eq!(st, 200, "{path}");
        assert_eq!(
            records(&b),
            Vec::<serde_json::Value>::new(),
            "{path}: the refused append wrote nothing"
        );
    }
    engine_shutdown(&state).await;
}

/// A browser may send the `Streams-` forms: the product preflight allows
/// each of them beside its `Prisma-` form.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_product_preflight_allows_the_streams_forms() {
    let (state, addr) = http_rig(mem()).await;
    let (st, h, _) = preq(addr, "OPTIONS", "/v1/streams/pf/records", &[], b"").await;
    assert_eq!(st, 204);
    let allowed: Vec<String> = h
        .get("access-control-allow-headers")
        .expect("allow-headers")
        .split(',')
        .map(|name| name.trim().to_string())
        .collect();
    for name in ["encryption-key", "routing-key", "consumer-version"] {
        assert!(allowed.contains(&format!("prisma-{name}")), "{allowed:?}");
        assert!(allowed.contains(&format!("streams-{name}")), "{allowed:?}");
    }
    engine_shutdown(&state).await;
}
