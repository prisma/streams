//! `Streams-<Name>` request headers on the product surface (edge change
//! #173): the platform edge in front of `*.prisma.build` strips every
//! request header named `Prisma-*`, so each request header the product
//! surface reads as `Prisma-<Name>` is also read as `Streams-<Name>`;
//! it strips `Prisma-*` response headers too, so every one is also sent
//! as `Streams-<Name>` (edge change #176).

use std::collections::HashMap;

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
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

/// Every `prisma-` header of `headers` is also there under its `Streams-`
/// name with the same value, and each of `expected` is among them.
fn assert_mirrored(what: &str, headers: &HashMap<String, String>, expected: &[&str]) {
    for name in expected {
        assert!(headers.contains_key(*name), "{what}: {name} in {headers:?}");
    }
    for (name, value) in headers {
        if let Some(rest) = name.strip_prefix("prisma-") {
            // `prisma-streams-origin` is `streams-origin`: one prefix.
            let twin = if rest.starts_with("streams-") {
                rest.to_string()
            } else {
                format!("streams-{rest}")
            };
            assert_eq!(
                headers.get(&twin),
                Some(value),
                "{what}: {name}'s streams- form in {headers:?}"
            );
        }
    }
}

/// The edge strips `Prisma-*` response headers too (edge change #176):
/// each product answer that carries a `Prisma-<Name>` header carries
/// `Streams-<Name>` with the same value, and a browser may read both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn product_answers_send_each_prisma_response_header_as_its_streams_name() {
    let (state, addr) = http_rig(mem()).await;
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (st, h, _) = preq(
        addr,
        "PUT",
        "/v1/streams/rs",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    assert_mirrored("create", &h, &["prisma-streams-origin"]);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/rs/records", &key, br#"{"n":1}"#).await;
    assert_eq!(st, 200);
    let (st, h, b) = preq(addr, "GET", "/v1/streams/rs/records", &key, b"").await;
    assert_eq!(st, 200);
    assert_eq!(records(&b), vec![serde_json::json!({"n": 1})]);
    assert_mirrored("read", &h, &["prisma-next-cursor", "prisma-up-to-date"]);
    let exposed: Vec<&str> = h["access-control-expose-headers"]
        .split(',')
        .map(str::trim)
        .collect();
    for name in [
        "next-cursor",
        "up-to-date",
        "sealed",
        "next-scan-cursor",
        "scan-complete",
        "durable-cursor",
        "pending-from",
        "consumer-version",
    ] {
        assert!(
            exposed.contains(&format!("prisma-{name}").as_str()),
            "{exposed:?}"
        );
        assert!(
            exposed.contains(&format!("streams-{name}").as_str()),
            "{exposed:?}"
        );
    }
    let (st, h, _) = preq(addr, "GET", "/v1/streams/rs:scan", &key, b"").await;
    assert_eq!(st, 200);
    assert_mirrored("scan", &h, &["prisma-scan-complete"]);
    let cfg = br#"{"visibilityTimeoutMs":5000,"maxAttempts":3}"#;
    let (st, h, _) = preq(addr, "PUT", "/v1/streams/rs/consumers/w", &key, cfg).await;
    assert_eq!(st, 201);
    assert_mirrored("consumer create", &h, &["prisma-consumer-version"]);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/rs:seal", &key, b"{}").await;
    assert!(st == 200 || st == 204, "seal: {st}");
    let (st, h, _) = preq(addr, "GET", "/v1/streams/rs/records", &key, b"").await;
    assert_eq!(st, 200);
    assert_mirrored("sealed read", &h, &["prisma-sealed", "prisma-next-cursor"]);
    engine_shutdown(&state).await;
}

/// The mirror is the server's, not the product route's: the listing, the
/// raw surface and an unrouted path send it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn every_surface_sends_its_prisma_response_headers_as_streams_names() {
    let (state, addr) = http_rig(mem()).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    for (method, path) in [
        ("GET", "/v1/streams"),
        ("GET", "/v1/stream/absent"),
        ("GET", "/v1/streams/absent"),
        ("GET", "/no/such/route"),
    ] {
        let (_, h, _) = preq(addr, method, path, &key, b"").await;
        assert_mirrored(path, &h, &["prisma-streams-origin", "streams-origin"]);
        assert!(!h.contains_key("streams-streams-origin"), "{path}: {h:?}");
    }
    engine_shutdown(&state).await;
}
