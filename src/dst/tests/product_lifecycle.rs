//! Product lifecycle.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// Typed creation, idempotence, config conflict, metadata shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_create_metadata_roundtrip() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let cfg = br#"{"format":{"kind":"json"},"expiry":{"idle":"30d"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        cfg,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["name"], "customers/acme/orders");
    assert_eq!(v["contentType"], "application/json");
    assert_eq!(v["sealed"], false);
    assert_eq!(v["expiry"]["idle"], "2592000s");
    assert_eq!(v["watches"][0]["name"], "by-customer");

    // Idempotent re-PUT → 200.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        cfg,
    )
    .await;
    assert_eq!(st, 200);

    // Different immutable config → 409.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"7d"}}"#,
    )
    .await;
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "config_mismatch");
    assert_eq!(v["error"]["retryable"], false);

    // Metadata GET: product shape, no internals leaked.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/customers/acme/orders", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["watches"][0]["fields"][0], "/customerId");
    let text = String::from_utf8_lossy(&b).to_string();
    for leak in ["fingerprint", "segment", "route_hash", "layout_version"] {
        assert!(!text.contains(leak), "metadata leaks {leak}: {text}");
    }
    // Unknown config fields rejected (v1 typo guard).
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/typoed",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"profile":"queue"}"#,
    )
    .await;
    assert_eq!(st, 400);
    engine_shutdown(&state).await;
}

/// The clean switch rejects experimental product inputs; __ds is
/// reserved on both surfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_clean_switch_rejections() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // Legacy header on the product route → 400, never translated.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/legacy",
        &[("stream-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 400);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "unknown_field");
    // Legacy query names rejected.
    let (st, _, _) = preq(addr, "GET", "/v1/streams/legacy?key=x", &[], b"").await;
    assert_eq!(st, 400);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/legacy?offset=0", &[], b"").await;
    assert_eq!(st, 400);
    // Reserved namespace: product name, raw create, raw subpath.
    // Since the telemetry cutover the whole `_` prefix refuses as
    // 403 reserved_stream (docs/OBSERVABILITY-BILLING.md §8/§15) —
    // earlier it fell through to the name grammar's 400. Refusal
    // either way; the guard now names the reason.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/__ds/x",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 403);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/stream/__ds",
        &[
            ("stream-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        b"",
    )
    .await;
    // Raw `__ds` create: refused as reserved since the telemetry
    // cutover (was the __ds dead-route 400/404 family).
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(addr, "GET", "/v1/stream/__ds/subscriptions", &[], b"").await;
    assert_eq!(st, 404);
    // Reserved final segments can never be stream names: the path
    // parses as the records SUBRESOURCE of stream "a" (PUT on records
    // is method_not_allowed), so no stream named "a/records" exists.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/a/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 405);
    engine_shutdown(&state).await;
}

/// Product seal is durable, idempotent, collection-wide, and the RAW
/// default-key view observes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_seal_collection_wide() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealme",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Raw append on the default key (shared collection).
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealme",
        &[("content-type", "application/json")],
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 204, "raw append {st}");

    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealme:seal", &[], b"{}").await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealme:seal", &[], b"{}").await;
    assert_eq!(st, 200, "seal is idempotent");

    let (st, _, b) = preq(addr, "GET", "/v1/streams/sealme", &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], true);

    // Raw view: further appends refuse; drained read reports closure.
    let (st, _, b2) = hreq(
        addr,
        "POST",
        "/v1/stream/sealme",
        &[("content-type", "application/json")],
        br#"[{"n":2}]"#,
    )
    .await;
    assert_eq!(
        st,
        409,
        "sealed collection refuses appends: {}",
        String::from_utf8_lossy(&b2)
    );
    let (st, h, _) = hreq(addr, "GET", "/v1/stream/sealme", &[], b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        h.get("stream-closed").map(String::as_str),
        Some("true"),
        "raw default-key view reports closure: {h:?}"
    );
    engine_shutdown(&state).await;
}

/// Stage 1 exit criteria: profile machinery is GONE — removed product
/// inputs are rejected (never translated), removed routes are unknown,
/// and the descriptor no longer carries profile fields (enforced at
/// compile time by their absence; this test pins the wire behavior).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profiles_are_removed_from_every_surface() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    // Product surface: removed names are 400 unknown_field.
    for h in [
        "stream-profile",
        "stream-touch-templates",
        "stream-queue-max-deliveries",
        "stream-ttl",
    ] {
        let (st, _, b) = preq(
            addr,
            "PUT",
            "/v1/streams/np",
            &[("prisma-encryption-key", PRISMA_KEY), (h, "queue")],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 400, "{h}: {}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["error"]["code"], "unknown_field", "{h}");
    }
    // Removed profile routes are plain unknown routes — no alias, no
    // deprecation surface.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/qs",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    for path in ["/v1/stream/qs/queue/w/receive", "/v1/stream/qs/touch/meta"] {
        // Body-less probes: the 404 path never reads a request body, and
        // an unread body can turn the server's close into a RST before
        // the client reads the response (macOS, parallel-suite timing).
        let (st, _, _) = hreq(addr, "POST", path, &[], b"").await;
        assert!(
            st == 404 || st == 400 || st == 405,
            "removed route {path} must not exist (got {st})"
        );
    }
    // The raw route IGNORES unknown headers per the pinned protocol —
    // a Stream-Profile header neither errors nor configures anything.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/qp",
        &[
            ("content-type", "application/json"),
            ("stream-profile", "queue"),
        ],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "raw create ignores unknown headers");
    engine_shutdown(&state).await;
}

/// Stage 7 §14: the raw PUT and the product create resolve to ONE
/// stream incarnation. A raw idempotent PUT against a product-created
/// stream compares protocol config only (watches unchanged); a product
/// create against a raw-created stream succeeds when the immutable
/// config matches (empty capability config); equivalent duration
/// spellings normalize to the same config.
#[expect(
    clippy::too_many_lines,
    reason = "dual creation scenario; the raw idempotent PUT and the product create resolve against one incarnation in order; helper phases would hide which surface created a second incarnation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_creation_dual_contract() {
    let store = mem();
    let (state, addr) = http_rig(store).await;

    // Product create with watches, then a raw idempotent PUT.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"30d"},"watches":[{"name":"w","fields":["/a"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = hreq(
        addr,
        "PUT",
        "/v1/stream/dual1",
        &[
            ("content-type", "application/json"),
            ("stream-ttl", "2592000"),
        ],
        b"",
    )
    .await;
    assert_eq!(
        st,
        200,
        "raw PUT compares protocol config only: {}",
        String::from_utf8_lossy(&b)
    );
    // Watches survived the raw PUT.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/dual1/watches",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        v["watches"][0]["name"], "w",
        "raw PUT must not clear watches"
    );

    // Raw create, then product create with matching config: one
    // incarnation, not a duplicate; records flow both ways.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/dual2",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/dual2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        200,
        "product open of a raw stream: {}",
        String::from_utf8_lossy(&b)
    );
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dual2",
        &[("content-type", "application/json")],
        br#"[{"via":"raw"}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/dual2/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        recs[0]["via"], "raw",
        "one canonical sequence across surfaces"
    );

    // Different immutable config conflicts (409), never merges.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual2",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"}}"#,
    )
    .await;
    assert_eq!(st, 409);

    // Equivalent duration spellings normalize identically: create with
    // 30d, retry with 720h -> 200 (same normalized seconds).
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual3",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"30d"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/dual3",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"},"expiry":{"idle":"720h"}}"#,
    )
    .await;
    assert_eq!(
        st,
        200,
        "30d == 720h after normalization: {}",
        String::from_utf8_lossy(&b)
    );

    // The product create stored NO records (config is never content).
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/dual3/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert!(recs.is_empty(), "product create must not append its config");

    // Watches on a bytes stream are rejected at creation.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dual4",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"},"watches":[{"name":"w","fields":["/a"]}]}"#,
    )
    .await;
    assert_eq!(st, 400, "watches require JSON");
    engine_shutdown(&state).await;
}

/// Stage 8 §7.2 + §10: seal with an atomic final append (deduped under
/// a producer retry), and the paginated catalog list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_seal_final_append_and_catalog() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    for s in ["cat-a", "cat-b", "cat-c"] {
        let path = format!("/v1/streams/{s}");
        let (st, _, _) = preq(
            addr,
            "PUT",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    // Seal cat-a with a final record through a producer (retry dedups).
    let hdrs = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "closer"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let body = br#"{"final":{"type":"completed"},"routingKey":"c1"}"#;
    let (st, _, b) = preq(addr, "POST", "/v1/streams/cat-a:seal", &hdrs, body).await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    // Retry the same seal: the final append dedups, seal is idempotent.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/cat-a:seal", &hdrs, body).await;
    assert!(st == 200 || st == 204);
    // Exactly one final record, and the collection is sealed.
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/cat-a/records?routingKey=c1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "final record exactly once");
    assert_eq!(recs[0]["type"], "completed");
    assert_eq!(h.get("prisma-sealed").map(String::as_str), Some("true"));
    // Further appends refuse.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/cat-a/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{\"late\":1}",
    )
    .await;
    assert_eq!(st, 409);

    // Catalog: paginated, name-ordered, sealed flag surfaced.
    let (st, _, b) = preq(addr, "GET", "/v1/streams?limit=2", &[], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let page1: Vec<String> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page1, vec!["cat-a", "cat-b"]);
    assert_eq!(v["streams"][0]["sealed"], true);
    let cur = v["cursor"].as_str().unwrap().to_string();
    let path = format!("/v1/streams?limit=2&cursor={cur}");
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let page2: Vec<String> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page2, vec!["cat-c"]);
    assert!(v["cursor"].is_null(), "final page carries no cursor");
    engine_shutdown(&state).await;
}

/// Appendix §8: the 12-case dual-surface equivalence corpus — for the
/// default routing key, equivalent operations through the raw standards
/// route and the product route resolve to ONE collection incarnation
/// with identical canonical data and lifecycle state, in both orders.
#[expect(
    clippy::too_many_lines,
    reason = "dual-surface corpus scenario; the twelve equivalent operation pairs are checked against one stream in order; helper phases would hide which pair diverged"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_surface_equivalence_corpus() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let ct = [("content-type", "application/json")];

    // 1. Product create -> raw append -> product read.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq1",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/eq1", &ct, br#"[{"c":1}]"#).await;
    assert!(st == 200 || st == 204, "case 1 raw append {st}");
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq1/records", &pk, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "case 1");
    assert_eq!(recs[0]["c"], 1);

    // 2. Raw create -> product append (no routing key) -> raw read.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq2", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = preq(addr, "POST", "/v1/streams/eq2/records", &pk, br#"{"c":2}"#).await;
    assert_eq!(st, 200);
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/eq2", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "case 2");
    assert_eq!(recs[0]["c"], 2);

    // 3. Raw producer append -> product read. 4. Product producer
    // append -> raw read. One producer scope, one sequence.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq3", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let rawp = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/eq3", &rawp, br#"[{"c":3}]"#).await;
    assert!(st == 200 || st == 204, "case 3 {st}");
    let prodp = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "1"),
    ];
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/eq3/records",
        &prodp,
        br#"{"c":4}"#,
    )
    .await;
    assert_eq!(
        st, 200,
        "case 4: the product append continues the RAW producer's sequence"
    );
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq3/records", &pk, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2, "cases 3+4 share one sequence");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/eq3", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2);

    // 5. Product seal -> raw closed-tail read. 6-adjacent: raw HEAD
    // reports the closure.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/eq3:seal", &pk, b"{}").await;
    assert!(st == 200 || st == 204);
    let (st, h, _) = hreq(addr, "GET", "/v1/stream/eq3", &[], b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        h.get("stream-closed").map(String::as_str),
        Some("true"),
        "case 5"
    );

    // 6. Raw close -> product metadata sealed.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq6", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/eq6",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        br#"[{"fin":true}]"#,
    )
    .await;
    assert!(st == 200 || st == 204, "case 6 close {st}");
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq6", &pk, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], true, "case 6");

    // 7. Product delete -> raw gone. 8. Raw delete -> product gone.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq7",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/eq7", &pk, b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/eq7", &[], b"").await;
    assert_eq!(st, 404, "case 7");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/eq8", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/eq8", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/eq8", &pk, b"").await;
    assert_eq!(st, 404, "case 8");

    // 9. Raw TTL create -> product metadata expiry. 10. Product idle
    // expiry -> raw HEAD TTL.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/eq9",
        &[("content-type", "application/json"), ("stream-ttl", "3600")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq9", &pk, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["expiry"]["idle"].is_string() || v["expiry"].is_object(),
        "case 9: product metadata reflects the raw TTL: {v}"
    );
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq10",
        &pk,
        br#"{"format":{"kind":"json"},"expiry":{"idle":"1h"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/eq10", &[], b"").await;
    assert_eq!(st, 200);
    assert!(
        h.contains_key("stream-ttl"),
        "case 10: raw HEAD reports TTL"
    );

    // 11. Product-created JSON stream -> raw JSON array flattening.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/eq11",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/eq11",
        &ct,
        br#"[{"a":1},{"a":2}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(addr, "GET", "/v1/streams/eq11/records", &pk, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2, "case 11: raw flattening stored two messages");

    // 12. Token classes never cross: a product cursor is rejected as a
    // raw offset; a raw offset is rejected as a product cursor.
    let (st, h, _) = preq(addr, "GET", "/v1/streams/eq11/records", &pk, b"").await;
    assert_eq!(st, 200);
    let cursor = h.get("prisma-next-cursor").unwrap().clone();
    let path = format!("/v1/stream/eq11?offset={cursor}");
    let (st, _, _) = hreq(addr, "GET", &path, &[], b"").await;
    assert_eq!(st, 400, "case 12a: product cursor on the raw route");
    let (st, h, _) = hreq(addr, "GET", "/v1/stream/eq11", &[], b"").await;
    assert_eq!(st, 200);
    let raw_off = h.get("stream-next-offset").unwrap().clone();
    let path = format!("/v1/streams/eq11/records?cursor={raw_off}");
    let (st, _, _) = preq(addr, "GET", &path, &pk, b"").await;
    assert_eq!(st, 400, "case 12b: raw offset on the product route");
    engine_shutdown(&state).await;
}
