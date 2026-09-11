//! Security routes.

use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::ObjectStore;
use std::sync::Arc;

/// A rig whose AppState carries an account token, for the negative
/// authorization matrix.
async fn http_rig_auth(
    store: Arc<dyn ObjectStore>,
    token: &str,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(
        store,
        RigRuntime::first(),
        HttpRigOptions {
            auth: Some(token.to_string()),
            ..Default::default()
        },
    )
    .await
    .parts()
}

/// Collection names are hierarchical, so the product routes have to be
/// matched as SUFFIXES. Searching for the first `/records/` in the path
/// split `customers/records/2026/records` after `customers` — writing
/// to a collection nobody asked for, or 404ing when it did not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hierarchical_names_do_not_shadow_subresources() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];

    // A name whose MIDDLE segments spell subresources is legal.
    let deep = "customers/records/2026";
    let (st, _, b) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{deep}"),
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{deep}/records"),
        &key,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    // The record landed in the deep collection, and `customers` was
    // never created as a side effect.
    let (st, _, b) = preq(
        addr,
        "GET",
        &format!("/v1/streams/{deep}/records"),
        &key,
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/customers", &key, b"").await;
    assert_eq!(st, 404, "the prefix must not become a collection");

    // A consumer may be called "records": the suffix that wins is the
    // one that leaves an addressable collection behind.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/shop",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/shop/consumers/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        b"{}",
    )
    .await;
    assert!(st == 200 || st == 201, "{}", String::from_utf8_lossy(&b));

    // That URL is the consumer route, always — a creation document sent
    // there is a bad consumer config, never a collection called
    // `shop/consumers/records`. Which is why such a name is refused
    // wherever one can still be written down: as a dead-letter target.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/shop/consumers/records",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 400);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "invalid_config");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/shop/consumers/w",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        br#"{"deadLetterStream":"shop/consumers/records"}"#,
    )
    .await;
    assert_eq!(st, 400, "an unaddressable name is not a usable target");
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "invalid_config");

    // A colon is legal in a name; only the known verbs are verbs.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/ns/a:b",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "a colon is part of the name");
    let (st, _, b) = preq(addr, "GET", "/v1/streams/ns/a:b", &key, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["name"], "ns/a:b");
    // A mistyped verb addresses a collection that does not exist —
    // never a different verb, and never the collection without it.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/ns/a:seel", &key, b"").await;
    assert_eq!(st, 404);
    engine_shutdown(&state).await;
}

/// The signed watch URL is the ONE product route that authorizes
/// itself, and "looks like a watch URL" was decided by substring tests
/// on the raw path. Collection names are hierarchical, so
/// `acme/watches/x/keys/y/extra` is a legal COLLECTION whose path
/// contains every fragment a watch URL has — it, and its `/records`
/// subresource, skipped the account token entirely. Records could be
/// read with the encryption key alone, which is exactly the credential
/// separation the product surface exists to keep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_exact_signed_watch_route_skips_the_token() {
    let store = mem();
    let (state, addr) = http_rig_auth(store, "tok").await;
    let auth = [
        ("authorization", "Bearer tok"),
        ("prisma-encryption-key", PRISMA_KEY),
    ];
    // A collection whose NAME carries every watch-URL fragment.
    let evil = "acme/watches/x/keys/y/extra";
    let (st, _, b) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{evil}"),
        &auth,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{evil}/records"),
        &auth,
        br#"{"secret":"x"}"#,
    )
    .await;
    assert_eq!(st, 200);

    // Without the token, with a capability pasted on, every one of
    // these is 401 — the observation exception is EXACTLY the watch
    // wait route, and the retired sig= spelling buys nothing anywhere.
    let key_only = [("prisma-encryption-key", PRISMA_KEY)];
    for path in [
        format!("/v1/streams/{evil}?cap=1.aa"),
        format!("/v1/streams/{evil}/records?cap=1.aa"),
        format!("/v1/streams/{evil}/records?routingKey=&cap=1.aa"),
        format!("/v1/streams/{evil}/watches?cap=1.aa"),
        format!("/v1/streams/{evil}/consumers/c?cap=1.aa"),
        format!("/v1/streams/{evil}/records?sig=anything"),
        // a watch-shaped path with EXTRA segments after the key
        "/v1/streams/acme/watches/w/keys/0011223344556677/extra?cap=1.aa".to_string(),
        "/v1/streams/acme/watches/w/keys/0011223344556677/extra?sig=x".to_string(),
    ] {
        let (st, _, b) = preq(addr, "GET", &path, &key_only, b"").await;
        assert_eq!(
            st,
            401,
            "token bypass via {path}: {}",
            String::from_utf8_lossy(&b)
        );
    }
    // The write path is refused too, before any body is read.
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{evil}/records?sig=anything"),
        &key_only,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 401);

    // The exact signed route still works without a token: create a
    // collection WITH a watch, derive the URL, present it bare.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/wauth",
        &auth,
        br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (_, _, b) = preq(addr, "GET", "/v1/streams/wauth", &auth, b"").await;
    let meta: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let epoch: [u8; 16] = crate::crypto::unhex(meta["epoch"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let khex = crate::product::watch_key_hex("w", &["/id".to_string()], &[r#""a""#.to_string()]);
    let tok = crate::crypto::touch_token(&key, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let cap = format!(
        "proj-test.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &crate::crypto::wait_sig_key(&tok, &epoch),
            &state.deployment.raw_adapter_sref("wauth"),
            meta["epoch"].as_str().unwrap(),
            "w",
            &khex,
            "GET",
            exp,
        )
    );
    let path =
        format!("/v1/streams/wauth/watches/w/keys/{khex}?cursor=now&timeoutMs=150&cap={cap}");
    let (st, _, b) = preq(addr, "GET", &path, &[], b"").await;
    assert_eq!(
        st,
        200,
        "the exact signed route must still self-authorize: {}",
        String::from_utf8_lossy(&b)
    );
    // …but not without the signature.
    let bare = format!("/v1/streams/wauth/watches/w/keys/{khex}?cursor=now&timeoutMs=150");
    let (st, _, _) = preq(addr, "GET", &bare, &[], b"").await;
    assert_eq!(st, 401);
    engine_shutdown(&state).await;
}

/// CORS that only answers preflights is not CORS: the browser passes
/// the OPTIONS and then blocks the response it was asking about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_responses_carry_cors_not_just_preflights() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, h, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cors",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    assert_eq!(
        h.get("access-control-allow-origin").map(String::as_str),
        Some("*")
    );
    let expose = |h: &std::collections::HashMap<String, String>| {
        h.get("access-control-expose-headers")
            .cloned()
            .unwrap_or_default()
    };
    assert!(
        expose(&h).contains("prisma-next-cursor"),
        "{:?}",
        expose(&h)
    );

    // an actual GET…
    let (st, h, _) = preq(addr, "GET", "/v1/streams/cors/records", &key, b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        h.get("access-control-allow-origin").map(String::as_str),
        Some("*")
    );
    assert!(expose(&h).contains("prisma-sealed"));
    // …a POST…
    let (st, h, _) = preq(
        addr,
        "POST",
        "/v1/streams/cors/records",
        &key,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(
        h.get("access-control-allow-origin").map(String::as_str),
        Some("*")
    );
    // …and an ERROR, which a browser must be able to read to retry.
    let (st, h, _) = preq(addr, "GET", "/v1/streams/nope-missing", &key, b"").await;
    assert_eq!(st, 404);
    assert_eq!(
        h.get("access-control-allow-origin").map(String::as_str),
        Some("*")
    );
    assert!(expose(&h).contains("retry-after"));
    engine_shutdown(&state).await;
}

/// The seal machinery mints producer identities for records nobody
/// coordinated. They share the durable producer keyspace, so a caller
/// who could NAME one could pre-create its row at sequence 0 and turn a
/// later final append into a false duplicate — sealing the collection
/// without ever writing the record. The wire refuses the reserved
/// prefix, and a duplicate that did not close is never accepted as a
/// final.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_producer_identities_are_unreachable_from_the_wire() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/nsguard",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let reserved = format!("{}seal.whatever", crate::shard::INTERNAL_PRODUCER_PREFIX);
    // Raw route refuses it…
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/nsguard",
        &[
            ("content-type", "application/json"),
            ("producer-id", reserved.as_str()),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"[{"n":1}]"#,
    )
    .await;
    assert_eq!(
        st,
        400,
        "the wire could name an internal producer: {}",
        String::from_utf8_lossy(&b)
    );
    // …and so does the product route.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/nsguard/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", reserved.as_str()),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 400);

    // A producer whose sequence already committed a NON-closing record
    // cannot be reused to "seal": the duplicate does not close, so the
    // seal must refuse rather than mark a final it never wrote.
    let ph = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "pp"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/nsguard/records",
        &ph,
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/nsguard:seal",
        &ph,
        br#"{"final":{"n":1}}"#,
    )
    .await;
    assert_eq!(
        st,
        409,
        "a non-closing duplicate was accepted as the final: {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("nsguard"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("nsguard"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "the collection sealed without its final record");
    assert!(
        d.sealing.is_none(),
        "a refused attempt left its intent: {:?}",
        d.sealing
    );
    engine_shutdown(&state).await;
}

/// SECURITY (audit P0): the account token gates EVERY product
/// operation when configured. The encryption key is a separate
/// credential and never substitutes for it. The one exception is the
/// signed watch observation URL — an explicit delegated capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_requires_the_account_token() {
    let store = mem();
    let (state, addr) = http_rig_auth(store, "s3cret").await;
    let bear = |t: &'static str| ("authorization", t);
    let ok = [
        ("authorization", "Bearer s3cret"),
        ("prisma-encryption-key", PRISMA_KEY),
    ];
    // Create needs the token; the key alone is not enough.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/sec",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        401,
        "key without token must not create: {}",
        String::from_utf8_lossy(&b)
    );
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "unauthorized");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sec",
        &[bear("Bearer wrong"), ("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 401, "wrong token rejected");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sec",
        &ok,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "token + key creates");

    // Every other product operation: tokenless is 401.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sec/records", &ok, b"{\"n\":1}").await;
    assert_eq!(st, 200);
    for (m, path, body) in [
        ("GET", "/v1/streams/sec", &b""[..]),
        ("POST", "/v1/streams/sec/records", &b"{\"n\":2}"[..]),
        ("POST", "/v1/streams/sec/records:batch", &b"[{\"n\":3}]"[..]),
        ("GET", "/v1/streams/sec/records", &b""[..]),
        (
            "GET",
            "/v1/streams/sec/records:long-poll?waitMs=50",
            &b""[..],
        ),
        ("GET", "/v1/streams/sec:scan", &b""[..]),
        ("PUT", "/v1/streams/sec/consumers/w", &b"{}"[..]),
        ("GET", "/v1/streams/sec/consumers/w", &b""[..]),
        ("POST", "/v1/streams/sec/consumers/w:pull", &b"{}"[..]),
        ("POST", "/v1/streams/sec/consumers/w:settle", &b"{}"[..]),
        ("DELETE", "/v1/streams/sec/consumers/w", &b""[..]),
        ("GET", "/v1/streams/sec/watches", &b""[..]),
        ("GET", "/v1/streams", &b""[..]),
        ("POST", "/v1/streams/sec:seal", &b"{}"[..]),
        ("DELETE", "/v1/streams/sec", &b""[..]),
    ] {
        let (st, _, _) = preq(
            addr,
            m,
            path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            body,
        )
        .await;
        assert_eq!(st, 401, "{m} {path} must require the account token");
    }
    // The stream still exists (no tokenless delete/seal took effect).
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sec", &ok, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], false, "tokenless seal must not have landed");

    // Token + WRONG key is 403 (authorization passes, key access fails).
    let wrong_key = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg";
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/sec/records",
        &[bear("Bearer s3cret"), ("prisma-encryption-key", wrong_key)],
        b"",
    )
    .await;
    assert_eq!(st, 403, "token ok, key wrong -> 403");

    // Browser preflight is answered WITHOUT credentials (a preflight
    // never carries them) and advertises the product headers.
    let (st, h, _) = preq(addr, "OPTIONS", "/v1/streams/sec/records", &[], b"").await;
    assert!(st == 200 || st == 204, "preflight status {st}");
    assert!(
        h.contains_key("access-control-allow-headers"),
        "preflight must allow the product headers"
    );
    let (st, _, _) = preq(addr, "OPTIONS", "/v1/streams", &[], b"").await;
    assert!(st == 200 || st == 204, "catalog preflight status {st}");
    engine_shutdown(&state).await;
}

/// Round-13 review (red): authentication precedes tarpit work and
/// capacity answers. The ordinary inflight admission gate lived in
/// PRE-auth middleware — an unauthenticated caller at a saturated
/// instance burned a 25 ms tarpit slot and received a 429 capacity
/// answer before auth ever ran, giving an anonymous flood both a
/// tarpit-slot lever against unrelated projects and free capacity
/// posture. Contract: unauthenticated => fast 401 with no capacity
/// information; the typed tarpitted 429 answers only AFTER bearer +
/// stream-key auth; pre-auth keeps only the catastrophic survival
/// bound (4x the cap; instant, generic).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_admission_answers_only_after_authentication() {
    let store = mem();
    let (state, addr) = http_rig_auth(store, "tok").await;
    let auth = ("authorization", "Bearer tok");
    let ct = ("content-type", "text/plain");
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/adm", &[auth, ct], b"seed").await;
    assert!(
        st == 200 || st == 201,
        "seed: {st} {}",
        String::from_utf8_lossy(&b)
    );

    // Saturate the ordinary cap (over cap, under the 4x survival bound).
    state.admission.set_max_inflight(4);
    state.admission.add_inflight_for_test(8);

    // 1. Unauthenticated at saturation: 401, NO 25 ms tarpit, and no
    //    capacity vocabulary in the body. (elapsed < 25ms is exclusive
    //    with the tarpit's guaranteed >= 25ms sleep.)
    let t0 = std::time::Instant::now();
    let (st, _, body) = hreq(addr, "POST", "/v1/stream/adm", &[ct], b"x").await;
    let dt = t0.elapsed();
    assert_eq!(st, 401, "unauthenticated gets 401, not a capacity answer");
    assert!(
        dt < std::time::Duration::from_millis(25),
        "no pre-auth tarpit: {dt:?}"
    );
    let text = String::from_utf8_lossy(&body).to_string();
    assert!(
        !text.contains("capacity") && !text.contains("overloaded"),
        "no capacity posture pre-auth:\n{text}"
    );

    // 2. Authenticated over-cap append: the typed tarpitted 429.
    let t0 = std::time::Instant::now();
    let (st, _, body) = hreq(addr, "POST", "/v1/stream/adm", &[auth, ct], b"x").await;
    assert_eq!(st, 429, "{}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("admission capacity"),
        "typed refusal:\n{}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        t0.elapsed() >= std::time::Duration::from_millis(25),
        "the authenticated refusal keeps the tarpit"
    );
    assert!(state.admission.snapshot().shed.inflight > 0);

    // 3. Catastrophic survival bound (>4x cap): pre-auth instant
    //    generic refusal — sockets are being defended, no tarpit.
    state.admission.add_inflight_for_test(100);
    let t0 = std::time::Instant::now();
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/adm", &[ct], b"x").await;
    assert_eq!(st, 503, "survival bound answers even unauthenticated");
    assert!(
        t0.elapsed() < std::time::Duration::from_millis(25),
        "the survival refusal never tarpits"
    );
    assert!(state.admission.snapshot().shed.survival > 0);

    // 4. Pressure released: an authenticated append flows again.
    state.admission.add_inflight_for_test(-108);
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/adm", &[auth, ct], b"y").await;
    assert!(
        st == 200 || st == 201 || st == 204,
        "recovered: {st} {}",
        String::from_utf8_lossy(&b)
    );
}
