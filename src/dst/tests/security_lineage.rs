//! Security lineage.

use super::fixture_auth::sr_rig;
use super::fixture_http::{engine_shutdown, http_rig_with_auth_service};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// RED (Søren review, blocker 1): a product DELETE authorized for
/// project B must operate on B end to end. Today delete_stream loads
/// B's descriptor and then hands delete_lifecycle the BARE NAME, which
/// reloads and CAS-mutates state.deployment.raw_adapter_sref(name) — the DEPLOYMENT tenant.
/// D-is-A variant: the deployment tenant also owns "orders", so the
/// bug deletes A and leaves B alive behind a 204.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_stays_inside_the_requesting_project() {
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    let now = crate::shard::now_ms() / 1000;
    let svc = std::sync::Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        "del-1".to_string(),
        crate::auth::JwksKey {
            alg: jsonwebtoken::Algorithm::RS256,
            key: jsonwebtoken::DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
            fp: crate::auth::key_fp(PUB.as_bytes()),
        },
    );
    svc.publish_jwks(crate::auth::JwksSnapshot {
        keys,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.lifecycle.manage";
    let pid = crate::tenant::ProjectId::new("proj-delb").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new("ws_delb").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 1,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas::default(),
        },
    );
    let mut credentials = std::collections::HashMap::new();
    credentials.insert(
        std::sync::Arc::from("c_delb"),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from("c_delb"),
            project_id: pid,
            grant_version: 1,
            status: crate::project_policy::CredentialStatus::Active,
            scopes: crate::tenant::ScopeSet::parse(scopes).0,
            grant: crate::tenant::StreamGrant::All,
            expires_at: None,
        },
    );
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let (state, addr) = http_rig_with_auth_service(mem(), svc).await;

    #[derive(serde::Serialize)]
    struct C<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        credential_id: &'a str,
        project_id: &'a str,
        workspace_id: &'a str,
        cell_id: &'a str,
        ownership_version: u64,
        grant_version: u64,
        scope: &'a str,
        jti: &'a str,
        iat: i64,
        exp: i64,
    }
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("del-1".into());
    let token = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: "c_delb",
            project_id: "proj-delb",
            workspace_id: "ws_delb",
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: scopes,
            jti: "t",
            iat: now - 60,
            exp: now + 600,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    let auth_hdr = format!("Bearer {token}");
    let auth = ("authorization", auth_hdr.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);

    // D (deployment tenant) owns "orders" via the raw surface, SAME
    // encryption key as B — a different key would turn an identity bug
    // into a 403 and hide the cross-project write. The raw surface is
    // internal under enforce (SR-5): stage as the fleet operator.
    let ct = [("content-type", "application/json")];
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/orders",
        &[ct[0], fleet],
        br#"[{"d":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 201, "raw create: {st}");
    // B owns "orders" via the product surface.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "product create for B");

    // B deletes ITS stream.
    let (st, _, b) = preq(addr, "DELETE", "/v1/streams/orders", &[ekey, auth], b"").await;
    assert_eq!(st, 204, "B's delete: {st} {}", String::from_utf8_lossy(&b));

    // B's stream must be GONE on the product surface...
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/orders/records",
        &[ekey, auth],
        b"",
    )
    .await;
    assert!(
        st == 404 || st == 410,
        "B's stream must be deleted for B, got {st}"
    );
    // ...and the DEPLOYMENT stream must be untouched.
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("orders"))
        .await
        .expect("registry read");
    assert!(
        d.as_ref().is_some_and(|d| crate::http::desc_alive(d)),
        "deployment tenant's same-named stream must be untouched by B's delete: {d:?}"
    );
    engine_shutdown(&state).await;
}

/// RED (Søren review, blocker 2): an append whose resolved segment is
/// sealed mid-transition refreshes the descriptor via state.deployment.raw_adapter_sref(name)
/// — the DEPLOYMENT tenant — and adopts it without any project/epoch
/// revalidation. With A and B same-named and (validly) sharing an
/// encryption key, B's append lands durably in A's stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transition_append_stays_inside_the_requesting_project() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read";
    let (state, addr, tok) = sr_rig("proj-trab", "ws_trab", "c_trab", "tra-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = [("content-type", "application/json")];

    // A (deployment tenant) owns "orders", alive and OPEN, same key.
    // Raw is internal under enforce: stage as the fleet operator.
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/orders",
        &[ct[0], fleet],
        br#"[{"who":"a"}]"#,
    )
    .await;
    assert!(st == 200 || st == 201, "raw create: {st}");
    // B owns "orders" on the product surface.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "B create: {}", String::from_utf8_lossy(&b));

    // Stage B mid-split: its only segment is SEALED with no successor
    // published yet — the exact state the append refresh path handles.
    let bref = crate::tenant::ProjectId::new("proj-trab")
        .unwrap()
        .stream_ref("orders");
    state
        .registry
        .cas_update(&bref, |d| {
            d.segments = Some(crate::segmap::SegmentMap {
                version: 1,
                next_seg_id: 1,
                segments: vec![crate::segmap::SegmentDesc {
                    seg_id: 0,
                    lo: 0,
                    hi: crate::segmap::KEYSPACE_END,
                    shard_prefix: String::new(),
                    route_hash: [0u8; 16],
                    created_ms: 1,
                    predecessors: Vec::new(),
                    successors: Vec::new(),
                    sealed_ms: Some(1),
                    sealed_next_offset: Some(0),
                }],
                pending: None,
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate(&bref);

    // B appends into ITS mid-transition stream.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[ekey, auth],
        br#"{"who":"b-crossed"}"#,
    )
    .await;
    // Fixed behavior: a 503 segment_transition retry (B's successor is
    // not published). Broken behavior: 200 — the record went SOMEWHERE.
    // Either way, the isolation invariant below is what must hold.
    let _ = st;

    // A's stream must contain none of B's bytes — and must still be
    // READABLE: today the adopted-descriptor append poisons A's
    // segment state badly enough that A's own read answers 500.
    let (st, _, body) = hreq(addr, "GET", "/v1/stream/orders", &[fleet], b"").await;
    assert_eq!(
        st,
        200,
        "A's stream must remain readable after B's transition append: {}",
        String::from_utf8_lossy(&body)
    );
    let text = String::from_utf8_lossy(&body);
    assert!(
        !text.contains("b-crossed"),
        "B's mid-transition append leaked into the deployment tenant's stream: {text}"
    );
    engine_shutdown(&state).await;
}

/// RED (Søren review, blocker 2, read side): a read cursor naming a
/// segment outside the stream's lineage refreshes via state.deployment.raw_adapter_sref(name)
/// and recurses into the DEPLOYMENT tenant's descriptor. With same
/// name and (validly) the same key, B's read serves A's records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_lineage_read_stays_inside_the_requesting_project() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read";
    let (state, addr, tok) = sr_rig("proj-lrb", "ws_lrb", "c_lrb", "lrb-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = [("content-type", "application/json")];

    // A (deployment tenant) owns "orders" and holds a segment id (1)
    // that B's lineage does not know. Raw is internal under enforce:
    // stage as the fleet operator.
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/orders", &[ct[0], fleet], b"[]").await;
    assert!(st == 200 || st == 201, "raw create: {st}");
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("orders"), |d| {
            d.segments = Some(crate::segmap::SegmentMap {
                version: 1,
                next_seg_id: 2,
                segments: vec![crate::segmap::SegmentDesc {
                    seg_id: 1,
                    lo: 0,
                    hi: crate::segmap::KEYSPACE_END,
                    shard_prefix: String::new(),
                    route_hash: [0u8; 16],
                    created_ms: 1,
                    predecessors: Vec::new(),
                    successors: Vec::new(),
                    sealed_ms: None,
                    sealed_next_offset: None,
                }],
                pending: None,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("orders"));
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/orders",
        &[ct[0], fleet],
        br#"[{"who":"a-secret"}]"#,
    )
    .await;
    assert!(st == 200 || st == 204, "raw append to A: {st}");

    // B owns "orders" with its own single-segment MAP (segment 0,
    // live) — the v3 lineage read path, same key.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-lrb")
        .unwrap()
        .stream_ref("orders");
    state
        .registry
        .cas_update(&bref, |d| {
            d.segments = Some(crate::segmap::SegmentMap {
                version: 2,
                next_seg_id: 3,
                segments: vec![
                    crate::segmap::SegmentDesc {
                        seg_id: 0,
                        lo: 0,
                        hi: crate::segmap::KEYSPACE_END,
                        shard_prefix: String::new(),
                        route_hash: [0u8; 16],
                        created_ms: 1,
                        predecessors: Vec::new(),
                        successors: vec![2],
                        sealed_ms: Some(1),
                        sealed_next_offset: Some(0),
                    },
                    crate::segmap::SegmentDesc {
                        seg_id: 2,
                        lo: 0,
                        hi: crate::segmap::KEYSPACE_END,
                        shard_prefix: String::new(),
                        route_hash: [0u8; 16],
                        created_ms: 2,
                        predecessors: vec![0],
                        successors: Vec::new(),
                        sealed_ms: None,
                        sealed_next_offset: None,
                    },
                ],
                pending: None,
            });
            true
        })
        .await
        .unwrap();
    state.registry.invalidate(&bref);

    // B reads with a VALID, B-bound signed cursor naming segment 1 —
    // a segment the cell's cached lineage for B has not seen (the
    // cursor is legitimate; only the cell's map knowledge is stale).
    let bdesc = state.registry.get(&bref).await.unwrap().unwrap();
    let epoch: [u8; 16] = crate::crypto::unhex(&bdesc.stream_epoch)
        .unwrap()
        .try_into()
        .unwrap();
    let skey = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let cur = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: crate::crypto::stream_hash(""),
        seg_id: 1,
        offset: 0,
    }
    .encode(&crate::tenant::ProjectId::new("proj-lrb").unwrap(), &skey);
    let (st, _, body) = preq(
        addr,
        "GET",
        &format!("/v1/streams/orders/records?cursor={cur}"),
        &[ekey, auth],
        b"",
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    // Fixed behavior: invalid_offset (the segment is not in B's
    // lineage, and the refresh must stay inside B). Broken behavior:
    // the refresh adopts A's descriptor and serves A's records.
    assert!(
        !text.contains("a-secret"),
        "B's stale-lineage read served the deployment tenant's records \
         (status {st}): {text}"
    );
    engine_shutdown(&state).await;
}

/// RED (Søren review): stale JWKS is a CELL fault — the token is
/// fine. Mapping it to 401 tells clients to refresh a valid
/// credential during a key-feed outage; it must be a retryable 503
/// like the other own-feed staleness classes.
#[test]
fn stale_jwks_maps_to_retryable_503() {
    let r = crate::product::auth_failure_response(&crate::auth::AuthError::KeysStale);
    assert_eq!(
        r.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "KeysStale must answer 503, not {}",
        r.status()
    );
}

/// RED (Søren review): TTL slides reconstruct identity from the bare
/// name — the slide task CASes state.deployment.raw_adapter_sref(name), the DEPLOYMENT
/// tenant. A non-deployment project's active stream fails the epoch
/// fence and expires despite traffic; same-name projects suppress one
/// another's slides through the global name-keyed in-flight set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_slide_stays_inside_the_owning_project() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read";
    let (state, addr, tok) = sr_rig("proj-ttlb", "ws_ttlb", "c_ttlb", "ttl-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = [("content-type", "application/json")];

    // D (deployment) owns a same-named stream WITHOUT a TTL. Raw is
    // internal under enforce: stage as the fleet operator.
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/orders", &[ct[0], fleet], b"[]").await;
    assert!(st == 200 || st == 201);
    // B owns "orders" with a TTL whose window is nearly exhausted.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-ttlb")
        .unwrap()
        .stream_ref("orders");
    let now = crate::shard::now_ms();
    state
        .registry
        .cas_update(&bref, |d| {
            d.ttl_secs = Some(60);
            d.expires_at_ms = Some(now + 5_000);
            true
        })
        .await
        .unwrap();
    state.registry.invalidate(&bref);

    // Drive the slide exactly as B's traffic would.
    let bdesc = state.registry.get(&bref).await.unwrap().unwrap();
    crate::http::touch_ttl(&state, &bdesc);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    state.registry.invalidate(&bref);
    let after = state.registry.get(&bref).await.unwrap().unwrap();
    assert!(
        after.expires_at_ms.unwrap_or(0) > now + 40_000,
        "B's active stream must have its TTL slid (got {:?}, wanted ~{})",
        after.expires_at_ms,
        now + 60_000
    );
    engine_shutdown(&state).await;
}

/// RED (Søren review): the seal fence resolves the collection via
/// state.deployment.raw_adapter_sref(name). For any non-deployment project the fence loads
/// the WRONG (or no) descriptor and the seal machinery reports "the
/// collection no longer exists" for a perfectly healthy stream — the
/// D-has-no-orders fixture variant that detects silent misdirection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_fence_resolves_the_owning_project() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.lifecycle.manage";
    let (state, addr, tok) = sr_rig("proj-fenb", "ws_fenb", "c_fenb", "fen-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);

    // ONLY B owns "orders" — the deployment tenant has no such stream.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-fenb")
        .unwrap()
        .stream_ref("orders");
    let bdesc = state.registry.get(&bref).await.unwrap().unwrap();
    let fenced =
        crate::http::fence_segment_for_key(&state, &bref, &bdesc.stream_epoch, "", 1).await;
    assert!(
        fenced.is_ok(),
        "the fence must resolve B's own collection: {fenced:?}"
    );
    engine_shutdown(&state).await;
}
