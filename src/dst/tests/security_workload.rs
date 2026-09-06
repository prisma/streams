//! Security workload.

use super::fixture_auth::{sr_rig, sr2_workload_jwt, sr2_workload_jwt_exp};
use super::fixture_failpoints::gap_lock;
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_livefeed::{hub_sse_collect, sse_head};
use super::fixture_requests::{PRISMA_KEY, RIG_KEY_B64, hreq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;

/// RED (round-4 finding 2): a RAW live subscription opened under a
/// short-lived workload JWT must reach EOF at token expiry. The gate
/// used to return only a boolean and DISCARD the verified principal's
/// `expires_at`, so `GET ...?live=sse` parked forever after the JWT
/// died — permanent read authority from a temporary credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_sse_terminates_at_workload_token_expiry() {
    // LEASE_TERMINATIONS is process-global; serialize with the other
    // expiry tests that move it (gap_lock convention).
    let _serial = gap_lock().lock().await;
    let scopes = "streams.create streams.records.append streams.records.read";
    let (_state, addr, _tok) = sr_rig("proj-rwl", "ws_rwl", "c_rwl", "rwl-1", scopes).await;
    let ct = ("content-type", "application/json");
    // Stage the stream as the static fleet operator (bridge posture),
    // then subscribe under the SHORT-LIVED workload identity.
    let fleet = ("authorization", "Bearer dst-internal-token");
    let ekey = ("stream-encryption-key", RIG_KEY_B64);
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/rwl",
        &[ct, fleet, ekey],
        br#"[{"i":0}]"#,
    )
    .await;
    assert!(st == 200 || st == 201, "stage: {st}");

    // Expires in 4 s: long enough to open and park, short enough to
    // watch the deadline fire.
    let now = crate::shard::now_ms() / 1000;
    let wl = format!(
        "Bearer {}",
        sr2_workload_jwt_exp("rwl-1", &["raw-read"], now + 4)
    );
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::AsyncWriteExt;
    let req = format!(
        "GET /v1/stream/rwl?live=sse&offset=now HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nauthorization: {wl}\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let (head, head_text) = sse_head(&mut sck).await;
    assert_eq!(
        head, 200,
        "workload-JWT raw SSE must be authorized:\n{head_text}"
    );
    let (body, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(body.contains("upToDate"), "parks before expiry:\n{body}");
    // No appends, no feed activity: the JWT's expiry ALONE must end
    // the connection.
    let (_, eof) = hub_sse_collect(&mut sck, 10, |_| false).await;
    assert!(
        eof,
        "raw SSE must terminate at its workload token's expiry, not outlive it"
    );
}

/// Round-4 finding 2 (companion): /v1/internal/segment-read is a
/// bounded relay PAGE route; live semantics are refused outright so
/// no parked subscription can hang off an internal page route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_segment_read_refuses_live_semantics() {
    let scopes = "streams.create streams.records.append streams.records.read";
    let (_state, addr, _tok) = sr_rig("proj-rlv", "ws_rlv", "c_rlv", "rlv-1", scopes).await;
    let ct = ("content-type", "application/json");
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rlv", &[ct, fleet], br#"[{"i":0}]"#).await;
    assert!(st == 200 || st == 201, "stage: {st}");
    let now = crate::shard::now_ms() / 1000;
    let sa = format!(
        "Bearer {}",
        sr2_workload_jwt("rlv-1", &["segment-read"], now)
    );
    let (st, _, body) = hreq(
        addr,
        "GET",
        "/v1/internal/segment-read/rlv?live=sse&offset=now",
        &[("authorization", sa.as_str())],
        b"",
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        st == 400 && text.contains("live_unsupported"),
        "live on an internal page route must be refused as live_unsupported \
         (got {st}): {text}"
    );
}

/// RED (review finding 1): a workload JWT's `operations` claim must
/// SCOPE what the token can do. Today every gate reduces verification
/// to a boolean, so a token minted for nothing at all — or for
/// segment reads only — is a cell-wide administrator credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workload_jwt_operations_scope_the_internal_surface() {
    let scopes = "streams.create streams.records.append streams.records.read";
    let (_state, addr, _tok) = sr_rig("proj-opsc", "ws_opsc", "c_opsc", "opsc-1", scopes).await;
    let now = crate::shard::now_ms() / 1000;
    let ct = ("content-type", "application/json");
    let fleet = ("authorization", "Bearer dst-internal-token");

    // Stage a raw stream as the static fleet operator (bridge posture).
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/opx", &[ct, fleet], br#"[{"i":0}]"#).await;
    assert!(st == 200 || st == 201, "stage: {st}");

    // A token with an EMPTY operations claim grants NOTHING.
    let none = format!("Bearer {}", sr2_workload_jwt("opsc-1", &[], now));
    let na = ("authorization", none.as_str());
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/opx", &[ct, na], br#"[{"i":1}]"#).await;
    assert_eq!(st, 401, "empty-operations token appended via raw: {st}");
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/opx", &[na], b"").await;
    assert_eq!(st, 401, "empty-operations token read via raw: {st}");
    let (st, _, _) = hreq(addr, "GET", "/v1/segments/opx", &[na], b"").await;
    assert_eq!(st, 401, "empty-operations token listed segments: {st}");

    // segment-read authorizes EXACTLY segment reads: the internal
    // segment-read gate passes (any downstream 4xx is fine — the claim
    // under test is the authorization boundary), while raw appends,
    // telemetry appends, and unrelated internal routes refuse.
    let sr = format!(
        "Bearer {}",
        sr2_workload_jwt("opsc-1", &["segment-read"], now)
    );
    let sa = ("authorization", sr.as_str());
    let (st, _, _) = hreq(addr, "GET", "/v1/internal/segment-read/opx", &[sa], b"").await;
    assert_ne!(
        st, 401,
        "segment-read token refused its own operation: {st}"
    );
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/opx", &[ct, sa], br#"[{"i":2}]"#).await;
    assert_eq!(st, 401, "segment-read token appended via raw: {st}");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_usage",
        &[ct, sa],
        br#"[]"#,
    )
    .await;
    assert_eq!(st, 401, "segment-read token reached telemetry-append: {st}");

    // consumer-sweep must not open segment scans.
    let cs = format!(
        "Bearer {}",
        sr2_workload_jwt("opsc-1", &["consumer-sweep"], now)
    );
    let ca = ("authorization", cs.as_str());
    let (st, _, _) = hreq(addr, "GET", "/v1/internal/segment-scan/opx", &[ca], b"").await;
    assert_eq!(st, 401, "consumer-sweep token reached segment-scan: {st}");

    // An UNKNOWN operation name grants nothing.
    let junk = format!(
        "Bearer {}",
        sr2_workload_jwt("opsc-1", &["everything"], now)
    );
    let ja = ("authorization", junk.as_str());
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/opx", &[ct, ja], br#"[{"i":3}]"#).await;
    assert_eq!(st, 401, "unknown-operation token appended via raw: {st}");
}

/// RED (review finding 1, outbound half): a two-instance fleet where
/// the SENDER holds NO static fleet token relays a system append to
/// the owner using a workload JWT from its token source — §14.1
/// outbound identity, end to end through the receiver's
/// operation-scoped gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jwt_only_fleet_relay_succeeds() {
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
        "wl-1".to_string(),
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

    let store = mem();
    // Receiver A: owns its shard, static bridge token configured (the
    // receiving posture is irrelevant to the claim — the SENDER is
    // token-free).
    let (state_a, addr_a) = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-a".to_string()),
            auth_service: Some(svc.clone()),
            ..Default::default()
        },
    )
    .await
    .parts();
    // Sender B: NO static fleet token; its outbound identity is a
    // workload JWT minted by the source with EXACTLY telemetry-append.
    let src_calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let sc = src_calls.clone();
    let src: crate::peer::FleetTokenSource = std::sync::Arc::new(move |_force: bool| {
        sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(sr2_workload_jwt(
            "wl-1",
            &["telemetry-append"],
            crate::shard::now_ms() / 1000,
        ))
    });
    let (state_b, _addr_b) = http_rig_build(
        store.clone(),
        RigRuntime::incarnation(1),
        HttpRigOptions {
            instance: Some("rig-b".to_string()),
            auth_service: Some(svc.clone()),
            fleet_auth: Some((None, Some(src))),
            ..Default::default()
        },
    )
    .await
    .parts();
    // B's ring says A owns the shard; B's peer map resolves A's URL.
    state_b
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-b".to_string()]);
    state_b.ownership.set_override("00", "rig-a");
    state_b.peer.set_peer("rig-a", &format!("http://{addr_a}"));

    // Prime: the OWNER creates the system stream (production owners
    // do this on their own telemetry path); B's local attempt then
    // refuses on OWNERSHIP with streams-replay-to, not existence.
    crate::billing::system_append(
        &state_a,
        "_ops_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"prime-0","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("owner-side prime");

    crate::billing::system_append(
        &state_b,
        "_ops_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"jwt-relay-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("JWT-only relay must land on the owner");
    assert!(
        src_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the relay must have drawn its bearer from the workload source"
    );
    engine_shutdown(&state_a).await;
    engine_shutdown(&state_b).await;
}

/// RED (review finding 1, rotation): the sender's first token is
/// EXPIRED — the peer 401s, the source is force-refreshed exactly
/// once, and the retried relay succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotated_workload_jwt_refreshes_and_retries() {
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
        "wl-1".to_string(),
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
    let store = mem();
    let (state_a, addr_a) = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-a".to_string()),
            auth_service: Some(svc.clone()),
            ..Default::default()
        },
    )
    .await
    .parts();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let forced = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (c2, f2) = (calls.clone(), forced.clone());
    let src: crate::peer::FleetTokenSource = std::sync::Arc::new(move |force: bool| {
        c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let now = crate::shard::now_ms() / 1000;
        if force {
            f2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some(sr2_workload_jwt("wl-1", &["telemetry-append"], now))
        } else {
            // The cached token has ROTATED OUT: already expired.
            Some(sr2_workload_jwt("wl-1", &["telemetry-append"], now - 700))
        }
    });
    let (state_b, _addr_b) = http_rig_build(
        store.clone(),
        RigRuntime::incarnation(1),
        HttpRigOptions {
            instance: Some("rig-b".to_string()),
            auth_service: Some(svc.clone()),
            fleet_auth: Some((None, Some(src))),
            ..Default::default()
        },
    )
    .await
    .parts();
    state_b
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-b".to_string()]);
    state_b.ownership.set_override("00", "rig-a");
    state_b.peer.set_peer("rig-a", &format!("http://{addr_a}"));

    // Prime: the OWNER creates the system stream (production owners
    // do this on their own telemetry path); B's local attempt then
    // refuses on OWNERSHIP with streams-replay-to, not existence.
    crate::billing::system_append(
        &state_a,
        "_ops_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"prime-0","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("owner-side prime");

    crate::billing::system_append(
        &state_b,
        "_ops_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"jwt-rotate-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("rotated token must refresh and retry to success");
    assert_eq!(
        forced.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "exactly one forced refresh"
    );
    engine_shutdown(&state_a).await;
    engine_shutdown(&state_b).await;
}

/// RED (round-3 finding 1): in WORKLOAD mode the static fleet
/// credential is DEAD at runtime — a coexisting FLEET_INTERNAL_TOKEN
/// must not authorize internal routes, or the "release posture has no
/// permanent shared credential" claim is configuration-dependent
/// fiction. The workload JWT keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_token_is_dead_in_workload_mode() {
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
        "wm-1".to_string(),
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
    // MISCONFIGURED coexistence: a static token AND a workload source.
    let src: crate::peer::FleetTokenSource = std::sync::Arc::new(move |_| {
        Some(sr2_workload_jwt(
            "wm-1",
            &["telemetry-append"],
            crate::shard::now_ms() / 1000,
        ))
    });
    let (state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-wm".to_string()),
            auth_service: Some(svc),
            fleet_auth: Some((Some("dst-internal-token".to_string()), Some(src.clone()))),
            ..Default::default()
        },
    )
    .await
    .parts();
    let ct = ("content-type", "application/json");
    let stat = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_ops_events",
        &[ct, stat],
        br#"[]"#,
    )
    .await;
    assert_eq!(
        st, 401,
        "the static token must be DEAD when a workload source is configured: {st}"
    );
    // The workload JWT with the exact operation still authorizes.
    let wl = format!("Bearer {}", src(false).unwrap());
    let wla = ("authorization", wl.as_str());
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_ops_events",
        &[ct, wla],
        br#"[]"#,
    )
    .await;
    assert_ne!(st, 401, "the workload JWT must still authorize: {st}");
    engine_shutdown(&state).await;
}
