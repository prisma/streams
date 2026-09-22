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

// ---- review rank 21: create-vs-relay follows the typed verdicts ----------

/// The two-instance JWT-only fleet the relay tests share: A owns the
/// single shard `00`; B's ring says so, B holds NO static fleet token
/// and presents `src`'s workload JWT outbound.
async fn jwt_only_fleet(
    src: crate::peer::FleetTokenSource,
) -> (
    std::sync::Arc<crate::http::AppState>,
    std::sync::Arc<crate::http::AppState>,
) {
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
    let (state_b, _addr_b) = http_rig_build(
        store,
        RigRuntime::incarnation(1),
        HttpRigOptions {
            instance: Some("rig-b".to_string()),
            auth_service: Some(svc),
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
    (state_a, state_b)
}

fn telemetry_append_source() -> crate::peer::FleetTokenSource {
    std::sync::Arc::new(move |_force: bool| {
        Some(sr2_workload_jwt(
            "wl-1",
            &["telemetry-append"],
            crate::shard::now_ms() / 1000,
        ))
    })
}

/// RED (review rank 21): a fleet member that does NOT own a reserved
/// stream nobody has created yet must still land its first batch on
/// the owner. The sender sniffed statuses: its local create was
/// refused on OWNERSHIP as 409 not_ring_owner, which it read as
/// "exists", re-appended locally, got 404 not_found (no
/// streams-replay-to on a 404) and reported
/// `system append _audit_events: 404 Not Found`: the relay branch was
/// unreachable until the owner happened to create the stream itself.
/// (`_audit_events` routes to shard `00`, the one the rig's override
/// governs; `_ops_events` lands in the empty prefix by rendezvous.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unprimed_system_append_relays_its_first_batch_to_the_owner() {
    let (state_a, state_b) = jwt_only_fleet(telemetry_append_source()).await;
    // NO owner-side prime: `_audit_events` exists nowhere yet.
    crate::billing::system_append(
        &state_b,
        "_audit_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"unprimed-relay-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("a non-owner's FIRST system append must relay to the owner");
    // The record is readable on the OWNER through the system path.
    let (page, _) = crate::billing::system_read(&state_a, "_audit_events", PRISMA_KEY, None)
        .await
        .expect("owner-side system read")
        .expect("the owner now holds _audit_events");
    let events: Vec<serde_json::Value> =
        serde_json::from_slice(&page).expect("system page is a JSON array");
    assert_eq!(
        events
            .iter()
            .filter(|e| e["eventId"] == "unprimed-relay-1")
            .count(),
        1,
        "the relayed batch must be durable on the owner exactly once: {events:?}"
    );
    engine_shutdown(&state_a).await;
    engine_shutdown(&state_b).await;
}

/// RED (review rank 21, receiver half): a telemetry-append RECEIVER whose
/// own ring assigns the shard elsewhere (fleet skew) must answer the
/// creation's ownership refusal, 409 not_ring_owner + Streams-Replay-To,
/// not mask it as "exists" and report the append's 404 not_found. The
/// `__ds` root stays refused before any identity is constructed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_append_receiver_reports_ownership_not_absence() {
    let (state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-a".to_string()),
            ..Default::default()
        },
    )
    .await
    .parts();
    state
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-c".to_string()]);
    state.ownership.set_override("00", "rig-c");
    let fleet = [
        ("content-type", "application/json"),
        ("authorization", "Bearer dst-internal-token"),
    ];
    let (st, headers, body) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_audit_events",
        &fleet,
        br#"[{"v":1,"eventId":"skew-1","eventTimeMs":1,"eventType":"t"}]"#,
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert_eq!(st, 409, "ownership refusal must not be masked: {text}");
    assert!(
        text.contains("not_ring_owner"),
        "typed code on the wire: {text}"
    );
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("rig-c"),
        "the relay target must be on the refusal: {headers:?}"
    );
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/__ds",
        &fleet,
        b"[]",
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        st == 400 && text.contains("invalid_name"),
        "the __ds root stays refused, never constructed: {st} {text}"
    );
    let bad_key = [
        ("content-type", "application/json"),
        ("authorization", "Bearer dst-internal-token"),
        ("stream-encryption-key", "not-a-key"),
    ];
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_audit_events",
        &bad_key,
        b"[]",
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        st == 400 && text.contains("invalid_key"),
        "an unparsable system key is refused by name: {st} {text}"
    );
    engine_shutdown(&state).await;
}

/// RED (review rank 21, diagnostics): a sender with no route to the
/// owner reports the owner's typed refusal (code and message), never a
/// flattened `404 Not Found` that hides which step refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn system_append_without_a_peer_reports_the_typed_refusal() {
    let (state, _addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-b".to_string()),
            ..Default::default()
        },
    )
    .await
    .parts();
    state
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-b".to_string()]);
    state.ownership.set_override("00", "rig-a");
    // No `peer.set_peer`: the owner has no published URL.
    let err = crate::billing::system_append(
        &state,
        "_audit_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"no-peer-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect_err("no peer URL: the append cannot land");
    assert_eq!(
        err,
        "system append _audit_events: create not_ring_owner: shard 00 belongs to rig-a"
    );
    engine_shutdown(&state).await;
}

/// Only a MISSING stream creates: a system append under a key the
/// stream was not created with is the append's own `wrong_key`
/// refusal, never a re-creation attempt under the new key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotated_system_key_is_the_appends_refusal_never_a_recreate() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let (state, _addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-a".to_string()),
            ..Default::default()
        },
    )
    .await
    .parts();
    crate::billing::system_append(
        &state,
        "_audit_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"rotated-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("first use creates the stream under the deployment key");
    let err = crate::billing::system_append(
        &state,
        "_audit_events",
        OTHER_KEY,
        br#"[{"v":1,"eventId":"rotated-2","eventTimeMs":2,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect_err("a rotated key cannot append to the existing stream");
    assert_eq!(
        err,
        "system append _audit_events: append wrong_key: key mismatch"
    );
    let (page, _) = crate::billing::system_read(&state, "_audit_events", PRISMA_KEY, None)
        .await
        .expect("system read")
        .expect("the stream still exists under the deployment key");
    let text = String::from_utf8_lossy(&page);
    assert!(
        text.contains("rotated-1") && !text.contains("rotated-2"),
        "nothing lands under the rotated key: {text}"
    );
    engine_shutdown(&state).await;
}

/// Ring skew: the sender's ring assigns shard `00` to rig-a, rig-a's own
/// ring assigns it to rig-c. The owner-side receiver refuses the relay
/// 409 not_ring_owner and the sender REPORTS that refusal: one hop, and a
/// refused relay is never counted as landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_the_skewed_owner_refuses_is_reported_not_counted_as_landed() {
    let (state_a, state_b) = jwt_only_fleet(telemetry_append_source()).await;
    state_a
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-c".to_string()]);
    state_a.ownership.set_override("00", "rig-c");
    let err = crate::billing::system_append(
        &state_b,
        "_audit_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"skewed-relay-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect_err("the owner refused the relay; the sender must say so");
    assert_eq!(err, "telemetry relay _audit_events: 409 Conflict");
    engine_shutdown(&state_a).await;
    engine_shutdown(&state_b).await;
}
