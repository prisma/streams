//! Admission memory.

use super::fixture_auth::{
    RIG_SCOPES, auth_rig, mint_token, rig_append, rig_create, rig_policy, rig_sse,
};
use super::fixture_failpoints::gap_lock;
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_livefeed::{
    hub_append_lf, hub_sse_collect, last_next_cursor, lf_connect, lf_record_and_status,
};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Round-10 review reopen (red): PROJECT ISOLATION of the feed
/// retention budget. The process budget is a CELL safety ceiling —
/// one noisy project consuming it must not force an unrelated,
/// quota-respecting project's healthy subscribers into the uncached
/// reconnect posture. Noisy project A fills the (shrunk) global
/// budget with real retained rings across many two-subscriber
/// streams; victim project B then publishes on its own stream: B's
/// subscribers must receive the record IN PLACE — no EOF, no lag
/// disconnect. Multitenancy contract: only the offending project is
/// constrained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_budget_noisy_project_cannot_evict_another() {
    let (svc, state, addr) = auth_rig("proj-noisy", "ws_iso", &["ca"], None).await;
    // Second project in the same cell: republish policies + grants at
    // v2 carrying BOTH projects (an omission would kill proj-noisy via
    // the high-water rules).
    {
        let mut projects = std::collections::HashMap::new();
        for p in ["proj-noisy", "proj-victim"] {
            let pol = rig_policy(p, "ws_iso", 1, 1);
            projects.insert(pol.project_id.clone(), pol);
        }
        svc.publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: crate::shard::now_ms() / 1000,
            feed_version: 2,
        })
        .unwrap();
        let mut credentials = std::collections::HashMap::new();
        for (cred, proj) in [("ca", "proj-noisy"), ("cb", "proj-victim")] {
            credentials.insert(
                std::sync::Arc::from(cred),
                crate::project_policy::CredentialGrant {
                    credential_id: std::sync::Arc::from(cred),
                    project_id: crate::tenant::ProjectId::new(proj).unwrap(),
                    grant_version: 1,
                    status: crate::project_policy::CredentialStatus::Active,
                    scopes: crate::tenant::ScopeSet::parse(RIG_SCOPES).0,
                    grant: crate::tenant::StreamGrant::All,
                    expires_at: None,
                },
            );
        }
        svc.publish_grants(crate::project_policy::GrantSnapshot {
            credentials,
            fetched_at_unix: crate::shard::now_ms() / 1000,
            feed_version: 2,
        })
        .unwrap();
    }
    // Cheap exhaustion geometry: 32-KiB rings under a 256-KiB cell
    // ceiling — twelve noisy feeds demand ~384 KiB of retention.
    state.livefeed.set_ring_bytes(32 * 1024);
    state.livefeed.budget().set_max_for_test(256 * 1024);
    let tok_a = mint_token("ca", "proj-noisy", "ws_iso", 1, 1, "na", 600);
    let tok_b = mint_token("cb", "proj-victim", "ws_iso", 1, 1, "vb", 600);

    // Noisy project A: 12 two-subscriber streams, rings filled with
    // real retained batches (8-KiB payloads; ~11-KiB prepared charge).
    let payload_a = format!(r#"{{"fill":"{}"}}"#, "a".repeat(8 * 1024));
    let mut noisy_subs = Vec::new();
    for s in 0..12 {
        let name = format!("na{s}");
        rig_create(addr, &name, &tok_a).await;
        for _ in 0..2 {
            let mut sck = rig_sse(addr, &name, &tok_a, "", None).await;
            let (acc, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
            assert!(acc.contains("upToDate"));
            noisy_subs.push(sck);
        }
        for _ in 0..4 {
            assert_eq!(rig_append(addr, &name, &tok_a, &payload_a).await, 200);
        }
    }
    // Let the noisy drives settle: the reserved gauge quiesces once
    // every publication decided its retention posture.
    let mut last = u64::MAX;
    for _ in 0..100 {
        let now = state.livefeed.budget().reserved();
        if now == last {
            break;
        }
        last = now;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }

    // Victim project B: one stream, two healthy parked subscribers.
    rig_create(addr, "vb", &tok_b).await;
    let mut v1 = rig_sse(addr, "vb", &tok_b, "", None).await;
    let (b1, _) = hub_sse_collect(&mut v1, 8, |t| t.contains("upToDate")).await;
    assert!(b1.contains("upToDate"));
    let mut v2 = rig_sse(addr, "vb", &tok_b, "", None).await;
    let (b2, _) = hub_sse_collect(&mut v2, 8, |t| t.contains("upToDate")).await;
    assert!(b2.contains("upToDate"));
    // B publishes one 16-KiB record.
    let payload_b = format!(r#"{{"victim":"{}"}}"#, "b".repeat(16 * 1024));
    assert_eq!(rig_append(addr, "vb", &tok_b, &payload_b).await, 200);
    for (n, v) in [(1, &mut v1), (2, &mut v2)] {
        let (acc, eof) =
            hub_sse_collect(v, 10, |t| lf_record_and_status(t, "\"victim\":\"bb")).await;
        assert!(
            !eof,
            "victim sub{n}: a NOISY NEIGHBOR forced an unrelated project's \
             subscriber into the reconnect posture:\n…{}",
            &acc[acc.len().saturating_sub(400)..]
        );
        assert_eq!(
            acc.matches("\"victim\":\"").count(),
            1,
            "victim sub{n}: the record arrives in place exactly once"
        );
    }
}

/// Replaces `hub_delayed_reader_lands_on_the_current_scan_head`:
/// foreign-key appends advance a lane's scanned position without
/// delivering records; a DELAYED reader of the same lane must
/// converge to exactly the cursor the live subscriber holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_delayed_reader_lands_on_the_current_scan_head() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfdr",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let mut s1 = lf_connect(addr, "lfdr", "?routingKey=ka").await;
    let (a, _) = hub_sse_collect(&mut s1, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"));
    // Foreign-lane appends: the ka lane's scan head advances with NO
    // ka records.
    for i in 0..3 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/lfdr/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", "other"),
            ],
            format!(r#"{{"f":{i}}}"#).as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204, "foreign append {st}");
    }
    // Drain to QUIESCENCE, not to the first control: the session may
    // emit an intermediate upToDate between the three appends on a
    // slow runner, and grabbing that cursor races the scan head (CI
    // flake at 11.6). All appends are durable before this collect, so
    // a bounded full drain always ends at the final head.
    let (live, eof) = hub_sse_collect(&mut s1, 3, |_| false).await;
    assert!(!eof, "foreign progress never disconnects:\n{live}");
    assert!(
        live.matches("\"upToDate\":true").count() >= 1 && live.contains("nextCursor"),
        "the live subscriber reports the advanced scan head:\n{live}"
    );
    assert!(
        !live.contains("\"f\":"),
        "foreign records must not cross lanes:\n{live}"
    );
    let live_cursor = last_next_cursor(&live);

    // The DELAYED reader of the same lane converges to the same head.
    let mut s2 = lf_connect(addr, "lfdr", "?routingKey=ka&cursor=beginning").await;
    let (d, eof2) = hub_sse_collect(&mut s2, 10, |t| t.contains("\"upToDate\":true")).await;
    assert!(!eof2);
    assert!(
        !d.contains("\"f\":"),
        "foreign records must not cross lanes:\n{d}"
    );
    assert_eq!(
        last_next_cursor(&d),
        live_cursor,
        "the delayed reader must land on the live subscriber's scan head:\n{d}"
    );
}

/// Replaces `hub_mass_disconnect_tears_down_within_deadline`: 20
/// LiveFeed subscribers dropped at once — connections AND feeds reach
/// zero within the teardown deadline (drop detaches; the last leave
/// evicts the feed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_mass_disconnect_tears_down_within_deadline() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/lfmd",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "lfmd", r#"{"m":0}"#).await;
    let mut socks = Vec::new();
    for _ in 0..20 {
        let mut sck = lf_connect(addr, "lfmd", "").await;
        let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
        assert!(a.contains("upToDate"));
        socks.push(sck);
    }
    assert!(
        state.livefeed.registry().len() >= 1,
        "the shared feed exists"
    );
    drop(socks);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let conns = state.admission.snapshot().sse_connections;
        let feeds = state.livefeed.registry().len();
        if conns == 0 && feeds == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "teardown deadline: conns={conns} feeds={feeds} after 3s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Round-13 enforce-mode rig for the memory-pressure battery: one
/// project ("proj-pm"), full data scopes, RS256 JWT — returns the
/// bearer for wire requests.
async fn pm_enforce_rig(
    store: Arc<dyn ObjectStore>,
    runtime: RigRuntime,
) -> (
    Arc<crate::http::AppState>,
    std::net::SocketAddr,
    String,
    crate::tenant::ProjectId,
) {
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    const SCOPES: &str =
        "streams.metadata.read streams.create streams.records.append streams.records.read";
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
        "pm-1".to_string(),
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
    let pid = crate::tenant::ProjectId::new("proj-pm").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new("ws_pm").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 1,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas::default(),
        },
    );
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let mut credentials = std::collections::HashMap::new();
    credentials.insert(
        std::sync::Arc::from("cpm"),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from("cpm"),
            project_id: pid.clone(),
            grant_version: 1,
            status: crate::project_policy::CredentialStatus::Active,
            scopes: crate::tenant::ScopeSet::parse(SCOPES).0,
            grant: crate::tenant::StreamGrant::All,
            expires_at: None,
        },
    );
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let (state, addr) = http_rig_build(
        store,
        runtime,
        HttpRigOptions {
            auth_service: Some(svc),
            ..Default::default()
        },
    )
    .await
    .parts();

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
    header.kid = Some("pm-1".into());
    let jwt = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: "cpm",
            project_id: "proj-pm",
            workspace_id: "ws_pm",
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: SCOPES,
            jti: "tpm",
            iat: now - 60,
            exp: now + 600,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    (state, addr, format!("Bearer {jwt}"), pid)
}

/// Round-13 memory-pressure backstop on the wire: a project over its
/// per-project pressure watermark gets the typed, retryable 429
/// `project_memory_pressure` on NEW appends ONLY — reads continue,
/// the refusal is project-scoped, and dropping below the release
/// point restores appends (hysteresis pinned at the unit level).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn project_memory_pressure_throttles_new_appends_only() {
    let (state, addr, bearer, pid) = pm_enforce_rig(mem(), RigRuntime::first()).await;
    state
        .admission
        .set_project_memory_pressure_bytes(256 * 1024);
    let auth = ("authorization", bearer.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");

    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/pm",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/pm/records",
        &[ekey, auth, ct],
        br#"{"i":1}"#,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "under the watermark: {st} {}",
        String::from_utf8_lossy(&b)
    );

    // Drive the project's EXACT retained pressure over the watermark
    // (the same counter the LiveFeed budget mirrors into).
    let adm = state.quotas.pressure_handle(&pid).unwrap();
    adm.retained_sse_add(300 * 1024);

    let (st, h, b) = preq(
        addr,
        "POST",
        "/v1/streams/pm/records",
        &[ekey, auth, ct],
        br#"{"i":2}"#,
    )
    .await;
    assert_eq!(st, 429, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "project_memory_pressure");
    assert_eq!(v["error"]["retryable"], true);
    assert_eq!(h.get("retry-after").map(String::as_str), Some("1"));

    // Reads continue while the project's writes are throttled.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/pm/records", &[ekey, auth], b"").await;
    assert_eq!(st, 200, "reads flow: {}", String::from_utf8_lossy(&b));

    // Below the release point (75% of 256 KiB): appends restore.
    adm.retained_sse_sub(300 * 1024);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/pm/records",
        &[ekey, auth, ct],
        br#"{"i":3}"#,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "recovered: {st} {}",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}

/// Round-13 battery 9: durable frame debt SURVIVES a restart via the
/// tail seed — the new incarnation's binding starts from the tail's
/// persisted unabsorbed_bytes, never from zero, and absorption then
/// retires it to zero. (Battery 10's owner-movement release is the
/// same Drop path, pinned at the unit level; the fleet leg exercises
/// it in the field.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frame_debt_survives_restart_via_tail_seed() {
    let _l = gap_lock().lock().await; // shared failpoint schedule
    let store = mem();
    let (state, addr, bearer, pid) = pm_enforce_rig(store.clone(), RigRuntime::first()).await;
    state.runtime.history.paused.store(true, Ordering::Relaxed);
    let auth = ("authorization", bearer.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/debt",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let payload = format!(r#"{{"pad":"{}"}}"#, "x".repeat(64 * 1024));
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/debt/records",
        &[ekey, auth, ct],
        payload.as_bytes(),
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "append: {st} {}",
        String::from_utf8_lossy(&b)
    );
    let adm = state.quotas.pressure_handle(&pid).unwrap();
    let debt_before = adm.unabsorbed_frame_bytes_now();
    assert!(
        debt_before > 64 * 1024,
        "the paused absorber leaves attributed frame debt: {debt_before}"
    );
    engine_shutdown(&state).await;
    drop(state);

    // New incarnation on the SAME store: the first pressured append
    // binds and seeds from the durable tail — never from zero.
    let (state2, addr2, bearer2, pid2) = pm_enforce_rig(store, RigRuntime::incarnation(1)).await;
    state2.runtime.history.paused.store(true, Ordering::Relaxed);
    let auth2 = ("authorization", bearer2.as_str());
    let (st, _, b) = preq(
        addr2,
        "POST",
        "/v1/streams/debt/records",
        &[ekey, auth2, ct],
        br#"{"i":"tiny"}"#,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "reopen append: {st} {}",
        String::from_utf8_lossy(&b)
    );
    let adm2 = state2.quotas.pressure_handle(&pid2).unwrap();
    let seeded = adm2.unabsorbed_frame_bytes_now();
    assert!(
        seeded >= debt_before,
        "restart seeds durable debt ({seeded} >= {debt_before}), never zero"
    );
    assert!(adm2.dirty_streams_now() >= 1);

    // Unpause: absorption retires the debt to exactly zero.
    state2
        .runtime
        .history
        .paused
        .store(false, Ordering::Relaxed);
    let mut zeroed = false;
    for _ in 0..600 {
        if adm2.unabsorbed_frame_bytes_now() == 0 && adm2.dirty_streams_now() == 0 {
            zeroed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(zeroed, "absorption retires attributed debt to zero");
    engine_shutdown(&state2).await;
}
