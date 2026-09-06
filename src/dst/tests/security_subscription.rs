//! Security subscription.

use super::fixture_auth::{
    auth_rig, mint_token, rig_append, rig_create, rig_policy, rig_publish_grants,
    rig_publish_policy, rig_sse,
};
use super::fixture_failpoints::gap_lock;
use super::fixture_http::http_rig_with_auth_service;
use super::fixture_livefeed::{hub_sse_collect, sse_head};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

// ---- #274 hub promotion + teardown (Søren review F8+F9) ------------

/// Review round 3 F3 replacement (1): the billing resolver uses the
/// CURRENT workspace-at-event state — a valid transfer (ownership
/// bump) moves the billed workspace; nothing else can.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_of_resolves_the_workspace_at_event() {
    let (svc, state, addr) = auth_rig("proj-idw", "ws_ida", &["c1"], None).await;
    let tok = mint_token("c1", "proj-idw", "ws_ida", 1, 1, "idw", 600);
    rig_create(addr, "idw", &tok).await;
    // Enforce mode namespaces the stream under its PROJECT.
    let sref = crate::tenant::TenantStreamRef::new(
        crate::tenant::ProjectId::new("proj-idw").unwrap(),
        crate::tenant::CanonicalStreamName::new("idw").unwrap(),
    );
    let desc = state.registry.get(&sref).await.unwrap().expect("desc");
    assert_eq!(
        crate::billing::identity_of(&state, &desc).account_id,
        "ws_ida"
    );
    // Hostile shape is refused and leaves attribution untouched.
    assert!(rig_publish_policy(&svc, rig_policy("proj-idw", "ws_idb", 1, 2), 2).is_err());
    assert_eq!(
        crate::billing::identity_of(&state, &desc).account_id,
        "ws_ida"
    );
    // The valid transfer moves it.
    rig_publish_policy(&svc, rig_policy("proj-idw", "ws_idb", 2, 3), 3).unwrap();
    assert_eq!(
        crate::billing::identity_of(&state, &desc).account_id,
        "ws_idb"
    );
}

/// Review round 3 F3 replacement (2): a VALID transfer end to end —
/// pre-transfer delivery bills workspace A, the ownership version
/// increments and the old subscription terminates, a new credential
/// opens a new subscription, post-transfer delivery bills workspace B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_transfer_bills_each_workspace_on_its_own_side() {
    let _xr = crate::billing::billing_clock_lock().read().await;
    let (svc, state, addr) = auth_rig("proj-vtx", "ws_vta", &["c1"], None).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    let _ = state.rollup.install(std::sync::Arc::new(rollup));
    let tok_a = mint_token("c1", "proj-vtx", "ws_vta", 1, 1, "va", 600);
    rig_create(addr, "vtx", &tok_a).await;
    let mut promoter = rig_sse(addr, "vtx", &tok_a, "", None).await;
    let (a, _) = hub_sse_collect(&mut promoter, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"));
    let mut sub_a = rig_sse(addr, "vtx", &tok_a, "", None).await;
    let (b, _) = hub_sse_collect(&mut sub_a, 8, |t| t.contains("upToDate")).await;
    assert!(b.contains("upToDate"));
    assert_eq!(
        rig_append(addr, "vtx", &tok_a, r#"{"side":"a"}"#).await,
        200
    );
    let (d1, _) = hub_sse_collect(&mut sub_a, 10, |t| t.contains("\"side\":\"a\"")).await;
    assert!(
        d1.contains("\"side\":\"a\""),
        "pre-transfer delivery:\n{d1}"
    );

    // THE TRANSFER: ownership 2, workspace B, c1 revoked, c2 granted.
    rig_publish_policy(&svc, rig_policy("proj-vtx", "ws_vtb", 2, 2), 2).unwrap();
    rig_publish_grants(
        &svc,
        "proj-vtx",
        &[
            ("c1", crate::project_policy::CredentialStatus::Revoked, 2),
            ("c2", crate::project_policy::CredentialStatus::Active, 1),
        ],
        2,
    )
    .unwrap();
    let (_, eof_p) = hub_sse_collect(&mut promoter, 25, |_| false).await;
    let (_, eof_s) = hub_sse_collect(&mut sub_a, 5, |_| false).await;
    assert!(
        eof_p && eof_s,
        "old subscriptions terminate on the transfer"
    );
    assert_eq!(
        rig_append(addr, "vtx", &tok_a, r#"{"side":"stale"}"#).await,
        401
    );

    // New owner: fresh credential, new subscription, post-transfer
    // delivery.
    let tok_b = mint_token("c2", "proj-vtx", "ws_vtb", 2, 1, "vb", 600);
    let mut p2 = rig_sse(addr, "vtx", &tok_b, "?cursor=now", None).await;
    let (c, _) = hub_sse_collect(&mut p2, 8, |t| t.contains("event: control")).await;
    assert!(c.contains(" 200 "), "new owner subscribes:\n{c}");
    let mut sub_b = rig_sse(addr, "vtx", &tok_b, "?cursor=now", None).await;
    let (c2, _) = hub_sse_collect(&mut sub_b, 8, |t| t.contains("event: control")).await;
    assert!(c2.contains(" 200 "));
    assert_eq!(
        rig_append(addr, "vtx", &tok_b, r#"{"side":"b"}"#).await,
        200
    );
    let (d2, _) = hub_sse_collect(&mut sub_b, 10, |t| t.contains("\"side\":\"b\"")).await;
    assert!(
        d2.contains("\"side\":\"b\""),
        "post-transfer delivery:\n{d2}"
    );

    // Books: reads under BOTH workspaces for this project, each side
    // attributed to the owner at event time.
    state.billing.reads().seal_if_aged(0);
    for _ in 0..100 {
        if crate::billing::drain_once(&state).await.expect("drain") == 0 {
            break;
        }
    }
    for _ in 0..50 {
        if crate::billing::rollup_step(&state).await.expect("rollup") == 0 {
            break;
        }
    }
    let (y, m) = crate::billing::utc_year_month(crate::billing::billing_now_ms());
    let month = crate::billing::month_str(y, m);
    let rollup = state.rollup.get().unwrap();
    let mut per_ws: std::collections::HashMap<String, u64> = Default::default();
    let mut iter = rollup
        .db
        .scan_prefix(format!("project/{month}/").as_bytes(), ..)
        .await
        .unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        let k = String::from_utf8_lossy(&kv.key).to_string();
        if !k.ends_with("/proj-vtx") {
            continue;
        }
        let row: crate::rollup::AggRow = serde_json::from_slice(&kv.value).unwrap();
        per_ws.insert(
            k.split('/').nth(2).unwrap_or("?").to_string(),
            row.read_records,
        );
    }
    assert!(
        per_ws.get("ws_vta").copied().unwrap_or(0) > 0,
        "pre-transfer reads bill A: {per_ws:?}"
    );
    assert!(
        per_ws.get("ws_vtb").copied().unwrap_or(0) > 0,
        "post-transfer reads bill B: {per_ws:?}"
    );
}

/// Review V4 red (t1): a REAL ownership transfer (ownership_version
/// bump + old credential revoked) must TERMINATE established live
/// subscriptions — an old owner must not keep receiving records
/// through a connection opened before the transfer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_terminates_established_subscriptions() {
    let _xr = crate::billing::billing_clock_lock().read().await;
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
        "wtx-1".to_string(),
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
                  streams.metadata.read";
    let pid = crate::tenant::ProjectId::new("proj-ttx").unwrap();
    let policy_in = |ws: &str, ppv: u64, ov: u64| crate::project_policy::ProjectPolicy {
        project_id: pid.clone(),
        workspace_id: crate::tenant::WorkspaceId::new(ws).unwrap(),
        cell_id: std::sync::Arc::from("test-cell"),
        project_policy_version: ppv,
        ownership_version: ov,
        status: crate::project_policy::ProjectStatus::Active,
        quotas: crate::project_policy::ProjectQuotas::default(),
    };
    let publish_policy = |p: crate::project_policy::ProjectPolicy, fv: u64| {
        let mut projects = std::collections::HashMap::new();
        projects.insert(pid.clone(), p);
        svc.publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: now + fv as i64,
            feed_version: fv,
        })
        .unwrap();
    };
    publish_policy(policy_in("ws_ta", 1, 1), 1);
    let grant = |gv: u64, status: crate::project_policy::CredentialStatus, fv: u64| {
        let mut credentials = std::collections::HashMap::new();
        credentials.insert(
            std::sync::Arc::from("c_tx"),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from("c_tx"),
                project_id: pid.clone(),
                grant_version: gv,
                status,
                scopes: crate::tenant::ScopeSet::parse(scopes).0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
        svc.publish_grants(crate::project_policy::GrantSnapshot {
            credentials,
            fetched_at_unix: now + fv as i64,
            feed_version: fv,
        })
        .unwrap();
    };
    grant(1, crate::project_policy::CredentialStatus::Active, 1);
    let (_state, addr) = http_rig_with_auth_service(mem(), svc.clone()).await;

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
    let mint = |ws: &'static str, ov: u64, jti: &'static str, exp: i64| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("wtx-1".into());
        jsonwebtoken::encode(
            &header,
            &C {
                iss: "https://auth.prisma.io",
                aud: "prisma-streams-data",
                sub: "u",
                credential_id: "c_tx",
                project_id: "proj-ttx",
                workspace_id: ws,
                cell_id: "test-cell",
                ownership_version: ov,
                grant_version: 1,
                scope: "streams.create streams.records.append streams.records.read \
                        streams.metadata.read",
                jti,
                iat: now - 60,
                exp,
            },
            &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
        )
        .unwrap()
    };
    let ta = format!("Bearer {}", mint("ws_ta", 1, "ta", now + 600));
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let auth_a = ("authorization", ta.as_str());
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/ttx",
        &[ekey, auth_a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    use tokio::io::AsyncWriteExt;
    let sse = |bearer: String| async move {
        let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET /v1/streams/ttx/records:sse HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nauthorization: {bearer}\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
        );
        sck.write_all(req.as_bytes()).await.unwrap();
        sck
    };
    let mut promoter = sse(ta.clone()).await;
    let (acc, _) = hub_sse_collect(&mut promoter, 8, |t| t.contains("upToDate")).await;
    assert!(acc.contains("upToDate"), "promoter parks:\n{acc}");
    let mut sub = sse(ta.clone()).await;
    let (acc0, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "sub parks:\n{acc0}");

    // THE TRANSFER: ownership_version 2, new workspace, old credential
    // revoked — the full §Phase-D shape, not just a billing remap.
    publish_policy(policy_in("ws_tb", 2, 2), 2);
    grant(2, crate::project_policy::CredentialStatus::Revoked, 2);

    // Old token: NEW requests must fail closed.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/ttx/records",
        &[ekey, auth_a],
        br#"{"r":9}"#,
    )
    .await;
    assert_eq!(st, 401, "stale-ownership token must be refused (got {st})");

    // ESTABLISHED subscriptions must terminate within the recheck
    // bound (heartbeat cadence + slack), not deliver indefinitely.
    let (_, eof_promoter) = hub_sse_collect(&mut promoter, 25, |_| false).await;
    let (_, eof_sub) = hub_sse_collect(&mut sub, 5, |_| false).await;
    assert!(
        eof_promoter && eof_sub,
        "established subscriptions must terminate after the transfer \
         (promoter eof={eof_promoter}, hub sub eof={eof_sub})"
    );
}

/// Review V4 (t3): a live subscription terminates NO LATER than its
/// access token's expiry — even with no appends, no feed changes, and
/// no client activity (the nap deadline, not the generation check).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_terminates_at_token_expiry() {
    let _xr = crate::billing::billing_clock_lock().read().await;
    // LEASE_TERMINATIONS is process-global; serialize with the other
    // expiry tests that move it (gap_lock convention).
    let _serial = gap_lock().lock().await;
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
        "wtx-1".to_string(),
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
                  streams.metadata.read";
    let pid = crate::tenant::ProjectId::new("proj-texp").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new("ws_te").unwrap(),
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
        std::sync::Arc::from("c_te"),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from("c_te"),
            project_id: pid.clone(),
            grant_version: 1,
            status: crate::project_policy::CredentialStatus::Active,
            scopes: crate::tenant::ScopeSet::parse(scopes).0,
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
    let (_state, addr) = http_rig_with_auth_service(mem(), svc).await;

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
    header.kid = Some("wtx-1".into());
    // Expires FOUR seconds from now: long enough to subscribe, short
    // enough to watch the deadline fire.
    let tok = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: "c_te",
            project_id: "proj-texp",
            workspace_id: "ws_te",
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: scopes,
            jti: "te",
            iat: now - 60,
            exp: now + 4,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    let ta = format!("Bearer {tok}");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/texp",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", ta.as_str()),
        ],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    use tokio::io::AsyncWriteExt;
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/texp/records:sse HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nauthorization: {ta}\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    let (acc, _) = hub_sse_collect(&mut sck, 6, |t| t.contains("upToDate")).await;
    assert!(acc.contains("upToDate"), "parks before expiry:\n{acc}");
    // No appends, no feed publications: the expiry deadline alone must
    // end the connection (within expiry + small slack).
    let (_, eof) = hub_sse_collect(&mut sck, 8, |_| false).await;
    assert!(eof, "subscription must terminate at token expiry");
}

/// Review V4 (t2): project SUSPENSION must terminate established
/// live subscriptions — same lease machinery, distinct cause (the
/// review's contract names all four: transfer, suspension,
/// revocation, expiry).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspension_terminates_established_subscriptions() {
    let _xr = crate::billing::billing_clock_lock().read().await;
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
        "wtx-1".to_string(),
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
                  streams.metadata.read";
    let pid = crate::tenant::ProjectId::new("proj-tsx").unwrap();
    let policy_in = |ws: &str, ppv: u64, ov: u64| crate::project_policy::ProjectPolicy {
        project_id: pid.clone(),
        workspace_id: crate::tenant::WorkspaceId::new(ws).unwrap(),
        cell_id: std::sync::Arc::from("test-cell"),
        project_policy_version: ppv,
        ownership_version: ov,
        status: crate::project_policy::ProjectStatus::Active,
        quotas: crate::project_policy::ProjectQuotas::default(),
    };
    let publish_policy = |p: crate::project_policy::ProjectPolicy, fv: u64| {
        let mut projects = std::collections::HashMap::new();
        projects.insert(pid.clone(), p);
        svc.publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: now + fv as i64,
            feed_version: fv,
        })
        .unwrap();
    };
    publish_policy(policy_in("ws_sa", 1, 1), 1);
    let grant = |gv: u64, status: crate::project_policy::CredentialStatus, fv: u64| {
        let mut credentials = std::collections::HashMap::new();
        credentials.insert(
            std::sync::Arc::from("c_sx"),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from("c_sx"),
                project_id: pid.clone(),
                grant_version: gv,
                status,
                scopes: crate::tenant::ScopeSet::parse(scopes).0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
        svc.publish_grants(crate::project_policy::GrantSnapshot {
            credentials,
            fetched_at_unix: now + fv as i64,
            feed_version: fv,
        })
        .unwrap();
    };
    grant(1, crate::project_policy::CredentialStatus::Active, 1);
    let (_state, addr) = http_rig_with_auth_service(mem(), svc.clone()).await;

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
    let mint = |ws: &'static str, ov: u64, jti: &'static str, exp: i64| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("wtx-1".into());
        jsonwebtoken::encode(
            &header,
            &C {
                iss: "https://auth.prisma.io",
                aud: "prisma-streams-data",
                sub: "u",
                credential_id: "c_sx",
                project_id: "proj-tsx",
                workspace_id: ws,
                cell_id: "test-cell",
                ownership_version: ov,
                grant_version: 1,
                scope: "streams.create streams.records.append streams.records.read \
                        streams.metadata.read",
                jti,
                iat: now - 60,
                exp,
            },
            &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
        )
        .unwrap()
    };
    let ta = format!("Bearer {}", mint("ws_sa", 1, "ta", now + 600));
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let auth_a = ("authorization", ta.as_str());
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/tsx",
        &[ekey, auth_a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    use tokio::io::AsyncWriteExt;
    let sse = |bearer: String| async move {
        let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET /v1/streams/tsx/records:sse HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nauthorization: {bearer}\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
        );
        sck.write_all(req.as_bytes()).await.unwrap();
        sck
    };
    let mut promoter = sse(ta.clone()).await;
    let (acc, _) = hub_sse_collect(&mut promoter, 8, |t| t.contains("upToDate")).await;
    assert!(acc.contains("upToDate"), "promoter parks:\n{acc}");
    let mut sub = sse(ta.clone()).await;
    let (acc0, _) = hub_sse_collect(&mut sub, 8, |t| t.contains("upToDate")).await;
    assert!(acc0.contains("upToDate"), "sub parks:\n{acc0}");

    // THE SUSPENSION: same workspace, same versions, same credential —
    // only the project status flips. The lease must fail on status
    // alone.
    let mut suspended = policy_in("ws_sa", 2, 1);
    suspended.status = crate::project_policy::ProjectStatus::Suspended;
    publish_policy(suspended, 2);

    // ESTABLISHED subscriptions must terminate within the recheck
    // bound (heartbeat cadence + slack), not deliver indefinitely.
    let (_, eof_promoter) = hub_sse_collect(&mut promoter, 25, |_| false).await;
    let (_, eof_sub) = hub_sse_collect(&mut sub, 5, |_| false).await;
    assert!(
        eof_promoter && eof_sub,
        "established subscriptions must terminate on suspension \
         (promoter eof={eof_promoter}, hub sub eof={eof_sub})"
    );
}

// ===================================================================
// Review round 4, finding 1 (red): the SUBSCRIPTION-CONSTRUCTION
// race. LeaseWatch::new recorded the CURRENT generation without
// validating against it, so invalidation published AFTER token
// verification but BEFORE body construction left last_gen already
// equal to the new generation — the first revoked() fast path
// ("generation unchanged since construction") passed forever and the
// subscription started on authorization that was already dead.
//
// The SseBeforeLeaseGate failpoint opens exactly that window: verify
// under ownership 1 -> PARK -> publish ownership 2 + revoke -> resume.
// The subscription must be REFUSED (non-200) or terminate with NO
// initial control and NO data — on every SSE surface.
//
// The failpoint registry is global; gap_lock serializes armers.
// ===================================================================

/// Releases the SSE lease-gate failpoint even if the test panics — a
/// parked subscription must never leak into sibling tests.
struct SseGateGuard(String);
impl Drop for SseGateGuard {
    fn drop(&mut self) {
        crate::failpoints::release_sse_before_lease_gate(&self.0);
    }
}

/// THE assertion shared by the three surface legs: after the window
/// closed over an installed revocation, the connection must carry NO
/// control frame and NO data — refused outright, or 200 followed by
/// immediate termination.
async fn assert_no_frames_after_pre_establishment_invalidation(
    sck: &mut tokio::net::TcpStream,
    surface: &str,
) {
    let (status, head) = sse_head(sck).await;
    if status != 200 {
        // Refused before establishment: acceptable.
        assert!(
            status == 401 || status == 403 || status == 503,
            "{surface}: unexpected refusal status {status}:\n{head}"
        );
        return;
    }
    let (body, eof) = hub_sse_collect(sck, 8, |_| false).await;
    assert!(
        !body.contains("event:"),
        "{surface}: frames were delivered on pre-invalidated authorization:\n{}",
        &body[body.len().saturating_sub(400)..]
    );
    assert!(
        eof,
        "{surface}: subscription stayed open on pre-invalidated authorization"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lineage_sse_refuses_authorization_invalidated_before_body_construction() {
    let _serial = gap_lock().lock().await;
    let (svc, state, addr) = auth_rig("proj-f1l", "ws_f1", &["c1"], None).await;
    let tok = mint_token("c1", "proj-f1l", "ws_f1", 1, 1, "f1l", 600);
    rig_create(addr, "f1l", &tok).await;
    for i in 0..4 {
        for k in ["ga", "gb"] {
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/f1l/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("authorization", tok.as_str()),
                    ("prisma-routing-key", k),
                ],
                format!(r#"{{"k":"{k}","n":{i}}}"#).as_bytes(),
            )
            .await;
            assert_eq!(st, 200, "append {k}/{i}");
        }
    }
    // Split: keyed lineage SSE traverses segments via
    // the deleted legacy lineage responder (own LeaseWatch). The
    // stream lives on the PRODUCT surface, so the sref is
    // project-qualified.
    let f1l_ref = crate::tenant::ProjectId::new("proj-f1l")
        .unwrap()
        .stream_ref("f1l");
    assert!(crate::scaler3::execute_split(&state, &f1l_ref, 0, 0x8000_0000_0000_0000).await);
    for i in 4..6 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/f1l/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("authorization", tok.as_str()),
                ("prisma-routing-key", "ga"),
            ],
            format!(r#"{{"k":"ga","n":{i}}}"#).as_bytes(),
        )
        .await;
        assert_eq!(st, 200, "post-split append {i}");
    }

    crate::failpoints::park_sse_before_lease_gate("f1l");
    let _guard = SseGateGuard("f1l".into());
    let mut sub = rig_sse(addr, "f1l", &tok, "?routingKey=ga&cursor=beginning", None).await;
    for _ in 0..200 {
        if crate::failpoints::parked(crate::failpoints::Fp::SseBeforeLeaseGate, "f1l") > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        crate::failpoints::parked(crate::failpoints::Fp::SseBeforeLeaseGate, "f1l") > 0,
        "request never reached the construction window"
    );
    rig_publish_policy(&svc, rig_policy("proj-f1l", "ws_other", 2, 2), 2).unwrap();
    rig_publish_grants(
        &svc,
        "proj-f1l",
        &[("c1", crate::project_policy::CredentialStatus::Revoked, 2)],
        2,
    )
    .unwrap();
    crate::failpoints::release_sse_before_lease_gate("f1l");

    assert_no_frames_after_pre_establishment_invalidation(&mut sub, "lineage").await;
}
