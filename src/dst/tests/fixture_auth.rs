//! Fixture auth.

use super::fixture_http::http_rig_with_auth_service;
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;
use std::sync::Arc;

/// One-project enforce rig for the Søren-review red tests: publishes
/// jwks+policy+grant for a single project and mints one token.
pub(super) async fn sr_rig(
    project: &str,
    ws: &str,
    cred: &str,
    kid: &str,
    scopes: &str,
) -> (
    std::sync::Arc<crate::http::AppState>,
    std::net::SocketAddr,
    String,
) {
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
        kid.to_string(),
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
    let pid = crate::tenant::ProjectId::new(project).unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new(ws).unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 1,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas::default(),
        },
    );
    let mut credentials = std::collections::HashMap::new();
    credentials.insert(
        std::sync::Arc::from(cred),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from(cred),
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
    header.kid = Some(kid.to_string());
    let token = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: cred,
            project_id: project,
            workspace_id: ws,
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
    (state, addr, format!("Bearer {token}"))
}

// ---------------------------------------------------------------------------
// SR2 (second review round): red tests. Written BEFORE the fixes and
// confirmed FAILING at ce475426 — findings 1-3 of the follow-up review.
// ---------------------------------------------------------------------------

/// Mint a workload JWT with an explicit operations claim (§14.1).
pub(super) fn sr2_workload_jwt(kid: &str, operations: &[&str], now: i64) -> String {
    sr2_workload_jwt_exp(kid, operations, now + 300)
}

/// Round-4 finding 2: same, with an explicit expiry.
pub(super) fn sr2_workload_jwt_exp(kid: &str, operations: &[&str], exp: i64) -> String {
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
    let now = crate::shard::now_ms() / 1000;
    #[derive(serde::Serialize)]
    struct W<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        cell_id: &'a str,
        operations: Vec<&'a str>,
        iat: i64,
        exp: i64,
    }
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(
        &header,
        &W {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-internal",
            sub: "slot-op",
            cell_id: "test-cell",
            operations: operations.to_vec(),
            iat: now - 1,
            exp,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap()
}

// ===================================================================
// Review round 3, Phase 0: the long-lived authorization boundary.
// Shared rig helpers (enforce mode, RSA kid "rig-1").
// ===================================================================
pub(super) const RIG_PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
pub(super) const RIG_PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
pub(super) const RIG_SCOPES: &str = "streams.create streams.records.append streams.records.read \
                          streams.metadata.read";

pub(super) fn rig_policy(
    project: &str,
    ws: &str,
    ov: u64,
    ppv: u64,
) -> crate::project_policy::ProjectPolicy {
    crate::project_policy::ProjectPolicy {
        project_id: crate::tenant::ProjectId::new(project).unwrap(),
        workspace_id: crate::tenant::WorkspaceId::new(ws).unwrap(),
        cell_id: std::sync::Arc::from("test-cell"),
        project_policy_version: ppv,
        ownership_version: ov,
        status: crate::project_policy::ProjectStatus::Active,
        quotas: crate::project_policy::ProjectQuotas::default(),
    }
}

pub(super) fn rig_publish_policy(
    svc: &crate::auth::AuthService,
    p: crate::project_policy::ProjectPolicy,
    fv: u64,
) -> Result<(), &'static str> {
    let mut projects = std::collections::HashMap::new();
    projects.insert(p.project_id.clone(), p);
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: fv,
    })
}

pub(super) fn rig_publish_grants(
    svc: &crate::auth::AuthService,
    project: &str,
    creds: &[(&str, crate::project_policy::CredentialStatus, u64)],
    fv: u64,
) -> Result<(), &'static str> {
    let mut credentials = std::collections::HashMap::new();
    for (cred, status, gv) in creds {
        credentials.insert(
            std::sync::Arc::from(*cred),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from(*cred),
                project_id: crate::tenant::ProjectId::new(project).unwrap(),
                grant_version: *gv,
                status: *status,
                scopes: crate::tenant::ScopeSet::parse(RIG_SCOPES).0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
    }
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: fv,
    })
}

/// Enforce-mode rig: keys + policy v1 (Active, ov 1) + the given
/// credentials (Active, gv 1). `staleness` shortens the feed window.
pub(super) async fn auth_rig(
    project: &str,
    ws: &str,
    creds: &[&str],
    staleness: Option<i64>,
) -> (
    std::sync::Arc<crate::auth::AuthService>,
    Arc<crate::http::AppState>,
    std::net::SocketAddr,
) {
    let now = crate::shard::now_ms() / 1000;
    let svc = std::sync::Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    if let Some(s) = staleness {
        svc.set_staleness_max_secs(s);
    }
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        "rig-1".to_string(),
        crate::auth::JwksKey {
            alg: jsonwebtoken::Algorithm::RS256,
            key: jsonwebtoken::DecodingKey::from_rsa_pem(RIG_PUB.as_bytes()).unwrap(),
            fp: crate::auth::key_fp(RIG_PUB.as_bytes()),
        },
    );
    svc.publish_jwks(crate::auth::JwksSnapshot {
        keys,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    rig_publish_policy(&svc, rig_policy(project, ws, 1, 1), 1).unwrap();
    let cs: Vec<_> = creds
        .iter()
        .map(|c| (*c, crate::project_policy::CredentialStatus::Active, 1u64))
        .collect();
    rig_publish_grants(&svc, project, &cs, 1).unwrap();
    let (state, addr) = http_rig_with_auth_service(mem(), svc.clone()).await;
    (svc, state, addr)
}

pub(super) fn mint_token(
    cred: &str,
    project: &str,
    ws: &str,
    ov: u64,
    gv: u64,
    jti: &str,
    exp_in: i64,
) -> String {
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
    let now = crate::shard::now_ms() / 1000;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("rig-1".into());
    let tok = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: cred,
            project_id: project,
            workspace_id: ws,
            cell_id: "test-cell",
            ownership_version: ov,
            grant_version: gv,
            scope: RIG_SCOPES,
            jti,
            iat: now - 60,
            exp: now + exp_in,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(RIG_PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    format!("Bearer {tok}")
}

pub(super) async fn rig_create(addr: std::net::SocketAddr, name: &str, bearer: &str) {
    let (st, _, _) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{name}"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", bearer),
        ],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "create {name}");
}

pub(super) async fn rig_append(
    addr: std::net::SocketAddr,
    name: &str,
    bearer: &str,
    body: &str,
) -> u16 {
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", bearer),
        ],
        body.as_bytes(),
    )
    .await;
    st
}

/// Raw SSE connect with bearer + optional query; `rcvbuf` shrinks the
/// socket receive buffer so "slow reader" legs can really stall the
/// producer (loopback buffers otherwise swallow megabytes).
pub(super) async fn rig_sse(
    addr: std::net::SocketAddr,
    name: &str,
    bearer: &str,
    query: &str,
    rcvbuf: Option<u32>,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    if let Some(b) = rcvbuf {
        sock.set_recv_buffer_size(b).unwrap();
    }
    let mut sck = sock.connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/streams/{name}/records:sse{query} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nauthorization: {bearer}\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    sck
}
