//! Security isolation.

use super::fixture_http::{engine_shutdown, http_rig_with_auth_service};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

/// Stage 5d (review item 1): ONE enforce-mode cell serves TWO
/// projects. The verified principal — not the deployment tenant —
/// selects the storage identity, so both projects create the SAME
/// stream name, write distinct records, and each reads back only its
/// own; the catalog shows each credential its own project's streams
/// only. This is the request path the layout-4 storage was built for.
#[expect(
    clippy::too_many_lines,
    reason = "two-project isolation scenario; the verified principals, cross-project requests and each project's private view form one causal sequence; helper phases would hide which boundary leaked"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_cell_serves_two_projects_with_full_isolation() {
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
        "mp-1".to_string(),
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
                  streams.metadata.read streams.catalog.read";
    let mut projects = std::collections::HashMap::new();
    let mut credentials = std::collections::HashMap::new();
    for (proj, ws, cred) in [("proj-test", "ws_a", "c_a"), ("proj-b", "ws_b", "c_b")] {
        let pid = crate::tenant::ProjectId::new(proj).unwrap();
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
    }
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
    let mint = |cred: &str, proj: &str, ws: &str| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("mp-1".into());
        jsonwebtoken::encode(
            &header,
            &C {
                iss: "https://auth.prisma.io",
                aud: "prisma-streams-data",
                sub: "u",
                credential_id: cred,
                project_id: proj,
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
        .unwrap()
    };
    let ta = format!("Bearer {}", mint("c_a", "proj-test", "ws_a"));
    let tb = format!("Bearer {}", mint("c_b", "proj-b", "ws_b"));
    let auth_a = ("authorization", ta.as_str());
    let auth_b = ("authorization", tb.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let body = br#"{"format":{"kind":"json"}}"#;

    // Both projects create the SAME name on the SAME cell — two
    // streams, not one.
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/shared", &[ekey, auth_a], body).await;
    assert_eq!(st, 201, "A create: {}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/shared", &[ekey, auth_b], body).await;
    assert_eq!(st, 201, "B create: {}", String::from_utf8_lossy(&b));

    // Distinct writes...
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/shared/records",
        &[ekey, auth_a],
        br#"{"who":"a"}"#,
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/shared/records",
        &[ekey, auth_b],
        br#"{"who":"b"}"#,
    )
    .await;
    assert_eq!(st, 200);

    // ...and each principal reads back ONLY its own project's records.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/shared/records",
        &[ekey, auth_a],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let sa = String::from_utf8_lossy(&b).to_string();
    assert!(sa.contains(r#""who":"a""#), "A sees its record: {sa}");
    assert!(!sa.contains(r#""who":"b""#), "A must not see B's: {sa}");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/shared/records",
        &[ekey, auth_b],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let sb = String::from_utf8_lossy(&b).to_string();
    assert!(sb.contains(r#""who":"b""#), "B sees its record: {sb}");
    assert!(!sb.contains(r#""who":"a""#), "B must not see A's: {sb}");

    // A second stream only in B: A's catalog never shows it.
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/only-b", &[ekey, auth_b], body).await;
    assert_eq!(st, 201);
    let names = |b: &[u8]| -> Vec<String> {
        serde_json::from_slice::<serde_json::Value>(b).unwrap()["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap().to_string())
            .collect()
    };
    let (st, _, b) = preq(addr, "GET", "/v1/streams", &[auth_a], b"").await;
    assert_eq!(st, 200);
    let na = names(&b);
    assert!(
        na.contains(&"shared".to_string()) && !na.contains(&"only-b".to_string()),
        "A catalog: {na:?}"
    );
    let (st, _, b) = preq(addr, "GET", "/v1/streams", &[auth_b], b"").await;
    assert_eq!(st, 200);
    let nb = names(&b);
    assert!(
        nb.contains(&"shared".to_string()) && nb.contains(&"only-b".to_string()),
        "B catalog: {nb:?}"
    );
    engine_shutdown(&state).await;
}

/// Stage 6 exit / review item 8: the noisy-neighbor campaign at the
/// mechanism level. TWO projects on one enforce cell — B (hostile,
/// tight quotas) hammers as fast as it can while A (compliant,
/// generous quotas) sends a paced steady load CONCURRENTLY. The
/// backstop must (a) refuse B with the project-scoped 429s, (b) admit
/// EVERY one of A's requests — no refusal leaks across projects — and
/// (c) keep A's worst-case latency bounded while B floods. The
/// at-scale binary campaign (latency percentiles under real load)
/// remains a field exercise for the release push.
#[expect(
    clippy::too_many_lines,
    reason = "noisy-neighbor scenario; two projects on one enforce cell, the campaign against one and the compliant project's service form one causal sequence; helper phases would hide which mechanism let the noise through"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn noisy_neighbor_cannot_degrade_a_compliant_project() {
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
        "nn-1".to_string(),
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
    let mut projects = std::collections::HashMap::new();
    let mut credentials = std::collections::HashMap::new();
    for (proj, ws, cred, rps, recs) in [
        // A: generous — its paced load must never brush the limits.
        ("proj-test", "ws_a", "c_a", 10_000u64, 100_000u64),
        // B: tight — the flood must hit the wall immediately.
        ("proj-b", "ws_b", "c_b", 5, 10),
    ] {
        let pid = crate::tenant::ProjectId::new(proj).unwrap();
        projects.insert(
            pid.clone(),
            crate::project_policy::ProjectPolicy {
                project_id: pid.clone(),
                workspace_id: crate::tenant::WorkspaceId::new(ws).unwrap(),
                cell_id: std::sync::Arc::from("test-cell"),
                project_policy_version: 1,
                ownership_version: 1,
                status: crate::project_policy::ProjectStatus::Active,
                quotas: crate::project_policy::ProjectQuotas {
                    requests_per_sec: rps,
                    append_records_per_sec: recs,
                    ..Default::default()
                },
            },
        );
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
    }
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
    let mint = |cred: &str, proj: &str, ws: &str| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("nn-1".into());
        jsonwebtoken::encode(
            &header,
            &C {
                iss: "https://auth.prisma.io",
                aud: "prisma-streams-data",
                sub: "u",
                credential_id: cred,
                project_id: proj,
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
        .unwrap()
    };
    let ta = format!("Bearer {}", mint("c_a", "proj-test", "ws_a"));
    let tb = format!("Bearer {}", mint("c_b", "proj-b", "ws_b"));
    let body = br#"{"format":{"kind":"json"}}"#;

    // Both create their own stream (uses one token from each bucket).
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/nn",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", &ta),
        ],
        body,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/nn",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", &tb),
        ],
        body,
    )
    .await;
    assert_eq!(st, 201);

    // CONCURRENT: B floods 300 appends flat-out; A paces 40 appends at
    // 25ms intervals (~1s of steady compliant load).
    let hostile = {
        let tb = tb.clone();
        tokio::spawn(async move {
            let mut refused = 0u32;
            let mut ok = 0u32;
            let mut codes: std::collections::HashSet<String> = Default::default();
            for i in 0..300u32 {
                let (st, _, b) = preq(
                    addr,
                    "POST",
                    "/v1/streams/nn/records",
                    &[
                        ("prisma-encryption-key", PRISMA_KEY),
                        ("authorization", &tb),
                    ],
                    format!("{{\"n\":{i}}}").as_bytes(),
                )
                .await;
                if st == 429 {
                    refused += 1;
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b)
                        && let Some(c) = v["error"]["code"].as_str()
                    {
                        codes.insert(c.to_string());
                    }
                } else if st == 200 {
                    ok += 1;
                }
            }
            (ok, refused, codes)
        })
    };
    let compliant = {
        let ta = ta.clone();
        tokio::spawn(async move {
            let mut worst_ms: u128 = 0;
            let mut failures: Vec<(u16, String)> = Vec::new();
            for i in 0..40u32 {
                let t0 = std::time::Instant::now();
                let (st, _, b) = preq(
                    addr,
                    "POST",
                    "/v1/streams/nn/records",
                    &[
                        ("prisma-encryption-key", PRISMA_KEY),
                        ("authorization", &ta),
                    ],
                    format!("{{\"n\":{i}}}").as_bytes(),
                )
                .await;
                worst_ms = worst_ms.max(t0.elapsed().as_millis());
                if st != 200 {
                    failures.push((st, String::from_utf8_lossy(&b).into_owned()));
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            (worst_ms, failures)
        })
    };
    let (hostile_out, compliant_out) = tokio::join!(hostile, compliant);
    let (b_ok, b_refused, b_codes) = hostile_out.unwrap();
    let (a_worst_ms, a_failures) = compliant_out.unwrap();

    // (a) The backstop FIRED on the hostile project, with the
    // project-scoped refusal class...
    assert!(
        b_refused > 200,
        "hostile flood must be mostly refused: ok={b_ok} refused={b_refused}"
    );
    assert!(b_ok >= 1, "B's in-quota fraction still lands: {b_ok}");
    assert!(
        b_codes.iter().all(|c| c.starts_with("project_")),
        "refusals are project-scoped: {b_codes:?}"
    );
    // (b) ...and NOT ONE compliant request was refused or failed.
    assert!(
        a_failures.is_empty(),
        "the compliant neighbor was degraded: {a_failures:?}"
    );
    // (c) A's worst latency stayed sane while B flooded (generous
    // in-process bound — the real envelope is the field campaign's).
    assert!(
        a_worst_ms < 2_000,
        "compliant worst-case latency {a_worst_ms}ms under flood"
    );
    engine_shutdown(&state).await;
}
