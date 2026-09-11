//! Quota enforcement.

use super::fixture_http::{engine_shutdown, http_rig, http_rig_with_auth_service};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::{mem, skey};

/// MT Stage 6a (§17.3): the server-side project admission backstop on
/// the wire. A verified project with `requests_per_sec: 2` in the
/// CURRENT policy gets two requests and then 429 `project_rate_limit`
/// with a Retry-After — scoped to that project alone (bucket isolation
/// is pinned at the unit level).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn project_rate_quota_backstop_answers_429() {
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
        "q-1".to_string(),
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
    let pid = crate::tenant::ProjectId::new("proj-test").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new("ws_789").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 1,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas {
                requests_per_sec: 2,
                ..Default::default()
            },
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
        std::sync::Arc::from("cq"),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from("cq"),
            project_id: pid,
            grant_version: 1,
            status: crate::project_policy::CredentialStatus::Active,
            scopes: crate::tenant::ScopeSet::parse("streams.metadata.read streams.create").0,
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
    header.kid = Some("q-1".into());
    let jwt = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: "cq",
            project_id: "proj-test",
            workspace_id: "ws_789",
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: "streams.metadata.read streams.create",
            jti: "t",
            iat: now - 60,
            exp: now + 600,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    let bearer = format!("Bearer {jwt}");
    let auth = ("authorization", bearer.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);

    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qb",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(addr, "GET", "/v1/streams/qb", &[ekey, auth], b"").await;
    assert_eq!(st, 200);
    // Third request inside the same second: the project's bucket is
    // dry — 429 project_rate_limit, Retry-After present, retryable.
    let (st, h, b) = preq(addr, "GET", "/v1/streams/qb", &[ekey, auth], b"").await;
    assert_eq!(st, 429, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "project_rate_limit");
    assert_eq!(v["error"]["retryable"], true);
    assert!(
        h.get("retry-after").and_then(|s| s.parse::<u64>().ok()) >= Some(1),
        "retry-after header: {h:?}"
    );
    engine_shutdown(&state).await;
}

/// MT Stage 6b (§17.2 volume backstop): append records are metered
/// with the EXACT parsed count — a batch of 3 is 3, not 1 — against
/// the policy's append_records_per_sec, and refusal is the same 429
/// project_rate_limit. No sleeps: the legs spend a 2-token budget and
/// probe both the batch-count and the drained-bucket refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volume_quotas_meter_appends_and_reads() {
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
        "v-1".to_string(),
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
    let pid = crate::tenant::ProjectId::new("proj-test").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new("ws_789").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 1,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas {
                append_records_per_sec: 2,
                read_bytes_per_sec: 8,
                ..Default::default()
            },
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
        std::sync::Arc::from("cv"),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from("cv"),
            project_id: pid,
            grant_version: 1,
            status: crate::project_policy::CredentialStatus::Active,
            scopes: crate::tenant::ScopeSet::parse(
                "streams.create streams.records.append streams.records.read \
                 streams.metadata.read",
            )
            .0,
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
    header.kid = Some("v-1".into());
    let jwt = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: "cv",
            project_id: "proj-test",
            workspace_id: "ws_789",
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: "streams.create streams.records.append streams.records.read \
                    streams.metadata.read",
            jti: "t",
            iat: now - 60,
            exp: now + 600,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    let bearer = format!("Bearer {jwt}");
    let auth = ("authorization", bearer.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let code = |b: &[u8]| -> String {
        serde_json::from_slice::<serde_json::Value>(b)
            .map(|v| v["error"]["code"].as_str().unwrap_or("").to_string())
            .unwrap_or_default()
    };

    // Create is not append volume.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/vq",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // One record: budget 2 -> ~1 left.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/vq/records",
        &[ekey, auth],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    // A 3-record batch is THREE records, and 3 > 1: refused — the
    // count is the parsed batch size, not "one request".
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/vq/records:batch",
        &[ekey, auth],
        br#"[{"n":2},{"n":3},{"n":4}]"#,
    )
    .await;
    assert_eq!(st, 429, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "project_rate_limit");
    // A refused batch consumed nothing: one single record still fits.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/vq/records",
        &[ekey, auth],
        br#"{"n":5}"#,
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    // Bucket drained: the next record is refused.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/vq/records",
        &[ekey, auth],
        br#"{"n":6}"#,
    )
    .await;
    assert_eq!(st, 429, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "project_rate_limit");

    // Read volume is POST-HOC: the first read serves (no debt yet) and
    // its response — larger than the 64 B/s budget — leaves the bucket
    // in debt, so the SECOND read is refused with the same 429 class.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/vq/records", &[ekey, auth], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    assert!(b.len() > 8, "response must exceed the budget: {}", b.len());
    let (st, _, b) = preq(addr, "GET", "/v1/streams/vq/records", &[ekey, auth], b"").await;
    assert_eq!(st, 429, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "project_rate_limit");
    engine_shutdown(&state).await;
}

/// Build an enforce rig with ONE project whose quotas are custom —
/// the SR2-4 quota-enforcement fixture.
async fn quota_rig(
    tag: &str,
    scopes: &str,
    quotas: crate::project_policy::ProjectQuotas,
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
    let kid = format!("q-{tag}");
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        kid.clone(),
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
    let pid = crate::tenant::ProjectId::new(&format!("proj-q-{tag}")).unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new(&format!("ws-q-{tag}")).unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 1,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas,
        },
    );
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let mut credentials = std::collections::HashMap::new();
    let cred: std::sync::Arc<str> = std::sync::Arc::from(format!("c-q-{tag}"));
    credentials.insert(
        cred.clone(),
        crate::project_policy::CredentialGrant {
            credential_id: cred,
            project_id: pid,
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
    header.kid = Some(kid);
    let tok = jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: &format!("c-q-{tag}"),
            project_id: &format!("proj-q-{tag}"),
            workspace_id: &format!("ws-q-{tag}"),
            cell_id: "test-cell",
            ownership_version: 1,
            grant_version: 1,
            scope: scopes,
            jti: "t",
            iat: now - 60,
            exp: now + 3600,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    (state, addr, format!("Bearer {tok}"))
}

/// RED (review, declared quotas): max_streams is enforced RACE-SAFELY
/// at create. Sequential creates beyond the cap refuse typed; a
/// concurrent burst admits exactly the cap; a hard delete releases a
/// slot. Soft-deleted fork-retained names still count until their
/// terminal hard delete (posture: they hold storage and the name).
#[expect(
    clippy::disallowed_methods,
    reason = "stream quota fixture; the racing creation is joined before the quota verdicts are compared; it must run concurrently to contend for the last slot"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_streams_is_enforced_at_create() {
    let quotas = crate::project_policy::ProjectQuotas {
        max_streams: 3,
        ..Default::default()
    };
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.lifecycle.manage";
    let (_state, addr, auth) = quota_rig("ms", scopes, quotas).await;
    let a = ("authorization", auth.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let body = br#"{"format":{"kind":"json"}}"#;
    for i in 0..3 {
        let (st, _, b) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/ms-{i}"),
            &[ekey, a],
            body,
        )
        .await;
        assert_eq!(st, 201, "create {i}: {}", String::from_utf8_lossy(&b));
    }
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/ms-3", &[ekey, a], body).await;
    assert_eq!(
        st,
        429,
        "create beyond max_streams must refuse typed: {st} {}",
        String::from_utf8_lossy(&b)
    );
    assert!(
        String::from_utf8_lossy(&b).contains("stream_limit"),
        "typed code: {}",
        String::from_utf8_lossy(&b)
    );
    // Idempotent re-create of an EXISTING stream is not a new stream.
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/ms-0", &[ekey, a], body).await;
    assert!(st == 200 || st == 201, "idempotent recreate: {st}");
    // A hard delete releases the slot.
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/ms-2", &[ekey, a], b"").await;
    assert!(st == 200 || st == 204, "delete: {st}");
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/ms-3", &[ekey, a], body).await;
    assert_eq!(
        st,
        201,
        "slot released by hard delete: {st} {}",
        String::from_utf8_lossy(&b)
    );

    // Concurrent burst on a FRESH project: 6 racing creates, cap 3 —
    // exactly 3 win. (Same rig, distinct names; the 3 already-created
    // streams occupy the cap, so first free the project by deleting.)
    for n in ["ms-0", "ms-1", "ms-3"] {
        let (st, _, _) = preq(addr, "DELETE", &format!("/v1/streams/{n}"), &[ekey, a], b"").await;
        assert!(st == 200 || st == 204);
    }
    let mut handles = Vec::new();
    for i in 0..6 {
        let auth2 = auth.clone();
        let h = tokio::spawn(async move {
            let a2 = ("authorization", auth2.as_str());
            let ekey2 = ("prisma-encryption-key", PRISMA_KEY);
            let (st, _, _) = preq(
                addr,
                "PUT",
                &format!("/v1/streams/burst-{i}"),
                &[ekey2, a2],
                br#"{"format":{"kind":"json"}}"#,
            )
            .await;
            st
        });
        handles.push(h);
    }
    let mut created = 0;
    for h in handles {
        let st = h.await.unwrap();
        if st == 201 {
            created += 1;
        } else {
            assert_eq!(st, 429, "loser must refuse typed: {st}");
        }
    }
    assert_eq!(created, 3, "exactly the cap may win the burst");
}

/// RED (review, declared quotas): queued append bytes are charged to
/// the project BEFORE the committer sees the record and released when
/// the append is DECIDED. A 1-byte budget refuses even one small
/// append; a modest budget survives many sequential appends whose
/// TOTAL far exceeds it (release works — a leak would wedge).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_append_bytes_charge_and_release() {
    let scopes = "streams.create streams.records.append streams.records.read";
    let tiny = crate::project_policy::ProjectQuotas {
        queued_append_bytes: 1,
        ..Default::default()
    };
    let (_s1, addr, auth) = quota_rig("qb1", scopes, tiny).await;
    let a = ("authorization", auth.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qb",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/qb/records",
        &[ekey, a, ct],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(
        st,
        429,
        "a 1-byte queued budget must refuse the append typed: {st} {}",
        String::from_utf8_lossy(&b)
    );
    assert!(
        String::from_utf8_lossy(&b).contains("queued_bytes"),
        "typed code: {}",
        String::from_utf8_lossy(&b)
    );

    let modest = crate::project_policy::ProjectQuotas {
        queued_append_bytes: 4096,
        ..Default::default()
    };
    let (_s2, addr2, auth2) = quota_rig("qb2", scopes, modest).await;
    let a2 = ("authorization", auth2.as_str());
    let (st, _, _) = preq(
        addr2,
        "PUT",
        "/v1/streams/qb",
        &[ekey, a2],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // 50 sequential ~300B appends = ~15KB total through a 4KB budget:
    // passes ONLY if every decided append releases its charge.
    let payload = format!("{{\"pad\":\"{}\"}}", "x".repeat(280));
    for i in 0..50 {
        let (st, _, b) = preq(
            addr2,
            "POST",
            "/v1/streams/qb/records",
            &[ekey, a2, ct],
            payload.as_bytes(),
        )
        .await;
        assert_eq!(
            st,
            200,
            "decided appends must release their charge (i={i}): {st} {}",
            String::from_utf8_lossy(&b)
        );
    }
}

/// RED (review, declared quotas): capability watch WAITS occupy the
/// project's live-subscription pool — a project cannot hold unbounded
/// long polls through capabilities.
#[expect(
    clippy::disallowed_methods,
    reason = "subscription pool fixture; the long watch is joined after the refused second wait is observed; it must hold a pool slot concurrently"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_waits_occupy_the_subscription_pool() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.watches.manage";
    let quotas = crate::project_policy::ProjectQuotas {
        max_live_subscriptions: 1,
        ..Default::default()
    };
    let (state, addr, auth) = quota_rig("ww", scopes, quotas).await;
    let a = ("authorization", auth.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, a],
        br#"{"format":{"kind":"json"},"watches":[{"name":"by-x","fields":["/id"]}]}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-q-ww")
        .unwrap()
        .stream_ref("orders");
    let desc = state.registry.get(&bref).await.unwrap().unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let khex = format!("{:016x}", 7u64);
    let tokk = crate::crypto::touch_token(&skey(), &epoch);
    let sk = crate::crypto::wait_sig_key(&tokk, &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let cap = format!(
        "proj-q-ww.{exp}.{}",
        crate::crypto::watch_capability_sig(
            &sk,
            &bref,
            &desc.stream_epoch,
            "by-x",
            &khex,
            "GET",
            exp
        )
    );
    let path_long =
        format!("/v1/streams/orders/watches/by-x/keys/{khex}?cursor=now&timeoutMs=5000&cap={cap}");
    let path_short =
        format!("/v1/streams/orders/watches/by-x/keys/{khex}?cursor=now&timeoutMs=100&cap={cap}");
    // Occupy the single subscription slot with a LONG wait...
    let long = tokio::spawn(async move { preq(addr, "GET", &path_long, &[], b"").await });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // ...the second concurrent wait must refuse typed, not stack.
    let (st, _, b) = preq(addr, "GET", &path_short, &[], b"").await;
    assert_eq!(
        st,
        429,
        "second capability wait must refuse when the pool is full: {st} {}",
        String::from_utf8_lossy(&b)
    );
    let (st_long, _, _) = long.await.unwrap();
    assert_eq!(st_long, 200, "the FIRST wait still completes");
    engine_shutdown(&state).await;
}

/// RED (round-3 finding 2.2): a customer may legally name a stream
/// with '#'. It must COUNT against max_streams (after a reseed) and
/// its hard delete must RELEASE the slot — no name-syntax
/// classification of resource kinds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn customer_streams_with_hash_count_and_release() {
    let quotas = crate::project_policy::ProjectQuotas {
        max_streams: 2,
        ..Default::default()
    };
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read streams.lifecycle.manage";
    let (_state, addr, auth) = quota_rig("hs", scopes, quotas).await;
    let a = ("authorization", auth.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let body = br#"{"format":{"kind":"json"}}"#;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders%23archive",
        &[ekey, a],
        body,
    )
    .await;
    assert_eq!(st, 201, "'#' name creates");
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/plain", &[ekey, a], body).await;
    assert_eq!(st, 201);
    // Cap 2 reached — the '#' stream occupies a REAL slot.
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/third", &[ekey, a], body).await;
    assert_eq!(st, 429, "'#' stream must occupy a slot: {st}");
    // Its hard delete releases.
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/orders%23archive",
        &[ekey, a],
        b"",
    )
    .await;
    assert!(st == 200 || st == 204, "delete '#': {st}");
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/third", &[ekey, a], body).await;
    assert_eq!(st, 201, "'#' delete must release the slot: {st}");
}

/// RED (round-3 finding 2.1): a catalog failure during the max_streams
/// seed fails CLOSED with a retryable 503 — a partial count must
/// never seed the limiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_failure_fails_the_stream_seed_closed() {
    let quotas = crate::project_policy::ProjectQuotas {
        max_streams: 5,
        ..Default::default()
    };
    let scopes = "streams.create streams.records.append streams.records.read";
    let (state, addr, auth) = quota_rig("cf", scopes, quotas).await;
    let a = ("authorization", auth.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    state.registry.fail_next_list("proj-q-cf");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cfx",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        503,
        "catalog failure must fail the seed closed: {st} {}",
        String::from_utf8_lossy(&b)
    );
    // The failpoint consumed itself: the retry succeeds.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cfx",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "retry after catalog recovery: {st}");
    engine_shutdown(&state).await;
}

/// RED (round-3 finding 2.3): the fork CASCADE's terminal hard delete
/// releases the max_streams slot — not only the direct delete path.
/// Observed through the quota registry on the deployment tenant: the
/// probe reservation at a cap of 1 succeeds only if BOTH the fork's
/// own delete and the cascaded source tombstone released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_cascade_hard_delete_releases_the_stream_slot() {
    let (state, addr) = http_rig(mem()).await;
    let ct = ("content-type", "application/json");
    let auth = ("authorization", "Bearer dst-internal-token");
    // Source with one record, then a fork of it.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/fcq-src",
        &[ct, auth],
        br#"[{"i":0}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/fcq-src", &[auth], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let fork = [
        ct,
        auth,
        ("stream-forked-from", "fcq-src"),
        ("stream-fork-offset", boundary.as_str()),
    ];
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/fcq-child", &fork, b"").await;
    assert!(
        st == 200 || st == 201,
        "fork create: {st} {}",
        String::from_utf8_lossy(&b)
    );
    // Track + seed the deployment project at the CURRENT live count.
    let tenant = state.deployment.deployment_tenant().clone();
    let q = crate::project_policy::ProjectQuotas::default();
    let _ = state.quotas.admit(&tenant, &q, crate::shard::now_ms());
    let seed_q = crate::project_policy::ProjectQuotas {
        max_streams: 100,
        ..Default::default()
    };
    let live0 = 2u64; // fcq-src + fcq-child
    drop(
        state
            .quotas
            .reserve_stream(&tenant, &seed_q, Some(live0))
            .expect("seed"),
    );
    // Soft delete the source (fork retains it): still counts.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fcq-src", &[auth], b"").await;
    assert!(st == 200 || st == 204, "source delete: {st}");
    // Delete the fork: its own hard delete releases one slot AND the
    // cascade tombstones the retained source — releasing the second.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fcq-child", &[auth], b"").await;
    assert!(st == 200 || st == 204, "fork delete: {st}");
    // Probe: count must be back to 0 — a cap of 1 admits.
    let probe_q = crate::project_policy::ProjectQuotas {
        max_streams: 1,
        ..Default::default()
    };
    match state.quotas.reserve_stream(&tenant, &probe_q, None) {
        Ok(Some(r)) => drop(r),
        other => panic!(
            "cascade must release the retained source's slot (count stuck above 0): {:?}",
            other.err()
        ),
    }
    engine_shutdown(&state).await;
}

/// Quota transition: max_streams 0 (unlimited) -> nonzero seeds from
/// the catalog at the FIRST limited create, counting what unlimited
/// mode already made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_streams_transition_seeds_from_reality() {
    let scopes = "streams.create streams.records.append streams.records.read";
    let unlimited = crate::project_policy::ProjectQuotas::default();
    let (state, addr, auth) = quota_rig("tr", scopes, unlimited).await;
    let a = ("authorization", auth.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    for i in 0..3 {
        let (st, _, _) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/t{i}"),
            &[ekey, a],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    // Policy update: the limit arrives BELOW the existing count.
    let pid = crate::tenant::ProjectId::new("proj-q-tr").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid,
            workspace_id: crate::tenant::WorkspaceId::new("ws-q-tr").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 2,
            ownership_version: 1,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas {
                max_streams: 2,
                ..Default::default()
            },
        },
    );
    state
        .auth
        .publish_policies(crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: crate::shard::now_ms() / 1000,
            feed_version: 2,
        })
        .unwrap();
    // The next create seeds count=3 from the catalog and refuses.
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/t3",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        429,
        "re-enabled limit must seed from the real count: {st} {}",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}
