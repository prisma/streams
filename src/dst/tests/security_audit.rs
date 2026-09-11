//! Security audit.

use super::fixture_auth::sr_rig;
use super::fixture_http::{engine_shutdown, http_rig_with_auth_service};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

/// MT Stage 7 / §10.4 `_audit_events`: enforce-mode denials journal to
/// a durable system stream. A verified-but-underscoped request lands
/// with its VERIFIED project attached; a garbage token lands with no
/// project (unverified claims are never identity); the journal lives
/// under the system project where no customer credential reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enforce_denials_journal_to_audit_events() {
    // Deliberately NO streams.records.read: the read leg must deny.
    let scopes = "streams.create streams.records.append";
    let (state, addr, token) = sr_rig("proj-audj", "ws_audj", "c_audj", "audj-1", scopes).await;
    let auth = ("authorization", token.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);

    // A fresh runtime owns an empty journal; another rig cannot drain into it.
    assert_eq!(crate::audit::drain_audit_once(&state).await.unwrap(), 0);

    // Compliant create: no denial, no journal entry.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/audj-probe",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Verified but underscoped: 403, journals WITH the project.
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/audj-probe/records",
        &[ekey, auth],
        b"",
    )
    .await;
    assert_eq!(st, 403);
    // Garbage token: 401, journals WITHOUT a project.
    let bad = ("authorization", "Bearer not-a-jwt");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/audj-probe/records",
        &[ekey, bad],
        b"",
    )
    .await;
    assert_eq!(st, 401);
    // Wrong ENCRYPTION key on an authorized append: 403 wrong_key —
    // key-guessing probes journal too, and the entry wrapper fills the
    // verified project into the event.
    let wkey = (
        "prisma-encryption-key",
        "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=",
    );
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/audj-probe/records",
        &[wkey, auth],
        br#"{"x":1}"#,
    )
    .await;
    assert_eq!(st, 403);

    // Drain this runtime's queue to its `_audit_events`.
    let mut appended = 0usize;
    for _ in 0..40 {
        let n = crate::audit::drain_audit_once(&state)
            .await
            .expect("audit drain");
        if n == 0 {
            break;
        }
        appended += n;
    }
    assert!(appended >= 2, "denials appended: {appended}");

    // Read through the system path; invalid JSON is a failed oracle.
    let events = audit_events(&state).await;
    let probe: Vec<&serde_json::Value> = events
        .iter()
        .filter(|e| e["route"] == "audj-probe/records")
        .collect();
    assert!(
        probe.iter().any(|e| e["code"] == "missing_scope"
            && e["project_id"] == "proj-audj"
            && e["method"] == "GET"
            && e["status"] == 403),
        "verified denial with project missing from journal: {events:?}"
    );
    assert!(
        probe
            .iter()
            .any(|e| e["status"] == 401 && e["project_id"].is_null()),
        "unverified denial (without identity) missing from journal: {events:?}"
    );
    assert!(
        probe
            .iter()
            .any(|e| e["code"] == "stale_or_wrong_credentials"
                && e["project_id"] == "proj-audj"
                && e["method"] == "POST"
                && e["status"] == 403),
        "wrong-key probe with wrapper-filled project missing: {events:?}"
    );
    assert!(
        !probe
            .iter()
            .any(|e| e["status"] == 401 && !e["project_id"].is_null()),
        "unverified claims must never journal as identity: {events:?}"
    );
    // The compliant create journaled nothing.
    assert!(
        !events.iter().any(|e| e["status"] == 201),
        "success responses must not journal: {events:?}"
    );
    engine_shutdown(&state).await;
}

/// Response tagging is independent of journal storage and runtime construction.
#[test]
fn audit_tags_distinguish_caller_failures_from_cell_failures() {
    // Journal scope (§8.1/§7.1): placement and own-feed staleness are
    // NOT denials of the caller and must never be tagged; real
    // verification failures are.
    for e in [
        crate::auth::AuthError::WrongCell,
        crate::auth::AuthError::PolicyStale,
        crate::auth::AuthError::KeysStale,
    ] {
        let r = crate::product::auth_failure_response(&e);
        assert!(
            r.extensions().get::<crate::audit::DenialTag>().is_none(),
            "{} must not journal",
            e.kind()
        );
    }
    let r = crate::product::auth_failure_response(&crate::auth::AuthError::Expired);
    assert!(
        r.extensions().get::<crate::audit::DenialTag>().is_some(),
        "expired must journal"
    );
}

/// Read the durable journal through the system path, retaining every event.
async fn audit_events(state: &std::sync::Arc<crate::http::AppState>) -> Vec<serde_json::Value> {
    let key = state.billing.usage_key().expect("rig usage key");
    let mut events = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..200 {
        let Some((page, next)) = crate::billing::system_read(
            state,
            crate::billing::AUDIT_EVENTS_STREAM,
            &key,
            cursor.clone(),
        )
        .await
        .expect("system read") else {
            break;
        };
        if page.is_empty() {
            break;
        }
        let values: Vec<serde_json::Value> =
            serde_json::from_slice(&page).expect("journal page is an array");
        if values.is_empty() {
            break;
        }
        events.extend(values);
        if cursor.as_deref() == Some(next.as_str()) {
            break;
        }
        cursor = Some(next);
    }
    events
}

/// MT Stage 8 (shared-cell certification, in-suite gate): 128 projects
/// on ONE cell — every project reuses the SAME stream name, a noisy
/// subset appends distinct volumes, the majority stays idle — and the
/// cell proves: per-project read isolation, per-project catalogs,
/// suspension and revocation cutoffs that touch ONLY their target,
/// balanced books under reconciliation, and journaled denials. The
/// at-scale (≥1,000 projects) and fleet legs run as the field
/// campaign; this gate keeps the shared-cell mechanism honest in CI.
#[expect(
    clippy::too_many_lines,
    reason = "shared-cell certification; isolation, suspension, revocation and books share one ordered runtime; splitting the scenario would hide their common state and before/after assertions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_cell_certification_smoke() {
    let _xr = crate::billing::billing_clock_lock().read().await;
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    // Suite default = 128 (fast gate). The at-scale certification leg
    // (contract Stage 8: >= 1,000 projects, 32-64 noisy) runs the SAME
    // test with MT_CERT_PROJECTS=1000 — one code path, two postures.
    #[expect(
        clippy::disallowed_methods,
        reason = "shared-cell certification owner; CI selects 128 or 1000 projects before fixture construction; removing the explicit process input would disconnect the at-scale campaign"
    )]
    let project_count: usize = std::env::var("MT_CERT_PROJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);
    let noisy_count: usize = if project_count >= 1000 { 48 } else { 16 };
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
        "cert-1".to_string(),
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
                  streams.metadata.read streams.usage.read streams.catalog.read";
    let proj_name = |i: usize| format!("proj-c{i:03}");
    let cred_name = |i: usize| format!("c-c{i:03}");
    // Workspaces are SHARED across projects (16 workspaces for 128
    // projects) — the realistic shape, and it exercises per-project
    // rollup rows under one account.
    let ws_name = |i: usize| format!("ws-c{:02}", i % 16);
    let mut base_projects = std::collections::HashMap::new();
    let mut base_credentials = std::collections::HashMap::new();
    for i in 0..project_count {
        let pid = crate::tenant::ProjectId::new(&proj_name(i)).unwrap();
        base_projects.insert(
            pid.clone(),
            crate::project_policy::ProjectPolicy {
                project_id: pid.clone(),
                workspace_id: crate::tenant::WorkspaceId::new(&ws_name(i)).unwrap(),
                cell_id: std::sync::Arc::from("test-cell"),
                project_policy_version: 1,
                ownership_version: 1,
                status: crate::project_policy::ProjectStatus::Active,
                quotas: crate::project_policy::ProjectQuotas::default(),
            },
        );
        base_credentials.insert(
            std::sync::Arc::from(cred_name(i).as_str()),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from(cred_name(i).as_str()),
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
        projects: base_projects.clone(),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: base_credentials.clone(),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let (state, addr) = http_rig_with_auth_service(mem(), svc.clone()).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    assert!(
        state.rollup.install(std::sync::Arc::new(rollup)).is_ok(),
        "fresh certification runtime has an empty rollup slot"
    );

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
    let enc = jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap();
    let mint = |i: usize| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("cert-1".into());
        let (p, w, c) = (proj_name(i), ws_name(i), cred_name(i));
        format!(
            "Bearer {}",
            jsonwebtoken::encode(
                &header,
                &C {
                    iss: "https://auth.prisma.io",
                    aud: "prisma-streams-data",
                    sub: "u",
                    credential_id: &c,
                    project_id: &p,
                    workspace_id: &w,
                    cell_id: "test-cell",
                    ownership_version: 1,
                    grant_version: 1,
                    scope: scopes,
                    jti: "t",
                    iat: now - 60,
                    exp: now + 600,
                },
                &enc,
            )
            .unwrap()
        )
    };
    let tokens: Vec<String> = (0..project_count).map(mint).collect();
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let create = br#"{"format":{"kind":"json"}}"#;

    // Every project creates the SAME name — 128 coexisting "cert"
    // streams, one per project.
    for t in tokens.iter() {
        let auth = ("authorization", t.as_str());
        let (st, _, _) = preq(addr, "PUT", "/v1/streams/cert", &[ekey, auth], create).await;
        assert_eq!(st, 201);
    }
    // Noisy subset appends DISTINCT volumes (project i: i%3+1 records
    // carrying its own marker); the other 112 projects stay idle.
    for (i, t) in tokens.iter().enumerate().take(noisy_count) {
        let auth = ("authorization", t.as_str());
        for k in 0..(i % 3 + 1) {
            let body = format!("{{\"p\":{i},\"k\":{k}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cert/records",
                &[ekey, auth],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200, "append p{i} k{k}");
        }
    }

    // ISOLATION: a token reads ITS project's "cert" — never a byte of
    // a neighbor's. Probe noisy-vs-noisy, noisy-vs-idle, idle-vs-noisy.
    for (reader, must_see, never_see) in [
        (3usize, Some(3usize), 5usize),
        (120, None, 0),
        (15, Some(15), 14),
    ] {
        let auth = ("authorization", tokens[reader].as_str());
        let (st, _, b) = preq(addr, "GET", "/v1/streams/cert/records", &[ekey, auth], b"").await;
        assert_eq!(st, 200);
        let text = String::from_utf8_lossy(&b);
        if let Some(m) = must_see {
            assert!(
                text.contains(&format!("\"p\":{m}")),
                "project {reader} must read its own data: {text}"
            );
        }
        assert!(
            !text.contains(&format!("\"p\":{never_see},")),
            "project {reader} leaked project {never_see}'s data: {text}"
        );
    }
    // Per-project catalog: exactly ONE stream, the project's own.
    let auth3 = ("authorization", tokens[3].as_str());
    let (st, _, b) = preq(addr, "GET", "/v1/streams", &[auth3], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let names: Vec<&str> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["cert"], "catalog is project-scoped: {names:?}");

    // SUSPENSION touches only its target: proj-c000 suspended at feed
    // 2; its requests refuse 403, its neighbor is untouched.
    let mut suspended = base_projects.clone();
    let p0 = crate::tenant::ProjectId::new(&proj_name(0)).unwrap();
    if let Some(p) = suspended.get_mut(&p0) {
        p.status = crate::project_policy::ProjectStatus::Suspended;
        p.project_policy_version = 2;
    }
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: suspended,
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .unwrap();
    let auth0 = ("authorization", tokens[0].as_str());
    let (st, _, _) = preq(addr, "GET", "/v1/streams/cert/records", &[ekey, auth0], b"").await;
    assert_eq!(st, 403, "suspended project refused");
    let auth1 = ("authorization", tokens[1].as_str());
    let (st, _, _) = preq(addr, "GET", "/v1/streams/cert/records", &[ekey, auth1], b"").await;
    assert_eq!(st, 200, "neighbor unaffected by suspension");

    // REVOCATION touches only its credential: c-c001 revoked at grant
    // feed 2 with a STRICTLY newer grant_version (the monotonic rule).
    let mut revoked = base_credentials.clone();
    if let Some(c) = revoked.get_mut(cred_name(1).as_str()) {
        c.status = crate::project_policy::CredentialStatus::Revoked;
        c.grant_version = 2;
    }
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: revoked,
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .unwrap();
    let (st, _, _) = preq(addr, "GET", "/v1/streams/cert/records", &[ekey, auth1], b"").await;
    assert!(
        st == 401 || st == 403,
        "revoked credential refused, got {st}"
    );
    let auth2 = ("authorization", tokens[2].as_str());
    let (st, _, _) = preq(addr, "GET", "/v1/streams/cert/records", &[ekey, auth2], b"").await;
    assert_eq!(st, 200, "neighbor unaffected by revocation");

    // BOOKS: drain, roll up, reconcile — per-(account, project) totals
    // agree and every noisy project's row exists.
    state.billing.reads().seal_if_aged(0);
    for _ in 0..100 {
        if crate::billing::drain_once(&state).await.expect("drain") == 0 {
            break;
        }
    }
    for _ in 0..100 {
        if crate::billing::rollup_step(&state).await.expect("rollup") == 0 {
            break;
        }
    }
    let (y, m) = crate::billing::utc_year_month(crate::billing::billing_now_ms());
    let month = crate::billing::month_str(y, m);
    let rollup = state.rollup.get().unwrap();
    let rep = rollup.reconcile_month(&month).await.expect("reconcile");
    assert!(rep.ok, "books must balance: {:?}", rep.mismatches);
    assert!(
        rep.projects >= noisy_count,
        "every noisy project rolled up: {rep:?}"
    );

    // DENIALS journaled: the suspension + revocation probes above.
    let mut appended = 0usize;
    for _ in 0..40 {
        let n = crate::audit::drain_audit_once(&state).await.expect("drain");
        if n == 0 {
            break;
        }
        appended += n;
    }
    assert!(appended >= 2, "denials journaled: {appended}");
    engine_shutdown(&state).await;
}
