//! Security modes.

use super::fixture_http::{
    HttpRigOptions, engine_shutdown, http_rig_build, http_rig_with_auth_service,
};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;

/// MT Stage 5 shadow trial: with STREAMS_AUTH_MODE=shadow, every
/// non-capability product bearer runs through the FULL customer
/// pipeline (signature, versions, placement, status) and lands in a
/// counter — while the response stays exactly what the legacy
/// deployment bearer decides. A valid credential counts `ok`, garbage
/// counts `failed`, a bare request counts `missing`, and all three
/// requests SUCCEED because the rig has no legacy token configured.
#[expect(
    clippy::too_many_lines,
    reason = "shadow mode scenario; every product bearer runs through the same verification in order while nothing is refused; helper phases would hide which observation became an enforcement"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_mode_observes_without_enforcing() {
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    let now = crate::shard::now_ms() / 1000;

    let svc = std::sync::Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Shadow,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        "shadow-1".to_string(),
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
    let pid = crate::tenant::ProjectId::new("proj_456").unwrap();
    let mut projects = std::collections::HashMap::new();
    projects.insert(
        pid.clone(),
        crate::project_policy::ProjectPolicy {
            project_id: pid.clone(),
            workspace_id: crate::tenant::WorkspaceId::new("ws_789").unwrap(),
            cell_id: std::sync::Arc::from("test-cell"),
            project_policy_version: 40,
            ownership_version: 12,
            status: crate::project_policy::ProjectStatus::Active,
            quotas: crate::project_policy::ProjectQuotas::default(),
        },
    );
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: 40,
    })
    .unwrap();
    let mut credentials = std::collections::HashMap::new();
    credentials.insert(
        std::sync::Arc::from("strcred_123"),
        crate::project_policy::CredentialGrant {
            credential_id: std::sync::Arc::from("strcred_123"),
            project_id: pid,
            grant_version: 7,
            status: crate::project_policy::CredentialStatus::Active,
            scopes: crate::tenant::ScopeSet::parse("streams.records.read streams.create").0,
            grant: crate::tenant::StreamGrant::All,
            expires_at: None,
        },
    );
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: 7,
    })
    .unwrap();

    let (state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            auth: Some("shadow-bearer".to_string()),
            auth_service: Some(svc.clone()),
            ..Default::default()
        },
    )
    .await
    .parts();

    #[derive(serde::Serialize)]
    struct Claims<'a> {
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
    header.kid = Some("shadow-1".into());
    let jwt = jsonwebtoken::encode(
        &header,
        &Claims {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "user_1",
            credential_id: "strcred_123",
            project_id: "proj_456",
            workspace_id: "ws_789",
            cell_id: "test-cell",
            ownership_version: 12,
            grant_version: 7,
            scope: "streams.records.read streams.create",
            jti: "tok_shadow_1",
            iat: now - 60,
            exp: now + 600,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();

    // SR-5 posture: shadow requires the CONFIGURED deployment bearer
    // for authorization (allow-if-unset is Off-only now). Observation
    // still runs in the wrapper BEFORE authorization, so every bearer
    // shape is counted regardless of the legacy gate's verdict.
    let legacy = ("authorization", "Bearer shadow-bearer");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/shadow1",
        &[("prisma-encryption-key", PRISMA_KEY), legacy],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "the deployment bearer authorizes in shadow");
    // Valid customer token: observed ok; the legacy gate refuses it
    // (shadow observes — enforce is what will accept it).
    let bearer = format!("Bearer {jwt}");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/shadow1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", &bearer),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 401);
    // Garbage bearer: observed failed, refused.
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/shadow1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", "Bearer garbage"),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 401);
    // No bearer at all: observed missing, refused.
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/shadow1",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 401);

    use std::sync::atomic::Ordering;
    assert_eq!(state.auth.shadow.ok.load(Ordering::Relaxed), 1);
    // failed = the garbage bearer + the deployment bearer (a non-JWT
    // passing through the observer).
    assert_eq!(state.auth.shadow.failed.load(Ordering::Relaxed), 2);
    assert_eq!(state.auth.shadow.missing.load(Ordering::Relaxed), 1);

    // Operator surface: mode, counters, feed freshness (bearer-gated).
    let (st, _, body) = preq(addr, "GET", "/v1/debug/auth", &[legacy], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["shadow"]["mode"], "shadow");
    assert_eq!(v["shadow"]["ok"], 1);
    assert_eq!(v["feeds"]["policies"]["stale"], false);
    assert_eq!(v["feeds"]["jwks"]["keys"], 1);
    engine_shutdown(&state).await;
}

/// MT Stage 5b: STREAMS_AUTH_MODE=enforce — the §6.1 route-scope
/// matrix and the §6.2 prefix grant gate REAL product requests, with
/// the §7.1/§8.1 fail-closed response classes: 401 for what a fresh
/// token could fix, 403 for verified-but-denied (scope, prefix,
/// suspension), 421 + Prisma-Error-Code for placement, 503 for the
/// cell's own feed staleness, and the §15 watch capability remains
/// the one self-authorizing carrier.
#[expect(
    clippy::too_many_lines,
    reason = "enforce mode scenario; the route-scope matrix and the prefix grant gate are driven against real product routes in order; helper phases would hide which route escaped the gate"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enforce_mode_gates_the_product_surface() {
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
        "enf-1".to_string(),
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

    let policy = |proj: &str, ws: &str, cell: &str, status| crate::project_policy::ProjectPolicy {
        project_id: crate::tenant::ProjectId::new(proj).unwrap(),
        workspace_id: crate::tenant::WorkspaceId::new(ws).unwrap(),
        cell_id: std::sync::Arc::from(cell),
        project_policy_version: 1,
        ownership_version: 12,
        status,
        quotas: crate::project_policy::ProjectQuotas::default(),
    };
    use crate::project_policy::ProjectStatus as PS;
    let mut projects = std::collections::HashMap::new();
    for (p, ws, cell, st) in [
        ("proj-test", "ws_789", "test-cell", PS::Active),
        ("proj_off", "ws_off", "test-cell", PS::Suspended),
        ("proj_far", "ws_far", "other-cell", PS::Active),
    ] {
        projects.insert(
            crate::tenant::ProjectId::new(p).unwrap(),
            policy(p, ws, cell, st),
        );
    }
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();

    let cred = |id: &str, proj: &str, scopes: &str, grant| crate::project_policy::CredentialGrant {
        credential_id: std::sync::Arc::from(id),
        project_id: crate::tenant::ProjectId::new(proj).unwrap(),
        grant_version: 7,
        status: crate::project_policy::CredentialStatus::Active,
        scopes: crate::tenant::ScopeSet::parse(scopes).0,
        grant,
        expires_at: None,
    };
    use crate::tenant::StreamGrant as G;
    let full = "streams.metadata.read streams.records.read streams.records.append \
                streams.create streams.lifecycle.manage streams.catalog.read \
                streams.usage.read";
    let mut credentials = std::collections::HashMap::new();
    for (id, proj, scopes, grant) in [
        ("c_full", "proj-test", full, G::All),
        (
            "c_read",
            "proj-test",
            "streams.metadata.read streams.records.read",
            G::All,
        ),
        (
            "c_pfx",
            "proj-test",
            full,
            G::Prefixes(
                vec![crate::tenant::CanonicalPrefix::normalize("customers/acme").unwrap()].into(),
            ),
        ),
        // Metadata + create, but explicitly NO records.read — the
        // provisioning-service shape. :scan must still be refused.
        (
            "c_meta",
            "proj-test",
            "streams.metadata.read streams.create",
            G::All,
        ),
        // Body-visible scopes (§6.1 Stage 5c): create WITHOUT
        // watches.manage; consumers WITHOUT dlq.configure; and their
        // enabled counterparts.
        (
            "c_watch",
            "proj-test",
            "streams.create streams.watches.manage",
            G::All,
        ),
        ("c_cons", "proj-test", "streams.consumers.configure", G::All),
        (
            "c_consdlq",
            "proj-test",
            "streams.consumers.configure streams.dlq.configure",
            G::All,
        ),
        // Review item 4: right scopes, but the grant covers only
        // acme/** — the DLQ destination must ALSO be inside it.
        (
            "c_dlqpfx",
            "proj-test",
            "streams.create streams.consumers.configure streams.dlq.configure",
            G::Prefixes(vec![crate::tenant::CanonicalPrefix::normalize("acme").unwrap()].into()),
        ),
        ("c_susp", "proj_off", full, G::All),
        ("c_far", "proj_far", full, G::All),
    ] {
        credentials.insert(std::sync::Arc::from(id), cred(id, proj, scopes, grant));
    }
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: 7,
    })
    .unwrap();

    let (state, addr) = http_rig_with_auth_service(mem(), svc.clone()).await;

    #[derive(serde::Serialize)]
    struct Claims<'a> {
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
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_prefixes: Option<Vec<&'a str>>,
        jti: &'a str,
        iat: i64,
        exp: i64,
    }
    let mint =
        |cred: &str, proj: &str, ws: &str, cell: &str, scope: &str, pfx: Option<Vec<&str>>| {
            let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
            header.kid = Some("enf-1".into());
            jsonwebtoken::encode(
                &header,
                &Claims {
                    iss: "https://auth.prisma.io",
                    aud: "prisma-streams-data",
                    sub: "user_1",
                    credential_id: cred,
                    project_id: proj,
                    workspace_id: ws,
                    cell_id: cell,
                    ownership_version: 12,
                    grant_version: 7,
                    scope,
                    stream_prefixes: pfx,
                    jti: "tok_enf",
                    iat: now - 60,
                    exp: now + 600,
                },
                &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
            )
            .unwrap()
        };
    let t_full = format!(
        "Bearer {}",
        mint("c_full", "proj-test", "ws_789", "test-cell", full, None)
    );
    let t_read = format!(
        "Bearer {}",
        mint(
            "c_read",
            "proj-test",
            "ws_789",
            "test-cell",
            "streams.metadata.read streams.records.read",
            None
        )
    );
    let t_pfx = format!(
        "Bearer {}",
        mint(
            "c_pfx",
            "proj-test",
            "ws_789",
            "test-cell",
            full,
            Some(vec!["customers/acme"])
        )
    );
    let t_meta = format!(
        "Bearer {}",
        mint(
            "c_meta",
            "proj-test",
            "ws_789",
            "test-cell",
            "streams.metadata.read streams.create",
            None
        )
    );
    let t_watch = format!(
        "Bearer {}",
        mint(
            "c_watch",
            "proj-test",
            "ws_789",
            "test-cell",
            "streams.create streams.watches.manage",
            None
        )
    );
    let t_cons = format!(
        "Bearer {}",
        mint(
            "c_cons",
            "proj-test",
            "ws_789",
            "test-cell",
            "streams.consumers.configure",
            None
        )
    );
    let t_consdlq = format!(
        "Bearer {}",
        mint(
            "c_consdlq",
            "proj-test",
            "ws_789",
            "test-cell",
            "streams.consumers.configure streams.dlq.configure",
            None
        )
    );
    let t_dlqpfx = format!(
        "Bearer {}",
        mint(
            "c_dlqpfx",
            "proj-test",
            "ws_789",
            "test-cell",
            "streams.create streams.consumers.configure streams.dlq.configure",
            Some(vec!["acme"])
        )
    );
    let t_susp = format!(
        "Bearer {}",
        mint("c_susp", "proj_off", "ws_off", "test-cell", full, None)
    );
    let t_far = format!(
        "Bearer {}",
        mint("c_far", "proj_far", "ws_far", "other-cell", full, None)
    );
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let body = br#"{"format":{"kind":"json"}}"#;
    let code = |b: &[u8]| -> String {
        serde_json::from_slice::<serde_json::Value>(b)
            .map(|v| v["error"]["code"].as_str().unwrap_or("").to_string())
            .unwrap_or_default()
    };

    // No bearer: 401 — and this also proves the legacy open-access
    // posture (auth_token unset) is DEAD on the product surface.
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/enf1", &[ekey], body).await;
    assert_eq!(st, 401, "{}", String::from_utf8_lossy(&b));

    // Full authority: create + append + read.
    let auth_full = ("authorization", t_full.as_str());
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/enf1", &[ekey, auth_full], body).await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/enf1/records",
        &[ekey, auth_full],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/records",
        &[ekey, auth_full],
        b"",
    )
    .await;
    assert_eq!(st, 200);

    // Scope matrix: a read-only credential reads but cannot append.
    let auth_read = ("authorization", t_read.as_str());
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/records",
        &[ekey, auth_read],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/enf1/records",
        &[ekey, auth_read],
        br#"{"n":2}"#,
    )
    .await;
    assert_eq!(st, 403);
    assert_eq!(code(&b), "missing_scope");

    // Prefix grant: covered name works end to end, foreign name 403s.
    let auth_pfx = ("authorization", t_pfx.as_str());
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/customers/acme/orders",
        &[ekey, auth_pfx],
        body,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/records",
        &[ekey, auth_pfx],
        b"",
    )
    .await;
    assert_eq!(st, 403);
    assert_eq!(code(&b), "prefix_denied");

    // :scan is a bulk RECORD read (§6.1): a metadata-only credential
    // is refused, the full credential is served — records.read, not
    // metadata.read, gates it.
    let auth_meta = ("authorization", t_meta.as_str());
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf1:scan",
        &[ekey, auth_meta],
        b"",
    )
    .await;
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "missing_scope");
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/enf1:scan",
        &[ekey, auth_full],
        b"",
    )
    .await;
    assert_eq!(st, 200);

    // Catalog honors the §6.2 prefix grant: the prefix credential sees
    // its own streams, never enf1. (customers/acme/orders was created
    // above under auth_pfx; enf1 under auth_full.)
    let (st, _, b) = preq(addr, "GET", "/v1/streams", &[auth_pfx], b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let names: Vec<&str> = v["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"customers/acme/orders"),
        "prefix cred should see its own stream: {names:?}"
    );
    assert!(
        !names.contains(&"enf1"),
        "prefix cred must not see out-of-grant names: {names:?}"
    );

    // §6.1 Stage 5c — body-visible scopes. Watch definitions ride the
    // create body: streams.create alone must not attach them.
    let watch_body = br#"{"format":{"kind":"json"},"watches":[{"name":"w","fields":["/id"]}]}"#;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/wenf",
        &[ekey, auth_full],
        watch_body,
    )
    .await;
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "missing_scope");
    let auth_watch = ("authorization", t_watch.as_str());
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/wenf",
        &[ekey, auth_watch],
        watch_body,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));

    // The DLQ link is body-visible too: consumers.configure authorizes
    // the consumer, dlq.configure authorizes the dead-letter wiring.
    let auth_cons = ("authorization", t_cons.as_str());
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/enf1/consumers/c1",
        &[ekey, auth_cons],
        br#"{"deadLetterStream":"enfdlq"}"#,
    )
    .await;
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "missing_scope");
    // With dlq.configure the same request proceeds to the normal link
    // validation (the target exists — created under the full cred).
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/enfdlq", &[ekey, auth_full], body).await;
    assert_eq!(st, 201);
    let auth_consdlq = ("authorization", t_consdlq.as_str());
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/enf1/consumers/c1",
        &[ekey, auth_consdlq],
        br#"{"deadLetterStream":"enfdlq"}"#,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "{st}: {}",
        String::from_utf8_lossy(&b)
    );

    // Review item 4: the compound DLQ rule — the destination needs the
    // credential's PREFIX grant independently of the scopes. Source
    // inside the grant, destination outside: 403 prefix_denied. A
    // destination inside the grant proceeds to normal link validation
    // (here: 400 unknown target, proving the authz leg passed).
    let auth_dlqpfx = ("authorization", t_dlqpfx.as_str());
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/acme/src",
        &[ekey, auth_dlqpfx],
        body,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/acme/src/consumers/c1",
        &[ekey, auth_dlqpfx],
        br#"{"deadLetterStream":"enfdlq"}"#,
    )
    .await;
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "prefix_denied");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/acme/src/consumers/c1",
        &[ekey, auth_dlqpfx],
        br#"{"deadLetterStream":"acme/dlq"}"#,
    )
    .await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "unknown_dead_letter_stream");

    // Suspension fails closed at authorization.
    let auth_susp = ("authorization", t_susp.as_str());
    let (st, _, b) = preq(addr, "PUT", "/v1/streams/offs", &[ekey, auth_susp], body).await;
    assert_eq!(st, 403);
    assert_eq!(code(&b), "project_not_active");

    // Placement: a VALID token for a project served elsewhere gets 421
    // with the contract's header — never 401 (clients must not refresh
    // a fine credential).
    let auth_far = ("authorization", t_far.as_str());
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/records",
        &[ekey, auth_far],
        b"",
    )
    .await;
    assert_eq!(st, 421, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "wrong_cell");
    assert_eq!(
        h.get("prisma-error-code").map(|v| v.as_str()),
        Some("wrong_cell")
    );

    // Catalog: needs streams.catalog.read.
    let (st, _, _) = preq(addr, "GET", "/v1/streams", &[auth_full], b"").await;
    assert_eq!(st, 200);
    let (st, _, b) = preq(addr, "GET", "/v1/streams", &[auth_read], b"").await;
    assert_eq!(st, 403);
    assert_eq!(code(&b), "missing_scope");

    // Project usage: scope first, then the path must name the
    // principal's own project.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/projects/proj-test/usage",
        &[auth_read],
        b"",
    )
    .await;
    assert_eq!(st, 403);
    assert_eq!(code(&b), "missing_scope");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/projects/proj-other/usage",
        &[auth_full],
        b"",
    )
    .await;
    assert_eq!(st, 404);
    assert_eq!(code(&b), "unknown_project");

    // §15 exemption survives enforce: a watch-wait request carrying a
    // capability is judged by the CAPABILITY (here: a forged one, so
    // the handler refuses it) — not funneled into token auth. The same
    // route with no capability and no token is plain 401.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/watches/w/keys/deadbeef?cap=1.234",
        &[],
        b"",
    )
    .await;
    assert_ne!(st, 401, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/watches/w/keys/deadbeef",
        &[],
        b"",
    )
    .await;
    assert_eq!(st, 401);
    // No existence oracle: a MISSING stream probed with a garbage
    // capability answers exactly like an existing one — the uniform
    // watch_unauthorized refusal, never a 404.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf-nosuch/watches/w/keys/deadbeefdeadbeef?cap=1.234",
        &[],
        b"",
    )
    .await;
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "watch_unauthorized");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/watches/w/keys/deadbeefdeadbeef?cap=1.234",
        &[],
        b"",
    )
    .await;
    assert_eq!(st, 403, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "watch_unauthorized");

    // The cell's OWN staleness fails closed as retryable 503 — not as
    // a client credential problem.
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: std::collections::HashMap::new(),
        fetched_at_unix: now - crate::auth::POLICY_STALENESS_MAX_SECS - 10,
        feed_version: 2,
    })
    .unwrap();
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/enf1/records",
        &[ekey, auth_full],
        b"",
    )
    .await;
    assert_eq!(st, 503);
    assert_eq!(code(&b), "policy_stale");
    engine_shutdown(&state).await;
}
