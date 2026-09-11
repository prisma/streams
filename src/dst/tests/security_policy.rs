//! Security policy.

use super::fixture_auth::{
    auth_rig, mint_token, rig_create, rig_policy, rig_publish_policy, sr_rig, sr2_workload_jwt,
};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// SR-4 (Søren review): resurrection safety survives snapshot
/// OMISSION. The publisher contract forbids removed-then-lower
/// reintroduction, but the cell's high-water table refuses it even
/// when the publisher misbehaves: remove, replay, reintroduce lower,
/// and reactivate-at-dead-version are all refused; a full identical
/// replay is idempotent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_high_water_survives_removal_and_replay() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    let now = crate::shard::now_ms() / 1000;
    let mk_policy = |ver: u64, over: u64, pver: u64| {
        let pid = crate::tenant::ProjectId::new("proj-hw").unwrap();
        let mut projects = std::collections::HashMap::new();
        projects.insert(
            pid.clone(),
            crate::project_policy::ProjectPolicy {
                project_id: pid,
                workspace_id: crate::tenant::WorkspaceId::new("ws_hw").unwrap(),
                cell_id: std::sync::Arc::from("test-cell"),
                project_policy_version: pver,
                ownership_version: over,
                status: crate::project_policy::ProjectStatus::Active,
                quotas: crate::project_policy::ProjectQuotas::default(),
            },
        );
        crate::project_policy::PolicySnapshot {
            projects,
            fetched_at_unix: now,
            feed_version: ver,
        }
    };
    let empty_policy = |ver: u64| crate::project_policy::PolicySnapshot {
        projects: std::collections::HashMap::new(),
        fetched_at_unix: now,
        feed_version: ver,
    };
    // Publish at ownership 12 / policy 7, remove, then try to
    // reintroduce LOWER — refused even though the loaded snapshot no
    // longer contains the project.
    svc.publish_policies(mk_policy(1, 12, 7)).unwrap();
    svc.publish_policies(empty_policy(2)).unwrap();
    assert!(
        svc.publish_policies(mk_policy(3, 11, 7)).is_err(),
        "ownership below high-water must be refused after omission"
    );
    assert!(
        svc.publish_policies(mk_policy(3, 12, 6)).is_err(),
        "policy version below high-water must be refused after omission"
    );
    // SR2 finding 2 SUPERSEDES the original equal-version allowance:
    // an OMITTED project cannot return at its unchanged version pair —
    // omission is a tombstone and reintroduction is an explicit act
    // under a strictly newer version.
    assert!(
        svc.publish_policies(mk_policy(3, 12, 7)).is_err(),
        "equal-version reintroduction after omission must be refused"
    );
    svc.publish_policies(mk_policy(3, 12, 8)).unwrap();

    let mk_grants = |ver: u64, gver: u64, status: crate::project_policy::CredentialStatus| {
        let mut credentials = std::collections::HashMap::new();
        credentials.insert(
            std::sync::Arc::from("c_hw"),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from("c_hw"),
                project_id: crate::tenant::ProjectId::new("proj-hw").unwrap(),
                grant_version: gver,
                status,
                scopes: crate::tenant::ScopeSet::parse("streams.records.read").0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
        crate::project_policy::GrantSnapshot {
            credentials,
            fetched_at_unix: now,
            feed_version: ver,
        }
    };
    let empty_grants = |ver: u64| crate::project_policy::GrantSnapshot {
        credentials: std::collections::HashMap::new(),
        fetched_at_unix: now,
        feed_version: ver,
    };
    use crate::project_policy::CredentialStatus as CS;
    // Revoke at version 10, REMOVE from the feed, then reintroduce
    // Active at 10 (same) and at 3 (lower): both refused; 11 accepted.
    svc.publish_grants(mk_grants(1, 10, CS::Revoked)).unwrap();
    svc.publish_grants(empty_grants(2)).unwrap();
    assert!(
        svc.publish_grants(mk_grants(3, 3, CS::Active)).is_err(),
        "grant_version below high-water must be refused after omission"
    );
    assert!(
        svc.publish_grants(mk_grants(3, 10, CS::Active)).is_err(),
        "reactivation at the dead version must be refused after omission"
    );
    svc.publish_grants(mk_grants(3, 11, CS::Active)).unwrap();
    // Idempotent replay of the exact same snapshot is accepted.
    svc.publish_grants(mk_grants(3, 11, CS::Active)).unwrap();
}

/// SR-4: the unknown-kid nudge is rate-limited — the first sighting
/// wakes the refresher, storms within the window do not.
#[test]
fn unknown_kid_nudge_is_rate_limited() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    assert!(svc.request_kid_refresh(), "first sighting nudges");
    assert!(
        !svc.request_kid_refresh(),
        "a storm within the window must not re-nudge"
    );
}

/// SR-5 (Søren decision): the raw Durable Streams surface is
/// INTERNAL-ONLY under enforce — no deployment-global customer bearer
/// exists on a shared cell. The deployment posture is refused; the
/// fleet credential and a short-lived workload JWT (§14.1, aud
/// prisma-streams-internal) are the only raw principals. The operator
/// surface requires a bearer in every non-Off mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_and_operator_surfaces_are_internal_under_enforce() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read";
    let (state, addr, _tok) = sr_rig("proj-rawb", "ws_rawb", "c_rawb", "raw-1", scopes).await;
    let ct = [("content-type", "application/json")];

    // Unauthenticated raw create: refused (the rig has no AUTH_TOKEN,
    // and allow-if-unset is Off-mode-only now).
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawx", &ct, b"[]").await;
    assert_eq!(st, 401, "raw surface must refuse the deployment posture");
    // Unauthenticated raw segments listing: refused.
    let (st, _, _) = hreq(addr, "GET", "/v1/segments/rawx", &[], b"").await;
    assert_eq!(st, 401, "segments listing must refuse too");
    // Operator surface: refused without a bearer.
    let (st, _, _) = hreq(addr, "GET", "/operator/data.json", &[], b"").await;
    assert_eq!(st, 401, "operator surface must require a bearer");

    // The FLEET credential authorizes the raw surface (internal use).
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawx", &[ct[0], fleet], b"[]").await;
    assert!(st == 200 || st == 201, "fleet credential on raw: {st}");

    // A short-lived WORKLOAD JWT authorizes it too (§14.1 — the
    // static token's replacement path).
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
    let now = crate::shard::now_ms() / 1000;
    #[derive(serde::Serialize)]
    struct W<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        cell_id: &'a str,
        exp: i64,
    }
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("raw-1".into());
    let wl = jsonwebtoken::encode(
        &header,
        &W {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-internal",
            sub: "slot-7",
            cell_id: "test-cell",
            exp: now + 120,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
    )
    .unwrap();
    let wl_hdr = format!("Bearer {wl}");
    let wla = ("authorization", wl_hdr.as_str());
    // SR2 finding 1: this token carries NO operations claim — under
    // §14.1 least privilege it opens NOTHING (it used to be a
    // cell-wide credential; that behavior is retired).
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawx",
        &[ct[0], wla],
        br#"[{"i":1}]"#,
    )
    .await;
    assert_eq!(
        st, 401,
        "operation-less workload JWT must open nothing: {st}"
    );
    // The SAME identity WITH the raw-append operation performs it.
    let now2 = crate::shard::now_ms() / 1000;
    let wl_op = format!(
        "Bearer {}",
        sr2_workload_jwt("raw-1", &["raw-append"], now2)
    );
    let wopa = ("authorization", wl_op.as_str());
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawx",
        &[ct[0], wopa],
        br#"[{"i":1}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "raw-append workload JWT on raw: {st}"
    );
    // A CUSTOMER-audience JWT must NOT open the raw surface.
    let cust = format!("Bearer {_tok}");
    let _ = cust;
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawx",
        &[ct[0], ("authorization", _tok.as_str())],
        br#"[{"i":2}]"#,
    )
    .await;
    assert_eq!(
        st, 401,
        "a customer token must not authorize the raw surface"
    );
    engine_shutdown(&state).await;
}

/// MT Stage 4 same-project rule: stored references (fork parentage,
/// dead-letter targets) bind inside the REFERRING stream's project. A
/// foreign project's stream with the SAME name — even one carrying the
/// same fork id, conditioned so an unscoped release would tombstone
/// it — is invisible: each release touches exactly its own project's
/// descriptor, and a DLQ link cannot be configured against a target
/// that exists only in another project.
#[expect(
    clippy::too_many_lines,
    reason = "stored reference scenario; fork parentage and dead-letter targets are bound inside the referring project across the same requests; helper phases would hide which reference crossed the project"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stored_references_bind_inside_the_referring_project() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let po = crate::tenant::ProjectId::new("proj-other").unwrap();
    let foreign = |name: &str| {
        crate::tenant::TenantStreamRef::new(
            po.clone(),
            crate::tenant::CanonicalStreamName::new(name).unwrap(),
        )
    };
    let foreign_desc = |name: &str, epoch: &str| crate::registry::PersistedDescriptor {
        seal_gen_counter: 0,
        account_id: None,
        project_id: po.clone(),
        name: name.to_string(),
        stream_epoch: epoch.to_string(),
        key_fingerprint: "fp".into(),
        created_ms: 1,
        expires_at_ms: None,
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        content_type: "application/json".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    };

    // ---- Fork-release leg ----
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk4d-src", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("frk4d-src"), |d| {
            d.fork_children = vec!["child-ref".into()];
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk4d-src"));
    let ea = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk4d-src"))
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();
    // The look-alike: same name, same fork id, soft-deleted — an
    // unscoped release would remove its last child and TOMBSTONE it.
    let eb = format!("{:032x}", 0xb4d_u64);
    let mut lk = foreign_desc("frk4d-src", &eb);
    lk.fork_children = vec!["child-ref".into()];
    lk.soft_deleted = true;
    state.registry.create(lk).await.unwrap();

    // Release against the FOREIGN identity: only the foreign
    // descriptor changes (tombstoned, by its own lifecycle).
    let ok = crate::http::release_fork_ref_for_test(&state, foreign("frk4d-src"), "child-ref", &eb)
        .await
        .unwrap();
    assert!(ok);
    let fd = state
        .registry
        .get(&foreign("frk4d-src"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        fd.deleted,
        "the foreign release tombstones the foreign desc"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk4d-src"));
    let td = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk4d-src"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        td.fork_children,
        vec!["child-ref".to_string()],
        "a foreign-project release touched this project's source"
    );
    assert!(!td.deleted && !td.soft_deleted);
    assert_eq!(td.stream_epoch, ea);

    // Release against THIS project's identity: reference removed here,
    // stream stays alive (it was never soft-deleted).
    let ok = crate::http::release_fork_ref_for_test(
        &state,
        state.deployment.raw_adapter_sref("frk4d-src"),
        "child-ref",
        &ea,
    )
    .await
    .unwrap();
    assert!(ok);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk4d-src"));
    let td = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk4d-src"))
        .await
        .unwrap()
        .unwrap();
    assert!(td.fork_children.is_empty() && !td.deleted);

    // ---- DLQ-binding leg ----
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dlq4d-main",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // The target exists ONLY in the foreign project.
    state
        .registry
        .create(foreign_desc("dlq4d-tgt", &format!("{:032x}", 0xd1_u64)))
        .await
        .unwrap();
    let (st, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/dlq4d-main/consumers/c1",
        &key,
        br#"{"deadLetterStream":"dlq4d-tgt"}"#,
    )
    .await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&body));
    let e: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        e["error"]["code"], "unknown_dead_letter_stream",
        "a foreign project's same-named stream must be invisible to the DLQ link"
    );
    // Creating the target in THIS project (same key) makes the exact
    // same config valid.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dlq4d-tgt",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/dlq4d-main/consumers/c1",
        &key,
        br#"{"deadLetterStream":"dlq4d-tgt"}"#,
    )
    .await;
    assert!(
        st == 200 || st == 201,
        "{st}: {}",
        String::from_utf8_lossy(&body)
    );
    engine_shutdown(&state).await;
}

/// RED (review finding 2): omission from a full snapshot must
/// tombstone the entry at its last observed version — reintroducing it
/// at the SAME version must refuse. Today the high-water loops only
/// inspect IDs PRESENT in the incoming snapshot, so omitted-then-
/// replayed entries walk straight back in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_credential_omitted_then_same_version_active_is_refused() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    let now = crate::shard::now_ms() / 1000;
    let cred = |ver: u64| {
        let mut m = std::collections::HashMap::new();
        m.insert(
            std::sync::Arc::from("c-omit"),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from("c-omit"),
                project_id: crate::tenant::ProjectId::new("proj-omit").unwrap(),
                grant_version: ver,
                status: crate::project_policy::CredentialStatus::Active,
                scopes: crate::tenant::ScopeSet::parse("streams.records.read").0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
        m
    };
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: cred(7),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    // Full snapshot WITHOUT the credential: fails closed, and must
    // tombstone it at version 7.
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: std::collections::HashMap::new(),
        fetched_at_unix: now + 1,
        feed_version: 2,
    })
    .unwrap();
    // Same-version reintroduction: a publisher replaying stale state.
    let r = svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: cred(7),
        fetched_at_unix: now + 2,
        feed_version: 3,
    });
    assert!(
        r.is_err(),
        "omitted credential reintroduced at its old grant_version was accepted"
    );
    // A strictly newer version is the legitimate path back.
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: cred(8),
        fetched_at_unix: now + 3,
        feed_version: 4,
    })
    .expect("newer grant_version must reintroduce cleanly");
}

/// RED (review finding 2, project leg): same omission rule for the
/// policy snapshot — a project dropped from the feed cannot return at
/// an unchanged version pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_omitted_then_same_policy_version_is_refused() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    let now = crate::shard::now_ms() / 1000;
    let proj = |ppv: u64, status: crate::project_policy::ProjectStatus| {
        let pid = crate::tenant::ProjectId::new("proj-pomit").unwrap();
        let mut m = std::collections::HashMap::new();
        m.insert(
            pid.clone(),
            crate::project_policy::ProjectPolicy {
                project_id: pid,
                workspace_id: crate::tenant::WorkspaceId::new("ws_pomit").unwrap(),
                cell_id: std::sync::Arc::from("test-cell"),
                project_policy_version: ppv,
                ownership_version: 1,
                status,
                quotas: crate::project_policy::ProjectQuotas::default(),
            },
        );
        m
    };
    use crate::project_policy::ProjectStatus as PS;
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: proj(4, PS::Active),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: std::collections::HashMap::new(),
        fetched_at_unix: now + 1,
        feed_version: 2,
    })
    .unwrap();
    let r = svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: proj(4, PS::Active),
        fetched_at_unix: now + 2,
        feed_version: 3,
    });
    assert!(
        r.is_err(),
        "omitted project reintroduced at its old policy version was accepted"
    );
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: proj(5, PS::Active),
        fetched_at_unix: now + 3,
        feed_version: 4,
    })
    .expect("newer project_policy_version must reintroduce cleanly");
}

/// RED (review finding 2, semantic leg): an EQUAL version with
/// DIFFERENT content is a publisher defect — versions must pin bytes.
/// Identical replay at the same version stays accepted (at-least-once
/// feeds re-send).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_version_with_changed_content_is_refused() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    let now = crate::shard::now_ms() / 1000;
    let cred = |scopes: &str| {
        let mut m = std::collections::HashMap::new();
        m.insert(
            std::sync::Arc::from("c-sem"),
            crate::project_policy::CredentialGrant {
                credential_id: std::sync::Arc::from("c-sem"),
                project_id: crate::tenant::ProjectId::new("proj-sem").unwrap(),
                grant_version: 7,
                status: crate::project_policy::CredentialStatus::Active,
                scopes: crate::tenant::ScopeSet::parse(scopes).0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
        m
    };
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: cred("streams.records.read"),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    // Identical replay: accepted (at-least-once delivery).
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: cred("streams.records.read"),
        fetched_at_unix: now + 1,
        feed_version: 2,
    })
    .expect("identical same-version replay must stay accepted");
    // Same grant_version, WIDER scopes: refused.
    let r = svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials: cred("streams.records.read streams.records.append"),
        fetched_at_unix: now + 2,
        feed_version: 3,
    });
    assert!(
        r.is_err(),
        "same grant_version with changed scopes was accepted"
    );

    // Project leg: same policy version, Suspended -> Active.
    use crate::project_policy::ProjectStatus as PS;
    let proj = |status: PS| {
        let pid = crate::tenant::ProjectId::new("proj-sem2").unwrap();
        let mut m = std::collections::HashMap::new();
        m.insert(
            pid.clone(),
            crate::project_policy::ProjectPolicy {
                project_id: pid,
                workspace_id: crate::tenant::WorkspaceId::new("ws_sem2").unwrap(),
                cell_id: std::sync::Arc::from("test-cell"),
                project_policy_version: 4,
                ownership_version: 1,
                status,
                quotas: crate::project_policy::ProjectQuotas::default(),
            },
        );
        m
    };
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: proj(PS::Suspended),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let r = svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: proj(PS::Active),
        fetched_at_unix: now + 1,
        feed_version: 2,
    });
    assert!(
        r.is_err(),
        "same project_policy_version with changed status was accepted"
    );
}

#[test]
fn jwks_kid_lifecycle_rules() {
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    let key = |pem: &str| crate::auth::JwksKey {
        alg: jsonwebtoken::Algorithm::RS256,
        key: jsonwebtoken::DecodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
        fp: crate::auth::key_fp(pem.as_bytes()),
    };
    let snap = |ver: u64, kids: &[&str]| {
        let mut keys = std::collections::HashMap::new();
        for k in kids {
            keys.insert(k.to_string(), key(PUB));
        }
        crate::auth::JwksSnapshot {
            keys,
            fetched_at_unix: 1_000_000,
            feed_version: ver,
        }
    };
    // Overlap rotation: {A} -> {A,B} -> {B}: all accepted.
    svc.publish_jwks(snap(1, &["kid-a"])).unwrap();
    svc.publish_jwks(snap(2, &["kid-a", "kid-b"])).unwrap();
    svc.publish_jwks(snap(3, &["kid-b"])).unwrap();
    // Identical generation replay: accepted.
    svc.publish_jwks(snap(3, &["kid-b"])).unwrap();
    // Same generation, different key SET: refused.
    assert!(
        svc.publish_jwks(snap(3, &["kid-b", "kid-c"])).is_err(),
        "same generation with a different key set must refuse"
    );
    // Retired kid reintroduced at a HIGHER generation: refused.
    assert!(
        svc.publish_jwks(snap(4, &["kid-a", "kid-b"])).is_err(),
        "a retired kid must never return"
    );
    // Same kid, DIFFERENT key material: refused. (A second RSA key is
    // synthesized by fingerprint tampering at the type level being
    // impossible — so use the ed25519 test key's PEM as different
    // material under the same kid.)
    let mut keys = std::collections::HashMap::new();
    let mut k = key(PUB);
    k.fp = crate::auth::key_fp(b"different-material");
    keys.insert("kid-b".to_string(), k);
    let r = svc.publish_jwks(crate::auth::JwksSnapshot {
        keys,
        fetched_at_unix: 1_000_000,
        feed_version: 5,
    });
    assert!(r.is_err(), "kid rebound to different material must refuse");
    // Fresh kid at a new generation still works (rotation continues).
    svc.publish_jwks(snap(6, &["kid-b", "kid-d"])).unwrap();
}

/// Round-3 finding (feeds): an entry ADDED under an already-published
/// generation is refused by the generation digest — per-ID checks
/// alone cannot see it.
#[test]
fn same_feed_generation_cannot_gain_entries() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    let proj = |ids: &[&str]| {
        let mut m = std::collections::HashMap::new();
        for id in ids {
            let pid = crate::tenant::ProjectId::new(id).unwrap();
            m.insert(
                pid.clone(),
                crate::project_policy::ProjectPolicy {
                    project_id: pid,
                    workspace_id: crate::tenant::WorkspaceId::new("ws-g").unwrap(),
                    cell_id: std::sync::Arc::from("test-cell"),
                    project_policy_version: 1,
                    ownership_version: 1,
                    status: crate::project_policy::ProjectStatus::Active,
                    quotas: crate::project_policy::ProjectQuotas::default(),
                },
            );
        }
        crate::project_policy::PolicySnapshot {
            projects: m,
            fetched_at_unix: 1_000_000,
            feed_version: 9,
        }
    };
    svc.publish_policies(proj(&["proj-ga"])).unwrap();
    // Identical replay of generation 9: fine.
    svc.publish_policies(proj(&["proj-ga"])).unwrap();
    // Generation 9 grows a NEW project: refused.
    assert!(
        svc.publish_policies(proj(&["proj-ga", "proj-gb"])).is_err(),
        "an entry added under a published generation must refuse"
    );
}

/// RED (round-3 hardening): an unknown-kid nudge fired BETWEEN two
/// refresher iterations must not be lost. notify_one stores a permit
/// for the next waiter; notify_waiters does not — the nudge
/// evaporated and the rotated key waited out the polling interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kid_nudge_fired_before_the_waiter_is_not_lost() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    assert!(svc.request_kid_refresh(), "the nudge fires");
    // The refresher arrives LATE: the stored permit must wake it.
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        svc.kid_wakeup.notified(),
    )
    .await
    .expect("a pre-fired nudge must wake a late waiter (permit stored)");
}

// ------------------------------------------------------------------
// F3 (red): a workspace change WITHOUT an ownership_version increment
// must be refused by the snapshot publisher — the contract ties
// ownership changes to ownership_version structurally.
// ------------------------------------------------------------------
#[test]
fn publish_rejects_workspace_change_without_ownership_bump() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    rig_publish_policy(&svc, rig_policy("proj-wob", "ws-a", 1, 1), 1).unwrap();
    assert!(
        rig_publish_policy(&svc, rig_policy("proj-wob", "ws-b", 1, 2), 2).is_err(),
        "workspace changed without ownership_version increment must be refused"
    );
    // The refusal leaves no trace; the legitimate transfer shape lands.
    rig_publish_policy(&svc, rig_policy("proj-wob", "ws-b", 2, 3), 3)
        .expect("ownership bump + workspace change is the valid transfer");
    assert_eq!(
        svc.workspace_for(&crate::tenant::ProjectId::new("proj-wob").unwrap())
            .map(|w| w.as_str().to_string())
            .as_deref(),
        Some("ws-b")
    );
}

// ------------------------------------------------------------------
// Round-4 finding 3 (red): the direct-transition check compares a new
// policy only against a project still PRESENT in the current snapshot,
// so omit-and-reintroduce moved the workspace at an UNCHANGED
// ownership_version: no transfer event, no transfer-time credential
// revocation, no byte-time billing split — and an old durable project
// credential survived what was effectively an ownership change. The
// workspace bound to the ownership HIGH-WATER must survive omissions.
// ------------------------------------------------------------------
#[test]
fn publish_rejects_workspace_change_hidden_behind_omission() {
    let svc = crate::auth::AuthService::new(
        crate::auth::AuthMode::Enforce,
        "https://auth.prisma.io".into(),
        "test-cell",
    )
    .unwrap();
    // Snapshot 1: P under ws-a, ownership 1, policy 1.
    rig_publish_policy(&svc, rig_policy("proj-omi", "ws-a", 1, 1), 1).unwrap();
    // Snapshot 2: P OMITTED from the full snapshot (a removal).
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: std::collections::HashMap::new(),
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .expect("omission is a legal publication");
    // Snapshot 3: P reintroduced under ws-b at the SAME ownership
    // version with a higher policy version — satisfies the tombstone
    // rule (one component increased) and finds no P in the current
    // snapshot for the workspace check. It must STILL be refused.
    let err = rig_publish_policy(&svc, rig_policy("proj-omi", "ws-b", 1, 2), 3)
        .expect_err("omit-and-reintroduce must not smuggle an owner change");
    assert_eq!(err, "workspace changed without ownership_version increment");
    // The refusal leaves no trace: the honest shape (ownership bump)
    // lands, and the workspace travels with it.
    rig_publish_policy(&svc, rig_policy("proj-omi", "ws-b", 2, 3), 3)
        .expect("ownership bump across an omission is the valid transfer");
    assert_eq!(
        svc.workspace_for(&crate::tenant::ProjectId::new("proj-omi").unwrap())
            .map(|w| w.as_str().to_string())
            .as_deref(),
        Some("ws-b")
    );
}

/// Round-4 finding 3, hostile-publisher leg on a LIVE cell: after the
/// omit-and-reintroduce owner swap is refused, a token minted for the
/// NEW workspace at the unchanged ownership version must NOT verify —
/// ownership never moved, so both the old credential keeps working and
/// the new workspace gains nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn omit_reintroduce_owner_swap_gains_no_authority() {
    let (svc, _state, addr) = auth_rig("proj-orx", "ws-old", &["c1"], None).await;
    let tok_old = mint_token("c1", "proj-orx", "ws-old", 1, 1, "orx1", 600);
    rig_create(addr, "orx", &tok_old).await;

    // Omit the project from the feed entirely...
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: std::collections::HashMap::new(),
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .expect("omission is a legal publication");
    // ...and reintroduce it under a DIFFERENT workspace at the same
    // ownership version (the hostile publisher's move).
    let hostile = rig_policy("proj-orx", "ws-new", 1, 2);
    assert!(
        rig_publish_policy(&svc, hostile, 3).is_err(),
        "the omit/reintroduce owner swap must be refused"
    );
    // The refusal leaves no trace; the honest restore (same workspace,
    // newer policy version) lands and re-serves the project.
    rig_publish_policy(&svc, rig_policy("proj-orx", "ws-old", 1, 2), 3)
        .expect("honest reintroduction under the SAME workspace lands");

    // A token minted for the NEW workspace verifies NOTHING: ownership
    // never moved, so proj-orx still belongs to ws-old at ownership 1.
    let tok_new = mint_token("c1", "proj-orx", "ws-new", 1, 1, "orx2", 600);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orxn",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", tok_new.as_str()),
        ],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st, 401,
        "an owner swap hidden behind omission must not mint authority"
    );
    // And the REAL owner is untouched.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/orx/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("authorization", tok_old.as_str()),
        ],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200, "the legitimate owner keeps working");
}
