//! Billing attribution.

use super::fixture_http::{engine_shutdown, http_rig_with_auth_service};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

/// Stage 7 exit ("every billable unit belongs to exactly one project
/// and one workspace-at-event"): the same-name cross-project INVOICE
/// test. Two projects on one enforce cell create the SAME stream
/// name, append different volumes, and the whole pipeline — metering,
/// _usage ledger, rollup, usage APIs — attributes each byte to
/// exactly one project under its OWN workspace: A's invoice shows
/// only A's volume under ws_a, B's only B's under ws_b, and the
/// project-level rollups agree.
#[expect(
    clippy::too_many_lines,
    reason = "cross-project attribution scenario; same-named streams in two projects are driven through append, read and rollup on one rig; helper phases would hide which project a row was attributed to"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_name_cross_project_usage_attributes_exactly() {
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
        "inv-1".to_string(),
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
                  streams.metadata.read streams.usage.read";
    let mut projects = std::collections::HashMap::new();
    let mut credentials = std::collections::HashMap::new();
    // Review fix: BOTH projects are foreign to the rig's deployment
    // tenant (proj-test) — using proj-test as project A made this a
    // deployment+foreign proof, which masked the drain reconciler's
    // deployment-ref lookup bug.
    for (proj, ws, cred) in [("proj-inva", "ws_a", "c_a"), ("proj-b", "ws_b", "c_b")] {
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
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    assert!(
        state.rollup.install(std::sync::Arc::new(rollup)).is_ok(),
        "this rig installs its rollup once"
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
    let mint = |cred: &str, proj: &str, ws: &str| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("inv-1".into());
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
    let ta = format!("Bearer {}", mint("c_a", "proj-inva", "ws_a"));
    let tb = format!("Bearer {}", mint("c_b", "proj-b", "ws_b"));
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let auth_a = ("authorization", ta.as_str());
    let auth_b = ("authorization", tb.as_str());
    let create = br#"{"format":{"kind":"json"}}"#;

    // Same name, two projects; DIFFERENT volumes.
    for auth in [auth_a, auth_b] {
        let (st, _, _) = preq(addr, "PUT", "/v1/streams/inv", &[ekey, auth], create).await;
        assert_eq!(st, 201);
    }
    let pa = br#"{"who":"a","pad":"xxxxxxxx"}"#; // 27 B x2
    for _ in 0..2 {
        let (st, _, _) = preq(addr, "POST", "/v1/streams/inv/records", &[ekey, auth_a], pa).await;
        assert_eq!(st, 200);
    }
    let pb = br#"{"who":"b","padpad":"yyyyyyyyyyyyyyyyyyyy"}"#; // 43 B x3
    for _ in 0..3 {
        let (st, _, _) = preq(addr, "POST", "/v1/streams/inv/records", &[ekey, auth_b], pb).await;
        assert_eq!(st, 200);
    }

    // Drain the ledger and consume it into the rollup.
    state.billing.reads().seal_if_aged(0);
    let mut drained = 0usize;
    for _ in 0..100 {
        let n = crate::billing::drain_once(&state).await.expect("drain");
        drained += n;
        if n == 0 {
            break;
        }
    }
    assert!(drained >= 2, "snapshots drained: {drained}");
    for _ in 0..50 {
        if crate::billing::rollup_step(&state).await.expect("rollup") == 0 {
            break;
        }
    }

    // Each project's invoice-grade answer: ITS volume, under ITS
    // workspace, and nothing of the neighbor's.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/inv/usage/current",
        &[ekey, auth_a],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let va: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(va["ingestRecords"], 2, "A records: {va}");
    assert_eq!(
        va["ingestPayloadBytes"],
        (2 * pa.len()) as u64,
        "A bytes: {va}"
    );
    assert_eq!(va["projectId"], "proj-inva", "{va}");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/inv/usage/current",
        &[ekey, auth_b],
        b"",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let vb: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(vb["ingestRecords"], 3, "B records: {vb}");
    assert_eq!(
        vb["ingestPayloadBytes"],
        (3 * pb.len()) as u64,
        "B bytes: {vb}"
    );
    assert_eq!(vb["projectId"], "proj-b", "{vb}");

    // Project-level rollups agree, each under its own path+principal.
    let (st, _, b) = preq(addr, "GET", "/v1/projects/proj-inva/usage", &[auth_a], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let pva: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(pva["ingestRecords"], 2, "{pva}");
    assert_eq!(pva["accountId"], "ws_a", "workspace-at-event: {pva}");
    let (st, _, b) = preq(addr, "GET", "/v1/projects/proj-b/usage", &[auth_b], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let pvb: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(pvb["ingestRecords"], 3, "{pvb}");
    assert_eq!(pvb["accountId"], "ws_b", "workspace-at-event: {pvb}");
    engine_shutdown(&state).await;
}

/// MT Stage 7 invoice reconciliation: on a multi-project cell, the
/// per-(account, project) totals recomputed from the STREAM month rows
/// agree with the served project aggregates ("the books balance"),
/// and the reconciler actually detects disagreement — an injected
/// corrupt aggregate is reported, so a clean verdict is never
/// vacuous.
#[expect(
    clippy::too_many_lines,
    reason = "invoice reconciliation scenario; balancing, late usage and injected corruption are checked against the same frozen invoice base; helper phases would hide which reconciliation step detected the corruption"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invoice_reconciliation_balances_and_detects_corruption() {
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
        "rec-1".to_string(),
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
                  streams.metadata.read streams.usage.read";
    let mut projects = std::collections::HashMap::new();
    let mut credentials = std::collections::HashMap::new();
    for (proj, ws, cred) in [
        ("proj-reca", "ws_reca", "c_ra"),
        ("proj-recb", "ws_recb", "c_rb"),
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
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    assert!(
        state.rollup.install(std::sync::Arc::new(rollup)).is_ok(),
        "this rig installs its rollup once"
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
    let mint = |cred: &str, proj: &str, ws: &str| {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("rec-1".into());
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
    let ta = format!("Bearer {}", mint("c_ra", "proj-reca", "ws_reca"));
    let tb = format!("Bearer {}", mint("c_rb", "proj-recb", "ws_recb"));
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let auth_a = ("authorization", ta.as_str());
    let auth_b = ("authorization", tb.as_str());
    let create = br#"{"format":{"kind":"json"}}"#;
    for auth in [auth_a, auth_b] {
        let (st, _, _) = preq(addr, "PUT", "/v1/streams/recon", &[ekey, auth], create).await;
        assert_eq!(st, 201);
    }
    for _ in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/recon/records",
            &[ekey, auth_a],
            br#"{"who":"a"}"#,
        )
        .await;
        assert_eq!(st, 200);
    }
    for _ in 0..3 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/recon/records",
            &[ekey, auth_b],
            br#"{"who":"b"}"#,
        )
        .await;
        assert_eq!(st, 200);
    }
    // A read so the read-side dimensions flow too (whatever lands must
    // balance; no volume assertion — consistency is the invariant).
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/recon/records",
        &[ekey, auth_a],
        b"",
    )
    .await;
    assert_eq!(st, 200);

    state.billing.reads().seal_if_aged(0);
    let mut drained = 0usize;
    for _ in 0..100 {
        let n = crate::billing::drain_once(&state).await.expect("drain");
        drained += n;
        if n == 0 {
            break;
        }
    }
    assert!(drained >= 2, "snapshots drained: {drained}");
    for _ in 0..50 {
        if crate::billing::rollup_step(&state).await.expect("rollup") == 0 {
            break;
        }
    }

    let (y, m) = crate::billing::utc_year_month(crate::billing::billing_now_ms());
    let month = crate::billing::month_str(y, m);
    let rollup = state.rollup.get().unwrap();
    let clean = rollup.reconcile_month(&month).await.expect("reconcile");
    assert!(clean.ok, "books must balance: {:?}", clean.mismatches);
    assert!(clean.projects >= 2, "both projects walked: {clean:?}");
    assert!(clean.stream_rows >= 2, "stream rows walked: {clean:?}");

    // Both streams are LIVE and hold retained data: their rollup rows
    // must show a nonzero storage gauge. The drain reconciler used to
    // resolve dirty rows under the deployment tenant, "lose" every
    // foreign-project descriptor, and spuriously billing-close live
    // streams — the close zeroes the owned-bytes gauge, so storage
    // byte-time silently stops accruing while data is served.
    let mut live_rows = 0usize;
    let mut iter = rollup
        .db
        .scan_prefix(format!("month/{month}/").as_bytes(), ..)
        .await
        .unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        let k = String::from_utf8_lossy(&kv.key).to_string();
        if !k.contains("/proj-reca/") && !k.contains("/proj-recb/") {
            continue;
        }
        let row: crate::rollup::MonthRow = serde_json::from_slice(&kv.value).unwrap();
        live_rows += 1;
        let gauge: u64 = row.segments.values().map(|s| s.gauge_bytes).sum();
        assert!(
            gauge > 0,
            "live stream's storage gauge zeroed (spurious billing close): {k}"
        );
        for (seg, sm) in &row.segments {
            assert!(
                !sm.final_seen,
                "live stream spuriously month-finalized: {k} segment {seg}"
            );
        }
    }
    assert_eq!(live_rows, 2, "both project rows walked");

    // Inject a corrupt aggregate for proj-recb and prove detection —
    // the key layout is pinned here on purpose: if the keyspace moves,
    // this test must be revisited alongside the reconciler.
    let bogus = crate::rollup::AggRow {
        ingest_records: 999_999,
        ..Default::default()
    };
    let mut wb = slatedb::WriteBatch::new();
    wb.put(
        format!("project/{month}/ws_recb/proj-recb").into_bytes(),
        serde_json::to_vec(&bogus).unwrap(),
    );
    rollup.db.write(wb).await.unwrap();
    let dirty = rollup.reconcile_month(&month).await.expect("reconcile");
    assert!(!dirty.ok, "corruption must be detected: {dirty:?}");
    assert!(
        dirty
            .mismatches
            .iter()
            .any(|m| m.contains("ws_recb/proj-recb")),
        "the corrupt aggregate must be named: {:?}",
        dirty.mismatches
    );
    engine_shutdown(&state).await;
}
