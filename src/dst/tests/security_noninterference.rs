//! Security noninterference.

use super::fixture_http::http_rig_with_auth_service;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;
use crate::project_policy::ProjectStatus::{self, Active, Suspended};

// ---------------------------------------------------------------------------
// SR-6c: the D/A/B adversarial fixture and the certification campaign
// scaffolding (review validation strategy items B, D, E, F).
//
// The fixture is deliberately hostile to identity bugs: THREE owners —
// the deployment tenant D (raw surface, fleet identity under enforce)
// and two customer projects A and B — hold the SAME stream name with
// the SAME encryption key. A different key per project would convert a
// cross-project identity bug into a decryption 403 and HIDE it; the
// shared key means any leak is observable as bytes.
// ---------------------------------------------------------------------------

/// Build an enforce-mode rig with two customer projects (A, B) under
/// one JWKS kid, both Active with full-grant credentials, and return
/// (state, addr, svc, jwt_a, jwt_b). The deployment tenant D is
/// reachable on the raw surface with the rig's fleet bearer
/// ("dst-internal-token"). Policies are published at feed_version 1;
/// tests that suspend/reactivate republish v2/v3 through `svc`.
async fn dab_rig(
    tag: &str,
    scopes: &str,
) -> (
    std::sync::Arc<crate::http::AppState>,
    std::net::SocketAddr,
    std::sync::Arc<crate::auth::AuthService>,
    String,
    String,
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
    let kid = format!("dab-{tag}");
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
    svc.publish_policies(dab_policies(tag, Active, Active, 1))
        .unwrap();
    let mut credentials = std::collections::HashMap::new();
    for side in ["a", "b"] {
        let cred: std::sync::Arc<str> = std::sync::Arc::from(format!("c-dab-{side}-{tag}"));
        credentials.insert(
            cred.clone(),
            crate::project_policy::CredentialGrant {
                credential_id: cred,
                project_id: crate::tenant::ProjectId::new(&format!("proj-dab-{side}-{tag}"))
                    .unwrap(),
                grant_version: 1,
                status: crate::project_policy::CredentialStatus::Active,
                scopes: crate::tenant::ScopeSet::parse(scopes).0,
                grant: crate::tenant::StreamGrant::All,
                expires_at: None,
            },
        );
    }
    svc.publish_grants(crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let (state, addr) = http_rig_with_auth_service(mem(), svc.clone()).await;
    let jwt_a = dab_jwt(tag, "a", &kid, scopes, now);
    let jwt_b = dab_jwt(tag, "b", &kid, scopes, now);
    (state, addr, svc, jwt_a, jwt_b)
}

/// Full policy snapshot for the fixture's two projects (snapshots are
/// complete replacements, so suspension republishes BOTH projects).
fn dab_policies(
    tag: &str,
    a_status: ProjectStatus,
    b_status: ProjectStatus,
    feed_version: u64,
) -> crate::project_policy::PolicySnapshot {
    let mut projects = std::collections::HashMap::new();
    for (side, status) in [("a", a_status), ("b", b_status)] {
        let pid = crate::tenant::ProjectId::new(&format!("proj-dab-{side}-{tag}")).unwrap();
        projects.insert(
            pid.clone(),
            crate::project_policy::ProjectPolicy {
                project_id: pid,
                workspace_id: crate::tenant::WorkspaceId::new(&format!("ws-dab-{tag}")).unwrap(),
                cell_id: std::sync::Arc::from("test-cell"),
                project_policy_version: feed_version,
                ownership_version: 1,
                status,
                quotas: crate::project_policy::ProjectQuotas::default(),
            },
        );
    }
    crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version,
    }
}

fn dab_jwt(tag: &str, side: &str, kid: &str, scopes: &str, now: i64) -> String {
    const PRIV: &str = include_str!("../fixtures/mt-test-rsa.pem");
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
    jsonwebtoken::encode(
        &header,
        &C {
            iss: "https://auth.prisma.io",
            aud: "prisma-streams-data",
            sub: "u",
            credential_id: &format!("c-dab-{side}-{tag}"),
            project_id: &format!("proj-dab-{side}-{tag}"),
            workspace_id: &format!("ws-dab-{tag}"),
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
    .unwrap()
}

const DAB_SCOPES: &str = "streams.create streams.records.append streams.records.read \
     streams.metadata.read streams.lifecycle.manage streams.catalog.read";

/// B's (and D's) full observable view of a stream: read body bytes +
/// statuses. Epoch-bearing headers are deliberately excluded — a
/// recreate changes epochs by design; the isolation claim is about
/// BYTES and VISIBILITY, not incarnation ids.
async fn dab_view(
    addr: std::net::SocketAddr,
    auth: &(&str, &str),
    ekey: &(&str, &str),
    name: &str,
) -> (u16, Vec<u8>, u16) {
    let (rs, _, rb) = preq(
        addr,
        "GET",
        &format!("/v1/streams/{name}/records"),
        &[*ekey, *auth],
        b"",
    )
    .await;
    let (hs, _, _) = preq(
        addr,
        "HEAD",
        &format!("/v1/streams/{name}"),
        &[*ekey, *auth],
        b"",
    )
    .await;
    (rs, rb, hs)
}

/// Seed the same JSON stream/key for either customer, checking each write.
async fn dab_seed(
    addr: std::net::SocketAddr,
    auth: (&str, &str),
    name: &str,
    who: &str,
    records: u32,
) {
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    let (st, _, _) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{name}"),
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{who} create");
    for i in 0..records {
        let body = format!("{{\"who\":\"{who}\",\"i\":{i}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            &format!("/v1/streams/{name}/records"),
            &[ekey, auth, ct],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200, "{who} append {i}");
    }
}

/// SR-6c (validation item D): the transitions matrix. A runs its
/// stream through every lifecycle transition the cell can drive
/// locally — append, split, seal, delete, recreate — while B (same
/// name, same key, product surface) and D (same name, same key,
/// deployment tenant on the raw surface) hold baselines. After EVERY
/// transition, both foreign views must be byte-identical to their
/// baselines: no transition may touch, reroute, or resurrect a
/// same-named foreign stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dab_transitions_matrix_leaves_foreign_streams_byte_identical() {
    let (state, addr, _svc, jwt_a, jwt_b) = dab_rig("tmx", DAB_SCOPES).await;
    let auth_a = format!("Bearer {jwt_a}");
    let auth_b = format!("Bearer {jwt_b}");
    let a = ("authorization", auth_a.as_str());
    let b = ("authorization", auth_b.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    let fleet = ("authorization", "Bearer dst-internal-token");

    // B and D stage their same-named streams FIRST (they must never
    // move again). Shared key everywhere: leaks become visible bytes.
    dab_seed(addr, b, "tmx", "b", 3).await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/tmx",
        &[ct, fleet],
        br#"[{"who":"d"}]"#,
    )
    .await;
    assert!(st == 200 || st == 201, "D raw create: {st}");

    let base_b = dab_view(addr, &b, &ekey, "tmx").await;
    let (ds, _, db) = hreq(addr, "GET", "/v1/stream/tmx", &[fleet], b"").await;
    assert_eq!(base_b.0, 200, "B baseline read");
    assert_eq!(ds, 200, "D baseline read");

    let check = |step: &'static str, bv: (u16, Vec<u8>, u16), dv: (u16, Vec<u8>)| {
        assert_eq!(bv.0, base_b.0, "B read status after {step}");
        assert_eq!(
            bv.1,
            base_b.1,
            "B bytes after {step}: {}",
            String::from_utf8_lossy(&bv.1)
        );
        assert_eq!(bv.2, base_b.2, "B head status after {step}");
        assert_eq!(dv.0, ds, "D read status after {step}");
        assert_eq!(dv.1, db, "D bytes after {step}");
    };

    // Transition 1: A creates + appends (same name, same key).
    dab_seed(addr, a, "tmx", "a", 4).await;
    let bv = dab_view(addr, &b, &ekey, "tmx").await;
    let (s, _, d2) = hreq(addr, "GET", "/v1/stream/tmx", &[fleet], b"").await;
    check("A create+append", bv, (s, d2));

    // Transition 2: a physical SPLIT of A's stream (the scaler's
    // execute path, keyed by A's ref — the SR-2 conversion under test).
    let a_sref = crate::tenant::ProjectId::new("proj-dab-a-tmx")
        .unwrap()
        .stream_ref("tmx");
    let did = crate::scaler3::execute_split(&state, &a_sref, 0, 0x8000_0000_0000_0000).await;
    assert!(did, "split executed");
    let bv = dab_view(addr, &b, &ekey, "tmx").await;
    let (s, _, d2) = hreq(addr, "GET", "/v1/stream/tmx", &[fleet], b"").await;
    check("A split", bv, (s, d2));

    // Transition 3: A seals.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/tmx:seal", &[ekey, a], b"{}").await;
    assert!(st == 200 || st == 202 || st == 204, "A seal: {st}");
    let bv = dab_view(addr, &b, &ekey, "tmx").await;
    let (s, _, d2) = hreq(addr, "GET", "/v1/stream/tmx", &[fleet], b"").await;
    check("A seal", bv, (s, d2));

    // Transition 4: A deletes (cascade cleanup of the split family).
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/tmx", &[ekey, a], b"").await;
    assert!(st == 200 || st == 204, "A delete: {st}");
    let bv = dab_view(addr, &b, &ekey, "tmx").await;
    let (s, _, d2) = hreq(addr, "GET", "/v1/stream/tmx", &[fleet], b"").await;
    check("A delete", bv, (s, d2));

    // Transition 5: A recreates the same name fresh.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/tmx",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "A recreate");
    let bv = dab_view(addr, &b, &ekey, "tmx").await;
    let (s, _, d2) = hreq(addr, "GET", "/v1/stream/tmx", &[fleet], b"").await;
    check("A recreate", bv, (s, d2));
}

/// SR-6c (validation item E): property-based noninterference. For a
/// set of fixed seeds, a deterministic interleaving of A-ops and
/// B-ops (appends, reads, heads — plus a mid-sequence suspension and
/// reactivation of A) runs against one rig, and B's ops ALONE run
/// against a fresh rig. B's observables — status codes and read
/// bodies — must be IDENTICAL in both worlds: B's view is a pure
/// function of B's own operations, whatever A does and whatever
/// happens to A's project.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dab_noninterference_property() {
    for seed in [3u64, 17, 29] {
        dab_noninterference_for_seed(seed).await;
    }
}

async fn dab_observe(
    addr: std::net::SocketAddr,
    auth: &(&str, &str),
    &(is_b, op, i): &(bool, u8, u32),
) -> (u8, u16, Vec<u8>) {
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    match op {
        0 => {
            let actor = if is_b { "b" } else { "a" };
            let body = format!("{{\"actor\":\"{actor}\",\"i\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/ni/records",
                &[ekey, *auth, ct],
                body.as_bytes(),
            )
            .await;
            (0, st, Vec::new())
        }
        1 => {
            let (st, _, body) =
                preq(addr, "GET", "/v1/streams/ni/records", &[ekey, *auth], b"").await;
            (1, st, body)
        }
        _ => {
            let (st, _, _) = preq(addr, "HEAD", "/v1/streams/ni", &[ekey, *auth], b"").await;
            (2, st, Vec::new())
        }
    }
}

async fn dab_noninterference_for_seed(seed: u64) {
    use rand::{Rng, SeedableRng};
    // World 1: the full interleaving.
    let tag = format!("ni{seed}");
    let (_state, addr, svc, jwt_a, jwt_b) = dab_rig(&tag, DAB_SCOPES).await;
    let auth_a = format!("Bearer {jwt_a}");
    let auth_b = format!("Bearer {jwt_b}");
    let a = ("authorization", auth_a.as_str());
    let b = ("authorization", auth_b.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    for side in [&a, &b] {
        let (st, _, _) = preq(
            addr,
            "PUT",
            "/v1/streams/ni",
            &[ekey, *side],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201, "create seed {seed}");
    }
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    // (actor_is_b, op, payload_i) — generated ONCE, replayed for
    // world 2 by filtering. Ops: 0 = append, 1 = read, 2 = head.
    let script: Vec<(bool, u8, u32)> = (0..24)
        .map(|i| (rng.random_bool(0.5), rng.random_range(0..3u8), i))
        .collect();
    let mut world1_b: Vec<(u8, u16, Vec<u8>)> = Vec::new();
    for (step, operation) in script.iter().enumerate() {
        if step == 8 {
            // A's project is suspended mid-sequence...
            svc.publish_policies(dab_policies(&tag, Suspended, Active, 2))
                .unwrap();
        }
        if step == 16 {
            // ...and reactivated later.
            svc.publish_policies(dab_policies(&tag, Active, Active, 3))
                .unwrap();
        }
        let side = if operation.0 { &b } else { &a };
        let observed = dab_observe(addr, side, operation).await;
        if operation.0 {
            world1_b.push(observed);
        }
    }

    // World 2: a FRESH rig runs only B's subsequence (no A create,
    // no A ops, no feed events).
    let tag2 = format!("ni{seed}x");
    let (_s2, addr2, _svc2, _ja2, jwt_b2) = dab_rig(&tag2, DAB_SCOPES).await;
    let auth_b2 = format!("Bearer {jwt_b2}");
    let b2 = ("authorization", auth_b2.as_str());
    let (st, _, _) = preq(
        addr2,
        "PUT",
        "/v1/streams/ni",
        &[ekey, b2],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "world-2 create seed {seed}");
    let mut world2_b: Vec<(u8, u16, Vec<u8>)> = Vec::new();
    for operation in script.iter().filter(|operation| operation.0) {
        world2_b.push(dab_observe(addr2, &b2, operation).await);
    }
    assert_eq!(
        world1_b.len(),
        world2_b.len(),
        "observable counts, seed {seed}"
    );
    for (k, (w1, w2)) in world1_b.iter().zip(world2_b.iter()).enumerate() {
        assert_eq!(
            w1,
            w2,
            "seed {seed} observable {k} diverged: interleaved {:?} vs solo {:?}",
            (w1.0, w1.1, String::from_utf8_lossy(&w1.2)),
            (w2.0, w2.1, String::from_utf8_lossy(&w2.2)),
        );
    }
}

/// SR-6c (validation item F): a crash INSIDE A's seal machinery — the
/// execution stops dead after persisting the seal intent, exactly the
/// worst moment — leaves B's same-named, same-key stream untouched,
/// and A recovers to a completed seal after the "restart".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dab_seal_intent_crash_leaves_foreign_stream_intact() {
    let (_state, addr, _svc, jwt_a, jwt_b) = dab_rig("fpx", DAB_SCOPES).await;
    let auth_a = format!("Bearer {jwt_a}");
    let auth_b = format!("Bearer {jwt_b}");
    let a = ("authorization", auth_a.as_str());
    let b = ("authorization", auth_b.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let ct = ("content-type", "application/json");
    for side in [&a, &b] {
        let (st, _, _) = preq(
            addr,
            "PUT",
            "/v1/streams/fpx",
            &[ekey, *side],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    for i in 0..2 {
        let body = format!("{{\"who\":\"b\",\"i\":{i}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/fpx/records",
            &[ekey, b, ct],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let base = dab_view(addr, &b, &ekey, "fpx").await;
    assert_eq!(base.0, 200);

    // A's seal crashes after the intent persists.
    crate::failpoints::stop_after_seal_intent("fpx");
    let (_st, _, _) = preq(addr, "POST", "/v1/streams/fpx:seal", &[ekey, a], b"{}").await;
    let after_crash = dab_view(addr, &b, &ekey, "fpx").await;
    assert_eq!(after_crash, base, "B changed under A's seal crash");

    // "Restart": the failpoint clears and A retries; the seal must
    // complete (existing seal-crash tests pin the exact disposition;
    // here the claims are recovery-to-sealed AND B's invariance).
    crate::failpoints::stop_after_seal_intent_off("fpx");
    let (st, _, bb) = preq(addr, "POST", "/v1/streams/fpx:seal", &[ekey, a], b"{}").await;
    assert!(
        st == 200 || st == 202 || st == 204 || st == 409,
        "A re-seal: {st} {}",
        String::from_utf8_lossy(&bb)
    );
    let after_recover = dab_view(addr, &b, &ekey, "fpx").await;
    assert_eq!(after_recover, base, "B changed under A's seal recovery");
}
