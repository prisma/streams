//! Shared-cell hostile legs (Layer A of the shared-cells plan, section
//! 4.1, rows A2b and A7-A12): what a tenant on a shared cell can do to
//! its neighbours through the cell's SHARED bounds and trust roots.
//!
//! Every leg here fails on today's tree; each is ignored with the plan
//! step that must turn it green, and its exact current failure is
//! recorded in the step notes (`shared-cells/impl/step1-scale-tests.md`).
//! A step that fixes one removes its `ignore` in the same commit. Run
//! them with `cargo test --locked --lib -- --ignored shared_cell_hostile`.

use super::fixture_auth::sr2_workload_jwt;
use super::fixture_cell::{
    CREATE, Cell, CellSpec, SHARE_K, Token, burst, credential, error_code, grant, journal,
    jwks_key, open_cell, padded, policy, project,
};
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use crate::auth::ceiling::SharedBounds;
use crate::project_policy::{CredentialStatus, ProjectQuotas};
use crate::tenant::ProjectId;
use std::collections::HashMap;
use std::sync::Arc;

/// The owner's default quota divisor (PROJECT_SHARE_K, decision of
/// 2026-10-07): a per-project ceiling is the shared bound divided by k.
const K: usize = SHARE_K;
/// The instance inflight bound these rigs set (`ADMIT_MAX_INFLIGHT`).
const BOUND: i64 = 64;
/// A long-poll that parks: no records arrive while it waits.
const PARK: &str = "/v1/streams/orders/records:long-poll?cursor=now&waitMs=2000";

/// The victim's append and read after the crowd has parked.
async fn victim_turn(cell: &Cell, victim: usize) -> ((u16, Option<String>), u16) {
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let body = br#"{"victim":true}"#;
    let (st, _, b) = cell
        .call(victim, "POST", "/v1/streams/orders/records", body)
        .await;
    let (read, _, _) = cell
        .call(victim, "GET", "/v1/streams/orders/records", b"")
        .await;
    ((st, error_code(&b)), read)
}

/// A2b (H1): k - 1 compliant tenants whose feed carries no inflight
/// quota (0, missing: it takes the ceiling) each try to park twice their
/// ceiling, 16 long-polls, against an instance bound of 64 (112 in all,
/// below the survival line at four times the bound). Each must hold
/// exactly its ceiling, bound / k = 8, with every other attempt refused
/// as its own typed `project_concurrency_limit`, and the victim, the k-th
/// tenant, is admitted to append and read. The plan's "k + 1 tenants at
/// their ceilings" cannot leave room at ceiling = bound / k, so the crowd
/// is k - 1 (step 1 notes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compliant_crowd_at_its_ceilings_leaves_the_victim_admitted() {
    let bounds = SharedBounds {
        inflight: u64::try_from(BOUND).unwrap(),
        ..SharedBounds::default()
    };
    let cell = open_cell(CellSpec {
        bounds,
        ..CellSpec::open(K)
    })
    .await;
    for i in 0..K {
        assert_eq!(
            cell.call(i, "PUT", "/v1/streams/orders", CREATE).await.0,
            201
        );
    }
    let cell = &cell;
    let crowd = futures_util::future::join_all(
        (0..K - 1).map(|i| burst(cell, i, ("GET", PARK, b""), 16, true)),
    );
    let (parked, victim) = futures_util::future::join(crowd, victim_turn(cell, K - 1)).await;
    assert_eq!(
        victim,
        ((200, None), 200),
        "the victim among a parked crowd"
    );
    let ceiling = usize::try_from(BOUND).unwrap() / K;
    for (i, answers) in parked.iter().enumerate() {
        let held = answers.iter().filter(|(st, _)| *st < 300).count();
        assert_eq!(held, ceiling, "{} parked", project(i));
        for (st, code) in answers.iter().filter(|(st, _)| *st >= 300) {
            let typed = (*st, code.as_deref());
            assert_eq!(
                typed,
                (429, Some("project_concurrency_limit")),
                "{}",
                project(i)
            );
        }
    }
    engine_shutdown(&cell.state).await;
}

/// Project `i` creates `count` streams under distinct names, in order.
async fn create_streams(cell: &Cell, i: usize, count: usize) -> Vec<(u16, Option<String>)> {
    let mut answers = Vec::new();
    for s in 0..count {
        let path = format!("/v1/streams/s{s:02}");
        let (st, _, b) = cell.call(i, "PUT", &path, CREATE).await;
        answers.push((st, error_code(&b)));
    }
    answers
}

/// A2b, stream axis (H1): with the per-stream maps' bound at 64, k - 1
/// tenants whose feed sets no `max_streams` (a 0 took no reservation at
/// all before the ceiling) each create twice their ceiling under distinct
/// names. Each holds exactly bound / k = 8 streams, every further create
/// is its own `429 stream_limit`, and the victim still creates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crowd_without_stream_quotas_holds_exactly_its_share_of_the_stream_maps() {
    let bounds = SharedBounds {
        streams: 64,
        ..SharedBounds::default()
    };
    let cell = open_cell(CellSpec {
        bounds,
        ..CellSpec::open(K)
    })
    .await;
    let cell = &cell;
    let crowd =
        futures_util::future::join_all((0..K - 1).map(|i| create_streams(cell, i, 16))).await;
    for (i, answers) in crowd.iter().enumerate() {
        let made = answers.iter().filter(|(st, _)| *st == 201).count();
        assert_eq!(made, 64 / K, "{} created", project(i));
        for (st, code) in answers.iter().filter(|(st, _)| *st != 201) {
            let typed = (*st, code.as_deref());
            assert_eq!(typed, (429, Some("stream_limit")), "{}", project(i));
        }
    }
    let victim = cell.call(K - 1, "PUT", "/v1/streams/victim", CREATE).await;
    assert_eq!(victim.0, 201, "the victim's create");
    engine_shutdown(&cell.state).await;
}

/// A12 (L2): a policy naming the system project, one naming the
/// deployment's own `PROJECT_ID`, and a project whose workspace is the
/// deployment's `ACCOUNT_ID` are dropped from the feed and counted (the
/// cell serves its 2 projects and reports 3 dropped): the deployment and
/// account tokens are refused as for a project the cell does not serve
/// (`421 wrong_cell`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserved_ids_are_dropped_from_the_feed() {
    let mut cell = open_cell(CellSpec::open(2)).await;
    let deployment = cell
        .state
        .deployment
        .deployment_tenant()
        .as_str()
        .to_string();
    let account = cell.state.deployment.account_id().to_string();
    let mut tokens = Vec::new();
    for (pid, ws) in [
        (deployment.as_str(), "ws-sc00"),
        ("proj-scacct", account.as_str()),
    ] {
        let mut p = policy(0, ProjectQuotas::default());
        p.project_id = ProjectId::new(pid).unwrap();
        p.workspace_id = crate::tenant::WorkspaceId::new(ws).unwrap();
        cell.policies.insert(p.project_id.clone(), p);
        let mut g = grant(0, 1);
        let cred = format!("c-{pid}");
        g.credential_id = Arc::from(cred.as_str());
        g.project_id = ProjectId::new(pid).unwrap();
        cell.grants.insert(Arc::from(cred.as_str()), g);
        let mut token = Token::of(0);
        token.project_id = pid.to_string();
        token.workspace_id = ws.to_string();
        token.credential_id = cred;
        tokens.push((pid.to_string(), token.bearer()));
    }
    let mut system = policy(0, ProjectQuotas::default());
    system.project_id = crate::tenant::system_project();
    cell.policies.insert(system.project_id.clone(), system);
    cell.publish();
    let feed = cell.svc.feed_json(crate::shard::now_ms() / 1000);
    let counted = (
        feed["policies"]["projects"].as_u64(),
        feed["policies"]["reservedDropped"].as_u64(),
    );
    assert_eq!(counted, (Some(2), Some(3)), "(served, dropped as reserved)");
    for (pid, bearer) in tokens {
        let (st, _, b) = cell
            .call_with(&bearer, "PUT", "/v1/streams/reserved", CREATE)
            .await;
        let got = (st, error_code(&b));
        assert_eq!(
            got,
            (421, Some("wrong_cell".to_string())),
            "a policy for {pid}"
        );
    }
    engine_shutdown(&cell.state).await;
}

/// A8 (M1): four tenants park 16 watch waits, 16 long-polls and 16
/// consumer pulls each (192 parked against an instance bound of 64).
/// Parked waits are not writes in flight: the victim's append and its
/// own long-poll are admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase A step 5: parked waits leave the inflight write gate"]
async fn a_parked_crowd_never_sheds_the_victims_writes() {
    let cell = open_cell(CellSpec::open(5)).await;
    cell.state.admission.set_max_inflight(BOUND);
    let watched = br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
    for i in 0..5 {
        assert_eq!(
            cell.call(i, "PUT", "/v1/streams/orders", watched).await.0,
            201
        );
        let (st, _, _) = cell
            .call(i, "PUT", "/v1/streams/orders/consumers/g1", b"{}")
            .await;
        assert_eq!(st, 201);
    }
    let fields = ["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);
    let wait =
        format!("/v1/streams/orders/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=2000");
    let pull = "/v1/streams/orders/consumers/g1:pull";
    let cell = &cell;
    let wait = wait.as_str();
    let crowd = futures_util::future::join_all((0..4).map(|i| async move {
        let waits = burst(cell, i, ("GET", wait, b""), 16, true);
        let polls = burst(cell, i, ("GET", PARK, b""), 16, true);
        let pulls = burst(
            cell,
            i,
            ("POST", pull, br#"{"max":1,"waitMs":2000}"#),
            16,
            true,
        );
        futures_util::future::join3(waits, polls, pulls).await
    }));
    let victim = async {
        let ((append, read), (poll, _, _)) = futures_util::future::join(
            victim_turn(cell, 4),
            cell.call(
                4,
                "GET",
                "/v1/streams/orders/records:long-poll?cursor=now&waitMs=100",
                b"",
            ),
        )
        .await;
        (append, read, poll < 300)
    };
    let (_, victim) = futures_util::future::join(crowd, victim).await;
    assert_eq!(
        victim,
        ((200, None), 200, true),
        "the victim among parked waits"
    );
    engine_shutdown(&cell.state).await;
}

/// A9 (M3): a credential revoked at grant version 2 stays refused after
/// the cell restarts and its feed source serves the older bundle again.
/// The restart here is a fresh runtime and authorization service over the
/// same store; step 4 replaces it with its boot path that seeds the
/// persisted high-water marks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase A step 4: accepted feed versions persist across restarts"]
async fn a_feed_rollback_after_a_restart_keeps_a_revoked_credential_refused() {
    let mut cell = open_cell(CellSpec::open(2)).await;
    let first = (cell.policies.clone(), cell.grants.clone());
    let revoked: Arc<str> = credential(0).into();
    let g = cell.grants.get_mut(&revoked).unwrap();
    g.status = CredentialStatus::Revoked;
    g.grant_version = 2;
    cell.publish();
    let refused = (403, Some("credential_not_active".to_string()));
    let (st, _, b) = cell.call(0, "GET", "/v1/streams", b"").await;
    assert_eq!((st, error_code(&b)), refused, "revoked before the restart");
    let store = cell.state.data_store.clone();
    engine_shutdown(&cell.state).await;
    let svc = Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    let now = crate::shard::now_ms() / 1000;
    svc.publish_jwks(crate::auth::JwksSnapshot {
        keys: HashMap::from([jwks_key("rig-1")]),
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let (projects, credentials) = first;
    let policies = crate::project_policy::PolicySnapshot {
        projects,
        fetched_at_unix: now,
        feed_version: 1,
    };
    svc.publish_policies(policies).unwrap();
    let grants = crate::project_policy::GrantSnapshot {
        credentials,
        fetched_at_unix: now,
        feed_version: 1,
    };
    let stale = svc.publish_grants(grants);
    let options = HttpRigOptions {
        auth_service: Some(svc),
        ..Default::default()
    };
    let (_, addr) = http_rig_build(store, RigRuntime::incarnation(2), options)
        .await
        .parts();
    let headers = [("authorization", cell.bearers[0].as_str())];
    let (st, _, b) = preq(addr, "GET", "/v1/streams", &headers, b"").await;
    assert_eq!(
        (st, error_code(&b)),
        refused,
        "after the restart (stale publish: {stale:?})"
    );
}

/// A10 (H6): an internal-audience workload token signed by a key the
/// customer JWKS publishes is refused on the internal surface; today it
/// is a fleet credential for every project on the cell (here it reads
/// the victim's `orders` segment, named only by headers, with the
/// stream key).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase A step 8: each signing key is pinned to one audience"]
async fn an_internal_token_signed_by_a_customer_key_is_refused() {
    let cell = open_cell(CellSpec::open(2)).await;
    assert_eq!(
        cell.call(1, "PUT", "/v1/streams/orders", CREATE).await.0,
        201
    );
    let body = br#"{"secret":1}"#;
    assert_eq!(
        cell.call(1, "POST", "/v1/streams/orders/records", body)
            .await
            .0,
        200
    );
    let victim = project(1);
    let sref = ProjectId::new(&victim).unwrap().stream_ref("orders");
    let desc = cell.state.registry.get(&sref).await.unwrap().unwrap();
    // The production internal target (project-qualified) names the
    // victim's segment, exactly as a fleet peer would.
    let target = crate::application::read_remote::InternalTarget::of(&desc, 0).unwrap();
    let now = crate::shard::now_ms() / 1000;
    let token = format!(
        "Bearer {}",
        sr2_workload_jwt("rig-1", &["segment-read"], now)
    );
    let named = target.headers();
    let mut headers: Vec<(&str, &str)> = named.iter().map(|(k, v)| (*k, v.as_str())).collect();
    headers.push(("authorization", token.as_str()));
    headers.push(("stream-encryption-key", PRISMA_KEY));
    let path = "/v1/internal/segment-read/orders";
    let (st, _, b) = preq(cell.addr, "GET", path, &headers, b"").await;
    let shown = String::from_utf8_lossy(&b[..b.len().min(160)]).to_string();
    assert_eq!(
        st, 401,
        "customer-key internal token read {victim}'s segment: {shown}"
    );
    engine_shutdown(&cell.state).await;
}

/// A10 (M5): a watch observation carrying a garbage capability that names
/// project B, plus B's stream key and no token, is refused under
/// enforce: a key never substitutes for both a principal and a verified
/// capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase A step 8: a capability carrier must verify"]
async fn a_capability_carrier_with_only_a_key_is_refused() {
    let cell = open_cell(CellSpec::open(2)).await;
    let watched = br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
    assert_eq!(cell.call(1, "PUT", "/v1/streams/w", watched).await.0, 201);
    let fields = ["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);
    let path = format!(
        "/v1/streams/w/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=100&cap={}.garbage",
        project(1)
    );
    let (st, _, b) = preq(
        cell.addr,
        "GET",
        &path,
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 401, "key-only carrier: {}", String::from_utf8_lossy(&b));
    engine_shutdown(&cell.state).await;
}

/// A7 (H2): two hostile tenants grind stream names onto the shard that
/// holds the victim's `orders` and each append 600 bytes, pushing it
/// past its unabsorbed-bytes line (1 KiB here, absorption paused). The
/// victim's append is still admitted; the shed lands on the
/// contributors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase B step 12: per-(project, shard) backlog attribution"]
async fn names_ground_onto_the_victims_shard_never_shed_the_victim() {
    let mut admission = crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
    .admission;
    admission.unabsorbed_bytes_shard = 1024;
    let mut spec = CellSpec::open(3);
    spec.prefixes = ["00", "01", "10", "11"].map(str::to_string).to_vec();
    spec.admission = Some(admission);
    let cell = open_cell(spec).await;
    cell.state
        .runtime
        .history
        .paused
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let shard = |i: usize, name: &str| {
        let sref = ProjectId::new(&project(i)).unwrap().stream_ref(name);
        cell.state
            .shards
            .prefix_for(&crate::crypto::RouteHash::for_stream(&sref).0)
    };
    assert_eq!(
        cell.call(2, "PUT", "/v1/streams/orders", CREATE).await.0,
        201
    );
    let target = shard(2, "orders");
    let mut hostile = Vec::new();
    for i in 0..2 {
        let name = (0..)
            .map(|j| format!("g{j}"))
            .find(|n| shard(i, n) == target)
            .unwrap();
        assert_eq!(
            cell.call(i, "PUT", &format!("/v1/streams/{name}"), CREATE)
                .await
                .0,
            201
        );
        let path = format!("/v1/streams/{name}/records");
        let (st, _, _) = cell.call(i, "POST", &path, &padded(600)).await;
        assert_eq!(st, 200, "{} ground {name} onto {target}", project(i));
        hostile.push(path);
    }
    let (st, _, b) = cell
        .call(2, "POST", "/v1/streams/orders/records", br#"{"victim":1}"#)
        .await;
    assert_eq!(
        (st, error_code(&b)),
        (200, None),
        "the victim on the ground shard"
    );
    for (i, path) in hostile.iter().enumerate() {
        let (st, _, b) = cell.call(i, "POST", path, &padded(600)).await;
        let typed = (st, error_code(&b));
        assert_eq!(
            typed,
            (503, Some("maintenance_backpressure".into())),
            "{}",
            project(i)
        );
    }
    engine_shutdown(&cell.state).await;
}

/// A11 (M6): an anonymous flood of unverifiable bearers fills the denial
/// journal's queue; the victim's own denial afterwards (a probe of a
/// neighbour's usage) is still journaled with its project.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase B step 14: unverified denials are counted, not queued per event"]
async fn an_anonymous_denial_flood_never_drops_the_victims_denial() {
    let cell = open_cell(CellSpec::open(2)).await;
    let cell = &cell;
    super::fixture_cell::for_each(4_200, |_| async move {
        let headers = [("authorization", "Bearer not-a-jwt")];
        let (st, _, _) = preq(cell.addr, "GET", "/v1/streams/x/records", &headers, b"").await;
        assert_eq!(st, 401);
    })
    .await;
    let probe = format!("/v1/projects/{}/usage", project(1));
    assert_eq!(cell.call(0, "GET", &probe, b"").await.0, 404);
    let events = journal(&cell.state).await;
    let mine = events
        .iter()
        .filter(|e| e["project_id"] == project(0).as_str())
        .count();
    assert_eq!(
        mine,
        1,
        "the victim's denial among {} journaled events",
        events.len()
    );
    engine_shutdown(&cell.state).await;
}

/// A11 (M2): an anonymous flood of capability lookups naming the victim's
/// project (garbage signatures, distinct names) spends no budget the
/// victim needs. The lookup budget refills on real time (500 a second,
/// 1,000 at most), so the flood runs in rounds of 256 concurrent
/// lookups, each followed by one 100 ms victim watch; every victim
/// watch, before and through 40 rounds, must answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase B step 15: unverified capability lookups are not charged to a shared budget"]
async fn a_capability_flood_never_starves_the_victims_watch() {
    let cell = open_cell(CellSpec::open(2)).await;
    let watched = br#"{"format":{"kind":"json"},"watches":[{"name":"by-customer","fields":["/customerId"]}]}"#;
    assert_eq!(cell.call(1, "PUT", "/v1/streams/w", watched).await.0, 201);
    let fields = ["/customerId".to_string()];
    let khex = crate::product::watch_key_hex("by-customer", &fields, &["\"c42\"".to_string()]);
    let cap = capability(&cell, 1, &khex).await;
    let path =
        format!("/v1/streams/w/watches/by-customer/keys/{khex}?cursor=now&timeoutMs=100&cap={cap}");
    for round in 0..=40 {
        let (st, _, b) = preq(cell.addr, "GET", &path, &[], b"").await;
        let shown = String::from_utf8_lossy(&b);
        assert_eq!(
            st, 200,
            "the victim's capability watch after {round} rounds: {shown}"
        );
        garbage_round(&cell, &khex, round).await;
    }
    engine_shutdown(&cell.state).await;
}

/// 256 concurrent anonymous lookups with garbage capabilities that name
/// project 1, each on a distinct name.
async fn garbage_round(cell: &Cell, khex: &str, round: usize) {
    let victim = project(1);
    let lookups = (0..256).map(|j| {
        let path = format!(
            "/v1/streams/n{round}-{j}/watches/by-customer/keys/{khex}?timeoutMs=1&cap={victim}.1.x"
        );
        async move { preq(cell.addr, "GET", &path, &[], b"").await.0 }
    });
    let answers = futures_util::future::join_all(lookups).await;
    assert!(
        answers.iter().all(|st| *st == 403),
        "garbage capabilities: {answers:?}"
    );
}

/// A valid two-minute observation capability for project `i`'s `w`.
async fn capability(cell: &Cell, i: usize, khex: &str) -> String {
    let sref = ProjectId::new(&project(i)).unwrap().stream_ref("w");
    let desc = cell.state.registry.get(&sref).await.unwrap().unwrap();
    let epoch = desc.epoch();
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let signing = crate::crypto::wait_sig_key(&crate::crypto::touch_token(&key, &epoch), &epoch);
    let exp = crate::shard::now_ms() / 1000 + 120;
    let sig = crate::crypto::watch_capability_sig(
        &signing,
        &sref,
        &desc.stream_epoch,
        "by-customer",
        khex,
        "GET",
        exp,
    );
    format!("{}.{exp}.{sig}", project(i))
}
