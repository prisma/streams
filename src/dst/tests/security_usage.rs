//! Usage authorization: `?streamId=` on the per-stream usage route
//! addresses only incarnations of the name in its URL. The gate
//! prefix-checks that name and nothing else, so an id outside it must
//! never select the rollup row that answers.
use super::fixture_auth::{auth_rig, rig_append, rig_bearer, rig_create};
use super::fixture_http::{engine_shutdown, install_rollup};
use super::fixture_requests::{PRISMA_KEY, preq};
use crate::project_policy::{CredentialGrant, CredentialStatus, GrantSnapshot};
use crate::tenant::{ProjectId, Scope, ScopeSet, StreamGrant};
use std::sync::Arc;
use std::time::Duration;

const PROJECT: (&str, &str) = ("proj-usage", "ws-usage");
const USAGE_READ: &str = "streams.usage.read";

/// Grant version 2 of the rig's two credentials: `c-full` holds every
/// scope over every name, `c-pfx` only usage.read over the names under
/// "a". Returns a bearer for each.
fn bearers(svc: &crate::auth::AuthService) -> (String, String) {
    let all = Scope::ALL.map(Scope::as_str).join(" ");
    let under_a = crate::tenant::normalize_prefix_set(&["a"]).unwrap();
    let grants = [
        ("c-full", all.as_str(), StreamGrant::All),
        ("c-pfx", USAGE_READ, StreamGrant::Prefixes(under_a.into())),
    ];
    let credentials = grants.into_iter().map(|(cred, scopes, grant)| {
        let credential = CredentialGrant {
            credential_id: Arc::from(cred),
            project_id: ProjectId::new(PROJECT.0).unwrap(),
            grant_version: 2,
            status: CredentialStatus::Active,
            scopes: ScopeSet::parse(scopes).0,
            grant,
            expires_at: None,
        };
        (Arc::from(cred), credential)
    });
    svc.publish_grants(GrantSnapshot {
        credentials: credentials.collect(),
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .unwrap();
    (
        rig_bearer(PROJECT, "c-full", &all, 2),
        rig_bearer(PROJECT, "c-pfx", USAGE_READ, 2),
    )
}

/// Create the JSON stream `name` and append `records` records to it.
async fn stream_with(addr: std::net::SocketAddr, bearer: &str, name: &str, records: usize) {
    rig_create(addr, name, bearer).await;
    for n in 0..records {
        let body = format!(r#"{{"n":{n}}}"#);
        assert_eq!(rig_append(addr, name, bearer, &body).await, 200, "{name}");
    }
}

/// Drain the meters into `_usage` and roll the ledger up to its end.
async fn roll_up(state: &Arc<crate::http::AppState>) {
    state.billing.reads().seal_if_aged(0);
    for _ in 0..100 {
        if crate::billing::drain_once(state).await.unwrap() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for _ in 0..50 {
        if crate::billing::rollup_step(state).await.unwrap() == 0 {
            break;
        }
    }
}

/// GET a usage path as `bearer`: its status and JSON body.
async fn usage(addr: std::net::SocketAddr, bearer: &str, path: &str) -> (u16, serde_json::Value) {
    let (st, _, body) = preq(addr, "GET", path, &[("authorization", bearer)], b"").await;
    (st, serde_json::from_slice(&body).unwrap_or_default())
}

/// The id and ingest count a usage answer reports.
fn row_of(answer: &serde_json::Value) -> (Option<&str>, Option<u64>) {
    (
        answer["streamId"].as_str(),
        answer["ingestRecords"].as_u64(),
    )
}

/// Red at 26c555dd: a credential whose prefix grant covers only "a" read
/// b/secret's whole usage row through
/// `GET /v1/streams/a/mine/usage/current?streamId=<b/secret's id>` (200),
/// and any credential got a 200 for any id at all. Now an id that is
/// not an incarnation of the URL's name is 404 `not_found`, for every
/// credential. Controls: a prior incarnation of the same name (delete,
/// recreate) and the live id named explicitly are still served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_usage_stream_id_is_served_only_for_an_incarnation_of_the_url_name() {
    // usage/current is month-sensitive on the real clock.
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &["c-full", "c-pfx"], None).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let (full, pfx) = bearers(&svc);
    let current = "/v1/streams/a/mine/usage/current";

    // a/mine lives twice: one record, then (deleted, recreated) two.
    stream_with(addr, &full, "a/mine", 1).await;
    roll_up(&state).await;
    let (st, first) = usage(addr, &full, current).await;
    assert_eq!(st, 200, "{first}");
    let prior = first["streamId"].as_str().unwrap().to_string();
    let headers = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("authorization", full.as_str()),
    ];
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/a/mine", &headers, b"").await;
    assert!(st == 200 || st == 204, "delete: {st}");
    stream_with(addr, &full, "a/mine", 2).await;
    // b/secret, outside c-pfx's grant, holds three.
    stream_with(addr, &full, "b/secret", 3).await;
    roll_up(&state).await;

    let (st, mine) = usage(addr, &pfx, current).await;
    assert_eq!(st, 200, "{mine}");
    let live = mine["streamId"].as_str().unwrap().to_string();
    assert_ne!(live, prior);
    assert_eq!(row_of(&mine), (Some(live.as_str()), Some(2)), "{mine}");
    let listed = mine["incarnations"].as_array().unwrap();
    for id in [&prior, &live] {
        assert!(listed.iter().any(|i| i == id.as_str()), "{id}: {mine}");
    }
    let (st, secret) = usage(addr, &full, "/v1/streams/b/secret/usage/current").await;
    assert_eq!(st, 200, "{secret}");
    let foreign = secret["streamId"].as_str().unwrap();
    assert_eq!(row_of(&secret), (Some(foreign), Some(3)), "{secret}");
    let (st, refused) = usage(addr, &pfx, "/v1/streams/b/secret/usage/current").await;
    assert_eq!(
        (st, &refused["error"]["code"]),
        (403, &"prefix_denied".into())
    );

    // b/secret's id through a/mine's URL, and an id naming nothing: not
    // an incarnation of a/mine, so no credential gets a row for it.
    for bearer in [&pfx, &full] {
        for id in [foreign, "00000000000000ff"] {
            let (st, body) = usage(addr, bearer, &format!("{current}?streamId={id}")).await;
            assert_eq!(st, 404, "streamId={id}: {body}");
            assert_eq!(body["error"]["code"], "not_found", "{body}");
            assert_eq!(row_of(&body), (None, None), "{body}");
        }
    }

    // The prior incarnation of a/mine answers its own row, on both
    // spellings of the route, and the live id named explicitly answers
    // the live row.
    for path in [current, "/v1/streams/a/mine/usage"] {
        let (st, old) = usage(addr, &pfx, &format!("{path}?streamId={prior}")).await;
        assert_eq!(st, 200, "{old}");
        assert_eq!(row_of(&old), (Some(prior.as_str()), Some(1)), "{old}");
    }
    let (st, named) = usage(addr, &pfx, &format!("{current}?streamId={live}")).await;
    assert_eq!(st, 200, "{named}");
    assert_eq!(row_of(&named), (Some(live.as_str()), Some(2)), "{named}");
    engine_shutdown(&state).await;
}
