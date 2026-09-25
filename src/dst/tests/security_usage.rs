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

/// The month before `month` ("YYYY-MM").
fn previous(month: &str) -> String {
    let (y, m) = crate::billing::parse_month(month).unwrap();
    if m == 1 {
        crate::billing::month_str(y - 1, 12)
    } else {
        crate::billing::month_str(y, m - 1)
    }
}

/// An id is authorized by the name the rollup recorded for it, in every
/// month, not only by the requested month's aggregate. A prior
/// incarnation of the URL's name is served for a month it did not
/// contribute to (a zero row: its segment states carry the name), while
/// a foreign id stays 404 in that month too, where the URL's name has no
/// aggregate at all; and a name with no usage yet refuses a foreign id
/// and serves its own live id named explicitly. Reds against the first
/// version of the check (aggregate membership in the requested month
/// only): the prior incarnation's past month answered 404. The foreign
/// probes kill the two mutants a review planted: serving any id when the
/// name has no aggregate, and checking only the current month.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_usage_stream_id_is_authorized_by_the_name_the_rollup_recorded() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &["c-full", "c-pfx"], None).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let (full, pfx) = bearers(&svc);

    stream_with(addr, &full, "a/mine", 1).await;
    roll_up(&state).await;
    let (st, first) = usage(addr, &full, "/v1/streams/a/mine/usage/current").await;
    assert_eq!(st, 200, "{first}");
    let prior = first["streamId"].as_str().unwrap().to_string();
    let month = first["month"].as_str().unwrap().to_string();
    let past = previous(&month);
    let headers = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("authorization", full.as_str()),
    ];
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/a/mine", &headers, b"").await;
    assert!(st == 200 || st == 204, "delete: {st}");
    stream_with(addr, &full, "a/mine", 2).await;
    stream_with(addr, &full, "b/secret", 3).await;
    // a/empty exists but has no usage, so no aggregate names it.
    rig_create(addr, "a/empty", &full).await;
    roll_up(&state).await;
    let (st, secret) = usage(addr, &full, "/v1/streams/b/secret/usage/current").await;
    assert_eq!(st, 200, "{secret}");
    let foreign = secret["streamId"].as_str().unwrap().to_string();

    // The prior incarnation, in a month it did not contribute to: its
    // own zero row.
    let path = format!("/v1/streams/a/mine/usage?month={past}&streamId={prior}");
    let (st, old) = usage(addr, &pfx, &path).await;
    assert_eq!(st, 200, "{old}");
    assert_eq!(row_of(&old), (Some(prior.as_str()), Some(0)), "{old}");

    // A foreign id in that month, where a/mine has no aggregate, and
    // through a name with no usage at all: never its row.
    for path in [
        format!("/v1/streams/a/mine/usage?month={past}&streamId={foreign}"),
        format!("/v1/streams/a/mine/usage?month={month}&streamId={foreign}"),
        format!("/v1/streams/a/empty/usage/current?streamId={foreign}"),
        format!("/v1/streams/a/empty/usage?month={past}&streamId={foreign}"),
    ] {
        for bearer in [&pfx, &full] {
            let (st, body) = usage(addr, bearer, &path).await;
            assert_eq!(st, 404, "{path}: {body}");
            assert_eq!(body["error"]["code"], "not_found", "{body}");
            assert_eq!(row_of(&body), (None, None), "{body}");
        }
    }

    // a/empty's own live id, named explicitly, is served.
    let (st, empty) = usage(addr, &full, "/v1/streams/a/empty/usage/current").await;
    assert_eq!(st, 200, "{empty}");
    let live = empty["streamId"].as_str().unwrap().to_string();
    let path = format!("/v1/streams/a/empty/usage/current?streamId={live}");
    let (st, named) = usage(addr, &pfx, &path).await;
    assert_eq!(st, 200, "{named}");
    assert_eq!(row_of(&named), (Some(live.as_str()), Some(0)), "{named}");
    engine_shutdown(&state).await;
}

/// Project usage totals need an unrestricted effective stream grant as
/// well as `streams.usage.read` (owner decision, second external review):
/// `c-pfx`, limited to names under "a", could subtract a/mine's usage from
/// the project total and learn b/secret's. Red before: 200 with the
/// project's totals. Now 403 `prefix_denied` with no usage in the body,
/// while its per-stream usage for a/mine stays readable and `c-full` still
/// reads the totals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn project_usage_totals_need_an_unrestricted_stream_grant() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &["c-full", "c-pfx"], None).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let (full, pfx) = bearers(&svc);
    stream_with(addr, &full, "a/mine", 1).await;
    stream_with(addr, &full, "b/secret", 3).await;
    roll_up(&state).await;

    let totals = format!("/v1/projects/{}/usage", PROJECT.0);
    let (st, body) = usage(addr, &pfx, &totals).await;
    assert_eq!(
        (st, &body["error"]["code"]),
        (403, &"prefix_denied".into()),
        "{body}"
    );
    assert!(body.get("ingestRecords").is_none(), "{body}");
    let (st, body) = usage(addr, &full, &totals).await;
    assert_eq!(st, 200, "{body}");
    assert_eq!(body["ingestRecords"].as_u64(), Some(4), "{body}");
    let (st, mine) = usage(addr, &pfx, "/v1/streams/a/mine/usage/current").await;
    assert_eq!(st, 200, "{mine}");
    assert_eq!(mine["ingestRecords"].as_u64(), Some(1), "{mine}");
    engine_shutdown(&state).await;
}

/// A month is four ASCII digits, a dash and two: a signed field ("2026-+9",
/// "+026-09", "-026-09") parsed as a number and answered a zero row (200).
/// Now both usage routes answer 400 `invalid_month`, as for "2026-13".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signed_month_is_refused_on_both_usage_routes() {
    let (svc, state, addr) = auth_rig(PROJECT.0, PROJECT.1, &["c-full", "c-pfx"], None).await;
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    let (full, _) = bearers(&svc);
    stream_with(addr, &full, "a/mine", 0).await;
    let routes = [
        "/v1/streams/a/mine/usage",
        &format!("/v1/projects/{}/usage", PROJECT.0),
    ];
    for route in routes {
        for month in ["2026-+9", "+026-09", "-026-09", "2026-13"] {
            let (st, body) = usage(addr, &full, &format!("{route}?month={month}")).await;
            assert_eq!(
                (st, &body["error"]["code"]),
                (400, &"invalid_month".into()),
                "{route}?month={month}: {body}"
            );
        }
    }
    engine_shutdown(&state).await;
}
