//! Shared-cell lifecycle (Layer A of the shared-cells plan, section
//! 4.1): on one enforce-mode cell of `cell_scale()` projects, every
//! lifecycle event aimed at one project (suspension, revocation,
//! transfer, omission from the feed, a forced split, delete and
//! recreate, a signing-key rotation) leaves every neighbour's whole
//! view byte-identical, and per-project state returns to its baseline
//! once projects go idle.

use super::fixture_cell::{
    Cell, CellSpec, KEYS, Ledger, NAMES, Token, active, cell_scale, jwks_key, key_query, map_each,
    open_cell, project, seed, workspace,
};
use super::fixture_http::engine_shutdown;
use crate::project_policy::{CredentialStatus, ProjectStatus};
use crate::tenant::ProjectId;
use serde_json::Value;
use std::collections::HashMap;

/// One project's whole observable view: every read, every metadata
/// answer and its catalog, as (status, body bytes).
type View = Vec<(u16, Vec<u8>)>;

async fn view(cell: &Cell, bearer: &str) -> View {
    let mut out = Vec::new();
    for name in NAMES {
        for key in KEYS {
            let path = format!("/v1/streams/{name}/records{}", key_query(key));
            let (st, _, b) = cell.call_with(bearer, "GET", &path, b"").await;
            out.push((st, b));
        }
        let (st, _, b) = cell
            .call_with(bearer, "GET", &format!("/v1/streams/{name}"), b"")
            .await;
        out.push((st, b));
    }
    let (st, _, b) = cell.call_with(bearer, "GET", "/v1/streams", b"").await;
    out.push((st, b));
    out
}

/// The views of `who` under the given bearers, in order.
async fn views(cell: &Cell, bearers: &[String]) -> Vec<View> {
    map_each(bearers.len(), |j| async move {
        (j, view(cell, &bearers[j]).await)
    })
    .await
}

/// Assert every neighbour's view still equals the baseline.
async fn unchanged(cell: &Cell, bearers: &[String], baseline: &[View], after: &str) {
    let now = views(cell, bearers).await;
    for (j, (got, want)) in now.iter().zip(baseline).enumerate() {
        assert!(got == want, "neighbour {j} changed after {after}");
    }
    assert_eq!(now.len(), baseline.len());
}

/// The status and error code project `bearer` gets reading `orders`.
async fn refusal(cell: &Cell, bearer: &str) -> (u16, Option<String>) {
    let (st, _, b) = cell
        .call_with(bearer, "GET", "/v1/streams/orders/records", b"")
        .await;
    (st, super::fixture_cell::error_code(&b))
}

/// A5: six active projects each take one lifecycle event; after each,
/// every neighbour (every other active project and every idle project at
/// index 8k) sees a byte-identical view, and the target answers as its
/// new state requires. A signing-key rotation then keeps every view
/// identical under the new key and refuses the retired one everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transitions_leave_every_neighbour_byte_identical() {
    let mut cell = open_cell(CellSpec::open(cell_scale())).await;
    cell.create_everywhere().await;
    let ledger = seed(&cell).await;
    let t: Vec<usize> = (0..cell.projects).filter(|i| active(*i)).take(6).collect();
    let neighbours: Vec<usize> = (0..cell.projects)
        .filter(|i| !t.contains(i) && (active(*i) || i % 8 == 0))
        .collect();
    let bearers: Vec<String> = neighbours
        .iter()
        .map(|&i| cell.bearers[i].clone())
        .collect();
    let baseline = views(&cell, &bearers).await;
    suspend(&mut cell, t[0]).await;
    unchanged(&cell, &bearers, &baseline, "suspension").await;
    revoke(&mut cell, t[1]).await;
    unchanged(&cell, &bearers, &baseline, "revocation").await;
    transfer(&mut cell, t[2], &ledger).await;
    unchanged(&cell, &bearers, &baseline, "transfer").await;
    omit(&mut cell, t[3]).await;
    unchanged(&cell, &bearers, &baseline, "omission").await;
    split(&cell, t[4], &ledger).await;
    unchanged(&cell, &bearers, &baseline, "split").await;
    recreate(&cell, t[5]).await;
    unchanged(&cell, &bearers, &baseline, "delete and recreate").await;
    rotate_keys(&cell, &neighbours, &baseline).await;
    engine_shutdown(&cell.state).await;
}

async fn suspend(cell: &mut Cell, t: usize) {
    let p = cell.policy_mut(t);
    p.status = ProjectStatus::Suspended;
    p.project_policy_version = 2;
    cell.publish();
    let got = refusal(cell, &cell.bearers[t]).await;
    assert_eq!(
        got,
        (403, Some("project_not_active".into())),
        "{}",
        project(t)
    );
}

async fn revoke(cell: &mut Cell, t: usize) {
    let id: std::sync::Arc<str> = super::fixture_cell::credential(t).into();
    let g = cell.grants.get_mut(&id).unwrap();
    g.status = CredentialStatus::Revoked;
    g.grant_version = 2;
    cell.publish();
    let got = refusal(cell, &cell.bearers[t]).await;
    assert_eq!(
        got,
        (403, Some("credential_not_active".into())),
        "{}",
        project(t)
    );
}

/// Ownership 2 under another workspace: the old token is refused, a
/// token of the new ownership reads exactly the project's records.
async fn transfer(cell: &mut Cell, t: usize, ledger: &Ledger) {
    let to = workspace(t + 1);
    let p = cell.policy_mut(t);
    p.ownership_version = 2;
    p.project_policy_version = 2;
    p.workspace_id = crate::tenant::WorkspaceId::new(&to).unwrap();
    cell.publish();
    let got = refusal(cell, &cell.bearers[t]).await;
    assert_eq!(
        got,
        (401, Some("ownership_version_mismatch".into())),
        "{}",
        project(t)
    );
    let mut token = Token::of(t);
    token.ownership_version = 2;
    token.workspace_id = to;
    let (st, _, b) = cell
        .call_with(&token.bearer(), "GET", "/v1/streams/orders/records", b"")
        .await;
    let got: Value = serde_json::from_slice(&b).unwrap_or_default();
    assert_eq!(
        (st, got),
        (200, Value::Array(ledger.of(t, 0, 0))),
        "{}",
        project(t)
    );
}

/// The project leaves the feed: its token is refused.
async fn omit(cell: &mut Cell, t: usize) {
    cell.policies.remove(&ProjectId::new(&project(t)).unwrap());
    cell.publish();
    let got = refusal(cell, &cell.bearers[t]).await;
    assert_eq!(got, (421, Some("wrong_cell".into())), "{}", project(t));
}

/// Split the target's `orders` at the middle of the key space: every
/// key still reads exactly its ledger.
async fn split(cell: &Cell, t: usize, ledger: &Ledger) {
    let sref = ProjectId::new(&project(t)).unwrap().stream_ref("orders");
    let bound = std::time::Duration::from_secs(60);
    let run = crate::scaler3::execute_split(&cell.state, &sref, 0, 0x8000_0000_0000_0000);
    assert!(
        tokio::time::timeout(bound, run).await.unwrap(),
        "split refused"
    );
    let mut segments = 0;
    for _ in 0..200 {
        cell.state.registry.invalidate(&sref);
        let d = cell.state.registry.get(&sref).await.unwrap().unwrap();
        let map = d.segments.as_ref();
        if map.is_some_and(|m| m.pending.is_none() && m.segments.len() > 1) {
            segments = map.map_or(0, |m| m.segments.len());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(segments, 3, "{} orders after the split", project(t));
    for (k, key) in KEYS.iter().enumerate() {
        let path = format!("/v1/streams/orders/records{}", key_query(key));
        let got = cell.json(t, "GET", &path).await;
        assert_eq!(
            got,
            Value::Array(ledger.of(t, 0, k)),
            "{} key {key:?}",
            project(t)
        );
    }
}

/// Delete `orders` and create it again: the new incarnation is empty.
async fn recreate(cell: &Cell, t: usize) {
    let (st, _, b) = cell.call(t, "DELETE", "/v1/streams/orders", b"").await;
    assert_eq!(
        st,
        204,
        "{} delete: {}",
        project(t),
        String::from_utf8_lossy(&b)
    );
    let create = super::fixture_cell::CREATE;
    let (st, _, b) = cell.call(t, "PUT", "/v1/streams/orders", create).await;
    assert_eq!(
        st,
        201,
        "{} recreate: {}",
        project(t),
        String::from_utf8_lossy(&b)
    );
    let got = cell.json(t, "GET", "/v1/streams/orders/records").await;
    assert_eq!(got, Value::Array(Vec::new()), "{}", project(t));
}

/// Overlap `rig-2` with `rig-1`, then retire `rig-1`: under the new key
/// every view is the baseline, and the old key is refused everywhere.
async fn rotate_keys(cell: &Cell, neighbours: &[usize], baseline: &[View]) {
    let fresh: Vec<String> = neighbours
        .iter()
        .map(|&i| {
            let mut token = Token::of(i);
            token.kid = "rig-2";
            token.bearer()
        })
        .collect();
    for (version, keys) in [(2, vec!["rig-1", "rig-2"]), (3, vec!["rig-2"])] {
        cell.svc
            .publish_jwks(crate::auth::JwksSnapshot {
                keys: keys.into_iter().map(jwks_key).collect::<HashMap<_, _>>(),
                fetched_at_unix: crate::shard::now_ms() / 1000,
                feed_version: version,
            })
            .unwrap();
        unchanged(cell, &fresh, baseline, "key rotation").await;
    }
    for &i in neighbours {
        let got = refusal(cell, &cell.bearers[i]).await;
        assert_eq!(
            got,
            (401, Some("kid_unknown".into())),
            "{} retired key",
            project(i)
        );
    }
}

/// Resident stream handles over every open engine.
fn resident(cell: &Cell) -> usize {
    cell.state
        .shards
        .engines()
        .iter()
        .map(|e| e.resident_streams())
        .sum()
}

/// Poll `probe` every 100 ms until it holds or 90 s pass (absorption
/// keeps touching handles for tens of seconds on a loaded host at 1,000
/// projects; each touch restarts a handle's idle window).
async fn eventually(what: &str, probe: impl Fn() -> bool) {
    for _ in 0..900 {
        if probe() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{what} never held within 90 s");
}

/// A6: per-project state is bounded while the cell works and released
/// when it goes idle. The auth snapshots hold exactly the feed. After
/// every project has spoken, the admission tracker holds exactly one
/// entry per project (below `MAX_TRACKED_PROJECTS`); only touched
/// streams hold a resident handle (an idle project's streams hold
/// none); live subscriptions
/// hold LiveFeed retention for their projects only, and closing them
/// returns every project's subscriptions and retained bytes to zero;
/// idle handles are evicted back to the boot baseline by the shard
/// ticker (`handle_idle_evict` shortened to 1 s); nothing is left
/// inflight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_project_state_is_bounded_and_released() {
    // Billing time moved by another test pins metered handles until the
    // real clock passes it; residency is measured on real billing time.
    let _clock = crate::billing::billing_clock_lock().read().await;
    let mut spec = CellSpec::open(cell_scale());
    spec.shard.handle_idle_evict = std::time::Duration::from_secs(1);
    let cell = open_cell(spec).await;
    let feed = cell.svc.feed_json(crate::shard::now_ms() / 1000);
    let counts = (
        &feed["policies"]["projects"],
        &feed["grants"]["credentials"],
    );
    assert_eq!(
        counts,
        (&Value::from(cell.projects), &Value::from(cell.projects)),
        "{feed}"
    );
    assert_eq!(
        cell.state.quotas.stats(),
        (0, 0),
        "tracker before any request"
    );
    let boot = resident(&cell);
    cell.create_everywhere().await;
    seed(&cell).await;
    let watched: Vec<usize> = (0..16).map(|j| j * cell.projects / 16).collect();
    let mut subs = Vec::new();
    for &i in &watched {
        let q = "?cursor=beginning";
        let mut sock =
            super::fixture_auth::rig_sse(cell.addr, "orders", &cell.bearers[i], q, None).await;
        let (head, _) =
            super::fixture_livefeed::hub_sse_collect(&mut sock, 10, |t| t.contains("upToDate"))
                .await;
        assert!(head.contains(" 200 "), "{}: {head}", project(i));
        subs.push(sock);
    }
    let tracked = cell.state.quotas.stats().0;
    assert_eq!(tracked, cell.projects, "one tracker entry per project");
    assert!(tracked < crate::quota::MAX_TRACKED_PROJECTS);
    let live = cell.state.livefeed.snapshot();
    let mut retaining: Vec<String> = live.project_retention.iter().map(|r| r.0.clone()).collect();
    retaining.sort();
    let want: Vec<String> = watched.iter().map(|&i| project(i)).collect();
    assert_eq!(retaining, want, "LiveFeed retention rows");
    let writers = (0..cell.projects).filter(|i| active(*i)).count();
    let idle_watched = watched.iter().filter(|i| !active(**i)).count();
    let touched = boot + 3 * writers + idle_watched;
    assert_eq!(
        resident(&cell),
        touched,
        "resident handles: only touched streams"
    );
    drop(subs);
    eventually("LiveFeed released", || {
        cell.state.livefeed.snapshot().live_feeds == 0
    })
    .await;
    let rows = cell.state.quotas.memory_pressure_json(0, usize::MAX);
    for row in rows["rows"].as_array().unwrap() {
        let held = (&row["live_subscriptions"], &row["retained_sse_bytes"]);
        assert_eq!(held, (&Value::from(0), &Value::from(0)), "{row}");
    }
    eventually("idle handles evicted", || resident(&cell) == boot).await;
    assert_eq!(
        cell.state.quotas.stats(),
        (cell.projects, 0),
        "tracker after idle"
    );
    engine_shutdown(&cell.state).await;
}

/// Resets the injected billing clock even when the test fails.
struct BillingClockReset;

impl Drop for BillingClockReset {
    fn drop(&mut self) {
        crate::billing::BILLING_CLOCK_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Every descriptor the tombstone walk pages, cell-wide.
async fn descriptors(state: &std::sync::Arc<crate::http::AppState>) -> usize {
    let (mut count, mut after) = (0, None::<String>);
    loop {
        let page = state
            .registry
            .reconciliation_page(after.as_deref(), 256)
            .await
            .unwrap();
        count += page.streams.len();
        if page.exhausted || page.next_after.is_none() {
            return count;
        }
        after = page.next_after;
    }
}

/// Six streams in every project, and in every active project an `exp`
/// stream that expires in a minute and holds one record; the open
/// billing gauges of the `exp` streams.
async fn expiring_cell(cell: &Cell) -> Vec<(std::sync::Arc<crate::shard::ShardEngine>, [u8; 16])> {
    let at = chrono::DateTime::from_timestamp_millis(crate::shard::now_ms() + 60_000)
        .unwrap()
        .to_rfc3339();
    let expiring = format!(r#"{{"format":{{"kind":"json"}},"expiry":{{"at":"{at}"}}}}"#);
    let expiring = expiring.as_str();
    super::fixture_cell::for_each(cell.projects, |i| async move {
        for s in 0..6 {
            let path = format!("/v1/streams/s{s}");
            let create = super::fixture_cell::CREATE;
            assert_eq!(cell.call(i, "PUT", &path, create).await.0, 201);
        }
        if active(i) {
            let body = expiring.as_bytes();
            assert_eq!(cell.call(i, "PUT", "/v1/streams/exp", body).await.0, 201);
            let rec = br#"{"x":1}"#;
            assert_eq!(
                cell.call(i, "POST", "/v1/streams/exp/records", rec).await.0,
                200
            );
        }
    })
    .await;
    let mut gauges = Vec::new();
    for i in (0..cell.projects).filter(|i| active(*i)) {
        let sref = ProjectId::new(&project(i)).unwrap().stream_ref("exp");
        let d = cell.state.registry.get(&sref).await.unwrap().unwrap();
        let route = d.segment_route_by_id(0).unwrap();
        let engine = cell.state.engine_for(&route).await.unwrap();
        let identity = d.dynamic_segment_identity(0);
        let meta = engine.billing_meta(identity).await.unwrap();
        assert!(
            meta.owned_frame_bytes_current > 0,
            "{} exp gauge",
            project(i)
        );
        gauges.push((engine, identity));
    }
    gauges
}

/// Expire every `exp` stream on the billing clock, then run tombstone
/// walk passes until every gauge is closed (one pass past `bound` at
/// most): the closes land within `bound(D)` passes, D being the
/// descriptors the walk pages.
async fn closes_within(bound: fn(usize) -> usize) {
    let _clock = crate::billing::billing_clock_lock().write().await;
    let _reset = BillingClockReset;
    let cell = open_cell(CellSpec::open(cell_scale())).await;
    let mut open = expiring_cell(&cell).await;
    let d = descriptors(&cell.state).await;
    let expired = crate::shard::now_ms() + 120_000;
    crate::billing::BILLING_CLOCK_OVERRIDE.store(expired, std::sync::atomic::Ordering::Relaxed);
    let mut passes = 0;
    while !open.is_empty() && passes <= bound(d) {
        crate::billing::tombstone_walk(&cell.state).await;
        passes += 1;
        let mut still = Vec::new();
        for (engine, identity) in open {
            let meta = engine.billing_meta(identity).await.unwrap();
            if meta.owned_frame_bytes_current > 0 {
                still.push((engine, identity));
            }
        }
        open = still;
    }
    let shown = (open.len(), passes, d, bound(d));
    assert!(
        open.is_empty() && passes <= bound(d),
        "(open gauges, walk passes, descriptors, bound) = {shown:?}"
    );
    engine_shutdown(&cell.state).await;
}

/// A4 (R6): an expired stream nobody touches is closed by the tombstone
/// walk, which pages 256 descriptors per pass across every project: on
/// a cell of D descriptors every close lands within ceil(D / 256) + 1
/// passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_streams_close_within_one_walk_circle() {
    closes_within(|d| d.div_ceil(256) + 1).await;
}

/// A4 (L4): one project's closes do not wait on every other project's
/// descriptors: they land within two walk passes however many streams
/// the cell holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "red until shared-cells phase B step 17: owner-scheduled billing closes"]
async fn expired_streams_close_within_two_walk_passes() {
    closes_within(|_| 2).await;
}
