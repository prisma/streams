//! Shared-cell lifecycle (Layer A of the shared-cells plan, section
//! 4.1): on one enforce-mode cell of `cell_scale()` projects, every
//! lifecycle event aimed at one project (suspension, revocation,
//! transfer, omission from the feed, a forced split, delete and
//! recreate, a signing-key rotation) leaves every neighbour's whole
//! view byte-identical, per-project state returns to its baseline once
//! projects go idle, and a cell that wakes with a stale feed serves
//! every project again after one refresh pass.

use super::fixture_cell::{
    Cell, CellSpec, KEYS, Ledger, NAMES, Token, active, cell_keys, cell_scale, jwks_key, key_query,
    map_each, open_cell, project, seed, workspace,
};
use super::fixture_http::engine_shutdown;
use crate::project_policy::{CredentialStatus, GrantSnapshot, PolicySnapshot, ProjectStatus};
use crate::tenant::ProjectId;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Whether `probe` holds within 90 s, polled every 100 ms (absorption
/// keeps touching handles for tens of seconds on a loaded host at 1,000
/// projects; each touch restarts a handle's idle window).
async fn held_within_90s(probe: impl Fn() -> bool) -> bool {
    for _ in 0..900 {
        if probe() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

/// Fails naming `what` unless `probe` holds within 90 s.
async fn eventually(what: &str, probe: impl Fn() -> bool) {
    assert!(
        held_within_90s(probe).await,
        "{what} never held within 90 s"
    );
}

/// Wait until only the boot baseline's handles stay resident; a timeout
/// names every shard's absorption state.
async fn evicted_to(cell: &Cell, boot: usize) {
    let evicted = held_within_90s(|| resident(cell) == boot).await;
    assert!(
        evicted,
        "idle handles never evicted within 90 s: {}",
        absorption(cell)
    );
}

/// Every shard's absorption state, for an eviction wait that timed out:
/// a gather that cannot finish keeps its streams' handles referenced, and
/// a referenced handle is never evicted. Measured 2026-10-07 at 1,000
/// projects beside the scale module (one process, one two-thread storage
/// executor, the rig's 20 ms absorber tick): a history partition's L0
/// filled, its flush waited for the partition's 300 s manifest poll, and
/// for those 300 s neither shard absorbed a byte.
fn absorption(cell: &Cell) -> String {
    let now = crate::shard::now_ms();
    let shards: Vec<String> = cell
        .state
        .shards
        .engines()
        .iter()
        .map(|e| {
            let m = e.maintenance_snapshot();
            let stalled = if m.last_progress_ms > 0 { now - m.last_progress_ms } else { 0 };
            format!(
                "shard {:?}: {} resident, {} unabsorbed bytes, no absorption progress for {stalled} ms",
                e.prefix,
                e.resident_streams(),
                m.unabsorbed_frame_bytes,
            )
        })
        .collect();
    shards.join("; ")
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
    evicted_to(&cell, boot).await;
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

/// The feeds a woken cell's refresher fetches: the cell's own, at the
/// version it last published, and how many passes fetched them.
struct WakeFeeds {
    policies: PolicySnapshot,
    grants: GrantSnapshot,
    passes: AtomicU64,
}

/// One of the refresher's three sources; each pass asks each once.
struct WakeSource(Arc<WakeFeeds>);

#[async_trait::async_trait]
impl crate::auth_feed::KeySource for WakeSource {
    async fn fetch(&self) -> anyhow::Result<crate::auth::JwksSnapshot> {
        self.0.passes.fetch_add(1, Ordering::Relaxed);
        Ok(cell_keys(0))
    }
}

#[async_trait::async_trait]
impl crate::project_policy::PolicySource for WakeSource {
    async fn fetch(&self) -> anyhow::Result<PolicySnapshot> {
        Ok(self.0.policies.clone())
    }
}

#[async_trait::async_trait]
impl crate::project_policy::GrantSource for WakeSource {
    async fn fetch(&self) -> anyhow::Result<GrantSnapshot> {
        Ok(self.0.grants.clone())
    }
}

/// The feed a sleeping cell finds stale when it wakes.
#[derive(Clone, Copy, Debug)]
enum Aged {
    Policies,
    Grants,
    Keys,
}

impl Aged {
    /// The code a request answers while this feed is stale.
    fn code(self) -> String {
        match self {
            Aged::Policies => "policy_stale",
            Aged::Grants => "grants_stale",
            Aged::Keys => "keys_stale",
        }
        .to_string()
    }

    /// Republish this feed as the cell's last pass stamped it, one second
    /// past its window: the wall clock moved while the cell slept, and
    /// the refresher's monotonic tick did not.
    fn age(self, cell: &Cell, feeds: &WakeFeeds) {
        let now = crate::shard::now_ms() / 1000;
        let policy_window = now - crate::auth::POLICY_STALENESS_MAX_SECS - 1;
        match self {
            Aged::Policies => cell.svc.publish_policies(PolicySnapshot {
                fetched_at_unix: policy_window,
                ..feeds.policies.clone()
            }),
            Aged::Grants => cell.svc.publish_grants(GrantSnapshot {
                fetched_at_unix: policy_window,
                ..feeds.grants.clone()
            }),
            Aged::Keys => cell
                .svc
                .publish_jwks(cell_keys(now - crate::auth::JWKS_STALENESS_MAX_SECS - 1)),
        }
        .unwrap();
    }
}

/// What project `i` gets listing its catalog: (status, error code).
async fn catalog(cell: &Cell, i: usize) -> (u16, Option<String>) {
    let (st, _, b) = cell.call(i, "GET", "/v1/streams", b"").await;
    (st, super::fixture_cell::error_code(&b))
}

/// What every project gets listing its catalog, all at once, by index.
async fn catalogs(cell: &Cell) -> Vec<(u16, Option<String>)> {
    map_each(
        cell.projects,
        |i| async move { (i, catalog(cell, i).await) },
    )
    .await
}

/// Wait until `probe` holds, for at most `secs` seconds (polled every
/// 10 ms); the caller then asserts the exact state either way.
async fn settle(secs: u64, probe: impl Fn() -> bool) {
    for _ in 0..secs * 100 {
        if probe() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// One cell, its refresher at an hour's cadence, then a wake with
/// `aged` stale: the first request, every project's request meanwhile,
/// and every project's request once the wake's pass had 5 s to land.
/// Returns (refresh passes, projects served) after the wake.
async fn wake(aged: Aged) -> (u64, usize) {
    let cell = open_cell(CellSpec::open(cell_scale())).await;
    let (policies, grants) = cell.feeds(0);
    let passes = AtomicU64::new(0);
    let feeds = Arc::new(WakeFeeds {
        policies,
        grants,
        passes,
    });
    let tasks = crate::tasks::TaskSupervisor::new();
    let booted = cell.svc.auth_generation() + 3;
    crate::auth_feed::spawn_refresher(
        cell.svc.clone(),
        Box::new(WakeSource(feeds.clone())),
        Box::new(WakeSource(feeds.clone())),
        Box::new(WakeSource(feeds.clone())),
        std::time::Duration::from_secs(3_600),
        &tasks,
    );
    eventually("the boot pass", || cell.svc.auth_generation() == booted).await;
    aged.age(&cell, &feeds);
    let woken = cell.svc.auth_generation() + 3;
    let first = catalog(&cell, 0).await;
    assert_eq!(
        first,
        (503, Some(aged.code())),
        "{aged:?}: the first request"
    );
    let storm = catalogs(&cell).await;
    let served = (200, None);
    let typed = storm.iter().all(|s| *s == first || *s == served);
    assert!(
        typed,
        "{aged:?}: every answer meanwhile is the refusal or served"
    );
    settle(5, || cell.svc.auth_generation() >= woken).await;
    let after = catalogs(&cell).await;
    let shown = (
        feeds.passes.load(Ordering::Relaxed),
        after.iter().filter(|s| **s == served).count(),
    );
    tasks.shutdown(std::time::Duration::from_millis(100)).await;
    engine_shutdown(&cell.state).await;
    shown
}

/// Step 3 (R2): the refresher's cadence runs on monotonic time and the
/// feeds age on the wall clock, so a cell that slept wakes with a stale
/// feed and no tick due. Its first request is refused as retryable (503
/// and the feed's code), and that refusal wakes the refresher: one pass
/// out of cadence, however many projects were refused meanwhile, and
/// every project is served again. The cadence is an hour and tokio time
/// never advances, so only a refusal can start that pass. Each feed in
/// turn: policies, grants, keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_woken_cell_refreshes_on_its_first_stale_refusal() {
    let (mut shown, mut want) = (Vec::new(), Vec::new());
    for aged in [Aged::Policies, Aged::Grants, Aged::Keys] {
        shown.push((aged.code(), wake(aged).await));
        want.push((aged.code(), (2, cell_scale())));
    }
    assert_eq!(
        shown, want,
        "(stale feed, (refresh passes, projects served)) after each wake"
    );
}
