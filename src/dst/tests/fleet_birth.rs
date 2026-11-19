#![cfg(test)]
//! A split's children are born on its actor (GATE-DECISION-f2-children-on-actor-2026-11-16, the S5
//! addendum F2, edge change #265). Two real members, `streams-1` and `streams-2`, over one data
//! store and one fleet store: the ring gives the one-shard cell's leaf `""` to `streams-1` and both
//! halves, `0` and `1`, to `streams-2` (rendezvous over FNV-1a keeps siblings together), the case of
//! meas-149's run. Each scenario holds an append across the split at its park and checks who opens
//! the children and who answers the append.

use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::topology_policy::store_topology;
use crate::topology::gate::PausePoint;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// How long a fleet step (a view, a park, a publish) may take: several 2 s ticks.
const BOUND: Duration = Duration::from_secs(20);

/// The stream every scenario appends to.
const STREAM: &str = "/v1/stream/born";

/// The ring holds both members for the scenario: no shrink is published before an hour.
fn two_members(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
}

/// The cell of the meas-149 rerun's planned move: [`two_members`] under the rig's in-flight cap of
/// 512 (`posture-on-1-1x1.env`, `ADMIT_MAX_INFLIGHT`).
fn loaded_members(cli: &mut crate::config::CliArgs) {
    two_members(cli);
    cli.admit_max_inflight = 512;
}

/// A cell's data store and its members' configuration.
type CellOf<'a> = (&'a Arc<dyn ObjectStore>, fn(&mut crate::config::CliArgs));

/// A member `name` of the cell over `data` and `fleet`, whose leaves are `leaves`; its fleet
/// started when `ticks`.
async fn member(
    fleet: &Arc<dyn ObjectStore>,
    (data, cli): CellOf<'_>,
    (name, leaves): (&str, &[&str]),
    runtime: RigRuntime,
    ticks: bool,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        runtime,
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some(name.to_string()),
            cli,
            prefixes: leaves.iter().map(|leaf| (*leaf).to_string()).collect(),
            ..Default::default()
        },
    )
    .await;
    if ticks {
        assert!(crate::fleet::start_configured(
            rig.state.clone(),
            &rig.tasks
        ));
    }
    rig
}

/// The URL the held member's seeded beat publishes: an `https` origin the tick accepts (the actor
/// never forwards to it in these scenarios).
const HELD_URL: &str = "https://streams-2.invalid";

/// A heartbeat of `streams-2` that stays fresh for the scenario, so the actor's ring holds a member
/// whose own fleet tick never runs.
async fn beat_of_held(fleet: &Arc<dyn ObjectStore>) {
    let beat = format!(
        r#"{{"instance":"streams-2","ts_ms":{},"rps":0.0,"cpu_pct":0.0,"inflight":0,"owned_shards":[],"draining":false,"url":"{HELD_URL}"}}"#,
        crate::shard::now_ms() + 600_000
    );
    fleet
        .put(&Path::from("fleet/streams-2.json"), PutPayload::from(beat))
        .await
        .unwrap();
}

/// How `streams-2`'s fleet tick runs in a scenario.
#[derive(Clone, Copy)]
enum Tick {
    Runs,
    Held,
    Never,
}

/// The cell: `streams-1` (the split's actor, the ring's owner of `""`) and `streams-2` (the ring's
/// owner of both halves), each a peer of the other, once both views name both members.
pub(super) struct Pair {
    pub(super) a: HttpRig,
    pub(super) b: HttpRig,
    /// The cell's fleet store.
    fleet: Arc<dyn ObjectStore>,
}

impl Pair {
    /// Both members with their fleet ticks running.
    pub(super) async fn new() -> Self {
        Self::of(Tick::Runs).await
    }

    /// `streams-2` with its fleet tick held from before the split: it never runs, its view is the
    /// pre-split ring of both members with no override, its beat is kept fresh in the fleet store,
    /// and it revalidates its topology only when the scenario asks it to.
    pub(super) async fn held() -> Self {
        Self::of(Tick::Held).await
    }

    /// `streams-2` with its fleet tick held from boot: it has never had a view (edge change #263).
    async fn viewless() -> Self {
        Self::of(Tick::Never).await
    }

    /// Both members with their fleet ticks running, every data-store request 20 ms away and the
    /// rig's in-flight cap of 512: the meas-149 rerun's planned move (F6).
    pub(super) async fn loaded() -> Self {
        use crate::dst::fault_store::{FaultPlan, FaultStore};
        let plan = FaultPlan {
            latency_pct: 100,
            latency_ms: (20, 20),
            ..FaultPlan::CLEAN
        };
        let data: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 0xF6, plan);
        let pair = Self::over(Tick::Runs, (&data, loaded_members)).await;
        // Both members' URLs published, as a deploy publishes them, so a member forwards to the
        // owner instead of answering its client `409` (the ticks replace the peers set by hand).
        let urls = serde_json::json!({
            "streams-1": format!("http://{}", pair.a.addr),
            "streams-2": format!("http://{}", pair.b.addr),
        });
        let path = Path::from("fleet/urls.json");
        pair.fleet
            .put(&path, PutPayload::from(urls.to_string()))
            .await
            .unwrap();
        pair
    }

    async fn of(tick: Tick) -> Self {
        Self::over(tick, (&mem(), two_members)).await
    }

    /// Both members with their fleet ticks running over a cell of the two leaves `0` and `1`,
    /// both the ring's for `streams-2`, whose parent `""` the ring gives to `streams-1`.
    async fn halves() -> Self {
        Self::over_leaves(Tick::Runs, (&mem(), two_members), &["0", "1"]).await
    }

    async fn over(tick: Tick, cell: CellOf<'_>) -> Self {
        Self::over_leaves(tick, cell, &[""]).await
    }

    async fn over_leaves(tick: Tick, cell: CellOf<'_>, leaves: &[&str]) -> Self {
        let b_ticks = matches!(tick, Tick::Runs);
        let fleet = mem();
        let desired = r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
        fleet
            .put(&Path::from("fleet/desired.json"), PutPayload::from(desired))
            .await
            .unwrap();
        let b = member(
            &fleet,
            cell,
            ("streams-2", leaves),
            RigRuntime::incarnation(1),
            b_ticks,
        )
        .await;
        if !b_ticks {
            beat_of_held(&fleet).await;
        }
        if matches!(tick, Tick::Held) {
            let both = ["streams-1", "streams-2"].map(str::to_string).to_vec();
            b.state.ownership.set_view(both, Default::default());
        }
        let a = member(
            &fleet,
            cell,
            ("streams-1", leaves),
            RigRuntime::first(),
            true,
        )
        .await;
        store_topology(&a).await;
        b.state
            .peer
            .set_peer("streams-1", &format!("http://{}", a.addr));
        if b_ticks {
            a.state
                .peer
                .set_peer("streams-2", &format!("http://{}", b.addr));
        } else {
            // The actor's tick publishes `streams-2`'s URL from its seeded beat (`set_peers`).
            let t0 = tokio::time::Instant::now();
            while a.state.peer.url_for("streams-2").as_deref() != Some(HELD_URL) {
                assert!(
                    t0.elapsed() < BOUND,
                    "the actor's tick publishes the member's URL"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        let viewing: &[&HttpRig] = if matches!(tick, Tick::Never) {
            &[&a]
        } else {
            &[&a, &b]
        };
        let t0 = tokio::time::Instant::now();
        while viewing
            .iter()
            .any(|rig| rig.state.ownership.ring_active().len() < 2)
        {
            assert!(t0.elapsed() < BOUND, "both views name both members");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let owner = |p: &str| a.state.ownership.effective_owner(p);
        assert_eq!(
            [owner(""), owner("0"), owner("1")].map(Option::unwrap_or_default),
            ["streams-1", "streams-2", "streams-2"].map(str::to_string),
            "the ring of the run's case"
        );
        Self { a, b, fleet }
    }

    pub(super) async fn shutdown(self) {
        for rig in [&self.a, &self.b] {
            let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
            assert!(report.aborted.is_empty(), "{report:?}");
        }
        engine_shutdown(&self.a.state).await;
        engine_shutdown(&self.b.state).await;
    }
}

/// One record appended to [`STREAM`] at `addr`: its status and headers.
async fn append(
    addr: std::net::SocketAddr,
    n: u32,
) -> (u16, std::collections::HashMap<String, String>) {
    let json = [("content-type", "application/json")];
    let body = format!(r#"[{{"n":{n}}}]"#);
    let (status, headers, _) = hreq(addr, "POST", STREAM, &json, body.as_bytes()).await;
    (status, headers)
}

/// The split of `""` started on the actor `pair.a`, parked at its barrier with the append of record
/// 2 held there, then released: the held append's status and headers, once the split published.
pub(super) async fn split_with_a_held_append(
    pair: &Pair,
) -> (u16, std::collections::HashMap<String, String>) {
    let json = [("content-type", "application/json")];
    let (status, _, _) = hreq(pair.a.addr, "PUT", STREAM, &json, b"").await;
    assert_eq!(status, 201, "the stream is created");
    assert_eq!(append(pair.a.addr, 1).await.0, 204, "record 1");
    let parent = pair
        .a
        .state
        .shards
        .open("")
        .expect("the actor holds `\"\"`");
    let (reached, at_park) = tokio::sync::oneshot::channel();
    let (release, go) = tokio::sync::oneshot::channel();
    parent
        .gc_state()
        .gate
        .pause_at_park(PausePoint { reached, go });
    let started = pair
        .a
        .state
        .shards
        .topology_ops()
        .split(String::new())
        .await;
    assert!(started.is_ok(), "the split starts: {started:?}");
    tokio::time::timeout(BOUND, at_park)
        .await
        .expect("the split parks")
        .unwrap();
    let released = async {
        let t0 = tokio::time::Instant::now();
        while parent.queued_ops() == 0 {
            assert!(t0.elapsed() < BOUND, "the append is held at the park");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        release.send(()).unwrap();
    };
    let held = async {
        futures_util::future::join(append(pair.a.addr, 2), released)
            .await
            .0
    };
    let answer = tokio::time::timeout(BOUND, held)
        .await
        .expect("the held append is answered");
    let t0 = tokio::time::Instant::now();
    while pair.a.state.shards.prefixes().shards.shards != ["0", "1"] {
        assert!(t0.elapsed() < BOUND, "the split publishes");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The held append is answered once its own child opened; the publish opens both children
    // and retires the parent before the split ends its intent.
    intent_ended(&pair.a.state).await;
    answer
}

/// The records of [`STREAM`], read at `addr`.
async fn records(addr: std::net::SocketAddr) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", STREAM, &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// **A split's children are served by its actor at publish** (F2, T1). The run's case: the actor
/// `streams-1` splits `""`, whose halves the ring gives to `streams-2`. An append held at the
/// split's park is acked `204` by the actor's child, at the offset after record 1; the actor holds
/// both children open, `streams-2` opens nothing, and the stream reads back both records at the
/// actor. Red before F2: the actor's child opens are refused `NotOwner`, the held append is forwarded
/// and `streams-2` opens a child cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_splits_children_are_served_by_the_actor_at_publish() {
    let pair = Pair::new().await;
    let (status, headers) = split_with_a_held_append(&pair).await;
    let (a, b) = (&pair.a.state.shards, &pair.b.state.shards);
    let opened = (a.is_open("0"), a.is_open("1"), b.open_count());
    assert_eq!(
        (status, opened),
        (204, (true, true, 0)),
        "the held append's answer and who opened the children; headers {headers:?}"
    );
    assert_eq!(
        records(pair.a.addr).await,
        serde_json::json!([{"n": 1}, {"n": 2}]),
        "both records, at the actor"
    );
    pair.shutdown().await;
}

/// A stream whose route lands in the leaf `0` of `rig`'s cell, created there: its path.
async fn stream_in_0(rig: &HttpRig) -> String {
    use super::billing_closure_debts::{JSON, raw_put, stored};
    for taken in 0..200 {
        let name = format!("product-{taken}");
        raw_put(rig.addr, &name, &[JSON], b"").await;
        let desc = stored(&rig.state, &rig.state.deployment.raw_adapter_sref(&name)).await;
        let route = desc.resolve_segment("").shard_route;
        if rig.state.shards.prefix_for(&route) == "0" {
            return format!("/v1/stream/{name}");
        }
    }
    panic!("no stream lands in 0");
}

/// **A merge's product is served by its actor at publish** (§22 C19, GATE-DECISION-s22-addendum-
/// 2026-11-19; NEXT-WORK "A merge's child is born on the ring"). The ring gives the halves `0` and
/// `1` to `streams-2` and their parent `""` to `streams-1`. `streams-2` merges them with an append
/// held at the merge's park: the append is acked `204` by the actor's product, the actor holds
/// `""` open, `streams-1` opens nothing, the overrides document names the actor for `""`, and the
/// stream reads back both records at the actor. Red before C19: the product has no entry, the
/// actor's open of it is refused `NotOwner`, and `streams-1` opens it cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merges_child_is_served_by_the_actor_at_publish() {
    let pair = Pair::halves().await;
    let (a, b) = (&pair.a, &pair.b);
    let path = stream_in_0(b).await;
    let json = [("content-type", "application/json")];
    let append = |n: u32| {
        let path = path.clone();
        async move {
            let body = format!(r#"[{{"n":{n}}}]"#);
            hreq(b.addr, "POST", &path, &json, body.as_bytes()).await
        }
    };
    assert_eq!(append(1).await.0, 204, "record 1");
    let left = b.state.shards.open("0").expect("the actor holds `0`");
    let (reached, at_park) = tokio::sync::oneshot::channel();
    let (release, go) = tokio::sync::oneshot::channel();
    left.gc_state()
        .gate
        .pause_at_park(PausePoint { reached, go });
    let started = b.state.shards.topology_ops().merge(String::new()).await;
    assert!(started.is_ok(), "the merge starts: {started:?}");
    tokio::time::timeout(BOUND, at_park)
        .await
        .expect("the merge parks")
        .unwrap();
    let released = async {
        let t0 = tokio::time::Instant::now();
        while left.queued_ops() == 0 {
            assert!(t0.elapsed() < BOUND, "the append is held at the park");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        release.send(()).unwrap();
    };
    let held = async { futures_util::future::join(append(2), released).await.0 };
    let (status, headers, _) = tokio::time::timeout(BOUND, held)
        .await
        .expect("the held append is answered");
    let t0 = tokio::time::Instant::now();
    while b.state.shards.prefixes().shards.shards != [""] {
        assert!(t0.elapsed() < BOUND, "the merge publishes");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let entry = overrides_of(b).await.entries.get("").map(|e| e.to.clone());
    assert_eq!(
        (
            status,
            b.state.shards.is_open(""),
            a.state.shards.open_count(),
            entry
        ),
        (204, true, 0, Some("streams-2".to_string())),
        "the held append's answer, who opened the product, and its entry; headers {headers:?}"
    );
    let (status, _, body) = hreq(b.addr, "GET", &path, &[], b"").await;
    let read: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(
        (status, read),
        (200, serde_json::json!([{"n": 1}, {"n": 2}])),
        "both records, at the actor"
    );
    intent_ended(&b.state).await;
    pair.shutdown().await;
}

/// How the member whose tick is held learns the split.
#[derive(Clone, Copy, Debug)]
enum Install {
    /// Its periodic revalidation (`revalidate::once`).
    Revalidation,
    /// The forward path's catch-up: a request forwarded at the split's version.
    Forwarded,
}

/// The children's engines on the actor, by incarnation.
fn children_of(rig: &HttpRig) -> [Option<crate::sharddir::EngineIncarnation>; 2] {
    ["0", "1"].map(|child| rig.state.shards.resident_incarnation(child))
}

/// T2's scenario, by `how` the member `streams-2`, its tick held from before the split, installs
/// the split's version: the answer to record 3 appended there, and whether it opened anything.
async fn a_member_installs(how: Install) {
    let pair = Pair::held().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "{how:?}: the held append");
    let (a, b) = (&pair.a, &pair.b);
    let actor = children_of(a);
    assert!(
        actor.iter().all(Option::is_some),
        "{how:?}: the actor serves both children"
    );
    let json = [("content-type", "application/json")];
    let body = br#"[{"n":3}]"#;
    let (status, headers, _) = match how {
        Install::Revalidation => {
            let refresh =
                crate::topology::revalidate::Refresh::of(&b.state.fleet, &b.state.ownership);
            let now = b.state.runtime.clock.monotonic();
            let installed = crate::topology::revalidate::once(
                &b.state.registry,
                &b.state.shards,
                refresh.as_ref(),
                now,
            );
            assert!(
                installed.await.unwrap(),
                "{how:?}: the member installs the split"
            );
            hreq(b.addr, "POST", STREAM, &json, body).await
        }
        Install::Forwarded => {
            let forwarded = [json[0], ("streams-forwarded", "v=2")];
            hreq(b.addr, "POST", STREAM, &forwarded, body).await
        }
    };
    assert_eq!(
        b.state.shards.prefixes().shards.version,
        2,
        "{how:?}: installed"
    );
    let served_by = match how {
        Install::Revalidation => headers.get("streams-forwarded-to"),
        Install::Forwarded => headers.get("streams-replay-to"),
    };
    assert_eq!(
        (
            status,
            served_by.map(String::as_str),
            b.state.shards.open_count()
        ),
        (
            match how {
                Install::Revalidation => 204,
                Install::Forwarded => 409,
            },
            Some("streams-1"),
            0
        ),
        "{how:?}: record 3's answer names the actor, and the member opened nothing; headers {headers:?}"
    );
    if matches!(how, Install::Forwarded) {
        assert_eq!(
            append(a.addr, 3).await.0,
            204,
            "the client's replay to the actor"
        );
    }
    assert_eq!(
        children_of(a),
        actor,
        "{how:?}: the actor's children are never fenced"
    );
    assert_eq!(
        records(a.addr).await,
        serde_json::json!([{"n": 1}, {"n": 2}, {"n": 3}]),
        "{how:?}: every record, at the actor"
    );
    pair.shutdown().await;
}

/// **A member installing a split before its tick opens no child** (F2, T2; A1). `streams-2`'s fleet
/// tick is held from before the actor's publish, so its view is the pre-split ring, which gives it
/// both halves. It installs the split's version (a) by its revalidation and (b) by the forward
/// path's catch-up on a request forwarded at that version. Either install first refreshes the
/// overrides, which name the actor for both children: the member opens nothing, record 3 is
/// forwarded to the actor (a) or answered `409` naming it (b, a forwarded request is not forwarded
/// again at the same version), the actor's two engines keep their incarnations, and the stream
/// reads back records 1-3 at the actor. Red with A2 alone: the member installs the ring's owners,
/// opens a child and fences the actor's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_installing_a_split_before_its_tick_opens_no_child() {
    a_member_installs(Install::Revalidation).await;
    a_member_installs(Install::Forwarded).await;
}

/// **A member with no view installs a split and still opens nothing** (F2, T4; F3's edge change
/// #263). `streams-2` boots in fleet mode and its tick never runs, so it has no view. It installs
/// the split's version by its revalidation, whose refresh reads the overrides and keeps the view's
/// active members exactly as they are, none: it still awaits its first view, answers an append `503
/// temporarily_unavailable`, and opens nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_with_no_view_installs_a_split_and_still_opens_nothing() {
    let pair = Pair::viewless().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "the held append");
    let b = &pair.b.state;
    let refresh = crate::topology::revalidate::Refresh::of(&b.fleet, &b.ownership);
    let now = b.runtime.clock.monotonic();
    let installed =
        crate::topology::revalidate::once(&b.registry, &b.shards, refresh.as_ref(), now);
    assert!(installed.await.unwrap(), "the member installs the split");
    let (status, _) = append(pair.b.addr, 3).await;
    let view = b.ownership.view();
    assert_eq!(
        (
            status,
            b.ownership.awaits_view(),
            view.active.len(),
            b.shards.open_count()
        ),
        (503, true, 0, 0),
        "the member still awaits its view and opened nothing; view {view:?}"
    );
    pair.shutdown().await;
}

/// The overrides document of the cell at `rig`'s fleet store.
pub(super) async fn overrides_of(rig: &HttpRig) -> crate::fleet::Overrides {
    rig.state.fleet.read_overrides().await.unwrap().0
}

/// The children's entries, which name the actor, stamped back past the return window, by CAS.
pub(super) async fn age_the_children(a: &crate::http::AppState) {
    let window_ms = i64::try_from(a.config.fleet.rebalance_return_secs).unwrap() * 1000;
    age_entries(a, &["0", "1"], "streams-1", window_ms + 60_000).await;
}

/// `children`'s entries, each naming `actor`, stamped `back_ms` earlier, by CAS.
async fn age_entries(a: &crate::http::AppState, children: &[&str], actor: &str, back_ms: i64) {
    let t0 = tokio::time::Instant::now();
    loop {
        let (mut aged, version) = a.fleet.read_overrides().await.unwrap();
        for child in children {
            let entry = aged.entries.get_mut(*child).expect("a born entry");
            assert_eq!(entry.to, actor, "{child} born on the actor");
            entry.ms -= back_ms;
        }
        let raw = serde_json::to_vec(&aged).unwrap();
        let doc = crate::fleet::FleetDocument::Overrides;
        if a.fleet.replace_document(doc, raw, version).await {
            return;
        }
        assert!(t0.elapsed() < BOUND, "the aged document is written");
    }
}

/// **Exactly one child returns home after the window** (F2, T5). T1's cell, once the split ended:
/// its children's entries name the actor; aged past `REBALANCE_RETURN_SECS` (their stamps moved
/// back, as the window would), return-home gives back one child to its ring home `streams-2`, whose
/// fair share of the two leaves is one: one `rebalance_return` event, one entry left naming the
/// actor, one child owned by each member, and the actor holding only its own child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn exactly_one_child_returns_home_after_the_window() {
    let pair = Pair::new().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "the held append");
    let a = &pair.a.state;
    let t0 = tokio::time::Instant::now();
    while crate::topology::intent::read(&*a.data_store)
        .await
        .unwrap()
        .is_some_and(|held| !held.intent.is_terminal())
    {
        assert!(t0.elapsed() < BOUND, "the split's intent ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    age_the_children(a).await;
    let returned = |doc: &crate::fleet::Overrides| {
        doc.pending_events
            .iter()
            .filter(|e| e.event_type == "rebalance_return")
            .count()
    };
    let t0 = tokio::time::Instant::now();
    while returned(&overrides_of(&pair.a).await) == 0 {
        assert!(t0.elapsed() < BOUND, "a child returns home");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let doc = overrides_of(&pair.a).await;
    let left: Vec<(&String, &str)> = doc
        .entries
        .iter()
        .map(|(p, e)| (p, e.to.as_str()))
        .collect();
    let owners = ["0", "1"].map(|p| a.ownership.effective_owner(p).unwrap_or_default());
    let mut sorted = owners.clone();
    sorted.sort();
    assert_eq!(
        (returned(&doc), left.len(), sorted),
        (1, 1, ["streams-1", "streams-2"].map(str::to_string)),
        "one return, one entry left {left:?}, one child each {owners:?}"
    );
    let kept = if owners[0] == "streams-1" { "0" } else { "1" };
    let gone = if kept == "0" { "1" } else { "0" };
    assert_eq!(
        (a.shards.is_open(kept), a.shards.is_open(gone)),
        (true, false),
        "the actor keeps {kept} and handed {gone} off"
    );
    pair.shutdown().await;
}

/// Wait until the cell's intent at `state` is absent or terminal.
async fn intent_ended(state: &crate::http::AppState) {
    let t0 = tokio::time::Instant::now();
    while crate::topology::intent::read(&*state.data_store)
        .await
        .unwrap()
        .is_some_and(|held| !held.intent.is_terminal())
    {
        assert!(t0.elapsed() < BOUND, "the operation's intent ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Split `prefix` on `rig`, once it is settled, and wait until its installed leaves are `leaves`.
async fn split_on(rig: &HttpRig, prefix: &str, leaves: &[&str]) {
    let ops = rig.state.shards.topology_ops();
    let started = crate::topology::ops::Handle::split(ops, String::from(prefix)).await;
    assert!(
        started.is_ok(),
        "the split of {prefix:?} starts: {started:?}"
    );
    let t0 = tokio::time::Instant::now();
    while rig.state.shards.prefixes().shards.shards != leaves {
        assert!(t0.elapsed() < BOUND, "the split of {prefix:?} publishes");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    intent_ended(&rig.state).await;
}

/// **A born override does not pin a child past the return window** (F2, T6; A5). Three members,
/// `streams-4` (the ring's owner of `""`), `streams-5` and `streams-6`: the actor `streams-4` splits
/// `""`, then its child `1`, so the leaves `0`, `10` and `11`, all three ringed to `streams-6`, are
/// born on it. Past the return window (the entries' stamps moved back) return-home gives
/// `streams-6` one of them, its fair share of three leaves over three members; the actor still holds
/// two by their overrides. Count balance, once its own move cooldown passed on the rig's clock,
/// moves one of those to `streams-5`: every member holds one. Red with the addendum as written (a
/// born override never movable): the actor holds two for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_born_override_does_not_pin_a_child_past_the_return_window() {
    let fleet = mem();
    // The ring is the first `count` ordinal names that beat: a count of six over members 4-6.
    let desired = r#"{"count":6,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(desired))
        .await
        .unwrap();
    let data = mem();
    let names = ["streams-4", "streams-5", "streams-6"];
    let mut rigs = Vec::new();
    for (n, name) in names.iter().enumerate() {
        let runtime = RigRuntime::incarnation(u64::try_from(n).unwrap());
        let cell = (&data, two_members as fn(&mut crate::config::CliArgs));
        rigs.push(member(&fleet, cell, (*name, &[""]), runtime, true).await);
    }
    store_topology(&rigs[0]).await;
    for rig in &rigs {
        for (name, peer) in names.iter().zip(&rigs) {
            rig.state
                .peer
                .set_peer(name, &format!("http://{}", peer.addr));
        }
    }
    let t0 = tokio::time::Instant::now();
    while rigs
        .iter()
        .any(|rig| rig.state.ownership.ring_active().len() < 3)
    {
        assert!(t0.elapsed() < BOUND, "every view names the three members");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let actor = &rigs[0];
    let move_ms =
        i64::try_from(actor.state.config.fleet.rebalance_move_cooldown_secs).unwrap() * 1000;
    let window_ms = i64::try_from(actor.state.config.fleet.rebalance_return_secs).unwrap() * 1000;
    split_on(actor, "", &["0", "1"]).await;
    // Past the move cooldown (the cell's next split may start), within the return window.
    age_entries(&actor.state, &["0", "1"], "streams-4", move_ms + 10_000).await;
    split_on(actor, "1", &["0", "10", "11"]).await;
    let owner = |p: &str| actor.state.ownership.effective_owner(p).unwrap_or_default();
    let ringed = ["0", "10", "11"].map(|p| {
        let active = actor.state.ownership.ring_active();
        active[crate::ownership::ring_pick(p, &active)].clone()
    });
    assert_eq!(
        ringed,
        ["streams-6"; 3].map(str::to_string),
        "all three ring to streams-6"
    );
    assert_eq!(
        ["0", "10", "11"].map(owner),
        ["streams-4"; 3].map(str::to_string),
        "born"
    );
    age_entries(
        &actor.state,
        &["0", "10", "11"],
        "streams-4",
        window_ms + 60_000,
    )
    .await;
    // Count balance paces itself on each rig's manual clock: its boot hold, one move cooldown.
    // Every member's passes, not only the actor's: under load a member may miss a beat in another's
    // view for a tick, so return-home there sees a share of two and gives a member both returns;
    // that member's count balance then moves one on.
    let past_cooldown = u64::try_from(move_ms).unwrap() + 1_000;
    for rig in &rigs {
        rig.clock
            .advance_monotonic(Duration::from_millis(past_cooldown));
    }
    let spread = || {
        let mut held = ["0", "10", "11"].map(owner);
        held.sort();
        held == names.map(str::to_string)
    };
    let bound = BOUND * 2;
    let t0 = tokio::time::Instant::now();
    while !spread() {
        if t0.elapsed() >= bound {
            let doc = actor.state.fleet.read_overrides().await.unwrap().0;
            let entries: Vec<(&String, &str, i64)> = doc
                .entries
                .iter()
                .map(|(p, e)| (p, e.to.as_str(), e.ms))
                .collect();
            let events: Vec<String> = doc
                .pending_events
                .iter()
                .map(|e| serde_json::to_string(e).unwrap())
                .collect();
            panic!(
                "every member holds one of the three leaves: {:?}; entries {entries:?}; events {events:?}",
                ["0", "10", "11"].map(owner)
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for rig in &rigs {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
    }
    for rig in &rigs {
        engine_shutdown(&rig.state).await;
    }
}

/// **A tick that read before the split cannot unpublish its children** (F2, T3; A1). The actor's
/// fleet tick is held between its overrides read and its publication, the pre-split view in hand,
/// and the split of `""` is started meanwhile. The tick holds the directory's revalidation lock,
/// so the actor's birth waits for it: the split publishes only after the tick published its older
/// view, and the birth's mirror follows it. Then the actor serves both children through three
/// more ticks with the same incarnations (no `possession yields` retire), its view names it for
/// both, and the member, once its revalidation installed the split, opens nothing and names the
/// actor too. Red without the lock: the held tick publishes its pre-split view after the birth's
/// mirror, and the actor retires the children it just opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_tick_that_read_before_the_split_cannot_unpublish_its_children() {
    let pair = Pair::new().await;
    let (a, b) = (&pair.a.state, &pair.b.state);
    let (reached, at_read) = tokio::sync::oneshot::channel();
    let (release, go) = tokio::sync::oneshot::channel();
    a.fleet
        .standing()
        .pause_view_publication(PausePoint { reached, go });
    tokio::time::timeout(BOUND, at_read)
        .await
        .expect("the actor's tick reads the overrides")
        .unwrap();
    let ops = a.shards.topology_ops();
    let started = crate::topology::ops::Handle::split(ops, String::new()).await;
    assert!(started.is_ok(), "the split starts: {started:?}");
    let published = || a.shards.prefixes().shards.shards == ["0", "1"];
    let t0 = tokio::time::Instant::now();
    while !published() && t0.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let progress = || a.fleet.standing().progress_age_ms().unwrap_or(u64::MAX);
    assert!(
        progress() >= 2_000,
        "a held tick marks no progress: {}",
        progress()
    );
    release.send(()).unwrap();
    let t0 = tokio::time::Instant::now();
    while !published() {
        assert!(t0.elapsed() < BOUND, "the split publishes");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t0 = tokio::time::Instant::now();
    while progress() >= 2_000 {
        assert!(t0.elapsed() < BOUND, "the released tick marks its progress");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t0 = tokio::time::Instant::now();
    while children_of(&pair.a).iter().any(Option::is_none) {
        assert!(t0.elapsed() < BOUND, "the actor opens both children");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let opened = children_of(&pair.a);
    tokio::time::sleep(Duration::from_secs(6)).await;
    let owners = |state: &crate::http::AppState| {
        ["0", "1"].map(|p| state.ownership.effective_owner(p).unwrap_or_default())
    };
    assert_eq!(
        (children_of(&pair.a), owners(a)),
        (opened, ["streams-1", "streams-1"].map(str::to_string)),
        "three ticks on, the actor serves both children as it opened them"
    );
    let t0 = tokio::time::Instant::now();
    while b.shards.prefixes().shards.version < 2 {
        assert!(t0.elapsed() < BOUND, "the member installs the split");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        (b.shards.open_count(), owners(b)),
        (0, ["streams-1", "streams-1"].map(str::to_string)),
        "the member opens nothing and names the actor"
    );
    pair.shutdown().await;
}
