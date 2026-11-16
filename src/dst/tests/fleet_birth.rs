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

/// A member `name` of the cell over `data` and `fleet`, its fleet started, once its view names
/// both members.
async fn member(
    fleet: &Arc<dyn ObjectStore>,
    data: &Arc<dyn ObjectStore>,
    name: &str,
    runtime: RigRuntime,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        runtime,
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some(name.to_string()),
            cli: two_members,
            prefixes: vec![String::new()],
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

/// The cell: `streams-1` (the split's actor, the ring's owner of `""`) and `streams-2` (the ring's
/// owner of both halves), each a peer of the other, once both views name both members.
pub(super) struct Pair {
    pub(super) a: HttpRig,
    pub(super) b: HttpRig,
}

impl Pair {
    pub(super) async fn new() -> Self {
        let fleet = mem();
        let desired = r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
        fleet
            .put(&Path::from("fleet/desired.json"), PutPayload::from(desired))
            .await
            .unwrap();
        let data = mem();
        let a = member(&fleet, &data, "streams-1", RigRuntime::first()).await;
        store_topology(&a).await;
        let b = member(&fleet, &data, "streams-2", RigRuntime::incarnation(1)).await;
        for rig in [&a, &b] {
            let peer = &rig.state.peer;
            peer.set_peer("streams-1", &format!("http://{}", a.addr));
            peer.set_peer("streams-2", &format!("http://{}", b.addr));
        }
        let t0 = tokio::time::Instant::now();
        while [&a, &b]
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
        Self { a, b }
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
