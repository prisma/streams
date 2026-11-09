#![cfg(test)]
//! The forward's second hop and the topology version it carries (S4-DESIGN §3.4, edge change
//! #250). A request is forwarded once to the owner the first instance's topology names; that
//! instance forwards it once more only if its own topology is strictly newer than the
//! forwarder's, and never a third time. Every `409 not_ring_owner` names the answering
//! instance's topology version, and an instance that receives a forward from a newer topology
//! revalidates its own first.
//!
//! Three instances over one store: requests arrive at A, whose topology says B owns the
//! stream; B's says C does; C serves. Each instance's version is set by installing its shard
//! set at that version, as a split's publication would.

use super::fixture_http::{engine_shutdown, http_rig_named_at};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::Arc;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;

const CT: (&str, &str) = ("content-type", "application/json");
const NAME: &str = "hops";

struct Cell {
    store: Arc<dyn object_store::ObjectStore>,
    a: (State, Addr),
    b: (State, Addr),
    c: (State, Addr),
}

/// The ring {inst-a, inst-b, inst-c} is active, and `state` gives the rig's shards to
/// `owner`.
fn owned_by(state: &State, owner: &str) {
    let members = ["inst-a", "inst-b", "inst-c"].map(str::to_string).to_vec();
    state.ownership.set_ring_active(members);
    state.ownership.set_override("00", owner);
    state.ownership.set_override("", owner);
}

/// `state`'s shard set, installed at `version`.
fn at_version(state: &State, version: u64) {
    let mut shards = state.shards.prefixes().shards.clone();
    shards.version = version;
    let now = state.runtime.clock.monotonic();
    assert!(
        state.shards.install(shards, None, now),
        "installed v{version}"
    );
}

/// A, B and C at topology versions `va`, `vb` and `vc`. A believes B owns the shards and
/// knows B's URL; B believes C does and knows C's; C owns them. The stream exists, with one
/// record.
async fn cell(va: u64, vb: u64, vc: u64) -> Cell {
    let store = mem();
    let a = http_rig_named_at(store.clone(), "inst-a", RigRuntime::first()).await;
    let b = http_rig_named_at(store.clone(), "inst-b", RigRuntime::incarnation(1)).await;
    let c = http_rig_named_at(store.clone(), "inst-c", RigRuntime::incarnation(2)).await;
    owned_by(&a.0, "inst-b");
    owned_by(&b.0, "inst-c");
    owned_by(&c.0, "inst-c");
    a.0.peer.set_peer("inst-b", &format!("http://{}", b.1));
    b.0.peer.set_peer("inst-c", &format!("http://{}", c.1));
    for (state, version) in [(&a.0, va), (&b.0, vb), (&c.0, vc)] {
        at_version(state, version);
    }
    let path = format!("/v1/stream/{NAME}");
    let (status, _, _) = hreq(c.1, "PUT", &path, &[CT], b"").await;
    assert!(matches!(status, 200 | 201), "create at the owner: {status}");
    let (status, _, _) = hreq(c.1, "POST", &path, &[CT], br#"[{"n":1}]"#).await;
    assert_eq!(status, 204, "append at the owner");
    Cell { store, a, b, c }
}

/// An append of `{"n":2}` that arrives at A. Appends go through the forward (a read takes
/// its peer's span itself).
async fn append_at_a(cell: &Cell) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    let path = format!("/v1/stream/{NAME}");
    hreq(cell.a.1, "POST", &path, &[CT], br#"[{"n":2}]"#).await
}

/// The records C holds, read at C itself (never forwarded).
async fn records_at_c(cell: &Cell) -> serde_json::Value {
    let path = format!("/v1/stream/{NAME}");
    let opt_out = ("streams-forwarded", "test");
    let (status, _, body) = hreq(cell.c.1, "GET", &path, &[opt_out], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

async fn shutdown(cell: Cell) {
    for state in [&cell.a.0, &cell.b.0, &cell.c.0] {
        engine_shutdown(state).await;
    }
}

/// **A request through a split takes exactly two hops.** A forwards to B at version 1; B,
/// at version 2, forwards it once more to C, which serves it: the append lands at C. Before:
/// B answered its `409`, a forwarded request being never forwarded again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_request_through_a_split_takes_two_hops_to_its_owner() {
    let cell = cell(1, 2, 2).await;
    let (status, headers, _) = append_at_a(&cell).await;
    assert_eq!(
        (
            status,
            headers.get("streams-forwarded-to").map(String::as_str)
        ),
        (204, Some("inst-b")),
        "the append served by C through B"
    );
    assert_eq!(
        records_at_c(&cell).await,
        serde_json::json!([{"n": 1}, {"n": 2}])
    );
    shutdown(cell).await;
}

/// **Between equal topologies there is one hop, and the `409` names the version.** A and B
/// at version 1: B answers its `409 not_ring_owner` naming C, with
/// `Streams-Topology-Version: 1`, and A relays it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn between_equal_topologies_one_hop_answers_the_owners_409_with_its_version() {
    let cell = cell(1, 1, 1).await;
    let (status, headers, _) = append_at_a(&cell).await;
    assert_eq!(
        (
            status,
            headers.get("streams-replay-to").map(String::as_str),
            headers.get("streams-topology-version").map(String::as_str),
        ),
        (409, Some("inst-c"), Some("1"))
    );
    shutdown(cell).await;
}

/// **The second hop is the last.** A at 1, B at 2 and C at 3, where C believes A owns the
/// shards and knows A's URL: C answers its `409` (version 3) rather than forward a third
/// time, and the answer travels back through B and A.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_second_hop_is_never_forwarded_again() {
    let cell = cell(1, 2, 3).await;
    owned_by(&cell.c.0, "inst-a");
    cell.c
        .0
        .peer
        .set_peer("inst-a", &format!("http://{}", cell.a.1));
    let (status, headers, _) = append_at_a(&cell).await;
    assert_eq!(
        (
            status,
            headers.get("streams-replay-to").map(String::as_str),
            headers.get("streams-topology-version").map(String::as_str),
        ),
        (409, Some("inst-a"), Some("3"))
    );
    shutdown(cell).await;
}

/// **A forward from a newer topology makes its receiver revalidate first.** The store holds
/// version 2; A routes by it, B still by version 1. A's forward names version 2, so B reads
/// the stored topology before it routes: B is at version 2 after the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_forward_from_a_newer_topology_makes_its_receiver_revalidate() {
    let cell = cell(2, 1, 2).await;
    let mut stored = crate::config::validation::InitialShards::new(1)
        .unwrap()
        .topology(1 << 20);
    stored.version = 2;
    stored.shards = cell.b.0.shards.prefixes().shards.clone();
    stored.shards.version = 2;
    let raw = serde_json::to_vec(&stored).unwrap();
    let path = object_store::path::Path::from("topology.json");
    object_store::ObjectStoreExt::put(&*cell.store, &path, raw.into())
        .await
        .unwrap();
    let _ = append_at_a(&cell).await;
    assert_eq!(
        cell.b.0.shards.prefixes().shards.version,
        2,
        "B revalidated"
    );
    shutdown(cell).await;
}
