#![cfg(test)]
//! The split DSTs' cell (`split_k` and its siblings): a rig whose stored topology is its own, with a
//! stream in each half of the leaf `00` holding two records each, and the leaf's engine.

use super::billing_closure_debts::{JSON, engine_of, raw_put, stored};
use super::fixture_http::{http_rig_two_bit, http_rig_two_bit_cold};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use std::sync::Arc;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;

/// A stream created in the half `half` of the leaf `00` (its route's third bit): its name.
async fn stream_in_half(state: &State, addr: Addr, half: char, taken: &mut usize) -> String {
    loop {
        *taken += 1;
        assert!(*taken < 400, "no stream lands in 00{half}");
        let name = format!("k-{taken}");
        raw_put(addr, &name, &[JSON], b"").await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        let route = desc.resolve_segment("").shard_route;
        let third = (route[0] >> 5) & 1;
        if state.shards.prefix_for(&route) == "00" && third == u8::from(half == '1') {
            return name;
        }
    }
}

/// The records of `name`, read back.
pub(super) async fn records(addr: Addr, name: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// A rig whose stored topology is its own, with a stream in each half of the leaf `00` holding two
/// records each, and the leaf's engine.
pub(super) struct Cell {
    pub(super) state: State,
    pub(super) addr: Addr,
    pub(super) low: String,
    pub(super) high: String,
    pub(super) parent: Arc<crate::shard::ShardEngine>,
}

/// One record of `n`, padded.
pub(super) fn body(n: u32) -> String {
    format!(r#"[{{"n":{n},"pad":"{}"}}]"#, "x".repeat(64))
}

/// An append of `{"n": n}` to `name`: its status and the next offset it reports.
pub(super) async fn append(addr: Addr, name: &str, n: u32) -> (u16, Option<String>) {
    let path = format!("/v1/stream/{name}");
    let (status, headers, _) = hreq(addr, "POST", &path, &[JSON], body(n).as_bytes()).await;
    (status, headers.get("stream-next-offset").cloned())
}

pub(super) async fn cell() -> Cell {
    cell_over(mem()).await
}

/// `cell`, over `store`.
pub(super) async fn cell_over(store: Arc<dyn object_store::ObjectStore>) -> Cell {
    cell_at(store, super::fixture_runtime::RigRuntime::first()).await
}

/// `cell` whose sealers never seal on their own: only a requested seal runs (the park probes).
pub(super) async fn cold_cell() -> Cell {
    let store = mem();
    let runtime = super::fixture_runtime::RigRuntime::first();
    let rigged = http_rig_two_bit_cold(store.clone(), None, runtime).await;
    cell_of(store, rigged).await
}

/// `cell_over`, on `runtime` (whose manual clock the test may drive).
pub(super) async fn cell_at(
    store: Arc<dyn object_store::ObjectStore>,
    runtime: super::fixture_runtime::RigRuntime,
) -> Cell {
    let rigged = http_rig_two_bit(store.clone(), None, runtime).await;
    cell_of(store, rigged).await
}

/// The cell `rigged` over `store`: its topology stored, a stream in each half of `00`.
async fn cell_of(store: Arc<dyn object_store::ObjectStore>, rigged: (State, Addr)) -> Cell {
    let (state, addr) = rigged;
    // The cell's stored topology, as boot leaves it: the rig's own.
    let topology = crate::topology::model::Topology {
        version: 1,
        shards: state.shards.prefixes().shards.clone(),
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), raw.into())
        .await
        .unwrap();
    let mut taken = 0;
    let low = stream_in_half(&state, addr, '0', &mut taken).await;
    let high = stream_in_half(&state, addr, '1', &mut taken).await;
    for (name, n) in [(&low, 1), (&high, 2), (&low, 3), (&high, 4)] {
        assert_eq!(append(addr, name, n).await.0, 204, "append to {name}");
    }
    let desc = stored(&state, &state.deployment.raw_adapter_sref(&low)).await;
    let parent = engine_of(&state, &desc, 0).await;
    assert_eq!(parent.prefix, "00");
    Cell {
        state,
        addr,
        low,
        high,
        parent,
    }
}
