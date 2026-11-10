#![cfg(test)]
//! The split route's refusals that `split_k` does not pin (edge #251, EDGE-249-251-RATIFICATION
//! C3): `409 not_ring_owner {owner}` when another instance holds the parent, and `422
//! not_splittable` for both of `split::refuse`'s arms, a leaf at the routing width and a parent
//! whose log holds `'T'`. Each leaves the cell unsplit: no intent, the stored topology at version 1.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// A rig named `inst-a` over `prefixes`, its topology stored at version 1 as boot leaves it, and
/// the topology loop running with room for seven shards.
async fn rig(
    prefixes: Vec<String>,
) -> (super::fixture_http::HttpRig, crate::tasks::TaskSupervisor) {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("inst-a".into()),
            prefixes,
            ..Default::default()
        },
    )
    .await;
    let topology = crate::topology::model::Topology {
        version: 1,
        shards: rig.state.shards.prefixes().shards.clone(),
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    let store: &dyn ObjectStore = &*rig.state.data_store;
    object_store::ObjectStoreExt::put(store, &"topology.json".into(), raw.into())
        .await
        .unwrap();
    let tasks = crate::tasks::TaskSupervisor::new();
    let parts = crate::topology::ops::Parts {
        dir: rig.state.shards.clone(),
        fleet: rig.state.fleet.clone(),
        move_cooldown_ms: 600_000,
        clock: rig.state.runtime.clock.clone(),
        owner: "inst-a".to_string(),
        shards_max: 7,
    };
    crate::topology::ops::start(parts, &tasks);
    (rig, tasks)
}

/// A `POST` of `{"prefix": prefix}` to the split route: its status and JSON body.
async fn post_split(addr: std::net::SocketAddr, prefix: &str) -> (u16, serde_json::Value) {
    let body = serde_json::json!({ "prefix": prefix }).to_string();
    let json = [("content-type", "application/json")];
    let (status, _, body) = hreq(
        addr,
        "POST",
        "/operator/topology/split",
        &json,
        body.as_bytes(),
    )
    .await;
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

/// The cell was not split: no intent written, the stored topology still at version 1.
async fn unsplit(state: &crate::http::AppState) {
    let store: &dyn ObjectStore = &*state.data_store;
    assert!(
        crate::topology::intent::read(store)
            .await
            .unwrap()
            .is_none()
    );
    let (stored, _) = crate::topology::store::read(store).await.unwrap();
    assert_eq!(stored.version, 1);
}

/// This instance's open engine for `prefix`.
async fn opened(state: &crate::http::AppState, prefix: &str) -> Arc<crate::shard::ShardEngine> {
    match state
        .shards
        .open_or_wait(prefix, Duration::from_secs(10))
        .await
    {
        crate::sharddir::OpenOutcome::Ready(engine) => engine,
        _ => panic!("{prefix} did not open"),
    }
}

/// **A split of a leaf the ring gives another instance is refused naming it**: 409
/// `not_ring_owner {owner: "inst-b"}`. Operator routes are not forwarded, so the operator calls the
/// named instance; nothing is split here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_leaf_another_instance_owns_names_that_instance() {
    let (rig, tasks) = rig(vec!["0".into(), "1".into()]).await;
    // Rendezvous over "0" picks inst-b, over "1" inst-a (`ownership::ring_pick`).
    let ring = vec!["inst-a".to_string(), "inst-b".to_string()];
    rig.state.ownership.set_ring_active(ring);
    let refused = post_split(rig.addr, "0").await;
    let owner = serde_json::json!({"error": "not_ring_owner", "owner": "inst-b"});
    assert_eq!(refused, (409, owner));
    unsplit(&rig.state).await;
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// **A leaf at the routing width is not splittable**: a 24-bit leaf answers 422 `not_splittable`
/// with the message "at the routing width"; nothing is split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaf_at_the_routing_width_is_not_splittable() {
    let deepest = "0".repeat(crate::keyspace::ROUTE_PREFIX_BITS);
    let (rig, tasks) = rig(vec![deepest.clone(), "1".into()]).await;
    opened(&rig.state, &deepest).await;
    let refused = post_split(rig.addr, &deepest).await;
    let width = serde_json::json!({"error": "not_splittable", "message": "at the routing width"});
    assert_eq!(refused, (422, width));
    unsplit(&rig.state).await;
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// **A retired parent is not splittable**: a parent whose log holds `'T'` (a split already
/// retired it) answers 422 `not_splittable` with the message "a retired parent"; nothing is split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_parent_whose_log_holds_t_is_not_splittable() {
    let (rig, tasks) = rig(vec!["0".into(), "1".into()]).await;
    let parent = opened(&rig.state, "0").await;
    parent
        .db
        .put(crate::topology::fence::retired_key(), b"{}")
        .await
        .unwrap();
    let refused = post_split(rig.addr, "0").await;
    let retired = serde_json::json!({"error": "not_splittable", "message": "a retired parent"});
    assert_eq!(refused, (422, retired));
    unsplit(&rig.state).await;
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// **A leaf an override places here splits here** (edge #251 amendment R3): the ring gives `0`
/// to `inst-b`, and an override (a count-balance move) places it on `inst-a`. The split route
/// answers 202 and the split publishes `00` and `01` at version 2; the children carry no override,
/// and the parent's entry, naming no leaf now, stays until return-home drops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaf_an_override_places_here_splits_here() {
    let (rig, tasks) = rig(vec!["0".into(), "1".into()]).await;
    let ring = vec!["inst-a".to_string(), "inst-b".to_string()];
    rig.state.ownership.set_ring_active(ring);
    let moved = std::collections::HashMap::from([("0".to_string(), "inst-a".to_string())]);
    rig.state.ownership.set_overrides(moved);
    let (status, started) = post_split(rig.addr, "0").await;
    assert_eq!(status, 202, "{started}");
    let store: &dyn ObjectStore = &*rig.state.data_store;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let ended = loop {
        let held = crate::topology::intent::read(store).await.unwrap().unwrap();
        if held.intent.is_terminal() {
            break held.intent;
        }
        assert!(tokio::time::Instant::now() < deadline, "the split ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    let (stored, _) = crate::topology::store::read(store).await.unwrap();
    let leaves: Vec<&str> = stored.shards.leaves.keys().map(String::as_str).collect();
    assert_eq!((stored.version, leaves), (2, vec!["00", "01", "1"]));
    let overrides = rig.state.ownership.overrides();
    let named: Vec<&str> = overrides.keys().map(String::as_str).collect();
    assert_eq!(named, ["0"], "only the parent's entry, no child's");
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// **A leaf an override places on another instance is refused naming it** (edge #251 amendment
/// R3): the ring gives `1` to `inst-a`, and an override places it on `inst-b`. The split route
/// answers exactly 409 `not_ring_owner {owner: "inst-b"}`; nothing is split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaf_an_override_places_elsewhere_is_refused_naming_it() {
    let (rig, tasks) = rig(vec!["0".into(), "1".into()]).await;
    let ring = vec!["inst-a".to_string(), "inst-b".to_string()];
    rig.state.ownership.set_ring_active(ring);
    let moved = std::collections::HashMap::from([("1".to_string(), "inst-b".to_string())]);
    rig.state.ownership.set_overrides(moved);
    let refused = post_split(rig.addr, "1").await;
    let owner = serde_json::json!({"error": "not_ring_owner", "owner": "inst-b"});
    assert_eq!(refused, (409, owner));
    unsplit(&rig.state).await;
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}
