#![cfg(test)]
//! A topology operation whose actor died between its publish and its terminal CAS (TLA-045's
//! `OperationCompletes`, found by its first liveness run on 2026-11-10). The publish landed, so
//! nothing opens the retired source and no claimant ever runs for the intent; it would stay live
//! for good, holding every later split and merge `409 topology_busy {reason: "intent"}` and the
//! fleet at its size. Each test builds the durable state such a death leaves (the stored topology
//! naming the operation, the intent CASed back to the `Preparing` it held before its end) and pins
//! that whoever reads it next ends it `Reclaimed`, as its actor would have: a split's start (the
//! route), a merge's, and the heartbeat's intent watch.

use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::merge_k::{Rig, halves_of_00, rig, split_00};
use super::split_k::{start_ops_on, topology_view};
use crate::topology::intent::{self, State};
use object_store::ObjectStore;
use std::time::Duration;

/// Put the cell's terminal intent back to the `Preparing` it held before its actor's end: the
/// state an actor that died after its publish leaves. Its op id.
async fn died_before_its_end(store: &dyn ObjectStore) -> String {
    let held = intent::read(store).await.unwrap().expect("an intent");
    assert_eq!(held.intent.state, State::Reclaimed, "the operation ended");
    let mut left = held.intent.clone();
    left.state = State::Preparing;
    left.outcome = None;
    intent::replace(store, &held, left).await.unwrap();
    held.intent.op_id
}

/// The cell's intent once it is terminal, within 20 s.
async fn ended(store: &dyn ObjectStore) -> intent::Intent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let held = intent::read(store).await.unwrap().expect("an intent");
        if held.intent.is_terminal() {
            return held.intent;
        }
        assert!(tokio::time::Instant::now() < deadline, "the operation ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A `POST` of `{"prefix": prefix}` to the split route: its status and JSON body.
async fn post_split(addr: std::net::SocketAddr, prefix: &str) -> (u16, serde_json::Value) {
    let body = format!(r#"{{"prefix":"{prefix}"}}"#);
    let json = [("content-type", "application/json")];
    let route = "/operator/topology/split";
    let (status, _, body) = hreq(addr, "POST", route, &json, body.as_bytes()).await;
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

/// A rig named `inst-a` over the leaves `0` and `1`, its topology stored at version 1 as boot
/// leaves it, and the topology loop running with room for seven shards.
async fn two_leaves() -> (HttpRig, crate::tasks::TaskSupervisor) {
    let options = HttpRigOptions {
        instance: Some("inst-a".into()),
        prefixes: vec!["0".to_string(), "1".to_string()],
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
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
    let tasks = start_ops_on(&rig.state, 7);
    (rig, tasks)
}

/// **A split whose actor died after its publish leaves the cell free to split again.** The route
/// splits `0` (version 2), and its intent is put back to `Preparing`: the view shows it
/// `Published`. The route's split of the other leaf `1` then answers 202, its start having ended
/// the first intent `Reclaimed`, and publishes version 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_whose_actor_died_after_its_publish_leaves_the_cell_free_to_split_again() {
    let (rig, tasks) = two_leaves().await;
    let store = rig.state.data_store.clone();
    let (status, started) = post_split(rig.addr, "0").await;
    assert_eq!(status, 202, "{started}");
    assert_eq!(ended(&*store).await.state, State::Reclaimed);
    let first = died_before_its_end(&*store).await;
    let view = topology_view(rig.addr).await;
    assert_eq!(
        (
            view["intent"]["op_id"].as_str(),
            view["intent"]["state"].as_str()
        ),
        (Some(first.as_str()), Some("Published"))
    );
    let (status, again) = post_split(rig.addr, "1").await;
    assert_eq!(status, 202, "the second split starts: {again}");
    let second = ended(&*store).await;
    assert_ne!(second.op_id, first);
    assert_eq!(second.state, State::Reclaimed);
    let (stored, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(stored.version, 3);
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// **The heartbeat's intent watch ends a published split's intent its actor left live.** With the
/// split of `00` published and its intent put back to `Preparing`, the watch reports no live
/// intent (so the heartbeat names none, reserves nothing and lets the fleet shrink), and the stored
/// intent is `Reclaimed` with the result `published`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_intent_watch_ends_a_published_split_its_actor_left_live() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    split_00(&state, &parent).await;
    let op = died_before_its_end(&*store).await;
    let mut watch = intent::Watch::default();
    assert_eq!(watch.live(&*store).await, None, "no live intent");
    let healed = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(
        (healed.op_id.as_str(), healed.state),
        (op.as_str(), State::Reclaimed)
    );
    let result = healed.outcome.map(|o| o.result);
    assert_eq!(result.as_deref(), Some("published"));
    assert_eq!(watch.live(&*store).await, None, "still none");
    engine_shutdown(&state).await;
}

/// **A merge whose actor died after its publish holds nothing either.** The split of `00` and the
/// merge of its halves both publish (version 3), and the merge's intent is put back to `Preparing`:
/// the intent watch ends it `Reclaimed`. Put back again, a split of the merged leaf starts, its
/// start ending the merge's intent first, and publishes version 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_actor_died_after_its_publish_holds_nothing() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    split_00(&state, &parent).await;
    let halves = halves_of_00(&state).await;
    let clock = state.runtime.clock.clone();
    let merged = crate::topology::merge::merge(
        &state.shards,
        [&halves[0], &halves[1]],
        "op-m",
        "inst-a",
        &*clock,
    )
    .await
    .expect("the merge publishes");
    assert_eq!(merged.version, 3);
    let op = died_before_its_end(&*store).await;
    assert_eq!(op, "op-m");
    let mut watch = intent::Watch::default();
    assert_eq!(watch.live(&*store).await, None, "no live intent");
    assert_eq!(
        intent::read(&*store).await.unwrap().unwrap().intent.state,
        State::Reclaimed
    );
    died_before_its_end(&*store).await;
    let leaf = super::merge_k::opened(&state, "00").await;
    let split = crate::topology::split::split(&state.shards, &leaf, "op-s2", "inst-a", &*clock)
        .await
        .expect("the split of the merged leaf publishes");
    assert_eq!(split.version, 4);
    assert_eq!(ended(&*store).await.op_id, "op-s2");
    engine_shutdown(&state).await;
}
