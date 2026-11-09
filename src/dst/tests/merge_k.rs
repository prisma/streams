#![cfg(test)]
//! The merge end to end on a serving rig (S6-DESIGN rev 6 §7): the leaf `00` is split into `000`
//! and `001` (S4's split), each child holding its half of one shared segment object; then the two
//! children are merged back into `00` at a fresh home. The merged leaf serves both streams back
//! byte-exact, its `'R'` row for the shared object is the children's sum (the parent's), both
//! sources are retired, and `M`'s reference lists exist under the roots of the homes whose objects
//! it holds before it becomes a leaf (L5).

use super::billing_closure_debts::{JSON, engine_of, raw_put, stored};
use super::fixture_http::{engine_shutdown, http_rig, http_rig_named};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::spill::object::ObjectId;
use std::collections::BTreeMap;
use std::sync::Arc;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;

/// A stream created in the half `half` of the leaf `00` (its route's third bit): its name.
async fn stream_in_half(state: &State, addr: Addr, half: char, taken: &mut usize) -> String {
    loop {
        *taken += 1;
        assert!(*taken < 400, "no stream lands in 00{half}");
        let name = format!("m-{taken}");
        raw_put(addr, &name, &[JSON], b"").await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        let route = desc.resolve_segment("").shard_route;
        let third = (route[0] >> 5) & 1;
        if state.shards.prefix_for(&route) == "00" && third == u8::from(half == '1') {
            return name;
        }
    }
}

async fn records(addr: Addr, name: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

async fn refcounts(engine: &crate::shard::ShardEngine) -> BTreeMap<ObjectId, (u64, u64)> {
    let rows = engine.refcount_rows().await.unwrap();
    rows.into_iter()
        .filter_map(|(id, r)| Some((id, r.map(|r| (r.live_chunks, r.live_bytes))?)))
        .collect()
}

/// A rig whose stored topology is its own (version 1), holding `low` in `000`'s half and `high` in
/// `001`'s, each with two records sealed into one shared segment object of the leaf `00`.
struct Rig {
    store: Arc<dyn object_store::ObjectStore>,
    state: State,
    addr: Addr,
    low: String,
    high: String,
    parent: Arc<crate::shard::ShardEngine>,
}

async fn rig() -> Rig {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
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
    let body = |n: u32| format!(r#"[{{"n":{n},"pad":"{}"}}]"#, "x".repeat(64));
    for (name, n) in [(&low, 1), (&high, 2), (&low, 3), (&high, 4)] {
        let path = format!("/v1/stream/{name}");
        let (status, _, _) = hreq(addr, "POST", &path, &[JSON], body(n).as_bytes()).await;
        assert_eq!(status, 204, "append to {name}");
    }
    let desc = stored(&state, &state.deployment.raw_adapter_sref(&low)).await;
    let parent = engine_of(&state, &desc, 0).await;
    parent.seal_requested().await.expect("seal");
    Rig {
        store,
        state,
        addr,
        low,
        high,
        parent,
    }
}

/// **A merge of a split's two children serves both streams from one leaf, with the parent's
/// counts, and lists what it holds under each ancestor before it is a leaf.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_of_two_children_serves_both_streams_with_the_parents_counts() {
    let Rig {
        store,
        state,
        addr,
        low,
        high,
        parent,
    } = rig().await;
    let before = refcounts(&parent).await;
    let want = (records(addr, &low).await, records(addr, &high).await);
    let parent_home = state.shards.prefixes().shards.leaves["00"];

    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, "op-s", "inst-a", &*clock)
        .await
        .expect("the split publishes");
    let children: Vec<Arc<crate::shard::ShardEngine>> =
        split.children.iter().map(|(_, _, e)| e.clone()).collect();
    let child_homes: Vec<u32> = split.children.iter().map(|(_, h, _)| *h).collect();

    let merged = crate::topology::merge::merge(
        &state.shards,
        [&children[0], &children[1]],
        "op-m",
        "inst-a",
        &*clock,
    )
    .await
    .expect("the merge publishes");
    assert_eq!(merged.version, split.version + 1);
    let (prefix, home, engine) = &merged.merged;
    assert_eq!(prefix, "00");
    assert!(
        !child_homes.contains(home) && *home != parent_home,
        "a fresh home"
    );
    assert!(
        children.iter().all(|c| c.is_closed()),
        "both sources are retired"
    );
    let installed = state.shards.prefixes();
    assert_eq!(installed.shards.leaves.get("00"), Some(home));
    assert!(!installed.shards.leaves.contains_key("000"));
    assert_eq!(installed.shards.homes[home].parents, child_homes);

    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    assert_eq!(
        refcounts(engine).await,
        before,
        "M's counts are the parent's"
    );
    for ancestor in [parent_home] {
        let root = installed.shards.homes[&ancestor].path.clone();
        let list = crate::spill::lineage::reference_list_path(&root, *home);
        let got = object_store::ObjectStoreExt::get(&*store, &list.as_str().into()).await;
        assert!(got.is_ok(), "M's list under home {ancestor}: {list}");
    }
    engine_shutdown(&state).await;
}

/// **The merge route starts a merge that runs on to its publish** (S6 §7, edge #251). After a
/// split of `00`, `POST /operator/topology/merge {"prefix":"00"}` answers 202 with the merge's
/// operation and the version it saw; its intent ends `Reclaimed`, the stored topology is the next
/// version with `last_op` the merge, both streams read back byte-exact from the one leaf `00`, and
/// the view shows that leaf born at the merge's version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_merge_route_starts_a_merge_that_runs_on_to_its_publish() {
    let Rig {
        store,
        state,
        addr,
        low,
        high,
        parent,
    } = rig().await;
    let want = (records(addr, &low).await, records(addr, &high).await);
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, "op-s", "inst-a", &*clock)
        .await
        .expect("the split publishes");
    let tasks = super::split_k::start_ops_on(&state, 7);
    let route = "/operator/topology/merge";
    let (status, _, body) = hreq(addr, "POST", route, &[JSON], br#"{"prefix":"00"}"#).await;
    let started: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status, 202, "{started}");
    assert_eq!(started["version"], split.version);
    let op_id = started["op_id"].as_str().unwrap().to_string();
    let t0 = std::time::Instant::now();
    let ended = loop {
        let held = crate::topology::intent::read(&*store)
            .await
            .unwrap()
            .unwrap();
        if held.intent.op_id == op_id && held.intent.is_terminal() {
            break held.intent;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(20),
            "the merge ends"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(ended.kind, "merge");
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    let (topology, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(topology.version, split.version + 1);
    assert_eq!(topology.last_op.map(|op| op.op_id), Some(op_id));
    assert_eq!(topology.shards.leaves.get("00"), Some(&ended.children[0]));
    assert!(!topology.shards.leaves.contains_key("000"));
    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    let view = super::split_k::topology_view(addr).await;
    let leaves: Vec<(String, serde_json::Value)> = view["shards"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["prefix"].as_str().is_some_and(|p| p.starts_with("00")))
        .map(|s| {
            (
                s["prefix"].as_str().unwrap().to_string(),
                s["born_version"].clone(),
            )
        })
        .collect();
    assert_eq!(leaves, [("00".to_string(), (split.version + 1).into())]);
    assert_eq!(view["intent"]["state"], "Reclaimed");
    assert_eq!(view["intent"]["outcome"]["result"], "published");
    tasks.cancel();
    engine_shutdown(&state).await;
}

/// **The merge route's refusals** (S6 §7, edge #251). Before its loop runs: 503
/// `topology_unavailable`. Then: a body that is not `{"prefix"}`, 400 `bad_request`; a prefix whose
/// halves are not both leaves, 404 `unknown_prefix`; after the split, an override on a half, 409
/// `topology_busy {reason: "override"}`; a live intent, 409 `topology_busy {reason: "intent"}`.
/// Nothing was merged: the stored topology stays at the split's version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_merge_routes_refusals() {
    let Rig {
        store,
        state,
        addr,
        parent,
        ..
    } = rig().await;
    let post = async |body: &str| {
        let route = "/operator/topology/merge";
        let (status, _, body) = hreq(addr, "POST", route, &[JSON], body.as_bytes()).await;
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let error = body["error"].as_str().unwrap_or_default().to_string();
        (status, error, body["reason"].clone())
    };
    let halves = r#"{"prefix":"00"}"#;
    let none = serde_json::Value::Null;
    assert_eq!(
        post(halves).await,
        (503, "topology_unavailable".into(), none.clone())
    );
    let tasks = super::split_k::start_ops_on(&state, 7);
    assert_eq!(post("{}").await, (400, "bad_request".into(), none.clone()));
    assert_eq!(
        post(halves).await,
        (404, "unknown_prefix".into(), none.clone())
    );
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, "op-s", "inst-a", &*clock)
        .await
        .expect("the split publishes");
    let overrides = std::collections::HashMap::from([("001".to_string(), "inst-b".to_string())]);
    state.ownership.set_overrides(overrides);
    let busy = |reason: &str| (409, "topology_busy".to_string(), reason.into());
    assert_eq!(post(halves).await, busy("override"));
    state
        .ownership
        .set_overrides(std::collections::HashMap::new());
    let live = crate::topology::intent::Intent {
        kind: "split".to_string(),
        op_id: "op-other".to_string(),
        sources: vec![("1".to_string(), 9)],
        children: vec![90, 91],
        version_seen: split.version,
        state: crate::topology::intent::State::Preparing,
        lease_owner: "inst-b".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    crate::topology::intent::create(&*store, &live)
        .await
        .unwrap();
    assert_eq!(post(halves).await, busy("intent"));
    let (topology, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(topology.version, split.version);
    tasks.cancel();
    engine_shutdown(&state).await;
}

/// A `POST` of `body` to the merge route: its status and JSON body.
async fn post_merge(addr: Addr, body: &str) -> (u16, serde_json::Value) {
    let route = "/operator/topology/merge";
    let (status, _, body) = hreq(addr, "POST", route, &[JSON], body.as_bytes()).await;
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

/// Split the leaf `00` of `state` (whose stored topology is version 1) with `parent`, its engine.
async fn split_00(state: &State, parent: &Arc<crate::shard::ShardEngine>) -> u64 {
    let clock = state.runtime.clock.clone();
    crate::topology::split::split(&state.shards, parent, "op-s", "inst-a", &*clock)
        .await
        .expect("the split publishes")
        .version
}

/// Write `state`'s own topology as the stored version 1.
async fn store_topology(state: &State) {
    let topology = crate::topology::model::Topology {
        version: 1,
        shards: state.shards.prefixes().shards.clone(),
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    object_store::ObjectStoreExt::put(&*state.data_store, &"topology.json".into(), raw.into())
        .await
        .unwrap();
}

/// Whether nothing was merged: the stored topology is still at `version` and the cell's intent is
/// the split's.
async fn nothing_merged(store: &dyn object_store::ObjectStore, version: u64) {
    let (topology, _) = crate::topology::store::read(store).await.unwrap();
    assert_eq!(topology.version, version);
    let held = crate::topology::intent::read(store).await.unwrap().unwrap();
    assert_eq!(held.intent.kind, "split", "no merge intent was written");
}

/// **A merge that would fill its instance past `SHARDS_PER_INSTANCE_MAX` is refused** (S6 §7,
/// edge #251). The actor holds both halves and `M` until the publish, so with a ceiling of 1 the
/// merge answers 409 `instance_full` naming the shards it would leave resident (those open here,
/// both halves among them, plus one) and the ceiling; nothing is merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_that_would_fill_its_instance_is_refused() {
    let Rig {
        store,
        state,
        addr,
        parent,
        ..
    } = rig().await;
    let version = split_00(&state, &parent).await;
    let tasks = super::split_k::start_ops_on(&state, 1);
    let (status, refused) = post_merge(addr, r#"{"prefix":"00"}"#).await;
    let resident = state.shards.open_count() + 1;
    assert!(resident >= 3, "both halves and M: {resident}");
    assert_eq!(
        (status, refused),
        (
            409,
            serde_json::json!({"error": "instance_full", "resident": resident, "max": 1})
        )
    );
    nothing_merged(&*store, version).await;
    tasks.cancel();
    engine_shutdown(&state).await;
}

/// **A merge waits for the fleet to settle** (S4 §10.1 rule 1, S6 §7, edge #251). On a fleet cell
/// whose published count is 10 s old, the merge route answers 409 `topology_busy {reason:
/// "fleet"}`; with the count 61 s old but an override (of another prefix) published 10 s ago, the
/// same; with the override past the 600 s move cooldown too, the merge starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_waits_for_the_fleet_to_settle() {
    use super::fixture_http::{HttpRigOptions, http_rig_build};
    let fleet = mem();
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    store_topology(&rig.state).await;
    let parent = rig.state.engine_for_quiet(&[0; 16]).await.unwrap();
    assert_eq!(parent.prefix, "00");
    split_00(&rig.state, &parent).await;
    let tasks = super::split_k::start_ops_on(&rig.state, 7);
    let put = |path: &'static str, doc: serde_json::Value| {
        let fleet = fleet.clone();
        async move {
            let raw = serde_json::to_vec(&doc).unwrap();
            object_store::ObjectStoreExt::put(&*fleet, &path.into(), raw.into())
                .await
                .unwrap();
        }
    };
    let now = crate::shard::now_ms();
    let desired = |age_ms: i64| serde_json::json!({"count": 1, "reason": "t", "epoch": 1, "computed_at_ms": now - age_ms});
    let overrides = |age_ms: i64| serde_json::json!({"entries": {"1111": {"to": "streams-1", "ms": now - age_ms}}});
    let busy = (
        409,
        serde_json::json!({"error": "topology_busy", "reason": "fleet"}),
    );
    let body = r#"{"prefix":"00"}"#;
    put("fleet/desired.json", desired(10_000)).await;
    assert_eq!(post_merge(rig.addr, body).await, busy, "a young count");
    put("fleet/desired.json", desired(61_000)).await;
    put("fleet/overrides.json", overrides(10_000)).await;
    assert_eq!(post_merge(rig.addr, body).await, busy, "a young override");
    put("fleet/overrides.json", overrides(601_000)).await;
    let (status, started) = post_merge(rig.addr, body).await;
    assert_eq!(status, 202, "settled: {started}");
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// **A merge of a half another instance holds is refused** (S6 §7, edge #251): both halves must be
/// this instance's. With the ring naming only `inst-b`, the merge route on `inst-a` answers 409
/// `not_ring_owner {owner: "inst-b"}`; nothing is merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_of_halves_another_instance_holds_is_refused() {
    let (state, addr) = http_rig_named(mem(), "inst-a").await;
    store_topology(&state).await;
    let parent = state.engine_for_quiet(&[0; 16]).await.unwrap();
    let version = split_00(&state, &parent).await;
    let tasks = super::split_k::start_ops_on(&state, 7);
    state
        .ownership
        .set_view(vec!["inst-b".to_string()], std::collections::HashMap::new());
    assert_eq!(
        post_merge(addr, r#"{"prefix":"00"}"#).await,
        (
            409,
            serde_json::json!({"error": "not_ring_owner", "owner": "inst-b"})
        )
    );
    nothing_merged(&*state.data_store, version).await;
    tasks.cancel();
    engine_shutdown(&state).await;
}

/// **A merge of a retired half is refused** (S6 §7.2, edge #251): a half holding `'T'` is not
/// mergeable, so the route answers 422 `not_mergeable`; nothing is merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_of_a_retired_half_is_refused() {
    let Rig {
        store,
        state,
        addr,
        parent,
        ..
    } = rig().await;
    let version = split_00(&state, &parent).await;
    let tasks = super::split_k::start_ops_on(&state, 7);
    let half = state.shards.open("001").expect("001 is open here");
    half.db
        .put(crate::topology::fence::retired_key(), b"{}")
        .await
        .unwrap();
    let (status, refused) = post_merge(addr, r#"{"prefix":"00"}"#).await;
    assert_eq!(
        (status, &refused["error"]),
        (422, &"not_mergeable".into()),
        "{refused}"
    );
    nothing_merged(&*store, version).await;
    tasks.cancel();
    engine_shutdown(&state).await;
}

/// The log at `path`'s checkpoints named `name`.
async fn checkpoints(store: &Arc<dyn object_store::ObjectStore>, path: &str, name: &str) -> usize {
    let admin = slatedb::admin::AdminBuilder::new(path, store.clone()).build();
    admin.list_checkpoints(Some(name)).await.unwrap().len()
}

/// Whether nothing lies at or under `path`.
async fn gone(store: &dyn object_store::ObjectStore, path: &str) -> bool {
    use futures_util::TryStreamExt;
    let at = object_store::path::Path::from(path);
    let head = object_store::ObjectStoreExt::head(store, &at).await;
    let under: Vec<_> = store.list(Some(&at)).try_collect().await.unwrap();
    matches!(head, Err(object_store::Error::NotFound { .. })) && under.is_empty()
}

/// **A lapsed merge is rolled back by a source's opener, which fences both sources first.** An
/// actor that stopped in the merge's copy left a `topo-` checkpoint on each source, `M`'s lists
/// under both sources' roots, files at `M`'s path, and a live merge intent no one renews. The
/// reopen of `00` holds through the lapse; its opener opens `01`'s log for maintenance too (so
/// `01`'s previous writer is fenced), claims the intent, deletes what the actor left on both
/// sources, ends the intent `RolledBack`, and serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_lapsed_merge_is_rolled_back_by_a_sources_opener_that_fences_both_sources() {
    use crate::topology::intent::{self, Intent, State};
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
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
    let desc = stored(&state, &state.deployment.raw_adapter_sref(&low)).await;
    let parent = engine_of(&state, &desc, 0).await;
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, "op-s", "inst-a", &*clock)
        .await
        .expect("the split publishes");
    let snapshot = state.shards.prefixes();
    let homes = [snapshot.shards.leaves["000"], snapshot.shards.leaves["001"]];
    let merged_home = snapshot.shards.next_home;
    let mut logs = Vec::new();
    let mut lists = Vec::new();
    for (_, home, engine) in &split.children {
        let options = slatedb::config::CheckpointOptions {
            lifetime: Some(std::time::Duration::from_secs(3600)),
            source: None,
            name: Some("topo-op-merge".to_string()),
        };
        engine
            .db
            .create_checkpoint(slatedb::config::CheckpointScope::All, &options)
            .await
            .unwrap();
        logs.push(engine.log_path());
        let root = snapshot.shards.homes[home].path.clone();
        lists.push(crate::spill::lineage::reference_list_path(
            &root,
            merged_home,
        ));
    }
    let child = crate::topology::model::born_home_path("00", merged_home);
    let file = format!("{child}/manifest/00000000000000000001.manifest");
    for path in lists.iter().chain([&file]) {
        object_store::ObjectStoreExt::put(&*store, &path.as_str().into(), b"left".to_vec().into())
            .await
            .unwrap();
    }
    engine_shutdown(&state).await;
    let merge = Intent {
        kind: "merge".to_string(),
        op_id: "op-merge".to_string(),
        sources: vec![("000".to_string(), homes[0]), ("001".to_string(), homes[1])],
        children: vec![merged_home],
        version_seen: split.version,
        state: State::Preparing,
        lease_owner: "inst-elsewhere".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    intent::create(&*store, &merge).await.unwrap();
    let t0 = tokio::time::Instant::now();
    loop {
        let path = format!("/v1/stream/{low}");
        let (status, _, _) = hreq(addr, "POST", &path, &[JSON], br#"[{"n":9}]"#).await;
        if status == 204 {
            break;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(40),
            "served once rolled back"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let ended = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(ended.state, State::RolledBack);
    assert_eq!(ended.lease_epoch, 2, "claimed");
    for log in &logs {
        assert_eq!(
            checkpoints(&store, log, "topo-op-merge").await,
            0,
            "{log}'s checkpoint"
        );
    }
    for path in lists.iter().chain([&child]) {
        assert!(gone(&*store, path).await, "{path} is deleted");
    }
    engine_shutdown(&state).await;
}
