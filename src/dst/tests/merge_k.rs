#![cfg(test)]
//! The merge end to end on a serving rig (S6-DESIGN rev 6 §7): the leaf `00` is split into `000`
//! and `001` (S4's split), each child holding its half of one shared segment object; then the two
//! children are merged back into `00` at a fresh home. The merged leaf serves both streams back
//! byte-exact, its `'R'` row for the shared object is the children's sum (the parent's), both
//! sources are retired, and `M`'s reference lists exist under the roots of the homes whose objects
//! it holds before it becomes a leaf (L5).

use super::billing_closure_debts::{JSON, engine_of, raw_put, stored};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
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

/// **A merge of a split's two children serves both streams from one leaf, with the parent's
/// counts, and lists what it holds under each ancestor before it is a leaf.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_of_two_children_serves_both_streams_with_the_parents_counts() {
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
