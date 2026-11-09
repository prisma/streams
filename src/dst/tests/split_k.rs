#![cfg(test)]
//! The split end to end on a serving rig (S4-DESIGN §6, K-SPLIT-DECISION-2026-11-09 §5): two
//! streams, one in each half of the leaf `00`, sealed into one segment object (one `'K'` row naming
//! both), then `00` split into `000` and `001`. Each child serves its stream back byte-exact
//! through the row it was copied (keeping only its range's entries), and the children's `'R'` rows
//! for the object sum to the parent's.

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
async fn records(addr: Addr, name: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// Each object's `'R'` row: its live chunks and bytes.
async fn refcounts(engine: &crate::shard::ShardEngine) -> BTreeMap<ObjectId, (u64, u64)> {
    let rows = engine.refcount_rows().await.unwrap();
    rows.into_iter()
        .filter_map(|(id, r)| Some((id, r.map(|r| (r.live_chunks, r.live_bytes))?)))
        .collect()
}

/// **A split's children each serve their stream back byte-exact, and their counts sum to the
/// parent's.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_serves_each_stream_from_its_child_and_the_counts_sum() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
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
    let body = |n: u32| format!(r#"[{{"n":{n},"pad":"{}"}}]"#, "x".repeat(64));
    for (name, n) in [(&low, 1), (&high, 2), (&low, 3), (&high, 4)] {
        let (status, _, _) = hreq(
            addr,
            "POST",
            &format!("/v1/stream/{name}"),
            &[JSON],
            body(n).as_bytes(),
        )
        .await;
        assert_eq!(status, 204, "append to {name}");
    }
    let desc = stored(&state, &state.deployment.raw_adapter_sref(&low)).await;
    let parent = engine_of(&state, &desc, 0).await;
    assert_eq!(parent.prefix, "00");
    parent.seal_requested().await.expect("seal");
    let before = refcounts(&parent).await;
    let shared: Vec<ObjectId> = before
        .iter()
        .filter(|(_, (chunks, _))| *chunks == 2)
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(
        shared.len(),
        1,
        "one object holds both streams' chunks: {before:?}"
    );
    let want = (records(addr, &low).await, records(addr, &high).await);

    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-k", "inst-a", &*clock)
            .await
            .expect("the split publishes");
    assert_eq!(published.version, 2);
    let prefixes: Vec<&str> = published
        .children
        .iter()
        .map(|(p, _, _)| p.as_str())
        .collect();
    assert_eq!(prefixes, ["000", "001"]);
    assert!(parent.is_closed(), "the parent is retired");

    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    let mut summed: BTreeMap<ObjectId, (u64, u64)> = BTreeMap::new();
    for (_, _, child) in &published.children {
        for (id, (chunks, bytes)) in refcounts(child).await {
            let at = summed.entry(id).or_default();
            (at.0, at.1) = (at.0 + chunks, at.1 + bytes);
        }
    }
    assert_eq!(
        summed.get(&shared[0]),
        before.get(&shared[0]),
        "the children sum to the parent"
    );
    for (_, _, child) in &published.children {
        assert_eq!(
            refcounts(child).await.get(&shared[0]).map(|c| c.0),
            Some(1),
            "{}",
            child.prefix
        );
    }
    engine_shutdown(&state).await;
}
