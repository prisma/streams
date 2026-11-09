#![cfg(test)]
//! The open-path fence (S4-DESIGN §6.3) on a serving rig: an engine whose shard a live topology
//! intent names as a split's source does not serve, reads included, until the intent ends; one
//! the intent does not name serves at once.

use super::billing_closure_debts::stored;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use crate::topology::intent::{self, Intent, State};
use std::time::{Duration, Instant};

type App = std::sync::Arc<crate::http::AppState>;

const CT: (&str, &str) = ("content-type", "application/json");
const BOUND: Duration = Duration::from_secs(10);

/// A live split of `sources`.
fn split_of(sources: Vec<(String, u32)>) -> Intent {
    Intent {
        kind: "split".to_string(),
        op_id: "op-fence".to_string(),
        sources,
        children: vec![90, 91],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-elsewhere".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    }
}

/// A create of `name` and its status.
async fn create(addr: std::net::SocketAddr, name: &str) -> u16 {
    let path = format!("/v1/stream/{name}");
    hreq(addr, "PUT", &path, &[CT], b"").await.0
}

/// An append of one record to `name` and its status.
async fn append(addr: std::net::SocketAddr, name: &str) -> u16 {
    let path = format!("/v1/stream/{name}");
    hreq(addr, "POST", &path, &[CT], br#"[{"n":1}]"#).await.0
}

/// A stream of the rig in `prefix`'s shard: created (which opens its shard) until one routes there.
async fn stream_in(state: &App, addr: std::net::SocketAddr, prefix: &str) -> String {
    for i in 0..64 {
        let name = format!("fenced-{i}");
        assert!(matches!(create(addr, &name).await, 200 | 201));
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        if state
            .shards
            .prefix_for(&desc.resolve_segment("").shard_route)
            == prefix
        {
            return name;
        }
    }
    panic!("no stream of 64 routes to {prefix:?}");
}

/// **An open a live intent names as a source holds until the intent ends.** A stream of the
/// cell's one leaf shard is created and its engine retired. A live split then names that shard:
/// the stream's append, which reopens it, is refused retryably, and so is a read, and no engine is
/// published. Once the intent is rolled back, the append is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_open_a_live_intent_names_holds_until_the_intent_ends() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let leaves = state.shards.prefixes().shards.leaves.clone();
    let (prefix, _) = leaves.iter().next().expect("a leaf");
    let name = stream_in(&state, addr, prefix).await;
    engine_shutdown(&state).await;
    let live = intent::create(&*store, &split_of(leaves.clone().into_iter().collect()))
        .await
        .unwrap();
    assert_eq!(
        append(addr, &name).await,
        503,
        "the append waits on the held open"
    );
    let (status, _, _) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 503, "a read waits too");
    assert!(
        state.shards.open(prefix).is_none(),
        "the held engine is not published"
    );
    let mut ended = live.intent.clone();
    ended.state = State::RolledBack;
    intent::replace(&*store, &live, ended).await.unwrap();
    let t0 = Instant::now();
    loop {
        let status = append(addr, &name).await;
        if status == 204 {
            break;
        }
        assert!(
            t0.elapsed() < BOUND,
            "served once the intent ended: {status}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    engine_shutdown(&state).await;
}

/// **An intent that names another incarnation holds nothing.** A stream of the cell's leaf shard,
/// its engine retired; a live split names that prefix under another home, and another prefix
/// under that home: the reopen serves at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_intent_naming_another_incarnation_holds_nothing() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let leaves = state.shards.prefixes().shards.leaves.clone();
    let (prefix, home) = leaves.iter().next().expect("a leaf");
    let name = stream_in(&state, addr, prefix).await;
    engine_shutdown(&state).await;
    let elsewhere = vec![(prefix.clone(), home + 99), ("1111".to_string(), *home)];
    intent::create(&*store, &split_of(elsewhere)).await.unwrap();
    assert_eq!(append(addr, &name).await, 204, "served at once");
    engine_shutdown(&state).await;
}

/// **A shard log holding `'T'` does not serve before the topology that retires it.** The leaf
/// shard's log is given a retired-parent row for version 2 while the cell is at version 1, with
/// no intent: its reopen holds (the claimant rolls it forward, S4 step 11), reads included, and
/// no engine is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_log_holding_t_does_not_serve_before_the_topology_that_retires_it() {
    let (state, addr) = http_rig(mem()).await;
    let leaves = state.shards.prefixes().shards.leaves.clone();
    let (prefix, _) = leaves.iter().next().expect("a leaf");
    let name = stream_in(&state, addr, prefix).await;
    let engine = state.shards.open(prefix).expect("its engine");
    let retired = crate::topology::fence::Retired {
        version: 2,
        op_id: "op-t".to_string(),
        children: vec![(format!("{prefix}0"), 90), (format!("{prefix}1"), 91)],
    };
    let row = serde_json::to_vec(&retired).unwrap();
    engine
        .db
        .put(crate::topology::fence::retired_key(), row)
        .await
        .unwrap();
    engine.db.flush().await.unwrap();
    drop(engine);
    engine_shutdown(&state).await;
    assert_eq!(
        append(addr, &name).await,
        503,
        "the append waits on the held open"
    );
    let (status, _, _) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 503, "a read waits too");
    assert!(
        state.shards.open(prefix).is_none(),
        "the held engine is not published"
    );
}
