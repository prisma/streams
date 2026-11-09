#![cfg(test)]
//! The open-path fence (S4-DESIGN §6.3) on a serving rig: an engine whose shard a live topology
//! intent names as a split's source does not serve, reads included, until the intent ends; one
//! the intent does not name serves at once. An intent whose lease lapsed is claimed by the opener,
//! which rolls the split back and serves, or forward from `'T'` and never serves (§6.4).

use super::billing_closure_debts::stored;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use crate::topology::intent::{self, Intent, State};
use std::sync::Arc;
use std::time::{Duration, Instant};

type App = std::sync::Arc<crate::http::AppState>;

const CT: (&str, &str) = ("content-type", "application/json");
const BOUND: Duration = Duration::from_secs(10);
/// How long a claim may take: the lapse (`intent::LEASE` + `intent::SKEW`, 11 s) plus the fence's
/// re-read and the requests' own patience.
const CLAIM_BOUND: Duration = Duration::from_secs(40);

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

/// A live actor's renewals of `held`: a CAS of its `lease_until_ms` every 2 s (an actor renews every
/// 3.3 s), so no opener sees it lapse, until `stop` fires; then its last write.
async fn renew_until(
    store: &dyn object_store::ObjectStore,
    mut held: intent::Held,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> intent::Held {
    loop {
        tokio::select! {
            _ = &mut stop => return held,
            () = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
        let mut next = held.intent.clone();
        next.lease_until_ms += 1;
        held = intent::replace(store, &held, next).await.unwrap();
    }
}

/// **An open a live intent names as a source holds until the intent ends.** A stream of the
/// cell's one leaf shard is created and its engine retired. A live split, renewed as its actor
/// would, then names that shard: the stream's append, which reopens it, is refused retryably, and
/// so is a read, and no engine is published. Once the intent is rolled back, the append is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_open_a_live_intent_names_holds_until_the_intent_ends() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let leaves = state.shards.prefixes().shards.leaves.clone();
    let (prefix, _) = leaves.iter().next().expect("a leaf");
    let name = stream_in(&state, addr, prefix).await;
    engine_shutdown(&state).await;
    let created = intent::create(&*store, &split_of(leaves.clone().into_iter().collect()))
        .await
        .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let held = async {
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
        assert!(stop.send(()).is_ok(), "the renewer runs");
    };
    let (live, ()) = tokio::join!(renew_until(&*store, created, stopped), held);
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
/// no intent and no stored topology to publish into: the opener's roll forward fails, its reopen
/// holds, reads included, and no engine is published.
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

/// What an actor that stopped in its pause left on `engine` of leaf `prefix` at `home`: the
/// pause's `topo-op-fence` checkpoint, both children's lists (one a `-d` list) under the leaf's
/// root, and a file under the first child's path (homes 90 and 91, as `split_of` names them). The
/// paths of the lists and the child.
async fn abandoned(
    store: &dyn object_store::ObjectStore,
    engine: &crate::shard::ShardEngine,
    prefix: &str,
    home: u32,
) -> (Vec<String>, String) {
    use crate::spill::lineage::{reference_list_path, supplementary_list_path};
    let options = slatedb::config::CheckpointOptions {
        lifetime: Some(Duration::from_secs(3600)),
        source: None,
        name: Some("topo-op-fence".to_string()),
    };
    let scope = slatedb::config::CheckpointScope::All;
    engine.db.create_checkpoint(scope, &options).await.unwrap();
    let root = engine.object_root(home);
    let lists = vec![
        reference_list_path(&root, 90),
        supplementary_list_path(&root, 91),
    ];
    let child = crate::topology::model::born_home_path(&format!("{prefix}0"), 90);
    let file = format!("{child}/manifest/00000000000000000001.manifest");
    for path in lists.iter().chain([&file]) {
        object_store::ObjectStoreExt::put(store, &path.as_str().into(), b"left".to_vec().into())
            .await
            .unwrap();
    }
    (lists, child)
}

/// The checkpoints named `topo-op-fence` on the log at `path`.
async fn pause_checkpoints(store: &Arc<dyn object_store::ObjectStore>, path: &str) -> usize {
    let admin = slatedb::admin::AdminBuilder::new(path, store.clone()).build();
    let named = admin.list_checkpoints(Some("topo-op-fence")).await.unwrap();
    named.len()
}

/// Whether `path` is absent from `store`, and nothing lies under it.
async fn gone(store: &dyn object_store::ObjectStore, path: &str) -> bool {
    use futures_util::TryStreamExt;
    let at = object_store::path::Path::from(path);
    let head = object_store::ObjectStoreExt::head(store, &at).await;
    let under: Vec<_> = store.list(Some(&at)).try_collect().await.unwrap();
    matches!(head, Err(object_store::Error::NotFound { .. })) && under.is_empty()
}

/// **A lapsed intent is rolled back by the parent's opener, which then serves.** A stream of the
/// cell's leaf shard; an actor that stopped in its pause left its checkpoint, lists and a child
/// path, and a live split intent no one renews. The reopen holds through the lapse (no sooner than
/// 11 s), then the opener claims the intent (`lease_epoch` 2, its own owner), deletes what the
/// actor left, ends the intent `RolledBack`, and serves the append.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_lapsed_intent_is_rolled_back_by_the_parents_opener_which_then_serves() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let leaves = state.shards.prefixes().shards.leaves.clone();
    let (prefix, home) = leaves.iter().next().expect("a leaf");
    let name = stream_in(&state, addr, prefix).await;
    let engine = state.shards.open(prefix).expect("its engine");
    let (lists, child) = abandoned(&*store, &engine, prefix, *home).await;
    let log = engine.log_path();
    drop(engine);
    engine_shutdown(&state).await;
    intent::create(&*store, &split_of(vec![(prefix.clone(), *home)]))
        .await
        .unwrap();
    let t0 = Instant::now();
    while append(addr, &name).await != 204 {
        assert!(t0.elapsed() < CLAIM_BOUND, "served once rolled back");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        t0.elapsed() >= intent::LEASE + intent::SKEW,
        "the claim waited the lapse: {:?}",
        t0.elapsed()
    );
    let ended = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(ended.state, State::RolledBack);
    assert_eq!(ended.lease_epoch, 2, "claimed");
    assert!(
        ended.lease_owner.starts_with("opener:"),
        "{}",
        ended.lease_owner
    );
    assert_eq!(pause_checkpoints(&store, &log).await, 0, "the checkpoint");
    for path in lists.iter().chain([&child]) {
        assert!(gone(&*store, path).await, "{path} is deleted");
    }
    engine_shutdown(&state).await;
}

/// **A lapsed intent whose parent holds `'T'` is rolled forward by its opener, and the parent
/// never serves.** The cell's stored topology (version 1) has homes 90 and 91 reserved; the leaf's
/// log holds `'T'` for version 2 naming them, the pause's checkpoint is left, and a live split
/// intent no one renews names the leaf. Through the lapse and after it, the append is never
/// served and no engine is published; the opener claims the intent, publishes version 2 (the
/// split, `last_op` the operation), deletes the checkpoint and ends the intent `Reclaimed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_lapsed_intent_whose_parent_holds_t_is_rolled_forward_and_the_parent_never_serves() {
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
    let mut shards = state.shards.prefixes().shards.clone();
    let (prefix, home) = shards
        .leaves
        .iter()
        .next()
        .map(|(p, h)| (p.clone(), *h))
        .unwrap();
    shards.next_home = 92;
    let topology = crate::topology::model::Topology {
        version: 1,
        shards,
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), raw.into())
        .await
        .unwrap();
    let name = stream_in(&state, addr, &prefix).await;
    let engine = state.shards.open(&prefix).expect("its engine");
    abandoned(&*store, &engine, &prefix, home).await;
    let retired = crate::topology::fence::Retired {
        version: 2,
        op_id: "op-fence".to_string(),
        children: vec![(format!("{prefix}0"), 90), (format!("{prefix}1"), 91)],
    };
    let row = serde_json::to_vec(&retired).unwrap();
    let key = crate::topology::fence::retired_key();
    engine.db.put(key, row).await.unwrap();
    engine.db.flush().await.unwrap();
    let log = engine.log_path();
    drop(engine);
    engine_shutdown(&state).await;
    intent::create(&*store, &split_of(vec![(prefix.clone(), home)]))
        .await
        .unwrap();
    let t0 = Instant::now();
    let published = loop {
        let status = append(addr, &name).await;
        assert_ne!(status, 204, "the retired parent never serves");
        let (stored, _) = crate::topology::store::read(&*store).await.unwrap();
        if stored.version == 2 {
            break stored;
        }
        assert!(t0.elapsed() < CLAIM_BOUND, "rolled forward");
    };
    assert!(
        t0.elapsed() >= intent::LEASE + intent::SKEW,
        "the claim waited the lapse"
    );
    assert_eq!(
        published,
        topology.split(&prefix, [90, 91], "op-fence").unwrap()
    );
    assert!(
        state.shards.open(&prefix).is_none(),
        "no engine is published"
    );
    let t1 = Instant::now();
    let ended = loop {
        let ended = intent::read(&*store).await.unwrap().unwrap().intent;
        if ended.is_terminal() || t1.elapsed() > BOUND {
            break ended;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!((ended.state, ended.lease_epoch), (State::Reclaimed, 2));
    assert_eq!(pause_checkpoints(&store, &log).await, 0, "the checkpoint");
    engine_shutdown(&state).await;
}
