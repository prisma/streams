#![cfg(test)]
//! The never-leaf child-path sweep (S6-DESIGN rev 6 §4 step 5, S4-DESIGN §15 N1) on a serving rig:
//! the paths a split that never published left, deleted with the checkpoints they pinned on the
//! parent, while the live leaf keeps every table it serves from. Its rig is `merge_k`'s.

use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::merge_k::{Rig, rig};
use std::collections::BTreeSet;
use std::sync::Arc;

type Store = Arc<dyn object_store::ObjectStore>;

/// Every object under `path`, the parent's manifests left out (a checkpoint's delete writes one).
async fn tables_under(store: &Store, path: &str) -> BTreeSet<String> {
    use futures_util::TryStreamExt;
    let at = object_store::path::Path::from(path);
    let listed: Vec<_> = store.list(Some(&at)).try_collect().await.unwrap();
    listed
        .into_iter()
        .map(|meta| meta.location.to_string())
        .filter(|location| !location.contains("/manifest/"))
        .collect()
}

/// The checkpoints on the log at `path`.
async fn checkpoints(store: &Store, path: &str) -> BTreeSet<String> {
    let admin = slatedb::admin::AdminBuilder::new(path, store.clone()).build();
    let listed = admin.list_checkpoints(None).await.unwrap();
    listed.into_iter().map(|c| c.id.to_string()).collect()
}

/// The view's `never_leaf_paths`.
async fn never_leaf_paths(addr: std::net::SocketAddr) -> serde_json::Value {
    super::split_k::topology_view(addr).await["never_leaf_paths"].clone()
}

async fn records(addr: std::net::SocketAddr, name: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// **A rolled-back split's child paths are swept, and the live leaf is not touched** (S6 §4 step
/// 5). The split of `00` reserved homes `h` and `h + 1` and ended `RolledBack`; a stale actor's
/// late clones then left both children's paths, each pinning a final checkpoint on the parent.
/// The view counts 2 never-leaf paths. One sweep deletes both paths and both final checkpoints:
/// the view counts 0, the parent keeps only its own checkpoint, every table of the live leaf `00`
/// is still there, and both streams read back byte-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_rolled_back_splits_child_paths_are_swept_and_the_live_leaf_is_not_touched() {
    use crate::topology::intent::{self, Intent, State};
    let Rig {
        store,
        state,
        addr,
        low,
        high,
        parent,
    } = rig().await;
    let want = (records(addr, &low).await, records(addr, &high).await);
    let (stored, etag) = crate::topology::store::read(&*store).await.unwrap();
    let home = stored.shards.next_home;
    let mut reserved = stored.clone();
    reserved.shards.next_home = home + 2;
    crate::topology::store::replace(&*store, &etag, &reserved)
        .await
        .unwrap();
    let parent_home = stored.shards.leaves["00"];
    let rolled_back = Intent {
        kind: "split".to_string(),
        op_id: "op-late".to_string(),
        sources: vec![("00".to_string(), parent_home)],
        children: vec![home, home + 1],
        version_seen: stored.version,
        state: State::RolledBack,
        lease_owner: "opener:00@2".to_string(),
        lease_epoch: 2,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    intent::create(&*store, &rolled_back).await.unwrap();

    let log = parent.log_path();
    let own = checkpoints(&store, &log).await;
    let options = slatedb::config::CheckpointOptions {
        lifetime: Some(std::time::Duration::from_secs(3600)),
        source: None,
        name: Some("topo-op-late".to_string()),
    };
    let taken = parent
        .db
        .create_checkpoint(slatedb::config::CheckpointScope::All, &options)
        .await
        .unwrap();
    let children = [("000", home), ("001", home + 1)]
        .map(|(prefix, h)| crate::topology::model::born_home_path(prefix, h));
    for child in &children {
        slatedb::admin::AdminBuilder::new(child.as_str(), store.clone())
            .build()
            .create_clone_builder_from_source(slatedb::CloneSourceSpec::with_checkpoint(
                log.clone(),
                taken.id,
            ))
            .build()
            .await
            .expect("the late clone lands");
    }
    let pinned = checkpoints(&store, &log).await;
    assert_eq!(
        pinned.len(),
        own.len() + 3,
        "ours and two final checkpoints"
    );
    let live_tables = tables_under(&store, &log).await;
    assert_eq!(never_leaf_paths(addr).await, 2);

    let swept = crate::topology::lineage::never_leaf::sweep(&store)
        .await
        .unwrap();
    assert_eq!(swept, 2);
    assert_eq!(never_leaf_paths(addr).await, 0);
    for child in &children {
        assert!(
            tables_under(&store, child).await.is_empty(),
            "{child} is deleted"
        );
    }
    let mut kept = own.clone();
    kept.insert(taken.id.to_string());
    assert_eq!(
        checkpoints(&store, &log).await,
        kept,
        "only the parent's own checkpoints"
    );
    let after = tables_under(&store, &log).await;
    assert!(
        live_tables.is_subset(&after),
        "the live leaf keeps every table"
    );
    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    assert_eq!(
        crate::topology::lineage::never_leaf::sweep(&store)
            .await
            .unwrap(),
        0,
        "a second sweep finds nothing"
    );
    engine_shutdown(&state).await;
}

/// A cell's two shards `0` and `1`, homes 0 and 1, at version 1.
fn two_shards() -> crate::topology::model::Topology {
    crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20)
}

/// A directory over `topology` whose opener opens nothing: the view reads only the store.
fn view_dir(topology: &crate::topology::model::Topology) -> crate::shard_directory::ShardDirectory {
    let opener: crate::sharddir::OpenFn =
        Box::new(|_, _| Box::pin(std::future::ready(Err(anyhow::anyhow!("no engine here")))));
    crate::shard_directory::ShardDirectory::new(
        topology.shards.clone(),
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: std::time::Duration::from_secs(5),
            open_wait: std::time::Duration::from_millis(50),
        },
        |_| opener,
    )
}

/// An intent of `op_id` over `sources`, at `state`, having seen `version_seen`.
fn an_intent(
    op_id: &str,
    sources: &[(&str, u32)],
    state: crate::topology::intent::State,
    version_seen: u64,
) -> crate::topology::intent::Intent {
    crate::topology::intent::Intent {
        kind: "split".to_string(),
        op_id: op_id.to_string(),
        sources: sources
            .iter()
            .map(|(p, h)| ((*p).to_string(), *h))
            .collect(),
        children: vec![2, 3],
        version_seen,
        state,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    }
}

/// **A leaf shows `splitting` exactly while a live intent names it, prefix and home both.** Under
/// a live intent naming `1` at home 1, `1` is splitting and `0` serving; one naming `1` at a home
/// it does not live at, or a terminal intent naming it, leaves both serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_leaf_is_splitting_exactly_while_a_live_intent_names_its_prefix_and_home() {
    use crate::topology::intent::State;
    let topology = two_shards();
    let cases = [
        (
            "a live intent",
            State::Preparing,
            1,
            ["serving", "splitting"],
        ),
        ("another home", State::Preparing, 7, ["serving", "serving"]),
        (
            "a terminal intent",
            State::RolledBack,
            1,
            ["serving", "serving"],
        ),
    ];
    for (what, state, home, want) in cases {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        let intent = an_intent("op-s", &[("1", home)], state, 1);
        crate::topology::intent::create(&*store, &intent)
            .await
            .unwrap();
        let dir = view_dir(&topology);
        let ownership = crate::ownership::OwnershipService::new("");
        let view = crate::topology::view::topology(&dir, &ownership, &store).await;
        let states: Vec<&str> = view["shards"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["state"].as_str().unwrap())
            .collect();
        assert_eq!(states, want, "{what}");
    }
}

/// **An intent reads `Published` only once the stored topology is past what it saw and names it
/// as its last operation.** The stored topology is a split of `1` at version 2 by `op-s`: an
/// intent of `op-s` that saw version 1 is published; one that saw version 2, or another
/// operation's that saw version 1, is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_intent_is_published_only_past_its_version_and_as_the_last_operation() {
    use crate::topology::intent::State;
    let topology = two_shards();
    let (reserved, homes) = topology.reserve_homes();
    let split = reserved.split("1", homes, "op-s").unwrap();
    let cases = [
        ("its own, past what it saw", "op-s", 1, "Published"),
        ("its own, at what it saw", "op-s", 2, "Preparing"),
        ("another operation's", "op-x", 1, "Preparing"),
    ];
    for (what, op_id, seen, want) in cases {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        let stored = serde_json::to_vec(&split).unwrap();
        object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), stored.into())
            .await
            .unwrap();
        let intent = an_intent(op_id, &[("1", 1)], State::Preparing, seen);
        crate::topology::intent::create(&*store, &intent)
            .await
            .unwrap();
        let dir = view_dir(&topology);
        let ownership = crate::ownership::OwnershipService::new("");
        let view = crate::topology::view::topology(&dir, &ownership, &store).await;
        assert_eq!(view["intent"]["state"], want, "{what}");
    }
}
