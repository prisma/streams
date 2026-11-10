#![cfg(test)]
//! A published operation's intent left live, and only such an intent, ends `Reclaimed`.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::{published, read};
use crate::topology::intent::{self, Intent, State};
use crate::topology::model::Topology;

fn initial() -> Topology {
    crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20)
}

/// `initial` with its leaf `0` split by `op_id` (version 2).
fn split_by(op_id: &str) -> Topology {
    let (reserved, homes) = initial().reserve_homes();
    reserved.split("0", homes, op_id).unwrap()
}

fn live(op_id: &str) -> Intent {
    Intent {
        kind: "split".to_string(),
        op_id: op_id.to_string(),
        sources: vec![("0".to_string(), 0)],
        children: vec![2, 3],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 10_000,
        started_ms: 0,
        outcome: None,
    }
}

/// **Published means past the version the intent started from, `last_op` naming it**, and only
/// for a live intent.
#[test]
fn published_needs_the_operation_past_its_version() {
    let intent = live("op-x");
    assert!(!published(&initial(), &intent), "nothing published");
    assert!(published(&split_by("op-x"), &intent));
    assert!(!published(&split_by("op-y"), &intent), "another operation");
    let mut later = intent.clone();
    later.version_seen = 2;
    assert!(
        !published(&split_by("op-x"), &later),
        "not past its version"
    );
    let mut ended = intent;
    ended.state = State::RolledBack;
    assert!(!published(&split_by("op-x"), &ended), "already terminal");
}

async fn store_topology(store: &dyn ObjectStore, topology: &Topology) {
    let raw = serde_json::to_vec(topology).unwrap();
    store
        .put(&"topology.json".into(), raw.into())
        .await
        .unwrap();
}

/// **`read` ends a live intent `Reclaimed` only once the stored topology published it.** With
/// no stored topology, and with one that has not moved, the intent is read as it is; once the
/// topology names its operation the read ends it `Reclaimed` (result `published`), in the store,
/// and a later read finds it so.
#[tokio::test(crate = "crate::test_rt")]
async fn read_reclaims_only_a_published_live_intent() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    assert!(read(&*store).await.unwrap().is_none(), "no intent");
    intent::create(&*store, &live("op-x")).await.unwrap();
    let state = |held: Option<intent::Held>| held.map(|h| h.intent.state);
    assert_eq!(state(read(&*store).await.unwrap()), Some(State::Preparing));
    store_topology(&*store, &initial()).await;
    assert_eq!(state(read(&*store).await.unwrap()), Some(State::Preparing));
    store_topology(&*store, &split_by("op-x")).await;
    let reclaimed = read(&*store).await.unwrap().unwrap();
    assert_eq!(reclaimed.intent.state, State::Reclaimed);
    let result = reclaimed.intent.outcome.as_ref().map(|o| o.result.as_str());
    assert_eq!(result, Some("published"));
    let stored = intent::read(&*store).await.unwrap().unwrap();
    assert_eq!(
        (stored.intent, stored.etag),
        (reclaimed.intent, reclaimed.etag)
    );
}
