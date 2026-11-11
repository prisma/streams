#![cfg(test)]
//! Only entries that name no leaf of the stored topology are dropped, and none without one.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::{prune, pruned};
use crate::fleet::{FleetRepository, OverrideEntry, Overrides};

fn overrides(prefixes: &[&str]) -> Overrides {
    Overrides {
        entries: prefixes
            .iter()
            .map(|prefix| {
                let entry = OverrideEntry {
                    to: "streams-1".to_string(),
                    ms: 1,
                };
                (prefix.to_string(), entry)
            })
            .collect(),
        ..Default::default()
    }
}

/// The entries' prefixes, sorted.
fn named(doc: &Overrides) -> Vec<&str> {
    let mut named: Vec<&str> = doc.entries.keys().map(String::as_str).collect();
    named.sort_unstable();
    named
}

/// **An entry survives exactly when its prefix is a stored leaf**: over the leaves `0` and `1`
/// split at `0` (leaves `00`, `01`, `1`), the entries for `0` (the split parent) and `000` (no
/// leaf) are dropped, those for `00` and `1` kept. Without a stored topology nothing is dropped.
#[tokio::test(crate = "crate::test_rt")]
async fn only_entries_that_name_no_stored_leaf_are_dropped() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let doc = overrides(&["0", "00", "000", "1"]);
    let kept = pruned(doc.clone(), &*store).await;
    assert_eq!(named(&kept), ["0", "00", "000", "1"], "no stored topology");
    let initial = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let (reserved, homes) = initial.reserve_homes();
    let split = reserved.split("0", homes, "op-x").unwrap();
    let raw = serde_json::to_vec(&split).unwrap();
    store
        .put(&"topology.json".into(), raw.into())
        .await
        .unwrap();
    let kept = pruned(doc, &*store).await;
    assert_eq!(named(&kept), ["00", "1"]);
}

const OVERRIDES: &str = "fleet/overrides.json";

/// A store holding the topology split at `0` (leaves `00`, `01`, `1`) and `doc` as the overrides.
async fn split_store(doc: &Overrides) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let initial = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let (reserved, homes) = initial.reserve_homes();
    let split = reserved.split("0", homes, "op-x").unwrap();
    let raw = serde_json::to_vec(&split).unwrap();
    store
        .put(&"topology.json".into(), raw.into())
        .await
        .unwrap();
    let raw = serde_json::to_vec(doc).unwrap();
    store.put(&OVERRIDES.into(), raw.into()).await.unwrap();
    store
}

async fn stored(store: &Arc<dyn ObjectStore>) -> Overrides {
    let raw = store.get(&OVERRIDES.into()).await.unwrap();
    serde_json::from_slice(&raw.bytes().await.unwrap()).unwrap()
}

/// **A prune drops the stale entries in one CAS and writes nothing else** (#251 amendment R3-d):
/// after a split of `0`, `prune` drops `0` and `000` and keeps `00` and `1`, answering true; a
/// second prune finds nothing to drop, answers false and leaves the document's version as it was.
#[tokio::test(crate = "crate::test_rt")]
async fn a_prune_drops_the_entries_that_name_no_stored_leaf_in_one_cas() {
    let store = split_store(&overrides(&["0", "00", "000", "1"])).await;
    let fleet = FleetRepository::new(Some(store.clone()));
    assert!(prune(&fleet, &*store).await, "it wrote");
    assert_eq!(named(&stored(&store).await), ["00", "1"]);
    let version = store.head(&OVERRIDES.into()).await.unwrap().e_tag;
    assert!(!prune(&fleet, &*store).await, "nothing to drop");
    let after = store.head(&OVERRIDES.into()).await.unwrap().e_tag;
    assert_eq!(after, version, "nothing written");
}

/// **A lost CAS writes nothing**: a prune over a read the document has moved past answers false,
/// and the document is the newer writer's.
#[tokio::test(crate = "crate::test_rt")]
async fn a_prune_over_a_stale_read_loses_its_cas_and_writes_nothing() {
    let store = split_store(&overrides(&["0", "00"])).await;
    let fleet = FleetRepository::new(Some(store.clone()));
    let (read, version) = fleet.read_overrides().await.unwrap();
    let newer = serde_json::to_vec(&overrides(&["0", "00", "01"])).unwrap();
    store.put(&OVERRIDES.into(), newer.into()).await.unwrap();
    assert!(!super::write_pruned(&fleet, &*store, read, version).await);
    assert_eq!(named(&stored(&store).await), ["0", "00", "01"]);
}
