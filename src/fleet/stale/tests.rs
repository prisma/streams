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

/// A store holding the boot topology of leaves `0` and `1`.
async fn two_leaves() -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let initial = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let raw = serde_json::to_vec(&initial).unwrap();
    store
        .put(&"topology.json".into(), raw.into())
        .await
        .unwrap();
    store
}

/// The split of `1` as `state`.
fn split_of_1(state: crate::topology::intent::State) -> crate::topology::intent::Intent {
    crate::topology::intent::Intent {
        kind: "split".to_string(),
        op_id: "op-born".to_string(),
        sources: vec![("1".to_string(), 1)],
        children: vec![2, 3],
        version_seen: 1,
        state,
        lease_owner: "streams-1".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    }
}

/// **A live split's children are kept** (GATE-DECISION-f2-children-on-actor A4). Over the leaves `0`
/// and `1`, while the split of `1` is live, the entries its actor wrote for its children `10` and
/// `11` before its topology CAS are kept beside the leaf `0`'s, and `000` (no leaf, no child) is
/// dropped. Once the intent is rolled back the children's entries are dropped too.
#[tokio::test(crate = "crate::test_rt")]
async fn a_live_splits_children_are_kept() {
    use crate::topology::intent::{self, State};
    let store = two_leaves().await;
    let doc = overrides(&["0", "000", "10", "11"]);
    let held = intent::create(&*store, &split_of_1(State::Preparing))
        .await
        .unwrap();
    let kept = pruned(doc.clone(), &*store).await;
    assert_eq!(named(&kept), ["0", "10", "11"], "while the split is live");
    intent::replace(&*store, &held, split_of_1(State::RolledBack))
        .await
        .unwrap();
    let kept = pruned(doc, &*store).await;
    assert_eq!(named(&kept), ["0"], "once it ended");
}

/// **An unreadable intent prunes nothing** (A4): over the leaves `0` and `1` with an intent that
/// does not decode, the entries for `000` and `10`, which name no leaf, are both kept.
#[tokio::test(crate = "crate::test_rt")]
async fn an_unreadable_intent_prunes_nothing() {
    let store = two_leaves().await;
    store
        .put(&"topology/intent.json".into(), b"not json".to_vec().into())
        .await
        .unwrap();
    let kept = pruned(overrides(&["0", "000", "10"]), &*store).await;
    assert_eq!(named(&kept), ["0", "000", "10"]);
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

/// **A pin (an entry stamped 0, `join::PIN_MS`) is an ordinary entry to the stale prune** (the join
/// ruling §5): over the boot leaves `0` and `1`, a pin for `1` is kept and a pin for `2` (no leaf)
/// dropped, as any entry would be; and the document carrying it validates.
#[tokio::test(crate = "crate::test_rt")]
async fn a_pin_is_pruned_as_any_entry() {
    let store = two_leaves().await;
    let mut doc = overrides(&["1", "2"]);
    for entry in doc.entries.values_mut() {
        entry.ms = crate::fleet::join::PIN_MS;
    }
    let kept = pruned(doc, &*store).await;
    assert_eq!(named(&kept), ["1"]);
    assert!(crate::fleet::repository::validate_overrides(&kept).is_ok());
}
