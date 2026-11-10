#![cfg(test)]
//! Only entries that name no leaf of the stored topology are dropped, and none without one.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::pruned;
use crate::fleet::{OverrideEntry, Overrides};

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
