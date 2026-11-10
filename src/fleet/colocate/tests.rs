#![cfg(test)]
//! A co-location's one CAS over an in-memory fleet store: both halves named for the target with
//! a fresh stamp, an event each, the move mirrored here, the stale entries dropped; and nothing
//! moved when the document cannot be read.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use super::colocate;
use crate::fleet::{FleetRepository, Overrides};
use crate::ownership::OwnershipService;

const OVERRIDES: &str = "fleet/overrides.json";

fn halves() -> [String; 2] {
    ["00".to_string(), "01".to_string()]
}

/// A stored topology of the leaves `00`, `01` and `1` (the two-shard cell split at `0`).
async fn split_at_zero(store: &Arc<dyn ObjectStore>) {
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
}

async fn stored(store: &Arc<dyn ObjectStore>) -> Overrides {
    let raw = store.get(&OVERRIDES.into()).await.unwrap();
    serde_json::from_slice(&raw.bytes().await.unwrap()).unwrap()
}

/// **Both halves move to the target in one CAS**: the document names `00` and `01` for
/// `streams-2`, each stamped no earlier than the call, with one `rebalance_move` event each; the
/// entry `0`, which names no stored leaf, is dropped and `1`'s kept; this instance's view places
/// both halves on `streams-2` at once.
#[tokio::test(crate = "crate::test_rt")]
async fn a_colocation_names_both_halves_for_its_target_in_one_cas() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    split_at_zero(&store).await;
    let before = br#"{"entries":{"0":{"to":"streams-1","ms":1},"1":{"to":"streams-1","ms":1}}}"#;
    store
        .put(&OVERRIDES.into(), before.to_vec().into())
        .await
        .unwrap();
    let fleet = FleetRepository::new(Some(store.clone()));
    let ownership = OwnershipService::new("streams-1");
    let started = crate::shard::now_ms();
    assert!(colocate(&fleet, &ownership, &*store, &halves(), "streams-2").await);
    let doc = stored(&store).await;
    let mut named: Vec<(&str, &str)> = doc
        .entries
        .iter()
        .map(|(prefix, entry)| (prefix.as_str(), entry.to.as_str()))
        .collect();
    named.sort_unstable();
    assert_eq!(
        named,
        [("00", "streams-2"), ("01", "streams-2"), ("1", "streams-1")]
    );
    for half in halves() {
        assert!(doc.entries[&half].ms >= started, "{half} is stamped now");
    }
    let mut moved: Vec<&str> = doc
        .pending_events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    moved.dedup();
    assert_eq!(
        (doc.pending_events.len(), moved),
        (2, vec!["rebalance_move"])
    );
    let mirrored = ownership.overrides();
    assert_eq!(
        (mirrored.get("00"), mirrored.get("01")),
        (
            Some(&"streams-2".to_string()),
            Some(&"streams-2".to_string())
        )
    );
}

/// **An unreadable document moves nothing**: the call answers false, the stored bytes are as
/// they were and this instance's view holds no override.
#[tokio::test(crate = "crate::test_rt")]
async fn an_unreadable_overrides_document_moves_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(&OVERRIDES.into(), b"not json".to_vec().into())
        .await
        .unwrap();
    let fleet = FleetRepository::new(Some(store.clone()));
    let ownership = OwnershipService::new("streams-1");
    assert!(!colocate(&fleet, &ownership, &*store, &halves(), "streams-2").await);
    let raw = store.get(&OVERRIDES.into()).await.unwrap();
    assert_eq!(&raw.bytes().await.unwrap()[..], b"not json");
    assert!(ownership.overrides().is_empty());
}
