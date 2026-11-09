#![cfg(test)]
//! The claimant's lineage walk and its idempotent publish (S4-DESIGN §6.4).

use std::sync::Arc;

use object_store::ObjectStore;

use super::{lineage_of, publish};
use crate::config::validation::InitialShards;
use crate::topology::fence::Retired;
use crate::topology::model::Home;
use crate::topology::store;

/// **A home's lineage is itself and every home it descends from, each once, nearest first.**
/// Homes 0 and 1 are the cell's; 2 was split from 0, 3 merged from 2 and 1, and 4 split from 3:
/// 4's lineage reaches 0 by two paths and lists it once.
#[test]
fn a_homes_lineage_lists_each_ancestor_once_nearest_first() {
    let mut shards = InitialShards::new(2).unwrap().topology(1 << 20).shards;
    let born = |parents: Vec<u32>| Home {
        path: String::new(),
        prefix: String::new(),
        parents,
        born_version: 2,
        retired_version: None,
    };
    shards.homes.insert(2, born(vec![0]));
    shards.homes.insert(3, born(vec![2, 1]));
    shards.homes.insert(4, born(vec![3, 0]));
    assert_eq!(lineage_of(&shards, 4), [4, 3, 0, 2, 1]);
    assert_eq!(lineage_of(&shards, 0), [0]);
}

/// **The roll forward publishes the split `'T'` names once, and again changes nothing.** A cell of
/// two leaves at version 1 with homes 2 and 3 reserved; `'T'` names version 2 and those homes for
/// `0`'s halves. The publish lands the split at version 2 with `last_op` naming the operation; a
/// second publish leaves the object as it is (its ETag unchanged). A `'T'` two versions ahead is
/// stuck, and one naming no children of the prefix is refused.
#[tokio::test(crate = "crate::test_rt")]
async fn the_roll_forward_publishes_once_and_again_changes_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    crate::registry::load_or_init_topology(&store, InitialShards::new(2).unwrap(), 1 << 20)
        .await
        .unwrap();
    let (stored, etag) = store::read(&*store).await.unwrap();
    let (reserved, homes) = stored.reserve_homes();
    store::replace(&*store, &etag, &reserved).await.unwrap();
    let retired = Retired {
        version: 2,
        op_id: "op-f".to_string(),
        children: vec![("00".to_string(), homes[0]), ("01".to_string(), homes[1])],
    };
    publish(&*store, "0", &retired).await.unwrap();
    let (published, etag) = store::read(&*store).await.unwrap();
    assert_eq!(published, reserved.split("0", homes, "op-f").unwrap());
    publish(&*store, "0", &retired).await.unwrap();
    assert_eq!(store::read(&*store).await.unwrap(), (published, etag));
    let ahead = Retired {
        version: 4,
        ..retired.clone()
    };
    let stuck = publish(&*store, "0", &ahead).await.unwrap_err();
    assert!(stuck.contains("stuck"), "{stuck}");
    let none = Retired {
        version: 3,
        children: vec![],
        ..retired
    };
    let refused = publish(&*store, "1", &none).await.unwrap_err();
    assert!(refused.contains("no children"), "{refused}");
}
