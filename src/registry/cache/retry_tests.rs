//! The descriptor read's one bounded retry (tuning item 5): a store failure
//! is read again once, a corrupt descriptor is not.
#![cfg(test)]
use std::sync::Arc;

use object_store::ObjectStoreExt;

use super::failing_gets::{FailingGets, descriptor_suffix};
use crate::registry::Registry;
use crate::registry::tests::{desc, ts};

/// A registry over a store holding stream `s1`, read through nothing yet.
async fn rig() -> (Registry, Arc<FailingGets>) {
    let store = FailingGets::over(Arc::new(object_store::memory::InMemory::new()));
    let reg = Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    );
    reg.create(desc("s1", "00000000000000000000000000000001", false))
        .await
        .unwrap();
    reg.invalidate(&ts("s1"));
    (reg, store)
}

/// One failed GET is read again and the read answers the descriptor: two
/// GETs. Two failed GETs fail the read with the store's error after
/// exactly two, never a third.
#[tokio::test(crate = "crate::test_rt")]
async fn a_failed_descriptor_get_is_read_again_once() {
    let (reg, store) = rig().await;
    store.fail(&descriptor_suffix("s1"), 1);
    let read = reg.get(&ts("s1")).await.unwrap();
    assert_eq!(
        (read.map(|d| d.name.clone()), store.gets()),
        (Some("s1".to_string()), 2)
    );
    reg.invalidate(&ts("s1"));
    store.fail(&descriptor_suffix("s1"), 2);
    let failed = reg.get(&ts("s1")).await.unwrap_err();
    assert!(!super::is_corrupt_descriptor(&failed), "{failed}");
    assert_eq!(store.gets(), 2);
}

/// A corrupt descriptor is an answer, not a store failure: one GET.
#[tokio::test(crate = "crate::test_rt")]
async fn a_corrupt_descriptor_is_not_read_again() {
    let (reg, store) = rig().await;
    let path = crate::registry::desc_path("test-cell", &ts("s2"));
    let body = object_store::PutPayload::from(b"{ not json".to_vec());
    store.inner.put(&path, body).await.unwrap();
    store.fail(&descriptor_suffix("s2"), 0);
    let corrupt = reg.get(&ts("s2")).await.unwrap_err();
    assert!(super::is_corrupt_descriptor(&corrupt), "{corrupt}");
    assert_eq!(store.gets(), 1);
}
