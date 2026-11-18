//! The ahead registry's standings.

use super::{Ahead, Standing};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A registered prefix waits until it is a leaf, then stands as one while it is ours; a
/// registration the ring gave away is withdrawn at the leaf; dropping the guard before the install
/// withdraws it; after the install, a dropped registration is a leaf as any other.
#[test]
fn a_registered_child_waits_for_its_leaf_and_is_withdrawn_when_dropped() {
    let ahead = Arc::new(Ahead::default());
    let ours = Arc::new(AtomicBool::new(true));
    let still = ours.clone();
    let registered = ahead.register(
        "00",
        "shards/00-3".into(),
        Box::new(move || still.load(Ordering::Relaxed)),
    );
    assert_eq!(ahead.path("00").as_deref(), Some("shards/00-3"));
    assert_eq!(ahead.path("01"), None);
    assert_eq!(ahead.standing("00", false), Standing::Waiting);
    assert_eq!(ahead.standing("00", true), Standing::Leaf);
    ours.store(false, Ordering::Relaxed);
    assert_eq!(ahead.standing("00", true), Standing::Withdrawn);
    drop(registered);
    assert_eq!(ahead.path("00"), None);
    assert_eq!(ahead.standing("00", false), Standing::Withdrawn);
    assert_eq!(ahead.standing("00", true), Standing::Leaf);
}

/// A waiting open enabled before a change wakes on it: a withdrawal and an install each wake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_withdrawal_and_an_install_each_wake_a_waiting_open() {
    let ahead = Arc::new(Ahead::default());
    let registered = ahead.register("00", "shards/00-3".into(), Box::new(|| true));
    let mut changed = Box::pin(ahead.changed());
    changed.as_mut().enable();
    ahead.installed();
    let woke = tokio::time::timeout(std::time::Duration::from_secs(5), changed).await;
    assert!(woke.is_ok(), "an install wakes the open");
    let mut changed = Box::pin(ahead.changed());
    changed.as_mut().enable();
    drop(registered);
    let woke = tokio::time::timeout(std::time::Duration::from_secs(5), changed).await;
    assert!(woke.is_ok(), "a withdrawal wakes the open");
}
