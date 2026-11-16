#![cfg(test)]
//! A split's start (S4-DESIGN §6.2 steps 0.1 and 0.2): its refusals, the version it splits from and
//! the home its intent names, and the engines its publish hands back. `refuse` answers before
//! `begin` reserves homes, so a refused start leaves the stored topology as it was.

use super::fixture_http::engine_shutdown;
use super::fixture_split::{Cell, cell};

/// **A split of a split's child starts from the version the first published and names the child's
/// own home, and each published child is the engine of its own prefix.** Leaf `00` splits into
/// `000` and `001` at version 2, its intent ended `Reclaimed`; a terminal intent holds nothing, so
/// a split of `000` starts: it answers version 2, and its intent, `Preparing`, names `000` at the
/// home the first split gave it (never home 0) and the version it saw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_child_starts_from_the_published_version_at_the_childs_home() {
    let Cell { state, parent, .. } = cell().await;
    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-1", "inst-a", &*clock)
            .await
            .expect("the first split publishes");
    assert_eq!(published.version, 2);
    for (prefix, _, child) in &published.children {
        assert_eq!(&child.prefix, prefix, "the engine published for {prefix}");
    }
    let (_, home, child) = &published.children[0];
    assert!(*home > 0, "an operation-born home: {home}");
    let store = state.data_store.clone();
    let first = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(first.state, crate::topology::intent::State::Reclaimed);
    let started = crate::topology::split::start(&state.shards, child, "op-2", "inst-a")
        .await
        .map_err(|refused| refused.to_string())
        .expect("a split of the child starts");
    assert_eq!(started.version(), 2, "the version it splits from");
    let second = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(
        (second.op_id.as_str(), second.state, second.version_seen),
        ("op-2", crate::topology::intent::State::Preparing, 2)
    );
    assert_eq!(second.sources, [("000".to_string(), *home)]);
    drop(started);
    engine_shutdown(&state).await;
}

/// **A split under a live intent is refused `topology_busy` (`intent`) before it reserves homes.**
/// One split of `00` has started and holds the cell's intent, `Preparing`; a second start of the
/// same leaf is refused `Busy("intent")`, the stored topology is unchanged (no home CAS: its
/// `next_home` did not move), and the intent is still the first's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_under_a_live_intent_is_refused_busy() {
    let Cell { state, parent, .. } = cell().await;
    let first = crate::topology::split::start(&state.shards, &parent, "op-live", "inst-a")
        .await
        .map_err(|refused| refused.to_string())
        .expect("the first split starts");
    let store = state.data_store.clone();
    let (before, _) = crate::topology::store::read(&*store).await.unwrap();
    let refused = crate::topology::split::start(&state.shards, &parent, "op-late", "inst-a")
        .await
        .err()
        .expect("the second start is refused");
    assert!(
        matches!(refused, crate::topology::split::Refused::Busy("intent")),
        "{refused}"
    );
    let (after, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(after, before, "a refused start reserves no homes");
    let held = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(
        (held.op_id.as_str(), held.state),
        ("op-live", crate::topology::intent::State::Preparing)
    );
    drop(first);
    engine_shutdown(&state).await;
}
