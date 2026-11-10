#![cfg(test)]
//! A split of a split's child (S4-DESIGN §3.1, §6.2): a second-generation parent lives at an
//! operation-born home's path, and its children clone that path's own tables (its C4 seed table, and
//! what it flushed since the first split) as well as those it inherited from the first parent.

use super::fixture_http::engine_shutdown;
use super::split_k::{append, cell, records};

/// **A child of a split splits again and serves its stream from its own child.** Leaf `00` splits
/// into `000` and `001`; the lower stream is appended to on `000` and its log flushed, so `000`
/// holds tables of its own under its born home's path; then `000` splits into `0000` and `0001`.
/// The second split publishes version 3, `000` is retired, and the lower stream reads back its
/// three records from its new leaf.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_childs_split_clones_its_own_tables_and_serves_from_its_child() {
    let cell = cell().await;
    cell.parent.seal_requested().await.expect("seal");
    let clock = cell.state.runtime.clock.clone();
    let first =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-g1", "inst-a", &*clock)
            .await
            .expect("the first split publishes");
    let (prefix, _, child) = first
        .children
        .into_iter()
        .find(|(p, _, _)| p == "000")
        .unwrap();
    assert_eq!(prefix, "000");
    assert_eq!(
        append(cell.addr, &cell.low, 5).await.0,
        204,
        "appended on the child"
    );
    child.db.flush().await.unwrap();
    let want = records(cell.addr, &cell.low).await;
    assert_eq!(want.as_array().map(Vec::len), Some(3));
    let second =
        crate::topology::split::split(&cell.state.shards, &child, "op-g2", "inst-a", &*clock)
            .await
            .expect("the second split publishes");
    assert_eq!(second.version, 3);
    let prefixes: Vec<&str> = second.children.iter().map(|(p, _, _)| p.as_str()).collect();
    assert_eq!(prefixes, ["0000", "0001"]);
    assert!(child.is_closed(), "the first child is retired");
    assert_eq!(records(cell.addr, &cell.low).await, want);
    engine_shutdown(&cell.state).await;
}

/// **A child that fails to open after the publish leaves the split published, never rolled back**
/// (edge #251's view and route). Junk WAL objects planted under the lower child's path at the
/// park lets its clone through but fails its open after the topology CAS. The split still answers
/// published at version 2 with the other child open, its intent ends `Reclaimed` (not
/// `RolledBack`: an operator would retry a split that already happened), the parent is retired,
/// and the stored topology is the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_child_that_fails_to_open_after_the_publish_leaves_the_split_published() {
    let cell = cell().await;
    let store = cell.state.data_store.clone();
    let home = cell.state.shards.prefixes().shards.next_home;
    let child = crate::topology::model::born_home_path("000", home);
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    cell.parent.gc_state().gate.pause_at_park(point);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-o", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        for id in 1..=64u64 {
            let wal = format!("{child}/wal/{id:020}.sst");
            let junk = b"not a wal table".to_vec();
            object_store::ObjectStoreExt::put(&*store, &wal.as_str().into(), junk.into())
                .await
                .unwrap();
        }
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
    };
    let (split, ()) = tokio::join!(split, control);
    let published = split.expect("published, a child unopened");
    assert_eq!(published.version, 2);
    let opened: Vec<&str> = published
        .children
        .iter()
        .map(|(p, _, _)| p.as_str())
        .collect();
    assert_eq!(opened, ["001"]);
    let ended = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    assert!(cell.parent.is_closed(), "the parent is retired");
    let (stored, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(stored.version, 2);
    engine_shutdown(&cell.state).await;
}

/// **A split whose publish fails after its `'T'` is left to roll forward, never rolled back**
/// (S4-DESIGN §6.4; edge #251's view). `topology.json` is made unreadable at the park, so the
/// publish fails after `'T'`: the split refuses `Unfinished`, its intent stays live (`Preparing`,
/// for the next opener of the parent to claim and roll forward from `'T'`), never `RolledBack`, and
/// the parent's log holds `'T'`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_whose_publish_fails_after_t_is_left_to_roll_forward() {
    let cell = cell().await;
    let store = cell.state.data_store.clone();
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    cell.parent.gc_state().gate.pause_at_park(point);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-u", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        let junk = b"{".to_vec();
        object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), junk.into())
            .await
            .unwrap();
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
    };
    let (split, ()) = tokio::join!(split, control);
    let refused = split.err().expect("the publish fails");
    assert!(
        matches!(&refused, crate::topology::split::Refused::Unfinished(_)),
        "{refused}"
    );
    let left = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(
        left.state,
        crate::topology::intent::State::Preparing,
        "live, for a claimant"
    );
    let retired = cell
        .parent
        .db
        .get(crate::topology::fence::retired_key())
        .await;
    assert!(
        matches!(retired, Ok(Some(_))) || cell.parent.is_closed(),
        "'T' was written"
    );
    engine_shutdown(&cell.state).await;
}
