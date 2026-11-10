#![cfg(test)]
//! A split of a split's child (S4-DESIGN §3.1, §6.2): a second-generation parent lives at an
//! operation-born home's path, and its children clone that path's own tables (its C4 seed table, and
//! what it flushed since the first split) as well as those it inherited from the first parent.

use super::fixture_http::engine_shutdown;
use super::split_k::{append, cell, records};

/// **A child of a split splits again and serves its stream from its own child.** Leaf `00` splits
/// into `000` and `001`; the lower stream is appended to on `000` and its log flushed, so `000`
/// holds tables of its own under its born home's path; then `000` splits into `0000` and `0001`.
/// The second split publishes version 3 (the stored topology names `0000` and `0001`), `000` is
/// retired, and the lower stream reads back its three records from its new leaf, whose open clones
/// `000`'s own tables.
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
    // A child the publish could not open in time (a slow open under load) opens on its next
    // request; the stored topology names both, and the read below opens the stream's own.
    let (stored, _) = crate::topology::store::read(&*cell.state.data_store)
        .await
        .unwrap();
    let leaves: Vec<&str> = stored.shards.leaves.keys().map(String::as_str).collect();
    assert!(
        leaves.contains(&"0000") && leaves.contains(&"0001"),
        "{leaves:?}"
    );
    let opened = second.children.iter().map(|(p, _, _)| p.as_str());
    assert!(opened.into_iter().all(|p| p == "0000" || p == "0001"));
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

/// The split of a cell over a fault store whose first WAL write of the parent after the park
/// (`'T'`'s flush) is held and then released by `release` (lost, or failing before dispatch). The
/// split's result, the intent left behind, the cell, and the faults the store injected.
async fn split_with_t_held(
    release: fn(&crate::dst::fault_store::FaultStore),
) -> (
    Result<crate::topology::split::Published, crate::topology::split::Refused>,
    crate::topology::intent::Intent,
    super::split_k::Cell,
    u64,
) {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    let faults = FaultStore::uniform(super::fixture_storage::mem(), 0x7a17, FaultPlan::CLEAN);
    let store: std::sync::Arc<dyn object_store::ObjectStore> = faults.clone();
    let cell = super::split_k::cell_over(store.clone()).await;
    let wal = format!("{}/wal/", cell.parent.log_path());
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    cell.parent.gc_state().gate.pause_at_park(point);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-l", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        let engaged =
            faults.hold_class_under(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, &wal, 1);
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
        let t0 = std::time::Instant::now();
        while engaged.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(10),
                "'T''s flush parks"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        release(&faults);
    };
    let (split, ()) = tokio::join!(split, control);
    let left = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    let injected = faults.injected_lost() + faults.injected_errors();
    (split, left, cell, injected)
}

/// **A `'T'` whose flush SlateDB retries publishes the split** (S4-DESIGN §6.4). SlateDB's `put`
/// does not await durability, so the split's `'T'` is durable only once its flush lands. With that
/// flush's WAL write lost (it lands, its response an error) or failing before dispatch, SlateDB's
/// store retries it inside the flush, the flush returns, and the split publishes: a `'T'` flush
/// fails only for a fenced writer, whose WAL table never lands, so the rollback after a failed
/// `'T'` undoes a `'T'` that is not durable. Pinned for both faults, so a SlateDB that surfaced
/// either while landing the write would show here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_t_whose_flush_slatedb_retries_publishes_the_split() {
    let releases: [fn(&crate::dst::fault_store::FaultStore); 2] = [
        crate::dst::fault_store::FaultStore::release_hold_lost,
        crate::dst::fault_store::FaultStore::release_hold_failing,
    ];
    for release in releases {
        let (split, left, cell, injected) = split_with_t_held(release).await;
        assert_eq!(injected, 1, "the held flush took its fault");
        let published = split.expect("the split publishes");
        assert_eq!(published.version, 2);
        assert_eq!(left.state, crate::topology::intent::State::Reclaimed);
        engine_shutdown(&cell.state).await;
    }
}
