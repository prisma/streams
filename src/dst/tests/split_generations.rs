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
/// store retries it inside the flush, the flush returns, and the split publishes. A `'T'` write
/// that does fail leaves the split `Unfinished` for the next opener to decide
/// (`split_t_outcome`). Pinned for both faults, so a SlateDB that surfaced either while landing the
/// write would show here.
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

/// The split of `cell`'s parent paused at its park while `merge_finish_k::probe_at_park` runs
/// there on the parent (and, when `corrupt`, `topology.json` is then made unreadable): its result,
/// the park's account and the unfinished index batch.
async fn split_probed(
    cell: &super::split_k::Cell,
    corrupt: bool,
) -> (
    Result<crate::topology::split::Published, crate::topology::split::Refused>,
    super::merge_finish_k::AtPark,
    super::merge_finish_k::Batch,
) {
    use super::merge_finish_k::probe_at_park;
    let store = cell.state.data_store.clone();
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    cell.parent.gc_state().gate.pause_at_park(point);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-q", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        let sources = [cell.parent.clone()];
        let probed = probe_at_park(&store, &sources, cell.addr, [&cell.low, &cell.high]).await;
        if corrupt {
            super::merge_finish_k::unreadable_topology(&store).await;
        }
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
        probed
    };
    let (split, (at, batch)) = futures_util::future::join(split, control).await;
    (split, at, batch)
}

/// A split cell whose parent holds a record of each half.
async fn cell_with_records() -> super::split_k::Cell {
    use super::merge_finish_k::append_answer;
    let cell = cell().await;
    for (name, n) in [(&cell.low, 20), (&cell.high, 21)] {
        assert_eq!(append_answer(cell.addr, name, n).await.0, 204, "{name}");
    }
    cell
}

/// **A quiesced parent writes nothing until the split publishes** (the split's twin of
/// `merge_finish_k::a_quiesced_source_writes_nothing_until_the_merge_publishes`). At the split's
/// park the probe runs on the parent. Released, the split publishes; the index batch is then refused
/// as final (no write after `'T'`), and neither child holds the probe row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiesced_parent_writes_nothing_until_the_split_publishes() {
    use super::merge_finish_k::{assert_quiet, probe_row};
    let cell = cell_with_records().await;
    let (split, at, batch) = split_probed(&cell, false).await;
    assert_quiet(&at);
    split.expect("the split publishes");
    let parent = [cell.parent.clone()];
    super::merge_finish_k::refused_as_final(&cell.state.data_store, &parent, batch).await;
    for prefix in ["000", "001"] {
        let child = super::merge_k::opened(&cell.state, prefix).await;
        let row = child.db.get(probe_row()).await.unwrap();
        assert_eq!(row, None, "no probe row in {prefix}");
    }
    engine_shutdown(&cell.state).await;
}

/// **A quiesced parent writes nothing until the split rolls back** (the rollback variant). A
/// manifest that does not decode sits at the first child's path, so its clone fails inside the
/// pause. At the park the probe runs on the parent. Released, the split refuses `RolledBack` naming
/// that path; the index batch then lands on the parent, which holds the probe row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiesced_parent_writes_nothing_until_the_split_rolls_back() {
    use super::merge_finish_k::{assert_quiet, probe_row};
    let cell = cell_with_records().await;
    let home = cell.state.shards.prefixes().shards.next_home;
    let path = crate::topology::model::born_home_path("000", home);
    let junk = format!("{path}/manifest/00000000000000000001.manifest");
    let junk = object_store::path::Path::from(junk.as_str());
    let store = cell.state.data_store.clone();
    object_store::ObjectStoreExt::put(&*store, &junk, b"junk".to_vec().into())
        .await
        .unwrap();
    let (split, at, batch) = split_probed(&cell, false).await;
    assert_quiet(&at);
    let refused = split.err().expect("the split rolls back");
    assert!(
        matches!(&refused, crate::topology::split::Refused::RolledBack(why) if why.contains(&path)),
        "{refused}"
    );
    let ended = tokio::time::timeout(std::time::Duration::from_secs(20), batch).await;
    assert_eq!(ended, Ok(Ok(())), "the batch lands once the split ends");
    let row = cell.parent.db.get(probe_row()).await.unwrap();
    assert_eq!(
        row,
        Some(bytes::Bytes::from_static(b"1")),
        "the parent holds the probe row"
    );
    engine_shutdown(&cell.state).await;
}

/// **A quiesced parent writes nothing after the split's `'T'` when its publish fails** (the
/// `Unfinished` variant). At the park the probe runs on the parent, then `topology.json` is made
/// unreadable, so the publish fails after `'T'`: the split refuses `Unfinished`, and the queued
/// index batch is refused as final, the parent's WAL unchanged since.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_quiesced_parent_writes_nothing_after_the_splits_t_when_its_publish_fails() {
    use super::merge_finish_k::assert_quiet;
    let cell = cell_with_records().await;
    let (split, at, batch) = split_probed(&cell, true).await;
    assert_quiet(&at);
    let refused = split.err().expect("the publish fails");
    assert!(
        matches!(&refused, crate::topology::split::Refused::Unfinished(_)),
        "{refused}"
    );
    let parent = [cell.parent.clone()];
    super::merge_finish_k::refused_as_final(&cell.state.data_store, &parent, batch).await;
    engine_shutdown(&cell.state).await;
}
