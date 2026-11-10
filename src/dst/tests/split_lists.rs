#![cfg(test)]
//! A split's child has a reference list (`refs/<h>`) under every ancestor home it holds rows of,
//! whenever its first holding of that home arrived (S4-DESIGN §7.3; S6 §3.3's open-time detector
//! and §4's sweep both require one). Preparing PUTs a reference list for each home its plan holds;
//! a holding that first lands between that plan and the pause's checkpoint (a seal finishing in
//! the window) has no list to supplement, so the pause PUTs its reference list, never a `-d` list
//! alone. A `-d` list alone was refused `lineage_list_missing` on every open of the child, which
//! then sat in the directory's escalating holdoff (503 `shard_moving`) for good.

use super::fixture_http::engine_shutdown;
use super::fixture_storage::mem;
use super::split_k::{Cell, cell_over, records, tagged};
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::spill::lineage;
use std::sync::Arc;
use std::time::Duration;

/// Until `engaged` counts a parked write, within `bound`.
async fn parked_within(engaged: &std::sync::atomic::AtomicU64, bound: Duration) {
    let deadline = tokio::time::Instant::now() + bound;
    while engaged.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the held write parked"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Whether `path` exists in `store`.
async fn exists(store: &dyn object_store::ObjectStore, path: &str) -> bool {
    object_store::ObjectStoreExt::head(store, &path.into())
        .await
        .is_ok()
}

/// Split `cell`'s parent while its first seal, parked at its segment PUT (`engaged`), lands
/// between Preparing's plan and the quiesce: the test holds the parent's topology gate shared, so
/// the quiesce waits; once Preparing stood collection down (and planned), the seal is released and
/// awaited, then the gate let go. The published split.
async fn split_with_a_late_seal(
    cell: &Cell,
    faults: &FaultStore,
    engaged: &std::sync::atomic::AtomicU64,
) -> crate::topology::split::Published {
    let (state, parent) = (&cell.state, &cell.parent);
    let gate = &parent.gc_state().gate;
    let (landed_tx, landed) = tokio::sync::oneshot::channel();
    let sealing = async {
        let sealed = parent.seal_requested().await.map(drop);
        assert!(
            landed_tx.send(sealed).is_ok(),
            "the control awaits the seal"
        );
    };
    let split = async {
        parked_within(engaged, Duration::from_secs(5)).await;
        until(
            || !gate.collection_off(),
            "collection resumes after the open",
        )
        .await;
        let held = gate.shared().await;
        let clock = state.runtime.clock.clone();
        let split = crate::topology::split::split(&state.shards, parent, "op-l", "inst-a", &*clock);
        let control = async {
            until(|| gate.collection_off(), "the split prepares").await;
            // Preparing's plan is an in-memory scan right after collection stands down; the
            // quiesce then waits on the gate this test holds.
            tokio::time::sleep(Duration::from_millis(200)).await;
            faults.release_hold();
            let sealed = landed.await.expect("the seal ends");
            sealed.expect("the held seal lands before the quiesce");
            drop(held);
        };
        let (published, ()) = tokio::join!(split, control);
        published.expect("the split publishes")
    };
    let ((), published) = tokio::join!(sealing, split);
    published
}

/// Until `holds()`, within 5 s (`what` names the wait).
async fn until(holds: impl Fn() -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !holds() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// **A child whose first holding of its parent's home lands after Preparing's plan opens and
/// serves.** The parent's first seal is held at its segment PUT, and the test holds the parent's
/// topology gate shared, so the split's quiesce waits after Preparing: its plan sees no holding and
/// PUTs no list. The seal is then released and lands, the gate let go, and the pause PUTs each
/// child's reference list under the parent's home (no `-d` list). The split publishes with both
/// children open, none refused `lineage_list_missing`, and both streams read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_child_whose_first_holding_lands_after_the_plan_opens() {
    let faults = FaultStore::uniform(mem(), 0x5eed, FaultPlan::CLEAN);
    let store: Arc<dyn object_store::ObjectStore> = faults.clone();
    // Every segment PUT under the leaf `00`'s root parks from the start, the cell's own background
    // seals included, so no holding of home 0 exists before the split plans.
    let root = crate::sharddir::object_root(0, 2);
    let engaged = faults.hold_class_under(
        StoreOp::Put,
        ObjClass::Other,
        &format!("{root}/hseg/"),
        u64::MAX,
    );
    let cell = cell_over(store.clone()).await;
    let Cell {
        state,
        addr,
        low,
        high,
        parent,
    } = &cell;
    assert_eq!(
        parent.object_root(state.shards.prefixes().shards.leaves["00"]),
        root
    );
    let want = (records(*addr, low).await, records(*addr, high).await);
    let published = split_with_a_late_seal(&cell, &faults, &engaged).await;
    let leaves = state.shards.prefixes().shards.leaves.clone();
    for child_home in [leaves["000"], leaves["001"]] {
        let lists = (
            exists(&*store, &lineage::reference_list_path(&root, child_home)).await,
            exists(
                &*store,
                &lineage::supplementary_list_path(&root, child_home),
            )
            .await,
        );
        assert_eq!(
            lists,
            (true, false),
            "child {child_home}: its reference list, no -d list"
        );
    }
    let opened: Vec<&str> = published
        .children
        .iter()
        .map(|(prefix, _, _)| prefix.as_str())
        .collect();
    assert_eq!(opened, ["000", "001"], "both children opened");
    assert_eq!(
        (records(*addr, low).await, records(*addr, high).await),
        want
    );
    inherited_rows_fold_away(&published, *addr, (low, high), &want).await;
    engine_shutdown(state).await;
}

/// K-SPLIT-DECISION-2026-11-09 §5's first case, end to end: the late seal's `'K'` rows reached both
/// children byte for byte (the sibling's entries included; each child admits only its own range at
/// open and at its fold, `shard::seal::range_tests`), each child's own fold then folds them into its
/// run index, and both streams still read back `want`.
async fn inherited_rows_fold_away(
    published: &crate::topology::split::Published,
    addr: std::net::SocketAddr,
    (low, high): (&str, &str),
    want: &(serde_json::Value, serde_json::Value),
) {
    use crate::keyspace::tag::DIRECTORY;
    let children: Vec<_> = published
        .children
        .iter()
        .map(|(_, _, c)| c.clone())
        .collect();
    let rows = tagged(&children[0], DIRECTORY).await;
    assert!(!rows.is_empty(), "the late seal's 'K' rows were copied");
    assert_eq!(
        tagged(&children[1], DIRECTORY).await,
        rows,
        "to both children"
    );
    for child in &children {
        for _ in 0..8 {
            match child.fold_once().await {
                Ok(report) if report.more => {}
                done => {
                    done.expect("the child's fold");
                    break;
                }
            }
        }
        let left = tagged(child, DIRECTORY).await;
        assert!(
            left.is_empty(),
            "{}: its fold took the inherited rows",
            child.prefix
        );
    }
    assert_eq!((records(addr, low).await, records(addr, high).await), *want);
}
