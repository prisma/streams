#![cfg(test)]
//! The merge past its `'T'` (S6-DESIGN rev 6 §7.2, §7.4): once it attempted its `'T'` it only rolls
//! forward. A leaf that fails to open after the publish leaves the merge published, and a publish
//! that fails leaves the intent live for a claimant. Its rig is `merge_k`'s.

use super::fixture_http::engine_shutdown;
use super::merge_k::{Rig, State, rig, split_00};
use std::sync::Arc;

/// Pause the merge of `halves` at its park (the left half's gate), run `control` there, and let it
/// go: the merge's result.
async fn merge_paused_at_park<F: std::future::Future<Output = ()>>(
    state: &State,
    halves: &[Arc<crate::shard::ShardEngine>; 2],
    op_id: &str,
    control: impl FnOnce() -> F,
) -> Result<crate::topology::merge::Published, crate::topology::merge::Refused> {
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    halves[0].gc_state().gate.pause_at_park(point);
    let clock = state.runtime.clock.clone();
    let merge = crate::topology::merge::merge(
        &state.shards,
        [&halves[0], &halves[1]],
        op_id,
        "inst-a",
        &*clock,
    );
    let controlled = async {
        reached_rx.await.expect("the merge paused at its park");
        control().await;
        assert!(go_tx.send(()).is_ok(), "the merge waits for go");
    };
    let (merged, ()) = tokio::join!(merge, controlled);
    merged
}

/// **A merge whose leaf fails to open after the publish stays published** (S6 §7.2, as the split
/// does; edge #251's view). Junk WAL objects are planted under `M`'s path at the park, so `M`'s
/// clone passes and its open fails: the merge answers `Published` without `M`'s engine (it opens on
/// its next request), the intent ends `Reclaimed`, both sources are retired, and the stored
/// topology has `00` as a leaf again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_leaf_fails_to_open_after_the_publish_stays_published() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    let version = split_00(&state, &parent).await;
    let halves = ["000", "001"].map(|p| state.shards.open(p).expect("the half is open here"));
    let home = state.shards.prefixes().shards.next_home;
    let merged_path = crate::topology::model::born_home_path("00", home);
    let planted = || async {
        for id in 1..=64u64 {
            let wal = format!("{merged_path}/wal/{id:020}.sst");
            let junk = b"not a wal table".to_vec();
            object_store::ObjectStoreExt::put(&*store, &wal.as_str().into(), junk.into())
                .await
                .unwrap();
        }
    };
    let merged = merge_paused_at_park(&state, &halves, "op-o", planted)
        .await
        .expect("published, M unopened");
    assert_eq!(merged.version, version + 1);
    assert_eq!((merged.merged.0.as_str(), merged.merged.1), ("00", home));
    assert!(merged.merged.2.is_none(), "M did not open");
    let ended = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    assert!(
        halves.iter().all(|h| h.is_closed()),
        "both sources are retired"
    );
    let (topology, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(topology.version, version + 1);
    assert_eq!(topology.shards.leaves.get("00"), Some(&home));
    engine_shutdown(&state).await;
}

/// **A merge whose publish fails after its `'T'` is left live, never rolled back** (S6 §7.4; edge
/// #251's view). `topology.json` is made unreadable at the park, so the publish fails after both
/// `'T'`s: the merge refuses `Unfinished`, its intent stays live (`Preparing`, for the next opener
/// of a source to claim and roll forward), and nothing is undone: `M`'s reference list under the
/// parent's root is still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_publish_fails_after_t_is_left_live() {
    let Rig {
        store,
        state,
        parent,
        ..
    } = rig().await;
    let parent_root = state.shards.prefixes().shards.homes
        [&state.shards.prefixes().shards.leaves["00"]]
        .path
        .clone();
    split_00(&state, &parent).await;
    let halves = ["000", "001"].map(|p| state.shards.open(p).expect("the half is open here"));
    let home = state.shards.prefixes().shards.next_home;
    let corrupt = || async {
        let junk = b"{".to_vec();
        object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), junk.into())
            .await
            .unwrap();
    };
    let refused = merge_paused_at_park(&state, &halves, "op-u", corrupt)
        .await
        .err()
        .expect("the publish fails");
    assert!(
        matches!(&refused, crate::topology::merge::Refused::Unfinished(_)),
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
    let list = crate::spill::lineage::reference_list_path(&parent_root, home);
    let kept = object_store::ObjectStoreExt::get(&*store, &list.as_str().into()).await;
    assert!(
        kept.is_ok(),
        "M's list under the parent's root is kept: {list}"
    );
    engine_shutdown(&state).await;
}
