#![cfg(test)]
//! A split's or a merge's seeded clone whose clock runs behind its sources' (`topology::clone`).
//!
//! Every clone in the rig's store builds with a SlateDB clock 300 ms behind its sources'
//! (`clone::Lag`), as when a source engine opened long ago mints ahead of a fresh clock. The
//! operation publishes, through `clone::seeded`, on this tree and on the one before the retry: the
//! fork does not refuse a seed minted behind, so this rules that out as the cause of the clock
//! refusal NEXT-WORK §19 still has to name, and pins that both clones build through the retry.

use std::time::Duration;

use super::merge_k::{Rig, append_to, halves_of_00, records, rig, split_00};

/// How far the clones' clocks run behind their sources'.
const LAG: Duration = Duration::from_millis(300);

/// **A merge whose clone's clock runs behind its sources' publishes** (S6 §7.2). After a split
/// of `00`, one more append to each half's stream, so each source's checkpoint flushes an L0
/// table at the merge; `M`'s seed is minted 300 ms behind it. The merge publishes `00` at the
/// next version, and both streams read back their records with the new ones, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_clone_clock_runs_behind_its_sources_publishes() {
    let Rig {
        state,
        addr,
        low,
        high,
        parent,
        ..
    } = rig().await;
    let version = split_00(&state, &parent).await;
    for (name, n) in [(&low, 5), (&high, 6)] {
        assert_eq!(append_to(addr, name, n).await.0, 204, "append to {name}");
    }
    let want = (records(addr, &low).await, records(addr, &high).await);
    let halves = halves_of_00(&state).await;
    let lag = crate::topology::clone::Lag::of(&**halves[0].data_store(), LAG);
    let clock = state.runtime.clock.clone();
    let merged = crate::topology::merge::merge(
        &state.shards,
        [&halves[0], &halves[1]],
        "op-m",
        "inst-a",
        &*clock,
    )
    .await;
    drop(lag);
    assert_eq!(
        merged
            .map(|published| published.version)
            .map_err(|refused| format!("{refused:?}")),
        Ok(version + 1),
        "the merge publishes past its clone's clock skew"
    );
    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    super::fixture_http::engine_shutdown(&state).await;
}

/// **A split whose children's clones' clock runs behind the parent's publishes** (S4 §6.2 step
/// 2.4). One more append to each half's stream of `00`, so the parent's checkpoint flushes an
/// L0 table at the split; each child's seed is minted 300 ms behind it. The split publishes `000`
/// and `001` at the next version, and both streams read back their records, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_whose_clone_clock_runs_behind_its_parent_publishes() {
    let Rig {
        state,
        addr,
        low,
        high,
        parent,
        ..
    } = rig().await;
    for (name, n) in [(&low, 5), (&high, 6)] {
        assert_eq!(append_to(addr, name, n).await.0, 204, "append to {name}");
    }
    let want = (records(addr, &low).await, records(addr, &high).await);
    let lag = crate::topology::clone::Lag::of(&**parent.data_store(), LAG);
    let clock = state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&state.shards, &parent, "op-s", "inst-a", &*clock).await;
    drop(lag);
    let split = split.map(|published| (published.version, published.children.len()));
    assert_eq!(
        split.map_err(|refused| format!("{refused:?}")),
        Ok((2, 2)),
        "the split publishes past its clones' clock skew"
    );
    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    super::fixture_http::engine_shutdown(&state).await;
}
