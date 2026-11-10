#![cfg(test)]
//! The merge's pause and its tail (S6-DESIGN rev 6 §7.2, §7.4): while it holds its two sources
//! parked, both answer as a split's parent does (edge #249 B3); once it attempted its `'T'` it only
//! rolls forward. A leaf that fails to open after the publish leaves the merge published, and a
//! publish that fails leaves the intent live for a claimant. Its rig is `merge_k`'s.

use super::billing_closure_debts::JSON;
use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::merge_k::{Rig, State, halves_of_00, rig, split_00};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    let halves = halves_of_00(&state).await;
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
    let halves = halves_of_00(&state).await;
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

/// An append of `{"n": n}` to `name` through `addr`: its status, `Retry-After` and error code.
async fn append_answer(
    addr: std::net::SocketAddr,
    name: &str,
    n: u32,
) -> (u16, Option<String>, Option<String>) {
    let body = format!(r#"[{{"n":{n}}}]"#);
    let path = format!("/v1/stream/{name}");
    let (status, headers, body) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let code = error["error"]["code"].as_str().map(str::to_string);
    (status, headers.get("retry-after").cloned(), code)
}

/// The records of `name`.
async fn records(addr: std::net::SocketAddr, name: &str) -> Vec<serde_json::Value> {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    let read: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    read.as_array().cloned().unwrap_or_default()
}

/// **A merge pauses both its sources as a split pauses its parent** (S6 §7.2, edge #249 B3). At
/// the merge's park both sources' committers are parked: an append to a stream of either source
/// is answered after the 2 s hold, `503 shard_splitting` with `Retry-After: 1`, and so is a later
/// one, at once. Released, the merge publishes, and an append to each stream is served by the
/// merged leaf; neither stream holds a refused record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_pauses_both_sources_with_503_shard_splitting_and_serves_after_its_publish() {
    let Rig {
        state,
        addr,
        low,
        high,
        parent,
        ..
    } = rig().await;
    split_00(&state, &parent).await;
    let halves = halves_of_00(&state).await;
    let before = (records(addr, &low).await, records(addr, &high).await);
    let splitting = (
        503,
        Some("1".to_string()),
        Some("shard_splitting".to_string()),
    );
    let answers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (seen, names) = (answers.clone(), [low.clone(), high.clone()]);
    let paused = move || async move {
        // Both sources parked at once: their held appends go out together, each waits its hold.
        let sent_at = Instant::now();
        let (held_low, held_high) = futures_util::future::join(
            append_answer(addr, &names[0], 10),
            append_answer(addr, &names[1], 11),
        )
        .await;
        let waited = sent_at.elapsed();
        for (name, held, n) in [(&names[0], held_low, 20), (&names[1], held_high, 21)] {
            let sent_at = Instant::now();
            let later = append_answer(addr, name, n).await;
            let at_once = sent_at.elapsed();
            seen.lock()
                .unwrap()
                .push((name.clone(), held, waited, later, at_once));
        }
    };
    let merged = merge_paused_at_park(&state, &halves, "op-p", paused).await;
    merged.expect("the merge publishes");
    for (name, held, waited, later, at_once) in answers.lock().unwrap().iter() {
        assert_eq!(held, &splitting, "{name}: held");
        assert!(
            waited >= &Duration::from_secs(2),
            "{name} held for the hold: {waited:?}"
        );
        assert_eq!(later, &splitting, "{name}: a later append");
        assert!(
            at_once < &Duration::from_secs(1),
            "{name} answered at once: {at_once:?}"
        );
    }
    assert_eq!(answers.lock().unwrap().len(), 2, "both sources answered");
    for (name, n) in [(&low, 30), (&high, 31)] {
        assert_eq!(
            append_answer(addr, name, n).await.0,
            204,
            "{name} after the publish"
        );
    }
    let after = (records(addr, &low).await, records(addr, &high).await);
    let grown = |was: &Vec<serde_json::Value>, n: u32| {
        let mut want = was.clone();
        want.push(serde_json::json!({ "n": n }));
        want
    };
    assert_eq!(after, (grown(&before.0, 30), grown(&before.1, 31)));
    engine_shutdown(&state).await;
}
