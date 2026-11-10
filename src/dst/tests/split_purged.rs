#![cfg(test)]
//! A split of a shard holding a purged stream's folded entries (S4-DESIGN §6.2 step 2.2, S3a-DESIGN
//! §2.8). A purge decrements a dead stream's chunks in `'R'` and records `reclaimed` in its `'H'`
//! row; a run-index object written before the purge keeps the released entries until a later fold
//! or merge drops them (`fold::kept_entries`). A split recounts each child's `'R'` from its entries,
//! so a recount that kept them would count chunks the parent's `'R'` no longer does, its sum check
//! would fail, and every later split of the shard would roll back.

use super::billing_closure_debts::stored;
use super::fixture_billing::ack_clean;
use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::purge_lifecycle::{drive, loop_closed, sweep_until_purged};
use super::split_k::{Cell, cell, records, tagged};
use crate::keyspace::tag::{DIRECTORY, POINTER};
use crate::spill::object::ObjectId;
use std::collections::BTreeMap;

/// Each object's `'R'` row of `engine`: its live chunks and bytes.
async fn counted(engine: &crate::shard::ShardEngine) -> BTreeMap<ObjectId, (u64, u64)> {
    let rows = engine.refcount_rows().await.unwrap();
    rows.into_iter()
        .filter_map(|(id, r)| Some((id, r.map(|r| (r.live_chunks, r.live_bytes))?)))
        .collect()
}

/// **A shard whose dead stream was purged after its chunks were folded into a run splits, and the
/// children count only the live stream's chunks.** The leaf `00` holds a stream in each half,
/// sealed into one shared object and folded into a run-index object. The low stream is deleted and
/// purged: its chunks are decremented, so the parent's `'R'` counts only the high stream's, while
/// the run-index object still names the low stream's entries. The split publishes `000` and `001`,
/// the children's `'R'` rows sum to the parent's exactly (`000`, the low half, counts nothing), and
/// the high stream reads back as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_after_a_purge_of_folded_chunks_counts_only_live_entries() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let Cell {
        state,
        addr,
        low,
        high,
        parent,
    } = cell().await;
    drive(&parent);
    parent.seal_requested().await.expect("seal");
    parent.fold_once().await.expect("fold");
    assert!(
        tagged(&parent, DIRECTORY).await.is_empty(),
        "no 'K' row left"
    );
    assert!(
        !tagged(&parent, POINTER).await.is_empty(),
        "the run's 'I' row"
    );
    let desc = stored(&state, &state.deployment.raw_adapter_sref(&low)).await;
    let id = desc.dynamic_segment_identity(0);
    let before = counted(&parent).await;
    ack_clean(&state, &parent, &[id]).await;
    let (status, _, _) = hreq(addr, "DELETE", &format!("/v1/stream/{low}"), &[], b"").await;
    assert_eq!(status, 204, "delete {low}");
    loop_closed(&state, &parent, id).await;
    sweep_until_purged(&state, &parent, id).await;
    let purged = counted(&parent).await;
    assert_ne!(
        purged, before,
        "the purge decremented the low stream's chunks"
    );
    assert!(
        !tagged(&parent, POINTER).await.is_empty(),
        "the run-index object still names the released entries"
    );
    let want = records(addr, &high).await;

    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-purged", "inst-a", &*clock)
            .await
            .expect("the split publishes");
    let mut summed: BTreeMap<ObjectId, (u64, u64)> = BTreeMap::new();
    let mut per_child = Vec::new();
    for (prefix, _, child) in &published.children {
        let rows = counted(child).await;
        for (object, (chunks, bytes)) in &rows {
            let at = summed.entry(*object).or_default();
            (at.0, at.1) = (at.0 + chunks, at.1 + bytes);
        }
        per_child.push((prefix.clone(), rows.len()));
    }
    assert_eq!(
        summed, purged,
        "the children's 'R' rows sum to the parent's"
    );
    assert_eq!(
        per_child,
        [("000".to_string(), 0), ("001".to_string(), purged.len())],
        "the low half counts nothing"
    );
    assert_eq!(records(addr, &high).await, want);
    engine_shutdown(&state).await;
}
