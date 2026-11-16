#![cfg(test)]
//! A split of a shard whose folded chunks a newer object superseded (S4-DESIGN §6.2 step 2.2;
//! GATE-DECISION-meas149-reshaping-2026-11-16 F1a). A cleaner's copy and a consolidation each
//! decrement the old object's `'R'` for every chunk they copy and write the new object's rows, but
//! leave the old entries in the run-index object until a later fold or merge drops them. The
//! reclaim's live set (`spill::gc::live_entries`) drops a superseded entry; a split's recount that
//! kept it would count a chunk the parent's `'R'` no longer does, its sum check would fail, and
//! every split of the shard would roll back until a fold stripped the old entries.

use super::billing_closure_debts::stored;
use super::fixture_billing::ack_clean;
use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::fixture_split::{Cell, append, cell, records};
use super::purge_lifecycle::{drive, loop_closed, sweep_until_purged};
use super::split_k::tagged;
use super::split_purged::counted;
use crate::keyspace::tag::POINTER;
use crate::spill::object::ObjectId;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// The split of `cell`'s leaf `00`, which must publish: the children's `'R'` rows summed per
/// object, and the parent's before it.
async fn split_publishes(cell: &Cell, op: &str) -> BTreeMap<ObjectId, (u64, u64)> {
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, op, "inst-a", &*clock);
    let published = split.await.expect("the split publishes");
    let mut summed: BTreeMap<ObjectId, (u64, u64)> = BTreeMap::new();
    for (_, _, child) in &published.children {
        for (object, (chunks, bytes)) in counted(child).await {
            let at = summed.entry(object).or_default();
            (at.0, at.1) = (at.0 + chunks, at.1 + bytes);
        }
    }
    summed
}

/// **A shard whose cleaner copied a folded chunk splits, and the children count the copy, not its
/// original.** The leaf `00` holds a stream in each half, sealed into one shared object and folded
/// into a run-index object. The low stream is deleted and purged, so the object is half dead, and
/// the cleaner copies the high stream's chunk into a new object: the original's `'R'` row is
/// zeroed, while the run-index object still names its entry. With no further fold, the split
/// publishes, the children's `'R'` rows sum to the parent's per object (the copy's, not the
/// original's), and the high stream reads back as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_after_a_cleaners_copy_counts_the_copy_not_its_original() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let cell = cell().await;
    let (state, addr, parent) = (&cell.state, cell.addr, &cell.parent);
    drive(parent);
    parent
        .gc_state()
        .configure(|s| s.clean_pays = |_, _, live_chunks, _| live_chunks > 0);
    parent.seal_requested().await.expect("seal");
    parent.fold_once().await.expect("fold");
    let folded = counted(parent).await;
    let desc = stored(state, &state.deployment.raw_adapter_sref(&cell.low)).await;
    let id = desc.dynamic_segment_identity(0);
    ack_clean(state, parent, &[id]).await;
    let path = format!("/v1/stream/{}", cell.low);
    assert_eq!(hreq(addr, "DELETE", &path, &[], b"").await.0, 204, "delete");
    loop_closed(state, parent, id).await;
    sweep_until_purged(state, parent, id).await;
    // The collector's passes run the cleaner too; a pass of its own copies what they left.
    parent.clean_pass().await.expect("clean");
    let copied = parent
        .gc_state()
        .counters
        .cleaned_chunks
        .load(Ordering::Relaxed);
    assert!(copied > 0, "the cleaner copied");
    let cleaned = counted(parent).await;
    let originals: Vec<_> = folded.keys().filter(|o| !cleaned.contains_key(o)).collect();
    assert!(
        !originals.is_empty(),
        "the copy zeroed its originals: {folded:?} {cleaned:?}"
    );
    assert!(
        !tagged(parent, POINTER).await.is_empty(),
        "the run still names them"
    );
    let want = records(addr, &cell.high).await;

    let summed = split_publishes(&cell, "op-cleaned").await;
    assert_eq!(
        summed, cleaned,
        "the children's 'R' rows sum to the parent's"
    );
    assert_eq!(records(addr, &cell.high).await, want);
    engine_shutdown(state).await;
}

/// The consolidator of `engine` finished `n` jobs, within 10 s.
async fn consolidated(engine: &crate::shard::ShardEngine, n: u64) {
    let t0 = Instant::now();
    loop {
        let c = engine.seal_json()["consolidation"].clone();
        if c["done"].as_u64() >= Some(n) {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "no consolidation: {c}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// **A shard whose folded small chunks a read consolidated splits.** The high stream of the leaf
/// `00` takes ten more records, each sealed into its own object, and the leaf folds them all into a
/// run-index object. A read of the stream from its start fetches the small chunks and consolidates
/// them into one chunk of a new object: each original's `'R'` loses those chunks, while the run
/// still names them. With no further fold, the split publishes, the children's `'R'` rows sum to
/// the parent's per object, and the stream reads back as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_after_a_consolidation_of_folded_chunks_publishes() {
    let cell = cell().await;
    let (addr, parent) = (cell.addr, &cell.parent);
    drive(parent);
    for n in 0..10 {
        assert_eq!(append(addr, &cell.high, 10 + n).await.0, 204, "append {n}");
        parent.seal_requested().await.expect("seal");
    }
    parent.fold_once().await.expect("fold");
    let folded = counted(parent).await;
    let want = records(addr, &cell.high).await;
    consolidated(parent, 1).await;
    let after = counted(parent).await;
    assert_ne!(after, folded, "the consolidation moved the chunks");
    assert!(
        !tagged(parent, POINTER).await.is_empty(),
        "the run still names them"
    );

    let summed = split_publishes(&cell, "op-consolidated").await;
    assert_eq!(summed, after, "the children's 'R' rows sum to the parent's");
    assert_eq!(records(addr, &cell.high).await, want);
    engine_shutdown(&cell.state).await;
}
