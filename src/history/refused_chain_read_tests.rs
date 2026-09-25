//! HOLD-SPLIT-500 closure (owner decision, second external review): a
//! refused chain's overlapping postings pages, composed with the keyed read
//! paths the public read uses. Built on `bounded_discovery_tests`' rig.
#![cfg(test)]

use super::bounded_discovery_tests::{
    append_stored, applied_tail, enqueue_record, pages_tile, rig, stored_len, wait_until,
};
use super::{read_history2, read_history2_keyed_cached};
use crate::crypto::{RouteHash, SegmentHash};
use crate::shard::ShardEngine;
use std::sync::Arc;

/// Builds the refused-chain state the absorber review found (NEXT-WORK
/// item 1): G1 [0,4) waits at the held commit gate while G2 chains [4,8)
/// from the mark and flushes its pages; the group carrying both is refused;
/// the heal G3 regathers [0,9) from the durable boundary. Returns the
/// engine and each record's stored bytes (offsets 0..=9).
async fn refused_chain(name: &str, hash: [u8; 16]) -> (Arc<ShardEngine>, Vec<u64>) {
    let (engine, absorber, store) = rig(name).await;
    let handle = engine.stream_handle(hash).await.unwrap();
    let mut stored = Vec::new();
    for _ in 0..4 {
        append_stored(&engine, hash, &mut stored).await;
    }
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let held: Vec<_> = (0..4).map(|_| enqueue_record(&engine, hash)).collect();
    wait_until("four records applied behind a held WAL write", || {
        engaged.load(std::sync::atomic::Ordering::SeqCst) >= 1
            && handle.state.lock().unwrap().applied.next == 8
    })
    .await;
    let gate = engine.test_hold_commit().await;
    let g1 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        g1.advanced.first().map(|a| a.1),
        Some(4),
        "G1 gathers [0, 4)"
    );
    store.release_hold();
    for answer in held {
        answer.await.unwrap().unwrap();
    }
    for offset in 4..8 {
        stored.push(stored_len(&engine, &hash, offset).await.unwrap());
    }
    let g2 = absorber.absorb_gather_v2(&[hash]).await.unwrap();
    assert_eq!(
        g2.advanced.first().map(|a| a.1),
        Some(8),
        "G2 chains [4, 8)"
    );
    engine.fail_next_absorbed_group();
    drop(gate);
    wait_until("the chained group refused", || {
        engine.group_failures_tripped() >= 1
    })
    .await;
    append_stored(&engine, hash, &mut stored).await;
    absorber.absorb_gather_v2(&[hash]).await.unwrap();
    append_stored(&engine, hash, &mut stored).await;
    let tail = applied_tail(&engine, hash).await;
    assert_eq!(tail.absorbed, 9, "the refused chain was healed");
    assert_eq!(
        tail.unabsorbed_bytes, stored[9],
        "the healed ledger is exact"
    );
    (engine, stored)
}

/// The offsets of `frames`.
fn offsets(frames: &[crate::shard::record::CheckedFrame]) -> Vec<u64> {
    frames.iter().map(|f| f.view().header.offset).collect()
}

/// HOLD-SPLIT-500 closure (owner decision, second external review): a
/// refused chain's heal re-gathers rows its chained advance had already
/// paged, so the stream's postings pages overlap. Readers admit an
/// overlapping page whose offsets agree with those already admitted
/// (d16559b3), so no read falls back to the envelope scan. Composed with
/// the public read: both keyed read paths (the plain one and the cached one
/// the public read calls) answer exactly offsets 0..9, each once, in order,
/// (the counted corruption fallback is a process-wide counter, so the
/// admission is pinned by the pages tiling instead); and a paged read under
/// a small byte budget
/// continues honestly from each returned cursor, never skipping or
/// repeating a record and never reading far past its budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_chains_overlapping_pages_still_read_exactly() {
    let hash = [0x4b; 16];
    let (engine, stored) = refused_chain("refused-chain-read", hash).await;
    assert!(
        pages_tile(&engine, hash).await,
        "the agreeing overlap is admitted"
    );
    let part = engine.history_partition().await.unwrap();
    let (route, inc) = (RouteHash(hash), SegmentHash(hash));
    let all: Vec<u64> = (0..9).collect();
    for cached in [false, true] {
        let read = if cached {
            read_history2_keyed_cached(
                &engine.postings_cache,
                &part,
                route,
                inc,
                "",
                0,
                9,
                9,
                1 << 20,
            )
            .await
        } else {
            read_history2(&part, route, inc, 0, 9, Some(""), 1 << 20).await
        };
        let (frames, _, completed) = read.unwrap();
        assert_eq!(
            (offsets(&frames), completed),
            (all.clone(), true),
            "cached={cached}"
        );
    }
    // Two records' bytes per page: every page but the last stops on its
    // budget and names the last offset it scanned.
    let budget = usize::try_from(stored[0] * 2).unwrap();
    let (mut from, mut seen, mut pages) = (0, Vec::new(), 0);
    loop {
        pages += 1;
        assert!(pages <= 9, "the paged read did not finish");
        let (frames, last, completed) = read_history2(&part, route, inc, from, 9, Some(""), budget)
            .await
            .unwrap();
        let bytes: usize = frames.iter().map(|f| f.len()).sum();
        assert!(
            bytes <= budget + usize::try_from(stored[0]).unwrap(),
            "a page read past its budget"
        );
        seen.extend(offsets(&frames));
        if completed {
            break;
        }
        from = last.expect("a partial page names where it stopped") + 1;
    }
    assert_eq!(seen, all, "the paged read skipped or repeated a record");
    engine.begin_close();
}
