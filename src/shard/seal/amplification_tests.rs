#![cfg(test)]
//! What a sealed read costs: a ranged GET starts at the last anchor at or
//! before its window and stops at the first anchor past the window or the
//! byte budget, so its bytes exceed the pages it serves by at most about one
//! anchor span at each end (HIS-018); one read has at most eight GETs in
//! flight and fetches at most 256 chunks (HIS-017).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::{SeedableRng, rngs::StdRng};

use super::Trigger;
use super::test_hooks::Hold;
use super::tests::{
    Oracle, ReadAt, append_recorded, hashes, manual, merged, random_bytes, rig_with,
};
use crate::shard::Deliver;
use crate::spill::object::ANCHOR_BYTES;

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// One record a page, this large.
const RECORD: usize = 16 << 10;
/// A page's bytes besides its record (header, tag, page table), generously.
const PAGE_OVERHEAD: u64 = 1 << 10;

/// **HIS-018.** One stream sealed in a single 8 MiB chunk of 16 KiB pages
/// (anchors every 256 KiB), read without a ring from offsets across it with
/// budgets from one byte to 1 MiB: every read's GETs ask for at most the
/// bytes of the pages it served plus two anchor spans and two pages, while
/// for a read of up to 64 KiB the whole chunk is at least eight times that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_read_fetches_at_most_an_anchor_span_beyond_its_pages() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 1024;
    let rig = rig_with("amplification", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xA4F);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = std::sync::Mutex::new(Oracle::new());
    for _ in 0..512 {
        append_recorded(
            &rig.engine,
            &oracle,
            hash,
            vec![random_bytes(&mut rng, RECORD)],
        )
        .await;
    }
    let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!((report.chunks, report.pages), (1, 512), "{report:?}");
    let sp = &rig.engine.layout.seal;
    let page = RECORD as u64 + PAGE_OVERHEAD;
    for (from, budget) in [
        (0, 1),
        (100, 1),
        (255, 64 << 10),
        (300, 300 << 10),
        (128, 1 << 20),
        (511, 1),
    ] {
        let (bytes, gets) = (get(&sp.hooks.get_bytes), get(&sp.counters.sealed_gets));
        let read = merged(
            &rig.engine,
            hash,
            ReadAt::all(from, budget),
            Deliver::Durable,
        )
        .await
        .unwrap();
        let fetched = get(&sp.hooks.get_bytes) - bytes;
        let served = read.recs.len() as u64;
        assert!(served > 0, "no progress from {from}");
        assert_eq!(read.recs.iter().next().map(|r| r.off), Some(from));
        assert_eq!(
            get(&sp.counters.sealed_gets) - gets,
            1,
            "one ranged GET for one chunk"
        );
        let bound = served * page + 2 * ANCHOR_BYTES + 2 * page;
        assert!(
            fetched <= bound,
            "from {from}, budget {budget}: fetched {fetched} B for {served} pages (bound {bound} B)"
        );
        if budget <= 64 << 10 {
            assert!(
                bound * 8 <= report.page_bytes,
                "the bound is no bound: chunk {} B",
                report.page_bytes
            );
        }
    }
    rig.engine.begin_close();
}

/// **HIS-017.** One stream sealed one page a chunk, 300 chunks, read without
/// a ring and with every GET parked: exactly eight are in flight at once;
/// released, the read fetches 256 chunks and serves their 256 records, and
/// the next read goes on from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_read_has_eight_gets_in_flight_and_fetches_at_most_256_chunks() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 1;
    let rig = rig_with("inflight", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x1F8);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = std::sync::Mutex::new(Oracle::new());
    for _ in 0..300 {
        append_recorded(&rig.engine, &oracle, hash, vec![random_bytes(&mut rng, 64)]).await;
    }
    let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!(report.chunks, 300, "{report:?}");
    let sp = &rig.engine.layout.seal;
    let hold = Hold::new();
    *sp.hooks.get_hold.lock().unwrap() = Some(hold.clone());
    let gets = get(&sp.counters.sealed_gets);
    let peak = async {
        while hold.entered.load(Ordering::Acquire) < 8 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Long enough for a ninth GET to start if the reader allowed one.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let peak = hold.entered.load(Ordering::Acquire);
        hold.release();
        peak
    };
    let (read, peak) = tokio::join!(
        merged(
            &rig.engine,
            hash,
            ReadAt::all(0, 64 << 20),
            Deliver::Durable
        ),
        peak
    );
    let read = read.unwrap();
    assert_eq!(peak, 8, "GETs in flight at once");
    assert_eq!(
        get(&sp.counters.sealed_gets) - gets,
        256,
        "chunks one read fetches"
    );
    let offsets: Vec<u64> = read.recs.iter().map(|r| r.off).collect();
    assert_eq!(offsets, (0..256).collect::<Vec<_>>());
    assert!(
        !read.completed,
        "the read stopped at its chunk cap, not the stream's end"
    );
    let rest = merged(
        &rig.engine,
        hash,
        ReadAt::all(256, 64 << 20),
        Deliver::Durable,
    )
    .await
    .unwrap();
    assert_eq!(
        rest.recs.iter().map(|r| r.off).collect::<Vec<_>>(),
        (256..300).collect::<Vec<_>>()
    );
    rig.engine.begin_close();
}
