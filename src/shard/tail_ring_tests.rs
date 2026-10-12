//! The durable-tail ring's eviction bookkeeping across a gap reset, and the
//! pages a ring read inspects.
use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::{PageCipher, PageLane, stamped};
use crate::shard::{RingBatch, RingScan, ShardConfig, ShardEngine, ShardMaintenance, StreamHandle};
use bytes::Bytes;
use slatedb::Db;
use std::sync::Arc;

/// An engine on the base arm's ring (the engine-wide FIFO budget), whatever
/// `L6_SPILL` says: the tests that call this pin the base path.
async fn engine(tail_ring_bytes: usize) -> Arc<ShardEngine> {
    engine_cfg(ShardConfig {
        tail_ring_bytes,
        l6_spill: false,
        ..Default::default()
    })
    .await
}

async fn engine_cfg(cfg: ShardConfig) -> Arc<ShardEngine> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("tail-ring-tests", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    ShardEngine::start(
        "tail-ring-tests".into(),
        db,
        store,
        cfg,
        tx,
        None,
        ShardMaintenance::default(),
    )
}

/// A batch of `n` opaque one-record pages of `size` bytes at consecutive
/// offsets from `from`.
fn frames(from: u64, n: u64, size: usize) -> RingBatch {
    let mut batch = RingBatch::default();
    for offset in from..from + n {
        batch.push_page(offset, offset, Bytes::from(vec![0u8; size]));
    }
    batch
}

fn batch_starts(handle: &StreamHandle) -> Vec<u64> {
    handle
        .ring
        .lock()
        .unwrap()
        .batches
        .iter()
        .map(|b| b.first)
        .collect()
}

/// A gap reset drops the reset stream's stale batches and forgets only
/// ITS eviction slot: the other streams keep theirs, so budget pressure
/// still evicts them in publish order while the reset stream's later
/// batches survive behind them.
#[tokio::test]
async fn gap_reset_keeps_the_other_streams_eviction_slots() {
    let engine = engine(3 * 1024).await;
    let a = engine.stream_handle([1; 16]).await.unwrap();
    let b = engine.stream_handle([2; 16]).await.unwrap();
    engine.ring_publish(&a, &frames(0, 1, 1024));
    engine.ring_publish(&b, &frames(0, 1, 1024));
    // A's next batch skips offsets 1..5: its stale batch is dropped and its
    // slot re-queued behind B's.
    engine.ring_publish(&a, &frames(5, 1, 1024));
    assert_eq!(batch_starts(&a), vec![5]);
    assert_eq!(engine.ring_resident_bytes(), 2 * 1024);
    // Two more frames overrun the budget by one: the slot at the front of
    // the queue, B's, is evicted; A's batches are untouched.
    engine.ring_publish(&a, &frames(6, 2, 1024));
    assert_eq!(
        batch_starts(&a),
        vec![5, 6],
        "the reset stream keeps its later batches"
    );
    assert!(
        batch_starts(&b).is_empty(),
        "the other stream's slot was evicted first"
    );
    assert_eq!(engine.ring_resident_bytes(), 3 * 1024);
}

/// The stored page of the one record at `offset`, as the shard log seals it.
fn sealed_page(offset: u64) -> Bytes {
    let subkey = derive_subkey(&StreamKey([7; 32]), &[9; 16], "", 1);
    let lane = PageLane {
        key_version: 1,
        routing_key: "",
    };
    let page = PageCipher::new(&subkey, &[8; 16])
        .seal(&lane, offset, &stamped(0, &[b"record"]))
        .unwrap();
    Bytes::from(page.bytes)
}

/// A ring read inspects no batch that starts at its window's end: the
/// window [0, 2) is served from its own batch although the next batch, at
/// 2, holds a page that fails admission. The control: the window [0, 3),
/// which reaches that page, is refused, so the served window never
/// admitted it.
#[tokio::test]
async fn a_ring_read_inspects_no_batch_from_its_window_end() {
    for spill in [false, true] {
        a_ring_read_inspects_no_batch_from_its_window_end_on(spill).await;
    }
}

async fn a_ring_read_inspects_no_batch_from_its_window_end_on(l6_spill: bool) {
    let engine = engine_cfg(ShardConfig {
        tail_ring_bytes: 1 << 20,
        l6_spill,
        ..Default::default()
    })
    .await;
    let handle = engine.stream_handle([3; 16]).await.unwrap();
    let mut window = RingBatch::default();
    window.push_page(0, 0, sealed_page(0));
    window.push_page(1, 1, sealed_page(1));
    engine.ring_publish(&handle, &window);
    let mut beyond = RingBatch::default();
    beyond.push_page(2, 2, Bytes::from_static(b"broken"));
    engine.ring_publish(&handle, &beyond);
    handle.state.lock().unwrap().durable.next = 3;
    let scan = |to| RingScan {
        from: 0,
        to,
        max_bytes: usize::MAX,
    };
    let hit = engine.ring_read(&handle, scan(2), None).unwrap();
    let served: Vec<(u64, u64)> = hit.frames.iter().map(|s| (s.first(), s.last())).collect();
    assert_eq!(served, vec![(0, 0), (1, 1)]);
    assert_eq!(hit.last_offset, Some(1));
    assert!(engine.ring_read(&handle, scan(3), None).is_none());
}

/// A ring read skips every batch that ends at or before its window's
/// start without inspecting its pages: the window [2, 3) is served from the
/// batch at 2 although the batch before it, which ends at 2, holds an entry
/// that claims offset 2 and fails admission. The control: the window
/// [1, 3), which starts inside that batch, inspects the entry and is
/// refused.
#[tokio::test]
async fn a_ring_read_inspects_no_batch_that_ends_at_its_window_start() {
    for spill in [false, true] {
        a_ring_read_inspects_no_batch_that_ends_at_its_window_start_on(spill).await;
    }
}

async fn a_ring_read_inspects_no_batch_that_ends_at_its_window_start_on(l6_spill: bool) {
    let engine = engine_cfg(ShardConfig {
        tail_ring_bytes: 1 << 20,
        l6_spill,
        ..Default::default()
    })
    .await;
    let handle = engine.stream_handle([4; 16]).await.unwrap();
    let before = RingBatch {
        first: 0,
        next: 2,
        frames: vec![(2, Bytes::from_static(b"broken"))],
        bytes: 6,
    };
    engine.ring_publish(&handle, &before);
    let mut window = RingBatch::default();
    window.push_page(2, 2, sealed_page(2));
    engine.ring_publish(&handle, &window);
    handle.state.lock().unwrap().durable.next = 3;
    let scan = |from| RingScan {
        from,
        to: 3,
        max_bytes: usize::MAX,
    };
    let hit = engine.ring_read(&handle, scan(2), None).unwrap();
    let served: Vec<(u64, u64)> = hit.frames.iter().map(|s| (s.first(), s.last())).collect();
    assert_eq!(served, vec![(2, 2)]);
    assert_eq!(hit.last_offset, Some(2));
    assert!(engine.ring_read(&handle, scan(1), None).is_none());
}

// ---- L6 spike M2: the spill arm's ring under a process budget ----------

use crate::shard::{RingBudget, RingSettings, TailFields};
use rand::{Rng, SeedableRng, rngs::StdRng};

fn settings(process: u64, cap: u64, floor: u64) -> RingSettings {
    RingSettings {
        process_bytes: process,
        shard_cap_bytes: cap,
        shard_floor_bytes: floor,
    }
}

async fn spill_engine(budget: &Arc<RingBudget>) -> Arc<ShardEngine> {
    engine_cfg(ShardConfig {
        l6_spill: true,
        shared_ring: Some(budget.clone()),
        ..Default::default()
    })
    .await
}

fn share(engine: &ShardEngine) -> &super::tail_ring::RingShare {
    engine.ring_share.as_ref().expect("spill arm ring")
}

/// Set a stream's durable absorbed boundary (and its next), as a committed
/// seal would.
fn set_absorbed(handle: &StreamHandle, absorbed: u64, next: u64) {
    let mut st = handle.state.lock().unwrap();
    st.durable = TailFields {
        absorbed,
        next,
        ..st.durable.clone()
    };
}

fn counter(a: &std::sync::atomic::AtomicU64) -> u64 {
    a.load(std::sync::atomic::Ordering::Relaxed)
}

/// Past its share, the spill arm's ring evicts only batches below their
/// stream's absorbed boundary, oldest first, and otherwise refuses the
/// batch; a stream whose earlier batch was refused is refused until its
/// ring holds only absorbed pages, and is then restarted.
#[tokio::test]
async fn spill_ring_refuses_instead_of_evicting_unabsorbed_pages() {
    let budget = RingBudget::new(settings(4096, 4096, 4096));
    let engine = spill_engine(&budget).await;
    let a = engine.stream_handle([1; 16]).await.unwrap();
    let b = engine.stream_handle([2; 16]).await.unwrap();
    let s = share(&engine);
    for o in 0..4 {
        engine.ring_publish(&a, &frames(o, 1, 1024));
    }
    assert_eq!(batch_starts(&a), vec![0, 1, 2, 3]);
    assert_eq!(s.resident(), 4096);
    engine.ring_publish(&a, &frames(4, 1, 1024));
    assert_eq!(batch_starts(&a), vec![0, 1, 2, 3], "full of unabsorbed pages: refused");
    assert_eq!((counter(&s.refused), counter(&s.refused_gap)), (1, 0));
    engine.ring_publish(&a, &frames(5, 1, 1024));
    assert_eq!((counter(&s.refused), counter(&s.refused_gap)), (2, 1), "a gap behind unabsorbed pages");
    engine.ring_publish(&b, &frames(0, 1, 1024));
    assert!(batch_starts(&b).is_empty(), "another stream cannot evict them either");
    assert_eq!(counter(&engine.ring_evicted), 0);
    // A seal absorbs a through 2: b's batches now evict a's [0] and [1].
    set_absorbed(&a, 2, 6);
    engine.ring_publish(&b, &frames(0, 1, 1024));
    engine.ring_publish(&b, &frames(1, 1, 1024));
    assert_eq!(batch_starts(&a), vec![2, 3]);
    assert_eq!(batch_starts(&b), vec![0, 1]);
    // a's front batch [2, 3) is at absorbed: nothing more is evictable.
    engine.ring_publish(&b, &frames(2, 1, 1024));
    assert_eq!(batch_starts(&a), vec![2, 3]);
    assert_eq!(batch_starts(&b), vec![0, 1]);
    assert_eq!(counter(&s.refused), 4);
    // Absorbed through its ring's ceiling, a restarts at its next batch.
    set_absorbed(&a, 4, 6);
    engine.ring_publish(&a, &frames(6, 1, 1024));
    assert_eq!(batch_starts(&a), vec![6]);
    assert_eq!(counter(&s.gap_resets), 1);
    assert_eq!(counter(&s.evicted_absorbed), 4);
    assert_eq!(counter(&s.evicted_absorbed_bytes), 4096);
    assert_eq!(s.resident(), 3 * 1024);
    assert_eq!(budget.resident(), 3 * 1024);
}

/// The share rule (review B5): `max(floor, min(cap, process ÷ shards))`. One
/// shard alone keeps its cap, never the whole process budget; more shards
/// divide the process budget down to the floor; and a shrinking share sheds
/// only absorbed batches.
#[tokio::test]
async fn spill_ring_share_is_capped_and_floored_by_the_process_budget() {
    let budget = RingBudget::new(settings(64 << 10, 8 << 10, 2 << 10));
    let mut regs = Vec::new();
    let mut seen = Vec::new();
    for n in 1..=64u64 {
        regs.push(budget.register());
        seen.push((n, budget.share()));
    }
    for (n, want) in [(1, 8 << 10), (8, 8 << 10), (9, (64 << 10) / 9), (16, 4 << 10), (32, 2 << 10), (33, 2 << 10), (64, 2 << 10)] {
        assert_eq!(seen[n as usize - 1], (n, want), "{n} shards");
    }
    drop(regs);
    assert_eq!((budget.shards(), budget.share()), (0, 8 << 10));
    let engine = spill_engine(&budget).await;
    let h = engine.stream_handle([5; 16]).await.unwrap();
    for o in 0..12 {
        engine.ring_publish(&h, &frames(o, 1, 1024));
    }
    assert_eq!(batch_starts(&h), (0..8).collect::<Vec<_>>(), "one shard keeps 8 KiB, not 64");
    assert_eq!(budget.resident(), 8 << 10);
    let s = share(&engine);
    assert_eq!((counter(&s.refused), counter(&s.refused_gap)), (4, 3));
    // Fifteen more shards: the share falls to 4 KiB. The next publish sheds
    // absorbed batches only, oldest first, to fit.
    let others: Vec<_> = (0..15).map(|_| budget.register()).collect();
    assert_eq!(budget.share(), 4 << 10);
    set_absorbed(&h, 6, 12);
    engine.ring_publish(&h, &frames(8, 1, 1024));
    assert_eq!(batch_starts(&h), vec![5, 6, 7, 8]);
    assert_eq!(s.resident(), 4 << 10);
    // Past the process budget the floor holds: 64 shards of 2 KiB.
    let more: Vec<_> = (0..48).map(|_| budget.register()).collect();
    assert_eq!(budget.shards(), 64);
    assert_eq!(budget.share(), 2 << 10);
    drop((others, more));
    assert_eq!(budget.share(), 8 << 10);
}

/// A stream in the property model.
struct Model {
    handle: Arc<StreamHandle>,
    next: u64,
    absorbed: u64,
}

/// (first, next, bytes) of each of a stream's ring batches.
fn batches(handle: &StreamHandle) -> Vec<(u64, u64, usize)> {
    let ring = handle.ring.lock().unwrap();
    ring.batches.iter().map(|b| (b.first, b.next, b.bytes)).collect()
}

/// **Under budget, no page at or above `absorbed` is ever evicted, and
/// every shard keeps its share.** Three engines share one process budget;
/// random steps publish batches of random size to random streams, advance
/// absorbed boundaries, and register or drop other shards (moving every
/// share between cap, divided budget and floor). After every step:
/// - every batch that left a ring had all its pages below its stream's
///   absorbed boundary as of that step;
/// - an admitted batch left its engine at or under its share, and a
///   refused one (outside a gap) found nothing evictable in its engine;
/// - a publish to one engine never changed another engine's rings;
/// - rings are dense, and the engine and process byte counts are exact.
/// Then every stream is absorbed and every engine takes a full share.
#[tokio::test]
async fn spill_ring_property_never_evicts_unabsorbed_and_keeps_every_share() {
    for seed in 0..10u64 {
        let mut rng = StdRng::seed_from_u64(0x217 ^ seed);
        let budget = RingBudget::new(settings(24 << 10, 12 << 10, 4 << 10));
        let mut engines = Vec::new();
        for _ in 0..3 {
            engines.push(spill_engine(&budget).await);
        }
        let mut models: Vec<Vec<Model>> = Vec::new();
        for (e, engine) in engines.iter().enumerate() {
            let mut streams = Vec::new();
            for s in 0..4u8 {
                let handle = engine.stream_handle([e as u8 * 16 + s + 1; 16]).await.unwrap();
                streams.push(Model { handle, next: 0, absorbed: 0 });
            }
            models.push(streams);
        }
        let mut extra = Vec::new();
        let (mut admitted_bytes, mut refusals, mut evictions) = (0u64, 0u64, 0u64);
        for step in 0..800 {
            let before: Vec<Vec<Vec<(u64, u64, usize)>>> =
                models.iter().map(|ms| ms.iter().map(|m| batches(&m.handle)).collect()).collect();
            let absorbed_before: Vec<Vec<u64>> =
                models.iter().map(|ms| ms.iter().map(|m| m.absorbed).collect()).collect();
            let roll = rng.random_range(0..20);
            let mut published: Option<(usize, usize, u64, bool)> = None;
            if roll < 12 {
                let (e, s) = (rng.random_range(0..3), rng.random_range(0..4));
                let m = &mut models[e][s];
                let n = rng.random_range(1..=3u64);
                let mut batch = RingBatch::default();
                for o in m.next..m.next + n {
                    batch.push_page(o, o, Bytes::from(vec![0u8; rng.random_range(100..=1_500)]));
                }
                let bytes = batch.bytes as u64;
                let cap = budget.share();
                let sh = share(&engines[e]);
                let gaps0 = counter(&sh.refused_gap);
                engines[e].ring_publish(&m.handle, &batch);
                m.next += n;
                let ceil = batches(&m.handle).last().map(|b| b.1);
                let admitted = ceil == Some(m.next);
                if admitted {
                    admitted_bytes += bytes;
                    assert!(sh.resident() <= cap, "seed {seed} step {step}: over the share");
                } else {
                    refusals += 1;
                    let gap = counter(&sh.refused_gap) > gaps0;
                    if !gap {
                        assert!(sh.resident() + bytes > cap, "seed {seed} step {step}: refused with room");
                        for m in &models[e] {
                            for (_, next, _) in batches(&m.handle) {
                                assert!(next > m.absorbed, "seed {seed} step {step}: refused while an absorbed batch stayed");
                            }
                        }
                    }
                }
                published = Some((e, s, bytes, admitted));
            } else if roll < 17 {
                let (e, s) = (rng.random_range(0..3), rng.random_range(0..4));
                let m = &mut models[e][s];
                m.absorbed = rng.random_range(m.absorbed..=m.next);
                set_absorbed(&m.handle, m.absorbed, m.next);
            } else if roll < 19 && extra.len() < 8 {
                extra.push(budget.register());
            } else if !extra.is_empty() {
                extra.swap_remove(rng.random_range(0..extra.len()));
            }
            for (e, ms) in models.iter().enumerate() {
                let mut sum = 0usize;
                for (s, m) in ms.iter().enumerate() {
                    let after = batches(&m.handle);
                    for w in after.windows(2) {
                        assert_eq!(w[0].1, w[1].0, "seed {seed} step {step}: a ring with a hole");
                    }
                    let kept: std::collections::HashSet<u64> = after.iter().map(|b| b.0).collect();
                    for gone in before[e][s].iter().filter(|b| !kept.contains(&b.0)) {
                        evictions += 1;
                        assert!(
                            gone.1 <= absorbed_before[e][s],
                            "seed {seed} step {step}: evicted [{}, {}) above absorbed {}",
                            gone.0,
                            gone.1,
                            absorbed_before[e][s]
                        );
                    }
                    if let Some((pe, ..)) = published
                        && pe != e
                    {
                        assert_eq!(after, before[e][s], "seed {seed} step {step}: another engine's ring moved");
                    }
                    let ring_bytes = m.handle.ring.lock().unwrap().bytes;
                    assert_eq!(ring_bytes, after.iter().map(|b| b.2).sum::<usize>());
                    sum += ring_bytes;
                }
                assert_eq!(share(&engines[e]).resident(), sum as u64, "seed {seed} step {step}");
            }
            let total: u64 = engines.iter().map(|e| share(e).resident()).sum();
            assert_eq!(budget.resident(), total, "seed {seed} step {step}: process bytes");
        }
        let evicted: u64 = engines.iter().map(|e| counter(&share(e).evicted_absorbed_bytes)).sum();
        assert_eq!(evicted + budget.resident(), admitted_bytes, "seed {seed}: every admitted byte is resident or evicted");
        assert!(refusals > 0 && evictions > 0, "seed {seed}: the budget never acted ({refusals} refusals, {evictions} evictions)");
        // Drain: absorb everything; each engine then fills its share again.
        drop(extra);
        for ms in &mut models {
            for m in ms.iter_mut() {
                m.absorbed = m.next;
                set_absorbed(&m.handle, m.next, m.next);
            }
        }
        for (e, ms) in models.iter_mut().enumerate() {
            for _ in 0..40 {
                let m = &mut ms[0];
                engines[e].ring_publish(&m.handle, &frames(m.next, 1, 1024));
                m.next += 1;
            }
            let ring = batches(&ms[0].handle);
            assert!(ring.len() >= 7, "seed {seed}: engine {e} keeps a full share once absorbed ({})", ring.len());
        }
    }
}

async fn append(engine: &Arc<ShardEngine>, hash: [u8; 16], payload_bytes: usize) -> u64 {
    let subkey = derive_subkey(&StreamKey([7; 32]), &hash, "", 0);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: vec![Bytes::from(vec![b'x'; payload_bytes])],
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey,
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        finish: crate::shard::AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp: tx,
    };
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    rx.await.expect("resp").expect("ack").last_offset
}

/// **`put_bytes`: the ring and the memtable share one allocation per page**
/// (dB §1.1). After a real append on the spill arm, the page the memtable
/// returns is the ring's page by pointer; the control, the base arm's
/// `put`, holds a copy.
#[tokio::test]
async fn spill_pages_share_one_allocation_between_ring_and_memtable() {
    for l6_spill in [true, false] {
        let engine = engine_cfg(ShardConfig {
            tail_ring_bytes: 1 << 20,
            l6_spill,
            ..Default::default()
        })
        .await;
        let hash = [0x77; 16];
        // The 5 s memtable ticker's first tick is immediate and flushes the
        // memtable once anything was appended: take that flush first, so the
        // measured pages are still in the memtable (the next tick is 5 s on).
        append(&engine, hash, 10).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        for size in [700, 5_000, 90_000] {
            let last = append(&engine, hash, size).await;
            let handle = engine.stream_handle(hash).await.unwrap();
            let (ring_ptr, ring_len) = {
                let ring = handle.ring.lock().unwrap();
                let (page_last, page) = ring.batches.back().unwrap().frames.last().unwrap();
                assert_eq!(*page_last, last);
                (page.as_ptr(), page.len())
            };
            let stored = engine
                .db
                .get(crate::crypto_page::shard_page_key(&hash, last))
                .await
                .unwrap()
                .expect("the page is in the memtable");
            assert_eq!(stored.len(), ring_len);
            if l6_spill {
                assert_eq!(stored.as_ptr(), ring_ptr, "{size} B: spill arm shares the page");
            } else {
                assert_ne!(stored.as_ptr(), ring_ptr, "{size} B: base arm copies the page");
            }
        }
    }
}

/// The SlateDB property the spill arm relies on: a value written with
/// `put_bytes` is held by the memtable as the same allocation (a read
/// returns it by pointer, by get and by scan, before and after a WAL flush).
#[tokio::test]
async fn slatedb_keeps_a_put_bytes_value_as_its_allocation() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Db::builder("probe", store.clone()).build().await.unwrap();
    let v = Bytes::from(vec![7u8; 4096]);
    let mut wb = slatedb::WriteBatch::new();
    wb.put_bytes(Bytes::from_static(b"k1"), v.clone());
    db.write(wb).await.unwrap();
    assert_eq!(db.get(b"k1").await.unwrap().unwrap().as_ptr(), v.as_ptr());
    let mut it = db.scan_prefix(b"k", ..).await.unwrap();
    assert_eq!(it.next().await.unwrap().unwrap().value.as_ptr(), v.as_ptr());
    db.flush().await.unwrap();
    assert_eq!(db.get(b"k1").await.unwrap().unwrap().as_ptr(), v.as_ptr());
    // The control: `put` copies.
    let mut wb = slatedb::WriteBatch::new();
    wb.put(b"k2", &v);
    db.write(wb).await.unwrap();
    assert_ne!(db.get(b"k2").await.unwrap().unwrap().as_ptr(), v.as_ptr());
}
