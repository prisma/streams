#![cfg(test)]
//! Sealed reads (layout 6): exact data across chunks, anchors and budgets
//! at both visibilities; a reader racing a seal's commit; keyed reads that
//! skip the chunks a key summary excludes without a GET, and pay one GET
//! for a bloom's false positive; and a seal that wakes no waiter.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt};
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::Trigger;
use super::test_hooks::Hold;
use super::tests::{
    Oracle, ReadAt, append_lane, append_recorded, hashes, manual, merged, open, random_bytes,
    random_record, read_back, reopen, rig, rig_with, unique,
};
use crate::application::read::ReadPage;
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::shard::Deliver;
use crate::spill::directory::KeySummary;

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// Check `page`, read from `from`, against `oracle`: dense from `from`,
/// every payload exact. Returns the records served.
fn check_page(
    oracle: &BTreeMap<u64, bytes::Bytes>,
    from: u64,
    page: &ReadPage,
) -> Result<usize, String> {
    let mut due = from;
    for r in page.recs.iter() {
        if r.off != due {
            return Err(format!("offset {} where {due} was due", r.off));
        }
        match oracle.get(&r.off) {
            Some(rec) if rec[..] == r.payload[..] => {}
            _ => return Err(format!("offset {} differs from the record acked", r.off)),
        }
        due += 1;
    }
    if page.last.is_some_and(|l| l + 1 != due) {
        return Err(format!(
            "last {:?} past the records served (next {due})",
            page.last
        ));
    }
    Ok(page.recs.len())
}

/// Sealed ranges read back exactly from random offsets with random budgets
/// at both visibilities, then after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_ranges_read_back_exactly_across_chunks_anchors_and_budgets() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 48;
    // No ring: every read of a sealed range goes to the objects.
    let rig = rig_with("exact", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xE8AC);
    let streams = hashes(&mut rng, 4);
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..6 {
        for _ in 0..30 {
            let hash = streams[rng.random_range(0..streams.len())];
            let records = (0..rng.random_range(1..=3))
                .map(|_| random_record(&mut rng, 16, 60_000))
                .collect();
            append_recorded(&rig.engine, &oracle, hash, records).await;
        }
        rig.engine
            .seal_once(Trigger::Requested)
            .await
            .expect("seal");
    }
    let oracle = oracle.into_inner().unwrap();
    let counters = &rig.engine.layout.seal.counters;
    for _ in 0..200 {
        let hash = streams[rng.random_range(0..streams.len())];
        let o = &oracle[&hash];
        let from = rng.random_range(0..o.len() as u64);
        let budget = [1usize, 4096, 64 << 10, 300 << 10, 4 << 20][rng.random_range(0..5)];
        let deliver = if rng.random_bool(0.5) {
            Deliver::Durable
        } else {
            Deliver::Applied
        };
        let page = merged(&rig.engine, hash, ReadAt::all(from, budget), deliver)
            .await
            .expect("read");
        let n = check_page(o, from, &page).unwrap();
        assert!(n > 0, "no progress from {from}");
    }
    assert!(
        get(&counters.sealed_reads) > 0,
        "no read reached the sealed ranges"
    );
    eprintln!(
        "exact: sealed reads {}, GETs {}",
        get(&counters.sealed_reads),
        get(&counters.sealed_gets)
    );
    let rig = reopen(rig).await;
    for hash in &streams {
        read_back(&rig.engine, *hash, &oracle[hash], Deliver::Durable)
            .await
            .unwrap();
    }
    rig.engine.begin_close();
}

struct HeldRace {
    oracle: BTreeMap<u64, bytes::Bytes>,
    /// The reads during the race, as the reader answered them.
    during: [Result<ReadPage, String>; 2],
    after: [ReadPage; 2],
}

/// Wait until the seal's commit has moved the applied boundary of `handle`
/// to `n`, asserting the durable one has not moved (WAL PUTs are held).
async fn wait_applied_not_durable(
    handle: &crate::shard::StreamHandle,
    n: u64,
    t0: std::time::Instant,
) {
    loop {
        let (applied, durable) = {
            let st = handle.state.lock().unwrap();
            (st.applied.absorbed, st.durable.absorbed)
        };
        if applied == n {
            assert_eq!(
                durable, 0,
                "the seal's commit must stay non-durable while WAL PUTs are held"
            );
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the seal's commit never applied: {applied}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// A reader racing a seal, made deterministic: the seal's PUT lands, then
/// shard WAL PUTs are held, so its commit (the boundary move and its put of
/// the empty remainder) is applied but not durable. A durable and an
/// applied reader from 0 read meanwhile; the applied boundary already
/// dropped the pages from memory, so both read the sealed range from the
/// directory. `late` publishes the directory only after the commit is
/// durable (the negative control). Then the hold is released and both read
/// again.
async fn held_wal_race(late: bool) -> HeldRace {
    let inner: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = FaultStore::uniform(inner, 0x6276, FaultPlan::CLEAN);
    let mut settings = manual();
    settings.max_pages_per_chunk = 3;
    let engine = open(
        store.clone(),
        &unique("held"),
        settings,
        0,
        Duration::from_millis(2),
    )
    .await;
    let sp = &engine.layout.seal;
    sp.hooks.late_directory.store(late, Ordering::Relaxed);
    let mut rng = StdRng::seed_from_u64(0x6276);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..9 {
        let records = (0..rng.random_range(1..=3))
            .map(|_| random_bytes(&mut rng, 700))
            .collect();
        append_recorded(&engine, &oracle, hash, records).await;
    }
    let oracle = oracle.into_inner().unwrap().remove(&hash).unwrap();
    let n = oracle.len() as u64;
    let handle = engine.stream_handle(hash).await.unwrap();
    let before_commit = Hold::new();
    *sp.hooks.before_commit.lock().unwrap() = Some(before_commit.clone());
    let reads = async {
        let t0 = std::time::Instant::now();
        while before_commit.entered.load(Ordering::Acquire) == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "the seal never reached its commit"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let parked = store.hold_class(StoreOp::Put, ObjClass::Wal, u64::MAX);
        before_commit.release();
        wait_applied_not_durable(&handle, n, t0).await;
        let during = [
            merged(&engine, hash, ReadAt::all(0, 8 << 20), Deliver::Durable).await,
            merged(&engine, hash, ReadAt::all(0, 8 << 20), Deliver::Applied).await,
        ];
        // The group-commit pump issues the commit's WAL PUT on its own
        // schedule; the reads above ran while it could not be durable.
        while parked.load(Ordering::SeqCst) == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "no WAL PUT was ever parked"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        store.release_hold();
        during
    };
    let (report, during) = tokio::join!(engine.seal_once(Trigger::Requested), reads);
    let report = report.expect("the seal completes once WAL PUTs flow");
    assert!(report.pages > 0, "the seal sealed the stream's pages");
    let after = [
        merged(&engine, hash, ReadAt::all(0, 8 << 20), Deliver::Durable)
            .await
            .unwrap(),
        merged(&engine, hash, ReadAt::all(0, 8 << 20), Deliver::Applied)
            .await
            .unwrap(),
    ];
    engine.begin_close();
    HeldRace {
        oracle,
        during,
        after,
    }
}

/// A reader racing a seal's commit gets exact data at both visibilities:
/// the seal published its chunks before the commit moved the boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_racing_a_seal_gets_exact_data_at_both_visibilities() {
    let race = held_wal_race(false).await;
    let n = race.oracle.len();
    let during = race.during.iter().map(|r| r.as_ref().expect("read"));
    for page in during.chain(&race.after) {
        assert_eq!(check_page(&race.oracle, 0, page).unwrap(), n, "exact data");
        assert!(page.completed, "complete");
    }
}

/// **The negative control.** The same race with the directory published
/// only once the commit is durable: the readers find no chunk for the
/// offsets the boundary says are sealed, and both refuse the read as lost
/// records (#213), never an empty page (the exact-data assertion above
/// would fail here). After the commit, both read everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_control_a_directory_published_after_the_commit_leaves_a_hole() {
    let race = held_wal_race(true).await;
    let n = race.oracle.len() as u64;
    for read in &race.during {
        let Err(error) = read else {
            panic!("the hole is refused, not served");
        };
        assert!(
            error.contains(&format!("Missing {{ due: 0, to: {n} }}")),
            "the hole is lost records [0, {n}): {error}"
        );
    }
    for page in &race.after {
        assert_eq!(check_page(&race.oracle, 0, page).unwrap() as u64, n);
    }
}

/// A keyed read of sealed data skips every chunk its key summary excludes
/// without a GET (those offsets still count as consumed), fetches only the
/// chunks of its key, and serves exactly its key's records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_read_skips_chunks_the_summary_excludes_without_a_get() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 4;
    let rig = rig_with("keyed", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x4E7);
    let hash = hashes(&mut rng, 1)[0];
    let mut want = Vec::new();
    for round in 0..3 {
        for _ in 0..8 {
            append_lane(
                &rig.engine,
                hash,
                "other",
                vec![random_bytes(&mut rng, 200)],
            )
            .await;
        }
        rig.engine.seal_once(Trigger::Requested).await.unwrap();
        for _ in 0..2 {
            let record = random_bytes(&mut rng, 200);
            let last = append_lane(&rig.engine, hash, "mine", vec![record.clone()]).await;
            want.push((last, record));
        }
        rig.engine.seal_once(Trigger::Requested).await.unwrap();
        eprintln!("keyed round {round}: {} records of mine", want.len());
    }
    let counters = &rig.engine.layout.seal.counters;
    let (skips, gets) = (get(&counters.summary_skips), get(&counters.sealed_gets));
    let misses = get(&counters.summary_misses);
    let at = ReadAt {
        from: 0,
        selector: Some("mine"),
        max_bytes: 1 << 20,
    };
    let page = merged(&rig.engine, hash, at, Deliver::Durable)
        .await
        .unwrap();
    let served: Vec<_> = page
        .recs
        .iter()
        .map(|r| (r.off, bytes::Bytes::copy_from_slice(&r.payload)))
        .collect();
    assert_eq!(served, want, "exactly the key's records");
    assert!(page.completed, "the read consumed the whole stream");
    assert_eq!(
        get(&counters.summary_skips) - skips,
        6,
        "two chunks of the other key per round skipped"
    );
    assert_eq!(
        get(&counters.sealed_gets) - gets,
        3,
        "one GET per chunk of the key"
    );
    assert_eq!(
        get(&counters.summary_misses),
        misses,
        "every chunk fetched served the key"
    );
    rig.engine.begin_close();
}

/// A keyed read whose window ends inside a chunk the summary excludes
/// consumes that chunk only up to the window's last offset: the offset at
/// the window's end belongs to the next read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_skip_stops_at_the_window_end_inside_its_chunk() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 4;
    let rig = rig_with("keyed-end", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x5E1D);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..8 {
        append_lane(
            &rig.engine,
            hash,
            "other",
            vec![random_bytes(&mut rng, 200)],
        )
        .await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    for to in [2, 4, 6] {
        let window = crate::shard::tail_ring::RingScan {
            from: 0,
            to,
            max_bytes: 1 << 20,
        };
        let out = rig
            .engine
            .read_stream_window(&handle, window, Some("mine"), Deliver::Durable)
            .await
            .unwrap();
        assert!(out.frames.is_empty(), "no record of the key");
        assert_eq!(out.last_offset, Some(to - 1), "consumed through {}", to - 1);
    }
    rig.engine.begin_close();
}

/// A chunk of more keys than a summary names gets a bloom; a key the bloom
/// wrongly admits costs one GET that serves nothing, and its offsets are
/// still consumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bloom_false_positive_costs_one_get_that_serves_nothing() {
    let rig = rig_with("bloom", manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xB100);
    let hash = hashes(&mut rng, 1)[0];
    for i in 0..64 {
        append_lane(
            &rig.engine,
            hash,
            &format!("key-{i}"),
            vec![random_bytes(&mut rng, 64)],
        )
        .await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let entries = rig
        .engine
        .layout
        .seal
        .directory
        .read()
        .unwrap()
        .from(&hash, 0, usize::MAX);
    assert_eq!(entries.len(), 1, "one chunk");
    let KeySummary::Bloom(_) = &entries[0].keys else {
        panic!("64 keys take a bloom: {:?}", entries[0].keys);
    };
    let probe = (0..100_000)
        .map(|i| format!("absent-{i}"))
        .find(|k| entries[0].keys.may_hold(k))
        .expect("a 10-bit-per-key bloom admits some absent key");
    let counters = &rig.engine.layout.seal.counters;
    let (gets, misses) = (get(&counters.sealed_gets), get(&counters.summary_misses));
    let at = ReadAt {
        from: 0,
        selector: Some(&probe),
        max_bytes: 1 << 20,
    };
    let page = merged(&rig.engine, hash, at, Deliver::Durable)
        .await
        .unwrap();
    assert!(page.recs.is_empty(), "the false positive serves nothing");
    assert_eq!(page.last, Some(63), "its offsets are consumed");
    assert_eq!(get(&counters.sealed_gets) - gets, 1, "one GET");
    assert_eq!(
        get(&counters.summary_misses) - misses,
        1,
        "counted as a miss"
    );
    rig.engine.begin_close();
}

/// A seal moves a stream's boundary and nothing its waiters wait for: a
/// durable or applied waiter parked on the stream is not woken by it, and
/// is woken by the next append (the control).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_wakes_no_waiter_of_the_stream() {
    let rig = rig("nowake", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x3A4E);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..5 {
        append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 300)]).await;
    }
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let durable = handle.notify.notified();
    let applied = handle.applied_notify.notified();
    tokio::pin!(durable, applied);
    durable.as_mut().enable();
    applied.as_mut().enable();
    let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
    assert!(report.pages > 0);
    assert_eq!(
        handle.state.lock().unwrap().durable.absorbed,
        5,
        "the seal is durable"
    );
    let woken = tokio::time::timeout(Duration::from_millis(200), async {
        tokio::select! {
            () = &mut durable => "durable",
            () = &mut applied => "applied",
        }
    })
    .await;
    assert!(
        woken.is_err(),
        "the seal woke a {} waiter",
        woken.unwrap_or_default()
    );
    append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 300)]).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(durable, applied)
    })
    .await
    .expect("an append wakes both waiters");
    rig.engine.begin_close();
}

/// A sealed chunk whose bytes were altered surfaces as corruption, at both
/// visibilities: never as a page with records missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_sealed_chunk_surfaces_as_corruption() {
    let rig = rig_with("corrupt", manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xC0C0);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..6 {
        append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 500)]).await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let entry = rig
        .engine
        .layout
        .seal
        .directory
        .read()
        .unwrap()
        .from(&hash, 0, 1)[0]
        .clone();
    let path =
        object_store::path::Path::from(crate::spill::object::object_path(&rig.path, entry.object));
    let mut body = rig
        .store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    // A byte inside the chunk's last page (before its CRC).
    let at = usize::try_from(entry.chunk_off).unwrap() + entry.chunk_len as usize - 8;
    body[at] ^= 0x40;
    rig.store.put(&path, body.into()).await.unwrap();
    for deliver in [Deliver::Durable, Deliver::Applied] {
        let read = merged(&rig.engine, hash, ReadAt::all(0, 1 << 20), deliver).await;
        let Err(error) = read else {
            panic!("{deliver:?}: a corrupt chunk must fail the read");
        };
        assert!(error.contains("corrupt"), "{deliver:?}: {error}");
    }
    rig.engine.begin_close();
}

/// TLA-018-F4: a read that captured its boundary just before a seal
/// commits still serves exact data. The read takes the boundary and the
/// unsealed pages above it under one lock; the seal then drops those pages
/// from memory, and the read must not look for them there again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_that_captured_its_boundary_before_a_seal_commits_serves_exact_data() {
    let rig = rig_with("captured", manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xF4);
    let hash = hashes(&mut rng, 1)[0];
    let oracle = Mutex::new(Oracle::new());
    for _ in 0..6 {
        append_recorded(
            &rig.engine,
            &oracle,
            hash,
            vec![random_bytes(&mut rng, 600)],
        )
        .await;
    }
    let oracle = oracle.into_inner().unwrap().remove(&hash).unwrap();
    let hold = Hold::new();
    *rig.engine.layout.seal.hooks.window_hold.lock().unwrap() = Some(hold.clone());
    let seal = async {
        let t0 = std::time::Instant::now();
        while hold.entered.load(Ordering::Acquire) == 0 {
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "the read never captured its window"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        *rig.engine.layout.seal.hooks.window_hold.lock().unwrap() = None;
        let report = rig.engine.seal_once(Trigger::Requested).await.unwrap();
        assert_eq!(
            report.pages, 6,
            "the seal took every page the read had captured"
        );
        hold.release();
    };
    let (page, ()) = tokio::join!(
        merged(&rig.engine, hash, ReadAt::all(0, 1 << 20), Deliver::Applied),
        seal
    );
    let page = page.expect("no spurious corruption");
    assert_eq!(
        check_page(&oracle, 0, &page).unwrap(),
        oracle.len(),
        "exact data"
    );
    assert!(page.completed);
    rig.engine.begin_close();
}

/// A read the tail ring serves through the sealed boundary carries on into
/// the unsealed pages in the same call: one window from 0 returns every
/// record, the sealed ones from the ring and the rest from memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ring_read_through_the_boundary_continues_into_the_unsealed_pages() {
    let rig = rig("ring-through", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x7149);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..3 {
        append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 200)]).await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    for _ in 0..2 {
        append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 200)]).await;
    }
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    assert_eq!(
        handle.state.lock().unwrap().durable.absorbed,
        3,
        "three sealed"
    );
    let gets = get(&rig.engine.layout.seal.counters.sealed_gets);
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: u64::MAX,
        max_bytes: 1 << 20,
    };
    let out = rig
        .engine
        .read_stream_window(&handle, window, None, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(
        get(&rig.engine.layout.seal.counters.sealed_gets),
        gets,
        "the ring served the sealed range"
    );
    assert_eq!(out.last_offset, Some(4), "through the unsealed pages");
    // A budget the sealed range fills exactly stops the read at the
    // boundary: nothing past it is inspected.
    let sealed = crate::shard::tail_ring::RingScan { to: 3, ..window };
    let first = rig
        .engine
        .read_stream_window(&handle, sealed, None, Deliver::Durable)
        .await
        .unwrap();
    let bytes: usize = first
        .frames
        .iter()
        .map(crate::shard::record::PageSlice::stored_len)
        .sum();
    let exact = crate::shard::tail_ring::RingScan {
        max_bytes: bytes,
        ..window
    };
    let out = rig
        .engine
        .read_stream_window(&handle, exact, None, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(out.last_offset, Some(2), "the budget ends at the boundary");
    rig.engine.begin_close();
}

/// A closed engine serves nothing from memory: a read of a stream whose
/// records are all in memory meets the close as a store read would.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_engine_refuses_a_read_it_could_serve_from_memory() {
    let rig = rig("closed-read", manual(), Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xC105);
    let hash = hashes(&mut rng, 1)[0];
    append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 200)]).await;
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    rig.engine.begin_close();
    tokio::time::timeout(Duration::from_secs(10), rig.engine.closed())
        .await
        .expect("the engine closed");
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: u64::MAX,
        max_bytes: 1 << 20,
    };
    let refused = rig
        .engine
        .read_stream_window(&handle, window, None, Deliver::Durable)
        .await;
    assert!(
        matches!(refused, Err(crate::shard::record::RangeReadError::Store(_))),
        "a closed engine refuses the read"
    );
}

/// A sealed read stops planning at its window's end: a window ending at a
/// chunk boundary fetches that chunk alone, never the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_ending_at_a_chunk_boundary_fetches_no_further_chunk() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 4;
    let rig = rig_with("window-end", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xE4D);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..8 {
        append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 200)]).await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let gets = get(&rig.engine.layout.seal.counters.sealed_gets);
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: 4,
        max_bytes: 1 << 20,
    };
    let out = rig
        .engine
        .read_stream_window(&handle, window, None, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(out.last_offset, Some(3));
    assert_eq!(
        get(&rig.engine.layout.seal.counters.sealed_gets) - gets,
        1,
        "one chunk fetched"
    );
    rig.engine.begin_close();
}

/// One sealed read fetches at most 256 chunks: of 300 one-page chunks, a
/// read from 0 fetches the first 256 and ends at the last offset they hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_read_fetches_at_most_256_chunks() {
    let mut settings = manual();
    settings.max_pages_per_chunk = 1;
    let rig = rig_with("chunk-cap", settings, 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x256);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..300 {
        append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 16)]).await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let gets = get(&rig.engine.layout.seal.counters.sealed_gets);
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: u64::MAX,
        max_bytes: 64 << 20,
    };
    let out = rig
        .engine
        .read_stream_window(&handle, window, None, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(
        get(&rig.engine.layout.seal.counters.sealed_gets) - gets,
        256
    );
    assert_eq!(out.last_offset, Some(255), "the 256 chunks' offsets");
    rig.engine.begin_close();
}

/// A sealed-range GET the store keeps failing is tried three times, then
/// the read fails as the store's unavailability, at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_chunk_get_is_tried_three_times_then_the_read_fails() {
    let inner: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let profile = crate::dst::FaultProfile::clean().with_op_class(
        StoreOp::Get,
        ObjClass::Other,
        FaultPlan::new(100, 0, 0),
    );
    let store = FaultStore::new(inner, 0x6E7, profile);
    let engine = open(
        store,
        &unique("get-retries"),
        manual(),
        0,
        Duration::from_millis(2),
    )
    .await;
    let mut rng = StdRng::seed_from_u64(0x6E7);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..2 {
        append_lane(&engine, hash, "", vec![random_bytes(&mut rng, 200)]).await;
    }
    engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = engine.stream_handle(hash).await.unwrap();
    let gets = get(&engine.layout.seal.counters.sealed_gets);
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: u64::MAX,
        max_bytes: 1 << 20,
    };
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        engine.read_stream_window(&handle, window, None, Deliver::Durable),
    )
    .await
    .expect("the read gives up");
    assert!(
        matches!(read, Err(crate::shard::record::RangeReadError::Store(_))),
        "the store's unavailability"
    );
    assert_eq!(
        get(&engine.layout.seal.counters.sealed_gets) - gets,
        3,
        "three attempts"
    );
    engine.begin_close();
}

/// A read whose budget covers a whole chunk past several anchors fetches
/// it in one GET and serves every record of it in one call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_budget_past_every_anchor_reads_the_whole_chunk_at_once() {
    let rig = rig_with("whole-chunk", manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0xA9C);
    let hash = hashes(&mut rng, 1)[0];
    for _ in 0..12 {
        append_lane(
            &rig.engine,
            hash,
            "",
            vec![random_bytes(&mut rng, 64 << 10)],
        )
        .await;
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = rig.engine.stream_handle(hash).await.unwrap();
    let gets = get(&rig.engine.layout.seal.counters.sealed_gets);
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: u64::MAX,
        max_bytes: 64 << 20,
    };
    let out = rig
        .engine
        .read_stream_window(&handle, window, None, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(out.last_offset, Some(11), "every record of the chunk");
    assert_eq!(
        get(&rig.engine.layout.seal.counters.sealed_gets) - gets,
        1,
        "one GET"
    );
    // A 100 KiB budget fetches up to the first anchor past it (anchors
    // every 256 KiB of pages), not the whole 768 KiB chunk.
    let fetched = get(&rig.engine.layout.seal.hooks.get_bytes);
    let small = crate::shard::tail_ring::RingScan {
        max_bytes: 100 << 10,
        ..window
    };
    let out = rig
        .engine
        .read_stream_window(&handle, small, None, Deliver::Durable)
        .await
        .unwrap();
    let span = get(&rig.engine.layout.seal.hooks.get_bytes) - fetched;
    assert!(span <= 320 << 10, "one anchor span fetched: {span} B");
    assert!(
        out.last_offset.is_some_and(|l| l < 11),
        "{:?}",
        out.last_offset
    );
    rig.engine.begin_close();
}

/// A whole-chunk GET whose chunk header disagrees with the directory entry
/// that planned it is refused as corruption, never served: an entry that
/// names another stream's chunk, or another page count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunk_whose_header_disagrees_with_its_entry_is_refused() {
    let rig = rig_with("header-check", manual(), 0, Duration::from_millis(2)).await;
    let mut rng = StdRng::seed_from_u64(0x4EAD);
    let [a, b] = hashes(&mut rng, 2)[..] else {
        unreachable!()
    };
    for hash in [a, b] {
        for _ in 0..3 {
            append_lane(&rig.engine, hash, "", vec![random_bytes(&mut rng, 200)]).await;
        }
    }
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
    let dir = &rig.engine.layout.seal.directory;
    let entry_of = |hash| dir.read().unwrap().from(&hash, 0, 1)[0].clone();
    let (entry_a, entry_b) = (entry_of(a), entry_of(b));
    let window = crate::shard::tail_ring::RingScan {
        from: 0,
        to: u64::MAX,
        max_bytes: 1 << 20,
    };
    let read = |hash| {
        let engine = rig.engine.clone();
        async move {
            let handle = engine.stream_handle(hash).await.unwrap();
            engine
                .read_stream_window(&handle, window, None, Deliver::Durable)
                .await
        }
    };
    assert!(read(b).await.is_ok(), "B reads back as sealed");
    // B's entry names A's chunk (same offsets and page count).
    dir.write()
        .unwrap()
        .remove(&b, entry_b.last, entry_b.object);
    dir.write().unwrap().insert(b, entry_a.clone());
    assert!(read(b).await.is_err(), "another stream's chunk is refused");
    // A's entry claims one page fewer than its chunk holds.
    dir.write()
        .unwrap()
        .remove(&a, entry_a.last, entry_a.object);
    dir.write().unwrap().insert(
        a,
        crate::spill::directory::DirEntry {
            page_count: entry_a.page_count - 1,
            ..entry_a
        },
    );
    assert!(
        read(a).await.is_err(),
        "a page count the chunk disagrees with is refused"
    );
    rig.engine.begin_close();
}

/// A read captures only the unsealed pages its window and budget can
/// reach: those that start before its end, up to the page that passes its
/// byte budget.
#[test]
fn a_read_captures_only_the_unsealed_pages_it_can_reach() {
    let mut pending = crate::spill::pending::PendingPages::default();
    for offset in 0..10 {
        pending.push((offset, offset, bytes::Bytes::from(vec![0u8; 100])));
    }
    let captured = |start, to, max_bytes| {
        super::read::unsealed_pages(&pending, start, to, max_bytes)
            .iter()
            .map(|(first, _, _)| *first)
            .collect::<Vec<_>>()
    };
    assert_eq!(captured(0, 3, 1 << 20), vec![0, 1, 2], "the window's end");
    assert_eq!(captured(4, 6, 1 << 20), vec![4, 5]);
    assert_eq!(
        captured(0, u64::MAX, 250),
        vec![0, 1, 2],
        "the page past the budget"
    );
    assert_eq!(
        captured(0, u64::MAX, 300),
        vec![0, 1, 2],
        "a budget met exactly"
    );
    assert_eq!(captured(0, u64::MAX, 1 << 20).len(), 10);
}
