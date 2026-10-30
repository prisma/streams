#![cfg(test)]
//! The fold and its merges (layout 6, DESIGN-6A §2.3), in process:
//! - a fold moves every `'K'` row into one run-index object behind one
//!   pointer row, empties the directory, and every record reads back
//!   exactly once, before and after a restart;
//! - a fold or merge stopped after any step (PUT, read-back, durable
//!   batch) or whose batch fails leaves either every row it would fold or
//!   its pointer row, never neither: the restart reads everything back, and
//!   the only unreferenced run-index objects are those of stopped steps;
//! - a fenced folder's object is never referenced (an orphan), and the new
//!   owner folds the same rows itself;
//! - merges make level 1 and level 2 objects and delete their inputs after
//!   the grace;
//! - keyed reads skip a dense run their bloom excludes without a GET, and a
//!   run bloom's false positive costs one run GET and serves nothing;
//! - chunks tile `[0, absorbed)` across the directory and the run-index
//!   objects, and the pending pages `[absorbed, next)`, through random
//!   appends, seals, folds and merges, each stopped at a random step;
//! - a row of the index that does not decode is skipped and counted.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use object_store::ObjectStore;
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::Trigger;
use super::fold::FoldError;
use super::fold::test_hooks::FoldStep;
use super::test_hooks::Hold;
use super::tests::{
    Oracle, ReadAt, Rig, append_lane, append_recorded, directory_rows, hashes, manual, merged,
    open, random_bytes, read_back, reopen, rig_with, settle,
};
use crate::shard::{Deliver, RingScan};
use crate::spill::object::ObjectId;
use crate::spill::runindex::{
    POINTER_TAG, Pointer, decode_pointer, parse_sentinel_key, sentinel_prefix,
};

fn get(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// A rig with no ring (every sealed read goes to the objects).
async fn cold_rig(label: &str) -> Rig {
    let mut settings = manual();
    settings.max_pages_per_chunk = 4;
    rig_with(label, settings, 0, Duration::from_millis(2)).await
}

/// The shard's pointer rows, decoded.
async fn pointers(rig: &Rig) -> BTreeMap<ObjectId, Pointer> {
    let prefix = sentinel_prefix(POINTER_TAG);
    let mut rows = rig.engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut out = BTreeMap::new();
    while let Some(kv) = rows.next().await.unwrap() {
        let id = parse_sentinel_key(POINTER_TAG, &kv.key).expect("a pointer key");
        out.insert(id, decode_pointer(id, &kv.value).expect("a pointer row"));
    }
    out
}

/// The run-index objects in the store.
async fn index_objects(rig: &Rig) -> BTreeSet<ObjectId> {
    let prefix = object_store::path::Path::from(format!("{}/ridx", rig.path));
    let mut out = BTreeSet::new();
    let mut list = rig.store.list(Some(&prefix));
    while let Some(meta) = list.next().await {
        let meta = meta.unwrap();
        let parts: Vec<_> = meta
            .location
            .parts()
            .map(|p| p.as_ref().to_string())
            .collect();
        let n = parts.len();
        let epoch = u64::from_str_radix(&parts[n - 2], 16).unwrap();
        let seq = u64::from_str_radix(&parts[n - 1], 16).unwrap();
        out.insert(ObjectId { epoch, seq });
    }
    out
}

/// `rounds` rounds of random appends to `streams`, each sealed.
async fn sealed_rounds(
    rig: &Rig,
    rng: &mut StdRng,
    streams: &[[u8; 16]],
    oracle: &std::sync::Mutex<Oracle>,
    rounds: usize,
) {
    for _ in 0..rounds {
        for _ in 0..rng.random_range(1..=6) {
            let hash = streams[rng.random_range(0..streams.len())];
            let records = (0..rng.random_range(1..=3))
                .map(|_| {
                    let len = rng.random_range(16..3000);
                    random_bytes(rng, len)
                })
                .collect();
            append_recorded(&rig.engine, oracle, hash, records).await;
        }
        rig.engine
            .seal_once(Trigger::Requested)
            .await
            .expect("seal");
    }
}

/// Every stream reads back exactly at both visibilities.
async fn read_all(rig: &Rig, oracle: &Oracle, what: &str) {
    for (hash, o) in oracle {
        for deliver in [Deliver::Durable, Deliver::Applied] {
            read_back(&rig.engine, *hash, o, deliver)
                .await
                .unwrap_or_else(|e| panic!("{what}: {e}"));
        }
    }
}

/// Every consolidation a read offered has finished: one in flight publishes
/// its entry before its `'K'` row commits (as a seal does), which the
/// tiling check would read as an entry no row names.
async fn consolidations_done(rig: &Rig) {
    let c = &rig.engine.layout.seal.fold.consolidator;
    let t0 = std::time::Instant::now();
    while get(&c.done) + get(&c.skipped) + get(&c.failed) < get(&c.offered) {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the consolidator never finished"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The tiling invariant across the directory and the run-index objects:
/// the chunks a read picks tile `[0, absorbed)` with no gap, and every
/// directory entry is still named by a `'K'` row.
async fn check_tiling(rig: &Rig, streams: &[[u8; 16]], what: &str) {
    settle(&rig.engine).await;
    consolidations_done(rig).await;
    let rows = directory_rows(rig).await;
    for hash in streams {
        let handle = rig.engine.stream_handle(*hash).await.unwrap();
        let absorbed = handle.state.lock().unwrap().durable.absorbed;
        let scan = RingScan {
            from: 0,
            to: absorbed,
            max_bytes: usize::MAX,
        };
        let picks = rig.engine.sealed_picks(*hash, scan).await.unwrap();
        let mut due = 0;
        for s in &picks {
            assert!(
                s.first <= due && due <= s.last,
                "{what}: a pick [{}, {}] where {due} was due",
                s.first,
                s.last
            );
            due = s.last + 1;
        }
        assert_eq!(due, absorbed, "{what}: the chunks tile [0, absorbed)");
        let entries = rig
            .engine
            .layout
            .seal
            .directory
            .read()
            .unwrap()
            .from(hash, 0, usize::MAX);
        let named: BTreeSet<_> = rows
            .get(hash)
            .into_iter()
            .flatten()
            .map(|e| (e.last, e.object))
            .collect();
        for e in entries {
            assert!(
                named.contains(&(e.last, e.object)),
                "{what}: a directory entry no 'K' row names"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fold_moves_every_directory_row_into_one_object() {
    let rig = cold_rig("fold-one").await;
    let mut rng = StdRng::seed_from_u64(0xF01D);
    let streams = hashes(&mut rng, 5);
    let oracle = std::sync::Mutex::new(Oracle::new());
    sealed_rounds(&rig, &mut rng, &streams, &oracle, 12).await;
    let rows = directory_rows(&rig).await;
    let chunks: usize = rows.values().map(Vec::len).sum();
    let seals = get(&rig.engine.layout.seal.counters.seals);
    let report = rig.engine.fold_once().await.expect("fold");
    assert_eq!(report.rows as u64, seals, "one directory row per seal");
    assert_eq!(report.entries, chunks as u64);
    assert_eq!(report.dropped, 0);
    assert!(
        directory_rows(&rig).await.is_empty(),
        "every 'K' row folded"
    );
    let held = {
        let dir = rig.engine.layout.seal.directory.read().unwrap();
        (dir.entries(), dir.bytes())
    };
    assert_eq!(held, (0, 0), "the directory holds nothing folded");
    let p = pointers(&rig).await;
    assert_eq!(p.len(), 1);
    let pointer = p.values().next().unwrap();
    assert_eq!(
        (pointer.level, pointer.seals, pointer.entries),
        (0, seals, chunks as u64)
    );
    assert_eq!(pointer.runs, streams.len() as u64);
    assert_eq!(pointer.id, report.object.unwrap());
    assert_eq!(index_objects(&rig).await, BTreeSet::from([pointer.id]));
    assert_eq!(*rig.engine.layout.seal.fold.set(), p);
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, "after the fold").await;
    let fold = &rig.engine.layout.seal.fold.counters;
    assert_eq!(get(&fold.footer_gets), 1, "one footer GET, then the cache");
    assert!(get(&fold.run_gets) >= streams.len() as u64);
    assert!(
        rig.engine.fold_once().await.unwrap().object.is_none(),
        "nothing left to fold"
    );
    let rig = reopen(rig).await;
    assert_eq!(
        *rig.engine.layout.seal.fold.set(),
        p,
        "the open rebuilds the set from the pointer rows"
    );
    assert_eq!(
        rig.engine.layout.seal.directory.read().unwrap().entries(),
        0
    );
    read_all(&rig, &oracle, "after the restart").await;
    check_tiling(&rig, &streams, "after the restart").await;
    rig.engine.begin_close();
}

/// One fold stopped at `stop` (or whose batch fails), then a restart.
/// Before the batch, the `'K'` rows stay and the object (if PUT) is an
/// orphan; after it, the pointer row stands and the rows are gone.
async fn fold_stopped(stop: Option<FoldStep>, seed: u64) {
    let rig = cold_rig("fold-stop").await;
    let mut rng = StdRng::seed_from_u64(seed);
    let streams = hashes(&mut rng, 4);
    let oracle = std::sync::Mutex::new(Oracle::new());
    sealed_rounds(&rig, &mut rng, &streams, &oracle, 6).await;
    let rows_before = directory_rows(&rig).await;
    let fs = &rig.engine.layout.seal.fold;
    match stop {
        Some(step) => *fs.hooks.abort_at.lock().unwrap() = Some(step),
        None => fs.hooks.fail_next_batch.store(true, Ordering::Relaxed),
    }
    let result = rig.engine.fold_once().await;
    match (stop, &result) {
        (Some(step), Err(FoldError::Aborted(at))) => assert_eq!(*at, step),
        (None, Err(FoldError::Store(_))) => {}
        (want, got) => panic!("stop {want:?} gave {got:?}"),
    }
    *fs.hooks.abort_at.lock().unwrap() = None;
    let committed = stop == Some(FoldStep::Committed);
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &format!("{stop:?}, before the restart")).await;
    let rig = reopen(rig).await;
    let after = directory_rows(&rig).await;
    let p = pointers(&rig).await;
    let objects = index_objects(&rig).await;
    if committed {
        assert!(
            after.is_empty() && p.len() == 1,
            "{stop:?}: the batch landed whole"
        );
        assert_eq!(
            objects,
            p.keys().copied().collect(),
            "{stop:?}: the object is referenced"
        );
    } else {
        assert_eq!(after, rows_before, "{stop:?}: every 'K' row stays");
        assert!(p.is_empty(), "{stop:?}: no pointer row");
        assert_eq!(objects.len(), 1, "{stop:?}: the PUT's object is an orphan");
    }
    read_all(&rig, &oracle, &format!("{stop:?}, after the restart")).await;
    check_tiling(&rig, &streams, &format!("{stop:?}")).await;
    let again = rig.engine.fold_once().await.expect("the next owner folds");
    assert_eq!(again.object.is_some(), !committed);
    assert!(directory_rows(&rig).await.is_empty());
    read_all(&rig, &oracle, &format!("{stop:?}, after the next fold")).await;
    rig.engine.begin_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fold_stopped_at_any_step_leaves_the_rows_or_the_pointer() {
    for (i, stop) in [
        Some(FoldStep::Put),
        Some(FoldStep::Verified),
        Some(FoldStep::Committed),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        fold_stopped(stop, 0xC0 + i as u64).await;
    }
}

/// A fold parked before its batch while a second writer opens the shard
/// log (fencing the first): its batch fails, its engine closes, and its
/// object is referenced by no pointer row; the new owner reads everything
/// and folds the same rows under its own epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fenced_folders_object_is_an_orphan() {
    let rig = cold_rig("fold-fence").await;
    let mut rng = StdRng::seed_from_u64(0xFE7C);
    let streams = hashes(&mut rng, 3);
    let oracle = std::sync::Mutex::new(Oracle::new());
    sealed_rounds(&rig, &mut rng, &streams, &oracle, 5).await;
    let rows = directory_rows(&rig).await;
    let hold = Hold::new();
    *rig.engine.layout.seal.fold.hooks.batch_hold.lock().unwrap() = Some(hold.clone());
    let fence = async {
        while hold.entered.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let orphan = index_objects(&rig).await;
        assert_eq!(orphan.len(), 1, "the fold's PUT landed before the fence");
        let fresh = open(
            rig.store.clone(),
            &rig.prefix,
            rig.settings,
            0,
            Duration::from_millis(2),
        )
        .await;
        hold.release();
        (orphan, fresh)
    };
    let (result, (orphan, fresh)) = tokio::join!(rig.engine.fold_once(), fence);
    assert!(
        matches!(result, Err(FoldError::Batch(_))),
        "the fenced batch fails: {result:?}"
    );
    assert!(
        rig.engine.is_closed(),
        "an index batch of unknown outcome closes the engine"
    );
    let id = *orphan.iter().next().unwrap();
    assert_eq!(
        id.epoch, rig.engine.writer_epoch,
        "named by the fenced writer's epoch"
    );
    assert!(fresh.writer_epoch > id.epoch);
    let rig = Rig {
        engine: fresh,
        ..rig
    };
    assert_eq!(
        directory_rows(&rig).await,
        rows,
        "the new owner finds every 'K' row"
    );
    assert!(
        pointers(&rig).await.is_empty(),
        "no pointer names the fenced object"
    );
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, "the new owner").await;
    let report = rig.engine.fold_once().await.unwrap();
    let mine = report.object.unwrap();
    assert_eq!(mine.epoch, rig.engine.writer_epoch);
    assert_eq!(
        index_objects(&rig).await,
        BTreeSet::from([id, mine]),
        "the fenced object stays an orphan"
    );
    assert_eq!(
        pointers(&rig).await.keys().copied().collect::<Vec<_>>(),
        vec![mine]
    );
    read_all(&rig, &oracle, "after the new owner's fold").await;
    rig.engine.begin_close();
}

fn configure_merges(rig: &Rig) {
    rig.engine.layout.seal.fold.configure(|s| {
        s.fan_in = [2, 2];
        s.auto = false;
    });
    rig.engine.gc_state().configure(|s| {
        s.grace = Duration::ZERO;
        s.auto = false;
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn merges_make_hourly_and_daily_objects_and_retire_their_inputs() {
    let mut rig = cold_rig("merge").await;
    configure_merges(&rig);
    let mut rng = StdRng::seed_from_u64(0x3E6);
    let streams = hashes(&mut rng, 4);
    let oracle = std::sync::Mutex::new(Oracle::new());
    for _ in 0..4 {
        sealed_rounds(&rig, &mut rng, &streams, &oracle, 3).await;
        rig.engine.fold_once().await.unwrap();
    }
    let folded: u64 = pointers(&rig).await.values().map(|p| p.entries).sum();
    assert_eq!(pointers(&rig).await.len(), 4);
    let mut merges = 0;
    while let Some(r) = rig.engine.merge_once().await {
        r.expect("merge");
        merges += 1;
    }
    assert_eq!(
        merges, 3,
        "four level 0 objects -> two level 1 -> one level 2"
    );
    let p = pointers(&rig).await;
    assert_eq!(p.len(), 1);
    let daily = p.values().next().unwrap().clone();
    let seals = get(&rig.engine.layout.seal.counters.seals);
    assert_eq!(
        (daily.level, daily.entries, daily.seals),
        (2, folded, seals)
    );
    rig.engine.collect().await.expect("garbage pass");
    assert_eq!(
        index_objects(&rig).await,
        BTreeSet::from([daily.id]),
        "merged-away objects deleted"
    );
    assert_eq!(get(&rig.engine.gc_state().counters.deleted), 6);
    let oracle_now = oracle.lock().unwrap().clone();
    read_all(&rig, &oracle_now, "after the merges").await;
    // A merge stopped after its read-back or after its batch, then a restart.
    for stop in [FoldStep::Verified, FoldStep::Committed] {
        for _ in 0..2 {
            sealed_rounds(&rig, &mut rng, &streams, &oracle, 2).await;
            rig.engine.fold_once().await.unwrap();
        }
        *rig.engine.layout.seal.fold.hooks.abort_at.lock().unwrap() = Some(stop);
        let r = rig.engine.merge_once().await.expect("a merge is due");
        assert!(matches!(r, Err(FoldError::Aborted(at)) if at == stop));
        *rig.engine.layout.seal.fold.hooks.abort_at.lock().unwrap() = None;
        let before = pointers(&rig).await;
        rig = reopen(rig).await;
        configure_merges(&rig);
        assert_eq!(
            pointers(&rig).await,
            before,
            "{stop:?}: the pointer rows are the batch's or the inputs'"
        );
        assert_eq!(
            *rig.engine.layout.seal.fold.set(),
            before,
            "{stop:?}: the open's set is the pointer rows"
        );
        let level = |l: u8| before.values().filter(|p| p.level == l).count();
        let committed = usize::from(stop == FoldStep::Committed);
        assert_eq!(
            (level(0), level(1), level(2)),
            (2, committed, 1),
            "{stop:?}"
        );
        let oracle_now = oracle.lock().unwrap().clone();
        read_all(&rig, &oracle_now, &format!("merge stopped at {stop:?}")).await;
        check_tiling(&rig, &streams, &format!("merge stopped at {stop:?}")).await;
    }
    rig.engine.begin_close();
}

/// Six seals of three records each to `hash`, under five routing keys in
/// turn: each record's offset and key.
async fn seed_keys(rig: &Rig, hash: [u8; 16]) -> Vec<(u64, String)> {
    let mut expected = Vec::new();
    for round in 0..6u64 {
        for i in 0..3u64 {
            let key = format!("key-{}", (round * 3 + i) % 5);
            let record = vec![bytes::Bytes::from(format!("r{round}-{i}"))];
            let last = append_lane(&rig.engine, hash, &key, record).await;
            expected.push((last, key));
        }
        rig.engine.seal_once(Trigger::Requested).await.unwrap();
    }
    expected
}

/// A durable read of `hash` from 0 that selects `key`.
async fn keyed(rig: &Rig, hash: [u8; 16], key: &str) -> crate::application::read::ReadPage {
    let at = ReadAt {
        from: 0,
        selector: Some(key),
        max_bytes: 1 << 20,
    };
    merged(&rig.engine, hash, at, Deliver::Durable)
        .await
        .unwrap()
}

/// Keyed reads of folded data: a key the runs hold reads back exactly; a
/// key no run holds skips every dense run by its bloom (no run GET, no
/// chunk GET) and consumes the offsets; a key a run bloom wrongly admits
/// costs that run's GET and no chunk GET (the chunk summaries are exact
/// fingerprints) and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn keyed_reads_skip_runs_their_bloom_excludes_and_survive_false_positives() {
    let rig = cold_rig("fold-keyed").await;
    let hash = [0x42; 16];
    let expected = seed_keys(&rig, hash).await;
    rig.engine.fold_once().await.unwrap();
    let end = expected.len() as u64;
    let fold = &rig.engine.layout.seal.fold.counters;
    let seal = &rig.engine.layout.seal.counters;
    let page = keyed(&rig, hash, "key-3").await;
    let want: Vec<u64> = expected
        .iter()
        .filter(|(_, k)| k == "key-3")
        .map(|(o, _)| *o)
        .collect();
    assert_eq!(
        page.recs.iter().map(|r| r.off).collect::<Vec<_>>(),
        want,
        "a held key reads back exactly"
    );
    let footer = rig
        .engine
        .fetch_footer(rig.engine.layout.seal.fold.set().values().next().unwrap())
        .await
        .unwrap();
    let run = footer.iter().find(|f| f.hash == hash).unwrap().clone();
    assert!(run.dense());
    let absent = (0..10_000)
        .map(|i| format!("absent-{i}"))
        .find(|k| !run.may_hold(crate::spill::directory::fingerprint(k)))
        .unwrap();
    let (runs_before, gets_before) = (get(&fold.run_gets), get(&seal.sealed_gets));
    let page = keyed(&rig, hash, &absent).await;
    assert!(page.recs.is_empty(), "a key no run holds serves nothing");
    assert_eq!(page.last, Some(end - 1), "and consumes every sealed offset");
    assert_eq!(
        (get(&fold.run_gets), get(&seal.sealed_gets)),
        (runs_before, gets_before),
        "without a GET"
    );
    assert!(get(&fold.run_bloom_skips) > 0);
    let held: BTreeSet<u64> = (0..5)
        .map(|i| crate::spill::directory::fingerprint(&format!("key-{i}")))
        .collect();
    let false_positive = (0..1_000_000)
        .map(|i| format!("fp-{i}"))
        .find(|k| {
            let fp = crate::spill::directory::fingerprint(k);
            run.may_hold(fp) && !held.contains(&fp)
        })
        .expect("a false positive of a 5-key bloom within a million keys");
    let (skips_before, gets_before) = (get(&seal.summary_skips), get(&seal.sealed_gets));
    let page = keyed(&rig, hash, &false_positive).await;
    assert!(
        page.recs.is_empty(),
        "a bloom's false positive serves nothing"
    );
    assert_eq!(page.last, Some(end - 1));
    assert_eq!(
        get(&seal.sealed_gets),
        gets_before,
        "the chunk summaries skip every chunk"
    );
    assert_eq!(get(&seal.summary_skips) - skips_before, run.entries as u64);
    rig.engine.begin_close();
}

/// **Tiling across the directory, the run-index objects and the pending
/// pages.** Random appends, seals, folds and merges, each fold or merge
/// stopped at a random step; after each, the tiling invariant; at the end
/// a restart and every record read back at both visibilities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn tiling_across_directory_run_index_and_pending_with_stopped_folds() {
    for seed in 0..4u64 {
        tiling_seed(seed).await;
    }
}

fn tiling_settings(s: &mut super::fold::FoldSettings) {
    s.fan_in = [2, 3];
    s.auto = false;
}

async fn tiling_seed(seed: u64) {
    let mut rng = StdRng::seed_from_u64(0x7105 ^ seed);
    let mut rig = cold_rig("fold-tiling").await;
    let max_rows = rng.random_range(1..=4);
    rig.engine.layout.seal.fold.configure(|s| {
        tiling_settings(s);
        s.max_rows = max_rows;
    });
    let n = rng.random_range(2..=5);
    let streams = hashes(&mut rng, n);
    let oracle = std::sync::Mutex::new(Oracle::new());
    let stops = [
        None,
        Some(FoldStep::Put),
        Some(FoldStep::Verified),
        Some(FoldStep::Committed),
    ];
    for round in 0..30 {
        let rounds = rng.random_range(0..=2);
        sealed_rounds(&rig, &mut rng, &streams, &oracle, rounds).await;
        let stop = stops[rng.random_range(0..stops.len())];
        *rig.engine.layout.seal.fold.hooks.abort_at.lock().unwrap() = stop;
        let result = if rng.random_bool(0.5) {
            rig.engine.fold_once().await.map(|_| ())
        } else {
            rig.engine
                .merge_once()
                .await
                .map_or(Ok(()), |r| r.map(|_| ()))
        };
        *rig.engine.layout.seal.fold.hooks.abort_at.lock().unwrap() = None;
        if let Err(e) = result {
            assert!(matches!(e, FoldError::Aborted(_)), "seed {seed}: {e}");
            if stop == Some(FoldStep::Committed) {
                rig = reopen(rig).await;
                rig.engine.layout.seal.fold.configure(tiling_settings);
            }
        }
        check_tiling(&rig, &streams, &format!("seed {seed} round {round}")).await;
    }
    for hash in &streams {
        append_recorded(
            &rig.engine,
            &oracle,
            *hash,
            vec![random_bytes(&mut rng, 200)],
        )
        .await;
    }
    rig = reopen(rig).await;
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, &format!("seed {seed} after the restart")).await;
    rig.engine.begin_close();
}

/// A pointer row that does not decode is skipped and counted at the open;
/// every other object is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_corrupt_index_row_is_skipped_and_counted() {
    let rig = cold_rig("fold-corrupt").await;
    let mut rng = StdRng::seed_from_u64(0xBAD);
    let streams = hashes(&mut rng, 2);
    let oracle = std::sync::Mutex::new(Oracle::new());
    sealed_rounds(&rig, &mut rng, &streams, &oracle, 3).await;
    rig.engine.fold_once().await.unwrap();
    let mut batch = slatedb::WriteBatch::new();
    batch.put(
        crate::spill::runindex::pointer_key(ObjectId { epoch: 1, seq: 999 }),
        b"\x09junk",
    );
    batch.put(
        crate::spill::directory::directory_key(ObjectId { epoch: 1, seq: 998 }),
        b"\x07",
    );
    let written = rig.engine.db.write(batch).await.unwrap();
    settle(&rig.engine).await;
    written.await_durable().await.unwrap();
    let rig = reopen(rig).await;
    assert_eq!(get(&rig.engine.layout.seal.fold.counters.corrupt_rows), 2);
    assert_eq!(rig.engine.layout.seal.fold.set().len(), 1);
    let oracle = oracle.into_inner().unwrap();
    read_all(&rig, &oracle, "beside a corrupt row").await;
    rig.engine.begin_close();
}

/// The `'K'` rows' keys, oldest first.
async fn directory_keys(rig: &Rig) -> Vec<Vec<u8>> {
    let prefix = crate::spill::directory::directory_prefix();
    let mut rows = rig.engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = rows.next().await.unwrap() {
        out.push(kv.key.to_vec());
    }
    out
}

/// Overwrite `key` with bytes no row codec accepts, durably.
async fn corrupt_row(rig: &Rig, key: Vec<u8>) {
    let mut batch = slatedb::WriteBatch::new();
    batch.put(key, b"\x7fnot a row");
    let written = rig.engine.db.write(batch).await.unwrap();
    settle(&rig.engine).await;
    written.await_durable().await.unwrap();
}

/// One append of three records to `hash` under key `k`, then a seal of
/// that stream alone (nothing else is unsealed).
async fn sealed_alone(
    rig: &Rig,
    oracle: &std::sync::Mutex<Oracle>,
    hash: [u8; 16],
    rng: &mut StdRng,
) {
    let records = (0..3).map(|_| random_bytes(rng, 400)).collect();
    append_recorded(&rig.engine, oracle, hash, records).await;
    rig.engine.seal_once(Trigger::Requested).await.unwrap();
}

/// A read of `hash` from `from` (keyed when `key` is set), as text.
async fn read_result(
    rig: &Rig,
    hash: [u8; 16],
    from: u64,
    key: Option<&str>,
) -> Result<usize, String> {
    let at = ReadAt {
        from,
        selector: key,
        max_bytes: 1 << 20,
    };
    merged(&rig.engine, hash, at, Deliver::Durable)
        .await
        .map(|p| p.recs.len())
}

/// **Edge #222's condition.** A seal directory row of stream A's middle
/// seal is overwritten with bytes that do not decode, then the shard
/// restarts: the open skips and counts it once, the sealer keeps sealing
/// and the fold keeps folding (leaving the row in place), every read of A
/// whose window reaches the hole answers corruption (unkeyed and keyed),
/// a read of A past the hole and every read of B are exact. The same for
/// a pointer row: after a fold, the pointer row is overwritten, and A's
/// folded offsets answer corruption while a stream sealed afterwards reads
/// exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_gap_a_corrupt_index_row_leaves_answers_corruption_and_nothing_else_does() {
    let rig = cold_rig("fold-gap").await;
    let mut rng = StdRng::seed_from_u64(0x222);
    let (a, b, c) = ([0xA1; 16], [0xB2; 16], [0xC3; 16]);
    let oracle = std::sync::Mutex::new(Oracle::new());
    for hash in [a, a, a, b] {
        sealed_alone(&rig, &oracle, hash, &mut rng).await;
    }
    let keys = directory_keys(&rig).await;
    assert_eq!(keys.len(), 4, "one 'K' row per seal");
    corrupt_row(&rig, keys[1].clone()).await;
    let rig = reopen(rig).await;
    let fold = &rig.engine.layout.seal.fold.counters;
    assert_eq!(
        (get(&fold.corrupt_rows), get(&fold.corrupt_index_rows)),
        (1, 1)
    );
    let hole = |r: Result<usize, String>| {
        r.is_err_and(|e| e.contains("sealed range corrupt") && e.contains("did not decode"))
    };
    assert!(
        hole(read_result(&rig, a, 0, None).await),
        "a read through the hole answers corruption"
    );
    assert!(
        hole(read_result(&rig, a, 0, Some("")).await),
        "and a keyed one"
    );
    assert_eq!(
        read_result(&rig, a, 6, None).await,
        Ok(3),
        "a read past the hole is exact"
    );
    let oracle_b = oracle.lock().unwrap()[&b].clone();
    read_back(&rig.engine, b, &oracle_b, Deliver::Durable)
        .await
        .unwrap();
    sealed_alone(&rig, &oracle, b, &mut rng).await;
    sealed_alone(&rig, &oracle, c, &mut rng).await;
    let report = rig
        .engine
        .fold_once()
        .await
        .expect("the fold keeps folding");
    assert_eq!(report.rows, 5, "every row but the corrupt one");
    assert_eq!(
        directory_keys(&rig).await,
        vec![keys[1].clone()],
        "the corrupt row stays where it is"
    );
    assert_eq!(
        get(&fold.corrupt_rows),
        1,
        "each skipped row is counted once"
    );
    assert!(
        hole(read_result(&rig, a, 0, None).await),
        "the hole outlives the fold"
    );
    for hash in [b, c] {
        let o = oracle.lock().unwrap()[&hash].clone();
        read_back(&rig.engine, hash, &o, Deliver::Durable)
            .await
            .unwrap();
    }
    assert_eq!(rig.engine.sealed_index_corrupt_rows(), 1);
    // A pointer row that does not decode.
    let pointer = pointers(&rig).await.keys().next().copied().unwrap();
    corrupt_row(&rig, crate::spill::runindex::pointer_key(pointer)).await;
    let d = [0xD4; 16];
    sealed_alone(&rig, &oracle, d, &mut rng).await;
    let rig = reopen(rig).await;
    let fold = &rig.engine.layout.seal.fold.counters;
    assert_eq!(
        get(&fold.corrupt_index_rows),
        2,
        "the 'K' row and the pointer row"
    );
    assert!(rig.engine.layout.seal.fold.set().is_empty());
    assert!(
        hole(read_result(&rig, c, 0, None).await),
        "c's folded chunks are behind the pointer row"
    );
    let o = oracle.lock().unwrap()[&d].clone();
    read_back(&rig.engine, d, &o, Deliver::Durable)
        .await
        .unwrap();
    rig.engine.begin_close();
}
