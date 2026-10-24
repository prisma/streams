//! R08-A: real persisted scans, rather than an ordered-key model.
use super::{
    Deliver, ShardConfig, ShardEngine, ShardMaintenance, TailFields, encode_tail, producer_key,
    read_frames, read_frames_range, record::RangeReadError, tail_key,
};
use crate::crypto_page::{PageCipher, PageLane, shard_page_key};
use bytes::Bytes;
use slatedb::{Db, WriteBatch};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

/// A one-record page at `offset` in `lane`.
fn encoded_record(offset: u64, lane: &str) -> Bytes {
    let lane = PageLane {
        key_version: 1,
        routing_key: lane,
    };
    let page = PageCipher::new(&[7; 32], &[8; 16])
        .seal(
            &lane,
            offset,
            &crate::crypto_page::stamped(1, &[b"retained payload"]),
        )
        .unwrap();
    Bytes::from(page.bytes)
}

async fn scan_fixture(
    label: &str,
    key: Vec<u8>,
    value: Bytes,
    ring_bytes: usize,
) -> Arc<ShardEngine> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder(label, store.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(5)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let mut batch = WriteBatch::new();
    batch.put(
        tail_key(&[8; 16]),
        encode_tail(&TailFields {
            next: 512,
            ..Default::default()
        }),
    );
    batch.put(key, value);
    batch.put(
        producer_key(&[8; 16], &[9; 16], "unchanged"),
        b"checkpoint sentinel",
    );
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let (tx, _rx) = mpsc::channel(1);
    ShardEngine::start(
        label.into(),
        db,
        store,
        ShardConfig {
            tail_ring_bytes: ring_bytes,
            ..Default::default()
        },
        tx,
        None,
        ShardMaintenance::default(),
    )
}

async fn stored_rows(engine: &ShardEngine) -> Vec<(Bytes, Bytes)> {
    let mut scan = engine.db.scan_prefix([8; 16], ..).await.unwrap();
    let mut rows = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key, row.value));
    }
    rows
}

#[expect(
    clippy::disallowed_methods,
    reason = "r08a_database_record_corruption_refuses_progress_without_mutation; the fixture spawns the readers it joins immediately so their panics surface as join errors; a supervised spawn would tie the fixture's teardown to a supervisor it never builds"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "r08a_database_record_corruption_refuses_progress_without_mutation; the fixture nests each read inside the task it spawns and joins; flattening it would separate the read from the panic boundary it needs"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_database_record_corruption_refuses_progress_without_mutation() {
    let mut short = shard_page_key(&[8; 16], 1);
    short.remove(17);
    let mut long = shard_page_key(&[8; 16], 1);
    long.push(0);
    let cases = [
        ("short", short, encoded_record(1, "wanted")),
        ("long", long, encoded_record(1, "wanted")),
        (
            "offset",
            shard_page_key(&[8; 16], 1),
            encoded_record(2, "other"),
        ),
        (
            "frame",
            shard_page_key(&[8; 16], 1),
            Bytes::from_static(b"invalid frame"),
        ),
    ];
    for (label, key, value) in cases {
        assert!(shard_page_key(&[8; 16], 0) < key && key < shard_page_key(&[8; 16], 512));
        let engine = scan_fixture(&format!("r08a-{label}"), key, value, 0).await;
        let handle = engine.stream_handle([8; 16]).await.unwrap();
        let before = stored_rows(&engine).await;
        let ordinary =
            tokio::spawn({
                let engine = engine.clone();
                let handle = handle.clone();
                async move {
                    read_frames(&engine, &handle, 0, Some("wanted"), 1024, Deliver::Durable).await
                }
            })
            .await;
        let absorber = tokio::spawn({
            let engine = engine.clone();
            let handle = handle.clone();
            async move { read_frames_range(&engine, &handle, 0, 512, 1024).await }
        })
        .await;
        assert_eq!(
            before,
            stored_rows(&engine).await,
            "reads must not alter data or checkpoint state"
        );
        engine.begin_close();
        engine
            .await_terminated(Duration::from_secs(5))
            .await
            .unwrap();
        assert!(ordinary.is_ok(), "{label}: ordinary scan panicked");
        assert!(absorber.is_ok(), "{label}: absorber scan panicked");
        assert_eq!(
            ordinary.unwrap().err().expect("corrupt row refused").kind(),
            slatedb::ErrorKind::Data
        );
        assert!(matches!(absorber.unwrap(), Err(RangeReadError::Corrupt(_))));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_valid_filtered_miss_keeps_legitimate_progress() {
    let engine = scan_fixture(
        "r08a-valid-miss",
        shard_page_key(&[8; 16], 1),
        encoded_record(1, "other"),
        0,
    )
    .await;
    let handle = engine.stream_handle([8; 16]).await.unwrap();
    let page = read_frames(&engine, &handle, 0, Some("wanted"), 1024, Deliver::Durable)
        .await
        .unwrap();
    assert!(page.frames.is_empty());
    assert_eq!(page.last_offset, Some(1));
    let range = read_frames_range(&engine, &handle, 0, 512, 1024)
        .await
        .unwrap();
    assert_eq!(range.frames.len(), 1);
    assert_eq!(range.last_offset, Some(1));
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
}

/// KANI-017's regression: a stored row is admitted as a page only under its
/// own stream's page prefix and its own last offset, and only when it is
/// exactly one whole page with its tag.
#[test]
fn r08a_record_boundary_validates_namespace_extent_and_offset() {
    use crate::crypto_page::{CheckedPage, PageCorruption, shard_page_prefix};
    let prefix = shard_page_prefix(&[8; 16]);
    let key = shard_page_key(&[8; 16], 1);
    let raw = encoded_record(1, "other");
    let admit = |key: &[u8], raw: &[u8]| {
        CheckedPage::from_row(key, &prefix, Bytes::copy_from_slice(raw)).map(|page| page.last())
    };
    assert_eq!(admit(&key, &raw), Ok(1));
    for at in [0, 15, 16] {
        let mut foreign = key.clone();
        foreign[at] ^= 1;
        assert_eq!(admit(&foreign, &raw), Err(PageCorruption::Namespace));
    }
    for width in [0, 17, 24, 26] {
        let mut invalid = key.clone();
        invalid.resize(width, 0);
        assert_eq!(admit(&invalid, &raw), Err(PageCorruption::KeyWidth));
    }
    assert_eq!(
        admit(&key, &encoded_record(2, "other")),
        Err(PageCorruption::Offset { key: 1, page: 2 })
    );
    let mut trailing = raw.to_vec();
    trailing.push(0);
    assert_eq!(admit(&key, &trailing), Err(PageCorruption::Trailing));
    assert_eq!(
        admit(&key, &raw[..raw.len() - 1]),
        Err(PageCorruption::Truncated)
    );
    // A ciphertext shorter than an AEAD tag can never authenticate, so a
    // length-consistent page carrying one is corrupt rather than short.
    let ct_len = raw.len() - 4 - (16 + b"retained payload".len() + 2);
    let mut tagless = raw[..ct_len].to_vec();
    tagless.extend_from_slice(&8u32.to_be_bytes());
    tagless.extend_from_slice(&[0; 8]);
    assert_eq!(admit(&key, &tagless), Err(PageCorruption::Tag));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_invalid_ring_copy_retries_storage_without_false_filtered_progress() {
    let engine = scan_fixture(
        "r08a-ring",
        shard_page_key(&[8; 16], 1),
        encoded_record(1, "wanted"),
        1 << 20,
    )
    .await;
    let handle = engine.stream_handle([8; 16]).await.unwrap();
    handle
        .ring
        .lock()
        .unwrap()
        .batches
        .push_back(super::RingBatch {
            first: 0,
            next: 512,
            frames: vec![(1, Bytes::from_static(b"broken"))],
            bytes: 6,
        });
    let window = crate::shard::RingScan {
        from: 0,
        to: 512,
        max_bytes: 1024,
    };
    assert!(engine.ring_read(&handle, window, None).is_none());
    assert!(engine.ring_read(&handle, window, Some("wanted")).is_none());
    let page = read_frames(&engine, &handle, 0, Some("wanted"), 1024, Deliver::Durable)
        .await
        .unwrap();
    let served: Vec<Bytes> = page.frames.iter().map(|s| s.page().raw().clone()).collect();
    assert_eq!(served, vec![encoded_from_store(&engine).await]);
    assert_eq!(page.last_offset, Some(1));
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
}

async fn encoded_from_store(engine: &ShardEngine) -> Bytes {
    engine
        .db
        .get(shard_page_key(&[8; 16], 1))
        .await
        .unwrap()
        .unwrap()
}

// ---- E2: a live window's scan keeps the blocks it fetched ----

/// The E2 fixtures' stream.
const LIVE: [u8; 16] = [0x2e; 16];
/// Its records: one per page, of `LIVE_PAYLOAD` bytes no compressor shrinks.
const LIVE_RECORDS: u64 = 1_200;
const LIVE_PAYLOAD: usize = 1_000;
/// Where the older of its two sorted runs ends.
const OLDER_RUN_END: u64 = 1_000;

/// `n` bytes of a xorshift64 stream seeded by `seed`.
fn incompressible(seed: u64, n: usize) -> Vec<u8> {
    let mut state = seed | 1;
    std::iter::repeat_with(|| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state.to_le_bytes()
    })
    .flatten()
    .take(n)
    .collect()
}

/// The row of the live stream's one-record page at `offset`.
fn live_page(offset: u64) -> (Vec<u8>, Bytes) {
    let lane = PageLane {
        key_version: 1,
        routing_key: "live",
    };
    let record = incompressible(offset, LIVE_PAYLOAD);
    let page = PageCipher::new(&[7; 32], &LIVE)
        .seal(&lane, offset, &crate::crypto_page::stamped(1, &[record]))
        .unwrap();
    (shard_page_key(&LIVE, page.last), Bytes::from(page.bytes))
}

/// Writes the live stream's pages of `offsets` and its tail row (its head
/// at `offsets.end`, `stored` plus these pages' bytes unabsorbed), makes
/// them durable and flushes the memtable to an L0. Returns the stream's
/// unabsorbed bytes.
async fn flush_live_pages(db: &Db, offsets: std::ops::Range<u64>, mut stored: u64) -> u64 {
    let mut batch = WriteBatch::new();
    for offset in offsets.clone() {
        let (key, value) = live_page(offset);
        stored += value.len() as u64;
        batch.put(key, value);
    }
    let tail = TailFields {
        next: offsets.end,
        unabsorbed_bytes: stored,
        ..Default::default()
    };
    batch.put(tail_key(&LIVE), encode_tail(&tail));
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    db.flush_with_options(memtable).await.unwrap();
    stored
}

/// Waits until the database's L0s are compacted into `runs` sorted runs.
async fn await_sorted_runs(db: &Db, runs: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let manifest = db.manifest();
        if (manifest.l0().len(), manifest.compacted().len()) == (0, runs) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the L0s never compacted into {runs} sorted runs"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A compactor that turns every four L0s into a sorted run of their own
/// and never merges sorted runs: each compaction takes exactly four
/// sources, and a run four L0s large is too large to join L0s.
fn four_l0s_per_run() -> slatedb::config::CompactorOptions {
    let scheduler = [
        ("min_compaction_sources", "4"),
        ("max_compaction_sources", "4"),
        ("include_size_threshold", "1.5"),
    ];
    slatedb::config::CompactorOptions {
        poll_interval: Duration::from_millis(10),
        commit_compacted_interval: Duration::from_millis(20),
        scheduler_options: scheduler
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect(),
        worker: Some(slatedb::config::CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(10),
            max_subcompactions: 1,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// An E2 fixture: the engine, its live stream's handle, and the store
/// beneath them counting GETs.
struct TwoRuns {
    engine: Arc<ShardEngine>,
    handle: Arc<super::StreamHandle>,
    store: Arc<crate::dst::FaultStore>,
}

/// The field's shard-log shape under a lagging reader, ring off: the live
/// stream's pages in two sorted runs (compaction output, whose data blocks
/// no write path caches), `[0, OLDER_RUN_END)` in the older and the rest
/// in the newer, over the block cache production passes slatedb
/// (`BlockCache`, one 16 MiB shard). Each run holds a version of the
/// stream's tail row, which sorts after its pages, so each overlaps every
/// window of the stream, as a real shard's other rows make it do.
async fn two_run_fixture(label: &str) -> TwoRuns {
    let store = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        0xe2,
        crate::dst::FaultPlan::CLEAN,
    );
    let cache =
        crate::runtime::block_cache::BlockCache::new(slatedb::db_cache::foyer::FoyerCacheOptions {
            max_capacity: 16 << 20,
            shards: 1,
        });
    let db = Db::builder(label, store.clone() as Arc<dyn object_store::ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            manifest_poll_interval: Duration::from_millis(20),
            compactor_options: Some(four_l0s_per_run()),
            ..Default::default()
        })
        .with_db_cache(Arc::new(cache))
        .build()
        .await
        .unwrap();
    let mut stored = 0;
    for (runs, offsets) in [(1, 0..OLDER_RUN_END), (2, OLDER_RUN_END..LIVE_RECORDS)] {
        let quarter = (offsets.end - offsets.start) / 4;
        for part in 0..4 {
            let start = offsets.start + part * quarter;
            stored = flush_live_pages(&db, start..start + quarter, stored).await;
        }
        await_sorted_runs(&db, runs).await;
    }
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        label.into(),
        Arc::new(db),
        store.clone(),
        ShardConfig {
            tail_ring_bytes: 0,
            ..Default::default()
        },
        tx,
        None,
        ShardMaintenance::default(),
    );
    let handle = engine.stream_handle(LIVE).await.unwrap();
    TwoRuns {
        engine,
        handle,
        store,
    }
}

/// SST GETs so far, once none is still landing: a scan spawns its
/// read-ahead fetches eagerly and may return before they complete.
async fn settled_sst_gets(store: &crate::dst::FaultStore) -> u64 {
    let mut last = None;
    loop {
        let now = store.count(crate::dst::StoreOp::Get, crate::dst::ObjClass::Sst);
        if last == Some(now) {
            return now;
        }
        last = Some(now);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Who reads a window: an application read under a stored-byte budget, or
/// the absorber's range read.
#[derive(Clone, Copy)]
enum Reader {
    Application(usize),
    Absorber,
}

/// The SST GETs of each of `reads` reads of `window` by `reader`, each of
/// which returns exactly the window's pages.
async fn sst_gets_per_read(
    fixture: &TwoRuns,
    reader: Reader,
    window: std::ops::Range<u64>,
    reads: usize,
) -> Vec<u64> {
    let TwoRuns {
        engine,
        handle,
        store,
    } = fixture;
    let (from, to) = (window.start, window.end);
    let mut gets = Vec::new();
    for _ in 0..reads {
        let before = settled_sst_gets(store).await;
        let read = match reader {
            Reader::Application(budget) => super::record::read_frames_until(
                engine,
                handle,
                from,
                to,
                None,
                budget,
                Deliver::Durable,
            )
            .await
            .unwrap(),
            Reader::Absorber => read_frames_range(engine, handle, from, to, 8 << 20)
                .await
                .unwrap(),
        };
        gets.push(settled_sst_gets(store).await - before);
        let pages: Vec<(u64, u64)> = read.frames.iter().map(|s| (s.first(), s.last())).collect();
        assert_eq!(pages, window.clone().map(|o| (o, o)).collect::<Vec<_>>());
        assert_eq!(read.last_offset, Some(to - 1));
    }
    gets
}

async fn close(engine: &ShardEngine) {
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
}

/// E2: a lagging reader's window reaching across both sorted runs to the
/// head GETs a block of each run on its first read; the scan keeps them in
/// the block cache, so reading the same window again GETs nothing. A tail
/// window lies in the newer run alone, but the older run overlaps it too
/// and costs a GET of its own on the first read, then none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2_a_live_window_reads_its_sorted_runs_once() {
    let fixture = two_run_fixture("e2-live").await;
    let lagging = OLDER_RUN_END - 40..LIVE_RECORDS;
    let gets = sst_gets_per_read(&fixture, Reader::Application(1 << 20), lagging, 3).await;
    assert_eq!(gets, [2, 0, 0], "lagging window GETs");
    close(&fixture.engine).await;
    let fixture = two_run_fixture("e2-tail").await;
    let tail = LIVE_RECORDS - 3..LIVE_RECORDS;
    let gets = sst_gets_per_read(&fixture, Reader::Application(1 << 20), tail, 2).await;
    assert_eq!(gets, [2, 0], "tail window GETs");
    close(&fixture.engine).await;
}

/// E2's bound: a catch-up through the stream's whole unabsorbed backlog,
/// over 1 MiB, is not a live window, so each read GETs its blocks again;
/// the absorber's range read caches nothing whatever its window, while an
/// application read of the same window then GETs once and is served from
/// the cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2_a_catch_up_and_the_absorber_leave_the_block_cache_alone() {
    let fixture = two_run_fixture("e2-walk").await;
    let backlog = 0..LIVE_RECORDS;
    let gets = sst_gets_per_read(&fixture, Reader::Application(8 << 20), backlog, 2).await;
    assert_eq!(gets, [2, 2], "catch-up GETs");
    let lagging = OLDER_RUN_END - 40..LIVE_RECORDS;
    let gets = sst_gets_per_read(&fixture, Reader::Absorber, lagging.clone(), 2).await;
    assert_eq!(gets, [2, 2], "absorber GETs");
    let gets = sst_gets_per_read(&fixture, Reader::Application(1 << 20), lagging, 2).await;
    assert_eq!(gets, [2, 0], "the same window read live");
    close(&fixture.engine).await;
}

/// A live window's scan is the page scan of its budget and durability,
/// and it keeps the blocks it fetches in the block cache.
#[test]
fn e2_a_live_window_scan_is_the_page_scan_that_caches_its_blocks() {
    use super::record::live_scan_opts;
    use slatedb::config::DurabilityLevel::{Memory, Remote};
    for (budget, durability, read_ahead) in [
        (0, Remote, 64 << 10),
        (1 << 20, Memory, (1 << 20) + (64 << 10)),
        (8 << 20, Remote, 2 << 20),
    ] {
        let opts = live_scan_opts(budget, durability);
        assert_eq!(
            (
                opts.durability_filter,
                opts.read_ahead_bytes,
                opts.max_fetch_tasks,
                opts.cache_blocks
            ),
            (durability, read_ahead, 4, true),
            "budget {budget}"
        );
    }
}

/// A window is live while the pages its scan can reach hold at most 1 MiB
/// at its stream's average unabsorbed bytes per offset. The scan reaches
/// from the window's start to the end of its key range (4,095 offsets past
/// the window) or the durable head, whichever is first, and never stops
/// before the window's own end.
#[test]
fn e2_a_live_window_reaches_at_most_one_mib_of_its_streams_pages() {
    use super::record::{LIVE_SCAN_BYTES, live_window};
    let tail = |absorbed: u64, next: u64, unabsorbed_bytes: u64| TailFields {
        absorbed,
        next,
        unabsorbed_bytes,
        ..Default::default()
    };
    assert_eq!(LIVE_SCAN_BYTES, 1 << 20);
    // 1 KiB per offset over [0, 2048): 1 MiB is 1,024 offsets.
    let kib = tail(0, 2048, 2 << 20);
    assert!(live_window(&kib, 1024, 2048));
    assert!(!live_window(&kib, 1023, 2048));
    // A window short of the head reaches the head.
    assert!(live_window(&kib, 1024, 1025));
    assert!(!live_window(&kib, 1023, 1024));
    // The average is over the unabsorbed offsets alone.
    let absorbed = tail(1000, 3048, 2 << 20);
    assert!(live_window(&absorbed, 2024, 3048));
    assert!(!live_window(&absorbed, 2023, 3048));
    // 16 bytes per offset: 1 MiB is 65,536 offsets, but the key range ends
    // 4,095 offsets past the window.
    let small = tail(0, 1 << 20, 16 << 20);
    assert!(live_window(&small, 0, 1));
    assert!(live_window(&small, 0, (1 << 16) - 4095));
    assert!(!live_window(&small, 0, (1 << 16) - 4094));
    // An applied read past the durable head reaches its own end.
    let head = tail(0, 100, 100 << 10);
    assert!(live_window(&head, 0, 1024));
    assert!(!live_window(&head, 0, 1025));
    // Nothing unabsorbed: the applied suffix alone, which no table holds.
    assert!(live_window(&tail(500, 500, 0), 500, 510));
    // The products cannot overflow.
    let huge = tail(0, u64::MAX, u64::MAX);
    assert!(live_window(&huge, 0, (1 << 20) - 4095));
    assert!(!live_window(&huge, 0, (1 << 20) - 4094));
}
