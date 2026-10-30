//! R08-A: real persisted reads, rather than an ordered-key model: a
//! stream's pages as layout 6 holds them, unsealed in its pending row and
//! sealed in a segment object.
use super::{
    Deliver, RingScan, ShardConfig, ShardEngine, TailFields, encode_tail, producer_key,
    read_frames, record::RangeReadError, tail_key,
};
use crate::crypto_page::{PageCipher, PageLane};
use crate::shard::seal::Trigger;
use crate::spill::pending::{concat_pages, pending_key};
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::WriteBatch;
use std::{sync::Arc, time::Duration};

/// The fixtures' stream.
const STREAM: [u8; 16] = [8; 16];

/// A one-record page at `offset` in `lane`.
fn encoded_record(offset: u64, lane: &str) -> Bytes {
    let lane = PageLane {
        key_version: 1,
        routing_key: lane,
    };
    let page = PageCipher::new(&[7; 32], &STREAM)
        .seal(
            &lane,
            offset,
            &crate::crypto_page::stamped(1, &[b"retained payload"]),
        )
        .unwrap();
    Bytes::from(page.bytes)
}

/// An engine over its own store whose stream holds `pages` unsealed, back
/// to back in its pending row as `[0, next)`, beside a producer row that no
/// read may change. Its sealer is open and seals only when a test asks.
async fn scan_fixture(
    label: &str,
    pages: &[Bytes],
    next: u64,
    ring_bytes: usize,
) -> (Arc<ShardEngine>, Arc<dyn ObjectStore>) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        crate::keyspace::db_builder(label, store.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(5)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let pending = concat_pages(pages);
    let mut batch = WriteBatch::new();
    batch.put(
        tail_key(&STREAM),
        encode_tail(&TailFields {
            next,
            unabsorbed_bytes: pending.len() as u64,
            ..Default::default()
        }),
    );
    batch.put(pending_key(&STREAM), pending);
    batch.put(
        producer_key(&STREAM, &[9; 16], "unchanged"),
        b"checkpoint sentinel",
    );
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let maintenance = super::load_or_rebuild_maintenance(&db).await.unwrap();
    let engine = ShardEngine::start(
        label.into(),
        db,
        store.clone(),
        ShardConfig {
            tail_ring_bytes: ring_bytes,
            ..Default::default()
        },
        None,
        maintenance,
    );
    engine.configure_seal(|s| s.auto = false);
    engine.seal_ready().await.unwrap();
    (engine, store)
}

/// Every stored row of the shard log.
async fn stored_rows(engine: &ShardEngine) -> Vec<(Bytes, Bytes)> {
    let mut scan = engine.db.scan(..).await.unwrap();
    let mut rows = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key, row.value));
    }
    rows
}

async fn close(engine: &ShardEngine) {
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
}

/// Seal the fixture stream, then put `replacement` in its segment object in
/// place of the sealed page `page` (same length, so every other byte of
/// the object stays where it was).
async fn corrupt_sealed_page(
    engine: &Arc<ShardEngine>,
    store: &Arc<dyn ObjectStore>,
    page: &Bytes,
    replacement: &[u8],
) {
    engine.seal_once(Trigger::Requested).await.unwrap();
    let entry = engine
        .layout
        .seal
        .directory
        .read()
        .unwrap()
        .from(&STREAM, 0, 1)[0]
        .clone();
    let path = crate::spill::object::object_path(
        &crate::sharddir::shard_db_path(&engine.prefix),
        entry.object,
    );
    let path = object_store::path::Path::from(path);
    let mut body = store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    let at = body
        .windows(page.len())
        .position(|bytes| bytes == page.as_ref())
        .expect("the object holds the page byte for byte");
    body[at..at + page.len()].copy_from_slice(replacement);
    store.put(&path, body.into()).await.unwrap();
}

/// A page that is not the one due (its offset is 2, offset 1 was due) and
/// bytes that are no page are refused without a panic and without changing
/// a stored row: in the pending row the stream's load fails, and in a
/// sealed chunk both the read and the range read beneath it fail with a
/// Data error, leaving the directory as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn r08a_database_record_corruption_refuses_progress_without_mutation() {
    let wanted = encoded_record(1, "wanted");
    let cases = [
        ("offset", encoded_record(2, "wanted")),
        ("frame", Bytes::from(vec![0xa5; wanted.len()])),
    ];
    for (label, bad) in cases {
        let pages = [encoded_record(0, "other"), bad.clone()];
        let (engine, _store) = scan_fixture(&format!("r08a-pending-{label}"), &pages, 2, 0).await;
        let before = stored_rows(&engine).await;
        let refused = engine.stream_handle(STREAM).await.err();
        assert_eq!(
            refused.map(|e| e.kind()),
            Some(slatedb::ErrorKind::Data),
            "{label}: a pending row that does not tile was loaded"
        );
        assert_eq!(
            before,
            stored_rows(&engine).await,
            "{label}: the load changed a row"
        );
        assert!(!engine.streams.lock().unwrap().contains_key(&STREAM));
        close(&engine).await;
        sealed_corruption_is_refused(label, &bad).await;
    }
}

/// The sealed half of the corruption case `label`: `bad` replaces the
/// stream's sealed page 1.
async fn sealed_corruption_is_refused(label: &str, bad: &Bytes) {
    let wanted = encoded_record(1, "wanted");
    let pages = [encoded_record(0, "other"), wanted.clone()];
    let (engine, store) = scan_fixture(&format!("r08a-sealed-{label}"), &pages, 2, 0).await;
    corrupt_sealed_page(&engine, &store, &wanted, bad).await;
    let handle = engine.stream_handle(STREAM).await.unwrap();
    let directory = || {
        engine
            .layout
            .seal
            .directory
            .read()
            .unwrap()
            .from(&STREAM, 0, 16)
    };
    let (rows, entries) = (stored_rows(&engine).await, directory());
    let ordinary = read_frames(&engine, &handle, 0, Some("wanted"), 1024, Deliver::Durable).await;
    assert_eq!(
        ordinary.err().map(|e| e.kind()),
        Some(slatedb::ErrorKind::Data),
        "{label}: a corrupt sealed page was served"
    );
    let window = RingScan {
        from: 0,
        to: 512,
        max_bytes: 1024,
    };
    let range = engine
        .read_stream_window(&handle, window, None, Deliver::Durable)
        .await;
    // A chunk failing its checks is refused as a Data error from the store
    // layer (`seal::read::sealed_corrupt`), never served.
    let Err(RangeReadError::Store(error)) = range else {
        panic!("{label}: a corrupt sealed chunk was not refused as corrupt");
    };
    assert_eq!(error.kind(), slatedb::ErrorKind::Data, "{label}");
    assert!(
        error.to_string().contains("sealed range corrupt"),
        "{label}: {error}"
    );
    assert_eq!(
        rows,
        stored_rows(&engine).await,
        "{label}: a read changed a row"
    );
    assert_eq!(
        entries,
        directory(),
        "{label}: a read changed the directory"
    );
    close(&engine).await;
}

/// A keyed read of pages of another key serves nothing and still consumes
/// them (its progress is their last record), unsealed and sealed; an
/// unkeyed read serves them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn r08a_valid_filtered_miss_keeps_legitimate_progress() {
    let pages = [encoded_record(0, "other"), encoded_record(1, "other")];
    let (engine, _store) = scan_fixture("r08a-valid-miss", &pages, 2, 0).await;
    let handle = engine.stream_handle(STREAM).await.unwrap();
    for sealed in [false, true] {
        if sealed {
            engine.seal_once(Trigger::Requested).await.unwrap();
        }
        let page = read_frames(&engine, &handle, 0, Some("wanted"), 1024, Deliver::Durable)
            .await
            .unwrap();
        assert!(page.frames.is_empty(), "sealed {sealed}");
        assert_eq!(page.last_offset, Some(1), "sealed {sealed}");
        let range = super::record::read_frames_until(
            &engine,
            &handle,
            0,
            512,
            None,
            1024,
            Deliver::Durable,
        )
        .await
        .unwrap();
        let served: Vec<Bytes> = range
            .frames
            .iter()
            .map(|s| s.page().raw().clone())
            .collect();
        assert_eq!(served, pages, "sealed {sealed}");
        assert_eq!(range.last_offset, Some(1), "sealed {sealed}");
    }
    close(&engine).await;
}

/// KANI-017's regression: stored bytes are admitted as a page only against
/// the last offset they are held under, and only when they are exactly one
/// whole page with its tag.
#[test]
fn r08a_record_boundary_validates_namespace_extent_and_offset() {
    use crate::crypto_page::{CheckedPage, PageCorruption};
    let raw = encoded_record(1, "other");
    let admit = |raw: &[u8], last: u64| {
        CheckedPage::admit(Bytes::copy_from_slice(raw), last).map(|page| page.last())
    };
    assert_eq!(admit(&raw, 1), Ok(1));
    assert_eq!(
        admit(&encoded_record(2, "other"), 1),
        Err(PageCorruption::Offset { key: 1, page: 2 })
    );
    let mut trailing = raw.to_vec();
    trailing.push(0);
    assert_eq!(admit(&trailing, 1), Err(PageCorruption::Trailing));
    assert_eq!(
        admit(&raw[..raw.len() - 1], 1),
        Err(PageCorruption::Truncated)
    );
    // A ciphertext shorter than an AEAD tag can never authenticate, so a
    // length-consistent page carrying one is corrupt rather than short.
    let ct_len = raw.len() - 4 - (16 + b"retained payload".len() + 2);
    let mut tagless = raw[..ct_len].to_vec();
    tagless.extend_from_slice(&8u32.to_be_bytes());
    tagless.extend_from_slice(&[0; 8]);
    assert_eq!(admit(&tagless, 1), Err(PageCorruption::Tag));
}

/// The ring serves only below the sealed boundary: with the stream sealed
/// and a broken copy of its pages in the ring, the ring refuses the window
/// for every selector, and the read falls back to the segment object and
/// serves the stored page, consuming only through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn r08a_invalid_ring_copy_retries_storage_without_false_filtered_progress() {
    let wanted = encoded_record(1, "wanted");
    let pages = [encoded_record(0, "other"), wanted.clone()];
    let (engine, _store) = scan_fixture("r08a-ring", &pages, 2, 1 << 20).await;
    engine.seal_once(Trigger::Requested).await.unwrap();
    let handle = engine.stream_handle(STREAM).await.unwrap();
    handle
        .ring
        .lock()
        .unwrap()
        .batches
        .push_back(super::RingBatch {
            first: 0,
            next: 2,
            frames: vec![(1, Bytes::from_static(b"broken"))],
            bytes: 6,
        });
    let window = RingScan {
        from: 0,
        to: 2,
        max_bytes: 1024,
    };
    assert!(engine.ring_read(&handle, window, None).is_none());
    assert!(engine.ring_read(&handle, window, Some("wanted")).is_none());
    let sealed_reads = || {
        engine
            .layout
            .seal
            .counters
            .sealed_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let before = sealed_reads();
    let page = read_frames(&engine, &handle, 0, Some("wanted"), 1024, Deliver::Durable)
        .await
        .unwrap();
    assert_eq!(
        sealed_reads(),
        before + 1,
        "the read did not fall back to the object"
    );
    let served: Vec<Bytes> = page.frames.iter().map(|s| s.page().raw().clone()).collect();
    assert_eq!(served, vec![wanted]);
    assert_eq!(page.last_offset, Some(1));
    close(&engine).await;
}
