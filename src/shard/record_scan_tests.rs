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
        ts_ms: 1,
        key_version: 1,
        routing_key: lane,
    };
    let page = PageCipher::new(&[7; 32], &[8; 16])
        .seal(&lane, offset, &[b"retained payload"])
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
