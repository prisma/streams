//! R08-A: real persisted scans, rather than an ordered-key model.
use super::{
    Deliver, ShardConfig, ShardEngine, ShardMaintenance, TailFields, encode_tail, producer_key,
    read_frames, read_frames_range, record_key, tail_key,
};
use bytes::Bytes;
use slatedb::{Db, WriteBatch};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

fn encoded_record(offset: u64, lane: &str) -> Bytes {
    Bytes::from(
        crate::crypto::FrameCipher::new(
            &[7; 32],
            &[8; 16],
            crate::crypto::FrameCompression::Disabled,
        )
        .encrypt(&[8; 16], offset, 1, 1, lane, b"retained payload"),
    )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_database_record_corruption_refuses_progress_without_mutation() {
    let mut short = record_key(&[8; 16], 1);
    short.remove(17);
    let mut long = record_key(&[8; 16], 1);
    long.push(0);
    let cases = [
        ("short", short, encoded_record(1, "wanted")),
        ("long", long, encoded_record(1, "wanted")),
        (
            "offset",
            record_key(&[8; 16], 1),
            encoded_record(2, "other"),
        ),
        (
            "frame",
            record_key(&[8; 16], 1),
            Bytes::from_static(b"invalid frame"),
        ),
    ];
    for (label, key, value) in cases {
        assert!(record_key(&[8; 16], 0) < key && key < record_key(&[8; 16], 512));
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
        assert_eq!(
            absorber.unwrap().err().expect("corrupt row refused").kind(),
            slatedb::ErrorKind::Data
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_valid_filtered_miss_keeps_legitimate_progress() {
    let engine = scan_fixture(
        "r08a-valid-miss",
        record_key(&[8; 16], 1),
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

#[test]
fn r08a_record_boundary_validates_namespace_extent_and_offset() {
    use super::record::{RecordCorruption, decode_row};
    let key = record_key(&[8; 16], 1);
    let raw = encoded_record(1, "other");
    assert!(decode_row(&key, &key[..17], &raw).is_ok());
    for at in [0, 16] {
        let mut foreign = key.clone();
        foreign[at] ^= 1;
        assert!(matches!(
            decode_row(&foreign, &key[..17], &raw),
            Err(RecordCorruption::Namespace)
        ));
    }
    for width in [0, 17, 24, 26] {
        let mut invalid = key.clone();
        invalid.resize(width, 0);
        assert!(matches!(
            decode_row(&invalid, &key[..17], &raw),
            Err(RecordCorruption::KeyWidth)
        ));
    }
    assert!(matches!(
        decode_row(&key, &key[..17], &encoded_record(2, "other")),
        Err(RecordCorruption::Offset { .. })
    ));
    let mut trailing = raw.to_vec();
    trailing.push(0);
    assert!(matches!(
        decode_row(&key, &key[..17], &trailing),
        Err(RecordCorruption::Frame)
    ));
    assert!(matches!(
        decode_row(&key, &key[..17], &raw[..raw.len() - 1]),
        Err(RecordCorruption::Frame)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r08a_invalid_ring_copy_retries_storage_without_false_filtered_progress() {
    let engine = scan_fixture(
        "r08a-ring",
        record_key(&[8; 16], 1),
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
    assert_eq!(page.frames, vec![encoded_from_store(&engine).await]);
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
        .get(record_key(&[8; 16], 1))
        .await
        .unwrap()
        .unwrap()
}
