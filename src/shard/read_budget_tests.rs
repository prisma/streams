//! R06-A exercises encoded storage, decryption and consumed progress together.
use crate::application::read::read_merged;
use crate::crypto::{FrameCipher, FrameCompression, StreamKey, derive_subkey};
use crate::shard::{
    Deliver, ShardConfig, ShardEngine, ShardMaintenance, TailFields, encode_tail, record_key,
    tail_key,
};
use bytes::Bytes;
use slatedb::{Db, WriteBatch};
use std::{sync::Arc, time::Duration};

async fn compressed_fixture() -> (Arc<ShardEngine>, StreamKey) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r06a-compressed", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let key = StreamKey([7; 32]);
    let cipher = FrameCipher::new(
        &derive_subkey(&key, &[9; 16], "", 1),
        &[8; 16],
        FrameCompression::ZstdLevel1,
    );
    let payload = vec![b'x'; 16 * 1024];
    let mut batch = WriteBatch::new();
    for offset in 0..1600 {
        let frame = cipher.encrypt(&[8; 16], offset, 0, 1, "", &payload);
        assert!(
            frame.len() < 100,
            "fixture must reproduce the compressed size gap"
        );
        batch.put(record_key(&[8; 16], offset), Bytes::from(frame));
    }
    batch.put(
        tail_key(&[8; 16]),
        encode_tail(&TailFields {
            next: 1600,
            ..Default::default()
        }),
    );
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    (
        ShardEngine::start(
            "r06a-compressed".into(),
            db,
            store,
            ShardConfig {
                tail_ring_bytes: 0,
                ..Default::default()
            },
            tx,
            None,
            ShardMaintenance::default(),
        ),
        key,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06a_compressed_database_pages_bound_plaintext_without_skipping() {
    let (engine, key) = compressed_fixture().await;
    let handle = engine.stream_handle([8; 16]).await.unwrap();
    let mut cursor = 0;
    let mut seen = Vec::new();
    let mut violation = None;
    while cursor < 1600 {
        let page = read_merged(
            &key,
            &[9; 16],
            &handle,
            &engine,
            cursor,
            None,
            64 * 1024,
            Deliver::Durable,
        )
        .await
        .unwrap();
        let size: usize = page.recs.iter().map(|r| r.payload.len()).sum();
        if size > 64 * 1024 {
            violation = Some(size);
            break;
        }
        assert_eq!(page.recs.len(), 4);
        for record in &page.recs {
            assert_eq!(record.payload.as_ref(), vec![b'x'; 16 * 1024]);
            seen.push(record.off);
        }
        let next = page.scanned_through(cursor);
        assert_eq!(next, cursor + 4, "withheld records are not consumed");
        assert_eq!(page.completed, next == 1600);
        cursor = next;
    }
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        violation, None,
        "encoded-byte selection exceeded the plaintext page bound"
    );
    assert_eq!(seen, (0..1600).collect::<Vec<_>>());
}

async fn mixed_fixture(
    compression: FrameCompression,
    ring: bool,
    history: usize,
    records: &[(String, usize)],
) -> (Arc<ShardEngine>, StreamKey) {
    use crate::crypto::{RouteHash, SegmentHash};
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r06a-mixed", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let mut batch = WriteBatch::new();
    let key = StreamKey([7; 32]);
    let mut frames = Vec::new();
    for (offset, (lane, size)) in records.iter().enumerate() {
        let frame = FrameCipher::new(
            &derive_subkey(&key, &[9; 16], lane, 1),
            &[8; 16],
            compression,
        )
        .encrypt(&[8; 16], offset as u64, 0, 1, lane, &vec![b'x'; *size]);
        let frame = Bytes::from(frame);
        if offset >= history {
            batch.put(record_key(&[8; 16], offset as u64), frame.clone());
        }
        frames.push((offset as u64, frame));
    }
    batch.put(
        tail_key(&[8; 16]),
        encode_tail(&TailFields {
            next: records.len() as u64,
            absorbed: history as u64,
            history_v2: history > 0,
            route: [4; 16],
            ..Default::default()
        }),
    );
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "r06a-mixed".into(),
        db,
        store,
        ShardConfig {
            tail_ring_bytes: if ring { 4 << 20 } else { 0 },
            ..Default::default()
        },
        tx,
        None,
        ShardMaintenance::default(),
    );
    if history > 0 {
        let part = engine.history_partition().await.unwrap();
        let mut batch = WriteBatch::new();
        let mut builder = crate::postings::PageBuilder::default();
        for (offset, raw) in &frames[..history] {
            batch.put(
                crate::history::hist2_record_key(RouteHash([4; 16]), SegmentHash([8; 16]), *offset),
                raw.clone(),
            );
            builder.note_frame(
                crate::postings::rk_hash(&records[*offset as usize].0),
                *offset,
                raw.len() as u64,
            );
        }
        for (rk, bucket, first, value) in builder.finish().0 {
            batch.put(
                crate::postings::postings_key(
                    RouteHash([4; 16]),
                    SegmentHash([8; 16]),
                    &rk,
                    bucket,
                    first,
                ),
                value,
            );
        }
        let write = part.write(batch).await.unwrap();
        part.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
        write.await_durable().await.unwrap();
    }
    if ring && history < frames.len() {
        let handle = engine.stream_handle([8; 16]).await.unwrap();
        engine.ring_publish(&handle, &frames[history..]);
    }
    (engine, key)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06a_budget_matrix_preserves_filtered_history_ring_and_visibility_progress() {
    let records = vec![
        ("wanted".into(), 6),
        ("other".into(), 5),
        ("wanted".into(), 5),
        ("wanted".into(), 12),
        ("other".into(), 4),
        ("wanted".into(), 4),
    ];
    let records: Vec<_> = records
        .into_iter()
        .map(|(lane, size)| (lane, size * 4096))
        .collect();
    for compression in [FrameCompression::Disabled, FrameCompression::ZstdLevel1] {
        for ring in [false, true] {
            for history in [0, 3, 6] {
                let (engine, key) = mixed_fixture(compression, ring, history, &records).await;
                let handle = engine.stream_handle([8; 16]).await.unwrap();
                let mut cursor = 0;
                let mut seen = Vec::new();
                for _ in 0..16 {
                    let page = read_merged(
                        &key,
                        &[9; 16],
                        &handle,
                        &engine,
                        cursor,
                        Some("wanted"),
                        10 * 4096,
                        Deliver::Durable,
                    )
                    .await
                    .unwrap();
                    let size: usize = page.recs.iter().map(|r| r.payload.len()).sum();
                    assert!(size <= 10 * 4096 || (page.recs.len() == 1 && size == 12 * 4096));
                    let next = page.scanned_through(cursor);
                    let expected: Vec<_> = (cursor..next)
                        .filter(|i| records[*i as usize].0 == "wanted")
                        .collect();
                    let actual: Vec<_> = page.recs.iter().map(|r| r.off).collect();
                    assert_eq!(actual, expected, "never consume a withheld match");
                    seen.extend(actual);
                    assert!(next > cursor || page.completed);
                    cursor = next;
                    if page.completed {
                        break;
                    }
                }
                assert_eq!(seen, vec![0, 2, 3, 5]);
                assert_eq!(cursor, 6);
                // Applied visibility may expose more records, but its durable
                // reconnect position is still clamped after budget truncation.
                let durable = (history as u64).max(4);
                handle.state.lock().unwrap().durable.next = durable;
                handle.state.lock().unwrap().applied.next = 6;
                let page = read_merged(
                    &key,
                    &[9; 16],
                    &handle,
                    &engine,
                    3,
                    None,
                    10 * 4096,
                    Deliver::Applied,
                )
                .await
                .unwrap();
                assert_eq!(page.recs[0].off, 3);
                assert_eq!(page.recs.len(), 1);
                assert_eq!(page.durable_resume(3), 4);
                engine.begin_close();
                engine
                    .await_terminated(Duration::from_secs(5))
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06a_metadata_and_individual_decompression_are_bounded() {
    let lane = "\u{1}".repeat(u16::MAX as usize);
    let records = vec![(lane.clone(), 1); 3];
    let (engine, key) = mixed_fixture(FrameCompression::ZstdLevel1, false, 0, &records).await;
    let handle = engine.stream_handle([8; 16]).await.unwrap();
    let page = read_merged(
        &key,
        &[9; 16],
        &handle,
        &engine,
        0,
        None,
        8 << 20,
        Deliver::Durable,
    )
    .await
    .unwrap();
    assert_eq!(page.recs.len(), 2);
    assert_eq!(page.scanned_through(0), 2);
    assert!(!page.completed);
    let next = read_merged(
        &key,
        &[9; 16],
        &handle,
        &engine,
        2,
        None,
        8 << 20,
        Deliver::Durable,
    )
    .await
    .unwrap();
    assert_eq!(next.recs.len(), 1);
    assert!(next.completed);
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
    let (engine, key) = mixed_fixture(
        FrameCompression::ZstdLevel1,
        false,
        0,
        &[(String::new(), crate::crypto::MAX_RECORD_PLAINTEXT + 1)],
    )
    .await;
    let handle = engine.stream_handle([8; 16]).await.unwrap();
    let result = read_merged(
        &key,
        &[9; 16],
        &handle,
        &engine,
        0,
        None,
        1,
        Deliver::Durable,
    )
    .await;
    assert!(result.err().unwrap().contains("exceeds 32 MiB"));
    engine.begin_close();
    engine
        .await_terminated(Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06a_zero_payload_and_filtered_miss_pages_bound_record_and_scan_work() {
    let records = vec![("wanted".into(), 0); 5000];
    for (ring, history) in [(false, 0), (true, 0), (false, 5000)] {
        let (engine, key) =
            mixed_fixture(FrameCompression::ZstdLevel1, ring, history, &records).await;
        let handle = engine.stream_handle([8; 16]).await.unwrap();
        for selector in [None, Some("absent")] {
            let first = read_merged(
                &key,
                &[9; 16],
                &handle,
                &engine,
                0,
                selector,
                1 << 20,
                Deliver::Durable,
            )
            .await
            .unwrap();
            assert_eq!(first.scanned_through(0), 4096);
            assert_eq!(first.recs.len(), if selector.is_none() { 4096 } else { 0 });
            assert!(!first.completed);
            let second = read_merged(
                &key,
                &[9; 16],
                &handle,
                &engine,
                4096,
                selector,
                1 << 20,
                Deliver::Durable,
            )
            .await
            .unwrap();
            assert_eq!(second.scanned_through(4096), 5000);
            assert_eq!(second.recs.len(), if selector.is_none() { 904 } else { 0 });
            assert!(second.completed);
        }
        engine.begin_close();
        engine
            .await_terminated(Duration::from_secs(5))
            .await
            .unwrap();
    }
}
