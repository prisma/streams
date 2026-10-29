//! R06-A exercises encoded storage, decryption and consumed progress together.
//! A stream holds one page per fixture record: unsealed in its pending row
//! and, below its sealed boundary, in a segment object.
use crate::application::read::read_merged;
use crate::crypto::{FrameCompression, StreamKey, derive_subkey};
use crate::crypto_page::{PageCipher, PageLane, SealError};
use crate::shard::seal::Trigger;
use crate::shard::{
    Deliver, RingBatch, ShardConfig, ShardEngine, TailFields, encode_tail, tail_key,
};
use crate::spill::pending::{concat_pages, pending_key};
use bytes::Bytes;
use object_store::ObjectStore;

/// The shard-log page of one fixture record of `size` bytes of `x` at
/// `offset` in `lane`.
fn record_page(key: &StreamKey, offset: u64, lane: &str, size: usize) -> Bytes {
    let cipher = PageCipher::new(&derive_subkey(key, &[9; 16], lane, 1), &[8; 16]);
    let lane = PageLane {
        key_version: 1,
        routing_key: lane,
    };
    let page = cipher
        .seal(
            &lane,
            offset,
            &crate::crypto_page::stamped(0, &[vec![b'x'; size]]),
        )
        .unwrap();
    assert_eq!(page.last, offset);
    Bytes::from(page.bytes)
}

/// The fixture records from `history` on as the shard log stores them: one
/// page per record, in a batch the ring can publish.
fn tail_pages(key: &StreamKey, records: &[(String, usize)], history: usize) -> RingBatch {
    let mut batch = RingBatch::default();
    for (offset, (lane, size)) in (0u64..).zip(records).skip(history) {
        batch.push_page(offset, offset, record_page(key, offset, lane, *size));
    }
    batch
}
use slatedb::WriteBatch;
use std::{sync::Arc, time::Duration};

/// An engine at `label` over `store` whose stream [8; 16] holds `pages`
/// unsealed from `absorbed` to `next` (its tail and pending row are put
/// before the engine opens), with a ring of `ring_bytes`. Its sealer is
/// open and seals only when a test asks.
async fn planted_engine(
    store: &Arc<dyn ObjectStore>,
    label: &str,
    pages: &[Bytes],
    (absorbed, next): (u64, u64),
    ring_bytes: usize,
) -> Arc<ShardEngine> {
    let db = Arc::new(
        crate::keyspace::db_builder(label, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let pending = concat_pages(pages);
    let mut batch = WriteBatch::new();
    batch.put(
        tail_key(&[8; 16]),
        encode_tail(&TailFields {
            next,
            absorbed,
            route: [4; 16],
            unabsorbed_bytes: pending.len() as u64,
            ..Default::default()
        }),
    );
    if !pending.is_empty() {
        batch.put(pending_key(&[8; 16]), pending);
    }
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
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
    engine
}

async fn compressed_fixture() -> (Arc<ShardEngine>, StreamKey) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = StreamKey([7; 32]);
    let pages: Vec<Bytes> = (0..1600)
        .map(|offset| {
            let page = record_page(&key, offset, "", 16 * 1024);
            assert!(
                page.len() < 100,
                "fixture must reproduce the compressed size gap"
            );
            page
        })
        .collect();
    let engine = planted_engine(&store, "r06a-compressed", &pages, (0, 1600), 0).await;
    (engine, key)
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

/// `_compression` names the matrix leg only: pages compress whenever that
/// pays, unsealed and sealed alike. The first `history` records are sealed
/// into a segment object (sealed by an earlier engine, the rest planted
/// unsealed before this one opens); with `ring`, the ring holds every page,
/// and serves the sealed ones.
async fn mixed_fixture(
    _compression: FrameCompression,
    ring: bool,
    history: usize,
    records: &[(String, usize)],
) -> (Arc<ShardEngine>, StreamKey) {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = StreamKey([7; 32]);
    let all = tail_pages(&key, records, 0);
    let pages: Vec<Bytes> = all.frames.iter().map(|(_, page)| page.clone()).collect();
    let (sealed, next) = (history as u64, records.len() as u64);
    if history > 0 {
        let first = planted_engine(&store, "r06a-mixed", &pages[..history], (0, sealed), 0).await;
        let report = first.seal_once(Trigger::Requested).await.unwrap();
        assert_eq!(report.pages, sealed);
        first.begin_close();
        first
            .await_terminated(Duration::from_secs(5))
            .await
            .unwrap();
    }
    let ring_bytes = if ring { 4 << 20 } else { 0 };
    let engine = planted_engine(
        &store,
        "r06a-mixed",
        &pages[history..],
        (sealed, next),
        ring_bytes,
    )
    .await;
    if ring {
        let handle = engine.stream_handle([8; 16]).await.unwrap();
        engine.ring_publish(&handle, &all);
    }
    (engine, key)
}

#[expect(
    clippy::excessive_nesting,
    reason = "r06a_budget_matrix_preserves_filtered_history_ring_and_visibility_progress; the fixture nests the budget walk inside the compression, ring and history matrix; flattening it would separate the walk from the shape it runs under"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "r06a_budget_matrix_preserves_filtered_history_ring_and_visibility_progress; the fixture's offsets index its own small record table; a checked conversion would only restate the fixture"
)]
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
    // A record past the 32 MiB record cap is never stored: its page refuses
    // to seal, and a stored page claiming one fails admission before any
    // decryption (its ciphertext exceeds the single-record body cap).
    let lane = PageLane {
        key_version: 1,
        routing_key: "",
    };
    let oversized = vec![0u8; crate::crypto::MAX_RECORD_PLAINTEXT + 1];
    assert_eq!(
        PageCipher::new(&[1; 32], &[8; 16])
            .seal(&lane, 0, &crate::crypto_page::stamped(0, &[oversized]))
            .unwrap_err(),
        SealError::RecordTooLarge
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r06a_zero_payload_and_filtered_miss_pages_bound_record_and_scan_work() {
    let records = vec![("wanted".into(), 0); 5000];
    // Unsealed, sealed and served from the ring, sealed and served from the
    // segment object.
    for (ring, history) in [(false, 0), (true, 5000), (false, 5000)] {
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
