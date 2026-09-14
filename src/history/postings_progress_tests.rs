//! Dense valid postings must not trap a keyed cursor behind the index byte cap.
#![cfg(test)]
use super::{hist2_record_key, read_history2_keyed_cached, read_history2_keyed_envelope};
use crate::crypto::{FrameCipher, FrameCompression, RouteHash, SegmentHash};
use crate::postings::{PostingRun, encode_page, postings_key, rk_hash};
use crate::postings_cache::{CacheRuns, LOAD_MAX_ENCODED_BYTES, PostingsCache};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const ROUTE: RouteHash = RouteHash([0xe1; 16]);
const INC: SegmentHash = SegmentHash([0xe2; 16]);
const FROM: u64 = 40_000;
const UPTO: u64 = FROM + 5;

async fn history_db(prefix: &str) -> Arc<Db> {
    Arc::new(
        Db::builder(prefix, Arc::new(object_store::memory::InMemory::new()))
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(5)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    )
}

async fn dense_bucket() -> Arc<Db> {
    let db = history_db("postings-dense-bucket-progress").await;
    let cipher = FrameCipher::new(&[7; 32], &INC.0, FrameCompression::Disabled);
    let mut batch = WriteBatch::new();
    let mut encoded = 0;
    for offset in 0..UPTO {
        let key = if offset >= FROM && offset % 2 == 1 {
            "other"
        } else {
            "wanted"
        };
        let frame = cipher.encrypt(&INC.0, offset, 1, 0, key, &offset.to_le_bytes());
        let page = encode_page(
            offset,
            &[PostingRun {
                gap_offsets: 0,
                record_count: 1,
                matching_frame_bytes: u64::try_from(frame.len()).unwrap(),
                gap_frame_bytes_before: 0,
            }],
        );
        if key == "wanted" {
            encoded += u64::try_from(page.len()).unwrap();
        }
        batch.put(hist2_record_key(ROUTE, INC, offset), frame);
        batch.put(postings_key(ROUTE, INC, &rk_hash(key), 0, offset), page);
    }
    assert_eq!(crate::postings::bucket_of(UPTO - 1), 0);
    assert!(
        encoded > LOAD_MAX_ENCODED_BYTES,
        "actual same-bucket byte cap"
    );
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db
}

async fn check_page_walk(cache: &Arc<PostingsCache>, db: &Arc<Db>) {
    let mut from = FROM;
    let mut found = Vec::new();
    let mut completed = false;
    for _ in 0..=UPTO - FROM {
        let (frames, last, complete) =
            read_history2_keyed_cached(cache, db, ROUTE, INC, "wanted", from, UPTO, UPTO, 1)
                .await
                .unwrap();
        assert!(
            frames.len() <= 1,
            "one canonical row consumes the page budget"
        );
        for frame in frames {
            assert_eq!(frame.view().header.routing_key, "wanted");
            found.push(frame.view().header.offset);
        }
        if complete {
            completed = true;
            break;
        }
        let next = last.unwrap() + 1;
        assert!(
            next > from,
            "dense bucket must advance: from={from}, next={next}"
        );
        assert!(next <= UPTO);
        from = next;
    }
    assert!(completed, "bounded repeated reads must reach completion");
    assert_eq!(from, UPTO);
    assert_eq!(found, [FROM, FROM + 2, FROM + 4]);
}

#[tokio::test]
async fn dense_bucket_cached_reads_progress_through_bounded_canonical_pages() {
    let db = dense_bucket().await;
    let cache = PostingsCache::new(1 << 20);
    let capped = cache
        .runs_for(&db, ROUTE, INC, rk_hash("wanted"), FROM, UPTO, UPTO)
        .await
        .unwrap();
    match capped {
        CacheRuns::Runs { provable_to, .. } => assert!(provable_to <= FROM),
        CacheRuns::Corrupt => panic!("the dense index consists of valid, nonoverlapping pages"),
    }
    assert!(cache.index_bytes_read.load(Ordering::Relaxed) >= LOAD_MAX_ENCODED_BYTES);
    for _ in 0..2 {
        check_page_walk(&cache, &db).await;
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn sparse_proven_index_skips_other_keys_within_one_record_budget() {
    let db = history_db("postings-sparse-budget-control").await;
    let cipher = FrameCipher::new(&[7; 32], &INC.0, FrameCompression::Disabled);
    let mut batch = WriteBatch::new();
    for (offset, key) in [(0_u64, "other"), (1, "other"), (2, "wanted")] {
        let frame = cipher.encrypt(&INC.0, offset, 1, 0, key, b"canonical record");
        let page = encode_page(
            offset,
            &[PostingRun {
                gap_offsets: 0,
                record_count: 1,
                matching_frame_bytes: u64::try_from(frame.len()).unwrap(),
                gap_frame_bytes_before: 0,
            }],
        );
        batch.put(hist2_record_key(ROUTE, INC, offset), frame);
        batch.put(postings_key(ROUTE, INC, &rk_hash(key), 0, offset), page);
    }
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let (frames, last, complete) = read_history2_keyed_envelope(&db, ROUTE, INC, "wanted", 0, 3, 1)
        .await
        .unwrap();
    assert!(
        frames.is_empty(),
        "ordinary scanning spends its budget on another key"
    );
    assert_eq!(last, Some(0));
    assert!(!complete);
    let cache = PostingsCache::new(1 << 20);
    for _ in 0..2 {
        let (frames, last, complete) =
            read_history2_keyed_cached(&cache, &db, ROUTE, INC, "wanted", 0, 3, 3, 1)
                .await
                .unwrap();
        assert_eq!(
            frames.len(),
            1,
            "proven absence must skip the unrelated prefix"
        );
        assert_eq!(frames[0].view().header.offset, 2);
        assert_eq!(frames[0].view().header.routing_key, "wanted");
        assert_eq!(last, Some(2));
        assert!(
            !complete,
            "the one required record consumes the byte budget"
        );
    }
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    db.close().await.unwrap();
}
