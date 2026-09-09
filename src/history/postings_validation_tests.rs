//! Stored derived-index corruption must never skip canonical matches.
use super::*;
use crate::crypto::{FrameCipher, FrameCompression};
use crate::postings::{PostingRun, encode_page, postings_key, rk_hash};

fn page(first: u64, count: u32, bytes: u64) -> Vec<u8> {
    encode_page(
        first,
        &[PostingRun {
            gap_offsets: 0,
            record_count: count,
            matching_frame_bytes: bytes,
            gap_frame_bytes_before: 0,
        }],
    )
}

async fn assert_canonical(
    label: &str,
    entries: Vec<(u64, Vec<u8>)>,
    from: u64,
    upto: u64,
    offset: u64,
) {
    let route = RouteHash([41; 16]);
    let inc = SegmentHash([42; 16]);
    let kh = rk_hash("wanted");
    let db = Arc::new(
        Db::builder(
            format!("postings-admission-{label}"),
            Arc::new(object_store::memory::InMemory::new()),
        )
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap(),
    );
    let frame = FrameCipher::new(&[7; 32], &inc.0, FrameCompression::Disabled).encrypt(
        &inc.0,
        offset,
        1,
        0,
        "wanted",
        b"canonical match",
    );
    db.put(hist2_record_key(route, inc, offset), frame.clone())
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    for (first, value) in entries {
        db.put(
            postings_key(route, inc, &kh, crate::postings::bucket_of(first), first),
            value,
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    }
    let cache = crate::postings_cache::PostingsCache::new(1 << 20);
    for cached in [false, true, true] {
        let (frames, last, complete) = if cached {
            read_history2_keyed_cached(
                &cache,
                &db,
                route,
                inc,
                "wanted",
                from,
                upto,
                upto,
                32 * 1024,
            )
            .await
        } else {
            read_history2_keyed(&db, route, inc, "wanted", from, upto, 32 * 1024).await
        }
        .unwrap();
        assert_eq!(
            frames.len(),
            1,
            "{label}, cached={cached}: canonical match was skipped"
        );
        assert_eq!(frames[0].as_ref(), frame.as_slice());
        assert_eq!(last, Some(upto - 1), "exact consumed frontier");
        assert!(complete);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn o4a_stored_overlapping_pages_cannot_skip_a_canonical_match() {
    assert_canonical(
        "overlap",
        vec![
            (0, page(0, 100, 100)),
            (10, page(10, 1, 1)),
            (20, page(20, 1, 1)),
        ],
        50,
        60,
        55,
    )
    .await;
}

#[tokio::test]
async fn o4a_stored_zero_count_cannot_panic_or_publish_empty_progress() {
    assert_canonical("zero", vec![(1, page(1, 0, (16 << 20) + 1))], 0, 2, 1).await;
}

#[tokio::test]
async fn o4a_stored_key_header_disagreement_uses_canonical_fallback() {
    assert_canonical("key-header", vec![(1, page(2, 1, 1))], 0, 3, 1).await;
}

#[tokio::test]
async fn o4a_valid_page_seams_and_match_free_progress_remain_usable() {
    assert_canonical(
        "valid-seams",
        vec![
            (0, page(0, 1, 1)),
            (10, page(10, 1, 1)),
            (55, page(55, 1, 1)),
        ],
        50,
        60,
        55,
    )
    .await;
    let db = Arc::new(
        Db::builder(
            "postings-empty",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let (frames, last, complete) = read_history2_keyed(
        &db,
        RouteHash([1; 16]),
        SegmentHash([2; 16]),
        "absent",
        50,
        60,
        1024,
    )
    .await
    .unwrap();
    assert!(frames.is_empty());
    assert_eq!(last, Some(59));
    assert!(complete);
    db.close().await.unwrap();
}

fn raw_page(first: u64, end: u64, total: u64, runs: &[[u64; 4]]) -> Vec<u8> {
    let mut value = vec![1, 0];
    value.extend_from_slice(&first.to_le_bytes());
    value.extend_from_slice(&end.to_le_bytes());
    value.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    value.extend_from_slice(&total.to_le_bytes());
    for run in runs {
        for &field in run {
            let mut field = field;
            loop {
                let byte = (field & 127) as u8;
                field >>= 7;
                value.push(byte | if field == 0 { 0 } else { 128 });
                if field == 0 {
                    break;
                }
            }
        }
    }
    value
}

#[tokio::test]
async fn o4a_stored_overflow_width_and_extent_corruption_fall_back() {
    for (label, value) in [
        (
            "count-width",
            raw_page(1, 2, 1, &[[0, (1u64 << 32) + 1, 1, 0]]),
        ),
        ("offset-overflow", raw_page(1, 1, 1, &[[u64::MAX, 1, 1, 0]])),
        (
            "byte-overflow",
            raw_page(1, 3, 0, &[[0, 1, u64::MAX, 0], [0, 1, 1, 0]]),
        ),
        ("nonzero-first-gap", raw_page(1, 3, 1, &[[1, 1, 1, 0]])),
        ("trailing", {
            let mut p = page(1, 1, 1);
            p.push(0);
            p
        }),
        ("varint-overflow", {
            let mut p = raw_page(1, 2, 1, &[]);
            p[18..22].copy_from_slice(&1u32.to_le_bytes());
            p.extend_from_slice(&[128, 128, 128, 128, 128, 128, 128, 128, 128, 2, 1, 1, 0]);
            p
        }),
    ] {
        assert!(
            crate::postings::decode_page_abs(1, &value).is_none(),
            "{label}"
        );
        assert_canonical(label, vec![(1, value)], 0, 3, 1).await;
    }
}

#[tokio::test]
async fn o4a_stored_bucket_crossing_range_uses_canonical_fallback() {
    assert_canonical(
        "bucket-crossing",
        vec![(65_535, page(65_535, 2, 2))],
        65_535,
        65_537,
        65_536,
    )
    .await;
}

#[tokio::test]
async fn o4a_stored_key_width_rejection_is_not_empty_progress() {
    let route = RouteHash([41; 16]);
    let inc = SegmentHash([42; 16]);
    let kh = rk_hash("wanted");
    let db = Arc::new(
        Db::builder(
            "postings-key-width",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap(),
    );
    let frame = FrameCipher::new(&[7; 32], &inc.0, FrameCompression::Disabled)
        .encrypt(&inc.0, 256, 1, 0, "wanted", b"present");
    db.put(hist2_record_key(route, inc, 256), frame.clone())
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let mut key = postings_key(route, inc, &kh, 0, 256);
    key.pop();
    db.put(key, page(256, 1, 1))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let cache = crate::postings_cache::PostingsCache::new(1 << 20);
    assert!(matches!(
        cache
            .runs_for(&db, route, inc, kh, 0, 512, 512)
            .await
            .unwrap(),
        crate::postings_cache::CacheRuns::Corrupt
    ));
    let (frames, last, complete) =
        read_history2_keyed_cached(&cache, &db, route, inc, "wanted", 0, 512, 512, 4096)
            .await
            .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].as_ref(), frame.as_slice());
    assert_eq!(last, Some(511));
    assert!(complete);
    db.close().await.unwrap();
}
