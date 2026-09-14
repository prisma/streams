//! A bucket scan proves only its durable target, including across warm installs.
#![cfg(test)]
use super::{CacheRuns, LOAD_MAX_ENCODED_BYTES, PostingsCache, load_runs};
use crate::crypto::{RouteHash, SegmentHash};
use crate::postings::{AbsRun, BUCKET_OFFSETS};
use slatedb::Db;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

const ROUTE: RouteHash = RouteHash([0xd1; 16]);
const INC: SegmentHash = SegmentHash([0xd2; 16]);

fn run(start: u64, count: u32) -> AbsRun {
    AbsRun {
        start,
        count,
        matching_bytes: u64::from(count) * 100,
        gap_bytes_before: 0,
    }
}

async fn stored_prefix(count: u32) -> Arc<Db> {
    let part = Arc::new(
        Db::builder(
            "cache-durable-frontier",
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
    let kh = crate::postings::rk_hash("wanted");
    part.put(
        crate::postings::postings_key(ROUTE, INC, &kh, 0, 0),
        crate::postings::encode_page(
            0,
            &[crate::postings::PostingRun {
                gap_offsets: 0,
                record_count: count,
                matching_frame_bytes: u64::from(count) * 100,
                gap_frame_bytes_before: 0,
            }],
        ),
    )
    .await
    .unwrap()
    .await_durable()
    .await
    .unwrap();
    part
}

async fn offsets(cache: &Arc<PostingsCache>, part: &Arc<Db>, upto: u64) -> Vec<u64> {
    let kh = crate::postings::rk_hash("wanted");
    let CacheRuns::Runs { runs, provable_to } = cache
        .runs_for(part, ROUTE, INC, kh, 0, upto, upto)
        .await
        .unwrap()
    else {
        panic!("valid postings must be admitted");
    };
    assert_eq!(provable_to, upto);
    runs.iter()
        .flat_map(|run| run.start..run.start + u64::from(run.count))
        .collect()
}

#[tokio::test]
async fn cold_read_then_absorb_in_same_bucket_preserves_new_record() {
    let part = stored_prefix(4).await;
    let cache = PostingsCache::new(1 << 20);
    let kh = crate::postings::rk_hash("wanted");
    assert_eq!(offsets(&cache, &part, 4).await, [0, 1, 2, 3]);
    cache.install_chunk(INC, 4, 5, vec![(kh.0, vec![run(4, 1)])]);
    assert_eq!(offsets(&cache, &part, 5).await, [0, 1, 2, 3, 4]);
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    part.close().await.unwrap();
}

#[tokio::test]
async fn flushed_future_runs_are_clipped_before_retained_slice_extension() {
    // History may flush rows beyond the shard's currently durable boundary.
    let part = stored_prefix(8).await;
    let cache = PostingsCache::new(1 << 20);
    let kh = crate::postings::rk_hash("wanted");
    assert_eq!(offsets(&cache, &part, 4).await, [0, 1, 2, 3]);
    assert_eq!(cache.debug_slice(&INC, &kh), Some((0, 4, 1)));
    assert_eq!(
        cache.runs_for_test(INC, kh),
        vec![AbsRun {
            matching_bytes: 800,
            gap_bytes_before: crate::postings::GAP_UNKNOWN,
            ..run(0, 4)
        }]
    );
    cache.install_chunk(INC, 4, 8, vec![(kh.0, vec![run(4, 4)])]);
    assert_eq!(offsets(&cache, &part, 8).await, (0..8).collect::<Vec<_>>());
    assert_eq!(cache.warm_extends.load(Ordering::Relaxed), 1);
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    part.close().await.unwrap();
}

#[tokio::test]
async fn superseded_cold_or_extension_load_cannot_replace_warm_install() {
    let part = stored_prefix(5).await;
    let kh = crate::postings::rk_hash("wanted");
    for resident in [false, true] {
        let cache = PostingsCache::new(1 << 20);
        if resident {
            cache.install_chunk(INC, 0, 2, vec![(kh.0, vec![run(0, 2)])]);
        }
        let existing = cache
            .inner
            .lock()
            .unwrap()
            .slices
            .get(&(INC.0, kh.0))
            .map(|entry| entry.slice.clone());
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        cache
            .inner
            .lock()
            .unwrap()
            .inflight
            .insert((INC.0, kh.0), rx.clone());
        cache.spawn_load(part.clone(), ROUTE, INC, kh, existing, 0, 4, tx, false);
        // This current-thread runtime cannot poll the spawned loader until
        // the await below. A newer complete install supersedes its snapshot.
        cache.install_chunk(INC, 0, 5, vec![(kh.0, vec![run(0, 5)])]);
        rx.changed().await.unwrap();
        assert!(
            !cache
                .inner
                .lock()
                .unwrap()
                .inflight
                .contains_key(&(INC.0, kh.0))
        );
        assert_eq!(offsets(&cache, &part, 5).await, (0..5).collect::<Vec<_>>());
        assert_eq!(
            cache.debug_slice(&INC, &kh).map(|(_, upto, _)| upto),
            Some(5)
        );
        assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    }
    part.close().await.unwrap();
}

#[tokio::test]
async fn absent_key_install_does_not_discard_unmodified_load() {
    let part = stored_prefix(4).await;
    let cache = PostingsCache::new(1 << 20);
    let kh = crate::postings::rk_hash("wanted");
    let (tx, mut rx) = tokio::sync::watch::channel(false);
    cache
        .inner
        .lock()
        .unwrap()
        .inflight
        .insert((INC.0, kh.0), rx.clone());
    cache.spawn_load(part.clone(), ROUTE, INC, kh, None, 0, 4, tx, false);
    cache.install_chunk(INC, 4, 5, vec![]);
    rx.changed().await.unwrap();
    assert_eq!(cache.debug_slice(&INC, &kh), Some((0, 4, 1)));
    assert_eq!(offsets(&cache, &part, 5).await, [0, 1, 2, 3]);
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    part.close().await.unwrap();
}

#[tokio::test]
async fn skipped_write_admission_cannot_prove_absent_matches() {
    let part = stored_prefix(6).await;
    let kh = crate::postings::rk_hash("wanted");
    for extend in [false, true] {
        let cache = PostingsCache::new(1 << 20);
        // Real weight crosses the write-admission line without eviction.
        cache.install_chunk(
            SegmentHash([0xe1; 16]),
            0,
            17_000,
            vec![([0xe2; 16], (0..17_000).map(|start| run(start, 1)).collect())],
        );
        cache.install_chunk(INC, 4, 5, vec![(kh.0, vec![run(4, 1)])]);
        {
            let inner = cache.inner.lock().unwrap();
            assert!(inner.total_bytes >= cache.max_bytes / 2);
            assert!(!inner.slices.contains_key(&(INC.0, kh.0)));
            assert!(!inner.warm.get(&INC.0).unwrap().admitted_all);
        }
        // The caller's shard frontier can lag the already-flushed history.
        assert_eq!(offsets(&cache, &part, 4).await, [0, 1, 2, 3]);
        let upto = if extend {
            cache.install_chunk(INC, 5, 6, vec![(kh.0, vec![run(5, 1)])]);
            6
        } else {
            5
        };
        assert_eq!(
            offsets(&cache, &part, upto).await,
            (0..upto).collect::<Vec<_>>()
        );
        assert_eq!(cache.index_loads.load(Ordering::Relaxed), 2);
    }
    part.close().await.unwrap();
}

async fn dense_bucket_part() -> Arc<Db> {
    let part = stored_prefix(1).await;
    let kh = crate::postings::rk_hash("wanted");
    let mut batch = slatedb::WriteBatch::new();
    for offset in 0..60_001 {
        batch.put(
            crate::postings::postings_key(ROUTE, INC, &kh, 0, offset),
            crate::postings::encode_page(
                offset,
                &[crate::postings::PostingRun {
                    gap_offsets: 0,
                    record_count: 1,
                    matching_frame_bytes: 100,
                    gap_frame_bytes_before: 0,
                }],
            ),
        );
    }
    part.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    part
}

#[tokio::test]
async fn byte_capped_extension_preserves_already_proven_prefix() {
    let part = dense_bucket_part().await;
    let kh = crate::postings::rk_hash("wanted");
    let cache = PostingsCache::new(1 << 20);
    cache.install_chunk(INC, 0, 60_000, vec![(kh.0, vec![run(0, 60_000)])]);
    let (_, encoded, proven, corrupt) = load_runs(&cache, &part, ROUTE, INC, kh, 0, 60_001)
        .await
        .unwrap();
    assert!(!corrupt);
    assert!(encoded >= LOAD_MAX_ENCODED_BYTES);
    assert!(
        proven < 60_000,
        "the byte limit must stop inside the old prefix"
    );
    let existing = cache
        .inner
        .lock()
        .unwrap()
        .slices
        .get(&(INC.0, kh.0))
        .map(|entry| entry.slice.clone());
    let (tx, mut rx) = tokio::sync::watch::channel(false);
    cache
        .inner
        .lock()
        .unwrap()
        .inflight
        .insert((INC.0, kh.0), rx.clone());
    cache.spawn_load(part.clone(), ROUTE, INC, kh, existing, 0, 60_001, tx, false);
    rx.changed().await.unwrap();
    assert_eq!(cache.debug_slice(&INC, &kh), Some((0, 60_000, 1)));
    assert_eq!(cache.runs_for_test(INC, kh), vec![run(0, 60_000)]);
    part.close().await.unwrap();
}

#[tokio::test]
async fn completed_capped_load_does_not_repeat_scans_after_skipped_admission() {
    let part = dense_bucket_part().await;
    let cache = PostingsCache::new(1 << 20);
    let kh = crate::postings::rk_hash("wanted");
    cache.install_chunk(
        SegmentHash([0xe3; 16]),
        0,
        17_000,
        vec![([0xe4; 16], (0..17_000).map(|start| run(start, 1)).collect())],
    );
    cache.install_chunk(INC, 0, 60_001, vec![(kh.0, vec![run(0, 60_001)])]);
    assert_eq!(cache.warm_installs.load(Ordering::Relaxed), 1);
    assert!(cache.runs_for_test(INC, kh).is_empty());
    {
        let CacheRuns::Runs { runs, provable_to } = cache
            .runs_for(&part, ROUTE, INC, kh, 0, 60_001, 60_001)
            .await
            .unwrap()
        else {
            panic!("valid capped postings must retain their proven prefix");
        };
        assert!(provable_to > 0 && provable_to < 60_001);
        assert_eq!(
            runs.iter()
                .flat_map(|run| run.start..run.start + u64::from(run.count))
                .collect::<Vec<_>>(),
            (0..provable_to).collect::<Vec<_>>()
        );
    }
    assert!(cache.index_bytes_read.load(Ordering::Relaxed) >= 2 * LOAD_MAX_ENCODED_BYTES);
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        2,
        "a completed short load must resolve directly, without repeating the same capped scan"
    );
    part.close().await.unwrap();
}

#[tokio::test]
async fn nonzero_bucket_warm_hit_prefetches_future_postings() {
    let part = stored_prefix(4).await;
    let cache = PostingsCache::new(1 << 20);
    let kh = crate::postings::rk_hash("wanted");
    let from = BUCKET_OFFSETS + 100;
    let warm_to = from + 100;
    let absorbed = warm_to + 100;
    part.put(
        crate::postings::postings_key(ROUTE, INC, &kh, 1, from),
        crate::postings::encode_page(
            from,
            &[crate::postings::PostingRun {
                gap_offsets: 0,
                record_count: 200,
                matching_frame_bytes: 20_000,
                gap_frame_bytes_before: 0,
            }],
        ),
    )
    .await
    .unwrap()
    .await_durable()
    .await
    .unwrap();
    cache.install_chunk(INC, from, warm_to, vec![(kh.0, vec![run(from, 100)])]);
    {
        let CacheRuns::Runs { runs, provable_to } = cache
            .runs_for(&part, ROUTE, INC, kh, from, warm_to, absorbed)
            .await
            .unwrap()
        else {
            panic!("a complete warm chunk must be readable");
        };
        assert_eq!(provable_to, warm_to);
        assert_eq!(runs.iter().collect::<Vec<_>>(), [run(from, 100)]);
    }
    assert_eq!(cache.prefetch_started.load(Ordering::Relaxed), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while cache.prefetch_completed.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let CacheRuns::Runs { runs, provable_to } = cache
            .runs_for(&part, ROUTE, INC, kh, from, absorbed, absorbed)
            .await
            .unwrap()
        else {
            panic!("prefetched valid postings must be readable");
        };
        assert_eq!(provable_to, absorbed);
        assert_eq!(
            runs.iter()
                .flat_map(|run| run.start..run.start + u64::from(run.count))
                .collect::<Vec<_>>(),
            (from..absorbed).collect::<Vec<_>>()
        );
    }
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    part.close().await.unwrap();
}

#[test]
fn quality_cold_load_clipping_and_extension_match_offset_oracle() {
    use proptest::test_runner::{Config, TestRunner};
    use proptest::{arbitrary::any, prop_assert_eq};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let part = runtime.block_on(stored_prefix(128));
    let kh = crate::postings::rk_hash("wanted");
    let mut runner = TestRunner::new(Config {
        cases: 1024,
        source_file: Some(file!()),
        ..Config::default()
    });
    runner
        .run(&(1u64..128, any::<bool>()), |(boundary, straddling)| {
            let cache = PostingsCache::new(1 << 20);
            let prefix = runtime.block_on(offsets(&cache, &part, boundary));
            prop_assert_eq!(prefix, (0..boundary).collect::<Vec<_>>());
            prop_assert_eq!(cache.debug_slice(&INC, &kh), Some((0, boundary, 1)));
            let fresh = if straddling {
                run(0, 128)
            } else {
                run(boundary, u32::try_from(128 - boundary).unwrap())
            };
            cache.install_chunk(INC, boundary, 128, vec![(kh.0, vec![fresh])]);
            let complete = runtime.block_on(offsets(&cache, &part, 128));
            prop_assert_eq!(complete, (0..128).collect::<Vec<_>>());
            prop_assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
            Ok(())
        })
        .unwrap();
    runtime.block_on(part.close()).unwrap();
}

#[tokio::test]
async fn cold_empty_key_slice_extends_when_key_first_appears() {
    let part = stored_prefix(4).await;
    let cache = PostingsCache::new(1 << 20);
    let kh = crate::postings::rk_hash("initially absent");
    {
        let CacheRuns::Runs { runs, provable_to } = cache
            .runs_for(&part, ROUTE, INC, kh, 0, 4, 4)
            .await
            .unwrap()
        else {
            panic!("valid empty index must be admitted");
        };
        assert_eq!(runs.iter().count(), 0);
        assert_eq!(provable_to, 4);
    }
    cache.install_chunk(INC, 4, 5, vec![(kh.0, vec![run(4, 1)])]);
    {
        let CacheRuns::Runs { runs, provable_to } = cache
            .runs_for(&part, ROUTE, INC, kh, 0, 5, 5)
            .await
            .unwrap()
        else {
            panic!("valid appended posting must be admitted");
        };
        assert_eq!(
            runs.iter()
                .map(|run| (run.start, run.count))
                .collect::<Vec<_>>(),
            [(4, 1)]
        );
        assert_eq!(provable_to, 5);
    }
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 1);
    part.close().await.unwrap();
}
