use super::*;

fn run(start: u64, count: u32) -> AbsRun {
    AbsRun {
        start,
        count,
        matching_bytes: count as u64 * 100,
        gap_bytes_before: 0,
    }
}

async fn mem_db(prefix: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    Arc::new(Db::builder(prefix, store).build().await.unwrap())
}

fn ids(n: u8) -> (RouteHash, SegmentHash, RoutingKeyHash) {
    (
        RouteHash([n; 16]),
        SegmentHash([n.wrapping_add(1); 16]),
        RoutingKeyHash([n.wrapping_add(2); 16]),
    )
}

async fn runs_of(
    c: &Arc<PostingsCache>,
    part: &Arc<Db>,
    n: u8,
    from: u64,
    upto: u64,
) -> Vec<AbsRun> {
    let (route, inc, kh) = ids(n);
    match c
        .runs_for(part, route, inc, kh, from, upto, upto)
        .await
        .unwrap()
    {
        CacheRuns::Runs { runs, provable_to } => {
            assert!(provable_to >= upto, "honest coverage to the request");
            runs.iter().collect()
        }
        CacheRuns::Corrupt => panic!("unexpected corruption"),
    }
}

/// Write-through warming: a chunk installed from offset 0 serves a
/// from-0 read entirely from the cache — no index round trip.
#[tokio::test]
async fn warm_install_serves_from_zero_without_index_load() {
    let part = mem_db("wt/a").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(1);
    cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 3)])]);
    let got = runs_of(&cache, &part, 1, 0, 100).await;
    assert_eq!(got, vec![run(5, 3)]);
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        0,
        "no store load"
    );
    assert_eq!(cache.hits.load(Ordering::Relaxed), 1);
    assert_eq!(cache.warm_installs.load(Ordering::Relaxed), 1);
}

/// Contiguous chunks extend the same slice; the stitched slice
/// serves the union without touching the store.
#[tokio::test]
async fn warm_extension_stitches_chunks() {
    let part = mem_db("wt/b").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(2);
    cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 3)])]);
    cache.install_chunk(inc, 100, 200, vec![(kh.0, vec![run(150, 2)])]);
    assert_eq!(cache.warm_extends.load(Ordering::Relaxed), 1);
    let got = runs_of(&cache, &part, 2, 0, 200).await;
    // The chunk seam is a stitched boundary: its gap is UNKNOWN by
    // design (the planner refuses to coalesce across it), exactly as
    // if the two pages had been loaded from the store.
    let seam = AbsRun {
        gap_bytes_before: crate::postings::GAP_UNKNOWN,
        ..run(150, 2)
    };
    assert_eq!(got, vec![run(5, 3), seam]);
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 0);
}

/// A gap in the chunk sequence (restart / ownership move) resets the
/// claim window: a key first seen AFTER the gap must not pretend its
/// earlier history is empty — a from-0 read consults the store.
#[tokio::test]
async fn noncontiguous_chunk_resets_absence_claim() {
    let part = mem_db("wt/c").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(3);
    cache.install_chunk(inc, 0, 100, vec![]);
    cache.install_chunk(inc, 150, 220, vec![(kh.0, vec![run(160, 1)])]);
    // From the chunk itself: warm hit.
    let got = runs_of(&cache, &part, 3, 150, 220).await;
    assert_eq!(got, vec![run(160, 1)]);
    assert_eq!(cache.index_loads.load(Ordering::Relaxed), 0);
    // From 0: below covered_from — must go to the store.
    let _ = runs_of(&cache, &part, 3, 0, 220).await;
    assert!(
        cache.index_loads.load(Ordering::Relaxed) >= 1,
        "read below the claim window must consult the index"
    );
}

/// A key absent from intermediate chunks stays warm: the clean
/// contiguous warm window proves the hole match-free, so the
/// extension bridges it (seam marked GAP_UNKNOWN like any stitched
/// page boundary).
#[tokio::test]
async fn warm_extension_bridges_matchfree_hole() {
    let part = mem_db("wt/e").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(5);
    cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 3)])]);
    cache.install_chunk(inc, 100, 200, vec![]); // key absent
    cache.install_chunk(inc, 200, 300, vec![(kh.0, vec![run(250, 2)])]);
    assert_eq!(cache.warm_extends.load(Ordering::Relaxed), 1);
    let got = runs_of(&cache, &part, 5, 0, 300).await;
    let seam = AbsRun {
        gap_bytes_before: crate::postings::GAP_UNKNOWN,
        ..run(250, 2)
    };
    assert_eq!(got, vec![run(5, 3), seam]);
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        0,
        "bridged, no load"
    );

    // A DIRTY window must NOT bridge: poison via a sweep, then a
    // later chunk cannot extend across the unproven middle.
    cache.sweep_idle(Duration::ZERO);
    cache.install_chunk(inc, 300, 400, vec![(kh.0, vec![run(350, 1)])]);
    let loads0 = cache.index_loads.load(Ordering::Relaxed);
    let _ = runs_of(&cache, &part, 5, 0, 400).await;
    assert!(
        cache.index_loads.load(Ordering::Relaxed) > loads0,
        "post-sweep reads must consult the store"
    );
}

/// Review finding 7's scale shape, cache-level and suite-sized (the
/// field campaign runs the full 1M x 32-engine version): a large
/// cold key population written through 32 segments must not blow
/// the ONE process budget; a small active read set stays >= 90%
/// warm after each key's first read; inactive keys do not hold
/// long-lived entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn million_key_shape_holds_process_budget() {
    let part = mem_db("wt/scale").await;
    let budget = 2 * 1024 * 1024usize;
    let cache = PostingsCache::new(budget);
    // 32 segments x 4,000 keys/segment x 8 chunks: 128k distinct
    // keys pushed through write-through installs.
    let per_seg_keys = 4_000u64;
    for seg in 0..32u8 {
        let inc = SegmentHash([seg.wrapping_add(50); 16]);
        for chunk in 0..8u64 {
            let base = chunk * 1_000;
            let per_key: Vec<([u8; 16], Vec<AbsRun>)> = (0..per_seg_keys / 8)
                .map(|i| {
                    let key_id = chunk * (per_seg_keys / 8) + i;
                    let mut kh = [seg; 16];
                    kh[..8].copy_from_slice(&key_id.to_le_bytes());
                    (kh, vec![run(base + i, 2)])
                })
                .collect();
            cache.install_chunk(inc, base, base + 1_000, per_key);
        }
    }
    let (bytes, entries) = {
        let g = cache.inner.lock().unwrap();
        (g.total_bytes, g.slices.len())
    };
    assert!(
        bytes <= budget,
        "process budget must hold: {bytes} > {budget}"
    );
    assert!(
        entries < 128_000 / 4,
        "cold keys must not all hold entries (entries={entries})"
    );

    // Active read set: 100 keys, 20 reads each. After each key's
    // FIRST read, everything must be a hit.
    let inc = SegmentHash([50; 16]);
    let route = RouteHash([0; 16]);
    let mut first_reads = 0u64;
    for key_id in 0..100u64 {
        let mut kh = [0u8; 16];
        kh[..8].copy_from_slice(&key_id.to_le_bytes());
        first_reads += 1;
        for _ in 0..20 {
            let _ = cache
                .runs_for(&part, route, inc, RoutingKeyHash(kh), 0, 8_000, 8_000)
                .await
                .unwrap();
        }
    }
    let hits = cache.hits.load(Ordering::Relaxed);
    let total_reads = 100 * 20;
    let warm_reads = total_reads - first_reads; // first read may load
    assert!(
        hits >= warm_reads * 9 / 10,
        "active-set warm hit rate >= 90%: hits={hits} warm={warm_reads}"
    );
}

/// Evicting any entry of a segment poisons its absence proof: a key
/// evicted and later reinstalled fresh must not claim from 0.
#[tokio::test]
async fn eviction_poisons_fresh_claims() {
    let part = mem_db("wt/d").await;
    let cache = PostingsCache::new(1); // clamps to the 1 MiB floor
    let (_, inc, kh) = ids(4);
    let (_, _, other) = ids(9);
    // Write-admission stops FRESH installs at half budget, so the
    // over-budget pressure comes from an EXTEND (extends always
    // apply to existing entries).
    cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 1)])]);
    cache.install_chunk(inc, 100, 200, vec![(other.0, vec![run(150, 1)])]);
    let fat: Vec<AbsRun> = (0..40_000u64).map(|i| run(200 + i * 2, 1)).collect();
    cache.install_chunk(inc, 200, 200_000, vec![(kh.0, fat)]);
    assert!(
        cache.evictions.load(Ordering::Relaxed) >= 1,
        "budget must evict"
    );
    // The evicted key reinstalls fresh in the next contiguous chunk:
    // it must claim only the chunk, so a from-0 read hits the store.
    cache.install_chunk(
        inc,
        200_000,
        200_100,
        vec![(other.0, vec![run(200_050, 1)])],
    );
    let loads_before = cache.index_loads.load(Ordering::Relaxed);
    let _ = runs_of(&cache, &part, 9, 0, 200_100).await;
    assert!(
        cache.index_loads.load(Ordering::Relaxed) > loads_before,
        "poisoned segment must not serve absence from the warm claim"
    );
}
