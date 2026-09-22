//! Postings cache fixtures: write-through installs, warm extensions,
//! the process budget at scale and eviction poisoning.
#![cfg(test)]
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

/// One write-through chunk of a segment's keys: `per_chunk` distinct keys,
/// each carrying a two-offset run at its own position in the chunk.
fn chunk_keys(seg: u8, chunk: u64, per_chunk: u64) -> Vec<([u8; 16], Vec<AbsRun>)> {
    let base = chunk * 1_000;
    (0..per_chunk)
        .map(|i| {
            let key_id = chunk * per_chunk + i;
            let mut kh = [seg; 16];
            kh[..8].copy_from_slice(&key_id.to_le_bytes());
            (kh, vec![run(base + i, 2)])
        })
        .collect()
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
            cache.install_chunk(
                inc,
                base,
                base + 1_000,
                chunk_keys(seg, chunk, per_seg_keys / 8),
            );
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

/// One stored postings page holding a single run (the shape the absorber
/// writes). Reads at the default memory durability level see it at once.
async fn put_run(part: &Arc<Db>, n: u8, start: u64, count: u32) {
    let (route, inc, kh) = ids(n);
    let page = crate::postings::encode_page(
        start,
        &[crate::postings::PostingRun {
            gap_offsets: 0,
            record_count: count,
            matching_frame_bytes: u64::from(count) * 100,
            gap_frame_bytes_before: 0,
        }],
    );
    let bucket = crate::postings::bucket_of(start);
    part.put(
        crate::postings::postings_key(route, inc, &kh, bucket, start),
        page,
    )
    .await
    .unwrap();
}

fn spans(runs: &[AbsRun]) -> Vec<(u64, u32)> {
    runs.iter().map(|r| (r.start, r.count)).collect()
}

/// A cold load proves nothing past the absorbed boundary it was asked for:
/// postings above it did not exist at scan time. The slice must end at the
/// target so the next write-through chunk in the same bucket extends it.
#[tokio::test]
async fn cold_load_claims_only_to_its_absorbed_target() {
    let part = mem_db("wt/f").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(6);
    assert!(runs_of(&cache, &part, 6, 0, 100).await.is_empty());
    assert_eq!(
        cache.debug_slice(&inc, &kh),
        Some((0, 100, 0)),
        "a cold load at absorbed=100 must not claim the rest of its bucket"
    );
    cache.install_chunk(inc, 100, 200, vec![(kh.0, vec![run(150, 2)])]);
    assert_eq!(
        spans(&runs_of(&cache, &part, 6, 100, 200).await),
        vec![(150, 2)],
        "a catch-up read over the later chunk must see its run"
    );
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 200, 1)));
    assert_eq!(cache.warm_extends.load(Ordering::Relaxed), 1);
}

/// The absorber flushes a chunk's pages BEFORE it installs the chunk and
/// advances the boundary, so a cold scan can see runs past the reader's
/// absorbed snapshot. They must be clipped to the claim, or the slice can
/// never be extended again (extend_after refuses a prefix past its cut).
#[tokio::test]
async fn cold_load_clips_runs_the_store_holds_past_its_target() {
    let part = mem_db("wt/g").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(7);
    put_run(&part, 7, 90, 30).await; // [90, 120): straddles absorbed = 100
    assert_eq!(
        spans(&runs_of(&cache, &part, 7, 0, 100).await),
        vec![(90, 10)]
    );
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 100, 1)));
    assert_eq!(spans(&cache.runs_for_test(inc, kh)), vec![(90, 10)]);
    // The absorber now installs the chunk it had already written.
    cache.install_chunk(inc, 100, 200, vec![(kh.0, vec![run(90, 30)])]);
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 200, 2)));
    assert_eq!(
        spans(&runs_of(&cache, &part, 7, 0, 200).await),
        vec![(90, 10), (100, 20)]
    );
}

/// Parks a keyed read of [0, upto) on its own single-flight load (on this
/// current-thread runtime the spawned loader cannot run until the test task
/// yields), runs `mid_load`, then lets the load publish and the read finish.
async fn read_with_install_mid_load(
    cache: &Arc<PostingsCache>,
    part: &Arc<Db>,
    n: u8,
    upto: u64,
    mid_load: impl FnOnce(),
) {
    let (route, inc, kh) = ids(n);
    let mut read = Box::pin(cache.runs_for(part, route, inc, kh, 0, upto, upto));
    let parked =
        std::future::poll_fn(|cx| std::task::Poll::Ready(read.as_mut().poll(cx).is_pending()))
            .await;
    assert!(parked, "the leader must park on its own load");
    mid_load();
    assert!(matches!(read.await.unwrap(), CacheRuns::Runs { .. }));
}

/// The write-through install is not gated by `inflight`: it can land
/// between a load's scan and its publish. The publish must merge into the
/// entry present NOW, never replace it from the Lead-time snapshot.
#[tokio::test]
async fn load_publish_joins_an_install_that_landed_mid_load() {
    let part = mem_db("wt/h").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(8);
    put_run(&part, 8, 10, 5).await;
    // Key cold at Lead (absorbed = 100); the absorber installs the next
    // chunk (chunk-only: the warm window starts at 100) before the publish.
    read_with_install_mid_load(&cache, &part, 8, 100, || {
        cache.install_chunk(inc, 100, 200, vec![(kh.0, vec![run(150, 4)])]);
    })
    .await;
    assert_eq!(
        spans(&runs_of(&cache, &part, 8, 0, 200).await),
        vec![(10, 5), (150, 4)],
        "the stale publish dropped the install's run"
    );
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 200, 2)));
}

/// Same race with an entry already resident at Lead time: the install
/// extends the live entry to 200 while the load still holds the {0,100}
/// snapshot. The publish must not lower indexed_to or drop the new run.
#[tokio::test]
async fn load_publish_never_regresses_an_entry_extended_mid_load() {
    let part = mem_db("wt/i").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(10);
    cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(10, 5)])]);
    read_with_install_mid_load(&cache, &part, 10, 150, || {
        cache.install_chunk(inc, 100, 200, vec![(kh.0, vec![run(150, 4)])]);
    })
    .await;
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 200, 2)));
    assert_eq!(
        spans(&cache.runs_for_test(inc, kh)),
        vec![(10, 5), (150, 4)]
    );
    assert_eq!(
        cache.inner.lock().unwrap().total_bytes,
        2 * std::mem::size_of::<AbsRun>() + ENTRY_OVERHEAD_BYTES,
        "a refused publish must not touch the resident weight"
    );
}

fn within(runs: &[AbsRun], from: u64, upto: u64) -> Vec<AbsRun> {
    runs.iter()
        .filter_map(|r| {
            let start = r.start.max(from);
            let end = (r.start + u64::from(r.count)).min(upto);
            (start < end).then(|| run(start, u32::try_from(end - start).unwrap()))
        })
        .collect()
}

fn offsets(runs: &[AbsRun]) -> Vec<u64> {
    runs.iter()
        .flat_map(|r| r.start..r.start + u64::from(r.count))
        .collect()
}

/// The pages the absorber writes for one chunk: per-chunk runs, split at
/// the bucket boundary (a stored page never crosses its bucket).
async fn put_chunk(part: &Arc<Db>, n: u8, runs: &[AbsRun], from: u64, upto: u64) {
    for (lo, hi) in [
        (from, upto.min(BUCKET_OFFSETS)),
        (from.max(BUCKET_OFFSETS), upto),
    ] {
        for r in within(runs, lo, hi) {
            put_run(part, n, r.start, r.count).await;
        }
    }
}

/// Cold-load at `bounds[0]`, then absorb the remaining chunks write-through.
/// `visible`: the first later chunk's pages are already stored when the cold
/// scan runs (its install and boundary advance still pending).
async fn cold_load_then_installs(runs: &[AbsRun], bounds: &[u64], visible: bool) {
    let part = mem_db("wt/prop").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(11);
    let (absorbed, end) = (bounds[0], bounds[bounds.len() - 1]);
    put_chunk(&part, 11, runs, 0, absorbed).await;
    if visible {
        put_chunk(&part, 11, runs, absorbed, bounds[1]).await;
    }
    let cold = runs_of(&cache, &part, 11, 0, absorbed).await;
    assert_eq!(offsets(&cold), offsets(&within(runs, 0, absorbed)));
    assert_eq!(
        cache.debug_slice(&inc, &kh).map(|s| (s.0, s.1)),
        Some((0, absorbed)),
        "indexed_to must equal the load target"
    );
    for (i, w) in bounds.windows(2).enumerate() {
        if !(visible && i == 0) {
            put_chunk(&part, 11, runs, w[0], w[1]).await;
        }
        let chunk = within(runs, w[0], w[1]);
        let per_key = if chunk.is_empty() {
            vec![]
        } else {
            vec![(kh.0, chunk)]
        };
        cache.install_chunk(inc, w[0], w[1], per_key);
    }
    let tail = runs_of(&cache, &part, 11, absorbed, end).await;
    assert_eq!(offsets(&tail), offsets(&within(runs, absorbed, end)));
    let all = runs_of(&cache, &part, 11, 0, end).await;
    assert_eq!(offsets(&all), offsets(runs));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(1024))]

    /// (absorbed-at-load, later chunk ends, run positions): the cold slice
    /// never claims past its target and every run installed afterwards is
    /// returned to a catch-up reader, inside one bucket and across its end.
    #[test]
    fn quality_a_cold_load_never_hides_a_later_install(
        at_bucket_end in proptest::bool::ANY,
        specs in proptest::collection::vec((0u64..60, 1u32..30), 1..40),
        cuts in proptest::collection::vec(1u64..1400, 1..6),
        visible in proptest::bool::ANY,
    ) {
        let base = if at_bucket_end { BUCKET_OFFSETS - 700 } else { 0 };
        let mut next = base;
        let runs: Vec<AbsRun> = specs
            .iter()
            .map(|&(gap, count)| {
                let r = run(next + gap, count);
                next = r.start + u64::from(count);
                r
            })
            .collect();
        let mut bounds: Vec<u64> = cuts.iter().map(|c| base + c).collect();
        bounds.push(next.max(base + 1400) + 1);
        bounds.sort_unstable();
        bounds.dedup();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(cold_load_then_installs(&runs, &bounds, visible));
    }
}

fn publish(
    cache: &PostingsCache,
    key: Key,
    start_bucket: u64,
    runs: Vec<AbsRun>,
    load_to: u64,
) -> bool {
    let loaded = ValidatedRuns::new(runs).unwrap();
    cache
        .inner
        .lock()
        .unwrap()
        .publish_load(key, start_bucket, loaded, load_to)
}

#[test]
fn publish_load_merges_into_the_resident_entry_or_publishes_nothing() {
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(12);
    let key: Key = (inc.0, kh.0);
    // No resident entry: the load is the entry.
    assert!(publish(&cache, key, 0, vec![run(10, 5)], 100));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 100, 1)));
    // Resident already covers the load: nothing to add, never lowered.
    assert!(!publish(&cache, key, 0, vec![run(10, 5)], 100));
    assert!(!publish(&cache, key, 0, vec![run(10, 5)], 60));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 100, 1)));
    // Resident behind the load and seaming with it: extended.
    assert!(publish(&cache, key, 0, vec![run(10, 5), run(120, 3)], 150));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 150, 2)));
    assert_eq!(
        spans(&cache.runs_for_test(inc, kh)),
        vec![(10, 5), (120, 3)]
    );
    // A load starting past the resident frontier would leave a hole.
    let far = BUCKET_OFFSETS + 5;
    assert!(!publish(
        &cache,
        key,
        1,
        vec![run(far, 1)],
        BUCKET_OFFSETS + 50
    ));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 150, 2)));
    assert_eq!(
        cache.inner.lock().unwrap().total_bytes,
        2 * std::mem::size_of::<AbsRun>() + ENTRY_OVERHEAD_BYTES,
        "a merge replaces the entry's weight, it does not add to it"
    );
    // A load starting EXACTLY at the resident frontier seams.
    let (_, inc, kh) = ids(16);
    let key: Key = (inc.0, kh.0);
    assert!(publish(&cache, key, 0, vec![], BUCKET_OFFSETS));
    let next = BUCKET_OFFSETS + 4;
    assert!(publish(
        &cache,
        key,
        1,
        vec![run(next, 1)],
        BUCKET_OFFSETS + 50
    ));
    assert_eq!(
        cache.debug_slice(&inc, &kh),
        Some((0, BUCKET_OFFSETS + 50, 1))
    );
}

#[test]
fn publish_load_joins_a_chunk_only_entry_only_across_a_proven_seam() {
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(13);
    let key: Key = (inc.0, kh.0);
    cache.install_chunk(inc, 0, 100, vec![]);
    cache.install_chunk(inc, 300, 400, vec![(kh.0, vec![run(350, 2)])]);
    assert_eq!(cache.debug_slice(&inc, &kh), Some((300, 400, 1)));
    // A hole between the load and the chunk-only entry: publish nothing.
    assert!(!publish(&cache, key, 0, vec![run(10, 5)], 299));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((300, 400, 1)));
    // Touching: one slice from the load's base, keeping the install's run.
    assert!(publish(&cache, key, 0, vec![run(10, 5)], 300));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 400, 2)));
    assert_eq!(
        spans(&cache.runs_for_test(inc, kh)),
        vec![(10, 5), (350, 2)]
    );
    // A load reaching past the chunk-only entry keeps the LOAD's base.
    let (_, inc, kh) = ids(15);
    let key: Key = (inc.0, kh.0);
    cache.install_chunk(inc, 0, 100, vec![]);
    cache.install_chunk(inc, 300, 400, vec![(kh.0, vec![run(350, 2)])]);
    let scanned = vec![run(10, 5), run(350, 2), run(450, 1)];
    assert!(publish(&cache, key, 0, scanned, 500));
    assert_eq!(cache.debug_slice(&inc, &kh), Some((0, 500, 3)));
}

/// A load publish evicts to budget like an install does: only when OVER the
/// budget, least-recent first, never the entry it just published.
#[tokio::test]
async fn load_publish_evicts_least_recent_others_only_over_budget() {
    let part = mem_db("wt/k").await;
    let cache = PostingsCache::new(1); // clamps to the 1 MiB floor
    let (_, fat_inc, fat_kh) = ids(20);
    // Sized so ONE more empty entry lands exactly ON the budget.
    let n = (1024 * 1024 - 2 * ENTRY_OVERHEAD_BYTES) / std::mem::size_of::<AbsRun>();
    let fat: Vec<AbsRun> = (0..u64::try_from(n).unwrap())
        .map(|i| run(i * 2, 1))
        .collect();
    cache.install_chunk(fat_inc, 0, 70_000, vec![(fat_kh.0, fat)]);
    assert!(runs_of(&cache, &part, 30, 0, 100).await.is_empty());
    assert_eq!(cache.inner.lock().unwrap().total_bytes, 1024 * 1024);
    assert_eq!(
        cache.evictions.load(Ordering::Relaxed),
        0,
        "on budget, not over"
    );
    assert!(runs_of(&cache, &part, 40, 0, 100).await.is_empty());
    assert_eq!(cache.evictions.load(Ordering::Relaxed), 1);
    assert!(
        cache.runs_for_test(fat_inc, fat_kh).is_empty(),
        "victim gone"
    );
    let (_, inc30, kh30) = ids(30);
    let (_, inc40, kh40) = ids(40);
    assert!(cache.debug_slice(&inc30, &kh30).is_some());
    assert!(
        cache.debug_slice(&inc40, &kh40).is_some(),
        "never evicts itself"
    );
    assert_eq!(
        cache.inner.lock().unwrap().total_bytes,
        2 * ENTRY_OVERHEAD_BYTES
    );
}

/// A byte-capped load is covered exactly through its last decoded run: the
/// pages behind the cap were never read, so nothing about them is proven
/// and the next load must scan them. This arm's arithmetic had no test at
/// all — `start + count` survived mutation to `start - count` and to
/// `start * count`.
#[tokio::test]
async fn byte_capped_load_claims_through_its_last_decoded_run() {
    const RUNS_PER_PAGE: u64 = 8_100; // four bytes a run: a ~32 KiB page
    const PAGE_SPAN: u64 = RUNS_PER_PAGE * 2; // each run is one offset, then one of gap
    const PAGES_PER_BUCKET: u64 = 4;
    const PAGES: u64 = 36;
    let first_offset =
        |p: u64| (p / PAGES_PER_BUCKET) * BUCKET_OFFSETS + (p % PAGES_PER_BUCKET) * PAGE_SPAN;
    let part = mem_db("wt/cap").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (route, inc, kh) = ids(11);
    let page_runs: Vec<_> = (0..RUNS_PER_PAGE)
        .map(|i| crate::postings::PostingRun {
            gap_offsets: u64::from(i > 0),
            record_count: 1,
            matching_frame_bytes: 1,
            gap_frame_bytes_before: 0,
        })
        .collect();
    let mut page_bytes = 0;
    for p in 0..PAGES {
        let first = first_offset(p);
        let page = crate::postings::encode_page(first, &page_runs);
        page_bytes = page.len() as u64;
        let bucket = crate::postings::bucket_of(first);
        part.put(
            crate::postings::postings_key(route, inc, &kh, bucket, first),
            page,
        )
        .await
        .unwrap();
    }
    // The cap trips inside the scan, with pages still unread behind it.
    let scanned = LOAD_MAX_ENCODED_BYTES.div_ceil(page_bytes);
    assert!(scanned < PAGES, "the fixture must outrun the byte cap");
    let last_run_end = first_offset(scanned - 1) + PAGE_SPAN - 1;
    let target = (PAGES / PAGES_PER_BUCKET) * BUCKET_OFFSETS;

    let (runs, encoded, provable_to, corrupt) = load_runs(&cache, &part, route, inc, kh, 0, target)
        .await
        .unwrap();
    assert!(!corrupt);
    assert_eq!(encoded, scanned * page_bytes, "stopped at the byte cap");
    assert_eq!(runs.len() as u64, scanned * RUNS_PER_PAGE);
    assert_eq!(
        provable_to, last_run_end,
        "a byte-capped load is proven through its last decoded run, no further"
    );
    assert!(provable_to < target, "and that is short of what was asked");

    // The cached read reports that short claim honestly, and a reader that
    // resumes from each claim is served every run behind the cap: nothing
    // there was ever treated as proven absent.
    let (mut cursor, mut served) = (0, 0);
    for _ in 0..8 {
        let answer = cache
            .runs_for(&part, route, inc, kh, cursor, target, target)
            .await
            .unwrap();
        let CacheRuns::Runs { runs, provable_to } = answer else {
            panic!("unexpected corruption");
        };
        assert!(provable_to > cursor, "a read must make progress");
        served += runs.iter().count() as u64;
        cursor = provable_to;
        if cursor >= target {
            break;
        }
    }
    assert_eq!(cursor, target, "the reader reached its target");
    assert_eq!(
        served,
        PAGES * RUNS_PER_PAGE,
        "runs behind the byte cap were treated as proven absent"
    );
}

/// A panicking OWNED load is a failed load: its single-flight marker
/// clears, so the next read of the key leads a fresh owned load that
/// publishes, instead of spinning on the dead channel and loading
/// uncached forever (with prefetch disabled for the key).
#[tokio::test]
async fn a_panicking_owned_load_clears_its_single_flight_marker() {
    let part = mem_db("wt/panic").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(9);
    cache.panic_next_owned_load();

    // Cold key: this read leads the owned load, which panics once; the
    // reader wakes on the finished (failed) load and loads directly.
    let _ = runs_of(&cache, &part, 9, 0, 100).await;
    assert_eq!(
        cache.coalesced.load(Ordering::Relaxed),
        0,
        "the reader spun on the dead loader's channel: the marker outlived its task"
    );
    assert!(
        cache.debug_slice(&inc, &kh).is_none(),
        "a panicked load publishes nothing"
    );

    // The next read leads a FRESH owned load, which publishes.
    let _ = runs_of(&cache, &part, 9, 0, 100).await;
    assert!(
        cache.debug_slice(&inc, &kh).is_some(),
        "the load after a panicked one must publish"
    );
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        2,
        "one direct load after the panic, one owned load that published"
    );

    // And the published slice serves every later read.
    let _ = runs_of(&cache, &part, 9, 0, 100).await;
    assert_eq!(cache.hits.load(Ordering::Relaxed), 2);
}
