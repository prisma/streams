//! History gather.

use super::fixture_storage::{
    append_sized, mem, open_engine_with_settings, skey, wait_all_absorbed,
};
use crate::dst::{FaultPlan, FaultStore};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

fn gather_hashes(tag: u8, count: u16) -> Vec<[u8; 16]> {
    (0..count)
        .map(|index| {
            let [high, low] = index.to_be_bytes();
            let mut hash = [0; 16];
            hash[0] = tag;
            hash[1] = high;
            hash[2] = low;
            hash
        })
        .collect()
}

async fn gather_after_reopen(
    path: &str,
    seed: u64,
    cfg: crate::history::AbsorberConfig,
    key: &crate::crypto::StreamKey,
) -> (Vec<([u8; 16], u64)>, Arc<crate::shard::ShardEngine>) {
    let hashes = gather_hashes(0xA9, 96);
    let inner = mem();
    let store = FaultStore::uniform(
        inner,
        seed,
        FaultPlan {
            error_pct: 0,
            lost_response_pct: 0,
            latency_pct: 100,
            latency_ms: (20, 20),
        },
    );
    let engine = open_engine_with_settings(
        store.clone(),
        path,
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    for h in &hashes {
        append_sized(&engine, *h, key, "", 2048).await;
    }
    // Fresh appends sit in the shard memtable and just-flushed
    // L0 blocks sit in slatedb's block cache — served from
    // either, a read costs no store op and both rigs time
    // identically. Flush, then REOPEN the db (CAS-fences the
    // seeder engine, the absorption-war precedent): the second
    // engine starts with a cold cache, so the gather's frame
    // reads genuinely traverse the latency-injected store.
    engine.db.flush().await.unwrap();
    let engine2 = open_engine_with_settings(
        store.clone(),
        path,
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine2.clone(),
        Arc::new(crate::history::KeyCache::default()),
        cfg,
    );
    let outcome = absorber.absorb_gather_v2(&hashes).await.expect("gather");
    let mut advanced: Vec<([u8; 16], u64)> = outcome
        .advanced
        .iter()
        .map(|(h, upto, _)| (*h, *upto))
        .collect();
    advanced.sort_unstable();
    (advanced, engine2)
}

async fn gather_with_pacing(
    path: &str,
    cfg: crate::history::AbsorberConfig,
    key: &crate::crypto::StreamKey,
) -> (
    Vec<([u8; 16], u64)>,
    std::time::Duration,
    Arc<crate::shard::ShardEngine>,
) {
    let hashes = gather_hashes(0xA8, 96);
    let store = mem();
    let engine = open_engine_with_settings(
        store.clone(),
        path,
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    for h in &hashes {
        append_sized(&engine, *h, key, "", 2048).await;
    }
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        cfg,
    );
    let t0 = std::time::Instant::now();
    let outcome = absorber.absorb_gather_v2(&hashes).await.expect("gather");
    let mut advanced: Vec<([u8; 16], u64)> = outcome
        .advanced
        .iter()
        .map(|(h, upto, _)| (*h, *upto))
        .collect();
    advanced.sort_unstable();
    (advanced, t0.elapsed(), engine)
}

/// Preserve the existing lower order statistic: floor((30 - 1) * p).
/// The fixed 30-probe episode uses indices 14 (p50) and 28 (p99).
async fn paced_append_p99_ms(
    engine: &Arc<crate::shard::ShardEngine>,
    key: &crate::crypto::StreamKey,
    tag: &str,
) -> u128 {
    let probe = [0xA7; 16];
    let mut latencies = [0; 30];
    for sample in &mut latencies {
        let started = std::time::Instant::now();
        append_sized(engine, probe, key, "", 1024).await;
        *sample = started.elapsed().as_millis();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    latencies.sort_unstable();
    eprintln!("{tag}: p50={}ms p99={}ms", latencies[14], latencies[28]);
    latencies[28]
}

/// #266 adaptive-estimate pin: the reservation estimate seeds at the
/// worst case (boot gathers cover restart-rediscovery bursts), decays
/// to the floor under sustained sparse gathers, and jumps back to a
/// fat observation immediately. The CHAOS-3 comment measured 6 MB
/// actual gathers against the fixed 96 MiB reservation; the L1 ladder
/// showed that fiction crossing the RSS shed line and shedding
/// appends whenever gathers overlapped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adaptive_gather_estimate_seeds_decays_and_jumps() {
    let store = mem();
    let engine = open_engine_with_settings(
        store.clone(),
        "dst-est",
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings::default(),
    )
    .await;
    let absorber = crate::history::Absorber::new(
        store,
        engine,
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig::default(),
    );
    let cap = crate::history::AbsorberConfig::default()
        .gather_max_bytes
        .saturating_mul(crate::history::ABSORB_BUILD_MULTIPLIER)
        .max(crate::history::absorb_worst_frame_transient());
    assert_eq!(
        absorber.adaptive_gather_est(),
        cap,
        "estimate must seed at the worst case"
    );
    // Sustained sparse gathers decay the estimate to the floor.
    for _ in 0..64 {
        absorber.observe_gather_transient_for_tests(2 * 1024 * 1024);
    }
    let floor = crate::history::worst_frame_transient_for(4 * 1024 * 1024);
    assert_eq!(
        absorber.adaptive_gather_est(),
        floor.min(cap),
        "sustained sparse gathers must decay the estimate to the floor"
    );
    // One fat gather jumps it immediately (x3 build multiplier).
    absorber.observe_gather_transient_for_tests(20 * 1024 * 1024);
    assert_eq!(
        absorber.adaptive_gather_est(),
        (60 * 1024 * 1024).min(cap),
        "a fat observation must raise the estimate immediately"
    );
}

/// #266 read-parallelism pin: a gather with concurrent per-stream
/// frame reads settles exactly the same streams to exactly the same
/// boundaries as a serial one — across a db REOPEN, so plan
/// collection runs against cold (non-resident) handles, the shape the
/// field lane actually sees. NO timing assertion: three rig
/// iterations showed a mem-store cannot isolate per-stream read
/// latency deterministically (96 tiny streams share one SST so one
/// fetch feeds the lane; WAL replay refills the memtable on reopen;
/// the block cache serves just-flushed blocks). The 8x overlap is a
/// FIELD measurement — gather_last_read_ms on /v1/debug/load, L1d10
/// vs L1d9 in bench/WORKLOAD-CERT-PLAN.md.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gather_parallel_reads_preserve_outcomes_across_reopen() {
    const N: usize = 96;
    let key = skey();
    let seed = 7501;

    let serial_cfg = crate::history::AbsorberConfig {
        gather_read_par: 1,
        gather_pace: std::time::Duration::ZERO,
        ..Default::default()
    };
    let par_cfg = crate::history::AbsorberConfig {
        gather_read_par: 8,
        gather_pace: std::time::Duration::ZERO,
        ..Default::default()
    };
    let (adv_serial, _e1) = gather_after_reopen("dst-rpar-1", seed, serial_cfg, &key).await;
    let (adv_par, _e2) = gather_after_reopen("dst-rpar-8", seed, par_cfg, &key).await;
    assert_eq!(adv_par.len(), N, "parallel gather must settle every stream");
    assert_eq!(
        adv_par, adv_serial,
        "read parallelism must not change WHAT is absorbed"
    );
}

/// #266 pacing pin: with gather micro-pacing configured, a wide sparse
/// gather (a) still settles exactly the same streams to exactly the
/// same boundaries as an unpaced one, and (b) actually parks between
/// frame reads — the duty cycle is real, not a dead knob. A ZERO
/// window parks after EVERY read (the documented maximum-pacing
/// semantics), so the park count equals the read count exactly and
/// the elapsed lower bound is deterministic even on a loaded runner;
/// no upper bound is asserted (that would flake). The FIELD effect
/// (append shed under real SlateDB contention) is validated by the L1
/// certification ladder, not reproducible against an in-memory
/// store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gather_pacing_preserves_outcomes_and_opens_windows() {
    const N: usize = 96;
    let key = skey();

    // read_par 1: parks happen between WAVES, so serial reads keep
    // the zero-window park count exactly equal to the read count.
    let paced_cfg = crate::history::AbsorberConfig {
        gather_pace_window: std::time::Duration::ZERO,
        gather_pace: std::time::Duration::from_millis(1),
        gather_read_par: 1,
        ..Default::default()
    };
    let unpaced_cfg = crate::history::AbsorberConfig {
        gather_pace: std::time::Duration::ZERO,
        gather_read_par: 1,
        ..Default::default()
    };
    let (adv_paced, t_paced, _e1) = gather_with_pacing("dst-pace-on", paced_cfg, &key).await;
    let (adv_unpaced, _t_unpaced, _e2) =
        gather_with_pacing("dst-pace-off", unpaced_cfg, &key).await;

    assert_eq!(adv_paced.len(), N, "paced gather must settle every stream");
    assert_eq!(
        adv_paced, adv_unpaced,
        "pacing must not change WHAT is absorbed, only when reads issue"
    );
    // Zero window parks after every one of the 96 reads: 96 x 1 ms of
    // guaranteed sleep. Assert with margin below it.
    assert!(
        t_paced >= std::time::Duration::from_millis(90),
        "paced gather finished in {t_paced:?} — the pace knob is dead"
    );
}

/// WC #266 gate: a forced sparse-absorption wave over MANY tiny
/// streams must not blow append latency through the shared per-shard
/// committer lane. Storage carries Tigris-shaped latency on EVERY op;
/// paced appends run before the wave (baseline) and during it (wave).
/// The bound is generous — the field failure was multi-second stalls
/// piling 2,048 in-flight, not a 2x degradation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_absorption_wave_bounds_append_latency() {
    let inner = mem();
    let store = FaultStore::uniform(
        inner.clone(),
        4242,
        FaultPlan {
            error_pct: 0,
            lost_response_pct: 0,
            latency_pct: 100,
            latency_ms: (5, 15),
        },
    );
    let key = skey();
    const N: u16 = 192;
    let hashes = gather_hashes(0xA6, N);

    let engine = open_engine_with_settings(
        store.clone(),
        "dst-wave",
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    // Sparse seed: ~2 KiB per stream — the 10k-tenant field shape in
    // miniature.
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 2048).await;
    }

    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig::default(),
    );

    // Probe stream must exist before measuring.
    append_sized(&engine, [0xA7; 16], &key, "", 1024).await;

    let base_p99 = paced_append_p99_ms(&engine, &key, "baseline").await;

    // The WAVE: one gather sweeping every sparse stream, concurrent
    // with the paced appends on the shared 2-worker runtime.
    #[expect(
        clippy::disallowed_methods,
        reason = "sparse_absorption_wave_bounds_append_latency; this test owns the concurrent wave and joins its actual result before asserting latency; replacing the spawned wave with sequential work would stop exercising cross-worker contention"
    )]
    let wave_task = {
        let absorber = absorber;
        let hashes = hashes.clone();
        tokio::spawn(async move {
            absorber
                .absorb_gather_v2(&hashes)
                .await
                .expect("wave gather")
        })
    };
    let wave_p99 = paced_append_p99_ms(&engine, &key, "during-wave").await;
    wave_task
        .await
        .expect("wave task must complete without panicking");

    assert!(
        wave_p99 <= base_p99 * 4 + 150,
        "append p99 under a sparse absorption wave must stay bounded: \
         baseline {base_p99}ms vs wave {wave_p99}ms"
    );
}

/// V2_LANE_PER_TICK x 4 MiB (~4 GiB) in ONE WriteBatch before any
/// backpressure could apply. The aggregate budget must pack streams up
/// to gather_max_bytes and defer the rest to later gathers — with no
/// stream starved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_gather_packs_to_the_aggregate_budget() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 91, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hashes: Vec<[u8; 16]> = (0u8..6).map(|i| [0x70 + i; 16]).collect();

    let db = slatedb::Db::builder("dst-budget", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-budget".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    for h in &hashes {
        append_sized(&engine, *h, &key, "", 16 * 1024).await;
    }

    // Unstarted absorber: gathers are driven directly so packing is
    // deterministic. ~16.6 KiB per unkeyed chunk against a 40 KiB budget
    // means exactly two streams per gather.
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 40 * 1024,
            ..Default::default()
        },
    );
    let mut per_gather = Vec::new();
    let mut first_deferred = None;
    for _ in 0..6 {
        let outcome = absorber.absorb_gather_v2(&hashes).await.expect("gather");
        if outcome.advanced.is_empty() {
            break;
        }
        if first_deferred.is_none() {
            first_deferred = Some(outcome.deferred_budget.len());
        }
        per_gather.push(outcome.advanced.len());
    }
    assert_eq!(
        per_gather,
        vec![2, 2, 2],
        "budget must pack exactly two 16 KiB streams per gather"
    );
    // Review round 4: streams that did not fit must be REPORTED as
    // budget-deferred (the pump keeps them pending off this signal).
    assert_eq!(
        first_deferred,
        Some(4),
        "the four streams that did not fit must classify as deferred_budget"
    );
    wait_all_absorbed(&engine, &hashes).await;
    engine.begin_close();
}

/// A chunk larger than the whole budget must still make progress — alone
/// — instead of starving (frame bodies can reach the 32 MiB API cap).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_chunk_gathers_alone() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 92, FaultPlan::new(0, 0, 0));
    let key = skey();
    let big = [0x80u8; 16];
    let small_a = [0x81u8; 16];
    let small_b = [0x82u8; 16];

    let db = slatedb::Db::builder("dst-oversize", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-oversize".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    append_sized(&engine, big, &key, "", 200 * 1024).await;
    append_sized(&engine, small_a, &key, "", 16 * 1024).await;
    append_sized(&engine, small_b, &key, "", 16 * 1024).await;

    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 64 * 1024,
            ..Default::default()
        },
    );
    let all = [big, small_a, small_b];
    let g1 = absorber.absorb_gather_v2(&all).await.expect("gather 1");
    assert_eq!(g1.advanced.len(), 1, "oversized chunk must gather alone");
    assert_eq!(g1.advanced[0].0, big);
    assert_eq!(g1.deferred_budget.len(), 2);
    let g2 = absorber.absorb_gather_v2(&all).await.expect("gather 2");
    assert_eq!(
        g2.advanced.len(),
        2,
        "both small streams fit the next gather"
    );
    wait_all_absorbed(&engine, &all).await;
    engine.begin_close();
}

/// ROUTING-V3 §3: postings REPLACED the covering index — a keyed frame
/// is stored once (plus a ~tens-of-bytes postings page), so keyed and
/// unkeyed streams now cost the same against the gather budget. The
/// 40 KiB budget that used to fit ONE keyed 16 KiB stream fits two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_frames_no_longer_count_twice_against_the_budget() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 93, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hashes: Vec<[u8; 16]> = (0u8..2).map(|i| [0x90 + i; 16]).collect();

    let db = slatedb::Db::builder("dst-keyedbudget", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-keyedbudget".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    for h in &hashes {
        append_sized(&engine, *h, &key, "k1", 16 * 1024).await;
    }

    // Keyed chunks now weigh what unkeyed ones do (~16.6 KiB): the
    // canonical row plus a compact postings allowance.
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            gather_max_bytes: 40 * 1024,
            ..Default::default()
        },
    );
    let before = crate::history::POSTINGS_BYTES_WRITTEN.load(Ordering::Relaxed);
    let g1 = absorber.absorb_gather_v2(&hashes).await.expect("gather 1");
    assert_eq!(
        g1.advanced.len(),
        2,
        "postings killed the keyed double-write: both streams fit one budget"
    );
    let postings = crate::history::POSTINGS_BYTES_WRITTEN.load(Ordering::Relaxed) - before;
    let canonical: u64 = g1.advanced.iter().map(|(_, _, b)| *b).sum();
    assert!(postings > 0, "keyed frames must produce postings pages");
    assert!(
        postings * 100 <= canonical * 8,
        "postings bytes must stay within the 8% batch-1 gate: {postings} vs {canonical}"
    );
    wait_all_absorbed(&engine, &hashes).await;
    engine.begin_close();
}

/// Static-audit P1: an unabsorbed tail must be rediscovered by a fresh
/// owner WITHOUT the customer ever touching the stream again. The old
/// rediscovery enumerated resident handles only — a restarted engine
/// has none, so a pre-crash stream's absorption never resumed and trim
/// never advanced. The durable dirty-stream index closes this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untouched_streams_absorb_after_restart() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 94, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xA4u8; 16];

    // Owner A: append + ack with NO absorber running (crash before
    // absorption), then drop the engine without a clean close.
    {
        let db = slatedb::Db::builder("dst-restart", store.clone() as Arc<dyn ObjectStore>)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db A");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        // R25-A: tests use the REAL load path — a fresh DB rebuilds to
        // zero; a reopened DB restores its durable backlog, exactly as
        // the production opener does.
        let __maint = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        let engine_a = crate::shard::ShardEngine::start(
            "dst-restart".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            __maint,
        );
        for _ in 0..5 {
            append_sized(&engine_a, hash, &key, "", 2 * 1024).await;
        }
        // Simulate a crash: close the engine (fencing handoff) but note
        // the absorber never ran, so absorbed == 0 < next == 5 and the
        // dirty marker is durably present.
        engine_a.begin_close();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Owner B: fresh engine + absorber, EMPTY key cache (v2 needs none),
    // and — the point — not a single request for the stream.
    let db = slatedb::Db::builder("dst-restart", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db B");
    let (absorb_tx, absorb_rx) = crate::history::absorber_channel();
    // R25-A: tests use the REAL load path — a fresh DB rebuilds to
    // zero; a reopened DB restores its durable backlog, exactly as
    // the production opener does.
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine_b = crate::shard::ShardEngine::start(
        "dst-restart".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );
    let _absorber = crate::history::Absorber::start(
        store.clone(),
        engine_b.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig {
            threshold_bytes: 1,
            threshold_age: std::time::Duration::from_millis(1),
            tick: std::time::Duration::from_millis(20),
            // Disable the resident-handle sweep entirely: convergence in
            // this test must come from the durable index seed ALONE, and
            // the test itself must not materialize the handle early (the
            // sweep would then find it and mask a broken seed).
            sweep_every: u32::MAX,
            ..Default::default()
        },
        absorb_rx,
    );

    // Wait on the MARKER only — it clears in the same committer batch
    // that brings absorbed up to next, and polling it does not touch the
    // stream.
    let mut cleared = false;
    // 30 s budget: under the fully-parallel release suite this box
    // saturates every core and 10 s starves legitimately converging
    // absorbers (~50% flake rate measured 2026-08-12); a real seed
    // wedge hangs forever, so the longer deadline loses no signal.
    for _ in 0..1500 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let dirty = engine_b.scan_dirty_streams().await.unwrap();
        if !dirty.iter().any(|(h, _, _)| *h == hash) {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "untouched pre-crash stream never absorbed after restart"
    );

    // Only now touch the stream to confirm the boundary state. The
    // marker-clear and the absorbed update land in one committer
    // batch, but under the fully-parallel release suite the freshly
    // opened handle's view of that batch can lag the dirty-scan by a
    // beat (2026-08-16: observed absorbed=0/next=5 immediately after
    // the marker cleared, converging on the next read). The invariant
    // this test guards is CONVERGENCE of index-seeded absorption, so
    // the boundary read polls on the same budget instead of asserting
    // the first sample.
    let mut last = (0u64, 0u64);
    for _ in 0..1500 {
        let st = engine_b.stream_handle(hash).await.unwrap();
        last = {
            let s = st.state.lock().unwrap();
            (s.durable.absorbed, s.durable.next)
        };
        if last.0 == last.1 && last.1 == 5 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(last.1, 5, "durable next lost across restart");
    assert_eq!(
        last.0, last.1,
        "absorbed must equal next after index-seeded absorption"
    );
    engine_b.begin_close();
}
